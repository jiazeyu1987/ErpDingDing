#!/usr/bin/env python3
"""
Query last-year sales orders from Kingdee K3Cloud via ExecuteBillQuery.

Usage example:
python query_last_year_sales_orders.py ^
  --base-url https://your-k3cloud-host ^
  --acct-id 6244xxxx ^
  --username demo ^
  --password your_password ^
  --csv sales_orders.csv
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sys
from typing import Any

import requests


def _normalize_base_url(base_url: str) -> str:
    return base_url.rstrip("/")


def _parse_json(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _service_urls(base_url: str, service_name: str) -> list[str]:
    base = _normalize_base_url(base_url)
    return [
        f"{base}/K3Cloud/{service_name}",
        f"{base}/k3cloud/{service_name}",
        f"{base}/{service_name}",
    ]


def _is_login_success(payload: Any) -> tuple[bool, str]:
    if isinstance(payload, dict):
        if payload.get("LoginResultType") == 1:
            return True, ""
        if payload.get("IsSuccessByAPI") is True:
            return True, ""
        msg = payload.get("Message") or payload.get("message") or "Login failed."
        return False, str(msg)
    return False, "Login response is not JSON object."


def login(
    session: requests.Session,
    base_url: str,
    acct_id: str,
    username: str,
    password: str,
    lcid: int,
    timeout: int,
    verify_ssl: bool,
) -> None:
    login_urls = _service_urls(
        base_url, "Kingdee.BOS.WebApi.ServicesStub.AuthService.ValidateUser.common.kdsvc"
    )
    payload_variants = [
        {
            "acctID": acct_id,
            "username": username,
            "password": password,
            "lcid": lcid,
        },
        {
            "AcctID": acct_id,
            "UserName": username,
            "Password": password,
            "Lcid": lcid,
        },
    ]

    last_err = "Unknown login error."
    for login_url in login_urls:
        for payload in payload_variants:
            resp = session.post(
                login_url,
                data=payload,
                timeout=timeout,
                verify=verify_ssl,
            )
            parsed = _parse_json(resp.text)
            ok, err = _is_login_success(parsed)
            if ok:
                return
            last_err = (
                f"url={login_url} HTTP {resp.status_code}: {err}; response={parsed!r}"
            )

    raise RuntimeError(last_err)


def execute_bill_query(
    session: requests.Session,
    base_url: str,
    query_obj: dict[str, Any],
    timeout: int,
    verify_ssl: bool,
) -> Any:
    query_urls = _service_urls(
        base_url,
        "Kingdee.BOS.WebApi.ServicesStub.DynamicFormService.ExecuteBillQuery.common.kdsvc",
    )
    last_err = "ExecuteBillQuery failed."
    for query_url in query_urls:
        resp = session.post(
            query_url,
            data={"data": json.dumps(query_obj, ensure_ascii=False)},
            timeout=timeout,
            verify=verify_ssl,
        )
        parsed = _parse_json(resp.text)
        if isinstance(parsed, dict):
            if parsed.get("Result", {}).get("ResponseStatus", {}).get("IsSuccess") is False:
                err = parsed["Result"]["ResponseStatus"].get("Errors") or parsed
                last_err = f"url={query_url} ExecuteBillQuery failed: {err}"
                continue
            last_err = f"url={query_url} Unexpected object response: {parsed!r}"
            continue
        if isinstance(parsed, list):
            return parsed
        last_err = f"url={query_url} Unexpected response: {parsed!r}"

    raise RuntimeError(last_err)


def rows_to_dicts(raw: Any, field_keys: str) -> list[dict[str, Any]]:
    fields = [f.strip() for f in field_keys.split(",") if f.strip()]
    if isinstance(raw, list) and raw and isinstance(raw[0], list):
        return [dict(zip(fields, row)) for row in raw]
    if isinstance(raw, list):
        # Some environments might return [] or list[dict].
        if raw and isinstance(raw[0], dict):
            return raw
        return []
    return []


def build_query(
    form_id: str,
    field_keys: str,
    start_date: dt.date,
    end_date: dt.date,
    order_string: str,
    start_row: int,
    limit: int,
    only_audited: bool,
) -> dict[str, Any]:
    # End date is inclusive for user intent; filter uses < next day to avoid time edge cases.
    next_day = end_date + dt.timedelta(days=1)
    filter_parts = [
        f"FDate >= '{start_date.isoformat()}'",
        f"FDate < '{next_day.isoformat()}'",
    ]
    if only_audited:
        filter_parts.append("FDocumentStatus = 'C'")

    return {
        "FormId": form_id,
        "FieldKeys": field_keys,
        "FilterString": " and ".join(filter_parts),
        "OrderString": order_string,
        "StartRow": start_row,
        "Limit": limit,
    }


def save_csv(rows: list[dict[str, Any]], csv_path: str) -> None:
    if not rows:
        with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
            f.write("")
        return
    fieldnames = list(rows[0].keys())
    with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query last-year sales orders via Kingdee ExecuteBillQuery."
    )
    parser.add_argument("--base-url", required=True, help="K3Cloud host, e.g. https://xxx")
    parser.add_argument("--acct-id", required=True, help="Data center DB ID (账套ID)")
    parser.add_argument("--username", required=True, help="K3Cloud username")
    parser.add_argument("--password", required=True, help="K3Cloud password")
    parser.add_argument("--lcid", type=int, default=2052, help="Language code, default 2052")

    parser.add_argument("--form-id", default="SAL_SaleOrder", help="Business object FormId")
    parser.add_argument(
        "--field-keys",
        default="FID,FBillNo,FDate,FCustId.FNumber,FDocumentStatus",
        help="Comma-separated query fields",
    )
    parser.add_argument(
        "--order-string",
        default="FDate DESC,FID DESC",
        help="Order clause",
    )
    parser.add_argument("--start-row", type=int, default=0, help="Pagination start row")
    parser.add_argument("--limit", type=int, default=2000, help="Max rows")
    parser.add_argument(
        "--all-pages",
        action="store_true",
        help="Fetch all pages by looping StartRow += Limit",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=200,
        help="Safety cap when --all-pages is enabled",
    )
    parser.add_argument(
        "--only-audited",
        action="store_true",
        help="Only include audited documents (FDocumentStatus='C')",
    )
    parser.add_argument(
        "--end-date",
        default=dt.date.today().isoformat(),
        help="End date YYYY-MM-DD, default today",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=365,
        help="Lookback days from end-date, default 365",
    )
    parser.add_argument("--csv", help="Optional CSV output path")
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="Disable SSL certificate verification",
    )
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        end_date = dt.date.fromisoformat(args.end_date)
    except ValueError:
        print(f"Invalid --end-date: {args.end_date}", file=sys.stderr)
        return 2
    start_date = end_date - dt.timedelta(days=args.days)

    session = requests.Session()
    verify_ssl = not args.insecure
    try:
        login(
            session=session,
            base_url=args.base_url,
            acct_id=args.acct_id,
            username=args.username,
            password=args.password,
            lcid=args.lcid,
            timeout=args.timeout,
            verify_ssl=verify_ssl,
        )
        if args.all_pages:
            rows: list[dict[str, Any]] = []
            start_row = args.start_row
            for page in range(args.max_pages):
                query_obj = build_query(
                    form_id=args.form_id,
                    field_keys=args.field_keys,
                    start_date=start_date,
                    end_date=end_date,
                    order_string=args.order_string,
                    start_row=start_row,
                    limit=args.limit,
                    only_audited=args.only_audited,
                )
                raw = execute_bill_query(
                    session=session,
                    base_url=args.base_url,
                    query_obj=query_obj,
                    timeout=args.timeout,
                    verify_ssl=verify_ssl,
                )
                batch = rows_to_dicts(raw, args.field_keys)
                rows.extend(batch)
                if len(batch) < args.limit:
                    break
                start_row += args.limit
            else:
                print(
                    f"Warning: reached --max-pages={args.max_pages}, data may be truncated.",
                    file=sys.stderr,
                )
        else:
            query_obj = build_query(
                form_id=args.form_id,
                field_keys=args.field_keys,
                start_date=start_date,
                end_date=end_date,
                order_string=args.order_string,
                start_row=args.start_row,
                limit=args.limit,
                only_audited=args.only_audited,
            )
            raw = execute_bill_query(
                session=session,
                base_url=args.base_url,
                query_obj=query_obj,
                timeout=args.timeout,
                verify_ssl=verify_ssl,
            )
            rows = rows_to_dicts(raw, args.field_keys)
    except Exception as exc:  # noqa: BLE001 - we want a friendly CLI error message.
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "formId": args.form_id,
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
                "count": len(rows),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    preview = rows[:10]
    if preview:
        print(json.dumps(preview, ensure_ascii=False, indent=2))
    else:
        print("No rows returned.")

    if args.csv:
        save_csv(rows, args.csv)
        print(f"CSV saved: {args.csv}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
