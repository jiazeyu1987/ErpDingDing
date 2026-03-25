#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from dingtalk_erp_bridge import upsert_link_record
import query_last_year_sales_orders as k3


DEFAULT_BASE_URL = "http://172.30.30.8"
DEFAULT_ACCT_ID = "6977227150362f"
DEFAULT_USERNAME = "\u8d3e\u6cfd\u5b87"
DEFAULT_PASSWORD = "Showgood1987!"
DEFAULT_LCID = 2052
DEFAULT_ORG_NO = "881"
DEFAULT_DINGTALK_ENV_FILE = r"D:\ProjectPackage\demo\dingding_demo\code\.env"
DEFAULT_DINGTALK_API_BASE = "https://api.dingtalk.com"
DEFAULT_MAPPING_DB = r"D:\ProjectPackage\demo\erp_demo\erp_dingtalk_links.db"

PURCHASE_FIELD_KEYS = (
    "FID,FBillNo,FDate,FCreateDate,FModifyDate,FDocumentStatus,"
    "FPurchaseOrgId.FNumber,FSupplierId.FNumber,FSupplierId.FName,"
    "FMaterialId.FNumber,FMaterialId.FName,FQty,FReceiveQty,FSrcBillNo"
)

DEFAULT_DINGTALK_FIELD_IDS = [
    "TextField-IH4T9JQ9=ERP new purchase order {FBillNo}",
    "DDSelectField-IH4T9JQA=\u529e\u516c\u7528\u54c1",
    "DDSelectField-IH4T9JQI=\u73b0\u91d1",
]
DEFAULT_DINGTALK_TABLE_ROWS = [
    (
        "TableField-IH4T9JQB|"
        "TextField-IH4T9JQC={LineMaterialNo},"
        "TextField-IH4T9JQD={LineMaterialName},"
        "NumberField-IH4T9JQF={LineQty},"
        "TextField-IH4T9JQH=pcs,"
        "NumberField-IH4T9JQG=0"
    )
]

TEMPLATE_TOKEN_RE = re.compile(r"\{([A-Za-z0-9_]+)\}")


def now_iso() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def to_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def parse_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(str(value))
    except Exception:  # noqa: BLE001
        return default


def safe_int(value: Any, default: int) -> int:
    try:
        if value is None or value == "":
            return default
        return int(str(value).strip())
    except Exception:  # noqa: BLE001
        return default


def normalize_date_text(value: Any) -> str:
    text = to_text(value).strip()
    if "T" in text:
        return text.split("T", 1)[0]
    return text


def render_template(template: str, ctx: dict[str, Any]) -> str:
    def repl(match: re.Match[str]) -> str:
        key = match.group(1)
        return to_text(ctx.get(key, ""))

    return TEMPLATE_TOKEN_RE.sub(repl, template)


def print_json_line(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False), flush=True)


def load_env_file(path: str) -> dict[str, str]:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        return {}

    env_map: dict[str, str] = {}
    text = p.read_text(encoding="utf-8-sig", errors="ignore")
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, val = line.split("=", 1)
        key = key.strip().lstrip("\ufeff")
        val = val.strip()
        if not key:
            continue
        if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
            val = val[1:-1]
        env_map[key] = val
    return env_map


def first_non_empty(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def parse_id_value_items(items: list[str], flag: str) -> list[tuple[str, str]]:
    parsed: list[tuple[str, str]] = []
    for raw in items:
        if "=" not in raw:
            raise RuntimeError(f"{flag} requires COMPONENT_ID=VALUE_TEMPLATE, got: {raw}")
        cid, template = raw.split("=", 1)
        cid = cid.strip()
        template = template.strip()
        if not cid:
            raise RuntimeError(f"{flag} has empty component id: {raw}")
        parsed.append((cid, template))
    return parsed


@dataclass(frozen=True)
class TableRowTemplate:
    table_id: str
    child_templates: dict[str, str]


def parse_table_row_templates(items: list[str]) -> list[TableRowTemplate]:
    templates: list[TableRowTemplate] = []
    for raw in items:
        if "|" not in raw:
            raise RuntimeError(
                f"--dingtalk-table-row requires TABLE_ID|CHILD_ID=VALUE_TEMPLATE,..., got: {raw}"
            )
        table_id, kv_part = raw.split("|", 1)
        table_id = table_id.strip()
        if not table_id:
            raise RuntimeError(f"--dingtalk-table-row has empty table id: {raw}")
        child_templates: dict[str, str] = {}
        for pair in kv_part.split(","):
            part = pair.strip()
            if not part:
                continue
            if "=" not in part:
                raise RuntimeError(
                    f"--dingtalk-table-row child item must be CHILD_ID=VALUE_TEMPLATE, got: {part}"
                )
            child_id, template = part.split("=", 1)
            child_id = child_id.strip()
            template = template.strip()
            if not child_id:
                raise RuntimeError(f"--dingtalk-table-row has empty child id in: {part}")
            child_templates[child_id] = template
        if not child_templates:
            raise RuntimeError(f"--dingtalk-table-row has no child templates: {raw}")
        templates.append(TableRowTemplate(table_id=table_id, child_templates=child_templates))
    return templates


@dataclass(frozen=True)
class ErpConfig:
    base_url: str
    acct_id: str
    username: str
    password: str
    lcid: int
    org_no: str
    days: int
    timeout: int
    verify_ssl: bool


def build_purchase_filter(org_no: str, days: int) -> str:
    parts: list[str] = []
    org_no = org_no.strip()
    if org_no:
        parts.append(f"FPurchaseOrgId.FNumber = '{org_no}'")
    if days > 0:
        today = dt.date.today()
        start_date = today - dt.timedelta(days=days)
        next_day = today + dt.timedelta(days=1)
        start_dt = f"{start_date.isoformat()} 00:00:00"
        next_dt = f"{next_day.isoformat()} 00:00:00"
        # Use create time as primary window for "new order" monitoring.
        # Keep FDate fallback to avoid missing records in environments where
        # create-time filtering behaves differently.
        parts.append(
            "("
            f"(FCreateDate >= '{start_dt}' and FCreateDate < '{next_dt}')"
            f" or (FDate >= '{start_date.isoformat()}' and FDate < '{next_day.isoformat()}')"
            ")"
        )
    return " and ".join(parts)


class ErpClient:
    def __init__(self, config: ErpConfig):
        self.config = config
        self.session = requests.Session()

    def login(self) -> None:
        k3.login(
            session=self.session,
            base_url=self.config.base_url,
            acct_id=self.config.acct_id,
            username=self.config.username,
            password=self.config.password,
            lcid=self.config.lcid,
            timeout=self.config.timeout,
            verify_ssl=self.config.verify_ssl,
        )

    def execute_bill_query_retry(self, query_obj: dict[str, Any]) -> Any:
        try:
            return k3.execute_bill_query(
                session=self.session,
                base_url=self.config.base_url,
                query_obj=query_obj,
                timeout=self.config.timeout,
                verify_ssl=self.config.verify_ssl,
            )
        except Exception as first_exc:  # noqa: BLE001
            first_msg = str(first_exc)
            lower_msg = first_msg.lower()
            if "session" not in lower_msg and "login" not in lower_msg:
                raise
            self.login()
            return k3.execute_bill_query(
                session=self.session,
                base_url=self.config.base_url,
                query_obj=query_obj,
                timeout=self.config.timeout,
                verify_ssl=self.config.verify_ssl,
            )

    def fetch_recent_purchase_rows(self, scan_limit: int) -> list[dict[str, Any]]:
        query: dict[str, Any] = {
            "FormId": "PUR_PurchaseOrder",
            "FieldKeys": PURCHASE_FIELD_KEYS,
            "OrderString": "FID DESC",
            "StartRow": 0,
            "Limit": scan_limit,
        }
        filter_str = build_purchase_filter(self.config.org_no, self.config.days)
        if filter_str:
            query["FilterString"] = filter_str
        raw = self.execute_bill_query_retry(query)
        return k3.rows_to_dicts(raw, PURCHASE_FIELD_KEYS)


def group_rows_by_fid(rows: list[dict[str, Any]]) -> tuple[list[str], dict[str, list[dict[str, Any]]]]:
    ordered_fids: list[str] = []
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        fid = to_text(row.get("FID")).strip()
        if not fid:
            continue
        if fid not in grouped:
            grouped[fid] = []
            ordered_fids.append(fid)
        grouped[fid].append(row)
    return ordered_fids, grouped


def fid_sort_key(fid: str) -> tuple[int, int | str]:
    try:
        return (0, int(float(fid)))
    except Exception:  # noqa: BLE001
        return (1, fid)


def build_order_context(order_rows: list[dict[str, Any]], timestamp: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if not order_rows:
        return {}, []

    header = order_rows[0]
    line_contexts: list[dict[str, Any]] = []
    total_qty = 0.0
    for row in order_rows:
        line_qty = to_text(row.get("FQty")).strip()
        total_qty += parse_float(line_qty, 0.0)
        line_contexts.append(
            {
                "LineMaterialNo": to_text(row.get("FMaterialId.FNumber")).strip(),
                "LineMaterialName": to_text(row.get("FMaterialId.FName")).strip(),
                "LineQty": line_qty,
                "LineReceiveQty": to_text(row.get("FReceiveQty")).strip(),
            }
        )

    first_line = line_contexts[0] if line_contexts else {}
    total_qty_str = f"{total_qty:.6f}".rstrip("0").rstrip(".")
    if not total_qty_str:
        total_qty_str = "0"

    context: dict[str, Any] = {
        "Now": timestamp,
        "FID": to_text(header.get("FID")).strip(),
        "FBillNo": to_text(header.get("FBillNo")).strip(),
        "FDate": normalize_date_text(header.get("FDate")),
        "FCreateDate": to_text(header.get("FCreateDate")).strip(),
        "FModifyDate": to_text(header.get("FModifyDate")).strip(),
        "FDocumentStatus": to_text(header.get("FDocumentStatus")).strip(),
        "OrgNo": to_text(header.get("FPurchaseOrgId.FNumber")).strip(),
        "SupplierNo": to_text(header.get("FSupplierId.FNumber")).strip(),
        "SupplierName": to_text(header.get("FSupplierId.FName")).strip(),
        "MaterialNo": to_text(first_line.get("LineMaterialNo")).strip(),
        "MaterialName": to_text(first_line.get("LineMaterialName")).strip(),
        "Qty": to_text(first_line.get("LineQty")).strip(),
        "ReceiveQty": to_text(first_line.get("LineReceiveQty")).strip(),
        "SrcBillNo": to_text(header.get("FSrcBillNo")).strip(),
        "LineCount": str(len(line_contexts)),
        "TotalQty": total_qty_str,
    }
    return context, line_contexts


def ensure_json_response(resp: requests.Response) -> dict[str, Any]:
    try:
        data = resp.json()
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"HTTP {resp.status_code}, invalid JSON: {resp.text}") from exc
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code}: {json.dumps(data, ensure_ascii=False)}")
    return data


@dataclass(frozen=True)
class DingTalkComponent:
    cid: str
    label: str
    ctype: str
    parent_table_id: str | None


def parse_dingtalk_schema(
    schema: dict[str, Any],
) -> tuple[dict[str, DingTalkComponent], dict[str, list[str]]]:
    components: dict[str, DingTalkComponent] = {}
    table_children: dict[str, list[str]] = {}

    for comp in schema.get("form_component_vos", []):
        if not isinstance(comp, dict):
            continue
        ctype = to_text(comp.get("component_name"))
        props = comp.get("props") or {}
        cid = to_text(props.get("id")).strip()
        label = to_text(props.get("label")).strip() or cid
        if cid:
            components[cid] = DingTalkComponent(
                cid=cid,
                label=label,
                ctype=ctype,
                parent_table_id=None,
            )

        if ctype == "TableField" and cid:
            table_children[cid] = []
            for child in comp.get("children") or []:
                if not isinstance(child, dict):
                    continue
                child_ctype = to_text(child.get("component_name")).strip()
                child_props = child.get("props") or {}
                child_id = to_text(child_props.get("id")).strip()
                child_label = to_text(child_props.get("label")).strip() or child_id
                if child_id:
                    components[child_id] = DingTalkComponent(
                        cid=child_id,
                        label=child_label,
                        ctype=child_ctype,
                        parent_table_id=cid,
                    )
                    table_children[cid].append(child_id)

    return components, table_children


@dataclass(frozen=True)
class DingTalkConfig:
    api_base: str
    app_key: str
    app_secret: str
    process_code: str
    originator_user_id: str
    dept_id: int
    approvers: list[str]
    cc_list: list[str]
    cc_position: str
    timeout: int
    verify_ssl: bool
    field_templates: list[tuple[str, str]]
    table_row_templates: list[TableRowTemplate]


class DingTalkTopApiClient:
    def __init__(self, config: DingTalkConfig):
        self.config = config
        self.session = requests.Session()

    def get_token(self) -> str:
        resp = self.session.post(
            f"{self.config.api_base.rstrip('/')}/v1.0/oauth2/accessToken",
            json={"appKey": self.config.app_key, "appSecret": self.config.app_secret},
            timeout=self.config.timeout,
            verify=self.config.verify_ssl,
        )
        data = ensure_json_response(resp)
        token = data.get("accessToken")
        if not token:
            raise RuntimeError(f"DingTalk accessToken missing: {json.dumps(data, ensure_ascii=False)}")
        return str(token)

    def topapi_post_form(self, token: str, path: str, form_data: dict[str, Any]) -> dict[str, Any]:
        resp = self.session.post(
            f"https://oapi.dingtalk.com{path}",
            params={"access_token": token},
            data=form_data,
            timeout=self.config.timeout,
            verify=self.config.verify_ssl,
        )
        return ensure_json_response(resp)

    def get_schema(self, token: str) -> dict[str, Any]:
        data = self.topapi_post_form(
            token=token,
            path="/topapi/process/form/get",
            form_data={"process_code": self.config.process_code},
        )
        if data.get("errcode") not in (0, "0", None):
            raise RuntimeError(f"DingTalk schema query failed: {json.dumps(data, ensure_ascii=False)}")
        result = data.get("result")
        if not isinstance(result, dict):
            raise RuntimeError(f"DingTalk schema response invalid: {json.dumps(data, ensure_ascii=False)}")
        return result

    def build_form_values(
        self,
        *,
        order_ctx: dict[str, Any],
        line_ctx_list: list[dict[str, Any]],
        components: dict[str, DingTalkComponent],
        table_children: dict[str, list[str]],
    ) -> list[dict[str, str]]:
        values: list[dict[str, str]] = []
        table_rows_grouped: dict[str, list[list[dict[str, str]]]] = {}

        for component_id, template in self.config.field_templates:
            meta = components.get(component_id)
            if not meta:
                raise RuntimeError(f"DingTalk component id not found in schema: {component_id}")
            if meta.ctype == "TableField" or meta.parent_table_id:
                raise RuntimeError(f"DingTalk field template id must be non-table field: {component_id}")
            rendered = render_template(template, order_ctx)
            values.append({"name": meta.label, "value": rendered})

        for table_template in self.config.table_row_templates:
            table_meta = components.get(table_template.table_id)
            if not table_meta or table_meta.ctype != "TableField":
                raise RuntimeError(f"DingTalk table id not found or not table field: {table_template.table_id}")

            child_ids = table_children.get(table_template.table_id) or list(table_template.child_templates.keys())
            raw_templates = list(table_template.child_templates.values())
            contains_line_vars = any("{Line" in tpl for tpl in raw_templates)
            source_lines = line_ctx_list if (contains_line_vars and line_ctx_list) else [{}]

            for line_ctx in source_lines:
                merged_ctx = dict(order_ctx)
                merged_ctx.update(line_ctx)
                one_row: list[dict[str, str]] = []
                for child_id in child_ids:
                    if child_id not in table_template.child_templates:
                        continue
                    child_meta = components.get(child_id)
                    if not child_meta:
                        raise RuntimeError(f"DingTalk child component id not found: {child_id}")
                    child_template = table_template.child_templates[child_id]
                    rendered = render_template(child_template, merged_ctx)
                    one_row.append({"name": child_meta.label, "value": rendered})
                if one_row:
                    table_rows_grouped.setdefault(table_template.table_id, []).append(one_row)

        for table_id, row_list in table_rows_grouped.items():
            table_label = components[table_id].label
            values.append({"name": table_label, "value": json.dumps(row_list, ensure_ascii=False)})

        if not values:
            raise RuntimeError("DingTalk form values are empty")
        return values

    def create_instance(self, order_ctx: dict[str, Any], line_ctx_list: list[dict[str, Any]]) -> dict[str, Any]:
        token = self.get_token()
        schema = self.get_schema(token)
        components, table_children = parse_dingtalk_schema(schema)
        form_values = self.build_form_values(
            order_ctx=order_ctx,
            line_ctx_list=line_ctx_list,
            components=components,
            table_children=table_children,
        )

        payload: dict[str, Any] = {
            "process_code": self.config.process_code,
            "originator_user_id": self.config.originator_user_id,
            "dept_id": str(self.config.dept_id),
            "form_component_values": json.dumps(form_values, ensure_ascii=False),
        }
        if self.config.approvers:
            payload["approvers"] = ",".join(self.config.approvers)
        if self.config.cc_list:
            payload["cc_list"] = ",".join(self.config.cc_list)
            payload["cc_position"] = self.config.cc_position

        create_resp = self.topapi_post_form(
            token=token,
            path="/topapi/processinstance/create",
            form_data=payload,
        )
        if create_resp.get("errcode") not in (0, "0", None):
            raise RuntimeError(f"DingTalk create failed: {json.dumps(create_resp, ensure_ascii=False)}")

        instance_id = to_text(create_resp.get("process_instance_id")).strip()
        if not instance_id:
            raise RuntimeError(f"DingTalk response missing process_instance_id: {json.dumps(create_resp, ensure_ascii=False)}")
        return {"processInstanceId": instance_id, "createResponse": create_resp}


def build_dingtalk_config(args: argparse.Namespace) -> tuple[DingTalkConfig | None, str]:
    if not args.dingtalk_enable:
        return None, "disabled by argument"

    env_file_values = load_env_file(args.dingtalk_env_file)

    def resolve(name: str, cli_value: Any) -> str:
        return first_non_empty(cli_value, os.getenv(name), env_file_values.get(name))

    app_key = resolve("DINGTALK_APP_KEY", args.dingtalk_app_key)
    app_secret = resolve("DINGTALK_APP_SECRET", args.dingtalk_app_secret)
    process_code = resolve("DINGTALK_PROCESS_CODE", args.dingtalk_process_code)
    originator_user_id = resolve("DINGTALK_ORIGINATOR_USER_ID", args.dingtalk_originator_user_id)
    dept_id_text = resolve("DINGTALK_DEPT_ID", args.dingtalk_dept_id)
    api_base = resolve("DINGTALK_API_BASE", args.dingtalk_api_base) or DEFAULT_DINGTALK_API_BASE

    missing: list[str] = []
    if not app_key:
        missing.append("DINGTALK_APP_KEY")
    if not app_secret:
        missing.append("DINGTALK_APP_SECRET")
    if not process_code:
        missing.append("DINGTALK_PROCESS_CODE")
    if not originator_user_id:
        missing.append("DINGTALK_ORIGINATOR_USER_ID")
    if not dept_id_text:
        missing.append("DINGTALK_DEPT_ID")

    if missing:
        return None, f"missing DingTalk config: {', '.join(missing)}"

    dept_id = safe_int(dept_id_text, -1)
    if dept_id <= 0:
        return None, f"invalid DINGTALK_DEPT_ID: {dept_id_text}"

    field_templates = parse_id_value_items(args.dingtalk_field_id, "--dingtalk-field-id")
    table_templates = parse_table_row_templates(args.dingtalk_table_row)
    if not field_templates and not table_templates and not args.dingtalk_no_default_template:
        field_templates = parse_id_value_items(DEFAULT_DINGTALK_FIELD_IDS, "--default-dingtalk-field-id")
        table_templates = parse_table_row_templates(DEFAULT_DINGTALK_TABLE_ROWS)

    if not field_templates and not table_templates:
        return None, "no DingTalk field mapping configured (use --dingtalk-field-id / --dingtalk-table-row)"

    cfg = DingTalkConfig(
        api_base=api_base,
        app_key=app_key,
        app_secret=app_secret,
        process_code=process_code,
        originator_user_id=originator_user_id,
        dept_id=dept_id,
        approvers=[x.strip() for x in args.dingtalk_approver if x.strip()],
        cc_list=[x.strip() for x in args.dingtalk_cc if x.strip()],
        cc_position=args.dingtalk_cc_position,
        timeout=args.dingtalk_timeout,
        verify_ssl=not args.dingtalk_insecure,
        field_templates=field_templates,
        table_row_templates=table_templates,
    )
    return cfg, "enabled"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Monitor new purchase orders and create DingTalk process instances."
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--acct-id", default=DEFAULT_ACCT_ID)
    parser.add_argument("--username", default=DEFAULT_USERNAME)
    parser.add_argument("--password", default=DEFAULT_PASSWORD)
    parser.add_argument("--lcid", type=int, default=DEFAULT_LCID)
    parser.add_argument("--org-no", default=DEFAULT_ORG_NO)
    parser.add_argument("--days", type=int, default=1, help="Recent days window.")
    parser.add_argument("--interval", type=float, default=2.0, help="Poll interval seconds.")
    parser.add_argument("--scan-limit", type=int, default=50, help="Top N rows to scan every loop.")
    parser.add_argument(
        "--from-now",
        action="store_true",
        help="Treat current list as baseline and only emit future new orders.",
    )
    parser.add_argument("--max-loops", type=int, default=0, help="0 means infinite.")
    parser.add_argument("--once", action="store_true", help="Run one scan and exit.")
    parser.add_argument("--timeout", type=int, default=60, help="ERP HTTP timeout seconds.")
    parser.add_argument("--insecure", action="store_true", help="Disable ERP TLS verification.")
    parser.add_argument("--verbose-heartbeat", action="store_true", help="Print heartbeat when no change.")

    parser.add_argument("--dingtalk-enable", action="store_true", help="Enable DingTalk process creation.")
    parser.add_argument("--dingtalk-strict", action="store_true", help="Exit when DingTalk config is invalid.")
    parser.add_argument("--dingtalk-env-file", default=DEFAULT_DINGTALK_ENV_FILE)
    parser.add_argument("--dingtalk-api-base", default="")
    parser.add_argument("--dingtalk-app-key", default="")
    parser.add_argument("--dingtalk-app-secret", default="")
    parser.add_argument("--dingtalk-process-code", default="")
    parser.add_argument("--dingtalk-originator-user-id", default="")
    parser.add_argument("--dingtalk-dept-id", default="")
    parser.add_argument("--dingtalk-approver", action="append", default=[])
    parser.add_argument("--dingtalk-cc", action="append", default=[])
    parser.add_argument(
        "--dingtalk-cc-position",
        choices=["START", "FINISH", "START_FINISH"],
        default="FINISH",
    )
    parser.add_argument(
        "--dingtalk-field-id",
        action="append",
        default=[],
        metavar="COMPONENT_ID=VALUE_TEMPLATE",
    )
    parser.add_argument(
        "--dingtalk-table-row",
        action="append",
        default=[],
        metavar="TABLE_ID|CHILD_ID=VALUE_TEMPLATE,...",
    )
    parser.add_argument(
        "--dingtalk-no-default-template",
        action="store_true",
        help="Do not auto-use built-in template mapping when no mapping is provided.",
    )
    parser.add_argument("--dingtalk-timeout", type=int, default=30)
    parser.add_argument("--dingtalk-insecure", action="store_true", help="Disable DingTalk TLS verification.")
    parser.add_argument("--mapping-db", default=DEFAULT_MAPPING_DB, help="SQLite file for ERP<->DingTalk mapping.")
    parser.add_argument("--disable-link-db", action="store_true", help="Disable mapping upsert.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.scan_limit <= 0:
        print_json_line({"level": "error", "time": now_iso(), "message": "--scan-limit must be > 0"})
        return 2
    if args.interval <= 0 and not args.once:
        print_json_line({"level": "error", "time": now_iso(), "message": "--interval must be > 0"})
        return 2
    if args.days < 0:
        print_json_line({"level": "error", "time": now_iso(), "message": "--days must be >= 0"})
        return 2

    erp_cfg = ErpConfig(
        base_url=args.base_url,
        acct_id=args.acct_id,
        username=args.username,
        password=args.password,
        lcid=args.lcid,
        org_no=args.org_no.strip(),
        days=args.days,
        timeout=args.timeout,
        verify_ssl=not args.insecure,
    )
    erp_client = ErpClient(erp_cfg)

    try:
        erp_client.login()
    except Exception as exc:  # noqa: BLE001
        print_json_line({"level": "error", "time": now_iso(), "message": f"ERP login failed: {exc}"})
        return 1

    dingtalk_cfg, dingtalk_state = build_dingtalk_config(args)
    dingtalk_client: DingTalkTopApiClient | None = None
    if dingtalk_cfg is None and args.dingtalk_enable:
        msg = f"DingTalk disabled: {dingtalk_state}"
        if args.dingtalk_strict:
            print_json_line({"level": "error", "time": now_iso(), "message": msg})
            return 1
        print_json_line({"level": "warn", "time": now_iso(), "message": msg})
    elif dingtalk_cfg is not None:
        dingtalk_client = DingTalkTopApiClient(dingtalk_cfg)

    print_json_line(
        {
            "level": "info",
            "time": now_iso(),
            "event": "monitor_start",
            "formId": "PUR_PurchaseOrder",
            "orgNo": erp_cfg.org_no or "ALL",
            "days": erp_cfg.days,
            "scanLimit": args.scan_limit,
            "intervalSec": args.interval,
            "fromNow": args.from_now,
            "dingtalk": "enabled" if dingtalk_client else f"disabled ({dingtalk_state})",
            "mappingDb": "" if args.disable_link_db else args.mapping_db,
        }
    )

    seen_fids: set[str] = set()
    total_detected = 0
    total_dingtalk_ok = 0
    total_dingtalk_failed = 0

    try:
        baseline_rows = erp_client.fetch_recent_purchase_rows(args.scan_limit)
    except Exception as exc:  # noqa: BLE001
        print_json_line({"level": "error", "time": now_iso(), "message": f"Initial ERP query failed: {exc}"})
        return 1

    baseline_ordered, _baseline_grouped = group_rows_by_fid(baseline_rows)
    if args.from_now:
        seen_fids.update(baseline_ordered)
        print_json_line(
            {
                "level": "info",
                "time": now_iso(),
                "event": "baseline_loaded",
                "baselineCount": len(baseline_ordered),
                "baselineNewestFid": baseline_ordered[0] if baseline_ordered else "",
            }
        )

    loop = 0
    while True:
        loop += 1
        scan_ts = now_iso()

        try:
            rows = erp_client.fetch_recent_purchase_rows(args.scan_limit)
        except Exception as exc:  # noqa: BLE001
            print_json_line({"level": "error", "time": scan_ts, "message": f"ERP query failed: {exc}"})
            if args.once:
                return 1
            time.sleep(args.interval)
            continue

        ordered_fids, grouped = group_rows_by_fid(rows)
        new_fids = [fid for fid in ordered_fids if fid not in seen_fids]
        new_fids.sort(key=fid_sort_key)

        if new_fids:
            for fid in new_fids:
                order_rows = grouped.get(fid, [])
                order_ctx, line_ctx_list = build_order_context(order_rows, scan_ts)
                total_detected += 1
                print_json_line(
                    {
                        "level": "info",
                        "time": scan_ts,
                        "event": "new_purchase_order",
                        "fid": fid,
                        "billNo": order_ctx.get("FBillNo", ""),
                        "orgNo": order_ctx.get("OrgNo", ""),
                        "supplierNo": order_ctx.get("SupplierNo", ""),
                        "supplierName": order_ctx.get("SupplierName", ""),
                        "lineCount": order_ctx.get("LineCount", "0"),
                        "totalQty": order_ctx.get("TotalQty", "0"),
                        "status": order_ctx.get("FDocumentStatus", ""),
                    }
                )

                if dingtalk_client is not None:
                    try:
                        create_result = dingtalk_client.create_instance(order_ctx, line_ctx_list)
                        total_dingtalk_ok += 1
                        print_json_line(
                            {
                                "level": "info",
                                "time": now_iso(),
                                "event": "dingtalk_created",
                                "fid": fid,
                                "billNo": order_ctx.get("FBillNo", ""),
                                "processInstanceId": create_result["processInstanceId"],
                            }
                        )
                        if not args.disable_link_db:
                            try:
                                upsert_link_record(
                                    args.mapping_db,
                                    process_instance_id=create_result["processInstanceId"],
                                    po_fid=str(order_ctx.get("FID", "")),
                                    po_bill_no=str(order_ctx.get("FBillNo", "")),
                                    org_no=str(order_ctx.get("OrgNo", "")),
                                    supplier_no=str(order_ctx.get("SupplierNo", "")),
                                    dingtalk_create_payload=create_result.get("createResponse", {}),
                                )
                                print_json_line(
                                    {
                                        "level": "info",
                                        "time": now_iso(),
                                        "event": "mapping_saved",
                                        "processInstanceId": create_result["processInstanceId"],
                                        "poFid": order_ctx.get("FID", ""),
                                        "db": args.mapping_db,
                                    }
                                )
                            except Exception as db_exc:  # noqa: BLE001
                                print_json_line(
                                    {
                                        "level": "warn",
                                        "time": now_iso(),
                                        "event": "mapping_save_failed",
                                        "processInstanceId": create_result["processInstanceId"],
                                        "poFid": order_ctx.get("FID", ""),
                                        "db": args.mapping_db,
                                        "error": str(db_exc),
                                    }
                                )
                    except Exception as exc:  # noqa: BLE001
                        total_dingtalk_failed += 1
                        print_json_line(
                            {
                                "level": "error",
                                "time": now_iso(),
                                "event": "dingtalk_failed",
                                "fid": fid,
                                "billNo": order_ctx.get("FBillNo", ""),
                                "error": str(exc),
                            }
                        )

                seen_fids.add(fid)
        elif args.verbose_heartbeat:
            print_json_line(
                {
                    "level": "info",
                    "time": scan_ts,
                    "event": "heartbeat",
                    "message": "no new purchase order",
                    "visibleRows": len(ordered_fids),
                    "knownFids": len(seen_fids),
                }
            )

        if args.once:
            break
        if args.max_loops > 0 and loop >= args.max_loops:
            break
        time.sleep(args.interval)

    print_json_line(
        {
            "level": "info",
            "time": now_iso(),
            "event": "monitor_stop",
            "loops": loop,
            "detectedOrders": total_detected,
            "dingtalkSuccess": total_dingtalk_ok,
            "dingtalkFailed": total_dingtalk_failed,
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
