#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

import requests


DEFAULT_MAPPING_DB = r"D:\ProjectPackage\demo\erp_demo\erp_dingtalk_links.db"


def read_latest_process_instance_id(db_path: str) -> str:
    if not Path(db_path).exists():
        return ""
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            """
            SELECT process_instance_id
            FROM dingtalk_po_links
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            return ""
        return str(row[0] or "").strip()
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Simulate DingTalk approval callback to local server.")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=19110)
    p.add_argument("--path", default="/dingtalk/callback")
    p.add_argument("--shared-token", default="", help="Optional X-Callback-Token.")
    p.add_argument("--mapping-db", default=DEFAULT_MAPPING_DB)
    p.add_argument("--process-instance-id", default="", help="If empty, use latest from mapping db.")
    p.add_argument("--status", default="COMPLETED")
    p.add_argument("--result", default="agree")
    p.add_argument("--event-type", default="bpms_instance_change")
    p.add_argument("--extra-json", default="", help="Optional extra JSON object merged into payload.")
    p.add_argument("--timeout", type=int, default=20)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    process_instance_id = args.process_instance_id.strip() or read_latest_process_instance_id(args.mapping_db)
    if not process_instance_id:
        print("No processInstanceId found. Use --process-instance-id or create one first.")
        return 1

    payload: dict[str, Any] = {
        "EventType": args.event_type,
        "processInstanceId": process_instance_id,
        "status": args.status,
        "result": args.result,
    }
    if args.extra_json:
        try:
            extra = json.loads(args.extra_json)
        except Exception as exc:  # noqa: BLE001
            print(f"--extra-json invalid: {exc}")
            return 2
        if isinstance(extra, dict):
            payload.update(extra)

    headers = {"Content-Type": "application/json"}
    if args.shared_token:
        headers["X-Callback-Token"] = args.shared_token

    path = args.path if args.path.startswith("/") else f"/{args.path}"
    url = f"http://{args.host}:{args.port}{path}"
    try:
        resp = requests.post(
            url,
            data=json.dumps(payload, ensure_ascii=False),
            headers=headers,
            timeout=args.timeout,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"Request failed: {exc}")
        return 1

    print(json.dumps({"statusCode": resp.status_code, "response": resp.text, "payload": payload}, ensure_ascii=False, indent=2))
    return 0 if resp.status_code == 200 else 1


if __name__ == "__main__":
    raise SystemExit(main())

