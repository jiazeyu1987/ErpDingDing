#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

import query_last_year_sales_orders as k3


LINK_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS dingtalk_po_links (
  process_instance_id TEXT PRIMARY KEY,
  po_fid TEXT NOT NULL,
  po_bill_no TEXT,
  org_no TEXT,
  supplier_no TEXT,
  created_at TEXT NOT NULL,
  dingtalk_create_payload TEXT,
  callback_status TEXT,
  callback_result TEXT,
  callback_time TEXT,
  last_callback_raw TEXT,
  erp_writeback_ok INTEGER,
  erp_writeback_msg TEXT,
  erp_writeback_time TEXT,
  updated_at TEXT NOT NULL
);
"""

LINK_INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_dingtalk_po_links_fid ON dingtalk_po_links(po_fid);",
    "CREATE INDEX IF NOT EXISTS idx_dingtalk_po_links_billno ON dingtalk_po_links(po_bill_no);",
]


def now_iso() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def ensure_link_db(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(LINK_TABLE_SQL)
        for sql in LINK_INDEX_SQL:
            conn.execute(sql)
        conn.commit()
    finally:
        conn.close()


def upsert_link_record(
    db_path: str,
    *,
    process_instance_id: str,
    po_fid: str,
    po_bill_no: str,
    org_no: str,
    supplier_no: str,
    dingtalk_create_payload: dict[str, Any] | None,
) -> None:
    ensure_link_db(db_path)
    created_at = now_iso()
    payload_text = json.dumps(dingtalk_create_payload or {}, ensure_ascii=False)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO dingtalk_po_links (
              process_instance_id, po_fid, po_bill_no, org_no, supplier_no,
              created_at, dingtalk_create_payload, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(process_instance_id) DO UPDATE SET
              po_fid=excluded.po_fid,
              po_bill_no=excluded.po_bill_no,
              org_no=excluded.org_no,
              supplier_no=excluded.supplier_no,
              dingtalk_create_payload=excluded.dingtalk_create_payload,
              updated_at=excluded.updated_at
            """,
            (
                process_instance_id,
                po_fid,
                po_bill_no,
                org_no,
                supplier_no,
                created_at,
                payload_text,
                created_at,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_link_by_process_instance_id(db_path: str, process_instance_id: str) -> dict[str, Any] | None:
    ensure_link_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM dingtalk_po_links WHERE process_instance_id = ?",
            (process_instance_id,),
        ).fetchone()
        if row is None:
            return None
        return dict(row)
    finally:
        conn.close()


def update_callback_info(
    db_path: str,
    *,
    process_instance_id: str,
    callback_status: str,
    callback_result: str,
    callback_time: str,
    raw_payload: dict[str, Any],
) -> None:
    ensure_link_db(db_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            UPDATE dingtalk_po_links
            SET callback_status = ?,
                callback_result = ?,
                callback_time = ?,
                last_callback_raw = ?,
                updated_at = ?
            WHERE process_instance_id = ?
            """,
            (
                callback_status,
                callback_result,
                callback_time,
                json.dumps(raw_payload, ensure_ascii=False),
                now_iso(),
                process_instance_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def update_writeback_result(
    db_path: str,
    *,
    process_instance_id: str,
    ok: bool,
    message: str,
) -> None:
    ensure_link_db(db_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            UPDATE dingtalk_po_links
            SET erp_writeback_ok = ?,
                erp_writeback_msg = ?,
                erp_writeback_time = ?,
                updated_at = ?
            WHERE process_instance_id = ?
            """,
            (1 if ok else 0, message, now_iso(), now_iso(), process_instance_id),
        )
        conn.commit()
    finally:
        conn.close()


def parse_save_response(resp_text: str) -> tuple[bool, Any]:
    try:
        parsed = json.loads(resp_text)
    except Exception:  # noqa: BLE001
        return False, resp_text
    if not isinstance(parsed, dict):
        return False, parsed
    status = parsed.get("Result", {}).get("ResponseStatus", {})
    if status.get("IsSuccess") is True:
        return True, parsed
    return False, status.get("Errors") or status or parsed


def stringify_error(payload: Any) -> str:
    if isinstance(payload, (dict, list)):
        return json.dumps(payload, ensure_ascii=False)
    return str(payload)


def parse_int(value: Any, default: int = 0) -> int:
    try:
        text = str(value).strip()
        if not text:
            return default
        return int(text)
    except Exception:  # noqa: BLE001
        return default


def _contains_any(text: str, markers: tuple[str, ...]) -> bool:
    if not text:
        return False
    lower = text.lower()
    return any(marker in lower for marker in markers)


SUBMIT_NOT_NEEDED_MARKERS = (
    "\u5df2\u63d0\u4ea4",
    "\u4e0d\u5141\u8bb8\u63d0\u4ea4",
    "\u53ea\u6709\u6682\u5b58\u3001\u521b\u5efa\u548c\u91cd\u65b0\u5ba1\u6838\u7684\u6570\u636e\u624d\u5141\u8bb8\u63d0\u4ea4",
    "already submit",
)

AUDIT_ALREADY_MARKERS = (
    "\u5df2\u5ba1\u6838",
    "already audit",
)

NON_RETRYABLE_WRITEBACK_MARKERS = (
    "\u6ca1\u6709\u4efb\u52a1\u5904\u7406\u6743\u9650",
    "\u5f53\u524d\u7528\u6237\u4e0d\u662f\u5f85\u529e\u4efb\u52a1\u7684\u5904\u7406\u4eba",
    "\u5355\u636e\u5df2\u5173\u8054\u5de5\u4f5c\u6d41\u5b9e\u4f8b",
    "\u53ea\u6709\u6682\u5b58\u3001\u521b\u5efa\u548c\u91cd\u65b0\u5ba1\u6838\u7684\u6570\u636e\u624d\u5141\u8bb8\u63d0\u4ea4",
)

RETRYABLE_WRITEBACK_MARKERS = (
    "\u4e1a\u52a1\u64cd\u4f5c",
    "\u51b2\u7a81",
    "\u8bf7\u7a0d\u5019\u518d\u4f7f\u7528",
    "\u6b63\u5728\u4f7f\u7528",
    "\u5355\u636e\u5df2\u88ab\u9501\u5b9a",
    "operation conflict",
    "is being used",
    "locked",
)


def is_non_retryable_writeback_message(message: str) -> bool:
    return _contains_any(message, NON_RETRYABLE_WRITEBACK_MARKERS)


def is_retryable_writeback_message(message: str) -> bool:
    return _contains_any(message, RETRYABLE_WRITEBACK_MARKERS)


@dataclass(frozen=True)
class ErpWritebackConfig:
    base_url: str
    acct_id: str
    username: str
    password: str
    lcid: int
    timeout: int
    verify_ssl: bool
    form_id: str = "PUR_PurchaseOrder"
    field_status: str = ""
    field_result: str = ""
    field_instance_id: str = ""
    field_callback_time: str = ""
    field_note: str = "FNote"
    probe_fields: bool = True
    approve_mode: str = "submit_audit"
    approve_comment: str = "Approved by DingTalk callback"
    workflow_user_id: str = ""
    workflow_user_name: str = ""
    workflow_post_id: str = ""
    workflow_post_number: str = ""
    workflow_approval_type: str = "1"


class ErpWritebackService:
    def __init__(self, config: ErpWritebackConfig):
        self.config = config
        self.session = requests.Session()
        self._field_cache: dict[str, bool] = {}

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

    def _save_dynamic_once(self, model_obj: dict[str, Any], need_update_fields: list[str]) -> tuple[bool, Any]:
        payload_obj = {"NeedUpDateFields": need_update_fields, "Model": model_obj}
        payload = {"formid": self.config.form_id, "data": json.dumps(payload_obj, ensure_ascii=False)}
        urls = [
            f"{self.config.base_url.rstrip('/')}/K3Cloud/Kingdee.BOS.WebApi.ServicesStub.DynamicFormService.Save.common.kdsvc",
            f"{self.config.base_url.rstrip('/')}/k3cloud/Kingdee.BOS.WebApi.ServicesStub.DynamicFormService.Save.common.kdsvc",
            f"{self.config.base_url.rstrip('/')}/Kingdee.BOS.WebApi.ServicesStub.DynamicFormService.Save.common.kdsvc",
        ]
        attempts: list[dict[str, Any]] = []
        for url in urls:
            resp = self.session.post(
                url,
                data=payload,
                timeout=self.config.timeout,
                verify=self.config.verify_ssl,
            )
            ok, parsed = parse_save_response(resp.text)
            if ok:
                return True, parsed
            attempts.append(
                {
                    "transport": "formid+data",
                    "url": url,
                    "httpStatus": resp.status_code,
                    "contentType": resp.headers.get("Content-Type", ""),
                    "error": parsed if parsed != "" else "<empty-response>",
                }
            )
        return (
            False,
            {
                "action": "Save",
                "payload": {"NeedUpDateFields": need_update_fields, "FID": str(model_obj.get("FID", ""))},
                "attempts": attempts,
            },
        )

    def _looks_like_relogin_needed(self, payload: Any) -> bool:
        if not isinstance(payload, dict):
            return False
        attempts = payload.get("attempts")
        if not isinstance(attempts, list):
            return False
        for item in attempts:
            if not isinstance(item, dict):
                continue
            try:
                status = int(item.get("httpStatus", 0))
            except Exception:  # noqa: BLE001
                status = 0
            err_text = stringify_error(item.get("error", "")).lower()
            if status in (401, 403):
                return True
            if any(
                key in err_text
                for key in ("login", "session", "context", "\u767b\u5f55", "\u91cd\u65b0\u767b\u5f55", "\u8bf7\u5148\u767b\u5f55")
            ):
                return True
        return False

    def _save_dynamic(self, model_obj: dict[str, Any], need_update_fields: list[str]) -> tuple[bool, Any]:
        ok, payload = self._save_dynamic_once(model_obj, need_update_fields)
        if ok:
            return True, payload

        if not self._looks_like_relogin_needed(payload):
            return False, payload

        try:
            self.login()
        except Exception as exc:  # noqa: BLE001
            return False, {"action": "Save", "relogin": f"failed: {exc}", "firstAttempt": payload}

        ok2, payload2 = self._save_dynamic_once(model_obj, need_update_fields)
        if ok2:
            return True, payload2
        return False, {"action": "Save", "relogin": "done", "firstAttempt": payload, "secondAttempt": payload2}

    def _call_dynamic_action(self, action_name: str, payload_obj: dict[str, Any]) -> tuple[bool, Any]:
        payload = {"formid": self.config.form_id, "data": json.dumps(payload_obj, ensure_ascii=False)}
        base = self.config.base_url.rstrip("/")
        service_name = f"Kingdee.BOS.WebApi.ServicesStub.DynamicFormService.{action_name}.common.kdsvc"
        urls = [
            f"{base}/K3Cloud/{service_name}",
            f"{base}/k3cloud/{service_name}",
            f"{base}/{service_name}",
        ]
        attempts: list[dict[str, Any]] = []
        for url in urls:
            resp = self.session.post(
                url,
                data=payload,
                timeout=self.config.timeout,
                verify=self.config.verify_ssl,
            )
            ok, parsed = parse_save_response(resp.text)
            if ok:
                return True, parsed
            attempts.append(
                {
                    "transport": "formid+data",
                    "url": url,
                    "httpStatus": resp.status_code,
                    "contentType": resp.headers.get("Content-Type", ""),
                    "error": parsed if parsed != "" else "<empty-response>",
                }
            )
        return False, {"action": action_name, "payload": payload_obj, "attempts": attempts}

    def _call_special_action(self, action_name: str, payload_obj: dict[str, Any]) -> tuple[bool, Any]:
        # Special interfaces (for example WorkflowAudit) are invoked with json data only.
        payload = {"data": json.dumps(payload_obj, ensure_ascii=False)}
        base = self.config.base_url.rstrip("/")
        service_name = f"Kingdee.BOS.WebApi.ServicesStub.DynamicFormService.{action_name}.common.kdsvc"
        urls = [
            f"{base}/K3Cloud/{service_name}",
            f"{base}/k3cloud/{service_name}",
            f"{base}/{service_name}",
        ]
        attempts: list[dict[str, Any]] = []
        for url in urls:
            resp = self.session.post(
                url,
                data=payload,
                timeout=self.config.timeout,
                verify=self.config.verify_ssl,
            )
            ok, parsed = parse_save_response(resp.text)
            if ok:
                return True, parsed
            attempts.append(
                {
                    "transport": "data-only",
                    "url": url,
                    "httpStatus": resp.status_code,
                    "contentType": resp.headers.get("Content-Type", ""),
                    "error": parsed if parsed != "" else "<empty-response>",
                }
            )
        return False, {"action": action_name, "payload": payload_obj, "attempts": attempts}

    def _query_document_status(self, po_fid: str) -> str:
        query_obj = {
            "FormId": self.config.form_id,
            "FieldKeys": "FDocumentStatus",
            "FilterString": f"FID = '{po_fid}'",
            "OrderString": "FID DESC",
            "StartRow": 0,
            "Limit": 1,
        }
        raw = k3.execute_bill_query(
            session=self.session,
            base_url=self.config.base_url,
            query_obj=query_obj,
            timeout=self.config.timeout,
            verify_ssl=self.config.verify_ssl,
        )
        rows = k3.rows_to_dicts(raw, "FDocumentStatus")
        if not rows:
            return ""
        return str(rows[0].get("FDocumentStatus", "")).strip().upper()

    def _is_already_done(self, action_name: str, payload: Any) -> bool:
        text = stringify_error(payload).lower()
        if action_name.lower() == "submit":
            return _contains_any(text, SUBMIT_NOT_NEEDED_MARKERS)
        if action_name.lower() in ("audit", "workflowaudit"):
            return _contains_any(text, AUDIT_ALREADY_MARKERS)
        return False

    def _run_submit(self, po_fid: str) -> tuple[bool, str]:
        ok, payload = self._call_dynamic_action("Submit", {"Ids": po_fid})
        if ok or self._is_already_done("Submit", payload):
            return True, "submit ok"
        return False, f"submit failed: {stringify_error(payload)}"

    def _run_audit(self, po_fid: str) -> tuple[bool, str]:
        ok, payload = self._call_dynamic_action("Audit", {"Ids": po_fid})
        if ok or self._is_already_done("Audit", payload):
            return True, "audit ok"
        return False, f"audit failed: {stringify_error(payload)}"

    def _run_workflow_audit(self, po_fid: str, po_bill_no: str) -> tuple[bool, str]:
        """
        WorkflowAudit is a special DynamicFormService API.
        Official payload keys include:
        FormId, Ids/Numbers, UserId/UserName, ApprovalType, PostId/PostNumber.
        """
        comment = self.config.approve_comment.strip() or "Approved by DingTalk callback"
        user_id = parse_int(self.config.workflow_user_id, 0)
        post_id = parse_int(self.config.workflow_post_id, 0)
        user_name = (self.config.workflow_user_name or self.config.username).strip()
        post_number = self.config.workflow_post_number.strip()
        approval_type = self.config.workflow_approval_type.strip() or "1"

        ids: list[int] = []
        fid_int = parse_int(po_fid, -1)
        if fid_int > 0:
            ids = [fid_int]
        numbers: list[str] = [po_bill_no] if po_bill_no else []

        candidates: list[dict[str, Any]] = []
        if numbers:
            candidates.append(
                {
                    "FormId": self.config.form_id,
                    "Ids": [],
                    "Numbers": numbers,
                    "UserId": user_id,
                    "UserName": user_name,
                    "ApprovalType": approval_type,
                    "ActionResultId": comment,
                    "PostId": post_id,
                    "PostNumber": post_number,
                }
            )
        candidates.append(
            {
                "FormId": self.config.form_id,
                "Ids": ids,
                "Numbers": [],
                "UserId": user_id,
                "UserName": user_name,
                "ApprovalType": approval_type,
                "ActionResultId": comment,
                "PostId": post_id,
                "PostNumber": post_number,
            }
        )

        last_msg = "workflow audit failed"
        for payload_obj in candidates:
            ok, payload = self._call_special_action("WorkflowAudit", payload_obj)
            if ok or self._is_already_done("WorkflowAudit", payload):
                return True, "workflow audit ok"
            last_msg = f"workflow audit failed: {stringify_error(payload)}"
        return False, last_msg

    def _auto_approve_if_needed(self, po_fid: str, po_bill_no: str, callback_status: str) -> tuple[bool, str]:
        mode = self.config.approve_mode.strip().lower()
        if callback_status.strip().upper() != "APPROVED":
            return True, "skip: callback status not APPROVED"
        if mode == "none":
            return True, "skip: approve mode none"

        try:
            status = self._query_document_status(po_fid)
        except Exception as exc:  # noqa: BLE001
            return False, f"query FDocumentStatus failed: {exc}"
        if status == "C":
            return True, "already audited"

        if mode == "workflow":
            return self._run_workflow_audit(po_fid, po_bill_no)

        # Status B usually means document is already submitted/in-approval.
        # In this case Submit is often rejected, so try Audit directly.
        if status == "B":
            audit_ok, audit_msg = self._run_audit(po_fid)
            if not audit_ok:
                return False, f"submit skipped(status=B); {audit_msg}"
            return True, f"submit skipped(status=B); {audit_msg}"

        submit_ok, submit_msg = self._run_submit(po_fid)
        if not submit_ok:
            # ERP may explicitly reject Submit in current state; then fallback to Audit once.
            if _contains_any(submit_msg.lower(), SUBMIT_NOT_NEEDED_MARKERS):
                audit_ok, audit_msg = self._run_audit(po_fid)
                if not audit_ok:
                    return False, f"{submit_msg}; {audit_msg}"
                return True, f"submit skipped(by ERP state); {audit_msg}"
            return False, submit_msg
        audit_ok, audit_msg = self._run_audit(po_fid)
        if not audit_ok:
            return False, f"{submit_msg}; {audit_msg}"
        return True, f"{submit_msg}; {audit_msg}"

    def _field_exists(self, field_name: str, po_fid: str) -> bool:
        if not self.config.probe_fields:
            return True
        if field_name in self._field_cache:
            return self._field_cache[field_name]
        filter_str = f"FID = '{po_fid}'"
        query_obj = {
            "FormId": self.config.form_id,
            "FieldKeys": field_name,
            "FilterString": filter_str,
            "StartRow": 0,
            "Limit": 1,
        }
        try:
            _ = k3.execute_bill_query(
                session=self.session,
                base_url=self.config.base_url,
                query_obj=query_obj,
                timeout=self.config.timeout,
                verify_ssl=self.config.verify_ssl,
            )
            self._field_cache[field_name] = True
        except Exception:  # noqa: BLE001
            self._field_cache[field_name] = False
        return self._field_cache[field_name]

    def writeback(
        self,
        *,
        po_fid: str,
        po_bill_no: str = "",
        process_instance_id: str,
        callback_status: str,
        callback_result: str,
        callback_time: str,
    ) -> tuple[bool, str]:
        updates: dict[str, str] = {}
        summary = (
            f"[DINGTALK] status={callback_status}; result={callback_result}; "
            f"instance={process_instance_id}; callback={callback_time}"
        )[:500]
        wanted = [
            (self.config.field_status.strip(), callback_status),
            (self.config.field_result.strip(), callback_result),
            (self.config.field_instance_id.strip(), process_instance_id),
            (self.config.field_callback_time.strip(), callback_time),
            (self.config.field_note.strip(), summary),
        ]
        for field_name, value in wanted:
            if not field_name:
                continue
            if self._field_exists(field_name, po_fid):
                updates[field_name] = value

        if not updates:
            approve_ok, approve_msg = self._auto_approve_if_needed(po_fid, po_bill_no, callback_status)
            if not approve_ok:
                return False, (
                    "no writable ERP fields available (check field config or permissions); "
                    f"{approve_msg}"
                )
            return True, f"no field writeback; {approve_msg}"

        model_obj: dict[str, Any] = {"FID": po_fid}
        model_obj.update(updates)
        ok, payload = self._save_dynamic(model_obj, list(updates.keys()))
        if not ok:
            return False, json.dumps(payload, ensure_ascii=False)
        approve_ok, approve_msg = self._auto_approve_if_needed(po_fid, po_bill_no, callback_status)
        if not approve_ok:
            return False, f"updated fields: {', '.join(updates.keys())}; {approve_msg}"
        return True, f"updated fields: {', '.join(updates.keys())}; {approve_msg}"


