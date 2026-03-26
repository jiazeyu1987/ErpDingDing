#!/usr/bin/env python3
"""
GUI for DingTalk approval callback detection and ERP writeback.

Features:
- Start/stop callback HTTP server.
- Display callback/writeback logs.
- Refresh mapping table from SQLite.
- Simulate callback POST (approved/rejected/canceled).
- Create random ERP purchase order in the same window.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import queue
import random
import sqlite3
import subprocess
import sys
import threading
import time
import tkinter as tk
from http.server import ThreadingHTTPServer
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any

import requests

import dingtalk_approval_callback_server as cb
from dingtalk_erp_bridge import (
    ErpWritebackConfig,
    ErpWritebackService,
    is_non_retryable_writeback_message,
    is_retryable_writeback_message,
    update_callback_info,
    update_writeback_result,
)
import query_last_year_sales_orders as k3


DEFAULT_ENV_FILE = r"D:\ProjectPackage\demo\dingding_demo\code\.env"
DEFAULT_MAPPING_DB = r"D:\ProjectPackage\demo\erp_demo\erp_dingtalk_links.db"
DEFAULT_DINGTALK_ORIGINATOR_ID = "025247281136343306"
DEFAULT_DINGTALK_APPROVER_ID = "143908412435636200"

DEFAULT_ERP_BASE_URL = "http://172.30.30.8"
DEFAULT_ERP_ACCT_ID = "6977227150362f"
DEFAULT_ERP_USERNAME = "\u8d3e\u6cfd\u5b87"
DEFAULT_ERP_PASSWORD = "Showgood1987!"
DINGTALK_USER_CHOICES: list[tuple[str, str]] = [
    ("\u9098\u5bb6\u4e50", "025247281136343306"),
    ("\u6c64\u658c", "3245020131886184"),
    ("\u5f20\u6b63\u5ef7", "204548010024278804"),
    ("\u8d3e\u6cfd\u5b87", "143908412435636200"),
]
DINGTALK_USER_NAME_TO_ID = {name: uid for name, uid in DINGTALK_USER_CHOICES}
DINGTALK_USER_ID_TO_NAME = {uid: name for name, uid in DINGTALK_USER_CHOICES}
ERP_WATCH_FIELD_KEYS = (
    "FID,FBillNo,FDate,FCreateDate,FModifyDate,FDocumentStatus,"
    "FPurchaseOrgId.FNumber,FSupplierId.FNumber,FSupplierId.FName,FMaterialId.FNumber,FQty"
)


def now_iso() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def read_latest_process_instance_id(db_path: str) -> str:
    p = Path(db_path)
    if not p.exists():
        return ""
    conn = sqlite3.connect(str(p))
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


def list_recent_links(db_path: str, limit: int = 200) -> list[dict[str, Any]]:
    p = Path(db_path)
    if not p.exists():
        return []
    conn = sqlite3.connect(str(p))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
              process_instance_id, po_bill_no, po_fid, org_no, supplier_no,
              callback_status, callback_result, erp_writeback_ok,
              erp_writeback_msg, updated_at
            FROM dingtalk_po_links
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def list_poll_targets(db_path: str, *, days: int, limit: int) -> list[dict[str, Any]]:
    p = Path(db_path)
    if not p.exists():
        return []

    where = ""
    params: list[Any] = []
    if days > 0:
        cutoff = (dt.datetime.now() - dt.timedelta(days=days)).isoformat(timespec="seconds")
        where = "WHERE created_at >= ?"
        params.append(cutoff)
    params.append(limit)

    sql = f"""
        SELECT
          process_instance_id,
          po_bill_no,
          po_fid,
          callback_status,
          callback_result,
          erp_writeback_ok,
          erp_writeback_msg,
          created_at,
          updated_at
        FROM dingtalk_po_links
        {where}
        ORDER BY created_at DESC
        LIMIT ?
    """

    conn = sqlite3.connect(str(p))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(sql, tuple(params)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def parse_iso_datetime(text: str) -> dt.datetime | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    for candidate in (raw, raw.replace(" ", "T")):
        try:
            return dt.datetime.fromisoformat(candidate)
        except Exception:  # noqa: BLE001
            continue
    return None


def list_pending_writeback_links(db_path: str, limit: int = 200) -> list[dict[str, Any]]:
    p = Path(db_path)
    if not p.exists():
        return []
    conn = sqlite3.connect(str(p))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
              process_instance_id, po_fid, po_bill_no,
              callback_status, callback_result, callback_time,
              erp_writeback_ok, erp_writeback_msg, erp_writeback_time, updated_at
            FROM dingtalk_po_links
            WHERE ifnull(trim(process_instance_id), '') <> ''
              AND ifnull(trim(po_fid), '') <> ''
              AND ifnull(trim(callback_status), '') <> ''
              AND ifnull(erp_writeback_ok, 0) <> 1
            ORDER BY
              CASE WHEN ifnull(trim(callback_time), '') = '' THEN updated_at ELSE callback_time END ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


WRITEBACK_TIMEOUT_MARKER = "[retry-timeout]"
TERMINAL_DINGTALK_STATUSES = {"APPROVED", "REJECTED", "CANCELED", "CANCELLED", "TERMINATED"}


def is_retry_timeout_writeback_message(message: str) -> bool:
    return WRITEBACK_TIMEOUT_MARKER in str(message or "").lower()


def ensure_json_response(resp: requests.Response) -> dict[str, Any]:
    try:
        data = resp.json()
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"HTTP {resp.status_code}, invalid JSON: {resp.text}") from exc
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code}: {json.dumps(data, ensure_ascii=False)}")
    return data


def dingtalk_get_token(
    session: requests.Session,
    api_base: str,
    app_key: str,
    app_secret: str,
    timeout: int = 30,
) -> str:
    resp = session.post(
        f"{api_base.rstrip('/')}/v1.0/oauth2/accessToken",
        json={"appKey": app_key, "appSecret": app_secret},
        timeout=timeout,
    )
    data = ensure_json_response(resp)
    token = data.get("accessToken")
    if not token:
        raise RuntimeError(f"DingTalk accessToken missing: {json.dumps(data, ensure_ascii=False)}")
    return str(token)


def dingtalk_get_instance_detail(
    session: requests.Session,
    api_base: str,
    token: str,
    process_instance_id: str,
    timeout: int = 30,
) -> dict[str, Any]:
    resp = session.get(
        f"{api_base.rstrip('/')}/v1.0/workflow/processInstances",
        headers={"x-acs-dingtalk-access-token": token},
        params={"processInstanceId": process_instance_id},
        timeout=timeout,
    )
    return ensure_json_response(resp)


def parse_json(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def service_urls(base_url: str, service_name: str) -> list[str]:
    base = base_url.rstrip("/")
    return [
        f"{base}/K3Cloud/{service_name}",
        f"{base}/k3cloud/{service_name}",
        f"{base}/{service_name}",
    ]


def parse_response_status(resp_text: str) -> tuple[bool, Any]:
    parsed = parse_json(resp_text)
    if not isinstance(parsed, dict):
        return False, parsed
    status = parsed.get("Result", {}).get("ResponseStatus", {})
    if status.get("IsSuccess") is True:
        return True, parsed
    return False, status.get("Errors") or status or parsed


def save_purchase_order(
    session: requests.Session,
    base_url: str,
    model: dict[str, Any],
    timeout: int,
    verify_ssl: bool,
) -> tuple[bool, Any]:
    urls = service_urls(
        base_url,
        "Kingdee.BOS.WebApi.ServicesStub.DynamicFormService.Save.common.kdsvc",
    )
    payload = {"formid": "PUR_PurchaseOrder", "data": json.dumps({"Model": model}, ensure_ascii=False)}
    last_err: Any = "Save failed"
    for url in urls:
        resp = session.post(url, data=payload, timeout=timeout, verify=verify_ssl)
        ok, out = parse_response_status(resp.text)
        if ok:
            return True, out
        last_err = {"url": url, "error": out}
    return False, last_err


class CallbackMonitorGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("DingTalk + ERP All-in-One Monitor")
        self.geometry("1480x900")

        self.msg_queue: queue.Queue[tuple[str, Any]] = queue.Queue()
        self.httpd: ThreadingHTTPServer | None = None
        self.server_thread: threading.Thread | None = None
        self.server_running = False
        self.erp_watch_thread: threading.Thread | None = None
        self.erp_watch_running = False
        self.erp_watch_stop_event = threading.Event()
        self.erp_seen_fids: set[str] = set()
        self.erp_detected_rows: list[dict[str, Any]] = []
        self.po_monitor_proc: subprocess.Popen[str] | None = None
        self.po_monitor_reader_thread: threading.Thread | None = None
        self.dt_poll_thread: threading.Thread | None = None
        self.dt_poll_running = False
        self.dt_poll_stop_event = threading.Event()
        self.dt_poll_last_sig: dict[str, str] = {}
        self.wb_retry_thread: threading.Thread | None = None
        self.wb_retry_running = False
        self.wb_retry_stop_event = threading.Event()

        self._build_vars()
        self._build_ui()
        self.after(200, self._poll_queue)
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_vars(self) -> None:
        self.env_file_var = tk.StringVar(value=DEFAULT_ENV_FILE)
        self.host_var = tk.StringVar(value="0.0.0.0")
        self.port_var = tk.StringVar(value="19110")
        self.path_var = tk.StringVar(value="/dingtalk/callback")
        self.shared_token_var = tk.StringVar(value="")
        self.mapping_db_var = tk.StringVar(value=DEFAULT_MAPPING_DB)
        self.writeback_on_var = tk.StringVar(value="approved")

        self.cb_token_var = tk.StringVar(value="")
        self.cb_aes_var = tk.StringVar(value="")
        self.require_signature_var = tk.BooleanVar(value=False)

        self.erp_base_var = tk.StringVar(value=DEFAULT_ERP_BASE_URL)
        self.erp_acct_var = tk.StringVar(value=DEFAULT_ERP_ACCT_ID)
        self.erp_user_var = tk.StringVar(value=DEFAULT_ERP_USERNAME)
        self.erp_pwd_var = tk.StringVar(value=DEFAULT_ERP_PASSWORD)
        self.erp_lcid_var = tk.StringVar(value="2052")
        self.erp_timeout_var = tk.StringVar(value="60")
        self.erp_insecure_var = tk.BooleanVar(value=True)

        self.erp_field_status_var = tk.StringVar(value="")
        self.erp_field_result_var = tk.StringVar(value="")
        self.erp_field_instance_var = tk.StringVar(value="")
        self.erp_field_cbtime_var = tk.StringVar(value="")
        self.erp_field_note_var = tk.StringVar(value="FNote")
        self.no_probe_var = tk.BooleanVar(value=False)
        self.erp_approve_mode_var = tk.StringVar(value="submit_audit")
        self.erp_approve_comment_var = tk.StringVar(value="钉钉审批通过自动审核")
        self.erp_wf_user_id_var = tk.StringVar(value="")
        self.erp_wf_user_name_var = tk.StringVar(value="")
        self.erp_wf_post_id_var = tk.StringVar(value="")
        self.erp_wf_post_no_var = tk.StringVar(value="")
        self.erp_wf_approval_type_var = tk.StringVar(value="1")

        self.sim_pid_var = tk.StringVar(value="")
        self.sim_status_var = tk.StringVar(value="COMPLETED")
        self.sim_result_var = tk.StringVar(value="agree")
        self.sim_event_var = tk.StringVar(value="bpms_instance_change")
        self.status_var = tk.StringVar(value="Idle")
        self.erp_org_no_var = tk.StringVar(value="881")
        self.erp_watch_days_var = tk.StringVar(value="1")
        self.erp_watch_limit_var = tk.StringVar(value="50")
        self.erp_watch_interval_var = tk.StringVar(value="2")
        self.erp_watch_from_now_var = tk.BooleanVar(value=True)
        self.po_monitor_days_var = tk.StringVar(value="1")
        self.po_monitor_limit_var = tk.StringVar(value="50")
        self.po_monitor_interval_var = tk.StringVar(value="2")
        self.po_monitor_from_now_var = tk.BooleanVar(value=True)
        self.po_monitor_status_var = tk.StringVar(value="PO->DingTalk: stopped")
        self.dt_api_base_var = tk.StringVar(value="https://api.dingtalk.com")
        self.dt_app_key_var = tk.StringVar(value="")
        self.dt_app_secret_var = tk.StringVar(value="")
        self.dt_poll_days_var = tk.StringVar(value="30")
        self.dt_poll_limit_var = tk.StringVar(value="200")
        self.dt_poll_interval_var = tk.StringVar(value="8")
        self.dt_poll_auto_writeback_var = tk.BooleanVar(value=True)
        self.dt_poll_status_var = tk.StringVar(value="DingTalk Poll: stopped")
        self.wb_retry_interval_var = tk.StringVar(value="10")
        self.wb_retry_max_minutes_var = tk.StringVar(value="30")
        self.wb_retry_status_var = tk.StringVar(value="Writeback Retry: stopped")
        self.quick_originator_name_var = tk.StringVar(
            value=DINGTALK_USER_ID_TO_NAME.get(DEFAULT_DINGTALK_ORIGINATOR_ID, DINGTALK_USER_CHOICES[0][0])
        )
        self.quick_approver_name_var = tk.StringVar(
            value=DINGTALK_USER_ID_TO_NAME.get(DEFAULT_DINGTALK_APPROVER_ID, DINGTALK_USER_CHOICES[0][0])
        )
        self.create_supplier_no_var = tk.StringVar(value="INT-010")
        self.create_materials_var = tk.StringVar(value="YXN.004.012.1003")
        self.create_qty_min_var = tk.StringVar(value="1")
        self.create_qty_max_var = tk.StringVar(value="20")
        self.create_status_var = tk.StringVar(value="Create: idle")

    def _selected_originator_id(self) -> str:
        name = self.quick_originator_name_var.get().strip()
        return DINGTALK_USER_NAME_TO_ID.get(name, DEFAULT_DINGTALK_ORIGINATOR_ID)

    def _selected_approver_id(self) -> str:
        name = self.quick_approver_name_var.get().strip()
        return DINGTALK_USER_NAME_TO_ID.get(name, DEFAULT_DINGTALK_APPROVER_ID)

    def _build_ui(self) -> None:
        root_tabs = ttk.Notebook(self)
        root_tabs.pack(fill=tk.BOTH, expand=True)

        tab_quick = ttk.Frame(root_tabs, padding=12)
        tab_main = ttk.Frame(root_tabs)
        root_tabs.add(tab_quick, text="快捷操作")
        root_tabs.add(tab_main, text="完整配置")

        quick = ttk.Frame(tab_quick)
        quick.pack(fill=tk.X, anchor=tk.N, pady=(8, 0))
        people_names = [name for name, _ in DINGTALK_USER_CHOICES]

        q_row0 = ttk.Frame(quick)
        q_row0.pack(fill=tk.X, pady=6)
        ttk.Label(q_row0, text="发起人").pack(side=tk.LEFT)
        ttk.Combobox(
            q_row0,
            textvariable=self.quick_originator_name_var,
            values=people_names,
            state="readonly",
            width=20,
        ).pack(side=tk.LEFT, padx=8)
        ttk.Label(q_row0, text="审批人").pack(side=tk.LEFT, padx=(20, 0))
        ttk.Combobox(
            q_row0,
            textvariable=self.quick_approver_name_var,
            values=people_names,
            state="readonly",
            width=20,
        ).pack(side=tk.LEFT, padx=8)

        q_row1 = ttk.Frame(quick)
        q_row1.pack(fill=tk.X, pady=6)
        ttk.Button(q_row1, text="开始监听", command=self._start_all).pack(side=tk.LEFT)
        ttk.Button(q_row1, text="采购订单", command=self._create_random_po).pack(side=tk.LEFT, padx=12)
        ttk.Label(q_row1, textvariable=self.po_monitor_status_var).pack(side=tk.LEFT, padx=16)
        ttk.Label(q_row1, textvariable=self.create_status_var).pack(side=tk.LEFT, padx=8)
        ttk.Label(q_row1, textvariable=self.wb_retry_status_var).pack(side=tk.LEFT, padx=8)

        top = ttk.Frame(tab_main, padding=10)
        top.pack(fill=tk.X)

        row0 = ttk.Frame(top)
        row0.pack(fill=tk.X, pady=2)
        ttk.Label(row0, text="Env File").pack(side=tk.LEFT)
        ttk.Entry(row0, textvariable=self.env_file_var, width=60).pack(side=tk.LEFT, padx=4)
        ttk.Button(row0, text="Load .env", command=self._load_env).pack(side=tk.LEFT, padx=6)
        ttk.Label(row0, textvariable=self.status_var).pack(side=tk.LEFT, padx=12)

        row1 = ttk.Frame(top)
        row1.pack(fill=tk.X, pady=2)
        ttk.Label(row1, text="Host").pack(side=tk.LEFT)
        ttk.Entry(row1, textvariable=self.host_var, width=12).pack(side=tk.LEFT, padx=4)
        ttk.Label(row1, text="Port").pack(side=tk.LEFT)
        ttk.Entry(row1, textvariable=self.port_var, width=8).pack(side=tk.LEFT, padx=4)
        ttk.Label(row1, text="Path").pack(side=tk.LEFT)
        ttk.Entry(row1, textvariable=self.path_var, width=26).pack(side=tk.LEFT, padx=4)
        ttk.Label(row1, text="Writeback").pack(side=tk.LEFT)
        ttk.Combobox(
            row1,
            textvariable=self.writeback_on_var,
            values=["approved", "terminal", "all"],
            width=10,
            state="readonly",
        ).pack(side=tk.LEFT, padx=4)
        ttk.Label(row1, text="SharedToken").pack(side=tk.LEFT)
        ttk.Entry(row1, textvariable=self.shared_token_var, width=20).pack(side=tk.LEFT, padx=4)
        ttk.Label(row1, text="Mapping DB").pack(side=tk.LEFT)
        ttk.Entry(row1, textvariable=self.mapping_db_var, width=42).pack(side=tk.LEFT, padx=4)

        row2 = ttk.Frame(top)
        row2.pack(fill=tk.X, pady=2)
        ttk.Label(row2, text="DingTalk Callback Token").pack(side=tk.LEFT)
        ttk.Entry(row2, textvariable=self.cb_token_var, width=24).pack(side=tk.LEFT, padx=4)
        ttk.Label(row2, text="AES Key").pack(side=tk.LEFT)
        ttk.Entry(row2, textvariable=self.cb_aes_var, width=36, show="*").pack(side=tk.LEFT, padx=4)
        ttk.Checkbutton(
            row2,
            text="Require Signature",
            variable=self.require_signature_var,
        ).pack(side=tk.LEFT, padx=8)

        row3 = ttk.Frame(top)
        row3.pack(fill=tk.X, pady=2)
        ttk.Label(row3, text="ERP Base").pack(side=tk.LEFT)
        ttk.Entry(row3, textvariable=self.erp_base_var, width=22).pack(side=tk.LEFT, padx=4)
        ttk.Label(row3, text="Acct").pack(side=tk.LEFT)
        ttk.Entry(row3, textvariable=self.erp_acct_var, width=18).pack(side=tk.LEFT, padx=4)
        ttk.Label(row3, text="User").pack(side=tk.LEFT)
        ttk.Entry(row3, textvariable=self.erp_user_var, width=10).pack(side=tk.LEFT, padx=4)
        ttk.Label(row3, text="Password").pack(side=tk.LEFT)
        ttk.Entry(row3, textvariable=self.erp_pwd_var, width=14, show="*").pack(side=tk.LEFT, padx=4)
        ttk.Label(row3, text="LCID").pack(side=tk.LEFT)
        ttk.Entry(row3, textvariable=self.erp_lcid_var, width=6).pack(side=tk.LEFT, padx=4)
        ttk.Label(row3, text="Timeout").pack(side=tk.LEFT)
        ttk.Entry(row3, textvariable=self.erp_timeout_var, width=6).pack(side=tk.LEFT, padx=4)
        ttk.Label(row3, text="Org").pack(side=tk.LEFT)
        ttk.Entry(row3, textvariable=self.erp_org_no_var, width=8).pack(side=tk.LEFT, padx=4)
        ttk.Checkbutton(row3, text="Insecure", variable=self.erp_insecure_var).pack(side=tk.LEFT, padx=8)

        row4 = ttk.Frame(top)
        row4.pack(fill=tk.X, pady=2)
        ttk.Label(row4, text="ERP Field Status").pack(side=tk.LEFT)
        ttk.Entry(row4, textvariable=self.erp_field_status_var, width=16).pack(side=tk.LEFT, padx=4)
        ttk.Label(row4, text="Result").pack(side=tk.LEFT)
        ttk.Entry(row4, textvariable=self.erp_field_result_var, width=16).pack(side=tk.LEFT, padx=4)
        ttk.Label(row4, text="InstanceId").pack(side=tk.LEFT)
        ttk.Entry(row4, textvariable=self.erp_field_instance_var, width=16).pack(side=tk.LEFT, padx=4)
        ttk.Label(row4, text="CallbackTime").pack(side=tk.LEFT)
        ttk.Entry(row4, textvariable=self.erp_field_cbtime_var, width=16).pack(side=tk.LEFT, padx=4)
        ttk.Label(row4, text="Note").pack(side=tk.LEFT)
        ttk.Entry(row4, textvariable=self.erp_field_note_var, width=12).pack(side=tk.LEFT, padx=4)
        ttk.Checkbutton(row4, text="No Probe Fields", variable=self.no_probe_var).pack(side=tk.LEFT, padx=8)

        row4b = ttk.Frame(top)
        row4b.pack(fill=tk.X, pady=2)
        ttk.Label(row4b, text="ERP Approve Mode").pack(side=tk.LEFT)
        ttk.Combobox(
            row4b,
            textvariable=self.erp_approve_mode_var,
            values=["submit_audit", "workflow", "none"],
            width=16,
            state="readonly",
        ).pack(side=tk.LEFT, padx=4)
        ttk.Label(row4b, text="Approve Comment").pack(side=tk.LEFT)
        ttk.Entry(row4b, textvariable=self.erp_approve_comment_var, width=52).pack(side=tk.LEFT, padx=4)

        row4c = ttk.Frame(top)
        row4c.pack(fill=tk.X, pady=2)
        ttk.Label(row4c, text="WF UserId").pack(side=tk.LEFT)
        ttk.Entry(row4c, textvariable=self.erp_wf_user_id_var, width=10).pack(side=tk.LEFT, padx=4)
        ttk.Label(row4c, text="WF UserName").pack(side=tk.LEFT)
        ttk.Entry(row4c, textvariable=self.erp_wf_user_name_var, width=14).pack(side=tk.LEFT, padx=4)
        ttk.Label(row4c, text="WF PostId").pack(side=tk.LEFT)
        ttk.Entry(row4c, textvariable=self.erp_wf_post_id_var, width=10).pack(side=tk.LEFT, padx=4)
        ttk.Label(row4c, text="WF PostNo").pack(side=tk.LEFT)
        ttk.Entry(row4c, textvariable=self.erp_wf_post_no_var, width=12).pack(side=tk.LEFT, padx=4)
        ttk.Label(row4c, text="WF ApprovalType").pack(side=tk.LEFT)
        ttk.Entry(row4c, textvariable=self.erp_wf_approval_type_var, width=6).pack(side=tk.LEFT, padx=4)

        row5 = ttk.Frame(top)
        row5.pack(fill=tk.X, pady=(6, 2))
        self.start_btn = ttk.Button(row5, text="Start Detection", command=self._start_server)
        self.start_btn.pack(side=tk.LEFT)
        self.stop_btn = ttk.Button(row5, text="Stop", command=self._stop_server, state=tk.DISABLED)
        self.stop_btn.pack(side=tk.LEFT, padx=8)
        ttk.Button(row5, text="Refresh Mapping", command=self._refresh_mapping).pack(side=tk.LEFT, padx=8)
        ttk.Button(row5, text="Clear Log", command=self._clear_log).pack(side=tk.LEFT, padx=8)

        row6 = ttk.Frame(top)
        row6.pack(fill=tk.X, pady=(2, 2))
        ttk.Label(row6, text="ERP Days").pack(side=tk.LEFT)
        ttk.Entry(row6, textvariable=self.erp_watch_days_var, width=6).pack(side=tk.LEFT, padx=4)
        ttk.Label(row6, text="Limit").pack(side=tk.LEFT)
        ttk.Entry(row6, textvariable=self.erp_watch_limit_var, width=6).pack(side=tk.LEFT, padx=4)
        ttk.Label(row6, text="Interval(s)").pack(side=tk.LEFT)
        ttk.Entry(row6, textvariable=self.erp_watch_interval_var, width=8).pack(side=tk.LEFT, padx=4)
        ttk.Checkbutton(row6, text="From Now", variable=self.erp_watch_from_now_var).pack(side=tk.LEFT, padx=8)
        self.erp_watch_start_btn = ttk.Button(row6, text="Start ERP Watch", command=self._start_erp_watch)
        self.erp_watch_start_btn.pack(side=tk.LEFT, padx=(12, 4))
        self.erp_watch_stop_btn = ttk.Button(
            row6, text="Stop ERP Watch", command=self._stop_erp_watch, state=tk.DISABLED
        )
        self.erp_watch_stop_btn.pack(side=tk.LEFT, padx=4)
        ttk.Button(row6, text="Refresh ERP List", command=self._refresh_erp_list_now).pack(side=tk.LEFT, padx=8)

        row7 = ttk.Frame(top)
        row7.pack(fill=tk.X, pady=(2, 2))
        ttk.Label(row7, text="PO Days").pack(side=tk.LEFT)
        ttk.Entry(row7, textvariable=self.po_monitor_days_var, width=6).pack(side=tk.LEFT, padx=4)
        ttk.Label(row7, text="Limit").pack(side=tk.LEFT)
        ttk.Entry(row7, textvariable=self.po_monitor_limit_var, width=6).pack(side=tk.LEFT, padx=4)
        ttk.Label(row7, text="Interval(s)").pack(side=tk.LEFT)
        ttk.Entry(row7, textvariable=self.po_monitor_interval_var, width=8).pack(side=tk.LEFT, padx=4)
        ttk.Checkbutton(row7, text="From Now", variable=self.po_monitor_from_now_var).pack(side=tk.LEFT, padx=8)
        self.po_start_btn = ttk.Button(row7, text="Start PO->DingTalk", command=self._start_po_monitor)
        self.po_start_btn.pack(side=tk.LEFT, padx=(12, 4))
        self.po_stop_btn = ttk.Button(row7, text="Stop PO->DingTalk", command=self._stop_po_monitor, state=tk.DISABLED)
        self.po_stop_btn.pack(side=tk.LEFT, padx=4)
        ttk.Button(row7, text="Start All", command=self._start_all).pack(side=tk.LEFT, padx=8)
        ttk.Button(row7, text="Stop All", command=self._stop_all).pack(side=tk.LEFT, padx=4)
        ttk.Label(row7, textvariable=self.po_monitor_status_var).pack(side=tk.LEFT, padx=12)

        row7b = ttk.Frame(top)
        row7b.pack(fill=tk.X, pady=(2, 2))
        ttk.Label(row7b, text="DT API").pack(side=tk.LEFT)
        ttk.Entry(row7b, textvariable=self.dt_api_base_var, width=24).pack(side=tk.LEFT, padx=4)
        ttk.Label(row7b, text="AppKey").pack(side=tk.LEFT)
        ttk.Entry(row7b, textvariable=self.dt_app_key_var, width=18).pack(side=tk.LEFT, padx=4)
        ttk.Label(row7b, text="AppSecret").pack(side=tk.LEFT)
        ttk.Entry(row7b, textvariable=self.dt_app_secret_var, width=24, show="*").pack(side=tk.LEFT, padx=4)
        ttk.Label(row7b, text="Days").pack(side=tk.LEFT)
        ttk.Entry(row7b, textvariable=self.dt_poll_days_var, width=6).pack(side=tk.LEFT, padx=4)
        ttk.Label(row7b, text="Limit").pack(side=tk.LEFT)
        ttk.Entry(row7b, textvariable=self.dt_poll_limit_var, width=6).pack(side=tk.LEFT, padx=4)
        ttk.Label(row7b, text="Interval(s)").pack(side=tk.LEFT)
        ttk.Entry(row7b, textvariable=self.dt_poll_interval_var, width=6).pack(side=tk.LEFT, padx=4)
        ttk.Checkbutton(row7b, text="Auto ERP Writeback", variable=self.dt_poll_auto_writeback_var).pack(
            side=tk.LEFT, padx=8
        )
        ttk.Label(row7b, text="WB Retry(s)").pack(side=tk.LEFT)
        ttk.Entry(row7b, textvariable=self.wb_retry_interval_var, width=5).pack(side=tk.LEFT, padx=4)
        ttk.Label(row7b, text="WB Max(min)").pack(side=tk.LEFT)
        ttk.Entry(row7b, textvariable=self.wb_retry_max_minutes_var, width=5).pack(side=tk.LEFT, padx=4)
        self.dt_poll_start_btn = ttk.Button(row7b, text="Start DingTalk Poll", command=self._start_dt_poll)
        self.dt_poll_start_btn.pack(side=tk.LEFT, padx=(8, 4))
        self.dt_poll_stop_btn = ttk.Button(
            row7b, text="Stop DingTalk Poll", command=self._stop_dt_poll, state=tk.DISABLED
        )
        self.dt_poll_stop_btn.pack(side=tk.LEFT, padx=4)
        ttk.Label(row7b, textvariable=self.dt_poll_status_var).pack(side=tk.LEFT, padx=8)

        row8 = ttk.Frame(top)
        row8.pack(fill=tk.X, pady=(2, 2))
        ttk.Label(row8, text="Create Supplier").pack(side=tk.LEFT)
        ttk.Entry(row8, textvariable=self.create_supplier_no_var, width=12).pack(side=tk.LEFT, padx=4)
        ttk.Label(row8, text="Materials(,split)").pack(side=tk.LEFT)
        ttk.Entry(row8, textvariable=self.create_materials_var, width=34).pack(side=tk.LEFT, padx=4)
        ttk.Label(row8, text="QtyMin").pack(side=tk.LEFT)
        ttk.Entry(row8, textvariable=self.create_qty_min_var, width=6).pack(side=tk.LEFT, padx=4)
        ttk.Label(row8, text="QtyMax").pack(side=tk.LEFT)
        ttk.Entry(row8, textvariable=self.create_qty_max_var, width=6).pack(side=tk.LEFT, padx=4)
        self.create_po_btn = ttk.Button(row8, text="Create Random PO", command=self._create_random_po)
        self.create_po_btn.pack(side=tk.LEFT, padx=10)
        ttk.Label(row8, textvariable=self.create_status_var).pack(side=tk.LEFT, padx=8)

        sep = ttk.Separator(tab_main, orient=tk.HORIZONTAL)
        sep.pack(fill=tk.X, padx=10, pady=(0, 6))

        sim = ttk.Frame(tab_main, padding=(10, 0, 10, 6))
        sim.pack(fill=tk.X)
        ttk.Label(sim, text="Sim PID").pack(side=tk.LEFT)
        ttk.Entry(sim, textvariable=self.sim_pid_var, width=28).pack(side=tk.LEFT, padx=4)
        ttk.Label(sim, text="Status").pack(side=tk.LEFT)
        ttk.Combobox(
            sim,
            textvariable=self.sim_status_var,
            values=["COMPLETED", "REJECTED", "CANCELED"],
            width=12,
            state="readonly",
        ).pack(side=tk.LEFT, padx=4)
        ttk.Label(sim, text="Result").pack(side=tk.LEFT)
        ttk.Entry(sim, textvariable=self.sim_result_var, width=12).pack(side=tk.LEFT, padx=4)
        ttk.Label(sim, text="Event").pack(side=tk.LEFT)
        ttk.Entry(sim, textvariable=self.sim_event_var, width=20).pack(side=tk.LEFT, padx=4)
        ttk.Button(sim, text="Simulate Callback", command=self._simulate_callback).pack(side=tk.LEFT, padx=8)

        bottom = ttk.Frame(tab_main, padding=(10, 0, 10, 10))
        bottom.pack(fill=tk.BOTH, expand=True)
        bottom.columnconfigure(0, weight=3)
        bottom.columnconfigure(1, weight=2)
        bottom.rowconfigure(0, weight=1)

        left = ttk.Frame(bottom)
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 6))
        ttk.Label(left, text="Logs").pack(anchor=tk.W)
        self.log_text = tk.Text(left, wrap=tk.WORD, font=("Consolas", 10))
        self.log_text.pack(fill=tk.BOTH, expand=True)

        right = ttk.Frame(bottom)
        right.grid(row=0, column=1, sticky="nsew", padx=(6, 0))
        tabs = ttk.Notebook(right)
        tabs.pack(fill=tk.BOTH, expand=True)

        tab_map = ttk.Frame(tabs)
        tab_erp = ttk.Frame(tabs)
        tabs.add(tab_map, text="Mapping")
        tabs.add(tab_erp, text="ERP New POs")

        ttk.Label(tab_map, text="Mapping Rows (latest)").pack(anchor=tk.W)
        cols = (
            "process_instance_id",
            "po_bill_no",
            "po_fid",
            "callback_status",
            "erp_writeback_ok",
            "updated_at",
        )
        self.tree_map = ttk.Treeview(tab_map, columns=cols, show="headings", height=20)
        for c in cols:
            self.tree_map.heading(c, text=c)
        self.tree_map.column("process_instance_id", width=210, anchor=tk.W)
        self.tree_map.column("po_bill_no", width=120, anchor=tk.W)
        self.tree_map.column("po_fid", width=80, anchor=tk.CENTER)
        self.tree_map.column("callback_status", width=110, anchor=tk.CENTER)
        self.tree_map.column("erp_writeback_ok", width=120, anchor=tk.CENTER)
        self.tree_map.column("updated_at", width=140, anchor=tk.W)
        y1 = ttk.Scrollbar(tab_map, orient=tk.VERTICAL, command=self.tree_map.yview)
        self.tree_map.configure(yscrollcommand=y1.set)
        self.tree_map.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        y1.pack(side=tk.RIGHT, fill=tk.Y)

        ttk.Label(tab_erp, text="Detected ERP New Purchase Orders").pack(anchor=tk.W)
        cols_erp = (
            "detected_at",
            "FID",
            "FBillNo",
            "FDate",
            "FDocumentStatus",
            "FPurchaseOrgId.FNumber",
            "FSupplierId.FNumber",
            "FSupplierId.FName",
            "FMaterialId.FNumber",
            "FQty",
        )
        self.tree_erp = ttk.Treeview(tab_erp, columns=cols_erp, show="headings", height=20)
        for c in cols_erp:
            self.tree_erp.heading(c, text=c)
        self.tree_erp.column("detected_at", width=130, anchor=tk.W)
        self.tree_erp.column("FID", width=75, anchor=tk.CENTER)
        self.tree_erp.column("FBillNo", width=130, anchor=tk.W)
        self.tree_erp.column("FDate", width=95, anchor=tk.W)
        self.tree_erp.column("FDocumentStatus", width=100, anchor=tk.CENTER)
        self.tree_erp.column("FPurchaseOrgId.FNumber", width=90, anchor=tk.CENTER)
        self.tree_erp.column("FSupplierId.FNumber", width=120, anchor=tk.W)
        self.tree_erp.column("FSupplierId.FName", width=150, anchor=tk.W)
        self.tree_erp.column("FMaterialId.FNumber", width=140, anchor=tk.W)
        self.tree_erp.column("FQty", width=70, anchor=tk.E)
        y2 = ttk.Scrollbar(tab_erp, orient=tk.VERTICAL, command=self.tree_erp.yview)
        self.tree_erp.configure(yscrollcommand=y2.set)
        self.tree_erp.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        y2.pack(side=tk.RIGHT, fill=tk.Y)

    def _log(self, text: str) -> None:
        self.log_text.insert(tk.END, text + "\n")
        self.log_text.see(tk.END)

    def _clear_log(self) -> None:
        self.log_text.delete("1.0", tk.END)

    def _load_env(self) -> None:
        env = cb.load_env_file(self.env_file_var.get().strip())
        self.dt_api_base_var.set(cb.first_non_empty(env.get("DINGTALK_API_BASE"), self.dt_api_base_var.get()))
        self.dt_app_key_var.set(cb.first_non_empty(env.get("DINGTALK_APP_KEY"), self.dt_app_key_var.get()))
        self.dt_app_secret_var.set(cb.first_non_empty(env.get("DINGTALK_APP_SECRET"), self.dt_app_secret_var.get()))
        env_originator = cb.first_non_empty(env.get("DINGTALK_ORIGINATOR_USER_ID"))
        if env_originator in DINGTALK_USER_ID_TO_NAME:
            self.quick_originator_name_var.set(DINGTALK_USER_ID_TO_NAME[env_originator])
        env_approver = cb.first_non_empty(env.get("DINGTALK_APPROVER_USER_ID"), env.get("DINGTALK_APPROVER"))
        if env_approver in DINGTALK_USER_ID_TO_NAME:
            self.quick_approver_name_var.set(DINGTALK_USER_ID_TO_NAME[env_approver])
        self.cb_token_var.set(cb.first_non_empty(env.get("DINGTALK_CALLBACK_TOKEN"), self.cb_token_var.get()))
        self.cb_aes_var.set(cb.first_non_empty(env.get("DINGTALK_CALLBACK_AES_KEY"), self.cb_aes_var.get()))
        self.erp_field_status_var.set(cb.first_non_empty(env.get("ERP_DD_FIELD_STATUS"), self.erp_field_status_var.get()))
        self.erp_field_result_var.set(cb.first_non_empty(env.get("ERP_DD_FIELD_RESULT"), self.erp_field_result_var.get()))
        self.erp_field_instance_var.set(cb.first_non_empty(env.get("ERP_DD_FIELD_INSTANCE_ID"), self.erp_field_instance_var.get()))
        self.erp_field_cbtime_var.set(cb.first_non_empty(env.get("ERP_DD_FIELD_CALLBACK_TIME"), self.erp_field_cbtime_var.get()))
        self.erp_field_note_var.set(cb.first_non_empty(env.get("ERP_DD_FIELD_NOTE"), self.erp_field_note_var.get()))
        self.erp_approve_mode_var.set(cb.first_non_empty(env.get("ERP_DD_APPROVE_MODE"), self.erp_approve_mode_var.get()))
        self.erp_approve_comment_var.set(
            cb.first_non_empty(env.get("ERP_DD_APPROVE_COMMENT"), self.erp_approve_comment_var.get())
        )
        self.erp_wf_user_id_var.set(
            cb.first_non_empty(env.get("ERP_DD_WORKFLOW_USER_ID"), self.erp_wf_user_id_var.get())
        )
        self.erp_wf_user_name_var.set(
            cb.first_non_empty(env.get("ERP_DD_WORKFLOW_USER_NAME"), self.erp_wf_user_name_var.get())
        )
        self.erp_wf_post_id_var.set(
            cb.first_non_empty(env.get("ERP_DD_WORKFLOW_POST_ID"), self.erp_wf_post_id_var.get())
        )
        self.erp_wf_post_no_var.set(
            cb.first_non_empty(env.get("ERP_DD_WORKFLOW_POST_NUMBER"), self.erp_wf_post_no_var.get())
        )
        self.erp_wf_approval_type_var.set(
            cb.first_non_empty(env.get("ERP_DD_WORKFLOW_APPROVAL_TYPE"), self.erp_wf_approval_type_var.get())
        )
        self._log(f"[{now_iso()}] loaded env: {self.env_file_var.get().strip()}")

    def _build_writeback_service(self, *, login: bool = True) -> ErpWritebackService:
        cfg = ErpWritebackConfig(
            base_url=self.erp_base_var.get().strip(),
            acct_id=self.erp_acct_var.get().strip(),
            username=self.erp_user_var.get().strip(),
            password=self.erp_pwd_var.get().strip(),
            lcid=int(self.erp_lcid_var.get().strip()),
            timeout=int(self.erp_timeout_var.get().strip()),
            verify_ssl=not self.erp_insecure_var.get(),
            field_status=self.erp_field_status_var.get().strip(),
            field_result=self.erp_field_result_var.get().strip(),
            field_instance_id=self.erp_field_instance_var.get().strip(),
            field_callback_time=self.erp_field_cbtime_var.get().strip(),
            field_note=self.erp_field_note_var.get().strip() or "FNote",
            probe_fields=not self.no_probe_var.get(),
            approve_mode=self.erp_approve_mode_var.get().strip() or "submit_audit",
            approve_comment=self.erp_approve_comment_var.get().strip() or "钉钉审批通过自动审核",
            workflow_user_id=self.erp_wf_user_id_var.get().strip(),
            workflow_user_name=self.erp_wf_user_name_var.get().strip(),
            workflow_post_id=self.erp_wf_post_id_var.get().strip(),
            workflow_post_number=self.erp_wf_post_no_var.get().strip(),
            workflow_approval_type=self.erp_wf_approval_type_var.get().strip() or "1",
        )
        svc = ErpWritebackService(cfg)
        if login:
            svc.login()
        return svc

    def _start_server(self) -> None:
        if self.server_running:
            return
        try:
            host = self.host_var.get().strip()
            port = int(self.port_var.get().strip())
            path = self.path_var.get().strip()
            mapping_db = self.mapping_db_var.get().strip()
            writeback_on = self.writeback_on_var.get().strip()
            shared_token = self.shared_token_var.get().strip()
            cb_token = self.cb_token_var.get().strip()
            cb_aes = self.cb_aes_var.get().strip()

            writeback_service = self._build_writeback_service(login=False)
            crypto = None
            if cb_token and cb_aes:
                crypto = cb.DingTalkCrypto(cb.DingTalkCryptoConfig(token=cb_token, aes_key=cb_aes))

            server_cfg = cb.ServerConfig(
                host=host,
                port=port,
                path=path,
                shared_token=shared_token,
                mapping_db=mapping_db,
                writeback_on=writeback_on,
                defer_writeback=True,
            )
            app = cb.CallbackApp(
                server_config=server_cfg,
                writeback_service=writeback_service,
                dingtalk_crypto=crypto,
                require_dingtalk_signature=self.require_signature_var.get(),
            )

            def log_redirect(payload: dict[str, Any]) -> None:
                self.msg_queue.put(("log", payload))

            cb.print_line = log_redirect  # type: ignore[assignment]
            handler = cb.build_handler(app)
            self.httpd = ThreadingHTTPServer((host, port), handler)
            self.server_thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
            self.server_thread.start()
            self.server_running = True
            self.start_btn.configure(state=tk.DISABLED)
            self.stop_btn.configure(state=tk.NORMAL)
            self.status_var.set(f"Running at http://{host}:{port}{path}")
            self._log(f"[{now_iso()}] callback server started")
            self._log(
                f"[{now_iso()}] writeback mode={writeback_on}, "
                f"erp approve mode={self.erp_approve_mode_var.get().strip() or 'submit_audit'}"
            )
            self._log(f"[{now_iso()}] callback defer writeback: enabled")
            self._log(
                f"[{now_iso()}] workflow params: userId={self.erp_wf_user_id_var.get().strip() or '-'}, "
                f"userName={self.erp_wf_user_name_var.get().strip() or '-'}, "
                f"postId={self.erp_wf_post_id_var.get().strip() or '-'}, "
                f"postNo={self.erp_wf_post_no_var.get().strip() or '-'}, "
                f"approvalType={self.erp_wf_approval_type_var.get().strip() or '1'}"
            )
            if self.dt_poll_auto_writeback_var.get():
                self._start_wb_retry()
            else:
                self.wb_retry_status_var.set("Writeback Retry: disabled")
            self._refresh_mapping()
            self._log(f"[{now_iso()}] tip: click 'Start ERP Watch' to detect new ERP purchase orders")
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Start failed", str(exc))
            self._log(f"[{now_iso()}] start failed: {exc}")

    def _stop_server(self) -> None:
        if self.httpd is None:
            return
        try:
            self.httpd.shutdown()
            self.httpd.server_close()
        except Exception:  # noqa: BLE001
            pass
        self.httpd = None
        self.server_thread = None
        self.server_running = False
        self.start_btn.configure(state=tk.NORMAL)
        self.stop_btn.configure(state=tk.DISABLED)
        self.status_var.set("Stopped")
        self._stop_wb_retry()
        self._log(f"[{now_iso()}] callback server stopped")

    def _simulate_callback(self) -> None:
        try:
            host = self.host_var.get().strip()
            port = int(self.port_var.get().strip())
            path = self.path_var.get().strip()
            shared_token = self.shared_token_var.get().strip()
            mapping_db = self.mapping_db_var.get().strip()
            process_id = self.sim_pid_var.get().strip() or read_latest_process_instance_id(mapping_db)
            if not process_id:
                raise RuntimeError("No processInstanceId. Create one first or input Sim PID.")
            payload = {
                "EventType": self.sim_event_var.get().strip() or "bpms_instance_change",
                "processInstanceId": process_id,
                "status": self.sim_status_var.get().strip(),
                "result": self.sim_result_var.get().strip(),
            }
            req_headers = {"Content-Type": "application/json"}
            if shared_token:
                req_headers["X-Callback-Token"] = shared_token
            full_path = path if path.startswith("/") else f"/{path}"
            req_host = host
            if req_host in ("", "0.0.0.0", "::", "[::]"):
                req_host = "127.0.0.1"
            url = f"http://{req_host}:{port}{full_path}"
            resp = requests.post(url, data=json.dumps(payload, ensure_ascii=False), headers=req_headers, timeout=20)
            self._log(
                f"[{now_iso()}] simulate -> {url} [{resp.status_code}] "
                f"{resp.text[:600]}"
            )
            self._refresh_mapping()
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Simulate failed", str(exc))
            self._log(f"[{now_iso()}] simulate failed: {exc}")

    def _refresh_mapping(self) -> None:
        try:
            rows = list_recent_links(self.mapping_db_var.get().strip(), limit=300)
        except Exception as exc:  # noqa: BLE001
            self._log(f"[{now_iso()}] refresh mapping failed: {exc}")
            return
        self.tree_map.delete(*self.tree_map.get_children())
        for row in rows:
            self.tree_map.insert(
                "",
                tk.END,
                values=(
                    row.get("process_instance_id", ""),
                    row.get("po_bill_no", ""),
                    row.get("po_fid", ""),
                    row.get("callback_status", ""),
                    row.get("erp_writeback_ok", ""),
                    row.get("updated_at", ""),
                ),
            )
        self._log(f"[{now_iso()}] mapping refreshed: {len(rows)} row(s)")

    def _unique_rows_by_fid(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen: set[str] = set()
        out: list[dict[str, Any]] = []
        for row in rows:
            fid = str(row.get("FID", "")).strip()
            if not fid or fid in seen:
                continue
            seen.add(fid)
            out.append(row)
        return out

    def _fetch_recent_po_rows(
        self,
        session: requests.Session,
        *,
        base_url: str,
        days: int,
        limit: int,
        timeout: int,
        verify_ssl: bool,
        org_no: str,
    ) -> list[dict[str, Any]]:
        today = dt.date.today()
        start_date = today - dt.timedelta(days=days)
        next_day = today + dt.timedelta(days=1)
        start_dt = f"{start_date.isoformat()} 00:00:00"
        next_dt = f"{next_day.isoformat()} 00:00:00"
        parts = [
            "("
            f"(FCreateDate >= '{start_dt}' and FCreateDate < '{next_dt}')"
            f" or (FDate >= '{start_date.isoformat()}' and FDate < '{next_day.isoformat()}')"
            ")"
        ]
        org_no = org_no.strip()
        if org_no:
            parts.insert(0, f"FPurchaseOrgId.FNumber = '{org_no}'")
        query_obj = {
            "FormId": "PUR_PurchaseOrder",
            "FieldKeys": ERP_WATCH_FIELD_KEYS,
            "FilterString": " and ".join(parts),
            "OrderString": "FID DESC",
            "StartRow": 0,
            "Limit": limit,
        }
        raw = k3.execute_bill_query(
            session=session,
            base_url=base_url,
            query_obj=query_obj,
            timeout=timeout,
            verify_ssl=verify_ssl,
        )
        rows = k3.rows_to_dicts(raw, ERP_WATCH_FIELD_KEYS)
        return self._unique_rows_by_fid(rows)

    def _start_erp_watch(self) -> None:
        if self.erp_watch_running:
            return
        try:
            params = {
                "base_url": self.erp_base_var.get().strip(),
                "acct_id": self.erp_acct_var.get().strip(),
                "username": self.erp_user_var.get().strip(),
                "password": self.erp_pwd_var.get().strip(),
                "lcid": int(self.erp_lcid_var.get().strip()),
                "timeout": int(self.erp_timeout_var.get().strip()),
                "verify_ssl": not self.erp_insecure_var.get(),
                "org_no": self.erp_org_no_var.get().strip(),
                "days": int(self.erp_watch_days_var.get().strip()),
                "limit": int(self.erp_watch_limit_var.get().strip()),
                "interval": float(self.erp_watch_interval_var.get().strip()),
                "from_now": self.erp_watch_from_now_var.get(),
            }
            if params["days"] <= 0 or params["limit"] <= 0 or params["interval"] <= 0:
                raise ValueError("ERP watch days/limit/interval must be > 0")
            self.erp_seen_fids.clear()
            self.erp_detected_rows.clear()
            self._refresh_erp_tree()
            self.erp_watch_stop_event.clear()
            self.erp_watch_thread = threading.Thread(
                target=self._erp_watch_worker,
                args=(params,),
                daemon=True,
            )
            self.erp_watch_thread.start()
            self.erp_watch_running = True
            self.erp_watch_start_btn.configure(state=tk.DISABLED)
            self.erp_watch_stop_btn.configure(state=tk.NORMAL)
            self._log(f"[{now_iso()}] ERP watch started")
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("ERP Watch start failed", str(exc))
            self._log(f"[{now_iso()}] ERP watch start failed: {exc}")

    def _stop_erp_watch(self) -> None:
        self.erp_watch_stop_event.set()
        self.erp_watch_running = False
        self.erp_watch_start_btn.configure(state=tk.NORMAL)
        self.erp_watch_stop_btn.configure(state=tk.DISABLED)
        self._log(f"[{now_iso()}] ERP watch stopping...")

    def _start_po_monitor(self) -> None:
        if self.po_monitor_proc is not None and self.po_monitor_proc.poll() is None:
            return
        try:
            days = int(self.po_monitor_days_var.get().strip())
            limit = int(self.po_monitor_limit_var.get().strip())
            interval = float(self.po_monitor_interval_var.get().strip())
            if days <= 0 or limit <= 0 or interval <= 0:
                raise ValueError("PO monitor days/limit/interval must be > 0")

            cmd: list[str] = [
                sys.executable,
                "-u",
                "purchase_order_new_monitor.py",
                "--base-url",
                self.erp_base_var.get().strip(),
                "--acct-id",
                self.erp_acct_var.get().strip(),
                "--username",
                self.erp_user_var.get().strip(),
                "--password",
                self.erp_pwd_var.get().strip(),
                "--lcid",
                self.erp_lcid_var.get().strip(),
                "--org-no",
                self.erp_org_no_var.get().strip(),
                "--days",
                str(days),
                "--interval",
                str(interval),
                "--scan-limit",
                str(limit),
                "--timeout",
                self.erp_timeout_var.get().strip(),
                "--dingtalk-enable",
                "--dingtalk-env-file",
                self.env_file_var.get().strip(),
                "--dingtalk-originator-user-id",
                self._selected_originator_id(),
                "--mapping-db",
                self.mapping_db_var.get().strip(),
                "--dingtalk-approver",
                self._selected_approver_id(),
            ]
            if self.erp_insecure_var.get():
                cmd.append("--insecure")
            if self.po_monitor_from_now_var.get():
                cmd.append("--from-now")

            self.po_monitor_proc = subprocess.Popen(
                cmd,
                cwd=str(Path(__file__).resolve().parent),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="ignore",
                bufsize=1,
            )
            self.po_monitor_reader_thread = threading.Thread(target=self._po_monitor_reader, daemon=True)
            self.po_monitor_reader_thread.start()
            self.po_start_btn.configure(state=tk.DISABLED)
            self.po_stop_btn.configure(state=tk.NORMAL)
            self.po_monitor_status_var.set("PO->DingTalk: running")
            self._log(f"[{now_iso()}] PO monitor started")
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Start PO monitor failed", str(exc))
            self._log(f"[{now_iso()}] PO monitor start failed: {exc}")

    def _stop_po_monitor(self) -> None:
        proc = self.po_monitor_proc
        if proc is None:
            self.po_monitor_status_var.set("PO->DingTalk: stopped")
            self.po_start_btn.configure(state=tk.NORMAL)
            self.po_stop_btn.configure(state=tk.DISABLED)
            return
        try:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=8)
                except Exception:  # noqa: BLE001
                    proc.kill()
        except Exception:  # noqa: BLE001
            pass
        self.po_monitor_proc = None
        self.po_monitor_reader_thread = None
        self.po_start_btn.configure(state=tk.NORMAL)
        self.po_stop_btn.configure(state=tk.DISABLED)
        self.po_monitor_status_var.set("PO->DingTalk: stopped")
        self._log(f"[{now_iso()}] PO monitor stopped")

    def _po_monitor_reader(self) -> None:
        proc = self.po_monitor_proc
        if proc is None or proc.stdout is None:
            return
        try:
            for line in proc.stdout:
                text = line.strip()
                if not text:
                    continue
                self.msg_queue.put(("po_log", text))
        except Exception as exc:  # noqa: BLE001
            self.msg_queue.put(("text", f"[{now_iso()}] PO monitor reader error: {exc}"))
        finally:
            rc = proc.poll()
            self.msg_queue.put(("po_stopped", rc))

    def _start_dt_poll(self) -> None:
        if self.dt_poll_running:
            return
        try:
            params = {
                "api_base": self.dt_api_base_var.get().strip(),
                "app_key": self.dt_app_key_var.get().strip(),
                "app_secret": self.dt_app_secret_var.get().strip(),
                "mapping_db": self.mapping_db_var.get().strip(),
                "days": int(self.dt_poll_days_var.get().strip()),
                "limit": int(self.dt_poll_limit_var.get().strip()),
                "interval": float(self.dt_poll_interval_var.get().strip()),
                "timeout": int(self.erp_timeout_var.get().strip()),
                "writeback_on": self.writeback_on_var.get().strip() or "approved",
                "auto_writeback": self.dt_poll_auto_writeback_var.get(),
            }
            if not params["api_base"]:
                raise ValueError("DingTalk API base is required.")
            if not params["app_key"] or not params["app_secret"]:
                raise ValueError("DingTalk AppKey/AppSecret are required.")
            if not params["mapping_db"]:
                raise ValueError("Mapping DB path is required.")
            if params["days"] < 0 or params["limit"] <= 0 or params["interval"] <= 0:
                raise ValueError("DingTalk poll days>=0, limit>0, interval>0.")

            self.dt_poll_stop_event.clear()
            self.dt_poll_last_sig.clear()
            self.dt_poll_thread = threading.Thread(target=self._dingtalk_poll_worker, args=(params,), daemon=True)
            self.dt_poll_thread.start()
            self.dt_poll_running = True
            self.dt_poll_start_btn.configure(state=tk.DISABLED)
            self.dt_poll_stop_btn.configure(state=tk.NORMAL)
            self.dt_poll_status_var.set("DingTalk Poll: running")
            if params["auto_writeback"]:
                self._start_wb_retry()
            self._log(
                f"[{now_iso()}] DingTalk poll started "
                f"(days={params['days']}, limit={params['limit']}, interval={params['interval']}s)"
            )
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Start DingTalk poll failed", str(exc))
            self._log(f"[{now_iso()}] DingTalk poll start failed: {exc}")

    def _stop_dt_poll(self) -> None:
        self.dt_poll_stop_event.set()
        self.dt_poll_running = False
        self.dt_poll_start_btn.configure(state=tk.NORMAL)
        self.dt_poll_stop_btn.configure(state=tk.DISABLED)
        self.dt_poll_status_var.set("DingTalk Poll: stopping")
        self._log(f"[{now_iso()}] DingTalk poll stopping...")

    def _dingtalk_poll_worker(self, params: dict[str, Any]) -> None:
        session = requests.Session()
        try:
            token = dingtalk_get_token(
                session=session,
                api_base=params["api_base"],
                app_key=params["app_key"],
                app_secret=params["app_secret"],
                timeout=params["timeout"],
            )
            self.msg_queue.put(("text", f"[{now_iso()}] DingTalk poll token acquired"))

            if params["auto_writeback"]:
                self.msg_queue.put(
                    (
                        "text",
                        f"[{now_iso()}] DingTalk poll writeback delegated "
                        f"to retry worker (mode={params['writeback_on']})",
                    )
                )
            else:
                self.msg_queue.put(("text", f"[{now_iso()}] DingTalk poll writeback disabled"))

            while not self.dt_poll_stop_event.is_set():
                rows = list_poll_targets(
                    params["mapping_db"],
                    days=params["days"],
                    limit=params["limit"],
                )
                for row in rows:
                    if self.dt_poll_stop_event.is_set():
                        break
                    process_instance_id = str(row.get("process_instance_id") or "").strip()
                    po_fid = str(row.get("po_fid") or "").strip()
                    po_bill_no = str(row.get("po_bill_no") or "").strip()
                    if not process_instance_id or not po_fid:
                        continue

                    try:
                        data = dingtalk_get_instance_detail(
                            session=session,
                            api_base=params["api_base"],
                            token=token,
                            process_instance_id=process_instance_id,
                            timeout=params["timeout"],
                        )
                    except Exception as exc:  # noqa: BLE001
                        first_err = str(exc)
                        lower = first_err.lower()
                        if "401" in lower or "accesstoken" in lower or "invalid token" in lower:
                            try:
                                token = dingtalk_get_token(
                                    session=session,
                                    api_base=params["api_base"],
                                    app_key=params["app_key"],
                                    app_secret=params["app_secret"],
                                    timeout=params["timeout"],
                                )
                                data = dingtalk_get_instance_detail(
                                    session=session,
                                    api_base=params["api_base"],
                                    token=token,
                                    process_instance_id=process_instance_id,
                                    timeout=params["timeout"],
                                )
                            except Exception as exc2:  # noqa: BLE001
                                self.msg_queue.put(
                                    (
                                        "text",
                                        f"[{now_iso()}] DingTalk query failed pid={process_instance_id}: {exc2}",
                                    )
                                )
                                continue
                        else:
                            self.msg_queue.put(
                                (
                                    "text",
                                    f"[{now_iso()}] DingTalk query failed pid={process_instance_id}: {exc}",
                                )
                            )
                            continue

                    result_obj = data.get("result")
                    if not isinstance(result_obj, dict):
                        continue
                    raw_status = str(result_obj.get("status") or "").strip()
                    op_records_raw = result_obj.get("operationRecords") or []
                    op_records = op_records_raw if isinstance(op_records_raw, list) else []
                    latest_op = op_records[-1] if op_records and isinstance(op_records[-1], dict) else {}
                    raw_result = str(latest_op.get("result") or result_obj.get("result") or "").strip()

                    mapped_status, mapped_reason = cb.classify_status(
                        {
                            "status": raw_status,
                            "result": raw_result,
                            "operationRecords": op_records,
                        }
                    )
                    mapped_status = mapped_status.upper()
                    raw_status_upper = raw_status.upper()
                    if mapped_status == "UNKNOWN":
                        if raw_status_upper in {"RUNNING", "PROCESSING", "STARTED"}:
                            mapped_status = "RUNNING"
                        elif raw_status_upper in {"CANCELED", "CANCELLED", "TERMINATED"}:
                            mapped_status = "CANCELED"
                        elif raw_status_upper == "COMPLETED":
                            mapped_status = "APPROVED"

                    snapshot = {
                        "mapped_status": mapped_status,
                        "raw_status": raw_status,
                        "raw_result": raw_result,
                        "latest_op_result": str(latest_op.get("result") or ""),
                        "op_count": len(op_records),
                    }
                    sig = json.dumps(snapshot, ensure_ascii=False, sort_keys=True)
                    if sig != self.dt_poll_last_sig.get(process_instance_id, ""):
                        self.dt_poll_last_sig[process_instance_id] = sig
                        self.msg_queue.put(
                            (
                                "dt_status_changed",
                                {
                                    "processInstanceId": process_instance_id,
                                    "poFid": po_fid,
                                    "poBillNo": po_bill_no,
                                    "rawStatus": raw_status,
                                    "rawResult": raw_result,
                                    "mappedStatus": mapped_status,
                                    "reason": mapped_reason,
                                },
                            )
                        )

                    prev_status = str(row.get("callback_status") or "").strip().upper()
                    prev_result = str(row.get("callback_result") or "").strip()
                    callback_time = now_iso()

                    if mapped_status not in {"", "UNKNOWN", "RUNNING"}:
                        if mapped_status != prev_status or raw_result != prev_result:
                            update_callback_info(
                                params["mapping_db"],
                                process_instance_id=process_instance_id,
                                callback_status=mapped_status,
                                callback_result=raw_result,
                                callback_time=callback_time,
                                raw_payload=data,
                            )
                            self.msg_queue.put(
                                (
                                    "dt_mapping_updated",
                                    {
                                        "processInstanceId": process_instance_id,
                                        "poFid": po_fid,
                                        "poBillNo": po_bill_no,
                                        "status": mapped_status,
                                        "result": raw_result,
                                    },
                                )
                            )

                if self.dt_poll_stop_event.wait(params["interval"]):
                    break
        except Exception as exc:  # noqa: BLE001
            self.msg_queue.put(("text", f"[{now_iso()}] DingTalk poll error: {exc}"))
        finally:
            self.msg_queue.put(("dt_poll_stopped", None))

    def _validate_create_params(self) -> dict[str, Any]:
        qty_min = int(self.create_qty_min_var.get().strip())
        qty_max = int(self.create_qty_max_var.get().strip())
        if qty_min <= 0 or qty_max < qty_min:
            raise ValueError("Create qty range invalid.")
        materials = [m.strip() for m in self.create_materials_var.get().split(",") if m.strip()]
        if not materials:
            raise ValueError("Create materials cannot be empty.")
        return {
            "base_url": self.erp_base_var.get().strip(),
            "acct_id": self.erp_acct_var.get().strip(),
            "username": self.erp_user_var.get().strip(),
            "password": self.erp_pwd_var.get().strip(),
            "lcid": int(self.erp_lcid_var.get().strip()),
            "timeout": int(self.erp_timeout_var.get().strip()),
            "verify_ssl": not self.erp_insecure_var.get(),
            "org_no": self.erp_org_no_var.get().strip(),
            "supplier_no": self.create_supplier_no_var.get().strip(),
            "materials": materials,
            "qty_min": qty_min,
            "qty_max": qty_max,
        }

    def _create_random_po(self) -> None:
        try:
            params = self._validate_create_params()
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Create PO failed", str(exc))
            return
        self.create_po_btn.configure(state=tk.DISABLED)
        self.create_status_var.set("Create: running")
        threading.Thread(target=self._create_random_po_worker, args=(params,), daemon=True).start()

    def _create_random_po_worker(self, params: dict[str, Any]) -> None:
        try:
            session = requests.Session()
            k3.login(
                session=session,
                base_url=params["base_url"],
                acct_id=params["acct_id"],
                username=params["username"],
                password=params["password"],
                lcid=params["lcid"],
                timeout=params["timeout"],
                verify_ssl=params["verify_ssl"],
            )

            material = random.choice(params["materials"])
            qty = random.randint(params["qty_min"], params["qty_max"])
            model = {
                "FDate": dt.date.today().isoformat(),
                "FPurchaseOrgId": {"FNumber": params["org_no"]},
                "FSupplierId": {"FNumber": params["supplier_no"]},
                "FPOOrderEntry": [{"FMaterialId": {"FNumber": material}, "FQty": qty}],
            }
            ok, payload = save_purchase_order(
                session=session,
                base_url=params["base_url"],
                model=model,
                timeout=params["timeout"],
                verify_ssl=params["verify_ssl"],
            )
            if not ok:
                raise RuntimeError(json.dumps(payload, ensure_ascii=False))

            result = payload.get("Result", {})
            out = {
                "id": result.get("Id"),
                "billNo": result.get("Number"),
                "orgNo": params["org_no"],
                "supplierNo": params["supplier_no"],
                "materialNo": material,
                "qty": qty,
            }
            self.msg_queue.put(("po_created", out))
        except Exception as exc:  # noqa: BLE001
            self.msg_queue.put(("po_create_failed", str(exc)))

    def _start_wb_retry(self) -> None:
        if self.wb_retry_running:
            return
        if not self.dt_poll_auto_writeback_var.get():
            self.wb_retry_status_var.set("Writeback Retry: disabled")
            return
        try:
            mapping_db = self.mapping_db_var.get().strip()
            if not mapping_db:
                raise ValueError("Mapping DB path is required.")

            interval_raw = float(self.wb_retry_interval_var.get().strip())
            interval_sec = max(10.0, min(30.0, interval_raw))
            if abs(interval_sec - interval_raw) > 1e-9:
                self._log(
                    f"[{now_iso()}] writeback retry interval clamped: {interval_raw}s -> {interval_sec}s "
                    "(required 10-30s)"
                )
                shown = str(int(interval_sec)) if float(interval_sec).is_integer() else str(interval_sec)
                self.wb_retry_interval_var.set(shown)

            max_minutes = float(self.wb_retry_max_minutes_var.get().strip())
            if max_minutes <= 0:
                raise ValueError("Writeback max minutes must be > 0.")

            scan_limit = int(self.dt_poll_limit_var.get().strip())
            if scan_limit <= 0:
                scan_limit = 200

            params = {
                "mapping_db": mapping_db,
                "interval": interval_sec,
                "max_minutes": max_minutes,
                "max_seconds": max_minutes * 60.0,
                "scan_limit": max(50, min(1000, scan_limit)),
                "writeback_on": self.writeback_on_var.get().strip() or "approved",
            }
            self.wb_retry_stop_event.clear()
            self.wb_retry_thread = threading.Thread(target=self._writeback_retry_worker, args=(params,), daemon=True)
            self.wb_retry_thread.start()
            self.wb_retry_running = True
            self.wb_retry_status_var.set(
                f"Writeback Retry: running ({int(params['interval'])}s/{int(params['max_minutes'])}m)"
            )
            self._log(
                f"[{now_iso()}] writeback retry started "
                f"(interval={params['interval']}s, max={params['max_minutes']}m, mode={params['writeback_on']})"
            )
        except Exception as exc:  # noqa: BLE001
            self.wb_retry_running = False
            self.wb_retry_thread = None
            self.wb_retry_status_var.set("Writeback Retry: failed")
            self._log(f"[{now_iso()}] writeback retry start failed: {exc}")

    def _stop_wb_retry(self) -> None:
        if not self.wb_retry_running:
            self.wb_retry_status_var.set("Writeback Retry: stopped")
            return
        self.wb_retry_stop_event.set()
        self.wb_retry_running = False
        self.wb_retry_status_var.set("Writeback Retry: stopping")
        self._log(f"[{now_iso()}] writeback retry stopping...")

    def _writeback_retry_worker(self, params: dict[str, Any]) -> None:
        writeback_service: ErpWritebackService | None = None
        retry_after: dict[str, float] = {}
        try:
            self.msg_queue.put(("wb_retry_started", params))
            while not self.wb_retry_stop_event.is_set():
                if writeback_service is None:
                    try:
                        writeback_service = self._build_writeback_service()
                    except Exception as exc:  # noqa: BLE001
                        self.msg_queue.put(("text", f"[{now_iso()}] writeback retry login failed: {exc}"))
                        if self.wb_retry_stop_event.wait(params["interval"]):
                            break
                        continue

                try:
                    rows = list_pending_writeback_links(params["mapping_db"], limit=params["scan_limit"])
                except Exception as exc:  # noqa: BLE001
                    self.msg_queue.put(("text", f"[{now_iso()}] load pending writeback rows failed: {exc}"))
                    if self.wb_retry_stop_event.wait(params["interval"]):
                        break
                    continue

                now_dt = dt.datetime.now()
                now_ts = time.time()
                for row in rows:
                    if self.wb_retry_stop_event.is_set():
                        break

                    process_instance_id = str(row.get("process_instance_id") or "").strip()
                    po_fid = str(row.get("po_fid") or "").strip()
                    po_bill_no = str(row.get("po_bill_no") or "").strip()
                    callback_status = str(row.get("callback_status") or "").strip().upper()
                    callback_result = str(row.get("callback_result") or "").strip()
                    callback_time = str(row.get("callback_time") or "").strip() or now_iso()

                    if not process_instance_id or not po_fid or callback_status in {"", "UNKNOWN"}:
                        continue

                    if not cb.should_writeback(callback_status, params["writeback_on"]):
                        if callback_status in TERMINAL_DINGTALK_STATUSES:
                            skip_msg = (
                                f"skip writeback for status={callback_status} "
                                f"with mode={params['writeback_on']}"
                            )
                            update_writeback_result(
                                params["mapping_db"],
                                process_instance_id=process_instance_id,
                                ok=True,
                                message=skip_msg,
                            )
                            self.msg_queue.put(
                                (
                                    "wb_writeback",
                                    {
                                        "processInstanceId": process_instance_id,
                                        "poFid": po_fid,
                                        "poBillNo": po_bill_no,
                                        "status": callback_status,
                                        "ok": True,
                                        "message": skip_msg,
                                        "skip": True,
                                    },
                                )
                            )
                        continue

                    prev_ok_val = row.get("erp_writeback_ok")
                    prev_ok_text = str(prev_ok_val).strip().lower() if prev_ok_val is not None else ""
                    if prev_ok_text in {"1", "true"}:
                        continue

                    prev_msg = str(row.get("erp_writeback_msg") or "")
                    if is_non_retryable_writeback_message(prev_msg):
                        continue
                    if is_retry_timeout_writeback_message(prev_msg):
                        continue

                    callback_dt = parse_iso_datetime(callback_time) or parse_iso_datetime(
                        str(row.get("updated_at") or "")
                    )
                    if callback_dt is not None:
                        elapsed_sec = (now_dt - callback_dt).total_seconds()
                        if elapsed_sec > float(params["max_seconds"]):
                            timeout_msg = (
                                f"{WRITEBACK_TIMEOUT_MARKER} exceeded {int(params['max_minutes'])} minutes "
                                f"for status={callback_status}"
                            )
                            update_writeback_result(
                                params["mapping_db"],
                                process_instance_id=process_instance_id,
                                ok=False,
                                message=timeout_msg,
                            )
                            self.msg_queue.put(
                                (
                                    "wb_writeback",
                                    {
                                        "processInstanceId": process_instance_id,
                                        "poFid": po_fid,
                                        "poBillNo": po_bill_no,
                                        "status": callback_status,
                                        "ok": False,
                                        "message": timeout_msg,
                                        "timeout": True,
                                    },
                                )
                            )
                            retry_after.pop(f"{process_instance_id}:{callback_status}", None)
                            continue

                    retry_key = f"{process_instance_id}:{callback_status}"
                    if now_ts < retry_after.get(retry_key, 0.0):
                        continue

                    try:
                        ok, msg = writeback_service.writeback(
                            po_fid=po_fid,
                            po_bill_no=po_bill_no,
                            process_instance_id=process_instance_id,
                            callback_status=callback_status,
                            callback_result=callback_result,
                            callback_time=callback_time,
                        )
                    except Exception as exc:  # noqa: BLE001
                        ok = False
                        msg = str(exc)
                        if any(mark in msg.lower() for mark in ("login", "session", "context", "403", "401")):
                            writeback_service = None

                    update_writeback_result(
                        params["mapping_db"],
                        process_instance_id=process_instance_id,
                        ok=ok,
                        message=msg,
                    )

                    retryable_now = is_retryable_writeback_message(msg)
                    non_retryable_now = is_non_retryable_writeback_message(msg)
                    if ok or non_retryable_now:
                        retry_after.pop(retry_key, None)
                    else:
                        retry_after[retry_key] = time.time() + float(params["interval"])

                    self.msg_queue.put(
                        (
                            "wb_writeback",
                            {
                                "processInstanceId": process_instance_id,
                                "poFid": po_fid,
                                "poBillNo": po_bill_no,
                                "status": callback_status,
                                "ok": ok,
                                "message": msg,
                                "retryable": retryable_now,
                                "nonRetryable": non_retryable_now,
                            },
                        )
                    )

                if self.wb_retry_stop_event.wait(params["interval"]):
                    break
        except Exception as exc:  # noqa: BLE001
            self.msg_queue.put(("text", f"[{now_iso()}] writeback retry worker error: {exc}"))
        finally:
            self.msg_queue.put(("wb_retry_stopped", None))

    def _start_all(self) -> None:
        if not self.server_running:
            self._start_server()
        if not self.erp_watch_running:
            self._start_erp_watch()
        if self.po_monitor_proc is None or self.po_monitor_proc.poll() is not None:
            self._start_po_monitor()
        if not self.dt_poll_running:
            self._start_dt_poll()
        if self.dt_poll_auto_writeback_var.get():
            self._start_wb_retry()
        self._log(f"[{now_iso()}] full flow started")

    def _stop_all(self) -> None:
        self._stop_wb_retry()
        self._stop_dt_poll()
        self._stop_po_monitor()
        self._stop_erp_watch()
        self._stop_server()
        self._log(f"[{now_iso()}] full flow stopped")

    def _erp_watch_worker(self, params: dict[str, Any]) -> None:
        session = requests.Session()
        try:
            k3.login(
                session=session,
                base_url=params["base_url"],
                acct_id=params["acct_id"],
                username=params["username"],
                password=params["password"],
                lcid=params["lcid"],
                timeout=params["timeout"],
                verify_ssl=params["verify_ssl"],
            )
            baseline_rows = self._fetch_recent_po_rows(
                session,
                base_url=params["base_url"],
                days=params["days"],
                limit=params["limit"],
                timeout=params["timeout"],
                verify_ssl=params["verify_ssl"],
                org_no=params["org_no"],
            )
            baseline_fids = {str(r.get("FID", "")).strip() for r in baseline_rows if str(r.get("FID", "")).strip()}
            if params["from_now"]:
                self.erp_seen_fids.update(baseline_fids)
                self.msg_queue.put(("text", f"[{now_iso()}] ERP watch baseline loaded: {len(baseline_fids)}"))
            else:
                for row in baseline_rows:
                    fid = str(row.get("FID", "")).strip()
                    if not fid:
                        continue
                    self.erp_seen_fids.add(fid)
                    item = dict(row)
                    item["detected_at"] = now_iso()
                    self.msg_queue.put(("erp_new_order", item))

            while not self.erp_watch_stop_event.is_set():
                if self.erp_watch_stop_event.wait(params["interval"]):
                    break
                try:
                    rows = self._fetch_recent_po_rows(
                        session,
                        base_url=params["base_url"],
                        days=params["days"],
                        limit=params["limit"],
                        timeout=params["timeout"],
                        verify_ssl=params["verify_ssl"],
                        org_no=params["org_no"],
                    )
                except Exception:
                    k3.login(
                        session=session,
                        base_url=params["base_url"],
                        acct_id=params["acct_id"],
                        username=params["username"],
                        password=params["password"],
                        lcid=params["lcid"],
                        timeout=params["timeout"],
                        verify_ssl=params["verify_ssl"],
                    )
                    rows = self._fetch_recent_po_rows(
                        session,
                        base_url=params["base_url"],
                        days=params["days"],
                        limit=params["limit"],
                        timeout=params["timeout"],
                        verify_ssl=params["verify_ssl"],
                        org_no=params["org_no"],
                    )

                new_rows: list[dict[str, Any]] = []
                for row in rows:
                    fid = str(row.get("FID", "")).strip()
                    if not fid or fid in self.erp_seen_fids:
                        continue
                    self.erp_seen_fids.add(fid)
                    new_rows.append(row)

                def sort_key(item: dict[str, Any]) -> tuple[int, int | str]:
                    fid = str(item.get("FID", "")).strip()
                    try:
                        return (0, int(float(fid)))
                    except Exception:  # noqa: BLE001
                        return (1, fid)

                new_rows.sort(key=sort_key)
                for row in new_rows:
                    item = dict(row)
                    item["detected_at"] = now_iso()
                    self.msg_queue.put(("erp_new_order", item))
        except Exception as exc:  # noqa: BLE001
            self.msg_queue.put(("text", f"[{now_iso()}] ERP watch error: {exc}"))
        finally:
            self.msg_queue.put(("erp_watch_stopped", None))

    def _refresh_erp_tree(self) -> None:
        self.tree_erp.delete(*self.tree_erp.get_children())
        for row in self.erp_detected_rows:
            self.tree_erp.insert(
                "",
                tk.END,
                values=(
                    row.get("detected_at", ""),
                    row.get("FID", ""),
                    row.get("FBillNo", ""),
                    str(row.get("FDate", "")).split("T", 1)[0],
                    row.get("FDocumentStatus", ""),
                    row.get("FPurchaseOrgId.FNumber", ""),
                    row.get("FSupplierId.FNumber", ""),
                    row.get("FSupplierId.FName", ""),
                    row.get("FMaterialId.FNumber", ""),
                    row.get("FQty", ""),
                ),
            )

    def _refresh_erp_list_now(self) -> None:
        try:
            base_url = self.erp_base_var.get().strip()
            acct_id = self.erp_acct_var.get().strip()
            username = self.erp_user_var.get().strip()
            password = self.erp_pwd_var.get().strip()
            lcid = int(self.erp_lcid_var.get().strip())
            timeout = int(self.erp_timeout_var.get().strip())
            verify_ssl = not self.erp_insecure_var.get()
            org_no = self.erp_org_no_var.get().strip()
            days = int(self.erp_watch_days_var.get().strip())
            limit = int(self.erp_watch_limit_var.get().strip())
            if days <= 0 or limit <= 0:
                raise ValueError("ERP Days and Limit must be > 0")

            s = requests.Session()
            k3.login(
                session=s,
                base_url=base_url,
                acct_id=acct_id,
                username=username,
                password=password,
                lcid=lcid,
                timeout=timeout,
                verify_ssl=verify_ssl,
            )
            rows = self._fetch_recent_po_rows(
                s,
                base_url=base_url,
                days=days,
                limit=limit,
                timeout=timeout,
                verify_ssl=verify_ssl,
                org_no=org_no,
            )
            snapshot_time = now_iso()
            self.erp_detected_rows = []
            for row in rows:
                item = dict(row)
                item["detected_at"] = snapshot_time
                self.erp_detected_rows.append(item)
            self._refresh_erp_tree()
            self._log(f"[{snapshot_time}] ERP snapshot refreshed: {len(rows)} row(s)")
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Refresh ERP List failed", str(exc))
            self._log(f"[{now_iso()}] ERP snapshot failed: {exc}")

    def _add_erp_detected_row(self, row: dict[str, Any]) -> None:
        self.erp_detected_rows.insert(0, row)
        if len(self.erp_detected_rows) > 500:
            self.erp_detected_rows = self.erp_detected_rows[:500]
        self._refresh_erp_tree()

    def _poll_queue(self) -> None:
        while True:
            try:
                kind, payload = self.msg_queue.get_nowait()
            except queue.Empty:
                break
            if kind == "log":
                self._log(json.dumps(payload, ensure_ascii=False))
                if isinstance(payload, dict) and payload.get("event") in (
                    "writeback_done",
                    "writeback_skipped",
                    "callback_pid_not_found",
                ):
                    self._refresh_mapping()
            elif kind == "text":
                self._log(str(payload))
            elif kind == "erp_new_order":
                self._add_erp_detected_row(dict(payload))
                self._log(
                    f"[{now_iso()}] ERP new PO detected: "
                    f"FID={payload.get('FID','')} BillNo={payload.get('FBillNo','')}"
                )
            elif kind == "erp_watch_stopped":
                self.erp_watch_running = False
                self.erp_watch_start_btn.configure(state=tk.NORMAL)
                self.erp_watch_stop_btn.configure(state=tk.DISABLED)
                self._log(f"[{now_iso()}] ERP watch stopped")
            elif kind == "po_log":
                text = str(payload)
                self._log(f"[PO] {text}")
                if "dingtalk_created" in text or "mapping_saved" in text:
                    self._refresh_mapping()
            elif kind == "po_created":
                self.create_po_btn.configure(state=tk.NORMAL)
                self.create_status_var.set("Create: success")
                info = dict(payload)
                fid = str(info.get("id", "")).strip()
                bill_no = str(info.get("billNo", "")).strip()
                self._log(
                    f"[{now_iso()}] random PO created: "
                    f"FID={fid} BillNo={bill_no} Supplier={info.get('supplierNo','')} "
                    f"Material={info.get('materialNo','')} Qty={info.get('qty','')}"
                )
                self._add_erp_detected_row(
                    {
                        "detected_at": now_iso(),
                        "FID": fid,
                        "FBillNo": bill_no,
                        "FDate": dt.date.today().isoformat(),
                        "FDocumentStatus": "",
                        "FPurchaseOrgId.FNumber": info.get("orgNo", ""),
                        "FSupplierId.FNumber": info.get("supplierNo", ""),
                        "FSupplierId.FName": "",
                        "FMaterialId.FNumber": info.get("materialNo", ""),
                        "FQty": info.get("qty", ""),
                    }
                )
            elif kind == "po_create_failed":
                self.create_po_btn.configure(state=tk.NORMAL)
                self.create_status_var.set("Create: failed")
                self._log(f"[{now_iso()}] random PO create failed: {payload}")
                messagebox.showerror("Create PO failed", str(payload))
            elif kind == "po_stopped":
                self.po_monitor_proc = None
                self.po_monitor_reader_thread = None
                self.po_start_btn.configure(state=tk.NORMAL)
                self.po_stop_btn.configure(state=tk.DISABLED)
                self.po_monitor_status_var.set("PO->DingTalk: stopped")
                self._log(f"[{now_iso()}] PO monitor exited rc={payload}")
            elif kind == "dt_status_changed":
                info = dict(payload)
                self._log(
                    f"[{now_iso()}] DingTalk status changed: "
                    f"PID={info.get('processInstanceId','')} "
                    f"PO={info.get('poBillNo','')} "
                    f"raw={info.get('rawStatus','')}/{info.get('rawResult','')} "
                    f"mapped={info.get('mappedStatus','')} ({info.get('reason','')})"
                )
            elif kind == "dt_mapping_updated":
                info = dict(payload)
                self._log(
                    f"[{now_iso()}] mapping callback updated by polling: "
                    f"PID={info.get('processInstanceId','')} "
                    f"PO={info.get('poBillNo','')} "
                    f"status={info.get('status','')} result={info.get('result','')}"
                )
                self._refresh_mapping()
            elif kind == "wb_retry_started":
                info = dict(payload)
                self.wb_retry_running = True
                self.wb_retry_status_var.set(
                    f"Writeback Retry: running ({int(float(info.get('interval', 10)))}s/"
                    f"{int(float(info.get('max_minutes', 30)))}m)"
                )
            elif kind == "wb_writeback":
                info = dict(payload)
                if info.get("skip"):
                    self._log(
                        f"[{now_iso()}] writeback skipped by mode: "
                        f"PID={info.get('processInstanceId','')} "
                        f"PO={info.get('poBillNo','')} status={info.get('status','')} "
                        f"msg={info.get('message','')}"
                    )
                elif info.get("timeout"):
                    self._log(
                        f"[{now_iso()}] writeback timeout: "
                        f"PID={info.get('processInstanceId','')} "
                        f"PO={info.get('poBillNo','')} status={info.get('status','')} "
                        f"msg={info.get('message','')}"
                    )
                else:
                    self._log(
                        f"[{now_iso()}] retry writeback "
                        f"{'ok' if info.get('ok') else 'failed'}: "
                        f"PID={info.get('processInstanceId','')} "
                        f"PO={info.get('poBillNo','')} status={info.get('status','')} "
                        f"retryable={bool(info.get('retryable'))} "
                        f"msg={info.get('message','')}"
                    )
                self._refresh_mapping()
            elif kind == "wb_retry_stopped":
                self.wb_retry_running = False
                self.wb_retry_thread = None
                self.wb_retry_status_var.set("Writeback Retry: stopped")
                self._log(f"[{now_iso()}] writeback retry stopped")
            elif kind == "dt_writeback":
                info = dict(payload)
                self._log(
                    f"[{now_iso()}] polling writeback "
                    f"{'ok' if info.get('ok') else 'failed'}: "
                    f"PID={info.get('processInstanceId','')} "
                    f"PO={info.get('poBillNo','')} "
                    f"status={info.get('status','')} "
                    f"msg={info.get('message','')}"
                )
                self._refresh_mapping()
            elif kind == "dt_poll_stopped":
                self.dt_poll_running = False
                self.dt_poll_start_btn.configure(state=tk.NORMAL)
                self.dt_poll_stop_btn.configure(state=tk.DISABLED)
                self.dt_poll_status_var.set("DingTalk Poll: stopped")
                self._log(f"[{now_iso()}] DingTalk poll stopped")
        self.after(200, self._poll_queue)

    def _on_close(self) -> None:
        self._stop_all()
        self.destroy()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="GUI for DingTalk callback monitor.")
    p.add_argument("--env-file", default=DEFAULT_ENV_FILE)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    app = CallbackMonitorGUI()
    app.env_file_var.set(args.env_file)
    app._load_env()
    app.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

