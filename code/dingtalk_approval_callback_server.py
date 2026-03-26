#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import json
import os
import secrets
import struct
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from dingtalk_erp_bridge import (
    ErpWritebackConfig,
    ErpWritebackService,
    get_link_by_process_instance_id,
    now_iso,
    update_callback_info,
    update_writeback_result,
)


DEFAULT_ENV_FILE = r"D:\ProjectPackage\demo\dingding_demo\code\.env"
DEFAULT_MAPPING_DB = r"D:\ProjectPackage\demo\erp_demo\erp_dingtalk_links.db"

DEFAULT_ERP_BASE_URL = "http://172.30.30.8"
DEFAULT_ERP_ACCT_ID = "6977227150362f"
DEFAULT_ERP_USERNAME = "\u8d3e\u6cfd\u5b87"
DEFAULT_ERP_PASSWORD = "Showgood1987!"
DEFAULT_ERP_LCID = 2052

APPROVED_STATUSES = {"APPROVED"}
TERMINAL_STATUSES = {"APPROVED", "REJECTED", "CANCELED", "TERMINATED"}


def print_line(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False), flush=True)


def load_env_file(path: str) -> dict[str, str]:
    if not path:
        return {}
    if not os.path.exists(path):
        return {}
    env: dict[str, str] = {}
    text = open(path, "r", encoding="utf-8-sig", errors="ignore").read()
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip().lstrip("\ufeff")
        v = v.strip()
        if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
            v = v[1:-1]
        if k:
            env[k] = v
    return env


def first_non_empty(*values: Any) -> str:
    for v in values:
        if v is None:
            continue
        text = str(v).strip()
        if text:
            return text
    return ""


def parse_json_maybe(raw: bytes) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        obj = json.loads(raw.decode("utf-8", errors="ignore"))
        if isinstance(obj, dict):
            return obj
        return {"_raw_json": obj}
    except Exception:  # noqa: BLE001
        return {"_raw_text": raw.decode("utf-8", errors="ignore")}


def flatten_text_values(obj: Any) -> list[str]:
    out: list[str] = []
    if isinstance(obj, dict):
        for v in obj.values():
            out.extend(flatten_text_values(v))
    elif isinstance(obj, list):
        for v in obj:
            out.extend(flatten_text_values(v))
    elif isinstance(obj, (str, int, float, bool)):
        out.append(str(obj))
    return out


def find_value_by_keys(obj: Any, keys: set[str]) -> str:
    lowered = {k.lower() for k in keys}

    def walk(node: Any) -> str:
        if isinstance(node, dict):
            for k, v in node.items():
                if str(k).lower() in lowered:
                    if isinstance(v, (str, int, float, bool)):
                        return str(v)
                found = walk(v)
                if found:
                    return found
        elif isinstance(node, list):
            for v in node:
                found = walk(v)
                if found:
                    return found
        return ""

    return walk(obj)


def classify_status(payload: dict[str, Any]) -> tuple[str, str]:
    key_values = [
        find_value_by_keys(payload, {"status", "processstatus", "processinstancestatus"}),
        find_value_by_keys(payload, {"result", "approvalresult", "taskresult"}),
        find_value_by_keys(payload, {"eventtype", "type"}),
    ]
    all_text = " ".join([x for x in key_values if x] + flatten_text_values(payload)).lower()

    reject_words = ["reject", "refuse", "disagree", "\u62d2\u7edd", "\u9a73\u56de"]
    cancel_words = ["cancel", "terminate", "stop", "\u64a4\u9500", "\u53d6\u6d88", "\u7ec8\u6b62"]
    approved_words = ["agree", "approved", "pass", "complete", "success", "\u540c\u610f", "\u5b8c\u6210", "\u901a\u8fc7"]

    if any(w in all_text for w in reject_words):
        return "REJECTED", "keyword match reject"
    if any(w in all_text for w in cancel_words):
        return "CANCELED", "keyword match cancel"
    if any(w in all_text for w in approved_words):
        return "APPROVED", "keyword match approved"
    return "UNKNOWN", "cannot classify"


def should_writeback(status: str, mode: str) -> bool:
    s = status.upper()
    if mode == "all":
        return True
    if mode == "terminal":
        return s in TERMINAL_STATUSES
    return s in APPROVED_STATUSES


def base64_decode_key(aes_key: str) -> bytes:
    padded = aes_key + ("=" * ((4 - len(aes_key) % 4) % 4))
    return base64.b64decode(padded)


def pkcs7_unpad(data: bytes) -> bytes:
    if not data:
        raise ValueError("empty data")
    pad = data[-1]
    if pad <= 0 or pad > 32:
        raise ValueError("invalid padding")
    if data[-pad:] != bytes([pad]) * pad:
        raise ValueError("invalid padding bytes")
    return data[:-pad]


def pkcs7_pad(data: bytes, block_size: int = 32) -> bytes:
    pad = block_size - (len(data) % block_size)
    if pad == 0:
        pad = block_size
    return data + bytes([pad]) * pad


@dataclass(frozen=True)
class DingTalkCryptoConfig:
    token: str
    aes_key: str


class DingTalkCrypto:
    def __init__(self, config: DingTalkCryptoConfig):
        self.token = config.token
        self.key = base64_decode_key(config.aes_key)
        if len(self.key) != 32:
            raise RuntimeError("DINGTALK_CALLBACK_AES_KEY decode length must be 32 bytes")
        self.iv = self.key[:16]

    def signature(self, timestamp: str, nonce: str, encrypt_text: str) -> str:
        arr = [self.token, timestamp, nonce, encrypt_text]
        arr.sort()
        raw = "".join(arr).encode("utf-8")
        return hashlib.sha1(raw).hexdigest()

    def decrypt(self, encrypt_text: str) -> tuple[dict[str, Any], str]:
        encrypted = base64.b64decode(encrypt_text)
        cipher = Cipher(algorithms.AES(self.key), modes.CBC(self.iv))
        decryptor = cipher.decryptor()
        plain = decryptor.update(encrypted) + decryptor.finalize()
        plain = pkcs7_unpad(plain)
        if len(plain) < 20:
            raise RuntimeError("decrypted body too short")
        msg_len = struct.unpack(">I", plain[16:20])[0]
        msg_bytes = plain[20 : 20 + msg_len]
        receiver_bytes = plain[20 + msg_len :]
        msg_text = msg_bytes.decode("utf-8", errors="ignore")
        receiver_id = receiver_bytes.decode("utf-8", errors="ignore")
        obj = json.loads(msg_text)
        if not isinstance(obj, dict):
            raise RuntimeError("decrypted payload is not object")
        return obj, receiver_id

    def encrypt(self, payload_obj: dict[str, Any], receiver_id: str = "") -> tuple[str, str, str]:
        raw_json = json.dumps(payload_obj, ensure_ascii=False).encode("utf-8")
        random16 = secrets.token_bytes(16)
        msg_len = struct.pack(">I", len(raw_json))
        plain = random16 + msg_len + raw_json + receiver_id.encode("utf-8")
        plain = pkcs7_pad(plain, 32)
        cipher = Cipher(algorithms.AES(self.key), modes.CBC(self.iv))
        encryptor = cipher.encryptor()
        encrypted = encryptor.update(plain) + encryptor.finalize()
        enc_text = base64.b64encode(encrypted).decode("utf-8")
        timestamp = str(int(dt.datetime.now().timestamp()))
        nonce = secrets.token_hex(8)
        sign = self.signature(timestamp, nonce, enc_text)
        return enc_text, sign, nonce


@dataclass(frozen=True)
class ServerConfig:
    host: str
    port: int
    path: str
    shared_token: str
    mapping_db: str
    writeback_on: str
    defer_writeback: bool = False


class CallbackApp:
    def __init__(
        self,
        *,
        server_config: ServerConfig,
        writeback_service: ErpWritebackService,
        dingtalk_crypto: DingTalkCrypto | None,
        require_dingtalk_signature: bool,
    ):
        self.server_config = server_config
        self.writeback_service = writeback_service
        self.dingtalk_crypto = dingtalk_crypto
        self.require_dingtalk_signature = require_dingtalk_signature

    def handle_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        callback_time = now_iso()
        process_instance_id = find_value_by_keys(
            payload,
            {"processInstanceId", "process_instance_id", "instanceId", "processinstanceid"},
        )
        status, status_reason = classify_status(payload)
        result_text = find_value_by_keys(payload, {"result", "taskResult", "approvalResult"})

        response: dict[str, Any] = {
            "ok": True,
            "time": callback_time,
            "processInstanceId": process_instance_id,
            "status": status,
            "statusReason": status_reason,
        }

        if not process_instance_id:
            response["message"] = "no processInstanceId in callback payload"
            print_line({"level": "warn", "event": "callback_no_pid", "payload": payload})
            return response

        link = get_link_by_process_instance_id(self.server_config.mapping_db, process_instance_id)
        if link is None:
            response["message"] = "processInstanceId not found in mapping db"
            print_line(
                {
                    "level": "warn",
                    "event": "callback_pid_not_found",
                    "processInstanceId": process_instance_id,
                    "status": status,
                }
            )
            return response

        update_callback_info(
            self.server_config.mapping_db,
            process_instance_id=process_instance_id,
            callback_status=status,
            callback_result=result_text,
            callback_time=callback_time,
            raw_payload=payload,
        )

        if self.server_config.defer_writeback:
            response["message"] = "callback stored; writeback deferred"
            response["queued"] = True
            print_line(
                {
                    "level": "info",
                    "event": "writeback_queued",
                    "processInstanceId": process_instance_id,
                    "poFid": link.get("po_fid"),
                    "poBillNo": link.get("po_bill_no"),
                    "status": status,
                    "mode": self.server_config.writeback_on,
                }
            )
            return response

        if not should_writeback(status, self.server_config.writeback_on):
            response["message"] = f"skip writeback for status={status} with mode={self.server_config.writeback_on}"
            print_line(
                {
                    "level": "info",
                    "event": "writeback_skipped",
                    "processInstanceId": process_instance_id,
                    "poFid": link.get("po_fid"),
                    "status": status,
                    "mode": self.server_config.writeback_on,
                }
            )
            return response

        try:
            ok, msg = self.writeback_service.writeback(
                po_fid=str(link.get("po_fid", "")),
                po_bill_no=str(link.get("po_bill_no", "")),
                process_instance_id=process_instance_id,
                callback_status=status,
                callback_result=result_text,
                callback_time=callback_time,
            )
        except Exception as exc:  # noqa: BLE001
            ok = False
            msg = str(exc)

        update_writeback_result(
            self.server_config.mapping_db,
            process_instance_id=process_instance_id,
            ok=ok,
            message=msg,
        )
        response["writebackOk"] = ok
        response["writebackMessage"] = msg

        print_line(
            {
                "level": "info" if ok else "error",
                "event": "writeback_done",
                "processInstanceId": process_instance_id,
                "poFid": link.get("po_fid"),
                "poBillNo": link.get("po_bill_no"),
                "status": status,
                "ok": ok,
                "message": msg,
            }
        )
        return response


def build_handler(app: CallbackApp):
    path = app.server_config.path if app.server_config.path.startswith("/") else f"/{app.server_config.path}"

    class Handler(BaseHTTPRequestHandler):
        server_version = "DingTalkCallbackHTTP/1.0"

        def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
            return

        def _send_json(self, code: int, body_obj: dict[str, Any]) -> None:
            data = json.dumps(body_obj, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:  # noqa: N802
            if self.path.startswith("/health"):
                self._send_json(200, {"ok": True, "service": "dingtalk_approval_callback_server"})
                return
            self._send_json(404, {"ok": False, "error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            parsed_url = urlparse(self.path)
            if parsed_url.path != path:
                self._send_json(404, {"ok": False, "error": "invalid path"})
                return

            if app.server_config.shared_token:
                req_token = self.headers.get("X-Callback-Token", "").strip()
                if req_token != app.server_config.shared_token:
                    self._send_json(403, {"ok": False, "error": "invalid callback token"})
                    return

            body_len = int(self.headers.get("Content-Length", "0") or 0)
            raw = self.rfile.read(body_len) if body_len > 0 else b""
            payload = parse_json_maybe(raw)
            encrypted_receiver = ""

            if "encrypt" in payload:
                if app.dingtalk_crypto is None:
                    self._send_json(400, {"ok": False, "error": "encrypted callback but crypto is not configured"})
                    return

                query = parse_qs(parsed_url.query, keep_blank_values=True)
                signature = first_non_empty(
                    query.get("msg_signature", [""])[0],
                    query.get("signature", [""])[0],
                    payload.get("msg_signature"),
                    payload.get("signature"),
                )
                timestamp = first_non_empty(
                    query.get("timestamp", [""])[0],
                    payload.get("timestamp"),
                    str(int(dt.datetime.now().timestamp())),
                )
                nonce = first_non_empty(query.get("nonce", [""])[0], payload.get("nonce"), "nonce")
                encrypt_text = str(payload.get("encrypt") or "")
                if app.require_dingtalk_signature:
                    local_sign = app.dingtalk_crypto.signature(timestamp, nonce, encrypt_text)
                    if not signature or signature != local_sign:
                        self._send_json(403, {"ok": False, "error": "invalid dingtalk signature"})
                        return
                try:
                    payload, encrypted_receiver = app.dingtalk_crypto.decrypt(encrypt_text)
                except Exception as exc:  # noqa: BLE001
                    self._send_json(400, {"ok": False, "error": f"decrypt failed: {exc}"})
                    return

            result = app.handle_payload(payload)

            if "encrypt" in parse_json_maybe(raw) and app.dingtalk_crypto is not None:
                enc_text, sign, nonce = app.dingtalk_crypto.encrypt({"success": True}, encrypted_receiver)
                self._send_json(
                    200,
                    {
                        "msg_signature": sign,
                        "encrypt": enc_text,
                        "timeStamp": str(int(dt.datetime.now().timestamp())),
                        "nonce": nonce,
                        "result": result,
                    },
                )
                return

            self._send_json(200, result)

    return Handler


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="DingTalk approval callback server with ERP writeback.")
    p.add_argument("--env-file", default=DEFAULT_ENV_FILE)
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=19110)
    p.add_argument("--path", default="/dingtalk/callback")
    p.add_argument("--shared-token", default="", help="Optional custom header token: X-Callback-Token.")
    p.add_argument("--mapping-db", default=DEFAULT_MAPPING_DB)
    p.add_argument("--writeback-on", choices=["approved", "terminal", "all"], default="approved")
    p.add_argument(
        "--defer-writeback",
        action="store_true",
        help="Only store callback to DB; do not write ERP immediately.",
    )

    p.add_argument("--dingtalk-callback-token", default="")
    p.add_argument("--dingtalk-callback-aes-key", default="")
    p.add_argument(
        "--require-dingtalk-signature",
        action="store_true",
        help="Require signature check for encrypted callback.",
    )

    p.add_argument("--base-url", default=DEFAULT_ERP_BASE_URL)
    p.add_argument("--acct-id", default=DEFAULT_ERP_ACCT_ID)
    p.add_argument("--username", default=DEFAULT_ERP_USERNAME)
    p.add_argument("--password", default=DEFAULT_ERP_PASSWORD)
    p.add_argument("--lcid", type=int, default=DEFAULT_ERP_LCID)
    p.add_argument("--timeout", type=int, default=60)
    p.add_argument("--insecure", action="store_true")

    p.add_argument("--erp-field-status", default="")
    p.add_argument("--erp-field-result", default="")
    p.add_argument("--erp-field-instance-id", default="")
    p.add_argument("--erp-field-callback-time", default="")
    p.add_argument("--erp-field-note", default="FNote")
    p.add_argument("--no-probe-fields", action="store_true")
    p.add_argument(
        "--erp-approve-mode",
        choices=["none", "submit_audit", "workflow"],
        default="submit_audit",
        help="How to push ERP document to approved state after DingTalk APPROVED callback.",
    )
    p.add_argument(
        "--erp-approve-comment",
        default="钉钉审批通过自动审核",
        help="Comment passed to workflow audit (when mode=workflow).",
    )
    p.add_argument("--erp-workflow-user-id", default="")
    p.add_argument("--erp-workflow-user-name", default="")
    p.add_argument("--erp-workflow-post-id", default="")
    p.add_argument("--erp-workflow-post-number", default="")
    p.add_argument("--erp-workflow-approval-type", default="1")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    env = load_env_file(args.env_file)

    callback_token = first_non_empty(
        args.dingtalk_callback_token,
        os.getenv("DINGTALK_CALLBACK_TOKEN"),
        env.get("DINGTALK_CALLBACK_TOKEN"),
    )
    callback_aes_key = first_non_empty(
        args.dingtalk_callback_aes_key,
        os.getenv("DINGTALK_CALLBACK_AES_KEY"),
        env.get("DINGTALK_CALLBACK_AES_KEY"),
    )

    erp_cfg = ErpWritebackConfig(
        base_url=first_non_empty(args.base_url, env.get("ERP_BASE_URL")),
        acct_id=first_non_empty(args.acct_id, env.get("ERP_ACCT_ID")),
        username=first_non_empty(args.username, env.get("ERP_USERNAME")),
        password=first_non_empty(args.password, env.get("ERP_PASSWORD")),
        lcid=args.lcid,
        timeout=args.timeout,
        verify_ssl=not args.insecure,
        field_status=first_non_empty(args.erp_field_status, env.get("ERP_DD_FIELD_STATUS")),
        field_result=first_non_empty(args.erp_field_result, env.get("ERP_DD_FIELD_RESULT")),
        field_instance_id=first_non_empty(args.erp_field_instance_id, env.get("ERP_DD_FIELD_INSTANCE_ID")),
        field_callback_time=first_non_empty(args.erp_field_callback_time, env.get("ERP_DD_FIELD_CALLBACK_TIME")),
        field_note=first_non_empty(args.erp_field_note, env.get("ERP_DD_FIELD_NOTE"), "FNote"),
        probe_fields=not args.no_probe_fields,
        approve_mode=first_non_empty(args.erp_approve_mode, env.get("ERP_DD_APPROVE_MODE"), "submit_audit"),
        approve_comment=first_non_empty(
            args.erp_approve_comment,
            env.get("ERP_DD_APPROVE_COMMENT"),
            "钉钉审批通过自动审核",
        ),
        workflow_user_id=first_non_empty(args.erp_workflow_user_id, env.get("ERP_DD_WORKFLOW_USER_ID")),
        workflow_user_name=first_non_empty(args.erp_workflow_user_name, env.get("ERP_DD_WORKFLOW_USER_NAME")),
        workflow_post_id=first_non_empty(args.erp_workflow_post_id, env.get("ERP_DD_WORKFLOW_POST_ID")),
        workflow_post_number=first_non_empty(
            args.erp_workflow_post_number, env.get("ERP_DD_WORKFLOW_POST_NUMBER")
        ),
        workflow_approval_type=first_non_empty(
            args.erp_workflow_approval_type,
            env.get("ERP_DD_WORKFLOW_APPROVAL_TYPE"),
            "1",
        ),
    )
    writeback_service = ErpWritebackService(erp_cfg)
    try:
        writeback_service.login()
    except Exception as exc:  # noqa: BLE001
        print_line({"level": "error", "event": "erp_login_failed", "message": str(exc)})
        return 1

    crypto: DingTalkCrypto | None = None
    if callback_token and callback_aes_key:
        try:
            crypto = DingTalkCrypto(
                DingTalkCryptoConfig(token=callback_token, aes_key=callback_aes_key)
            )
        except Exception as exc:  # noqa: BLE001
            print_line({"level": "error", "event": "crypto_init_failed", "message": str(exc)})
            return 1

    server_cfg = ServerConfig(
        host=args.host,
        port=args.port,
        path=args.path,
        shared_token=args.shared_token,
        mapping_db=args.mapping_db,
        writeback_on=args.writeback_on,
        defer_writeback=args.defer_writeback,
    )
    app = CallbackApp(
        server_config=server_cfg,
        writeback_service=writeback_service,
        dingtalk_crypto=crypto,
        require_dingtalk_signature=args.require_dingtalk_signature,
    )

    Handler = build_handler(app)
    httpd = ThreadingHTTPServer((server_cfg.host, server_cfg.port), Handler)
    print_line(
        {
            "level": "info",
            "event": "callback_server_start",
            "listen": f"http://{server_cfg.host}:{server_cfg.port}{server_cfg.path}",
            "health": f"http://{server_cfg.host}:{server_cfg.port}/health",
            "mappingDb": server_cfg.mapping_db,
            "writebackOn": server_cfg.writeback_on,
            "erpApproveMode": erp_cfg.approve_mode,
            "erpWorkflowUserId": erp_cfg.workflow_user_id,
            "erpWorkflowUserName": erp_cfg.workflow_user_name,
            "erpWorkflowPostId": erp_cfg.workflow_post_id,
            "erpWorkflowPostNumber": erp_cfg.workflow_post_number,
            "erpWorkflowApprovalType": erp_cfg.workflow_approval_type,
            "encryptedCallback": bool(crypto),
        }
    )
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
