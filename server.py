#!/usr/bin/env python3
import json
import os
import secrets
import sqlite3
from datetime import datetime, timezone
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "feedback.sqlite3"
HOST = os.environ.get("SERENZER_HOST", "127.0.0.1")
PORT = int(os.environ.get("SERENZER_PORT", "8000"))
IMPORT_API_KEY = os.environ.get("SERENZER_IMPORT_API_KEY", "").strip()


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def normalize_code(value):
    return str(value or "").strip().upper()


def make_remember_token():
    return secrets.token_urlsafe(32)


def is_meaningful_value(value):
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() != ""
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) > 0
    return True


def merge_saved_value(existing, incoming):
    if isinstance(existing, dict) and isinstance(incoming, dict):
        merged = dict(existing)
        for key, value in incoming.items():
            if key in merged:
                merged[key] = merge_saved_value(merged[key], value)
            elif is_meaningful_value(value):
                merged[key] = value
        return merged
    if isinstance(existing, list) and isinstance(incoming, list):
        return incoming if incoming else existing
    return incoming if is_meaningful_value(incoming) else existing


def get_db():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS feedback_submissions (
            submission_id TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            lang TEXT,
            is_complete INTEGER NOT NULL DEFAULT 0,
            email TEXT,
            invitation_number TEXT,
            completed_tabs_json TEXT NOT NULL,
            onboarding_json TEXT NOT NULL,
            tools_json TEXT NOT NULL,
            payload_json TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS invitation_codes (
            code TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            bound_submission_id TEXT,
            use_count INTEGER NOT NULL DEFAULT 0,
            used_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bug_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            submission_id TEXT,
            invitation_number TEXT,
            email TEXT,
            first_name TEXT,
            last_name TEXT,
            lang TEXT,
            active_tab INTEGER,
            active_tab_label TEXT,
            page_url TEXT,
            user_agent TEXT,
            message TEXT NOT NULL
        )
        """
    )
    existing_columns = {row["name"] for row in conn.execute("PRAGMA table_info(invitation_codes)").fetchall()}
    if "remember_token" not in existing_columns:
        conn.execute("ALTER TABLE invitation_codes ADD COLUMN remember_token TEXT")
    if "email" not in existing_columns:
        conn.execute("ALTER TABLE invitation_codes ADD COLUMN email TEXT")
    if "app_user_id" not in existing_columns:
        conn.execute("ALTER TABLE invitation_codes ADD COLUMN app_user_id TEXT")
    if "source" not in existing_columns:
        conn.execute("ALTER TABLE invitation_codes ADD COLUMN source TEXT")
    return conn


def claim_invitation_code(conn, code, submission_id):
    normalized = normalize_code(code)
    if not normalized or not submission_id:
        return False, "Invitation code is required"

    row = conn.execute(
        """
        SELECT code, is_active, bound_submission_id, use_count, email, app_user_id, source
        FROM invitation_codes
        WHERE code = ?
        """,
        (normalized,),
    ).fetchone()

    if row is None or not row["is_active"]:
        return False, "Invalid invitation code"

    if row["bound_submission_id"] and row["bound_submission_id"] != submission_id:
        return False, "This invitation code is already in use"

    first_activation = row["use_count"] < 1 and not row["bound_submission_id"]
    timestamp = now_iso()
    conn.execute(
        """
        UPDATE invitation_codes
        SET updated_at = ?,
            bound_submission_id = ?,
            use_count = CASE WHEN use_count < 1 THEN 1 ELSE use_count END,
            used_at = COALESCE(used_at, ?)
        WHERE code = ?
        """,
        (timestamp, submission_id, timestamp, normalized),
    )
    return True, {
        "code": normalized,
        "firstActivation": first_activation,
        "email": row["email"],
        "appUserId": row["app_user_id"],
        "source": row["source"],
    }


def restore_invitation_session(conn, remember_token, submission_id):
    token = str(remember_token or "").strip()
    if not token or not submission_id:
        return False, "Missing remember token"

    row = conn.execute(
        """
        SELECT code, is_active
        FROM invitation_codes
        WHERE remember_token = ?
        """,
        (token,),
    ).fetchone()

    if row is None or not row["is_active"]:
        return False, "No remembered invitation session found"

    timestamp = now_iso()
    conn.execute(
        """
        UPDATE invitation_codes
        SET updated_at = ?,
            bound_submission_id = ?
        WHERE remember_token = ?
        """,
        (timestamp, submission_id, token),
    )
    return True, row["code"]


def list_invitation_codes(conn):
    rows = conn.execute(
        """
        SELECT code, created_at, updated_at, is_active, bound_submission_id, use_count, used_at, email, app_user_id, source
        FROM invitation_codes
        ORDER BY created_at DESC, code ASC
        """
    ).fetchall()
    return [
        {
            "code": row["code"],
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
            "isActive": bool(row["is_active"]),
            "boundSubmissionId": row["bound_submission_id"],
            "useCount": row["use_count"],
            "usedAt": row["used_at"],
            "email": row["email"],
            "appUserId": row["app_user_id"],
            "source": row["source"],
        }
        for row in rows
    ]


def list_feedback_submissions(conn):
    rows = conn.execute(
        """
        SELECT submission_id, created_at, updated_at, lang, is_complete, email, invitation_number, completed_tabs_json
        FROM feedback_submissions
        ORDER BY updated_at DESC
        """
    ).fetchall()
    items = []
    for row in rows:
        completed_tabs = json.loads(row["completed_tabs_json"])
        items.append(
            {
                "submissionId": row["submission_id"],
                "createdAt": row["created_at"],
                "updatedAt": row["updated_at"],
                "lang": row["lang"],
                "isComplete": bool(row["is_complete"]),
                "email": row["email"],
                "invitationNumber": row["invitation_number"],
                "completedTabsCount": len(completed_tabs),
            }
        )
    return items


def list_feedback_entries(conn):
    submissions = list_feedback_submissions(conn)
    submission_invites = {
        normalize_code(item.get("invitationNumber")): item
        for item in submissions
        if normalize_code(item.get("invitationNumber"))
    }
    items = []

    for submission in submissions:
        submission["entryType"] = "submission"
        items.append(submission)

    invite_rows = conn.execute(
        """
        SELECT code, created_at, updated_at, is_active, bound_submission_id, use_count, used_at, email, app_user_id, source
        FROM invitation_codes
        ORDER BY updated_at DESC, created_at DESC, code ASC
        """
    ).fetchall()

    for row in invite_rows:
        code = row["code"]
        if code in submission_invites:
            continue
        items.append(
            {
                "submissionId": f"ghost:{code}",
                "createdAt": row["created_at"],
                "updatedAt": row["updated_at"],
                "lang": None,
                "isComplete": False,
                "email": row["email"],
                "invitationNumber": code,
                "completedTabsCount": 0,
                "entryType": "ghost",
                "isActive": bool(row["is_active"]),
                "boundSubmissionId": row["bound_submission_id"],
                "useCount": row["use_count"],
                "usedAt": row["used_at"],
                "appUserId": row["app_user_id"],
                "source": row["source"],
            }
        )

    items.sort(key=lambda item: item.get("updatedAt") or item.get("createdAt") or "", reverse=True)
    return items


def get_ghost_feedback_detail(conn, code):
    normalized = normalize_code(code)
    if not normalized:
        return None
    row = conn.execute(
        """
        SELECT code, created_at, updated_at, is_active, bound_submission_id, use_count, used_at, email, app_user_id, source
        FROM invitation_codes
        WHERE code = ?
        """,
        (normalized,),
    ).fetchone()
    if row is None:
        return None
    return {
        "submissionId": f"ghost:{row['code']}",
        "entryType": "ghost",
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "lang": None,
        "isComplete": False,
        "email": row["email"],
        "invitationNumber": row["code"],
        "completedTabs": [],
        "onboarding": {},
        "tools": {},
        "isActive": bool(row["is_active"]),
        "boundSubmissionId": row["bound_submission_id"],
        "useCount": row["use_count"],
        "usedAt": row["used_at"],
        "appUserId": row["app_user_id"],
        "source": row["source"],
        "payload": {
            "kind": "ghost",
            "invitationCode": row["code"],
            "email": row["email"],
            "appUserId": row["app_user_id"],
            "source": row["source"],
            "isActive": bool(row["is_active"]),
            "boundSubmissionId": row["bound_submission_id"],
            "useCount": row["use_count"],
            "usedAt": row["used_at"],
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
        },
    }


def upsert_invitation_codes(conn, codes):
    timestamp = now_iso()
    normalized_codes = []
    for raw_code in codes:
        code = normalize_code(raw_code)
        if not code:
            continue
        normalized_codes.append(code)
        conn.execute(
            """
            INSERT INTO invitation_codes (code, created_at, updated_at, is_active, source)
            VALUES (?, ?, ?, 1, 'manual')
            ON CONFLICT(code) DO UPDATE SET
                updated_at = excluded.updated_at,
                is_active = 1,
                source = 'manual'
            """,
            (code, timestamp, timestamp),
        )
    return normalized_codes


def import_invitation_entries(conn, entries):
    timestamp = now_iso()
    imported = []
    for entry in entries:
        if isinstance(entry, str):
            entry = {"code": entry}
        if not isinstance(entry, dict):
            continue
        code = normalize_code(entry.get("code"))
        if not code:
            continue
        email = str(entry.get("email") or "").strip() or None
        app_user_id = str(entry.get("appUserId") or entry.get("app_user_id") or "").strip() or None
        source = str(entry.get("source") or "serenzer-app").strip() or None
        imported.append(code)
        conn.execute(
            """
            INSERT INTO invitation_codes (
                code, created_at, updated_at, is_active, email, app_user_id, source
            ) VALUES (?, ?, ?, 1, ?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET
                updated_at = excluded.updated_at,
                is_active = 1,
                email = COALESCE(excluded.email, invitation_codes.email),
                app_user_id = COALESCE(excluded.app_user_id, invitation_codes.app_user_id),
                source = COALESCE(excluded.source, invitation_codes.source)
            """,
            (code, timestamp, timestamp, email, app_user_id, source),
        )
    return imported


def disable_invitation_code(conn, code):
    normalized = normalize_code(code)
    if not normalized:
        return False
    cursor = conn.execute(
        """
        UPDATE invitation_codes
        SET is_active = 0, updated_at = ?
        WHERE code = ?
        """,
        (now_iso(), normalized),
    )
    return cursor.rowcount > 0


def list_bug_reports(conn):
    rows = conn.execute(
        """
        SELECT id, created_at, submission_id, invitation_number, email, first_name, last_name,
               lang, active_tab, active_tab_label, page_url, user_agent, message
        FROM bug_reports
        ORDER BY created_at DESC, id DESC
        """
    ).fetchall()
    return [
        {
            "id": row["id"],
            "createdAt": row["created_at"],
            "submissionId": row["submission_id"],
            "invitationNumber": row["invitation_number"],
            "email": row["email"],
            "firstName": row["first_name"],
            "lastName": row["last_name"],
            "lang": row["lang"],
            "activeTab": row["active_tab"],
            "activeTabLabel": row["active_tab_label"],
            "pageUrl": row["page_url"],
            "userAgent": row["user_agent"],
            "message": row["message"],
        }
        for row in rows
    ]


def create_bug_report(conn, payload):
    message = str(payload.get("message") or "").strip()
    if not message:
        return False, "Bug description is required"

    onboarding = payload.get("onboarding") or {}
    timestamp = now_iso()
    conn.execute(
        """
        INSERT INTO bug_reports (
            created_at,
            submission_id,
            invitation_number,
            email,
            first_name,
            last_name,
            lang,
            active_tab,
            active_tab_label,
            page_url,
            user_agent,
            message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            timestamp,
            str(payload.get("submissionId") or "").strip() or None,
            normalize_code(onboarding.get("invitationNumber")) or None,
            str(onboarding.get("email") or "").strip() or None,
            str(onboarding.get("firstName") or "").strip() or None,
            str(onboarding.get("lastName") or "").strip() or None,
            str(payload.get("lang") or "").strip() or None,
            int(payload.get("activeTab")) if str(payload.get("activeTab") or "").isdigit() else None,
            str(payload.get("activeTabLabel") or "").strip() or None,
            str(payload.get("pageUrl") or "").strip() or None,
            str(payload.get("userAgent") or "").strip() or None,
            message,
        ),
    )
    return True, timestamp


class FeedbackHandler(BaseHTTPRequestHandler):
    server_version = "SerenzerFeedback/1.0"

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(204)
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/health":
            self._send_json(200, {"ok": True})
            return
        if parsed.path == "/api/admin/bug-reports":
            self._handle_bug_report_list()
            return
        if parsed.path == "/api/admin/invitations":
            self._handle_invitation_list()
            return
        if parsed.path == "/api/invitations":
            self._handle_invitation_list()
            return
        if parsed.path == "/api/invitations/session":
            self._send_json(405, {"error": "Use POST"})
            return
        if parsed.path == "/api/admin/feedback":
            self._handle_feedback_list()
            return
        if parsed.path == "/api/feedback":
            self._handle_feedback_list()
            return
        if parsed.path.startswith("/api/admin/feedback/"):
            submission_id = parsed.path.removeprefix("/api/admin/feedback/")
            self._handle_feedback_detail(submission_id)
            return
        if parsed.path.startswith("/api/feedback/"):
            submission_id = parsed.path.removeprefix("/api/feedback/")
            self._handle_feedback_detail(submission_id)
            return
        self._send_json(404, {"error": "Not found"})

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/feedback":
            self._handle_feedback_upsert()
            return
        if parsed.path == "/api/bug-reports":
            self._handle_bug_report_create()
            return
        if parsed.path == "/api/admin/invitations":
            self._handle_invitation_create()
            return
        if parsed.path == "/api/admin/invitations/disable":
            self._handle_invitation_disable()
            return
        if parsed.path == "/api/invitations/import":
            self._handle_invitation_import()
            return
        if parsed.path == "/api/invitations":
            self._handle_invitation_create()
            return
        if parsed.path == "/api/invitations/validate":
            self._handle_invitation_validate()
            return
        if parsed.path == "/api/invitations/session/restore":
            self._handle_invitation_restore()
            return
        if parsed.path == "/api/invitations/session/clear":
            self._handle_invitation_clear()
            return
        if parsed.path == "/api/invitations/disable":
            self._handle_invitation_disable()
            return
        self._send_json(404, {"error": "Not found"})

    def _read_json_body(self):
        try:
            content_length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(content_length)
            return json.loads(raw.decode("utf-8")), None
        except (ValueError, json.JSONDecodeError):
            return None, {"error": "Invalid JSON body"}

    def _read_bearer_token(self):
        auth = self.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            return auth.removeprefix("Bearer ").strip()
        return self.headers.get("X-API-Key", "").strip()

    def _require_import_auth(self):
        if not IMPORT_API_KEY:
            self._send_json(500, {"error": "Import API key is not configured on the server"})
            return False
        if self._read_bearer_token() != IMPORT_API_KEY:
            self._send_json(401, {"error": "Unauthorized"})
            return False
        return True

    def _handle_invitation_validate(self):
        payload, error = self._read_json_body()
        if error:
            self._send_json(400, error)
            return

        submission_id = str(payload.get("submissionId", "")).strip()
        conn = get_db()
        try:
            ok, result = claim_invitation_code(conn, payload.get("code"), submission_id)
            if not ok:
                conn.rollback()
                self._send_json(403, {"ok": False, "error": result})
                return
            remember_token = make_remember_token()
            conn.execute(
                """
                UPDATE invitation_codes
                SET remember_token = ?, updated_at = ?
                WHERE code = ?
                """,
                (remember_token, now_iso(), result["code"]),
            )
            conn.commit()
        finally:
            conn.close()

        self._send_json(
            200,
            {"ok": True, "code": result["code"]},
            extra_headers=[
                ("Set-Cookie", f"serenzer_invite={remember_token}; Path=/; Max-Age=31536000; SameSite=Lax; HttpOnly")
            ],
        )

    def _handle_invitation_list(self):
        conn = get_db()
        try:
            items = list_invitation_codes(conn)
        finally:
            conn.close()
        self._send_json(200, {"count": len(items), "items": items})

    def _handle_invitation_import(self):
        if not self._require_import_auth():
            return

        payload, error = self._read_json_body()
        if error:
            self._send_json(400, error)
            return

        entries = payload.get("codes")
        if entries is None and payload.get("code"):
            entries = [payload]
        if not isinstance(entries, list):
            self._send_json(400, {"error": "codes must be a list of code strings or objects"})
            return

        conn = get_db()
        try:
            imported = import_invitation_entries(conn, entries)
            conn.commit()
            items = list_invitation_codes(conn)
        finally:
            conn.close()

        self._send_json(200, {"ok": True, "imported": imported, "items": items})

    def _handle_invitation_create(self):
        payload, error = self._read_json_body()
        if error:
            self._send_json(400, error)
            return

        raw_codes = payload.get("codes")
        if isinstance(raw_codes, str):
            raw_codes = [part for part in raw_codes.replace("\n", ",").split(",")]
        if not isinstance(raw_codes, list):
            self._send_json(400, {"error": "codes must be a list or comma-separated string"})
            return

        conn = get_db()
        try:
            created = upsert_invitation_codes(conn, raw_codes)
            conn.commit()
            items = list_invitation_codes(conn)
        finally:
            conn.close()

        self._send_json(200, {"ok": True, "created": created, "items": items})

    def _handle_invitation_disable(self):
        payload, error = self._read_json_body()
        if error:
            self._send_json(400, error)
            return

        conn = get_db()
        try:
            ok = disable_invitation_code(conn, payload.get("code"))
            if not ok:
                conn.rollback()
                self._send_json(404, {"ok": False, "error": "Invitation code not found"})
                return
            conn.commit()
            items = list_invitation_codes(conn)
        finally:
            conn.close()

        self._send_json(200, {"ok": True, "items": items})

    def _handle_invitation_restore(self):
        payload, error = self._read_json_body()
        if error:
            self._send_json(400, error)
            return

        submission_id = str(payload.get("submissionId", "")).strip()
        remember_token = self._get_cookie("serenzer_invite")
        conn = get_db()
        try:
            ok, result = restore_invitation_session(conn, remember_token, submission_id)
            if not ok:
                conn.rollback()
                self._send_json(404, {"ok": False, "error": result})
                return
            conn.commit()
        finally:
            conn.close()

        self._send_json(200, {"ok": True, "code": result})

    def _handle_invitation_clear(self):
        self._send_json(
            200,
            {"ok": True},
            extra_headers=[
                ("Set-Cookie", "serenzer_invite=; Path=/; Max-Age=0; SameSite=Lax; HttpOnly")
            ],
        )

    def _handle_feedback_upsert(self):
        payload, error = self._read_json_body()
        if error:
            self._send_json(400, error)
            return

        submission_id = str(payload.get("submissionId", "")).strip()
        if not submission_id:
            self._send_json(400, {"error": "submissionId is required"})
            return

        onboarding = payload.get("onboarding") or {}
        tools = payload.get("tools") or {}
        completed_tabs = payload.get("completedTabs") or []
        timestamp = now_iso()
        invitation_number = normalize_code(onboarding.get("invitationNumber"))

        if not invitation_number:
            self._send_json(403, {"error": "A valid invitation code is required"})
            return

        conn = get_db()
        try:
            ok, result = claim_invitation_code(conn, invitation_number, submission_id)
            if not ok:
                conn.rollback()
                self._send_json(403, {"error": result})
                return

            existing_row = conn.execute(
                """
                SELECT lang, is_complete, email, completed_tabs_json, onboarding_json, tools_json, payload_json
                FROM feedback_submissions
                WHERE submission_id = ?
                """,
                (submission_id,),
            ).fetchone()

            existing_completed_tabs = json.loads(existing_row["completed_tabs_json"]) if existing_row else []
            existing_onboarding = json.loads(existing_row["onboarding_json"]) if existing_row else {}
            existing_tools = json.loads(existing_row["tools_json"]) if existing_row else {}
            existing_payload = json.loads(existing_row["payload_json"]) if existing_row else {}

            merged_onboarding = merge_saved_value(existing_onboarding, onboarding)
            merged_tools = merge_saved_value(existing_tools, tools)
            merged_completed_tabs = sorted({*existing_completed_tabs, *[int(tab) for tab in completed_tabs]})
            merged_payload = merge_saved_value(existing_payload, payload)
            merged_lang = payload.get("lang") or existing_payload.get("lang") or (existing_row["lang"] if existing_row else None)
            merged_is_complete = bool(payload.get("isComplete") or (existing_row["is_complete"] if existing_row else 0))
            merged_payload["submissionId"] = submission_id
            merged_payload["lang"] = merged_lang
            merged_payload["onboarding"] = merged_onboarding
            merged_payload["tools"] = merged_tools
            merged_payload["completedTabs"] = merged_completed_tabs
            merged_payload["isComplete"] = merged_is_complete

            conn.execute(
                """
                INSERT INTO feedback_submissions (
                    submission_id,
                    created_at,
                    updated_at,
                    lang,
                    is_complete,
                    email,
                    invitation_number,
                    completed_tabs_json,
                    onboarding_json,
                    tools_json,
                    payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(submission_id) DO UPDATE SET
                    updated_at = excluded.updated_at,
                    lang = excluded.lang,
                    is_complete = excluded.is_complete,
                    email = excluded.email,
                    invitation_number = excluded.invitation_number,
                    completed_tabs_json = excluded.completed_tabs_json,
                    onboarding_json = excluded.onboarding_json,
                    tools_json = excluded.tools_json,
                    payload_json = excluded.payload_json
                """,
                (
                    submission_id,
                    timestamp,
                    timestamp,
                    merged_lang,
                    1 if merged_is_complete else 0,
                    merged_onboarding.get("email"),
                    result["code"],
                    json.dumps(merged_completed_tabs, ensure_ascii=False),
                    json.dumps(merged_onboarding, ensure_ascii=False),
                    json.dumps(merged_tools, ensure_ascii=False),
                    json.dumps(merged_payload, ensure_ascii=False),
                ),
            )
            conn.commit()
        finally:
            conn.close()

        self._send_json(200, {"ok": True, "submissionId": submission_id})

    def _handle_feedback_list(self):
        conn = get_db()
        try:
            items = list_feedback_entries(conn)
        finally:
            conn.close()
        self._send_json(200, {"count": len(items), "items": items})

    def _handle_bug_report_list(self):
        conn = get_db()
        try:
            items = list_bug_reports(conn)
        finally:
            conn.close()
        self._send_json(200, {"count": len(items), "items": items})

    def _handle_bug_report_create(self):
        payload, error = self._read_json_body()
        if error:
            self._send_json(400, error)
            return

        conn = get_db()
        try:
            ok, result = create_bug_report(conn, payload)
            if not ok:
                conn.rollback()
                self._send_json(400, {"error": result})
                return
            conn.commit()
        finally:
            conn.close()

        self._send_json(200, {"ok": True, "createdAt": result})

    def _handle_feedback_detail(self, submission_id):
        if not submission_id:
            self._send_json(400, {"error": "submissionId is required"})
            return

        if submission_id.startswith("ghost:"):
            conn = get_db()
            try:
                detail = get_ghost_feedback_detail(conn, submission_id.removeprefix("ghost:"))
            finally:
                conn.close()
            if detail is None:
                self._send_json(404, {"error": "Submission not found"})
                return
            self._send_json(200, detail)
            return

        conn = get_db()
        try:
            row = conn.execute(
                """
                SELECT submission_id, created_at, updated_at, lang, is_complete, email,
                       invitation_number, completed_tabs_json, onboarding_json, tools_json, payload_json
                FROM feedback_submissions
                WHERE submission_id = ?
                """,
                (submission_id,),
            ).fetchone()
        finally:
            conn.close()

        if row is None:
            self._send_json(404, {"error": "Submission not found"})
            return

        self._send_json(
            200,
            {
                "submissionId": row["submission_id"],
                "createdAt": row["created_at"],
                "updatedAt": row["updated_at"],
                "lang": row["lang"],
                "isComplete": bool(row["is_complete"]),
                "email": row["email"],
                "invitationNumber": row["invitation_number"],
                "completedTabs": json.loads(row["completed_tabs_json"]),
                "onboarding": json.loads(row["onboarding_json"]),
                "tools": json.loads(row["tools_json"]),
                "payload": json.loads(row["payload_json"]),
            },
        )

    def _get_cookie(self, name):
        cookie = SimpleCookie()
        cookie.load(self.headers.get("Cookie", ""))
        morsel = cookie.get(name)
        return morsel.value if morsel else None

    def _send_json(self, status_code, body, extra_headers=None):
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Pragma", "no-cache")
        for header_name, header_value in (extra_headers or []):
            self.send_header(header_name, header_value)
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format, *args):
        return


def main():
    server = ThreadingHTTPServer((HOST, PORT), FeedbackHandler)
    print(f"Serenzer feedback API listening on http://{HOST}:{PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
