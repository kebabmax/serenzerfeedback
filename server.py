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


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def normalize_code(value):
    return str(value or "").strip().upper()


def make_remember_token():
    return secrets.token_urlsafe(32)


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
    existing_columns = {row["name"] for row in conn.execute("PRAGMA table_info(invitation_codes)").fetchall()}
    if "remember_token" not in existing_columns:
        conn.execute("ALTER TABLE invitation_codes ADD COLUMN remember_token TEXT")
    return conn


def claim_invitation_code(conn, code, submission_id):
    normalized = normalize_code(code)
    if not normalized or not submission_id:
        return False, "Invitation code is required"

    row = conn.execute(
        """
        SELECT code, is_active, bound_submission_id, use_count
        FROM invitation_codes
        WHERE code = ?
        """,
        (normalized,),
    ).fetchone()

    if row is None or not row["is_active"]:
        return False, "Invalid invitation code"

    if row["bound_submission_id"] and row["bound_submission_id"] != submission_id:
        return False, "This invitation code is already in use"

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
    return True, normalized


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
        SELECT code, created_at, updated_at, is_active, bound_submission_id, use_count, used_at
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
        }
        for row in rows
    ]


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
            INSERT INTO invitation_codes (code, created_at, updated_at, is_active)
            VALUES (?, ?, ?, 1)
            ON CONFLICT(code) DO UPDATE SET
                updated_at = excluded.updated_at,
                is_active = 1
            """,
            (code, timestamp, timestamp),
        )
    return normalized_codes


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
        if parsed.path == "/api/invitations":
            self._handle_invitation_list()
            return
        if parsed.path == "/api/invitations/session":
            self._send_json(405, {"error": "Use POST"})
            return
        if parsed.path == "/api/feedback":
            self._handle_feedback_list()
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
                (remember_token, now_iso(), result),
            )
            conn.commit()
        finally:
            conn.close()

        self._send_json(
            200,
            {"ok": True, "code": result},
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
                    payload.get("lang"),
                    1 if payload.get("isComplete") else 0,
                    onboarding.get("email"),
                    result,
                    json.dumps(completed_tabs, ensure_ascii=False),
                    json.dumps(onboarding, ensure_ascii=False),
                    json.dumps(tools, ensure_ascii=False),
                    json.dumps(payload, ensure_ascii=False),
                ),
            )
            conn.commit()
        finally:
            conn.close()

        self._send_json(200, {"ok": True, "submissionId": submission_id})

    def _handle_feedback_list(self):
        conn = get_db()
        try:
            rows = conn.execute(
                """
                SELECT submission_id, created_at, updated_at, lang, is_complete, email, invitation_number
                FROM feedback_submissions
                ORDER BY updated_at DESC
                """
            ).fetchall()
        finally:
            conn.close()

        items = [
            {
                "submissionId": row["submission_id"],
                "createdAt": row["created_at"],
                "updatedAt": row["updated_at"],
                "lang": row["lang"],
                "isComplete": bool(row["is_complete"]),
                "email": row["email"],
                "invitationNumber": row["invitation_number"],
            }
            for row in rows
        ]
        self._send_json(200, {"count": len(items), "items": items})

    def _handle_feedback_detail(self, submission_id):
        if not submission_id:
            self._send_json(400, {"error": "submissionId is required"})
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
