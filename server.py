#!/usr/bin/env python3
import json
import os
import sqlite3
from datetime import datetime, timezone
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
    return conn


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
        if parsed.path != "/api/feedback":
            self._send_json(404, {"error": "Not found"})
            return
        self._handle_feedback_upsert()

    def _handle_feedback_upsert(self):
        try:
            content_length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(content_length)
            payload = json.loads(raw.decode("utf-8"))
        except (ValueError, json.JSONDecodeError):
            self._send_json(400, {"error": "Invalid JSON body"})
            return

        submission_id = str(payload.get("submissionId", "")).strip()
        if not submission_id:
            self._send_json(400, {"error": "submissionId is required"})
            return

        onboarding = payload.get("onboarding") or {}
        tools = payload.get("tools") or {}
        completed_tabs = payload.get("completedTabs") or []
        timestamp = now_iso()

        conn = get_db()
        try:
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
                    onboarding.get("invitationNumber"),
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

    def _send_json(self, status_code, body):
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
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
