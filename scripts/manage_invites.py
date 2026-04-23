#!/usr/bin/env python3
import argparse
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "feedback.sqlite3"


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def normalize_code(value):
    return str(value or "").strip().upper()


def get_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
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
    return conn


def cmd_add(args):
    conn = get_db()
    timestamp = now_iso()
    try:
      for raw_code in args.codes:
          code = normalize_code(raw_code)
          if not code:
              continue
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
      conn.commit()
    finally:
      conn.close()
    print("Added/updated codes:", ", ".join(normalize_code(code) for code in args.codes))


def cmd_list(_args):
    conn = get_db()
    try:
      rows = conn.execute(
          """
          SELECT code, is_active, bound_submission_id, use_count, used_at, updated_at
          FROM invitation_codes
          ORDER BY code
          """
      ).fetchall()
    finally:
      conn.close()

    for row in rows:
      print(
          f"{row['code']} | active={row['is_active']} | bound={row['bound_submission_id'] or '-'} "
          f"| uses={row['use_count']} | used_at={row['used_at'] or '-'} | updated_at={row['updated_at']}"
      )


def cmd_disable(args):
    conn = get_db()
    try:
      conn.execute(
          "UPDATE invitation_codes SET is_active = 0, updated_at = ? WHERE code = ?",
          (now_iso(), normalize_code(args.code)),
      )
      conn.commit()
    finally:
      conn.close()
    print("Disabled:", normalize_code(args.code))


def main():
    parser = argparse.ArgumentParser(description="Manage Serenzer feedback invitation codes")
    sub = parser.add_subparsers(dest="command", required=True)

    add_parser = sub.add_parser("add", help="Add or reactivate invitation codes")
    add_parser.add_argument("codes", nargs="+")
    add_parser.set_defaults(func=cmd_add)

    list_parser = sub.add_parser("list", help="List invitation codes")
    list_parser.set_defaults(func=cmd_list)

    disable_parser = sub.add_parser("disable", help="Disable one invitation code")
    disable_parser.add_argument("code")
    disable_parser.set_defaults(func=cmd_disable)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
