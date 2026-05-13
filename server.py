#!/usr/bin/env python3
import json
import os
import re
import secrets
import sqlite3
import textwrap
import threading
import urllib.error
import urllib.request
from datetime import datetime, timezone
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "feedback.sqlite3"
HOST = os.environ.get("SERENZER_HOST", "127.0.0.1")
PORT = int(os.environ.get("SERENZER_PORT", "8000"))
IMPORT_API_KEY = os.environ.get("SERENZER_IMPORT_API_KEY", "").strip()
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
ANALYSIS_DEFAULT_MODEL = os.environ.get("SERENZER_ANALYSIS_MODEL", "gpt-5.5").strip() or "gpt-5.5"
DB_INIT_LOCK = threading.Lock()
DB_INITIALIZED = False


def now_iso():
    return datetime.now(timezone.utc).isoformat()


DEFAULT_ANALYSIS_PROMPT = """You are Serenzer's daily beta feedback analyst.

Your job is to read the supplied tester snapshot and produce one clear daily report for the product team.

Rules:
- Be concrete, evidence-led, and skeptical.
- Prioritize repeated patterns over one-off noise.
- Treat direct bug reports and repeated confusion as higher signal than isolated preferences.
- Call out uncertainty when evidence is thin.
- Prefer plain language over consultant language.
- Focus on what the team should learn and do next.
- If there is tension in the data, name it explicitly.
- Do not invent facts that are not present in the snapshot.

The resulting report should help the team answer:
1. What is going well?
2. What is breaking or confusing people?
3. Which tools/pages are strongest or weakest?
4. What should we change tomorrow?
5. How should we update our tester-facing communication or prompt next?
"""


ANALYSIS_REPORT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "reportDate",
        "overallHealth",
        "executiveSummary",
        "keySignals",
        "wins",
        "frictions",
        "toolInsights",
        "urgentBugs",
        "userRequests",
        "tomorrowActions",
        "promptAdjustments",
        "notableQuotes",
    ],
    "properties": {
        "reportDate": {"type": "string"},
        "overallHealth": {"type": "string", "enum": ["strong", "mixed", "at-risk"]},
        "executiveSummary": {"type": "string"},
        "keySignals": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["title", "severity", "insight", "evidence"],
                "properties": {
                    "title": {"type": "string"},
                    "severity": {"type": "string", "enum": ["high", "medium", "low"]},
                    "insight": {"type": "string"},
                    "evidence": {"type": "string"},
                },
            },
        },
        "wins": {"type": "array", "items": {"type": "string"}},
        "frictions": {"type": "array", "items": {"type": "string"}},
        "toolInsights": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["tool", "sentiment", "insight", "recommendation"],
                "properties": {
                    "tool": {"type": "string"},
                    "sentiment": {"type": "string", "enum": ["positive", "mixed", "negative"]},
                    "insight": {"type": "string"},
                    "recommendation": {"type": "string"},
                },
            },
        },
        "urgentBugs": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["title", "impact", "evidence"],
                "properties": {
                    "title": {"type": "string"},
                    "impact": {"type": "string"},
                    "evidence": {"type": "string"},
                },
            },
        },
        "userRequests": {"type": "array", "items": {"type": "string"}},
        "tomorrowActions": {"type": "array", "items": {"type": "string"}},
        "promptAdjustments": {"type": "array", "items": {"type": "string"}},
        "notableQuotes": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["quote", "context"],
                "properties": {
                    "quote": {"type": "string"},
                    "context": {"type": "string"},
                },
            },
        },
    },
}


def normalize_code(value):
    return str(value or "").strip().upper()


def normalize_name(value):
    return " ".join(str(value or "").strip().lower().split())


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


def _initialize_db(conn):
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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS activity_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            action TEXT NOT NULL,
            request_path TEXT,
            request_method TEXT,
            client_ip TEXT,
            submission_id TEXT,
            invitation_number TEXT,
            email TEXT,
            first_name TEXT,
            last_name TEXT,
            lang TEXT,
            active_tab INTEGER,
            active_tab_label TEXT,
            details_json TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS participant_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            invitation_number TEXT NOT NULL,
            submission_id TEXT,
            author TEXT,
            message TEXT NOT NULL,
            is_dismissed INTEGER NOT NULL DEFAULT 0,
            dismissed_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS analysis_config (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            updated_at TEXT NOT NULL,
            model TEXT NOT NULL,
            prompt_text TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS analysis_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            report_date TEXT NOT NULL,
            model TEXT NOT NULL,
            prompt_text TEXT NOT NULL,
            source_snapshot_json TEXT NOT NULL,
            report_json TEXT NOT NULL,
            usage_json TEXT,
            error_text TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO analysis_config (id, updated_at, model, prompt_text)
        VALUES (1, ?, ?, ?)
        ON CONFLICT(id) DO NOTHING
        """,
        (now_iso(), ANALYSIS_DEFAULT_MODEL, DEFAULT_ANALYSIS_PROMPT),
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
    participant_message_columns = {row["name"] for row in conn.execute("PRAGMA table_info(participant_messages)").fetchall()}
    if "invitation_number" not in participant_message_columns:
        conn.execute("ALTER TABLE participant_messages ADD COLUMN invitation_number TEXT")
        if "invitation_code" in participant_message_columns:
            conn.execute(
                """
                UPDATE participant_messages
                SET invitation_number = invitation_code
                WHERE invitation_number IS NULL OR invitation_number = ''
                """
            )
    if "submission_id" not in participant_message_columns:
        conn.execute("ALTER TABLE participant_messages ADD COLUMN submission_id TEXT")
    if "message" not in participant_message_columns:
        conn.execute("ALTER TABLE participant_messages ADD COLUMN message TEXT")
        if "body" in participant_message_columns:
            conn.execute(
                """
                UPDATE participant_messages
                SET message = body
                WHERE message IS NULL OR message = ''
                """
            )
    if "updated_at" not in participant_message_columns:
        conn.execute("ALTER TABLE participant_messages ADD COLUMN updated_at TEXT")
        conn.execute(
            """
            UPDATE participant_messages
            SET updated_at = created_at
            WHERE updated_at IS NULL OR updated_at = ''
            """
        )
    if "author" not in participant_message_columns:
        conn.execute("ALTER TABLE participant_messages ADD COLUMN author TEXT")
    if "sender_role" not in participant_message_columns:
        conn.execute("ALTER TABLE participant_messages ADD COLUMN sender_role TEXT")
    if "is_dismissed" not in participant_message_columns:
        conn.execute("ALTER TABLE participant_messages ADD COLUMN is_dismissed INTEGER NOT NULL DEFAULT 0")
    if "dismissed_at" not in participant_message_columns:
        conn.execute("ALTER TABLE participant_messages ADD COLUMN dismissed_at TEXT")
    participant_message_columns = {row["name"] for row in conn.execute("PRAGMA table_info(participant_messages)").fetchall()}
    if "invitation_code" in participant_message_columns:
        conn.execute(
            """
            UPDATE participant_messages
            SET invitation_number = COALESCE(NULLIF(invitation_number, ''), invitation_code)
            WHERE invitation_number IS NULL OR invitation_number = ''
            """
        )
    if "body" in participant_message_columns:
        conn.execute(
            """
            UPDATE participant_messages
            SET message = COALESCE(NULLIF(message, ''), body)
            WHERE message IS NULL OR message = ''
            """
        )
    conn.execute(
        """
        UPDATE participant_messages
        SET sender_role = CASE
            WHEN sender_role IS NOT NULL AND sender_role != '' THEN sender_role
            WHEN lower(COALESCE(author, '')) IN ('serenzer team', 'serenzer') THEN 'admin'
            ELSE 'admin'
        END
        """
    )
    conn.commit()


def get_db():
    global DB_INITIALIZED
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    if not DB_INITIALIZED:
        with DB_INIT_LOCK:
            if not DB_INITIALIZED:
                _initialize_db(conn)
                DB_INITIALIZED = True
    return conn


def get_feedback_detail_row(conn, submission_id):
    if not submission_id:
        return None
    return conn.execute(
        """
        SELECT submission_id, created_at, updated_at, lang, is_complete, email,
               invitation_number, completed_tabs_json, onboarding_json, tools_json, payload_json
        FROM feedback_submissions
        WHERE submission_id = ?
        """,
        (submission_id,),
    ).fetchone()


def feedback_detail_from_row(row):
    if row is None:
        return None
    return {
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
    }


ONBOARDING_FIELDS = [
    ("firstName", "First name"),
    ("lastName", "Last name"),
    ("email", "Email"),
    ("invitationNumber", "Invitation number"),
    ("tech", "Tech level"),
    ("devices", "Devices"),
    ("time", "Time available"),
    ("ageRange", "Age range"),
    ("gender", "Gender"),
    ("workSituation", "Work situation"),
    ("livingSituation", "Living situation"),
    ("familySituation", "Family situation"),
    ("hopes", "Expectations"),
]


TOOL_LABELS = {
    "tableauIntro": "Dashboard pt. 1",
    "coach": "My Coach",
    "calendrier": "Calendar",
    "rituels": "Rituals",
    "challenges": "Challenges",
    "organisation": "Organisation",
    "progression": "Progress",
    "tableauFinal": "Dashboard pt. 2",
    "finalThoughts": "Final thoughts",
    "closing": "Finish",
}


TOOL_FIELDS = {
    "tableauIntro": [
        ("dateTimeCorrect", "Are the date and time displayed correctly?"),
        ("greetingMatchesTime", "Is the welcome sentence consistent with the time of day?"),
        ("pageDetailsSense", "Do the details on this page make sense to you?"),
        ("startConversationWorks", "Does the \"Start a conversation\" button work properly?"),
    ],
    "coach": [
        ("coachAction1Done", "Start a text conversation with The AI"),
        ("coachAction2Done", "Talk about a real personal topic"),
        ("coachAction3Done", "Complete a session of at least 10 minutes"),
        ("coachObs1Answer", "The AI understands what I say well"),
        ("coachObs1Comment", "Comment - The AI understands what I say well"),
        ("coachObs2Answer", "The responses are relevant"),
        ("coachObs2Comment", "Comment - The responses are relevant"),
        ("coachObs3Answer", "The tone is appropriate and caring"),
        ("coachObs3Comment", "Comment - The tone is appropriate and caring"),
        ("coachObs4Answer", "The conversation feels natural"),
        ("coachObs4Comment", "Comment - The conversation feels natural"),
        ("coachObs5Answer", "I feel comfortable opening up"),
        ("coachObs5Comment", "Comment - I feel comfortable opening up"),
    ],
    "calendrier": [
        ("calendarGoogleAddDone", "Add your Google Calendar."),
        ("calendarGoogleShow", "Do your events show on the Serenzer Calendar?"),
        ("calendarSerenzerClickDone", "Click on a date in the calendar."),
        ("calendarSerenzerCreateDone", "Create a new event."),
        ("calendarSerenzerShow", "Does the event show on the Serenzer Calendar?"),
        ("calendarGoogleSync", "Does the event show on your Google Calendar?"),
    ],
    "rituels": [
        ("ritualNewActionDone", "Create a new ritual."),
        ("ritualNewWorks", "Does the \"New Ritual\" form work?"),
        ("ritualNewSense", "Does the \"New Ritual\" form make sense to you?"),
        ("ritualNewCalendar", "Is the new ritual added to your calendar?"),
        ("ritualNewMine", "Is the new ritual added to \"My Rituals\"?"),
        ("ritualAiOpenDone", "Click on \"AI Suggestions\"."),
        ("ritualAiLoad", "Do the suggestions load?"),
        ("ritualAiSense", "Do the suggestions make sense to you?"),
        ("ritualAiAcceptDone", "Accept one of the suggestions."),
        ("ritualAiCalendar", "Is the new ritual added to your calendar?"),
        ("ritualAiMine", "Is the new ritual added to \"My Rituals\"?"),
    ],
    "challenges": [
        ("challengesOpenDone", "Open the Challenges page."),
        ("challengesJoinDone", "Join a challenge or inspect one in detail."),
        ("challengesRelevant", "Do the available challenges feel relevant to you?"),
        ("challengesClear", "Is it clear how to participate in a challenge?"),
        ("challengesProgress", "Does the progress / reward system make sense to you?"),
    ],
    "organisation": [
        ("organisationProjectCreateDone", "Create a new Project."),
        ("organisationProjectChooseDone", "Choose \"Organise my week\" and name your Project."),
        ("organisationProjectWeekDone", "Select a week."),
        ("organisationProjectAskDone", "Ask about that week."),
        ("organisationProjectCalendar", "Does the AI mention the events of your connected calendar?"),
        ("organisationUseTellDone", "Tell the AI what you would like your week to look like."),
        ("organisationUseContext", "Does it understand and use context to help you?"),
        ("organisationUseBuildDone", "Build your week with the AI."),
        ("organisationFinaliseWrapDone", "Tell the AI to wrap up the project."),
        ("organisationFinaliseWorks", "Does it work?"),
        ("organisationFinaliseClickDone", "Click on finalise."),
        ("organisationFinaliseReport", "Does it generate a full report on your discussion?"),
        ("organisationFinaliseSense", "Does it make sense?"),
        ("organisationFinaliseCalendar", "Does it add your new plans to your calendar?"),
    ],
    "progression": [
        ("progressionOpenDone", "Open the Progress page."),
        ("progressionLoad", "Do the statistics and visuals load correctly?"),
        ("progressionCoherent", "Do the statistics seem coherent with what you have done in the app so far?"),
        ("progressionSense", "Do the badges, graphs, or indicators make sense to you?"),
        ("progressionMotivate", "Does this page motivate you to keep using Serenzer?"),
    ],
    "tableauFinal": [
        ("dashboardFinalReturnDone", "Return to the dashboard after exploring the app."),
        ("dashboardFinalPersonal", "Do your personal details now appear correctly on the dashboard?"),
        ("dashboardFinalUseful", "Does the dashboard feel richer and more useful than at the beginning?"),
        ("dashboardFinalSense", "Is the information displayed easy to understand?"),
        ("dashboardFinalReflect", "Does the dashboard now reflect your activity in the app in a convincing way?"),
    ],
    "finalThoughts": [
        ("finalThoughtsEssay", "Final thoughts"),
    ],
    "generic": [
        ("intuitive", "Intuitivity rating"),
        ("useful", "Usefulness rating"),
        ("genericToolSense", "Does this tool make sense to you?"),
        ("experience", "Experience tags"),
        ("description", "Describe your experience"),
        ("suggestions", "Suggestions or improvement ideas"),
    ],
}


def humanize_key(key):
    value = re.sub(r"([A-Z])", r" \1", str(key or ""))
    value = value.replace("_", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value[:1].upper() + value[1:] if value else ""


def format_answer_value(value):
    if value is True:
        return "Done"
    if value is False:
        return "Not done"
    if value == "yes":
        return "Yes"
    if value == "no":
        return "No"
    if value == "maybe":
        return "?"
    if value is None or value == "":
        return "—"
    if isinstance(value, list):
        return ", ".join(str(item) for item in value) if value else "—"
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, indent=2)
    return str(value)


def collect_labeled_items(data, fields):
    if not isinstance(data, dict):
        return []
    handled = set()
    items = []
    for key, label in fields:
        if key not in data:
            continue
        handled.add(key)
        items.append((label, format_answer_value(data.get(key))))
    for key, value in data.items():
        if key in handled:
            continue
        items.append((humanize_key(key), format_answer_value(value)))
    return items


def build_feedback_report_sections(detail):
    onboarding = detail.get("onboarding") or {}
    tools = detail.get("tools") or {}
    completed_tabs = detail.get("completedTabs") or []

    overview_items = [
        ("Submission ID", detail.get("submissionId") or "—"),
        ("Email", detail.get("email") or "—"),
        ("Invitation #", detail.get("invitationNumber") or "—"),
        ("Language", (detail.get("lang") or "—").upper()),
        ("Status", "Complete" if detail.get("isComplete") else "In progress"),
        ("Completed tabs", ", ".join(str(item) for item in completed_tabs) if completed_tabs else "—"),
        ("Created", detail.get("createdAt") or "—"),
        ("Updated", detail.get("updatedAt") or "—"),
    ]

    sections = [
        ("Overview", overview_items),
        ("Onboarding", collect_labeled_items(onboarding, ONBOARDING_FIELDS)),
    ]

    tool_sections = []
    for tool_key, value in tools.items():
        if not isinstance(value, dict):
            continue
        fields = list(TOOL_FIELDS.get(tool_key, [])) + list(TOOL_FIELDS["generic"])
        tool_sections.append((TOOL_LABELS.get(tool_key, humanize_key(tool_key)), collect_labeled_items(value, fields)))

    if tool_sections:
        sections.append(("Tool Responses", tool_sections))

    return sections


def make_report_filename(detail):
    parts = [
        detail.get("onboarding", {}).get("lastName") or "",
        detail.get("onboarding", {}).get("firstName") or "",
        detail.get("invitationNumber") or detail.get("submissionId") or "tester",
    ]
    raw = "-".join(part.strip() for part in parts if str(part).strip())
    safe = re.sub(r"[^A-Za-z0-9._-]+", "-", raw).strip("-") or "tester-feedback"
    return f"{safe}.pdf"


def pdf_escape(value):
    text = str(value or "").replace("\r\n", "\n").replace("\r", "\n")
    text = text.encode("cp1252", "replace").decode("cp1252")
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def wrap_pdf_text(value, width):
    lines = []
    for paragraph in str(value or "").split("\n"):
        cleaned = " ".join(paragraph.split())
        if not cleaned:
            lines.append("")
            continue
        lines.extend(textwrap.wrap(cleaned, width=width, break_long_words=True, break_on_hyphens=False) or [""])
    return lines or [""]


def build_pdf_bytes_from_sections(title, subtitle, sections):
    page_width = 595
    page_height = 842
    margin_x = 48
    top_y = 792
    bottom_y = 52
    line_height = 14
    pages = []
    commands = []
    current_y = top_y

    def flush_page():
        nonlocal commands, current_y
        pages.append("\n".join(commands))
        commands = []
        current_y = top_y

    def ensure_space(lines_needed=1, extra=0):
        nonlocal current_y
        needed = lines_needed * line_height + extra
        if current_y - needed < bottom_y:
            flush_page()

    def add_text_line(text, size=10, x=None, font="F1"):
        nonlocal current_y
        if x is None:
            x = margin_x
        commands.append(f"BT /{font} {size} Tf 0 g 1 0 0 1 {x} {current_y} Tm ({pdf_escape(text)}) Tj ET")
        current_y -= line_height

    def add_spacer(height=8):
        nonlocal current_y
        current_y -= height

    def add_rule():
        nonlocal current_y
        commands.append(f"0.82 0.79 0.73 RG {margin_x} {current_y} m {page_width - margin_x} {current_y} l S")
        current_y -= 10

    ensure_space(4)
    add_text_line(title, size=18, font="F2")
    add_text_line(subtitle, size=10)
    add_rule()

    for section_title, section_items in sections:
        if not section_items:
            continue
        if section_title == "Tool Responses":
            ensure_space(2, extra=10)
            add_spacer(4)
            add_text_line(section_title, size=13, font="F2")
            add_spacer(4)
            for tool_title, tool_items in section_items:
                if not tool_items:
                    continue
                ensure_space(2, extra=8)
                add_text_line(tool_title, size=11, font="F2")
                add_spacer(2)
                for label, value in tool_items:
                    value_lines = wrap_pdf_text(value, 82)
                    ensure_space(len(value_lines) + 2, extra=4)
                    add_text_line(f"{label}:", size=10, font="F2")
                    for line in value_lines:
                        add_text_line(line or " ", size=10, x=margin_x + 18)
                    add_spacer(2)
                add_spacer(4)
            add_rule()
            continue

        ensure_space(2, extra=10)
        add_spacer(4)
        add_text_line(section_title, size=13, font="F2")
        add_spacer(4)
        for label, value in section_items:
            value_lines = wrap_pdf_text(value, 82)
            ensure_space(len(value_lines) + 2, extra=4)
            add_text_line(f"{label}:", size=10, font="F2")
            for line in value_lines:
                add_text_line(line or " ", size=10, x=margin_x + 18)
            add_spacer(2)
        add_rule()

    if commands:
        flush_page()
    elif not pages:
        pages.append("")

    objects = []

    def add_object(data):
        objects.append(data)
        return len(objects)

    pages_obj_id = add_object(b"")
    font_regular_id = add_object(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    font_bold_id = add_object(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>")
    page_ids = []

    for content in pages:
        content_bytes = content.encode("latin-1", "replace")
        content_id = add_object(
            f"<< /Length {len(content_bytes)} >>\nstream\n".encode("latin-1")
            + content_bytes
            + b"\nendstream"
        )
        page_id = add_object(
            (
                f"<< /Type /Page /Parent {pages_obj_id} 0 R "
                f"/MediaBox [0 0 {page_width} {page_height}] "
                f"/Resources << /Font << /F1 {font_regular_id} 0 R /F2 {font_bold_id} 0 R >> >> "
                f"/Contents {content_id} 0 R >>"
            ).encode("latin-1")
        )
        page_ids.append(page_id)

    objects[pages_obj_id - 1] = (
        f"<< /Type /Pages /Kids [{' '.join(f'{page_id} 0 R' for page_id in page_ids)}] /Count {len(page_ids)} >>"
    ).encode("latin-1")
    catalog_id = add_object(f"<< /Type /Catalog /Pages {pages_obj_id} 0 R >>".encode("latin-1"))

    output = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]
    for index, obj in enumerate(objects, start=1):
        offsets.append(len(output))
        output.extend(f"{index} 0 obj\n".encode("latin-1"))
        output.extend(obj)
        output.extend(b"\nendobj\n")
    xref_offset = len(output)
    output.extend(f"xref\n0 {len(objects) + 1}\n".encode("latin-1"))
    output.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        output.extend(f"{offset:010d} 00000 n \n".encode("latin-1"))
    output.extend(
        (
            f"trailer\n<< /Size {len(objects) + 1} /Root {catalog_id} 0 R >>\n"
            f"startxref\n{xref_offset}\n%%EOF"
        ).encode("latin-1")
    )
    return bytes(output)


def build_feedback_report_pdf(detail):
    onboarding = detail.get("onboarding") or {}
    title_name = " ".join(part for part in [onboarding.get("firstName"), onboarding.get("lastName")] if part)
    title_name = title_name or (detail.get("email") or detail.get("invitationNumber") or detail.get("submissionId"))
    title = f"Serenzer Feedback Report - {title_name}"
    subtitle = f"Generated on {now_iso()} | Invitation {detail.get('invitationNumber') or '—'} | Lang {(detail.get('lang') or '—').upper()}"
    sections = build_feedback_report_sections(detail)
    return build_pdf_bytes_from_sections(title, subtitle, sections)


def get_onboarding_from_row(row):
    if row is None:
        return {}
    try:
        return json.loads(row["onboarding_json"] or "{}")
    except Exception:
        return {}


def claim_invitation_code(conn, code, submission_id, last_name=None):
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

    existing_submission_id = row["bound_submission_id"]
    existing_submission = None
    if existing_submission_id:
        existing_submission = get_feedback_detail_row(conn, existing_submission_id)
        saved_last_name = normalize_name(get_onboarding_from_row(existing_submission).get("lastName"))
        provided_last_name = normalize_name(last_name)
        if existing_submission_id != submission_id and saved_last_name:
            if not provided_last_name:
                return False, {
                    "error": "This code has already been used. Confirm the last name on file to continue.",
                    "requiresLastName": True,
                    "code": normalized,
                }
            if provided_last_name != saved_last_name:
                return False, {
                    "error": "That last name does not match our records.",
                    "requiresLastName": True,
                    "code": normalized,
                }

    first_activation = row["use_count"] < 1 and not existing_submission_id
    timestamp = now_iso()
    if not existing_submission_id:
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
    else:
        conn.execute(
            """
            UPDATE invitation_codes
            SET updated_at = ?,
                use_count = CASE WHEN use_count < 1 THEN 1 ELSE use_count END,
                used_at = COALESCE(used_at, ?)
            WHERE code = ?
            """,
            (timestamp, timestamp, normalized),
        )
    return True, {
        "code": normalized,
        "firstActivation": first_activation,
        "email": row["email"],
        "appUserId": row["app_user_id"],
        "source": row["source"],
        "submissionId": existing_submission_id or submission_id,
        "existingSubmission": feedback_detail_from_row(existing_submission),
    }


def restore_invitation_session(conn, remember_token, submission_id):
    token = str(remember_token or "").strip()
    if not token or not submission_id:
        return False, "Missing remember token"

    row = conn.execute(
        """
        SELECT code, is_active, bound_submission_id
        FROM invitation_codes
        WHERE remember_token = ?
        """,
        (token,),
    ).fetchone()

    if row is None or not row["is_active"]:
        return False, "No remembered invitation session found"

    existing_submission = get_feedback_detail_row(conn, row["bound_submission_id"]) if row["bound_submission_id"] else None
    timestamp = now_iso()
    if not row["bound_submission_id"]:
        conn.execute(
            """
            UPDATE invitation_codes
            SET updated_at = ?,
                bound_submission_id = ?
            WHERE remember_token = ?
            """,
            (timestamp, submission_id, token),
        )
    else:
        conn.execute(
            """
            UPDATE invitation_codes
            SET updated_at = ?
            WHERE remember_token = ?
            """,
            (timestamp, token),
        )
    return True, {
        "code": row["code"],
        "submissionId": row["bound_submission_id"] or submission_id,
        "existingSubmission": feedback_detail_from_row(existing_submission),
    }


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
        SELECT submission_id, created_at, updated_at, lang, is_complete, email,
               invitation_number, completed_tabs_json, onboarding_json
        FROM feedback_submissions
        ORDER BY updated_at DESC
        """
    ).fetchall()
    items = []
    for row in rows:
        completed_tabs = json.loads(row["completed_tabs_json"])
        onboarding = json.loads(row["onboarding_json"] or "{}")
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
                "tech": onboarding.get("tech"),
                "time": onboarding.get("time"),
                "ageRange": onboarding.get("ageRange"),
                "gender": onboarding.get("gender"),
                "workSituation": onboarding.get("workSituation"),
                "livingSituation": onboarding.get("livingSituation"),
                "familySituation": onboarding.get("familySituation"),
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


def delete_participant_entry(conn, submission_id):
    identifier = str(submission_id or "").strip()
    if not identifier:
        return False, "submissionId is required"

    if identifier.startswith("ghost:"):
        code = normalize_code(identifier.removeprefix("ghost:"))
        if not code:
            return False, "Invalid ghost participant"
        invite_row = conn.execute(
            """
            SELECT code, email, source
            FROM invitation_codes
            WHERE code = ?
            """,
            (code,),
        ).fetchone()
        if invite_row is None:
            return False, "Participant not found"
        conn.execute("DELETE FROM invitation_codes WHERE code = ?", (code,))
        return True, {
            "entryType": "ghost",
            "submissionId": identifier,
            "invitationNumber": code,
            "email": invite_row["email"],
            "source": invite_row["source"],
        }

    row = conn.execute(
        """
        SELECT submission_id, invitation_number, email
        FROM feedback_submissions
        WHERE submission_id = ?
        """,
        (identifier,),
    ).fetchone()
    if row is None:
        return False, "Participant not found"

    invitation_number = normalize_code(row["invitation_number"])
    conn.execute("DELETE FROM feedback_submissions WHERE submission_id = ?", (identifier,))

    if invitation_number:
        conn.execute(
            """
            UPDATE invitation_codes
            SET bound_submission_id = NULL,
                use_count = 0,
                used_at = NULL,
                remember_token = NULL,
                updated_at = ?,
                is_active = 1
            WHERE code = ?
            """,
            (now_iso(), invitation_number),
        )

    return True, {
        "entryType": "submission",
        "submissionId": identifier,
        "invitationNumber": invitation_number,
        "email": row["email"],
    }


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


def create_activity_log(conn, action, payload):
    details = payload.get("details")
    if not isinstance(details, dict):
        details = {}
    conn.execute(
        """
        INSERT INTO activity_logs (
            created_at,
            action,
            request_path,
            request_method,
            client_ip,
            submission_id,
            invitation_number,
            email,
            first_name,
            last_name,
            lang,
            active_tab,
            active_tab_label,
            details_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payload.get("createdAt") or now_iso(),
            str(action or "").strip() or "unknown",
            str(payload.get("requestPath") or "").strip() or None,
            str(payload.get("requestMethod") or "").strip() or None,
            str(payload.get("clientIp") or "").strip() or None,
            str(payload.get("submissionId") or "").strip() or None,
            normalize_code(payload.get("invitationNumber")) or None,
            str(payload.get("email") or "").strip() or None,
            str(payload.get("firstName") or "").strip() or None,
            str(payload.get("lastName") or "").strip() or None,
            str(payload.get("lang") or "").strip() or None,
            int(payload.get("activeTab")) if str(payload.get("activeTab") or "").isdigit() else None,
            str(payload.get("activeTabLabel") or "").strip() or None,
            json.dumps(details, ensure_ascii=False),
        ),
    )


def list_activity_logs(conn):
    rows = conn.execute(
        """
        SELECT id, created_at, action, request_path, request_method, client_ip, submission_id,
               invitation_number, email, first_name, last_name, lang, active_tab,
               active_tab_label, details_json
        FROM activity_logs
        ORDER BY created_at DESC, id DESC
        """
    ).fetchall()
    items = []
    for row in rows:
        details = json.loads(row["details_json"] or "{}")
        items.append(
            {
                "id": row["id"],
                "createdAt": row["created_at"],
                "action": row["action"],
                "requestPath": row["request_path"],
                "requestMethod": row["request_method"],
                "clientIp": row["client_ip"],
                "submissionId": row["submission_id"],
                "invitationNumber": row["invitation_number"],
                "email": row["email"],
                "firstName": row["first_name"],
                "lastName": row["last_name"],
                "lang": row["lang"],
                "activeTab": row["active_tab"],
                "activeTabLabel": row["active_tab_label"],
                "details": details,
            }
        )
    return items


def get_analysis_config(conn):
    row = conn.execute(
        """
        SELECT id, updated_at, model, prompt_text
        FROM analysis_config
        WHERE id = 1
        """
    ).fetchone()
    if row is None:
        return {
            "updatedAt": now_iso(),
            "model": ANALYSIS_DEFAULT_MODEL,
            "promptText": DEFAULT_ANALYSIS_PROMPT,
        }
    return {
        "updatedAt": row["updated_at"],
        "model": row["model"] or ANALYSIS_DEFAULT_MODEL,
        "promptText": row["prompt_text"] or DEFAULT_ANALYSIS_PROMPT,
    }


def format_analysis_report_name(report_date, sequence_number):
    raw = str(report_date or "").strip()
    try:
        dt = datetime.fromisoformat(raw)
        base = dt.strftime("%d-%m-%Y")
    except Exception:
        parts = raw.split("-")
        if len(parts) == 3 and all(part.isdigit() for part in parts):
            year, month, day = parts
            base = f"{day.zfill(2)}-{month.zfill(2)}-{year}"
        else:
            base = raw or "report"
    return f"{base}_{int(sequence_number):03d}"


def update_analysis_config(conn, model, prompt_text):
    cleaned_model = str(model or ANALYSIS_DEFAULT_MODEL).strip() or ANALYSIS_DEFAULT_MODEL
    cleaned_prompt = str(prompt_text or "").strip() or DEFAULT_ANALYSIS_PROMPT
    timestamp = now_iso()
    conn.execute(
        """
        INSERT INTO analysis_config (id, updated_at, model, prompt_text)
        VALUES (1, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            updated_at = excluded.updated_at,
            model = excluded.model,
            prompt_text = excluded.prompt_text
        """,
        (timestamp, cleaned_model, cleaned_prompt),
    )
    return {
        "updatedAt": timestamp,
        "model": cleaned_model,
        "promptText": cleaned_prompt,
    }


def compact_tool_answers(tools):
    if not isinstance(tools, dict):
        return {}
    compact = {}
    for tool_key, value in tools.items():
        if not isinstance(value, dict):
            continue
        fields = list(TOOL_FIELDS.get(tool_key, [])) + list(TOOL_FIELDS["generic"])
        compact_items = []
        for label, answer in collect_labeled_items(value, fields):
            if answer == "—":
                continue
            compact_items.append({"label": label, "answer": answer})
        if compact_items:
            compact[TOOL_LABELS.get(tool_key, humanize_key(tool_key))] = compact_items
    return compact


def build_analysis_snapshot(conn, report_date=None):
    rows = conn.execute(
        """
        SELECT submission_id, created_at, updated_at, lang, is_complete, email,
               invitation_number, completed_tabs_json, onboarding_json, tools_json
        FROM feedback_submissions
        ORDER BY updated_at DESC
        """
    ).fetchall()
    participants = []
    language_counts = {}
    complete_count = 0
    for row in rows:
        onboarding = json.loads(row["onboarding_json"] or "{}")
        tools = json.loads(row["tools_json"] or "{}")
        completed_tabs = json.loads(row["completed_tabs_json"] or "[]")
        lang = row["lang"] or "unknown"
        language_counts[lang] = language_counts.get(lang, 0) + 1
        if row["is_complete"]:
            complete_count += 1
        participants.append(
            {
                "submissionId": row["submission_id"],
                "updatedAt": row["updated_at"],
                "createdAt": row["created_at"],
                "language": lang,
                "status": "complete" if row["is_complete"] else "in_progress",
                "email": row["email"],
                "invitationNumber": row["invitation_number"],
                "completedTabsCount": len(completed_tabs),
                "profile": {
                    "tech": onboarding.get("tech"),
                    "time": onboarding.get("time"),
                    "ageRange": onboarding.get("ageRange"),
                    "gender": onboarding.get("gender"),
                    "workSituation": onboarding.get("workSituation"),
                    "livingSituation": onboarding.get("livingSituation"),
                    "familySituation": onboarding.get("familySituation"),
                },
                "expectations": onboarding.get("hopes"),
                "responses": compact_tool_answers(tools),
            }
        )

    bug_reports = list_bug_reports(conn)
    recent_bug_reports = bug_reports[:40]
    recent_logs = list_activity_logs(conn)[:80]
    invite_codes = list_invitation_codes(conn)
    report_date_value = str(report_date or now_iso()[:10]).strip() or now_iso()[:10]

    return {
        "reportDate": report_date_value,
        "generatedAt": now_iso(),
        "totals": {
            "participants": len(participants),
            "completed": complete_count,
            "inProgress": max(0, len(participants) - complete_count),
            "invitationCodes": len(invite_codes),
            "bugReports": len(bug_reports),
        },
        "languageCounts": language_counts,
        "participants": participants,
        "recentBugReports": recent_bug_reports,
        "recentActivityLogs": recent_logs,
    }


def extract_response_text(response_body):
    if isinstance(response_body.get("output_text"), str) and response_body.get("output_text"):
        return response_body["output_text"]
    for item in response_body.get("output", []) or []:
        if item.get("type") != "message":
            continue
        for content in item.get("content", []) or []:
            text = content.get("text")
            if isinstance(text, str) and text.strip():
                return text
    return ""


def request_openai_analysis(model, prompt_text, snapshot):
    if not OPENAI_API_KEY:
        return False, {"error": "OPENAI_API_KEY is not configured on the server"}

    payload = {
        "model": model,
        "instructions": prompt_text,
        "input": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": (
                            "Analyze this Serenzer beta feedback snapshot and return a structured JSON report.\n\n"
                            f"Snapshot:\n{json.dumps(snapshot, ensure_ascii=False)}"
                        ),
                    }
                ],
            }
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "daily_feedback_analysis",
                "schema": ANALYSIS_REPORT_SCHEMA,
                "strict": True,
            }
        },
        "max_output_tokens": 4000,
    }
    request = urllib.request.Request(
        "https://api.openai.com/v1/responses",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=180) as response:
            body = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        try:
            detail = error.read().decode("utf-8")
        except Exception:
            detail = str(error)
        return False, {"error": f"OpenAI request failed ({error.code})", "details": detail}
    except Exception as error:
        return False, {"error": f"OpenAI request failed: {error}"}

    text = extract_response_text(body)
    if not text:
        return False, {"error": "OpenAI returned no analysis text", "details": body}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return False, {"error": "OpenAI returned invalid JSON", "details": text}
    return True, {
        "report": parsed,
        "usage": body.get("usage") or {},
        "rawResponseId": body.get("id"),
    }


def create_analysis_report(conn, report_date, model, prompt_text, snapshot, report_json, usage=None, error_text=None):
    timestamp = now_iso()
    cursor = conn.execute(
        """
        INSERT INTO analysis_reports (
            created_at,
            report_date,
            model,
            prompt_text,
            source_snapshot_json,
            report_json,
            usage_json,
            error_text
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            timestamp,
            str(report_date or "").strip() or timestamp[:10],
            str(model or ANALYSIS_DEFAULT_MODEL).strip() or ANALYSIS_DEFAULT_MODEL,
            str(prompt_text or "").strip() or DEFAULT_ANALYSIS_PROMPT,
            json.dumps(snapshot, ensure_ascii=False),
            json.dumps(report_json, ensure_ascii=False),
            json.dumps(usage or {}, ensure_ascii=False) if usage is not None else None,
            str(error_text or "").strip() or None,
        ),
    )
    return cursor.lastrowid


def list_analysis_reports(conn):
    rows = conn.execute(
        """
        SELECT id, created_at, report_date, model, report_json, usage_json, error_text
        FROM analysis_reports
        ORDER BY report_date DESC, created_at DESC, id DESC
        """
    ).fetchall()
    sequence_by_date = {}
    items = []
    for row in reversed(rows):
        report_date = row["report_date"]
        sequence_by_date[report_date] = sequence_by_date.get(report_date, 0) + 1
    for row in rows:
        report = json.loads(row["report_json"] or "{}")
        usage = json.loads(row["usage_json"] or "{}") if row["usage_json"] else {}
        report_date = row["report_date"]
        sequence = sequence_by_date.get(report_date, 1)
        items.append(
            {
                "id": row["id"],
                "createdAt": row["created_at"],
                "reportDate": report_date,
                "reportName": format_analysis_report_name(report_date, sequence),
                "model": row["model"],
                "overallHealth": report.get("overallHealth"),
                "executiveSummary": report.get("executiveSummary"),
                "usage": usage,
                "error": row["error_text"],
            }
        )
        sequence_by_date[report_date] = max(1, sequence - 1)
    return items


def get_analysis_report(conn, report_id):
    row = conn.execute(
        """
        SELECT id, created_at, report_date, model, prompt_text, source_snapshot_json, report_json, usage_json, error_text
        FROM analysis_reports
        WHERE id = ?
        """,
        (int(report_id),),
    ).fetchone()
    if row is None:
        return None
    report_date = row["report_date"]
    sequence_row = conn.execute(
        """
        SELECT COUNT(*) AS seq
        FROM analysis_reports
        WHERE report_date = ?
          AND (
            created_at < ?
            OR (created_at = ? AND id <= ?)
          )
        """,
        (report_date, row["created_at"], row["created_at"], int(report_id)),
    ).fetchone()
    sequence = int(sequence_row["seq"]) if sequence_row and sequence_row["seq"] else 1
    return {
        "id": row["id"],
        "createdAt": row["created_at"],
        "reportDate": report_date,
        "reportName": format_analysis_report_name(report_date, sequence),
        "model": row["model"],
        "promptText": row["prompt_text"],
        "snapshot": json.loads(row["source_snapshot_json"] or "{}"),
        "report": json.loads(row["report_json"] or "{}"),
        "usage": json.loads(row["usage_json"] or "{}") if row["usage_json"] else {},
        "error": row["error_text"],
    }


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


def resolve_invitation_number(conn, invitation_number=None, submission_id=None):
    normalized = normalize_code(invitation_number)
    if normalized:
        return normalized
    identifier = str(submission_id or "").strip()
    if not identifier:
        return None
    row = conn.execute(
        """
        SELECT invitation_number
        FROM feedback_submissions
        WHERE submission_id = ?
        """,
        (identifier,),
    ).fetchone()
    if row is None:
        return None
    return normalize_code(row["invitation_number"])


def list_participant_messages(conn, invitation_number, include_dismissed=True):
    normalized = normalize_code(invitation_number)
    if not normalized:
        return []
    query = """
        SELECT id, created_at, updated_at, invitation_number, submission_id, author, sender_role, message, is_dismissed, dismissed_at
        FROM participant_messages
        WHERE invitation_number = ?
    """
    params = [normalized]
    if not include_dismissed:
        query += " AND is_dismissed = 0"
    query += " ORDER BY created_at ASC, id ASC"
    rows = conn.execute(query, params).fetchall()
    return [
        {
            "id": row["id"],
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
            "invitationNumber": row["invitation_number"],
            "submissionId": row["submission_id"],
            "author": row["author"],
            "senderRole": row["sender_role"] or "admin",
            "message": row["message"],
            "isDismissed": bool(row["is_dismissed"]),
            "dismissedAt": row["dismissed_at"],
        }
        for row in rows
    ]


def create_participant_message(conn, payload):
    message = str(payload.get("message") or "").strip()
    if not message:
        return False, "Message is required"
    invitation_number = resolve_invitation_number(
        conn,
        invitation_number=payload.get("invitationNumber"),
        submission_id=payload.get("submissionId"),
    )
    if not invitation_number:
        return False, "Invitation number is required"
    submission_id = str(payload.get("submissionId") or "").strip() or None
    author = str(payload.get("author") or "Serenzer team").strip() or "Serenzer team"
    sender_role = str(payload.get("senderRole") or "admin").strip().lower() or "admin"
    if sender_role not in {"admin", "participant"}:
        sender_role = "admin"
    timestamp = now_iso()
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(participant_messages)").fetchall()}

    insert_columns = ["created_at"]
    insert_values = [timestamp]

    if "updated_at" in columns:
        insert_columns.append("updated_at")
        insert_values.append(timestamp)
    if "invitation_number" in columns:
        insert_columns.append("invitation_number")
        insert_values.append(invitation_number)
    if "invitation_code" in columns:
        insert_columns.append("invitation_code")
        insert_values.append(invitation_number)
    if "submission_id" in columns:
        insert_columns.append("submission_id")
        insert_values.append(submission_id)
    if "author" in columns:
        insert_columns.append("author")
        insert_values.append(author)
    if "sender_role" in columns:
        insert_columns.append("sender_role")
        insert_values.append(sender_role)
    if "message" in columns:
        insert_columns.append("message")
        insert_values.append(message)
    if "body" in columns:
        insert_columns.append("body")
        insert_values.append(message)
    if "is_dismissed" in columns:
        insert_columns.append("is_dismissed")
        insert_values.append(0)
    if "dismissed_at" in columns:
        insert_columns.append("dismissed_at")
        insert_values.append(None)

    placeholders = ", ".join("?" for _ in insert_columns)
    cursor = conn.execute(
        f"""
        INSERT INTO participant_messages ({", ".join(insert_columns)})
        VALUES ({placeholders})
        """,
        insert_values,
    )
    return True, {
        "id": cursor.lastrowid,
        "createdAt": timestamp,
        "updatedAt": timestamp,
        "invitationNumber": invitation_number,
        "submissionId": submission_id,
        "author": author,
        "senderRole": sender_role,
        "message": message,
        "isDismissed": False,
        "dismissedAt": None,
    }


def dismiss_participant_message(conn, invitation_number, message_id):
    normalized = normalize_code(invitation_number)
    if not normalized or not message_id:
        return False
    cursor = conn.execute(
        """
        UPDATE participant_messages
        SET is_dismissed = 1,
            dismissed_at = ?,
            updated_at = ?
        WHERE id = ? AND invitation_number = ?
        """,
        (now_iso(), now_iso(), int(message_id), normalized),
    )
    return cursor.rowcount > 0


class FeedbackHandler(BaseHTTPRequestHandler):
    server_version = "SerenzerFeedback/1.0"

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
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
        if parsed.path == "/api/messages/current":
            self._handle_current_participant_message(parsed)
            return
        if parsed.path == "/api/messages/thread":
            self._handle_participant_message_thread(parsed)
            return
        if parsed.path == "/api/admin/messages":
            self._handle_admin_message_list(parsed)
            return
        if parsed.path == "/api/admin/logs":
            self._handle_activity_log_list()
            return
        if parsed.path == "/api/admin/analysis/config":
            self._handle_analysis_config_get()
            return
        if parsed.path == "/api/admin/analysis/reports":
            self._handle_analysis_report_list()
            return
        if parsed.path.startswith("/api/admin/analysis/reports/"):
            report_id = parsed.path.removeprefix("/api/admin/analysis/reports/")
            self._handle_analysis_report_detail(report_id)
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
        if parsed.path.startswith("/api/admin/feedback/") and parsed.path.endswith("/pdf"):
            submission_id = parsed.path.removeprefix("/api/admin/feedback/").removesuffix("/pdf").rstrip("/")
            self._handle_feedback_detail_pdf(submission_id)
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
        if parsed.path == "/api/admin/messages":
            self._handle_admin_message_create()
            return
        if parsed.path == "/api/messages/send":
            self._handle_participant_message_create()
            return
        if parsed.path == "/api/messages/dismiss":
            self._handle_participant_message_dismiss()
            return
        if parsed.path == "/api/admin/analysis/config":
            self._handle_analysis_config_update()
            return
        if parsed.path == "/api/admin/analysis/reports/generate":
            self._handle_analysis_report_generate()
            return
        if parsed.path == "/api/admin/participants/delete":
            self._handle_participant_delete()
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

    def _client_ip(self):
        forwarded = self.headers.get("X-Forwarded-For", "").strip()
        if forwarded:
            return forwarded.split(",")[0].strip()
        real_ip = self.headers.get("X-Real-IP", "").strip()
        if real_ip:
            return real_ip
        return self.client_address[0] if self.client_address else None

    def _log_activity(self, conn, action, **payload):
        create_activity_log(
            conn,
            action,
            {
                "createdAt": now_iso(),
                "requestPath": self.path,
                "requestMethod": self.command,
                "clientIp": self._client_ip(),
                **payload,
            },
        )

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
            ok, result = claim_invitation_code(
                conn,
                payload.get("code"),
                submission_id,
                payload.get("lastName"),
            )
            if not ok:
                conn.rollback()
                if isinstance(result, dict):
                    status = 409 if result.get("requiresLastName") else 403
                    self._send_json(status, {"ok": False, **result})
                else:
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
            self._log_activity(
                conn,
                "invitation_validated",
                submissionId=result.get("submissionId") or submission_id,
                invitationNumber=result["code"],
                email=result.get("email"),
                details={
                    "firstActivation": bool(result.get("firstActivation")),
                    "appUserId": result.get("appUserId"),
                    "source": result.get("source"),
                },
            )
            conn.commit()
        finally:
            conn.close()

        self._send_json(
            200,
            {
                "ok": True,
                "code": result["code"],
                "submissionId": result.get("submissionId"),
                "existingSubmission": result.get("existingSubmission"),
            },
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
            for entry in entries:
                current = entry
                if isinstance(current, str):
                    current = {"code": current}
                if not isinstance(current, dict):
                    continue
                normalized = normalize_code(current.get("code"))
                if not normalized:
                    continue
                self._log_activity(
                    conn,
                    "invitation_imported",
                    invitationNumber=normalized,
                    email=current.get("email"),
                    details={
                        "appUserId": current.get("appUserId") or current.get("app_user_id"),
                        "source": current.get("source") or "serenzer-app",
                    },
                )
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
            for code in created:
                self._log_activity(
                    conn,
                    "invitation_created_manual",
                    invitationNumber=code,
                    details={"source": "manual"},
                )
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
            self._log_activity(
                conn,
                "invitation_disabled",
                invitationNumber=payload.get("code"),
                details={"source": "admin"},
            )
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
            self._log_activity(
                conn,
                "invitation_restored",
                submissionId=result.get("submissionId") or submission_id,
                invitationNumber=result["code"],
                details={"rememberedSession": True},
            )
            conn.commit()
        finally:
            conn.close()

        self._send_json(
            200,
            {
                "ok": True,
                "code": result["code"],
                "submissionId": result.get("submissionId"),
                "existingSubmission": result.get("existingSubmission"),
            },
        )

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

        requested_submission_id = str(payload.get("submissionId", "")).strip()
        if not requested_submission_id:
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
            ok, result = claim_invitation_code(conn, invitation_number, requested_submission_id)
            if not ok:
                conn.rollback()
                self._send_json(403, {"error": result})
                return
            submission_id = result.get("submissionId") or requested_submission_id

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
            self._log_activity(
                conn,
                "feedback_saved",
                submissionId=submission_id,
                invitationNumber=result["code"],
                email=merged_onboarding.get("email"),
                firstName=merged_onboarding.get("firstName"),
                lastName=merged_onboarding.get("lastName"),
                lang=merged_lang,
                activeTab=payload.get("activeTab"),
                activeTabLabel=payload.get("activeTabLabel"),
                details={
                    "isComplete": merged_is_complete,
                    "completedTabsCount": len(merged_completed_tabs),
                    "completedTabs": merged_completed_tabs,
                    "pageUrl": payload.get("pageUrl"),
                },
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

    def _handle_admin_message_list(self, parsed):
        query = parse_qs(parsed.query)
        invitation = query.get("invitation", [""])[0]
        submission_id = query.get("submissionId", [""])[0]
        conn = get_db()
        try:
            resolved = resolve_invitation_number(conn, invitation_number=invitation, submission_id=submission_id)
            items = list_participant_messages(conn, resolved, include_dismissed=True)
        finally:
            conn.close()
        self._send_json(200, {"count": len(items), "items": items})

    def _handle_current_participant_message(self, parsed):
        query = parse_qs(parsed.query)
        invitation = query.get("invitation", [""])[0]
        submission_id = query.get("submissionId", [""])[0]
        conn = get_db()
        try:
            resolved = resolve_invitation_number(conn, invitation_number=invitation, submission_id=submission_id)
            items = list_participant_messages(conn, resolved, include_dismissed=False)
        finally:
            conn.close()
        self._send_json(200, {"ok": True, "message": items[0] if items else None})

    def _handle_participant_message_thread(self, parsed):
        query = parse_qs(parsed.query)
        invitation = query.get("invitation", [""])[0]
        submission_id = query.get("submissionId", [""])[0]
        conn = get_db()
        try:
            resolved = resolve_invitation_number(conn, invitation_number=invitation, submission_id=submission_id)
            items = list_participant_messages(conn, resolved, include_dismissed=False)
        finally:
            conn.close()
        self._send_json(200, {"count": len(items), "items": items})

    def _handle_admin_message_create(self):
        payload, error = self._read_json_body()
        if error:
            self._send_json(400, error)
            return
        conn = get_db()
        try:
            ok, result = create_participant_message(conn, payload)
            if not ok:
                conn.rollback()
                self._send_json(400, {"error": result})
                return
            self._log_activity(
                conn,
                "participant_message_created",
                submissionId=result.get("submissionId"),
                invitationNumber=result.get("invitationNumber"),
                details={"messageId": result.get("id"), "author": result.get("author")},
            )
            conn.commit()
        finally:
            conn.close()
        self._send_json(200, {"ok": True, "message": result})

    def _handle_participant_message_create(self):
        payload, error = self._read_json_body()
        if error:
            self._send_json(400, error)
            return
        conn = get_db()
        try:
            onboarding = payload.get("onboarding") or {}
            author = (
                str(onboarding.get("firstName") or "").strip()
                or str(onboarding.get("email") or "").strip()
                or "Participant"
            )
            payload["author"] = author
            payload["senderRole"] = "participant"
            ok, result = create_participant_message(conn, payload)
            if not ok:
                conn.rollback()
                self._send_json(400, {"error": result})
                return
            self._log_activity(
                conn,
                "participant_message_sent",
                submissionId=result.get("submissionId"),
                invitationNumber=result.get("invitationNumber"),
                email=str(onboarding.get("email") or "").strip() or None,
                firstName=str(onboarding.get("firstName") or "").strip() or None,
                lastName=str(onboarding.get("lastName") or "").strip() or None,
                lang=str(payload.get("lang") or "").strip() or None,
                activeTab=payload.get("activeTab"),
                activeTabLabel=payload.get("activeTabLabel"),
                details={"messageId": result.get("id"), "author": result.get("author")},
            )
            conn.commit()
        finally:
            conn.close()
        self._send_json(200, {"ok": True, "message": result})

    def _handle_participant_message_dismiss(self):
        payload, error = self._read_json_body()
        if error:
            self._send_json(400, error)
            return
        conn = get_db()
        try:
            invitation_number = resolve_invitation_number(
                conn,
                invitation_number=payload.get("invitationNumber"),
                submission_id=payload.get("submissionId"),
            )
            ok = dismiss_participant_message(conn, invitation_number, payload.get("messageId"))
            if not ok:
                conn.rollback()
                self._send_json(404, {"error": "Message not found"})
                return
            self._log_activity(
                conn,
                "participant_message_dismissed",
                submissionId=payload.get("submissionId"),
                invitationNumber=invitation_number,
                details={"messageId": payload.get("messageId")},
            )
            conn.commit()
        finally:
            conn.close()
        self._send_json(200, {"ok": True})

    def _handle_analysis_config_get(self):
        conn = get_db()
        try:
            config = get_analysis_config(conn)
        finally:
            conn.close()
        self._send_json(200, config)

    def _handle_analysis_config_update(self):
        payload, error = self._read_json_body()
        if error:
            self._send_json(400, error)
            return
        conn = get_db()
        try:
            config = update_analysis_config(conn, payload.get("model"), payload.get("promptText"))
            self._log_activity(
                conn,
                "analysis_config_updated",
                details={"model": config.get("model")},
            )
            conn.commit()
        finally:
            conn.close()
        self._send_json(200, {"ok": True, "config": config})

    def _handle_analysis_report_list(self):
        conn = get_db()
        try:
            items = list_analysis_reports(conn)
        finally:
            conn.close()
        self._send_json(200, {"count": len(items), "items": items})

    def _handle_analysis_report_detail(self, report_id):
        if not str(report_id or "").strip().isdigit():
            self._send_json(400, {"error": "Invalid report id"})
            return
        conn = get_db()
        try:
            report = get_analysis_report(conn, int(report_id))
        finally:
            conn.close()
        if report is None:
            self._send_json(404, {"error": "Analysis report not found"})
            return
        self._send_json(200, report)

    def _handle_analysis_report_generate(self):
        payload, error = self._read_json_body()
        if error:
            self._send_json(400, error)
            return
        conn = get_db()
        try:
            config = get_analysis_config(conn)
            report_date = str(payload.get("reportDate") or now_iso()[:10]).strip() or now_iso()[:10]
            snapshot = build_analysis_snapshot(conn, report_date=report_date)
            ok, result = request_openai_analysis(config["model"], config["promptText"], snapshot)
            if not ok:
                conn.rollback()
                self._send_json(502, result)
                return
            report_id = create_analysis_report(
                conn,
                report_date=report_date,
                model=config["model"],
                prompt_text=config["promptText"],
                snapshot=snapshot,
                report_json=result["report"],
                usage=result.get("usage"),
            )
            self._log_activity(
                conn,
                "analysis_report_generated",
                details={
                    "reportId": report_id,
                    "reportDate": report_date,
                    "model": config.get("model"),
                    "openaiResponseId": result.get("rawResponseId"),
                },
            )
            conn.commit()
            report = get_analysis_report(conn, report_id)
        finally:
            conn.close()
        self._send_json(200, {"ok": True, "report": report})

    def _handle_activity_log_list(self):
        conn = get_db()
        try:
            items = list_activity_logs(conn)
        finally:
            conn.close()
        self._send_json(200, {"count": len(items), "items": items})

    def _handle_participant_delete(self):
        payload, error = self._read_json_body()
        if error:
            self._send_json(400, error)
            return

        submission_id = payload.get("submissionId")
        conn = get_db()
        try:
            ok, result = delete_participant_entry(conn, submission_id)
            if not ok:
                conn.rollback()
                self._send_json(404, {"ok": False, "error": result})
                return
            self._log_activity(
                conn,
                "participant_deleted",
                submissionId=result.get("submissionId"),
                invitationNumber=result.get("invitationNumber"),
                email=result.get("email"),
                details={"entryType": result.get("entryType")},
            )
            conn.commit()
        finally:
            conn.close()

        self._send_json(200, {"ok": True, "deleted": result})

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
            onboarding = payload.get("onboarding") or {}
            self._log_activity(
                conn,
                "bug_report_created",
                submissionId=payload.get("submissionId"),
                invitationNumber=onboarding.get("invitationNumber"),
                email=onboarding.get("email"),
                firstName=onboarding.get("firstName"),
                lastName=onboarding.get("lastName"),
                lang=payload.get("lang"),
                activeTab=payload.get("activeTab"),
                activeTabLabel=payload.get("activeTabLabel"),
                details={
                    "message": str(payload.get("message") or "").strip(),
                    "pageUrl": payload.get("pageUrl"),
                    "userAgent": payload.get("userAgent"),
                },
            )
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
            row = get_feedback_detail_row(conn, submission_id)
        finally:
            conn.close()

        if row is None:
            self._send_json(404, {"error": "Submission not found"})
            return

        self._send_json(200, feedback_detail_from_row(row))

    def _handle_feedback_detail_pdf(self, submission_id):
        if not submission_id or submission_id.startswith("ghost:"):
            self._send_json(400, {"error": "PDF export is only available for saved participant submissions"})
            return

        conn = get_db()
        try:
            row = get_feedback_detail_row(conn, submission_id)
        finally:
            conn.close()

        if row is None:
            self._send_json(404, {"error": "Submission not found"})
            return

        detail = feedback_detail_from_row(row)
        pdf_bytes = build_feedback_report_pdf(detail)
        filename = make_report_filename(detail)
        self._send_bytes(
            200,
            pdf_bytes,
            "application/pdf",
            extra_headers=[
                ("Content-Disposition", f'attachment; filename="{filename}"'),
                ("Cache-Control", "no-store"),
                ("Pragma", "no-cache"),
            ],
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

    def _send_bytes(self, status_code, payload, content_type, extra_headers=None):
        self.send_response(status_code)
        self.send_header("Content-Type", content_type)
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
