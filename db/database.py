import json
import sqlite3
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

from config import GUARDABOT_DB


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def db_connect() -> sqlite3.Connection:
    return sqlite3.connect(GUARDABOT_DB)

def init_guardabot_db():
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS guard_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                discord_user TEXT,
                channel_id TEXT,
                session_id TEXT,
                action TEXT NOT NULL,
                item TEXT NOT NULL,
                qty REAL,
                unit TEXT,
                location_from TEXT,
                location_to TEXT,
                ecosystem TEXT,
                job TEXT,
                notes TEXT,
                photo_url TEXT,
                cost REAL,
                raw_text TEXT
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_guard_item_ts ON guard_events(item, ts)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_guard_session ON guard_events(session_id)")
        conn.commit()


def ensure_guardabot_schema():
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(guard_events)")
        cols = {row[1].lower() for row in cur.fetchall()}

        if "cost" not in cols:
            cur.execute("ALTER TABLE guard_events ADD COLUMN cost REAL")
        conn.commit()


def init_metiche_db():
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS metiche_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                discord_user TEXT,
                channel_id TEXT,
                job TEXT NOT NULL,
                kind TEXT NOT NULL,
                hours REAL,
                cost REAL,
                note TEXT
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_metiche_job_ts ON metiche_logs(job, ts)")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS metiche_weekly (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                discord_user TEXT,
                channel_id TEXT,
                week_of TEXT,
                weekly_goal REAL,
                jobs_json TEXT,
                pending_estimates_json TEXT,
                invoices_to_send_json TEXT,
                todays_schedule TEXT,
                wants_accountant INTEGER
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS metiche_checkins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                discord_user TEXT,
                channel_id TEXT,
                week_of TEXT,
                category TEXT,
                task TEXT,
                energy INTEGER,
                raw_text TEXT
            )
        """)
        conn.commit()


def init_crudobot_db():
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crudo_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                discord_user TEXT,
                channel_id TEXT,
                job TEXT NOT NULL,
                contract_amount REAL,
                collected REAL,
                materials_cost REAL,
                correction_cost REAL,
                labor_hours REAL,
                labor_cost REAL,
                profit REAL,
                margin REAL,
                narrative_sequence TEXT,
                narrative_divergence TEXT,
                narrative_checklist TEXT,
                narrative_friction TEXT,
                narrative_prevention TEXT,
                narrative_rule TEXT,
                raw_json TEXT
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_crudo_job_ts ON crudo_reports(job, ts)")
        conn.commit()
        
def insert_metiche_log(row: Dict[str, Any]):
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO metiche_logs (ts, discord_user, channel_id, job, kind, hours, cost, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row["ts"], row.get("discord_user"), row.get("channel_id"),
            row["job"], row["kind"], row.get("hours"), row.get("cost"), row.get("note")
        ))
        conn.commit()

def insert_guard_event(ev: Dict[str, Any]):
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO guard_events (
                ts, discord_user, channel_id, session_id,
                action, item, qty, unit,
                location_from, location_to, ecosystem, job,
                notes, photo_url, cost, raw_text
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            ev["ts"], ev.get("discord_user"), ev.get("channel_id"), ev.get("session_id"),
            ev["action"], ev["item"], ev.get("qty"), ev.get("unit"),
            ev.get("location_from"), ev.get("location_to"), ev.get("ecosystem"), ev.get("job"),
            ev.get("notes"), ev.get("photo_url"), ev.get("cost"), ev.get("raw_text")
        ))
        conn.commit()


def fetch_guard_session_events(session_id: str) -> List[Tuple]:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT ts, action, item, qty, unit, location_from, location_to, ecosystem, job, cost
            FROM guard_events
            WHERE session_id = ?
            ORDER BY id ASC
        """, (session_id,))
        return cur.fetchall()


def get_guard_last_known(item: str) -> Optional[Tuple]:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT ts, ecosystem, location_to, qty, unit, action
            FROM guard_events
            WHERE LOWER(item) = LOWER(?)
            ORDER BY id DESC
            LIMIT 1
        """, (item.strip(),))
        return cur.fetchone()


def build_guard_last_known_index() -> Dict[str, Tuple]:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT item, ts, ecosystem, location_to, qty, unit, action
            FROM guard_events
            ORDER BY id DESC
        """)
        rows = cur.fetchall()

    seen = {}
    for item, ts, eco, loc_to, qty, unit, action in rows:
        key = item.lower()
        if key not in seen:
            seen[key] = (item, ts, eco, loc_to, qty, unit, action)
    return seen


def insert_metiche_weekly(row: Dict[str, Any]):
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO metiche_weekly (
                ts, discord_user, channel_id,
                week_of, weekly_goal,
                jobs_json, pending_estimates_json, invoices_to_send_json,
                todays_schedule, wants_accountant
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row["ts"], row.get("discord_user"), row.get("channel_id"),
            row["week_of"], row.get("weekly_goal"),
            row.get("jobs_json"), row.get("pending_estimates_json"), row.get("invoices_to_send_json"),
            row.get("todays_schedule"), 1 if row.get("wants_accountant") else 0
        ))
        conn.commit()


def insert_metiche_checkin(row: Dict[str, Any]):
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO metiche_checkins (
                ts, discord_user, channel_id,
                week_of, category, task, energy, raw_text
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row["ts"], row.get("discord_user"), row.get("channel_id"),
            row.get("week_of"),
            row.get("category"), row.get("task"), row.get("energy"), row.get("raw_text")
        ))
        conn.commit()


def fetch_latest_metiche_weekly(week_of: str) -> Optional[Dict[str, Any]]:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT ts, discord_user, channel_id, week_of, weekly_goal,
                   jobs_json, pending_estimates_json, invoices_to_send_json,
                   todays_schedule, wants_accountant
            FROM metiche_weekly
            WHERE week_of = ?
            ORDER BY id DESC
            LIMIT 1
        """, (week_of,))
        r = cur.fetchone()

    if not r:
        return None

    ts, discord_user, channel_id, week_of, weekly_goal, jobs_json, est_json, inv_json, todays_schedule, wants_acc = r
    return {
        "ts": ts,
        "discord_user": discord_user,
        "channel_id": channel_id,
        "week_of": week_of,
        "weekly_goal": weekly_goal,
        "jobs": json.loads(jobs_json) if jobs_json else [],
        "pending_estimates": json.loads(est_json) if est_json else [],
        "invoices_to_send": json.loads(inv_json) if inv_json else [],
        "todays_schedule": todays_schedule or "",
        "wants_accountant": bool(wants_acc),
    }


def insert_crudo_report(data):
    with db_connect() as conn:
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO crudo_reports (
                ts, discord_user, channel_id, job,
                contract_amount, collected,
                materials_cost, correction_cost,
                labor_hours, labor_cost,
                profit, margin,
                narrative_sequence,
                narrative_divergence,
                narrative_checklist,
                narrative_friction,
                narrative_prevention,
                narrative_rule,
                raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data["ts"],
            data.get("discord_user"),
            data.get("channel_id"),
            data["job"],
            data.get("contract_amount"),
            data.get("collected"),
            data.get("materials_cost"),
            data.get("correction_cost"),
            data.get("labor_hours"),
            data.get("labor_cost"),
            data.get("profit"),
            data.get("margin"),
            data.get("narrative_sequence"),
            data.get("narrative_divergence"),
            data.get("narrative_checklist"),
            data.get("narrative_friction"),
            data.get("narrative_prevention"),
            data.get("narrative_rule"),
            json.dumps(data)
        ))

        conn.commit()

def fetch_latest_crudo_report(job: str):
    with db_connect() as conn:
        cur = conn.cursor()

        cur.execute("""
            SELECT raw_json
            FROM crudo_reports
            WHERE job = ?
            ORDER BY id DESC
            LIMIT 1
        """, (job,))

        row = cur.fetchone()

    if not row:
        return None

    return json.loads(row[0])