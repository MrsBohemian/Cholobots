"""
METICHEBOT
-----------

Metichebot is the operational coordination and task-accounting layer
for the Cholobots ecosystem.

Its purpose is to help neurodivergent tradespeople externalize:
- planning
- sequencing
- prioritization
- schedule coordination
- wakeup/morning activation
- time awareness
- task accounting
- daily operational flow

Metichebot is intentionally designed around conversational workflows
instead of rigid project-management abstractions.

PRIMARY RESPONSIBILITIES
------------------------
- Financial execution objective management (!mengine)
- Goal and schedule management (!mgoals, !mschedule)
- Daily task structuring (!mtoday)
- Morning activation and routines (!mwakeup, !mroutine)
- Persistent reminder/ping scheduling
- Task accounting and time-session tracking
- Calendar synchronization with the Command Center

RELATIONSHIP TO OTHER CHOLOBOTS
--------------------------------
Metichebot coordinates operational execution across the ecosystem:

- Chismebot:
    Relationship management, follow-ups, customer narratives,
    opportunity tracking, and social memory.

- Crudobot:
    Estimating, job costing, purchasing analysis,
    financial observations, and operational metrics.

- Guardabot:
    Inventory, garage zones, materials staging,
    logistics, and physical resource tracking.

Metichebot often acts as the orchestration layer connecting:
- scheduling
- operational execution
- accountability
- workflow continuity

TASK ACCOUNTING
----------------
Task accounting is not surveillance or productivity scoring.

It is a lightweight operational memory system intended to help users:
- understand where time is going
- externalize cognitive load
- re-enter interrupted workflows
- document operational drift
- support neurodivergent execution patterns

Persistent task accounting data eventually feeds the
Command Center dashboard visualization layer.

ARCHITECTURE NOTES
-------------------
- Persistent operational state is stored in Supabase.
- Discord serves as the conversational guild hall interface.
- The Command Center acts as the visualization layer.
- Railway hosts the operational bot services.

This system is evolving toward a distributed operational framework
for collaborative trades work and guild-style coordination.
"""

import asyncio
import json
import os
import re
import traceback

from dataclasses import dataclass, field
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple
from urllib import error, request
from zoneinfo import ZoneInfo

LOCAL_TZ = ZoneInfo("America/Chicago")

def local_now():
    return datetime.now(LOCAL_TZ)

import discord
from discord.ext import commands
from supabase import create_client

from db.database import (
    now_iso,
    insert_metiche_weekly,
    insert_metiche_checkin,
    fetch_latest_metiche_weekly,
)

DAY_NAMES = [
    "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"
]

VALID_PEOPLE = ["Heaven", "Daniel", "Jesse", "Samuel", "Handley Man"]

PERSON_TO_CALENDAR_KEY = {
    "Heaven": "heaven",
    "Daniel": "daniel",
    "Jesse": "jesse",
    "Samuel": "samuel",
    "Handley Man": "handley_man",
}

DISCORD_USER_TO_PERSON = {
    823352347715174421: "Heaven",
    532299697843601419: "Daniel",
    1046820625840349244: "Jesse",
    1477522070559654092: "Samuel",
}

def get_person_from_discord(author_id: int) -> str:
    return DISCORD_USER_TO_PERSON.get(author_id, f"UNKNOWN:{author_id}")
    
RAW_TIME_LABEL = "raw_time"

DEFAULT_CALENDAR = {
    "Heaven": {},
    "Daniel": {},
    "Jesse": {},
    "Samuel": {},
    "Handley Man": {}
}
DEFAULT_CHILLHOP_URL = os.getenv(
    "DANIEL_MORNING_AUDIO_URL",
    "https://www.youtube.com/results?search_query=chillhop+morning+radio",
)

metiche_instance = None
active_time_sessions: Dict[int, "TimeSession"] = {}
channels_waiting_for_command = set()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY) if SUPABASE_URL and SUPABASE_KEY else None

@dataclass
class TimeSession:
    channel_id: int
    person: str
    date_iso: str
    date_label: str
    last_timestamp: str
    last_activity_timestamp: Optional[str] = None

    # What Metiche currently believes you're doing.
    active_task: Optional[str] = None

    # The planned focus before reality diverged.
    intended_task: Optional[str] = None

    setup_complete: bool = False
    current_state: str = "active"  # active / paused / drift / transition
    paused_task: Optional[str] = None

    # Que Onda state.
    awaiting_checkin_response: bool = False
    last_checkin_focus: Optional[str] = None

    blocks: List[Dict[str, Any]] = field(default_factory=list)
    daily_tasks: List[Dict[str, Any]] = field(default_factory=list)
    parked_items: List[str] = field(default_factory=list)
    interruptions: List[Dict[str, Any]] = field(default_factory=list)

    # Reality that happened outside the planned list.
    other_tasks_accomplished: List[str] = field(default_factory=list)
    drift_events: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class FinancialExecution:
    objective_amount: float = 0.0
    due_date: Optional[str] = None
    objective_reason: str = ""
    planned_amount: float = 0.0
    remaining_gap: float = 0.0
    status: str = "open"
    priority_tasks: List[str] = field(default_factory=list)


# ---------- general parsing / formatting ----------

def parse_iso(ts: str) -> datetime:
    """Parse timestamps safely for local task accounting.

    Some timestamps come from now_iso() as offset-naive strings while
    local_now() is offset-aware. Normalize naive timestamps into LOCAL_TZ
    so duration math does not crash.
    """
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=LOCAL_TZ)
    return dt.astimezone(LOCAL_TZ)

def parse_wakeup_time(raw: str) -> Optional[datetime]:
    raw = raw.strip().lower().replace(".", "")
    today = local_now().date()

    for fmt in ("%I:%M %p", "%I %p", "%H:%M", "%H"):
        try:
            parsed = datetime.strptime(raw, fmt).time()
            wake_dt = datetime.combine(today, parsed, tzinfo=LOCAL_TZ)

            if wake_dt <= local_now():
                wake_dt = wake_dt + timedelta(days=1)

            return wake_dt
        except ValueError:
            continue

    return None


def week_of_monday(d: datetime) -> str:
    monday = d.date() - timedelta(days=d.weekday())
    return monday.isoformat()


def today_iso() -> str:
    return local_now().date().isoformat()


def today_label() -> str:
    fmt = "%A, %B %-d" if os.name != "nt" else "%A, %B %#d"
    return local_now().strftime(fmt)


def money_to_float(raw: str) -> float:
    cleaned = re.sub(r"[^0-9.\-]", "", raw or "")
    if not cleaned or cleaned in {"-", "."}:
        return 0.0
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def normalize_task(task: str) -> str:
    return re.sub(r"\s+", " ", str(task).strip().lower())


def parse_named_list(text: str) -> List[str]:
    cleaned = text.strip()
    if not cleaned or cleaned.lower() in {"none", "no", "n/a", "na"}:
        return []
    if "\n" in cleaned:
        parts = [line.strip("-• 1234567890.").strip() for line in cleaned.splitlines()]
    else:
        parts = [part.strip() for part in cleaned.split(",")]
    return [part for part in parts if part and part.lower() != "none"]


def parse_task_list(text: str) -> List[Dict[str, Any]]:
    return [{"text": item, "done": False} for item in parse_named_list(text)]


def day_to_iso(day_name: str, week_start: Optional[str] = None) -> str:
    today = local_now().date()
    target_index = DAY_NAMES.index(day_name.lower())
    today_index = today.weekday()

    days_ahead = (target_index - today_index) % 7

    return (today + timedelta(days=days_ahead)).isoformat()

def parse_schedule_block(text: str, week_start: str) -> Dict[str, List[Any]]:
    result: Dict[str, List[Any]] = {}
    for line in [line.strip() for line in text.splitlines() if line.strip()]:
        if ":" not in line:
            continue
        day_part, task_part = line.split(":", 1)
        day_name = day_part.strip().lower()
        if day_name not in DAY_NAMES:
            continue
        tasks = [{"text": t.strip(), "done": False} for t in task_part.split(",") if t.strip()]
        result[day_to_iso(day_name, week_start)] = tasks
    return result


def ensure_calendar(raw: Any = None) -> Dict[str, Dict[str, List[Any]]]:
    calendar = json_safe_load(raw, DEFAULT_CALENDAR.copy())
    for person in VALID_PEOPLE:
        calendar.setdefault(person, {})
    return calendar


def json_safe_load(raw: Any, fallback: Any):
    if raw is None:
        return fallback
    if isinstance(raw, (dict, list)):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return fallback
    return fallback


def normalize_daily_items(raw_items: List[Any]) -> List[Dict[str, Any]]:
    normalized = []

    for item in raw_items or []:
        if isinstance(item, dict):
            text = str(item.get("text") or item.get("task") or "").strip()
            done = bool(item.get("done") or item.get("completed") or False)
            source = item.get("source")
            item_type = item.get("type")
            priority = item.get("priority")
        else:
            text = str(item).strip()
            done = False
            source = None
            item_type = None
            priority = None

        if text:
            row = {"text": text, "done": done}

            if source:
                row["source"] = source
            if item_type:
                row["type"] = item_type
            if priority:
                row["priority"] = priority

            normalized.append(row)

    return normalized


def merge_days(existing: Dict[str, List[Any]], incoming: Dict[str, List[Any]]) -> Dict[str, List[Any]]:
    merged = {k: normalize_daily_items(v) for k, v in (existing or {}).items()}
    for day, new_tasks in incoming.items():
        current = merged.get(day, [])
        seen = {normalize_task(task.get("text", "")) for task in current}
        for task in normalize_daily_items(new_tasks):
            norm = normalize_task(task.get("text", ""))
            if norm and norm not in seen:
                current.append(task)
                seen.add(norm)
        merged[day] = current
    return merged

def remove_source_tasks(person_schedule: Dict[str, List[Any]], source: str) -> Dict[str, List[Any]]:
    cleaned = {}

    for day, tasks in (person_schedule or {}).items():
        kept = [
            task for task in normalize_daily_items(tasks)
            if task.get("source") != source
        ]

        if kept:
            cleaned[day] = kept

    return cleaned


def modify_days(existing: Dict[str, List[Any]], incoming: Dict[str, List[Any]]) -> Dict[str, List[Any]]:
    updated = {k: normalize_daily_items(v) for k, v in (existing or {}).items()}
    for day, tasks in incoming.items():
        updated[day] = normalize_daily_items(tasks)
    return updated


def replace_days(_: Dict[str, List[Any]], incoming: Dict[str, List[Any]]) -> Dict[str, List[Any]]:
    return {k: normalize_daily_items(v) for k, v in incoming.items()}


def total_minutes(blocks: List[Dict[str, Any]]) -> int:
    return int(sum(int(block.get("duration_minutes", 0) or 0) for block in blocks))


def minutes_to_label(minutes: int) -> str:
    hours = minutes // 60
    mins = minutes % 60
    if hours and mins:
        return f"{hours}h {mins}m"
    if hours:
        return f"{hours}h"
    return f"{mins}m"

# ---------- Supabase daily task persistence ----------

def require_supabase() -> bool:
    return supabase is not None


def load_daily_tasks(person: str, date_iso_value: str) -> List[Dict[str, Any]]:
    if not require_supabase():
        return []
    calendar_key = PERSON_TO_CALENDAR_KEY.get(person, "heaven")
    response = (
        supabase.table("daily_tasks")
        .select("*")
        .eq("calendar_key", calendar_key)
        .eq("date", date_iso_value)
        .order("created_at")
        .execute()
    )
    return [
        {"text": row.get("task", ""), "done": bool(row.get("completed", False))}
        for row in (response.data or [])
        if row.get("task")
    ]


def replace_daily_tasks(person: str, date_iso_value: str, tasks: List[Dict[str, Any]]):
    if not require_supabase():
        return
    calendar_key = PERSON_TO_CALENDAR_KEY.get(person, "heaven")
    (
        supabase.table("daily_tasks")
        .delete()
        .eq("calendar_key", calendar_key)
        .eq("date", date_iso_value)
        .execute()
    )
    inserts = [
        {
            "user_id": calendar_key,
            "calendar_key": calendar_key,
            "date": date_iso_value,
            "task": task["text"],
            "completed": bool(task.get("done", False)),
        }
        for task in normalize_daily_items(tasks)
    ]
    if inserts:
        supabase.table("daily_tasks").insert(inserts).execute()

def save_default_ping_interval(
    user_id: str,
    interval_minutes: int
):
    if not require_supabase():
        return {"ok": False}

    response = (
        supabase.table("metiche_ping_preferences")
        .upsert(
            {
                "user_id": user_id,
                "interval_minutes": interval_minutes,
                "is_enabled": interval_minutes > 0,
                "updated_at": local_now().isoformat(),
            },
            on_conflict="user_id",
        )
        .execute()
    )

    return {"ok": True, "data": response.data}

def fetch_default_ping_interval(user_id: str):
    if not require_supabase():
        return None

    response = (
        supabase.table("metiche_ping_preferences")
        .select("*")
        .eq("user_id", user_id)
        .limit(1)
        .execute()
    )

    rows = response.data or []

    if not rows:
        return None

    return rows[0]
    
def save_wakeup(channel_id: int, person: str, wake_time: datetime, set_by: str):
    if not require_supabase():
        return {"ok": False, "reason": "Supabase not configured"}

    response = (
        supabase.table("metiche_wakeups")
        .insert({
            "channel_id": str(channel_id),
            "person": person,
            "wake_time": wake_time.isoformat(),
            "status": "scheduled",
            "set_by": set_by,
        })
        .execute()
    )

    return {"ok": True, "data": response.data}


def fetch_due_wakeups(now: datetime):
    if not require_supabase():
        return []

    response = (
        supabase.table("metiche_wakeups")
        .select("*")
        .eq("status", "scheduled")
        .lte("wake_time", now.isoformat())
        .execute()
    )

    return response.data or []


def mark_wakeup_sent(wakeup_id: str):
    if not require_supabase():
        return

    (
        supabase.table("metiche_wakeups")
        .update({
            "status": "sent",
            "sent_at": datetime.now().isoformat(),
        })
        .eq("id", wakeup_id)
        .execute()
    )

# ---------- Project task persistence ----------

def find_chisme_contacts(lookup, limit=5):
    lookup = (lookup or "").strip()
    if not lookup or not require_supabase():
        return []

    return (
        supabase.table("chisme_contacts")
        .select("*")
        .ilike("name", f"%{lookup}%")
        .limit(limit)
        .execute()
    ).data or []


def format_chisme_match_list(matches):
    lines = ["I found multiple possible Rolodex cards:\n"]
    for i, c in enumerate(matches, 1):
        lines.append(
            f"{i}. **{c.get('name')}** — "
            f"{c.get('phone') or 'no phone'} — "
            f"{c.get('address') or c.get('source') or 'no clue'}"
        )
    lines.append("\nReply with the number.")
    return "\n".join(lines)


def fetch_project_tasks(contact_id, project_name=None):
    if not require_supabase():
        return []

    query = (
        supabase.table("metiche_project_tasks")
        .select("*")
        .eq("contact_id", contact_id)
    )

    if project_name:
        query = query.eq("project_name", project_name)

    return (
        query
        .order("sort_order")
        .order("created_at")
        .execute()
    ).data or []


def insert_project_tasks(contact_id, project_name, task_texts):
    if not require_supabase():
        return []

    rows = []
    for idx, text in enumerate(task_texts, start=1):
        text = str(text or "").strip()
        if not text:
            continue

        rows.append({
            "contact_id": contact_id,
            "project_name": project_name,
            "task_text": text,
            "sort_order": idx,
            "completed": False,
            "actual_minutes": 0,
            "estimated_minutes": 0,
        })

    if not rows:
        return []

    return supabase.table("metiche_project_tasks").insert(rows).execute().data or []


def calculate_project_progress(tasks):
    total = len(tasks or [])
    done = len([t for t in tasks if t.get("completed")])
    percent = round((done / total) * 100) if total else 0
    actual_minutes = sum(int(t.get("actual_minutes") or 0) for t in tasks)

    return {
        "done": done,
        "total": total,
        "percent": percent,
        "actual_minutes": actual_minutes,
    }
def fetch_customer_projects(contact_id):
    if not require_supabase():
        return []

    rows = (
        supabase.table("metiche_project_tasks")
        .select("project_name")
        .eq("contact_id", contact_id)
        .execute()
    ).data or []

    projects = []
    seen = set()

    for row in rows:
        name = row.get("project_name")
        if name and name not in seen:
            projects.append(name)
            seen.add(name)

    return projects
def format_project_tasks(contact, project_name, tasks):
    progress = calculate_project_progress(tasks)
    lines = [
        f"📋 **{contact.get('name')} — {project_name}**",
        f"Ready: {progress['percent']}% ({progress['done']} / {progress['total']})",
        "",
    ]

    if not tasks:
        lines.append("No tasks yet.")
        return "\n".join(lines)

    for idx, task in enumerate(tasks, start=1):
        mark = "✅" if task.get("completed") else "⬜"
        minutes = int(task.get("actual_minutes") or 0)
        time_label = f" ({minutes_to_label(minutes)})" if minutes else ""
        lines.append(f"{mark} {idx}. {task.get('task_text')}{time_label}")

    return "\n".join(lines)
    
# ---------- Routine persistence ----------

def fetch_active_routine(person: str = "Daniel") -> Optional[Dict[str, Any]]:
    if not require_supabase():
        return None

    response = (
        supabase.table("metiche_routines")
        .select("*")
        .eq("user_id", person.lower())
        .eq("active", True)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )

    rows = response.data or []
    return rows[0] if rows else None


def save_routine(person: str, routine_name: str, routine_text: str):
    if not require_supabase():
        return {"ok": False, "reason": "Supabase not configured"}

    user_id = person.lower()

    (
        supabase.table("metiche_routines")
        .update({"active": False})
        .eq("user_id", user_id)
        .execute()
    )

    response = (
        supabase.table("metiche_routines")
        .insert({
            "user_id": user_id,
            "routine_name": routine_name,
            "routine_text": routine_text,
            "active": True,
        })
        .execute()
    )

    return {"ok": True, "data": response.data}
# ---------- Persistent ping schedules ----------

def save_ping_schedule(
    channel_id: int,
    user_id: int,
    person: str,
    interval_minutes: int,
    prompt: str,
    source: str = "mtoday",
):
    if not require_supabase():
        return {"ok": False, "reason": "Supabase not configured"}

    next_ping_at = local_now() + timedelta(minutes=interval_minutes)
    
    response = (
        supabase.table("metiche_ping_schedules")
        .upsert({
            "channel_id": str(channel_id),
            "user_id": str(user_id),
            "person": person,
            "interval_minutes": interval_minutes,
            "next_ping_at": next_ping_at.isoformat(),
            "prompt": prompt,
            "source": source,
            "status": "active",
             "is_on": True,
        }, on_conflict="channel_id,person")
        .execute()
    )

    return {"ok": True, "data": response.data}

def fetch_due_pings(now: datetime):
    if not require_supabase():
        return []

    response = (
        supabase.table("metiche_ping_schedules")
        .select("*")
        .eq("status", "active")
        .eq("is_on", True)
        .lte("next_ping_at", now.isoformat())
        .execute()
    )

    return response.data or []


def advance_ping_schedule(ping_id: str, interval_minutes: int):
    if not require_supabase():
        return

    next_ping_at = local_now() + timedelta(minutes=interval_minutes)

    (
        supabase.table("metiche_ping_schedules")
        .update({
            "updated_at": local_now().isoformat(),
            "next_ping_at": next_ping_at.isoformat(),
        })
        .eq("id", ping_id)
        .execute()
    )


def stop_ping_schedules(channel_id: int):
    if not require_supabase():
        return
    (
        supabase.table("metiche_ping_schedules")
        .update({
            "status": "stopped",
            "is_on": False,
        })
        .eq("channel_id", str(channel_id))
        .eq("status", "active")
        .execute()
    )
    
def fetch_ping_schedule(channel_id: int, person: str):
    if not require_supabase():
        return None

    response = (
        supabase.table("metiche_ping_schedules")
        .select("*")
        .eq("channel_id", str(channel_id))
        .eq("person", person)
        .limit(1)
        .execute()
    )

    rows = response.data or []
    return rows[0] if rows else None


def sync_ping_focus(
    channel_id: int,
    person: str,
    focus: Optional[str],
    pause: bool = False,
):
    """
    Keep an existing Que Onda schedule aligned with the current mtoday focus.

    Does NOT create a ping schedule if the user did not already have one.
    """
    ping = fetch_ping_schedule(channel_id, person)

    if not ping:
        return

    # Do not resurrect a schedule the user deliberately stopped.
    if ping.get("status") == "stopped":
        return

    interval = int(ping.get("interval_minutes") or 120)

    if pause or not focus:
        (
            supabase.table("metiche_ping_schedules")
            .update({
                "status": "paused",
                "is_on": False,
                "updated_at": local_now().isoformat(),
            })
            .eq("id", ping["id"])
            .execute()
        )
        return

    (
        supabase.table("metiche_ping_schedules")
        .update({
            "status": "active",
            "is_on": True,
            "prompt": f"¿Qué onda? Still on {focus}, or did something change?",
            "next_ping_at": (
                local_now() + timedelta(minutes=interval)
            ).isoformat(),
            "updated_at": local_now().isoformat(),
        })
        .eq("id", ping["id"])
        .execute()
    )
#----------Behavior Identification/Reprogramming---------

def save_mdice_entry(
    person,
    statement,
    reason,
    discord_user_id,
    channel_id,
):
    if not require_supabase():
        return {"ok": False, "reason": "Supabase not configured"}

    response = (
        supabase.table("metiche_reprogramming_logs")
        .insert({
            "person": person,
            "statement": statement,
            "reason": reason,
            "discord_user_id": str(discord_user_id),
            "channel_id": str(channel_id),
            "status": "open",
        })
        .execute()
    )

    return {"ok": True, "data": response.data}
    
def save_important_items(
    person: str,
    discord_user: str,
    channel_id: str,
    items: List[str],
):
    if not require_supabase():
        return {"ok": False, "reason": "Supabase not configured"}

    inserts = [
        {
            "person": person,
            "discord_user": discord_user,
            "channel_id": channel_id,
            "item": item,
            "status": "open",
        }
        for item in items
        if item.strip()
    ]

    if not inserts:
        return {"ok": True, "data": []}

    response = supabase.table("metiche_important_items").insert(inserts).execute()
    return {"ok": True, "data": response.data}
    

def fetch_important_items(
    person: str,
    discord_user: str,
    channel_id: str,
    include_cancelled: bool = False,
) -> List[Dict[str, Any]]:
    if not require_supabase():
        return []

    query = (
        supabase.table("metiche_important_items")
        .select("id, person, discord_user, channel_id, item, status, created_at")
        .eq("person", person)
        .eq("discord_user", discord_user)
        .eq("channel_id", channel_id)
        .order("created_at")
    )

    if not include_cancelled:
        query = query.neq("status", "cancelled")

    response = query.execute()
    return response.data or []


def update_important_item_status(item_ids: List[Any], status: str) -> int:
    if not require_supabase() or not item_ids:
        return 0

    updated = 0
    for item_id in item_ids:
        response = (
            supabase.table("metiche_important_items")
            .update({"status": status})
            .eq("id", item_id)
            .execute()
        )
        updated += len(response.data or [])

    return updated


def format_important_items(items: List[Dict[str, Any]]) -> str:
    if not items:
        return (
            "⛽ **Important Action Plan**\n\n"
            "No important items are currently saved.\n"
            "Use `!mbraindump` and place actions under `I:` to create a plan."
        )

    icons = {
        "open": "⬜",
        "today": "🔥",
        "held": "⏸️",
        "completed": "✅",
    }

    lines = ["⛽ **Important Action Plan**", ""]

    for index, item in enumerate(items, start=1):
        status = str(item.get("status") or "open").lower()
        icon = icons.get(status, "⬜")
        label = status.replace("_", " ").title()
        lines.append(f"{icon} **{index}.** {item.get('item', '')}  _[{label}]_")

    completed = sum(
        1 for item in items
        if str(item.get("status") or "").lower() == "completed"
    )

    lines.extend([
        "",
        f"Gas can progress: **{completed} / {len(items)} complete**",
        "",
        "Commands:",
        "`!mimportant today 1,3` — add items to `!mtoday`",
        "`!mimportant done 2` — mark complete and add fuel",
        "`!mimportant hold 4` — intentionally defer",
        "`!mimportant open 4` — return a held item to the plan",
        "`!mimportant remove 3` — cancel an item",
    ])

    return "\n".join(lines)


def parse_important_indexes(raw: str, item_count: int) -> List[int]:
    return parse_task_indexes(raw, item_count)


# ---------- Financial execution logic ----------

def build_financial_execution(
    objective_amount: float,
    due_date: Optional[str],
    objective_reason: str,
    planned_amount: float = 0.0,
) -> FinancialExecution:
    """Build the current financial execution objective.

    ``objective_amount`` is the residual amount still needed after the
    Financial Engine spreadsheet has already accounted for known cash,
    scheduled work, and obligations.
    """
    objective_amount = max(0.0, float(objective_amount or 0.0))
    planned_amount = max(0.0, float(planned_amount or 0.0))
    remaining_gap = max(0.0, objective_amount - planned_amount)

    if remaining_gap <= 0:
        status = "covered"
        priority_tasks = [
            "Confirm the money arrives",
            "Complete the financial checkpoint",
        ]
    else:
        status = "open"
        priority_tasks = [
            f"Close the remaining {format_money(remaining_gap)} gap",
            "Use Customer Communication to pursue work or collections",
            "Use !mbraindump to add the necessary actions to today",
        ]

    return FinancialExecution(
        objective_amount=objective_amount,
        due_date=(due_date or "").strip() or None,
        objective_reason=(objective_reason or "").strip(),
        planned_amount=planned_amount,
        remaining_gap=remaining_gap,
        status=status,
        priority_tasks=priority_tasks,
    )


def financial_execution_to_json(execution: FinancialExecution) -> Dict[str, Any]:
    return {
        "objective_amount": execution.objective_amount,
        "due_date": execution.due_date,
        "objective_reason": execution.objective_reason,
        "planned_amount": execution.planned_amount,
        "remaining_gap": execution.remaining_gap,
        "status": execution.status,
        "priority_tasks": execution.priority_tasks,
    }


def financial_execution_from_plan(plan: Dict[str, Any]) -> FinancialExecution:
    """Load the current objective, including legacy weekly snapshots."""
    task_summary = json_safe_load(plan.get("task_summary_json"), {})
    if not isinstance(task_summary, dict):
        task_summary = {}

    raw = task_summary.get("financial_execution")
    if isinstance(raw, dict) and raw:
        objective_amount = float(raw.get("objective_amount") or 0.0)
        planned_amount = float(raw.get("planned_amount") or 0.0)
        remaining_gap = float(
            raw.get("remaining_gap")
            if raw.get("remaining_gap") is not None
            else max(0.0, objective_amount - planned_amount)
        )
        return FinancialExecution(
            objective_amount=objective_amount,
            due_date=raw.get("due_date"),
            objective_reason=str(raw.get("objective_reason") or ""),
            planned_amount=planned_amount,
            remaining_gap=remaining_gap,
            status=str(raw.get("status") or ("covered" if remaining_gap <= 0 else "open")),
            priority_tasks=raw.get("priority_tasks") or [],
        )

    # Backward compatibility for snapshots created by the old !mweekly flow.
    legacy = task_summary.get("weekly_execution")
    if not isinstance(legacy, dict):
        legacy = {}

    legacy_gap = float(
        legacy.get("revenue_gap")
        or legacy.get("target_amount")
        or plan.get("weekly_goal")
        or 0.0
    )
    return build_financial_execution(
        objective_amount=legacy_gap,
        due_date=None,
        objective_reason="Legacy weekly target" if legacy_gap else "",
        planned_amount=0.0,
    )


def format_money(value: float) -> str:
    return f"${value:,.0f}"


def format_financial_execution_summary(execution: FinancialExecution) -> str:
    due_label = execution.due_date or "Not set"
    purpose = execution.objective_reason or "Not specified"
    status_label = "Covered" if execution.status == "covered" else "Open"

    return (
        "💰 **Financial objective**\n\n"
        f"Additional money needed: **{format_money(execution.objective_amount)}**\n"
        f"Planned toward objective: **{format_money(execution.planned_amount)}**\n"
        f"Remaining gap: **{format_money(execution.remaining_gap)}**\n"
        f"Due: **{due_label}**\n"
        f"Purpose: **{purpose}**\n"
        f"Status: **{status_label}**"
    )


def build_wakeup_message(execution: FinancialExecution, routine: Optional[Dict[str, Any]] = None) -> str:
    routine_text = routine.get("routine_text") if routine else None

    if not routine_text:
        routine_text = (
            "1. Shower + shave\n"
            "2. Get dressed\n"
            "3. Make sit-down breakfast\n"
            "4. Kids pack snacks/lunch boxes from staged counter snacks\n"
            "5. Confirm first job / first work block"
        )

    priorities = "\n".join(f"- {task}" for task in execution.priority_tasks) or "- No financial priority set"
    due_label = execution.due_date or "Not set"
    purpose = execution.objective_reason or "Not specified"

    return (
        "🌅 Daniel morning boot sequence\n"
        f"Audio runway: {DEFAULT_CHILLHOP_URL}\n\n"
        f"{routine_text}\n\n"
        f"Financial objective: {format_money(execution.objective_amount)}\n"
        f"Remaining gap: {format_money(execution.remaining_gap)}\n"
        f"Due: {due_label}\n"
        f"Purpose: {purpose}\n\n"
        f"Priorities:\n{priorities}\n\n"
        "Start with the first physical step. No algorithm hole."
    )


# ---------- display helpers ----------

def format_person_schedule(person: str, person_schedule: Dict[str, List[Any]]) -> str:
    if not person_schedule:
        return f"{person}: (blank)"
    lines = [f"{person}:"]
    for iso_day in sorted(person_schedule.keys()):
        day_label_str = datetime.fromisoformat(iso_day).strftime("%A")
        tasks = normalize_daily_items(person_schedule.get(iso_day, []))
        display = [f"{'✅' if t.get('done') else '⬜'} {t.get('text', '')}" for t in tasks]
        lines.append(f"- {day_label_str} ({iso_day}): " + (", ".join(display) if display else "(blank)"))
    return "\n".join(lines)



def strip_task_sources(person_schedule: Dict[str, List[Any]], hidden_sources: Optional[set] = None) -> Dict[str, List[Any]]:
    hidden_sources = hidden_sources or {"mtoday", "mbraindump"}
    cleaned: Dict[str, List[Any]] = {}
    for iso_day, tasks in (person_schedule or {}).items():
        kept = [
            task for task in normalize_daily_items(tasks)
            if task.get("source") not in hidden_sources
        ]
        if kept:
            cleaned[iso_day] = kept
    return cleaned


def format_person_schedule_strategic(person: str, person_schedule: Dict[str, List[Any]]) -> str:
    strategic = strip_task_sources(person_schedule)
    hidden_count = 0
    for tasks in (person_schedule or {}).values():
        hidden_count += len([
            task for task in normalize_daily_items(tasks)
            if task.get("source") in {"mtoday", "mbraindump"}
        ])

    lines = [format_person_schedule(person, strategic)]
    if hidden_count:
        lines.append(f"\n({hidden_count} daily execution items hidden here. Use !mtoday to see today's working list.)")
    return "\n".join(lines)


def format_daily_tasks(tasks: List[Dict[str, Any]], person: str, date_label_str: str) -> str:
    tasks = normalize_daily_items(tasks)
    if not tasks:
        return "No tasks listed for today yet."
    lines = [f"📋 {person} — {date_label_str}"]
    for idx, task in enumerate(tasks, start=1):
        mark = "✅" if task.get("done") else "⬜"
        lines.append(f"{mark} {idx}. {task.get('text', '')}")
    return "\n".join(lines)


def find_best_task_match(tasks: List[Dict[str, Any]], text: str) -> Optional[int]:
    incoming = normalize_task(text)
    if not incoming:
        return None

    for idx, task in enumerate(tasks):
        if task.get("done"):
            continue
        task_text = normalize_task(task.get("text", ""))
        if task_text and (task_text in incoming or incoming in task_text):
            return idx

    incoming_words = {w for w in re.findall(r"[a-zA-Z0-9]+", incoming) if len(w) > 2}
    best_idx = None
    best_score = 0
    for idx, task in enumerate(tasks):
        if task.get("done"):
            continue
        task_words = {w for w in re.findall(r"[a-zA-Z0-9]+", normalize_task(task.get("text", ""))) if len(w) > 2}
        score = len(incoming_words & task_words)
        if score > best_score:
            best_score = score
            best_idx = idx
    return best_idx if best_score >= 2 else None


def parse_task_indexes(raw: str, task_count: int) -> List[int]:
    """Return zero-based task indexes from input like `4`, `4,5,6`, or `4 5 6`."""
    raw = (raw or "").strip()
    if not raw:
        return []
    if not re.fullmatch(r"\d+(?:\s*[, ]\s*\d+)*", raw):
        return []
    indexes: List[int] = []
    for piece in re.split(r"[,\s]+", raw):
        if not piece:
            continue
        idx = int(piece) - 1
        if 0 <= idx < task_count and idx not in indexes:
            indexes.append(idx)
    return indexes


def resolve_task_indexes(tasks: List[Dict[str, Any]], target: str) -> List[int]:
    """Resolve a number list first, then fall back to literal/fuzzy matching."""
    normalized_tasks = normalize_daily_items(tasks)
    target = (target or "").strip()
    numeric = parse_task_indexes(target, len(normalized_tasks))
    if numeric:
        return numeric
    match_idx = find_best_task_match(normalized_tasks, target)
    return [match_idx] if match_idx is not None else []


def compact_task_lines(tasks: List[Dict[str, Any]]) -> str:
    normalized_tasks = normalize_daily_items(tasks)
    if not normalized_tasks:
        return "(nothing listed)"
    return "\n".join(
        [f"{'✅' if task.get('done') else '⬜'} {idx}. {task.get('text', '')}" for idx, task in enumerate(normalized_tasks, start=1)]
    )


def apply_list_edit(tasks: List[Dict[str, Any]], instruction: str) -> Tuple[List[Dict[str, Any]], str]:
    """Apply a literal list edit without turning bare task numbers into new task text."""
    current = normalize_daily_items(tasks)
    raw = (instruction or "").strip()
    lower = raw.lower()

    if not raw:
        return current, "No change made."

    if lower.startswith("add "):
        new_items = parse_named_list(raw[4:].strip())
        for item in new_items:
            current.append({"text": item, "done": False, "source": "mtoday_add"})
        return current, f"Added {len(new_items)} item(s)."

    if lower.startswith("done ") or lower.startswith("check "):
        target = re.sub(r"^(done|check)\s+", "", raw, flags=re.IGNORECASE).strip()
        indexes = resolve_task_indexes(current, target)
        for idx in indexes:
            current[idx]["done"] = True
        return current, f"Checked off {len(indexes)} item(s)." if indexes else "No matching tasks checked off."

    if lower.startswith("remove ") or lower.startswith("drop "):
        target = re.sub(r"^(remove|drop)\s+", "", raw, flags=re.IGNORECASE).strip()
        indexes = set(resolve_task_indexes(current, target))
        if not indexes:
            return current, "No matching tasks removed."
        current = [task for idx, task in enumerate(current) if idx not in indexes]
        return current, f"Removed {len(indexes)} item(s)."

    if lower.startswith("keep "):
        target = re.sub(r"^keep\s+", "", raw, flags=re.IGNORECASE).strip()
        indexes = resolve_task_indexes(current, target)
        if not indexes:
            return current, "No matching tasks kept. No change made."
        current = [task for idx, task in enumerate(current) if idx in indexes]
        return current, f"Kept {len(indexes)} item(s)."

    if lower.startswith("rewrite "):
        rewritten = parse_task_list(raw[8:].strip())
        return (rewritten, f"Rewrote list with {len(rewritten)} item(s).") if rewritten else (current, "Could not parse rewrite. No change made.")

    # Important UX patch: bare numbers in edit mode mean KEEP those task numbers, not create tasks named "4".
    indexes = parse_task_indexes(raw, len(current))
    if indexes:
        current = [task for idx, task in enumerate(current) if idx in indexes]
        return current, f"Kept {len(indexes)} selected item(s)."

    rewritten = parse_task_list(raw)
    return (rewritten, f"Rewrote list with {len(rewritten)} item(s).") if rewritten else (current, "I couldn’t read that edit. No change made.")


# ---------- manager ----------

class MeticheManager:
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.data_service_url = os.getenv("DATA_SERVICE_URL", "").rstrip("/")

    async def start_loop(self):
        print("[METICHE LOOP] started", flush=True)
    
        while True:
            try:
                await asyncio.sleep(30)
                now = local_now()
    
                print(f"[METICHE LOOP] tick {now.isoformat()}", flush=True)
    
                due_wakeups = fetch_due_wakeups(now)
                print(f"[WAKEUPS DUE] {due_wakeups}", flush=True)
    
                for wakeup in due_wakeups:
                    try:
                        print(
                            f"[WAKEUP PROCESSING] id={wakeup.get('id')} "
                            f"channel={wakeup.get('channel_id')} "
                            f"time={wakeup.get('wake_time')}",
                            flush=True,
                        )
    
                        channel = self.bot.get_channel(int(wakeup["channel_id"]))
    
                        if not channel:
                            print(
                                f"[WAKEUP ERROR] Channel not found: {wakeup['channel_id']}",
                                flush=True,
                            )
                            continue
    
                        week = week_of_monday(local_now())
                        _, execution, _, _, _ = current_weekly_context(week)
    
                        routine = fetch_active_routine(
                            wakeup.get("person", "Daniel")
                        )
    
                        await channel.send(
                            build_wakeup_message(execution, routine)
                        )
    
                        mark_wakeup_sent(wakeup["id"])
    
                        print(
                            f"[WAKEUP SENT] id={wakeup['id']}",
                            flush=True,
                        )
    
                    except Exception as e:
                        print(
                            f"[WAKEUP ERROR] {type(e).__name__}: {e}",
                            flush=True,
                        )
                        traceback.print_exc()
    
                due_pings = fetch_due_pings(now)
                print(f"[PING SCHEDULES DUE] {due_pings}", flush=True)
    
                for ping in due_pings:
                    try:
                        if int(ping["channel_id"]) in channels_waiting_for_command:
                            continue
    
                        channel = self.bot.get_channel(
                            int(ping["channel_id"])
                        )
    
                        if not channel:
                            print(
                                f"[PING ERROR] Channel not found: {ping['channel_id']}",
                                flush=True,
                            )
                            continue
    
                        session = active_time_sessions.get(
                            int(ping["channel_id"])
                        )
    
                        interval = int(
                            ping.get("interval_minutes") or 120
                        )
    
                        if session and session.last_activity_timestamp:
                            last_activity = parse_iso(
                                session.last_activity_timestamp
                            )
    
                            idle_minutes = (
                                local_now() - last_activity
                            ).total_seconds() / 60
    
                            if idle_minutes < interval:
                                advance_ping_schedule(
                                    ping["id"],
                                    interval,
                                )
                                continue
    
                        prompt = (
                            ping.get("prompt")
                            or "¿Qué onda? What changed since the last time marker?"
                        )

                        if session:
                            session.awaiting_checkin_response = True
                            session.last_checkin_focus = session.active_task
    
                        await channel.send(prompt)
    
                        advance_ping_schedule(
                            ping["id"],
                            interval,
                        )
    
                    except Exception as e:
                        print(
                            f"[PING ERROR] {type(e).__name__}: {e}",
                            flush=True,
                        )
                        traceback.print_exc()
    
            except Exception as e:
                print(
                    f"[METICHE LOOP ERROR] {type(e).__name__}: {e}",
                    flush=True,
                )
                traceback.print_exc()

    def post_json(self, endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self.data_service_url:
            return {"ok": False, "reason": "DATA_SERVICE_URL not set"}

        url = f"{self.data_service_url}/{endpoint.lstrip('/')}"
        req = request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with request.urlopen(req, timeout=10) as resp:
                return {"ok": True, "status": resp.status, "body": resp.read().decode("utf-8")}
        except error.HTTPError as e:
            return {"ok": False, "reason": f"HTTP {e.code}"}
        except Exception as e:
            return {"ok": False, "reason": str(e)}

    def push_calendar_json(self, person: str, person_schedule: Dict[str, List[Any]]) -> Dict[str, Any]:
        payload = {
            "calendarKey": PERSON_TO_CALENDAR_KEY.get(person),
            "schedule": person_schedule,
        }

        print("PUSHING CALENDAR:", payload)
        result = self.post_json("calendar", payload)
        print("CALENDAR PUSH RESULT:", result)

        return result

    def push_task_summary_json(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.post_json("tasks", payload)


# ---------- persistence wrappers ----------

def get_metiche():
    return metiche_instance


def build_raw_time_payload(session: TimeSession) -> Dict[str, Any]:
    return {
        "mode": "raw_time_accounting",
        "date": session.date_iso,
        "person": session.person,
        "active_task": session.active_task,
        "intended_task": session.intended_task,
        "current_state": session.current_state,
        "paused_task": session.paused_task,
        "last_timestamp": session.last_timestamp,
        "parked_items": session.parked_items,
        "interruptions": session.interruptions,
        "other_tasks_accomplished": session.other_tasks_accomplished,
        "drift_events": session.drift_events,
        "total_minutes": total_minutes(session.blocks),
        "total_label": minutes_to_label(total_minutes(session.blocks)),
        "blocks_logged": len(session.blocks),
        "blocks": session.blocks,
    }
def resolve_focus(tasks: List[Dict[str, Any]], target: str) -> str:
    normalized = normalize_daily_items(tasks)
    raw = (target or "").strip()

    indexes = parse_task_indexes(raw, len(normalized))

    if indexes:
        return ", ".join(
            normalized[idx]["text"]
            for idx in indexes
        )

    return raw


def is_planned_task(tasks: List[Dict[str, Any]], text: str) -> bool:
    target = normalize_task(text)

    return any(
        normalize_task(task.get("text", "")) == target
        for task in normalize_daily_items(tasks)
    )


def add_other_task_accomplished(session: TimeSession, text: str):
    text = (text or "").strip()

    if not text:
        return

    normalized_existing = {
        normalize_task(item)
        for item in session.other_tasks_accomplished
    }

    if (
        not is_planned_task(session.daily_tasks, text)
        and normalize_task(text) not in normalized_existing
    ):
        session.other_tasks_accomplished.append(text)


def looks_like_on_task_checkin(text: str) -> bool:
    lower = normalize_task(text)

    prefixes = (
        "yes",
        "yeah",
        "yep",
        "yup",
        "still",
        "still on",
        "still working",
        "same",
        "on it",
        "working on it",
    )

    return any(lower.startswith(prefix) for prefix in prefixes)


def clean_drift_explanation(text: str) -> str:
    cleaned = (text or "").strip()

    cleaned = re.sub(
        r"^(sorry[\s,.-]*)?(no|nope|nah)[\s,.-]*",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )

    cleaned = re.sub(
        r"^sorry[\s,.-]*",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )

    return cleaned.strip() or text.strip()

def build_task_summary(financial_execution: Optional[FinancialExecution] = None, raw_time: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    if financial_execution is not None:
        payload["financial_execution"] = financial_execution_to_json(financial_execution)
    if raw_time is not None:
        payload["raw_time"] = raw_time
    return payload


def save_weekly_snapshot(
    ctx: commands.Context,
    week: str,
    execution: FinancialExecution,
    calendar_json: Dict[str, Any],
    wants_bodydouble: bool = False,
    quarterly_goals: Optional[List[str]] = None,
    yearly_goals: Optional[List[str]] = None,
    raw_time: Optional[Dict[str, Any]] = None,
):
    insert_metiche_weekly({
        "ts": now_iso(),
        "discord_user": str(ctx.author),
        "channel_id": str(ctx.channel.id),
        "week_of": week,
        # Keep legacy columns populated so the existing table schema does not need to change yet.
        "weekly_goal": execution.objective_amount,
        "jobs_json": "[]",
        "pending_estimates_json": "[]",
        "invoices_to_send_json": "[]",
        "calendar_json": json.dumps(calendar_json, ensure_ascii=False),
        "task_summary_json": json.dumps(build_task_summary(execution, raw_time), ensure_ascii=False),
        "wants_bodydouble": wants_bodydouble,
        "quarterly_goals_json": json.dumps(quarterly_goals or [], ensure_ascii=False),
        "yearly_goals_json": json.dumps(yearly_goals or [], ensure_ascii=False),
    })


def current_weekly_context(week: str) -> Tuple[Dict[str, Any], FinancialExecution, Dict[str, Any], List[str], List[str]]:
    plan = fetch_latest_metiche_weekly(week) or {}
    execution = financial_execution_from_plan(plan)
    calendar_json = ensure_calendar(plan.get("calendar_json"))
    quarterly_goals = json_safe_load(plan.get("quarterly_goals_json") or plan.get("quarterly_goals"), [])
    yearly_goals = json_safe_load(plan.get("yearly_goals_json") or plan.get("yearly_goals"), [])
    return plan, execution, calendar_json, quarterly_goals, yearly_goals


# ---------- registration / commands ----------
def parse_braindump_categories(response: str, items: List[str]) -> Dict[str, List[str]]:
    buckets = {
        "today": [],
        "week": [],
        "hold": [],
        "important": [],
    }

    lines = response.splitlines()

    for line in lines:
        line = line.strip()

        if ":" not in line:
            continue

        prefix, values = line.split(":", 1)
        prefix = prefix.strip().lower()

        indexes = []

        for part in values.split(","):
            part = part.strip()

            if part.isdigit():
                idx = int(part) - 1

                if 0 <= idx < len(items):
                    indexes.append(items[idx])

        if prefix == "t":
            buckets["today"].extend(indexes)
        elif prefix == "w":
            buckets["week"].extend(indexes)
        elif prefix == "h":
            buckets["hold"].extend(indexes)
        elif prefix == "i":
            buckets["important"].extend(indexes)

    return buckets
    
def register_metiche(bot: commands.Bot):
    global metiche_instance
    metiche_instance = MeticheManager(bot)
    
    mdice_waiting = {}
    
    async def push_daily_tasks_to_calendar(ctx: commands.Context, session: TimeSession):
        metiche = get_metiche()
    
        if metiche is None or session.person not in VALID_PEOPLE:
            return
    
        replace_daily_tasks(
            session.person,
            session.date_iso,
            session.daily_tasks,
        )

    async def log_raw_time_block(ctx: commands.Context, activity: str, source: str = "message") -> Optional[Dict[str, Any]]:
        metiche = get_metiche()
        session = active_time_sessions.get(ctx.channel.id)
        if metiche is None:
            await ctx.send("Metiche isn’t initialized yet.")
            return None
        if not session:
            session = TimeSession(
                channel_id=ctx.channel.id,
                person="Unassigned",
                date_iso=today_iso(),
                date_label=today_label(),
                last_timestamp=local_now().isoformat(),
            )
            active_time_sessions[ctx.channel.id] = session
            await ctx.send("Started raw time accounting. Tell me what you were doing at the next time marker.")
            return None

        activity_text = activity.strip()
        if not activity_text:
            return None

        now = local_now()
        duration = max(0, int((now - parse_iso(session.last_timestamp)).total_seconds() // 60))
        block = {
            "date": session.date_iso,
            "start": session.last_timestamp,
            "end": now.isoformat(),
            "duration_minutes": duration,
            "duration_label": minutes_to_label(duration),
            "activity": activity_text,
            "active_task": session.active_task,
            "source": source,
        }
        session.blocks.append(block)
        session.last_timestamp = now.isoformat()

        insert_metiche_checkin({
            "ts": now_iso(),
            "discord_user": str(ctx.author),
            "channel_id": str(ctx.channel.id),
            "week_of": week_of_monday(local_now()),
            "category": RAW_TIME_LABEL,
            "task": activity_text,
            "energy": None,
        })

        push_result = metiche.push_task_summary_json(build_raw_time_payload(session))

        msg = (
            f"⏱️ Logged {minutes_to_label(duration)} on {session.active_task or 'unassigned focus'}\n"
            f"Update: {activity_text}\n"
            f"Total accounted today: {build_raw_time_payload(session)['total_label']}"
        )
                
        if not push_result.get("ok"):
            msg += f"\nSaved, but dashboard push failed: {push_result.get('reason')}"
            
        await ctx.send(msg)
        return block

    async def show_active_day(ctx: commands.Context, session: TimeSession):
        tasks = normalize_daily_items(session.daily_tasks)
    
        lines = [
            f"📋 {session.person} — {session.date_label}",
            "",
        ]
    
        for idx, task in enumerate(tasks, start=1):
            text = task.get("text", "")
            done = bool(task.get("done"))
    
            if done:
                icon = "✅"
            elif (
                session.active_task
                and normalize_task(text) == normalize_task(session.active_task)
            ):
                icon = "🟢"
            else:
                icon = "⬜"
    
            lines.append(f"{icon} {idx}. {text}")
    
        if session.other_tasks_accomplished:
            lines.extend([
                "",
                "**Other tasks accomplished:**",
            ])
    
            for item in session.other_tasks_accomplished:
                lines.append(f"✅ {item}")
    
        if session.drift_events:
            lines.extend([
                "",
                "**Drift:**",
            ])
    
            for event in session.drift_events:
                label = event.get("actual_activity") or event.get("reason") or "unspecified"
                minutes = int(event.get("duration_minutes") or 0)
                time_text = f" — {minutes_to_label(minutes)}" if minutes else ""
                lines.append(f"🌀 {label}{time_text}")
    
        lines.extend([
            "",
            f"🎯 Current: {session.active_task or '(none)'}",
            f"⏱️ Accounted today: {build_raw_time_payload(session)['total_label']}",
        ])
    
        if session.parked_items:
            lines.extend([
                "",
                "**Parked:**",
                *[f"- {item}" for item in session.parked_items],
            ])
    
        await ctx.send("\n".join(lines))

    async def save_active_day_state(ctx: commands.Context, session: TimeSession):
        week = week_of_monday(local_now())
        _, execution, calendar_json, quarterly_goals, yearly_goals = current_weekly_context(week)
        replace_daily_tasks(session.person, session.date_iso, session.daily_tasks)
        save_weekly_snapshot(
            ctx,
            week,
            execution,
            calendar_json,
            wants_bodydouble=True,
            quarterly_goals=quarterly_goals,
            yearly_goals=yearly_goals,
            raw_time=build_raw_time_payload(session),
        )
        metiche = get_metiche()
        if metiche:
            metiche.push_task_summary_json(build_raw_time_payload(session))

    async def handle_active_day_command(ctx: commands.Context, message_text: str) -> bool:
        """Tiny command router for active mtoday sessions.

        This is intentionally boring and literal. It gives Metichebot stable V1 verbs
        instead of trying to infer every update from freeform text.
        """
        session = active_time_sessions.get(ctx.channel.id)
        if not session or not session.setup_complete:
            return False

        raw = message_text.strip()
        lower = raw.lower()

        if lower in {"show", "list", "status", "what was i doing", "what am i doing"}:
            await show_active_day(ctx, session)
            return True

        if lower.startswith("add "):
            item = raw[4:].strip()
            if not item:
                await ctx.send("Add what? Try `add call inspector`.")
                return True
            session.daily_tasks.append({"text": item, "done": False, "source": "mtoday_add"})
            await save_active_day_state(ctx, session)
            await ctx.send(f"➕ Added to today: {item}\nCurrent focus remains: {session.active_task or session.paused_task or '(none)'}")
            return True

        if lower.startswith("later ") or lower.startswith("park "):
            item = re.sub(r"^(later|park)\s+", "", raw, flags=re.IGNORECASE).strip()
            if not item:
                await ctx.send("Park what for later?")
                return True
            session.parked_items.append(item)
            await save_active_day_state(ctx, session)
            await ctx.send(f"🅿️ Parked for later: {item}\nNot added to the active queue.")
            return True

        if lower.startswith("drift"):
            explanation = re.sub(
                r"^drift\s*",
                "",
                raw,
                flags=re.IGNORECASE,
            ).strip() or "unspecified drift"
        
            previous_focus = session.active_task
        
            now = local_now()
            duration = max(
                0,
                int(
                    (
                        now - parse_iso(session.last_timestamp)
                    ).total_seconds() // 60
                ),
            )
        
            block = {
                "date": session.date_iso,
                "start": session.last_timestamp,
                "end": now.isoformat(),
                "duration_minutes": duration,
                "duration_label": minutes_to_label(duration),
                "planned_focus": previous_focus,
                "actual_activity": explanation,
                "classification": "drift",
                "reason": explanation,
                "source": "drift",
            }
        
            session.blocks.append(block)
            session.last_timestamp = now.isoformat()
        
            session.drift_events.append({
                "ts": now.isoformat(),
                "planned_focus": previous_focus,
                "actual_activity": explanation,
                "reason": explanation,
                "duration_minutes": duration,
            })
        
            insert_metiche_checkin({
                "ts": now_iso(),
                "discord_user": str(ctx.author),
                "channel_id": str(ctx.channel.id),
                "week_of": week_of_monday(local_now()),
                "category": "drift",
                "task": explanation,
                "energy": None,
            })
        
            # Remember what we meant to be doing,
            # but reality is now the current activity.
            session.intended_task = previous_focus
            session.active_task = explanation
            session.current_state = "drift"
        
            sync_ping_focus(
                ctx.channel.id,
                session.person,
                explanation,
            )
        
            await save_active_day_state(ctx, session)
        
            await ctx.send(
                f"🌀 Drift: {explanation}\n"
                f"⏱️ {minutes_to_label(duration)} since last marker\n"
                f"🎯 Intended: {previous_focus or '(none)'}"
            )
        
            return True

        if lower.startswith("pause"):
            reason = re.sub(r"^pause\s*", "", raw, flags=re.IGNORECASE).strip() or "paused"
            previous_focus = session.active_task
            await log_raw_time_block(ctx, f"pause: {reason}", source="pause")
            session.current_state = "paused"
            session.paused_task = previous_focus
            session.active_task = None
            
            sync_ping_focus(
                ctx.channel.id,
                session.person,
                None,
                pause=True,
            )
            
            session.interruptions.append({
                "type": "pause",
                "reason": reason,
                "ts": local_now().isoformat(),
                "paused_task": previous_focus,
            })
            await save_active_day_state(ctx, session)
            await ctx.send(f"⏸️ Paused: {previous_focus or '(no active focus)'}\nReason: {reason}")
            return True

        if lower.startswith("resume"):
            target = re.sub(r"^resume\s*", "", raw, flags=re.IGNORECASE).strip() or session.paused_task or session.active_task
            if not target:
                await ctx.send("Resume what? Try `resume kitchen`.")
                return True
            await log_raw_time_block(ctx, f"resume: {target}", source="resume")
            session.active_task = target

            sync_ping_focus(
                ctx.channel.id,
                session.person,
                target,
            )
            
            session.paused_task = None
            session.current_state = "active"
            await save_active_day_state(ctx, session)
            await ctx.send(f"▶️ Resumed: {target}")
            return True

        if lower.startswith("switch "):
            target = resolve_focus(
                session.daily_tasks,
                raw[7:].strip(),
            )
            if not target:
                await ctx.send("Switch to what?")
                return True
        
            previous_focus = session.active_task
        
            await log_raw_time_block(
                ctx,
                f"switch from {previous_focus or 'unassigned'} to {target}",
                source="switch",
            )
        
            if (
                previous_focus
                and not is_planned_task(session.daily_tasks, previous_focus)
            ):
                add_other_task_accomplished(
                    session,
                    previous_focus,
                )
        
            session.active_task = target
        
            sync_ping_focus(
                ctx.channel.id,
                session.person,
                target,
            )
        
            session.current_state = "active"
            session.paused_task = None
            await save_active_day_state(ctx, session)
            await ctx.send(
                f"🔀 Switched focus:\n"
                f"From: {previous_focus or '(none)'}\n"
                f"To: {target}"
            )
            return True
            

        if lower.startswith("ping ") or lower.startswith("pings "):
            raw_interval = re.sub(r"^pings?\s+", "", raw, flags=re.IGNORECASE).strip().lower()
            if raw_interval in {"none", "no", "off", "0"}:
                stop_ping_schedules(ctx.channel.id)
                await ctx.send("🔕 Que Onda pings are off for this channel.")
                return True
            try:
                interval = int(raw_interval)
            except ValueError:
                await ctx.send("I couldn’t read that ping interval. Try `ping 30`, `ping 60`, or `ping none`.")
                return True
            save_ping_schedule(
                channel_id=ctx.channel.id,
                user_id=ctx.author.id,
                person=session.person,
                interval_minutes=interval,
                prompt=f"¿Qué onda? Still on {session.active_task or session.paused_task or 'your current focus'}, or did something change?",
            )
            await ctx.send(f"🔔 Que Onda pings set for every {interval} minutes.")
            return True
            
        if session.awaiting_checkin_response:
            session.awaiting_checkin_response = False
        
            if looks_like_on_task_checkin(raw):
                block = await log_raw_time_block(
                    ctx,
                    f"on task: {session.active_task or raw}",
                    source="que_onda_on_task",
                )
        
                session.current_state = "active"
                session.intended_task = session.active_task
        
                await save_active_day_state(ctx, session)
        
                duration = block.get("duration_label") if block else "0m"
        
                await ctx.send(
                    f"👍 Still on {session.active_task or 'current focus'} — {duration}"
                )
        
                return True
        
            # Anything that says reality differs from the ping
            # becomes drift without requiring the word "drift".
            explanation = clean_drift_explanation(raw)
            previous_focus = session.active_task
        
            now = local_now()
            duration = max(
                0,
                int(
                    (
                        now - parse_iso(session.last_timestamp)
                    ).total_seconds() // 60
                ),
            )
        
            block = {
                "date": session.date_iso,
                "start": session.last_timestamp,
                "end": now.isoformat(),
                "duration_minutes": duration,
                "duration_label": minutes_to_label(duration),
                "planned_focus": previous_focus,
                "actual_activity": explanation,
                "classification": "drift",
                "reason": explanation,
                "source": "que_onda_drift",
            }
        
            session.blocks.append(block)
            session.last_timestamp = now.isoformat()
        
            session.drift_events.append({
                "ts": now.isoformat(),
                "planned_focus": previous_focus,
                "actual_activity": explanation,
                "reason": explanation,
                "duration_minutes": duration,
            })
        
            insert_metiche_checkin({
                "ts": now_iso(),
                "discord_user": str(ctx.author),
                "channel_id": str(ctx.channel.id),
                "week_of": week_of_monday(local_now()),
                "category": "drift",
                "task": explanation,
                "energy": None,
            })
        
            session.intended_task = previous_focus
            session.active_task = explanation
            session.current_state = "drift"
        
            sync_ping_focus(
                ctx.channel.id,
                session.person,
                explanation,
            )
        
            await save_active_day_state(ctx, session)
        
            await ctx.send(
                f"🌀 Drift: {explanation}\n"
                f"⏱️ {minutes_to_label(duration)}\n"
                f"🎯 Intended: {previous_focus or '(none)'}"
            )
        
            return True

        if lower.startswith("done") or lower.startswith("check "):
            target = re.sub(
                r"^(done|check)\s*",
                "",
                raw,
                flags=re.IGNORECASE,
            ).strip() or session.active_task
        
            if not target:
                await ctx.send("Done with what?")
                return True
        
            finished_focus = session.active_task
        
            block = await log_raw_time_block(
                ctx,
                f"done: {target}",
                source="done",
            )
        
            tasks = normalize_daily_items(session.daily_tasks)
            indexes = resolve_task_indexes(tasks, target)
        
            checked_labels = []
        
            for idx in indexes:
                tasks[idx]["done"] = True
                checked_labels.append(tasks[idx].get("text", ""))
        
            session.daily_tasks = tasks
        
            # The thing we were doing is finished.
            session.active_task = None
        
            pending = [
                task
                for task in tasks
                if not task.get("done")
            ]
        
            duration = block.get("duration_label") if block else "0m"
        
            if len(pending) == 1:
                # Only one sensible next thing. Just move into it.
                next_focus = pending[0]["text"]
                session.active_task = next_focus
        
                sync_ping_focus(
                    ctx.channel.id,
                    session.person,
                    next_focus,
                )
        
                await save_active_day_state(ctx, session)
        
                await ctx.send(
                    f"✅ {target} — {duration}\n"
                    f"🟢 Next: {next_focus}"
                )
        
                return True
        
            if not pending:
                # Day/list is complete. Nothing left for Que Onda to monitor.
                sync_ping_focus(
                    ctx.channel.id,
                    session.person,
                    None,
                    pause=True,
                )
        
                await save_active_day_state(ctx, session)
        
                await ctx.send(
                    f"✅ {target} — {duration}\n"
                    "🏁 Nothing else pending."
                )
        
                return True
        
            # Multiple possible next tasks.
            # Don't pretend we know which one Heaven chose.
            sync_ping_focus(
                ctx.channel.id,
                session.person,
                None,
                pause=True,
            )
        
            await save_active_day_state(ctx, session)
        
            pending_text = "\n".join(
                f"{i}. {task['text']}"
                for i, task in enumerate(pending, start=1)
            )
        
            await ctx.send(
                f"✅ {target} — {duration}\n\n"
                f"What's next?\n{pending_text}"
            )
        
            return True

        # No current focus means Metiche asked "What's next?"
        # Treat the user's ordinary reply as the new focus.
        if not session.active_task and session.current_state == "active":
            tasks = normalize_daily_items(session.daily_tasks)
            
            pending = [
                task
                for task in tasks
                if not task.get("done")
            ]
            
            indexes = parse_task_indexes(raw, len(pending))
            
            if indexes:
                selected = [
                    pending[idx]["text"]
                    for idx in indexes
                ]
                new_focus = ", ".join(selected)
            else:
                new_focus = raw
            
            if new_focus:
                session.active_task = new_focus
            
                sync_ping_focus(
                    ctx.channel.id,
                    session.person,
                    new_focus,
                )
            
                await save_active_day_state(ctx, session)
            
                await ctx.send(
                    f"🟢 Active: {new_focus}"
                )
            
                return True
            
        return False

    @bot.command(name="metichebot")
    async def metichebot_help(ctx):
        await ctx.send(
            "🧠 **METICHEBOT**\n\n"
    
            "Metichebot helps structure:\n"
            "• planning\n"
            "• routines\n"
            "• scheduling\n"
            "• priorities\n"
            "• operational flow\n"
            "• task accounting\n"
            "• body doubling\n"
            "• time awareness\n\n"
    
            "**Planning**\n"
            "`!mengine` — save the current financial gap and execution objective\n"
            "`!mplan` — show the current financial objective and Handley Man schedule\n"
            "`!mschedule` — add/change/replace weekly schedule\n"
            "`!mgoals` — save quarterly and yearly goals\n\n"
    
            "**Daily Operations**\n"
            "`!mbraindump` — dump the messy pile and sort it\n"
            "`!mtoday` — start today's work session\n"
            "`!mshow` — show today's task list\n"
            "`!mstopday` — stop today's work session\n"
            "`!mquiet` — stop active Que Onda pings\n\n"
    
            "**Routines & Wakeups**\n"
            "`!mroutine` — view or edit routines\n"
            "`!mwakeup` — schedule a wakeup sequence\n"
            "`!mping 37` — save a default Que Onda ping interval\n"
            "`!mping off` — disable default Que Onda pings\n\n"
    
            "**During an active `!mtoday` session**\n"
            "Type these WITHOUT `!`:\n\n"
    
            "`show` — current status and pending tasks\n"
            "`done` — complete current focus\n"
            "`done 3` — complete task #3\n"
            "`done clean kitchen` — complete matching task\n\n"
    
            "`add call inspector` — add task to today\n"
            "`later clean garage` — park task for later\n\n"
    
            "`switch estimate followups` — switch focus\n"
            "`pause lunch` — pause current work\n"
            "`resume estimate followups` — resume work\n\n"
    
            "`drift youtube rabbit hole` — log drift without shame\n\n"
    
            "`ping 30` — temporary Que Onda pings for this session\n"
            "`ping none` — stop session pings\n\n"
    
            "**Que Onda**\n"
            "Metichebot can periodically ask:\n"
            "¿Qué onda?\n"
            "Still on task, or did something change?\n"
            "Responses are logged into task accounting."
        )

    @bot.command(name="mengine", aliases=["mweekly"])
    async def mengine(ctx: commands.Context):
        """Save the current residual financial objective.

        The amount entered is the gap that remains after the Financial Engine
        spreadsheet has already accounted for known cash and expected work.
        This command saves the mission; !mbraindump and !mtoday build the work.
        """
        active_time_sessions.pop(ctx.channel.id, None)

        def check(message: discord.Message) -> bool:
            return (
                message.author.id == ctx.author.id
                and message.channel.id == ctx.channel.id
            )

        async def get_response(prompt: str, timeout: int = 300) -> Optional[str]:
            await ctx.send(prompt)
            try:
                message = await bot.wait_for("message", check=check, timeout=timeout)
            except asyncio.TimeoutError:
                await ctx.send(
                    "Financial Engine timed out without saving. "
                    "Run `!mengine` when you're ready to start again."
                )
                return None
            return message.content.strip()

        week = week_of_monday(local_now())
        _, _, calendar_json, quarterly_goals, yearly_goals = current_weekly_context(week)

        amount_raw = await get_response(
            "💰 How much additional money does Handley Man need?"
        )
        if amount_raw is None:
            return

        objective_amount = money_to_float(amount_raw)
        if objective_amount <= 0:
            await ctx.send(
                "Please enter an amount greater than $0. "
                "Run `!mengine` again when you have the current gap."
            )
            return

        due_raw = await get_response(
            "📅 When is this money needed?\n"
            "Examples: `today`, `tomorrow`, `August 4`, or `8/4/2026`."
        )
        if due_raw is None:
            return

        reason_raw = await get_response(
            "🎯 What is this money for?\n"
            "Examples: `owner draw`, `payroll`, `materials`, "
            "`operating expenses`, or `other`."
        )
        if reason_raw is None:
            return

        execution = build_financial_execution(
            objective_amount=objective_amount,
            due_date=due_raw or None,
            objective_reason=reason_raw or "Not specified",
            planned_amount=0.0,
        )

        save_weekly_snapshot(
            ctx,
            week,
            execution,
            calendar_json,
            wants_bodydouble=False,
            quarterly_goals=quarterly_goals,
            yearly_goals=yearly_goals,
        )

        await ctx.send(
            format_financial_execution_summary(execution)
            + "\n\nNext: use `!mbraindump` to build the work required to close this gap."
        )

    @bot.command(name="mplan")
    async def mplan(ctx: commands.Context):
        week = week_of_monday(local_now())
        plan = fetch_latest_metiche_weekly(week) or {}
        if not plan:
            await ctx.send("No weekly plan saved yet. Run !mengine first.")
            return
        execution = financial_execution_from_plan(plan)
        calendar_json = ensure_calendar(plan.get("calendar_json"))
        lines = [format_financial_execution_summary(execution), "", format_person_schedule("Handley Man", calendar_json.get("Handley Man", {}))]
        await ctx.send("\n".join(lines))

    @bot.command(name="mdice")
    async def mdice(ctx: commands.Context, person: str, *, statement: str):
        mdice_waiting[ctx.author.id] = {
            "person": person,
            "statement": statement,
            "channel_id": ctx.channel.id,
        }

        await ctx.send("¿Por qué dice?")
        
    @bot.command(name="mping")
    async def mping(ctx, interval: str):
    
        person = get_person_from_discord(ctx.author.id)
    
        if interval.lower() in {"off", "none", "0"}:
    
            save_default_ping_interval(
                user_id=person,
                interval_minutes=0,
            )
    
            await ctx.send("🔕 Default Que Onda pings disabled.")
            return
    
        try:
            minutes = int(interval)
        except ValueError:
            await ctx.send("Usage: !mping 37")
            return
    
        save_default_ping_interval(
            user_id=person,
            interval_minutes=minutes,
        )
    
        await ctx.send(
            f"🔔 Default Que Onda ping interval saved: {minutes} minutes."
        )

    @bot.command(name="mwakeup")
    async def mwakeup(ctx: commands.Context):
        channels_waiting_for_command.add(ctx.channel.id)
        try:
            def check(m: discord.Message):
                return m.author.id == ctx.author.id and m.channel.id == ctx.channel.id

            await ctx.send(
                "What time should I run Daniel’s wakeup sequence?\n"
                "Example: `7:00 AM` or `4:05 PM`\n"
                "Reply `cancel` to stop."
            )

            while True:
                raw_time = (await bot.wait_for("message", check=check)).content.strip()
                lowered = raw_time.lower()

                if lowered in {"cancel", "done", "stop", "nevermind"}:
                    await ctx.send("Okay. Exiting wakeup setup.")
                    return

                if raw_time.startswith("!"):
                    await ctx.send(
                        "I got another command, so I’m exiting wakeup setup instead of treating that as a time."
                    )
                    return

                wake_time = parse_wakeup_time(raw_time)

                if wake_time is None:
                    await ctx.send(
                        "I couldn’t read that time. Try `7:00 AM`, `6:30`, or reply `cancel`."
                    )
                    continue

                result = save_wakeup(
                    channel_id=ctx.channel.id,
                    person="Daniel",
                    wake_time=wake_time,
                    set_by=str(ctx.author),
                )

                if not result.get("ok"):
                    await ctx.send(f"Failed to save wakeup: {result.get('reason')}")
                    return

                await ctx.send(
                    f"✅ Daniel’s wakeup sequence is scheduled for {wake_time.strftime('%A, %B %-d at %-I:%M %p')}.\n"
                    "Set his actual phone alarm too. I can ping Discord, but I can’t make the phone scream."
                )
                return
        finally:
            channels_waiting_for_command.discard(ctx.channel.id)

    @bot.command(name="mschedule")
    async def mschedule(ctx: commands.Context):
        active_time_sessions.pop(ctx.channel.id, None)
        metiche = get_metiche()
        if metiche is None:
            await ctx.send("Metiche isn’t initialized yet.")
            return

        def check(m: discord.Message):
            return m.author.id == ctx.author.id and m.channel.id == ctx.channel.id

        week = week_of_monday(local_now())
        _, execution, calendar_json, quarterly_goals, yearly_goals = current_weekly_context(week)

        person = get_person_from_discord(ctx.author.id)
        
        full_person_schedule = calendar_json.get(person, {})
        person_schedule = strip_task_sources(full_person_schedule)
        await ctx.send(
            format_person_schedule_strategic(person, full_person_schedule)
            + "\n\nWhat do you want to do?\n1. Add to weekly schedule\n2. Change specific weekly days\n3. Start weekly schedule over\nReply with 1, 2, or 3"
        )
        mode_raw = (await bot.wait_for("message", check=check)).content.strip().lower()
        if mode_raw in {"cancel", "exit", "stop"}:
            await ctx.send("Okay. Exiting schedule flow.")
            return
        mode = {"1": "merge", "2": "modify", "3": "replace"}.get(mode_raw)
        if not mode:
            await ctx.send("Reply with 1, 2, or 3.")
            return

        await ctx.send("Use format:\nMonday: task, task\nTuesday: task")
        incoming = parse_schedule_block((await bot.wait_for("message", check=check)).content, week)
        if not incoming:
            await ctx.send("I couldn’t parse that. Use lines like `Monday: task, task`.")
            return

        if mode == "merge":
            updated = merge_days(person_schedule, incoming)
        elif mode == "modify":
            updated = modify_days(person_schedule, incoming)
        else:
            updated = replace_days(person_schedule, incoming)

        calendar_json[person] = updated
        save_weekly_snapshot(ctx, week, execution, calendar_json, quarterly_goals=quarterly_goals, yearly_goals=yearly_goals)
        push_result = metiche.push_calendar_json(person, updated)
        status = "Pushed to dashboard JSON." if push_result.get("ok") else f"Saved, but dashboard push failed: {push_result.get('reason')}"
        await ctx.send(format_person_schedule(person, updated) + f"\n\n{status}")


    @bot.command(name="mimportant")
    async def mimportant(
        ctx: commands.Context,
        action: str = "show",
        *,
        targets: str = "",
    ):
        person = get_person_from_discord(ctx.author.id)
        discord_user = str(ctx.author)
        channel_id = str(ctx.channel.id)

        items = fetch_important_items(
            person=person,
            discord_user=discord_user,
            channel_id=channel_id,
        )

        normalized_action = (action or "show").strip().lower()

        if normalized_action in {"show", "list", "status"}:
            await ctx.send(format_important_items(items))
            return

        if normalized_action not in {"today", "done", "hold", "open", "remove"}:
            await ctx.send(
                "I don't know that Important action. Try:\n"
                "`!mimportant`\n"
                "`!mimportant today 1,3`\n"
                "`!mimportant done 2`\n"
                "`!mimportant hold 4`\n"
                "`!mimportant open 4`\n"
                "`!mimportant remove 3`"
            )
            return

        indexes = parse_important_indexes(targets, len(items))
        if not indexes:
            await ctx.send(
                "Tell me which item numbers to update. "
                "Example: `!mimportant done 1,3`."
            )
            return

        selected = [items[index] for index in indexes]

        if normalized_action == "today":
            date_key = today_iso()
            existing_today = normalize_daily_items(
                load_daily_tasks(person, date_key)
            )
            existing_names = {
                normalize_task(task.get("text", ""))
                for task in existing_today
            }

            added = 0
            for item in selected:
                text = str(item.get("item") or "").strip()
                normalized = normalize_task(text)

                if text and normalized not in existing_names:
                    existing_today.append({
                        "text": text,
                        "done": False,
                        "source": "mimportant",
                    })
                    existing_names.add(normalized)
                    added += 1

            replace_daily_tasks(person, date_key, existing_today)
            changed = update_important_item_status(
                [item.get("id") for item in selected],
                "today",
            )

            await ctx.send(
                f"🔥 Added **{added}** item(s) to today's work list.\n"
                f"Updated **{changed}** Important item(s) to `today`.\n\n"
                + format_important_items(fetch_important_items(
                    person, discord_user, channel_id
                ))
            )
            return

        status_map = {
            "done": "completed",
            "hold": "held",
            "open": "open",
            "remove": "cancelled",
        }
        new_status = status_map[normalized_action]
        changed = update_important_item_status(
            [item.get("id") for item in selected],
            new_status,
        )

        labels = {
            "done": "✅ Completed",
            "hold": "⏸️ Held",
            "open": "⬜ Reopened",
            "remove": "🗑️ Removed",
        }

        await ctx.send(
            f"{labels[normalized_action]} **{changed}** Important item(s).\n\n"
            + format_important_items(fetch_important_items(
                person, discord_user, channel_id
            ))
        )


    @bot.command(name="mbraindump")
    async def mbraindump(ctx: commands.Context):
        metiche = get_metiche()
        if metiche is None:
            await ctx.send("Metiche isn’t initialized yet.")
            return

        def check(m: discord.Message):
            return m.author.id == ctx.author.id and m.channel.id == ctx.channel.id

        week = week_of_monday(local_now())
        _, execution, calendar_json, quarterly_goals, yearly_goals = current_weekly_context(week)

        await ctx.send(
            "🧠 Brain dump time.\n\n"
            "Drop the whole messy pile here. Use commas or separate lines.\n"
            "Don’t organize it yet."
        )

        raw_dump = (await bot.wait_for("message", check=check)).content.strip()
        dumped_items = parse_named_list(raw_dump)

        if not dumped_items:
            await ctx.send("I didn’t catch any items. Try again with a list or a few lines.")
            return

        preview = (
            "🧠 Here's what you're holding:\n\n"
            + "\n".join(
                [f"{idx + 1}. {item}" for idx, item in enumerate(dumped_items)]
            )
            + "\n\n"
            "What belongs:\n"
            "`T:` Today\n"
            "`W:` This Week\n"
            "`H:` Hold\n"
            "`I:` Important / Attention Required\n\n"
            "Example:\n"
            "T: 1, 3\n"
            "W: 2, 5\n"
            "H: 4\n"
            "I: 6"
        )
        
        await ctx.send(preview)
        
        response = (await bot.wait_for("message", check=check)).content.strip()
        buckets = parse_braindump_categories(response, dumped_items)

        person = get_person_from_discord(ctx.author.id)

        important_result = save_important_items(
            person=person,
            discord_user=str(ctx.author),
            channel_id=str(ctx.channel.id),
            items=buckets["important"],
        )    
        
        date_key = today_iso()
        person = get_person_from_discord(ctx.author.id)
        
        today_tasks = [{"text": item, "done": False, "source": "mbraindump"} for item in buckets["today"]]
        
        existing_today = load_daily_tasks(person, date_key)
        merged_today = normalize_daily_items(existing_today) + today_tasks
        
        replace_daily_tasks(person, date_key, merged_today)
        save_weekly_snapshot(
                ctx,
                week,
                execution,
                calendar_json,
                wants_bodydouble=False,
                quarterly_goals=quarterly_goals,
                yearly_goals=yearly_goals,
                )
        
        if buckets["today"]:
            status = "Added today’s brain dump items to mtoday."
        elif buckets["important"]:
            status = (
                "Saved the Important items as your persistent action plan. "
                "Use `!mimportant` to review or update them."
            )
        else:
            status = "Brain dump saved with no items added to mtoday."

        summary = (
            "🧠 Brain dump sorted.\n\n"
            f"Today: {len(buckets['today'])}\n"
            f"This Week: {len(buckets['week'])}\n"
            f"Hold: {len(buckets['hold'])}\n"
            f"Important: {len(buckets['important'])}\n\n"
            f"{status}\n\n"
            "Do you want to launch today's work session now?\n"
            "`yes` — continue into today's task accounting\n"
            "`later` — stop here"
        )

        await ctx.send(summary)

        launch_reply = (await bot.wait_for("message", check=check)).content.strip().lower()

        if launch_reply not in {"yes", "y"}:
            await ctx.send("Okay. Brain dump is held. Come back when you're ready.")
            return

        await ctx.send("Good. Next step is wiring this directly into the work session.")
                  
    @bot.command(name="mtoday")
    async def mtoday(ctx: commands.Context):
        active_time_sessions.pop(ctx.channel.id, None)
    
        metiche = get_metiche()
        if metiche is None:
            await ctx.send("Metiche isn’t initialized yet.")
            return
    
        def check(m: discord.Message):
            return (
                m.author.id == ctx.author.id
                and m.channel.id == ctx.channel.id
                and not m.content.strip().startswith("!")
            )
    
        week = week_of_monday(local_now())
        _, execution, calendar_json, quarterly_goals, yearly_goals = (
            current_weekly_context(week)
        )
    
        person = get_person_from_discord(ctx.author.id)
        date_key = today_iso()
    
        existing_today = load_daily_tasks(person, date_key)
    
        if not existing_today:
            existing_today = normalize_daily_items(
                calendar_json.get(person, {}).get(date_key, [])
            )
    
        await ctx.send(
            format_daily_tasks(existing_today, person, today_label())
            + "\n\nWhat are you working on?\n"
              "Reply with task number(s), a focus label, `edit`, or `cancel`."
        )
    
        choice = (
            await bot.wait_for("message", check=check)
        ).content.strip()
    
        if choice.lower() == "cancel":
            await ctx.send("Okay.")
            return
    
        while choice.lower() == "edit":
            await ctx.send(
                "`add task, task` — add one or more\n"
                "`done 2,4` — check off tasks\n"
                "`remove 3` — remove tasks\n"
                "`keep 1,2,5` — keep only those tasks\n"
                "`rewrite task, task` — replace the list"
            )
    
            edited = (
                await bot.wait_for("message", check=check)
            ).content.strip()
    
            existing_today, edit_message = apply_list_edit(
                existing_today,
                edited,
            )
    
            replace_daily_tasks(
                person,
                date_key,
                existing_today,
            )
    
            await ctx.send(
                f"{edit_message}\n\n"
                + format_daily_tasks(
                    existing_today,
                    person,
                    today_label(),
                )
                + "\n\nWhat are you working on?"
            )
    
            choice = (
                await bot.wait_for("message", check=check)
            ).content.strip()
    
            if choice.lower() == "cancel":
                await ctx.send("Okay.")
                return
    
        tasks = normalize_daily_items(existing_today)
    
        selected_indexes = parse_task_indexes(
            choice,
            len(tasks),
        )
    
        if selected_indexes:
            selected_tasks = [
                tasks[idx]["text"]
                for idx in selected_indexes
            ]
    
            active_focus = ", ".join(selected_tasks)
    
        else:
            active_focus = choice.strip()
    
        if not active_focus:
            await ctx.send("I need a task or focus before starting.")
            return
    
        now = local_now().isoformat()
    
        session = TimeSession(
            channel_id=ctx.channel.id,
            person=person,
            date_iso=date_key,
            date_label=today_label(),
            last_timestamp=now,
            last_activity_timestamp=now,
            active_task=active_focus,
            daily_tasks=tasks,
        )
    
        active_time_sessions[ctx.channel.id] = session
    
        replace_daily_tasks(
            person,
            date_key,
            session.daily_tasks,
        )
    
        save_weekly_snapshot(
            ctx,
            week,
            execution,
            calendar_json,
            wants_bodydouble=True,
            quarterly_goals=quarterly_goals,
            yearly_goals=yearly_goals,
            raw_time=build_raw_time_payload(session),
        )
    
        metiche.push_task_summary_json(
            build_raw_time_payload(session)
        )
    
        selected_normalized = {
            normalize_task(tasks[idx]["text"])
            for idx in selected_indexes
        }
    
        pending_tasks = [
            task
            for task in tasks
            if not task.get("done")
            and normalize_task(task.get("text", ""))
            not in selected_normalized
        ]
    
        pending_text = (
            "\n".join(
                f"- {task['text']}"
                for task in pending_tasks
            )
            or "(nothing else pending)"
        )
    
        session.setup_complete = True
    
        await save_active_day_state(
            ctx,
            session,
        )
    
        pref = fetch_default_ping_interval(person)
    
        if (
            pref
            and int(pref.get("interval_minutes") or 0) > 0
            and bool(pref.get("is_enabled", True))
        ):
            interval = int(pref["interval_minutes"])
    
            save_ping_schedule(
                channel_id=ctx.channel.id,
                user_id=ctx.author.id,
                person=person,
                interval_minutes=interval,
                prompt=(
                    f"¿Qué onda? Still on {active_focus}, "
                    "or did something change?"
                ),
            )
    
        await ctx.send(
            f"🟢 Active:\n{active_focus}\n\n"
            f"⏳ Pending:\n{pending_text}"
        )

    @bot.command(name="mstopday")
    async def mstopday(ctx: commands.Context):
        session = active_time_sessions.pop(ctx.channel.id, None)
        stop_ping_schedules(ctx.channel.id)
        
        if not session:
            await ctx.send("No active time session was running.")
            return    
        payload = build_raw_time_payload(session)
        await ctx.send(f"Stopped today’s time session.\nTotal accounted: {payload['total_label']}\nBlocks logged: {payload['blocks_logged']}")

    @bot.command(name="mquiet")
    async def mquiet(ctx: commands.Context):
        stop_ping_schedules(ctx.channel.id)
        await ctx.send("Okay. I stopped the check-in pings.")
        
    @bot.command(name="mtasks")
    async def mtasks(ctx: commands.Context, *, lookup: str = ""):
        if not lookup.strip():
            await ctx.send("Use: `!mtasks Customer Name`")
            return

        def check(m: discord.Message):
            return m.author.id == ctx.author.id and m.channel.id == ctx.channel.id

        matches = find_chisme_contacts(lookup)

        if not matches:
            await ctx.send(
                f"No Chismebot Rolodex card found for **{lookup}**.\n"
                f"Create the customer first with Chismebot, then come back to `!mtasks`."
            )
            return

        if len(matches) > 1:
            await ctx.send(
                format_chisme_match_list(matches) +
                "\n\nReply with the number."
            )
        
            choice = (await bot.wait_for("message", check=check)).content.strip()
        
            if not choice.isdigit():
                await ctx.send("I couldn’t read that number.")
                return
        
            idx = int(choice) - 1
        
            if idx < 0 or idx >= len(matches):
                await ctx.send("That number is out of range.")
                return
        
            contact = matches[idx]
        else:
            contact = matches[0]
    
        projects = fetch_customer_projects(contact["id"])

        if not projects:
            await ctx.send(
                f"Found Rolodex card: **{contact.get('name')}**.\n\n"
                "What project are we working on?"
            )
            project_name = (await bot.wait_for("message", check=check)).content.strip()

            if not project_name or project_name.lower() in {"cancel", "stop", "nevermind"}:
                await ctx.send("Okay. No project task list created.")
                return

            await ctx.send(
                f"Cool. **{contact.get('name')}**, we are working on **{project_name}**\n\n"
                "Paste the honey-do list now — **one task per line**."
            )

            raw_tasks = (await bot.wait_for("message", check=check)).content.strip()

            if not raw_tasks or raw_tasks.lower() in {"cancel", "stop", "nevermind"}:
                await ctx.send("Okay. No tasks added.")
                return

            task_texts = [
                line.strip("-• 1234567890.").strip()
                for line in raw_tasks.splitlines()
                if line.strip()
            ]

            inserted = insert_project_tasks(contact["id"], project_name, task_texts)
            tasks = fetch_project_tasks(contact["id"], project_name)

            await ctx.send(
                f"✅ Added {len(inserted)} task(s).\n\n"
                + format_project_tasks(contact, project_name, tasks)
            )
            return

        if len(projects) == 1:
            project_name = projects[0]
            tasks = fetch_project_tasks(contact["id"], project_name)

            await ctx.send(
                f"🔥 **{contact.get('name')}**\n"
                f"We are working on **{project_name}**.\n\n"
                + format_project_tasks(contact, project_name, tasks)
            )
            return

        lines = [
            f"**{contact.get('name')}** has multiple projects:",
            "",
        ]
        for idx, project in enumerate(projects, start=1):
            lines.append(f"{idx}. {project}")

        lines.append("\nWhich project? Reply with the number.")
        await ctx.send("\n".join(lines))

        choice = (await bot.wait_for("message", check=check)).content.strip()

        if not choice.isdigit():
            await ctx.send("I couldn’t read that project number.")
            return

        idx = int(choice) - 1

        if idx < 0 or idx >= len(projects):
            await ctx.send("That project number is out of range.")
            return

        project_name = projects[idx]
        tasks = fetch_project_tasks(contact["id"], project_name)

        await ctx.send(format_project_tasks(contact, project_name, tasks))

    @bot.command(name="mshow")
    async def mshow(ctx: commands.Context):
        session = active_time_sessions.get(ctx.channel.id)
        if not session:
            person = get_person_from_discord(ctx.author.id)
            date_key = today_iso()
            tasks = load_daily_tasks(person, date_key)
            await ctx.send(format_daily_tasks(tasks, person, today_label()))
            return
        await show_active_day(ctx, session)

    @bot.command(name="mdone")
    async def mdone(ctx: commands.Context, *, target: str = ""):
        session = active_time_sessions.get(ctx.channel.id)
        if not session:
            await ctx.send("No active `!mtoday` session is running. Start one with `!mtoday`, or use `!mshow` to see today's list.")
            return
        target = target.strip() or session.active_task
        if not target:
            await ctx.send("Done with what? Try `!mdone 2` or `!mdone clean kitchen`.")
            return
        block = await log_raw_time_block(ctx, f"done: {target}", source="done")
        tasks = normalize_daily_items(session.daily_tasks)
        indexes = resolve_task_indexes(tasks, target)
        checked_labels = []
        for idx in indexes:
            tasks[idx]["done"] = True
            checked_labels.append(tasks[idx].get("text", ""))
        session.daily_tasks = tasks
        if normalize_task(target) == normalize_task(session.active_task or "") or (len(indexes) == 1 and normalize_task(tasks[indexes[0]].get("text", "")) == normalize_task(session.active_task or "")):
            session.active_task = None
        await save_active_day_state(ctx, session)
        duration = block.get("duration_label") if block else "0m"
        checked = "\n✅ Checked off:\n" + "\n".join([f"- {label}" for label in checked_labels]) if checked_labels else "\n⚠️ I logged it, but I didn’t find a matching task to check off."
        await ctx.send(f"✅ Done: {target}\nTime since last marker: {duration}{checked}")

    @bot.command(name="mgoals")
    async def mgoals(ctx: commands.Context):
        def check(m: discord.Message):
            return m.author.id == ctx.author.id and m.channel.id == ctx.channel.id

        week = week_of_monday(local_now())
        _, execution, calendar_json, _, _ = current_weekly_context(week)

        await ctx.send("What are the quarterly goals? Comma-separated, or `none`.")
        quarterly_goals = parse_named_list((await bot.wait_for("message", check=check)).content)
        await ctx.send("What are the yearly goals? Comma-separated, or `none`.")
        yearly_goals = parse_named_list((await bot.wait_for("message", check=check)).content)

        save_weekly_snapshot(ctx, week, execution, calendar_json, quarterly_goals=quarterly_goals, yearly_goals=yearly_goals)
        await ctx.send("Locked. I saved your quarterly and yearly goals.")

    @bot.command(name="mwhoami")
    async def mwhoami(ctx):
        await ctx.send(
            f"Discord ID: {ctx.author.id}\n"
            f"Discord Name: {ctx.author.name}\n"
            f"Mapped Person: {get_person_from_discord(ctx.author.id)}"
        )

    @bot.listen("on_message")
    async def metiche_time_listener(message: discord.Message):
        
        if message.author.bot:
            return
            
        if message.author.id in mdice_waiting:

            pending = mdice_waiting.pop(message.author.id)

            save_mdice_entry(
                person=pending["person"],
                statement=pending["statement"],
                reason=message.content,
                discord_user_id=message.author.id,
                channel_id=message.channel.id,
            )

            await message.channel.send(
                f"📝 Logged reprogramming entry for {pending['person']}."
            )

            return
        
        session = active_time_sessions.get(message.channel.id)
        if session:
            session.last_activity_timestamp = local_now().isoformat()
        
        if message.content.startswith("!"):
            return

        metiche = get_metiche()
        if metiche is None:
            return

        ctx = await bot.get_context(message)

        if session and session.setup_complete:
            handled = await handle_active_day_command(ctx, message.content)
            if handled:
                return
            await log_raw_time_block(ctx, message.content, source="active_day")
            return
