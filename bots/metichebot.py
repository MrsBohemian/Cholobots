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
- Weekly operational forecasting (!mweekly)
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
    # Replace these with real Discord user IDs
    123456789: "Heaven",
    987654321: "Daniel",
    555555555: "Jesse",
}

def get_person_from_discord(author_id: int) -> str:
    return DISCORD_USER_TO_PERSON.get(author_id, "Heaven")
    
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
    active_task: Optional[str] = None
    setup_complete: bool = False
    current_state: str = "active"  # active / paused / drift / transition
    paused_task: Optional[str] = None
    blocks: List[Dict[str, Any]] = field(default_factory=list)
    daily_tasks: List[Dict[str, Any]] = field(default_factory=list)
    parked_items: List[str] = field(default_factory=list)
    interruptions: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class WeeklyExecution:
    target_amount: float = 0.0
    scheduled_revenue: float = 0.0
    outstanding_estimate_value: float = 0.0
    pending_invoice_value: float = 0.0
    earning_jobs: List[str] = field(default_factory=list)
    estimates_to_write: List[str] = field(default_factory=list)
    invoices_to_send: List[str] = field(default_factory=list)
    revenue_gap: float = 0.0
    pipeline_coverage_ratio: float = 0.0
    primary_mode: str = "not_set"
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
            wake_dt = datetime.combine(today, parsed)

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
            "person": person,
            "interval_minutes": interval_minutes,
            "next_ping_at": next_ping_at.isoformat(),
            "prompt": prompt,
            "source": source,
            "status": "active",
        }, on_conflict="channel_id")
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
            "last_sent_at": local_now().isoformat(),
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
        .update({"status": "stopped"})
        .eq("channel_id", str(channel_id))
        .eq("status", "active")
        .execute()
    )
# ---------- Weekly execution logic ----------

def build_weekly_execution(
    target_amount: float,
    scheduled_revenue: float,
    outstanding_estimate_value: float,
    pending_invoice_value: float,
    earning_jobs: List[str],
    estimates_to_write: List[str],
    invoices_to_send: List[str],
) -> WeeklyExecution:
    gap = max(0.0, target_amount - scheduled_revenue)
    ratio = round(outstanding_estimate_value / gap, 2) if gap else 999.0

    has_invoices = pending_invoice_value > 0 or bool(invoices_to_send)
    has_estimates = outstanding_estimate_value > 0 or bool(estimates_to_write)
    has_jobs = bool(earning_jobs)

    priority_tasks: List[str] = []

    if gap <= 0:
        primary_mode = "protect_scheduled_work"
        priority_tasks.append("Protect scheduled earning jobs")
        priority_tasks.append("Collect cleanly and avoid creating unnecessary new work")

    elif has_invoices and pending_invoice_value >= gap:
        primary_mode = "collections_push"
        priority_tasks.append("Send/collect pending invoices")

    elif has_estimates and outstanding_estimate_value >= gap:
        primary_mode = "estimate_conversion_push"
        priority_tasks.append("Write and follow up estimates")

    elif has_invoices or has_estimates or has_jobs:
        primary_mode = "mixed_operations_push"
        if has_invoices:
            priority_tasks.append("Send/collect pending invoices")
        if has_estimates:
            priority_tasks.append("Write and follow up estimates")
        if has_jobs:
            priority_tasks.append("Protect scheduled earning jobs")
        priority_tasks.append("Check remaining gap after known levers are worked")

    else:
        primary_mode = "crm_mining_push"
        priority_tasks.append("Known invoices, estimates, and earning jobs are exhausted")
        priority_tasks.append("Escalate to Chismebot CRM mining")
        priority_tasks.append("Look for dormant leads, past customers, unfinished conversations, and quick-close jobs")

    return WeeklyExecution(
        target_amount=target_amount,
        scheduled_revenue=scheduled_revenue,
        outstanding_estimate_value=outstanding_estimate_value,
        pending_invoice_value=pending_invoice_value,
        earning_jobs=earning_jobs,
        estimates_to_write=estimates_to_write,
        invoices_to_send=invoices_to_send,
        revenue_gap=gap,
        pipeline_coverage_ratio=ratio,
        primary_mode=primary_mode,
        priority_tasks=priority_tasks,
    )


def weekly_execution_to_json(execution: WeeklyExecution) -> Dict[str, Any]:
    return {
        "target_amount": execution.target_amount,
        "scheduled_revenue": execution.scheduled_revenue,
        "outstanding_estimate_value": execution.outstanding_estimate_value,
        "pending_invoice_value": execution.pending_invoice_value,
        "earning_jobs": execution.earning_jobs,
        "estimates_to_write": execution.estimates_to_write,
        "invoices_to_send": execution.invoices_to_send,
        "revenue_gap": execution.revenue_gap,
        "pipeline_coverage_ratio": execution.pipeline_coverage_ratio,
        "primary_mode": execution.primary_mode,
        "priority_tasks": execution.priority_tasks,
    }


def weekly_execution_from_plan(plan: Dict[str, Any]) -> WeeklyExecution:
    task_summary = json_safe_load(plan.get("task_summary_json"), {})
    raw = task_summary.get("weekly_execution", {}) if isinstance(task_summary, dict) else {}

    return WeeklyExecution(
        target_amount=float(raw.get("target_amount") or plan.get("weekly_goal") or 0.0),
        scheduled_revenue=float(raw.get("scheduled_revenue") or 0.0),
        outstanding_estimate_value=float(raw.get("outstanding_estimate_value") or 0.0),
        pending_invoice_value=float(raw.get("pending_invoice_value") or 0.0),
        earning_jobs=raw.get("earning_jobs") or json_safe_load(plan.get("jobs_json"), []),
        estimates_to_write=raw.get("estimates_to_write") or json_safe_load(plan.get("pending_estimates_json"), []),
        invoices_to_send=raw.get("invoices_to_send") or json_safe_load(plan.get("invoices_to_send_json"), []),
        revenue_gap=float(raw.get("revenue_gap") or 0.0),
        pipeline_coverage_ratio=float(raw.get("pipeline_coverage_ratio") or 0.0),
        primary_mode=str(raw.get("primary_mode") or "not_set"),
        priority_tasks=raw.get("priority_tasks") or [],
    )


def build_auto_schedule(start_iso: str, execution: WeeklyExecution) -> Dict[str, List[Dict[str, Any]]]:
    """Create rolling operational execution blocks based on current revenue-gap levers."""
    start_day = date.fromisoformat(start_iso)
    schedule: Dict[str, List[Dict[str, Any]]] = {}

    def add(day_offset: int, text: str, priority: str = "normal"):
        iso = (start_day + timedelta(days=day_offset)).isoformat()
        schedule.setdefault(iso, []).append({
            "text": text,
            "done": False,
            "source": "mweekly",
            "type": "operations",
            "priority": priority,
        })

    add(0, f"Current operating target: {format_money(execution.target_amount)}", "high")
    add(0, f"Remaining revenue gap: {format_money(execution.revenue_gap)}", "high")

    if execution.primary_mode == "protect_scheduled_work":
        for job in execution.earning_jobs:
            add(0, f"Protect scheduled job: {job}", "high")
        add(1, "Confirm collections and close out cleanly", "normal")

    elif execution.primary_mode == "collections_push":
        if execution.invoices_to_send:
            for invoice in execution.invoices_to_send:
                add(0, f"Send/collect invoice: {invoice}", "high")
        else:
            add(0, f"Send/collect invoices: {format_money(execution.pending_invoice_value)} pending", "high")
        add(2, "Second pass on invoice collection", "normal")

    elif execution.primary_mode == "estimate_conversion_push":
        if execution.estimates_to_write:
            for estimate in execution.estimates_to_write:
                add(0, f"Write/follow up estimate: {estimate}", "high")
        else:
            add(0, f"Estimate conversion block: {format_money(execution.outstanding_estimate_value)} pipeline", "high")
        add(1, "Second pass on estimate conversion", "normal")

    elif execution.primary_mode == "mixed_operations_push":
        for invoice in execution.invoices_to_send:
            add(0, f"Send/collect invoice: {invoice}", "high")

        for estimate in execution.estimates_to_write:
            add(1, f"Write/follow up estimate: {estimate}", "high")

        for job in execution.earning_jobs:
            add(0, f"Protect scheduled job: {job}", "high")

        add(2, "Check remaining gap after known levers are worked", "normal")

    elif execution.primary_mode == "crm_mining_push":
        add(0, "Known work levers exhausted — activate Chismebot", "high")
        add(0, "Run !followuplist and look for quick-close follow-ups", "high")
        add(1, "Run !chismelist and mine dormant leads / past customers", "high")
        add(1, "Create new follow-ups from Chismebot CRM mining", "normal")

    add(4, "Closeout: update invoices, estimates, receipts/materials, and next operating target", "normal")

    return schedule


def format_money(value: float) -> str:
    return f"${value:,.0f}"


def format_execution_summary(execution: WeeklyExecution) -> str:
    lines = [
        "📌 Weekly execution readout",
        f"Target: {format_money(execution.target_amount)}",
        f"Scheduled revenue: {format_money(execution.scheduled_revenue)}",
        f"Revenue gap: {format_money(execution.revenue_gap)}",
        f"Outstanding estimates: {format_money(execution.outstanding_estimate_value)}",
        f"Pending invoices: {format_money(execution.pending_invoice_value)}",
        f"Mode: {execution.primary_mode}",
        "Priorities:",
    ]
    lines.extend([f"- {task}" for task in execution.priority_tasks])
    return "\n".join(lines)

def build_wakeup_message(execution: WeeklyExecution, routine: Optional[Dict[str, Any]] = None) -> str:
    routine_text = None

    if routine:
        routine_text = routine.get("routine_text")

    if not routine_text:
        routine_text = (
            "1. Shower + shave\n"
            "2. Get dressed\n"
            "3. Make sit-down breakfast\n"
            "4. Kids pack snacks/lunch boxes from staged counter snacks\n"
            "5. Confirm first job / first work block"
        )

    return (
        "🌅 Daniel morning boot sequence\n"
        f"Audio runway: {DEFAULT_CHILLHOP_URL}\n\n"
        f"{routine_text}\n\n"
        f"Weekly target: {format_money(execution.target_amount)}\n"
        f"Scheduled revenue: {format_money(execution.scheduled_revenue)}\n"
        f"Revenue gap: {format_money(execution.revenue_gap)}\n"
        f"Mode: {execution.primary_mode}\n\n"
        "Priorities:\n"
        + "\n".join([f"- {task}" for task in execution.priority_tasks])
        + "\n\nStart with the first physical step. No algorithm hole."
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
        print("[METICHE LOOP] heartbeat")
        while True:
            await asyncio.sleep(30)
            now = local_now()

            due_wakeups = fetch_due_wakeups(now)

            for wakeup in due_wakeups:
                channel = self.bot.get_channel(int(wakeup["channel_id"]))

                if not channel:
                    continue

                week = week_of_monday(local_now())
                _, execution, _, _, _ = current_weekly_context(week)
                routine = fetch_active_routine(wakeup.get("person", "Daniel"))

                await channel.send(build_wakeup_message(execution, routine))
                mark_wakeup_sent(wakeup["id"])

            due_pings = fetch_due_pings(now)
            print(f"[PING SCHEDULES DUE] {due_pings}")

            for ping in due_pings:
                if int(ping["channel_id"]) in channels_waiting_for_command:
                    continue
                    
                channel = self.bot.get_channel(int(ping["channel_id"]))

                if not channel:
                    continue
                                session = active_time_sessions.get(int(ping["channel_id"]))
                interval = int(ping.get("interval_minutes") or 120)

                if session and session.last_activity_timestamp:
                    last_activity = parse_iso(session.last_activity_timestamp)
                    idle_minutes = (local_now() - last_activity).total_seconds() / 60

                    if idle_minutes < interval:
                        advance_ping_schedule(ping["id"], interval)
                        continue

                prompt = ping.get("prompt") or "¿Qué onda? What changed since the last time marker?"
                await channel.send(prompt)

                advance_ping_schedule(
                    ping["id"],
                    interval,
                )

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
        "current_state": session.current_state,
        "paused_task": session.paused_task,
        "last_timestamp": session.last_timestamp,
        "parked_items": session.parked_items,
        "interruptions": session.interruptions,
        "total_minutes": total_minutes(session.blocks),
        "total_label": minutes_to_label(total_minutes(session.blocks)),
        "blocks_logged": len(session.blocks),
        "blocks": session.blocks,
    }


def build_task_summary(weekly_execution: Optional[WeeklyExecution] = None, raw_time: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    if weekly_execution is not None:
        payload["weekly_execution"] = weekly_execution_to_json(weekly_execution)
    if raw_time is not None:
        payload["raw_time"] = raw_time
    return payload


def save_weekly_snapshot(
    ctx: commands.Context,
    week: str,
    execution: WeeklyExecution,
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
        "weekly_goal": execution.target_amount,
        "jobs_json": json.dumps(execution.earning_jobs, ensure_ascii=False),
        "pending_estimates_json": json.dumps(execution.estimates_to_write, ensure_ascii=False),
        "invoices_to_send_json": json.dumps(execution.invoices_to_send, ensure_ascii=False),
        "calendar_json": json.dumps(calendar_json, ensure_ascii=False),
        "task_summary_json": json.dumps(build_task_summary(execution, raw_time), ensure_ascii=False),
        "wants_bodydouble": wants_bodydouble,
        "quarterly_goals_json": json.dumps(quarterly_goals or [], ensure_ascii=False),
        "yearly_goals_json": json.dumps(yearly_goals or [], ensure_ascii=False),
    })


def current_weekly_context(week: str) -> Tuple[Dict[str, Any], WeeklyExecution, Dict[str, Any], List[str], List[str]]:
    plan = fetch_latest_metiche_weekly(week) or {}
    execution = weekly_execution_from_plan(plan)
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

    return buckets
    
def register_metiche(bot: commands.Bot):
    global metiche_instance
    metiche_instance = MeticheManager(bot)

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

        match_idx = find_best_task_match(session.daily_tasks, activity_text)
        if match_idx is not None:
            session.daily_tasks[match_idx]["done"] = True
            await push_daily_tasks_to_calendar(ctx, session)

        push_result = metiche.push_task_summary_json(build_raw_time_payload(session))

        msg = (
            f"⏱️ Logged {minutes_to_label(duration)} on {session.active_task or 'unassigned focus'}\n"
            f"Update: {activity_text}\n"
            f"Total accounted today: {build_raw_time_payload(session)['total_label']}"
        )
                
        if match_idx is not None:
            msg += f"\n✅ Checked off: {session.daily_tasks[match_idx]['text']}"
        if not push_result.get("ok"):
            msg += f"\nSaved, but dashboard push failed: {push_result.get('reason')}"
        await ctx.send(msg)
        return block

    async def show_active_day(ctx: commands.Context, session: TimeSession):
        pending_tasks = [
            task for task in normalize_daily_items(session.daily_tasks)
            if not task.get("done")
        ]
        pending_text = "\n".join([f"- {task['text']}" for task in pending_tasks]) or "(nothing pending)"
        parked_text = "\n".join([f"- {item}" for item in session.parked_items]) or "(nothing parked)"
        await ctx.send(
            f"📍 State: {session.current_state}\n"
            f"🎯 Active: {session.active_task or session.paused_task or '(none)'}\n"
            f"⏱️ Accounted today: {build_raw_time_payload(session)['total_label']}\n\n"
            f"Pending:\n{pending_text}\n\n"
            f"Parked for later:\n{parked_text}"
        )

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
            label = re.sub(r"^drift\s*", "", raw, flags=re.IGNORECASE).strip() or "unspecified drift"
            session.current_state = "drift"
            block = await log_raw_time_block(ctx, f"drift: {label}", source="drift")
            session.current_state = "active"
            await save_active_day_state(ctx, session)
            duration = block.get("duration_label") if block else "0m"
            await ctx.send(f"🌀 Drift captured: {label}\nDuration since last marker: {duration}\nRecovered focus: {session.active_task or '(none)'}")
            return True

        if lower.startswith("pause"):
            reason = re.sub(r"^pause\s*", "", raw, flags=re.IGNORECASE).strip() or "paused"
            previous_focus = session.active_task
            await log_raw_time_block(ctx, f"pause: {reason}", source="pause")
            session.current_state = "paused"
            session.paused_task = previous_focus
            session.active_task = None
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
            session.paused_task = None
            session.current_state = "active"
            await save_active_day_state(ctx, session)
            await ctx.send(f"▶️ Resumed: {target}")
            return True

        if lower.startswith("switch "):
            target = raw[7:].strip()
            if not target:
                await ctx.send("Switch to what?")
                return True
            previous_focus = session.active_task
            await log_raw_time_block(ctx, f"switch from {previous_focus or 'unassigned'} to {target}", source="switch")
            session.active_task = target
            session.current_state = "active"
            session.paused_task = None
            await save_active_day_state(ctx, session)
            await ctx.send(f"🔀 Switched focus:\nFrom: {previous_focus or '(none)'}\nTo: {target}")
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
                person=session.person,
                interval_minutes=interval,
                prompt=f"¿Qué onda? Still on {session.active_task or session.paused_task or 'your current focus'}, or did something change?",
            )
            await ctx.send(f"🔔 Que Onda pings set for every {interval} minutes.")
            return True

        if lower.startswith("done") or lower.startswith("check "):
            target = re.sub(r"^(done|check)\s*", "", raw, flags=re.IGNORECASE).strip() or session.active_task
            if not target:
                await ctx.send("Done with what? Try `done 2` or `done clean kitchen`.")
                return True
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
            checked = "\n✅ Checked off:\n" + "\n".join([f"- {label}" for label in checked_labels]) if checked_labels else "\n⚠️ Logged time, but no matching task was checked off."
            await ctx.send(f"✅ Done: {target}\nTime since last marker: {duration}{checked}\n\nType `show` to see the updated list, or `switch 3` to start another task.")
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
        "• task accounting\n\n"
    
        "**Planning**\n"
        "`!mweekly` — update weekly operating target and execution strategy\n"
        "`!mplan` — show current weekly execution plan\n"
        "`!mschedule` — add/change/replace a schedule\n"
        "`!mgoals` — save quarterly and yearly goals\n\n"
    
        "**Daily Operations**\n"
        "`!mroutine` — view or edit morning routines\n"
        "`!mwakeup` — schedule Daniel's morning boot sequence\n"
        "`!mbraindump` — capture and sort the messy pile before planning today\n"
        "`!mtoday` — structure today's work and optional check-in cadence\n"
        "`!mstopday` — stop today's time session\n"
        "`!mquiet` — stop reminder/check-in pings\n\n"

        "**During an active `!mtoday` session**\n"
        "Type these without `!`:\n"
        "`done` — complete the active focus and show elapsed time\n"
        "`done clean kitchen` — complete a named task\n"
        "`add call inspector` — append to today\n"
        "`later clean garage` — park for later\n"
        "`switch kitchen` — change focus\n"
        "`drift phone game` — capture drift without shame\n"
        "`show` — retrieve current state\n"
        "`ping 30` — set Que Onda pings"
    )

    @bot.command(name="mweekly")
    async def mweekly(ctx: commands.Context):
        active_time_sessions.pop(ctx.channel.id, None)

        def check(m: discord.Message):
            return m.author.id == ctx.author.id and m.channel.id == ctx.channel.id

        week = week_of_monday(local_now())
        _, _, calendar_json, quarterly_goals, yearly_goals = current_weekly_context(week)

        await ctx.send("Weekly target amount for the week:")
        target_amount = money_to_float((await bot.wait_for("message", check=check)).content)

        await ctx.send("Estimated revenue already scheduled from earning jobs:")
        scheduled_revenue = money_to_float((await bot.wait_for("message", check=check)).content)

        await ctx.send("Earning jobs on the schedule? List names comma-separated, or `none`:")
        earning_jobs = parse_named_list((await bot.wait_for("message", check=check)).content)

        await ctx.send("Total value of current outstanding estimates:")
        outstanding_estimate_value = money_to_float((await bot.wait_for("message", check=check)).content)

        await ctx.send("Which estimates need to be written or followed up? List comma-separated, or `none`:")
        estimates_to_write = parse_named_list((await bot.wait_for("message", check=check)).content)

        await ctx.send("Total value of invoices that need to be sent/collected:")
        pending_invoice_value = money_to_float((await bot.wait_for("message", check=check)).content)

        await ctx.send("Which invoices need to be sent/collected? List comma-separated, or `none`:")
        invoices_to_send = parse_named_list((await bot.wait_for("message", check=check)).content)

        execution = build_weekly_execution(
            target_amount=target_amount,
            scheduled_revenue=scheduled_revenue,
            outstanding_estimate_value=outstanding_estimate_value,
            pending_invoice_value=pending_invoice_value,
            earning_jobs=earning_jobs,
            estimates_to_write=estimates_to_write,
            invoices_to_send=invoices_to_send,
        )

        auto_schedule = build_auto_schedule(today_iso(), execution)
        handley_schedule = remove_source_tasks(calendar_json.get("Handley Man", {}), "mweekly")
        calendar_json["Handley Man"] = merge_days(handley_schedule, auto_schedule)
        
        save_weekly_snapshot(
            ctx, week, execution, calendar_json,
            wants_bodydouble=False,
            quarterly_goals=quarterly_goals,
            yearly_goals=yearly_goals,
        )

        metiche = get_metiche()
        push_result = metiche.push_calendar_json("Handley Man", calendar_json["Handley Man"]) if metiche else {"ok": False, "reason": "Metiche not initialized"}
        status = "Pushed auto schedule to dashboard." if push_result.get("ok") else f"Saved, but dashboard push failed: {push_result.get('reason')}"

        await ctx.send(format_execution_summary(execution) + f"\n\n{status}")

    @bot.command(name="mplan")
    async def mplan(ctx: commands.Context):
        week = week_of_monday(local_now())
        plan = fetch_latest_metiche_weekly(week) or {}
        if not plan:
            await ctx.send("No weekly plan saved yet. Run !mweekly first.")
            return
        execution = weekly_execution_from_plan(plan)
        calendar_json = ensure_calendar(plan.get("calendar_json"))
        lines = [format_execution_summary(execution), "", format_person_schedule("Handley Man", calendar_json.get("Handley Man", {}))]
        await ctx.send("\n".join(lines))

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
            "`H:` Hold\n\n"
            "Example:\n"
            "T: 1, 3\n"
            "W: 2, 5\n"
            "H: 4"
        )
        
        await ctx.send(preview)
        
        response = (await bot.wait_for("message", check=check)).content.strip()
        buckets = parse_braindump_categories(response, dumped_items)
        
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
        
        status = "Added today’s brain dump items to mtoday."
        
        summary = (
            "🧠 Brain dump sorted.\n\n"
            f"Today: {len(buckets['today'])}\n"
            f"This Week: {len(buckets['week'])}\n"
            f"Hold: {len(buckets['hold'])}\n\n"
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
            return m.author.id == ctx.author.id and m.channel.id == ctx.channel.id
    
        week = week_of_monday(local_now())
        _, execution, calendar_json, quarterly_goals, yearly_goals = current_weekly_context(week)
    
        person = get_person_from_discord(ctx.author.id)
        date_key = today_iso()
    
        existing_today = load_daily_tasks(person, date_key)
        if not existing_today:
            existing_today = normalize_daily_items(calendar_json.get(person, {}).get(date_key, []))
    
        await ctx.send(
            f"📅 Today is {today_label()}\n"
            f"💰 Weekly target: {format_money(execution.target_amount)}\n"
            f"🧭 Mode: {execution.primary_mode}\n\n"
            + format_daily_tasks(existing_today, person, today_label())
            + "\n\nWhat are you working on right now?\n"
              "Reply with a task number or a short focus label.\n"
              "Examples:\n"
              "`1`\n"
              "`metichebot`\n"
              "`customer communication`\n\n"
              "Or reply:\n"
              "`edit` — add/remove/check off/keep items before starting\n"
              "`cancel` — stop"
        )
    
        choice = (await bot.wait_for("message", check=check)).content.strip()
    
        if choice.lower() == "cancel":
            await ctx.send("Okay. I stopped before starting task accounting.")
            return

        if choice.lower() == "show":
            await ctx.send(format_daily_tasks(existing_today, person, today_label()))
            choice = (await bot.wait_for("message", check=check)).content.strip()

        if choice.lower().startswith("add "):
            item = choice[4:].strip()
            if item:
                existing_today = normalize_daily_items(existing_today) + [{"text": item, "done": False, "source": "mtoday_add"}]
                replace_daily_tasks(person, date_key, existing_today)
                await ctx.send(
                    f"➕ Added: {item}\n\n"
                    + format_daily_tasks(existing_today, person, today_label())
                    + "\n\nNow what are you working on right now?"
                )
                choice = (await bot.wait_for("message", check=check)).content.strip()
    
        while choice.lower() == "edit":
            await ctx.send(
                "List edit mode. Use one of these:\n"
                "`add task` — append a task\n"
                "`done 4` or `done 4,5` — check off task numbers\n"
                "`remove 4` — remove task numbers\n"
                "`keep 4,5,6` — keep only those task numbers\n"
                "`rewrite task, task` — replace the whole list\n\n"
                "Bare numbers like `4,5,6` mean KEEP those task numbers. They will not become fake tasks."
            )
    
            edited = (await bot.wait_for("message", check=check)).content.strip()
            existing_today, edit_message = apply_list_edit(existing_today, edited)
            replace_daily_tasks(person, date_key, existing_today)
    
            await ctx.send(
                f"{edit_message}\n\n"
                + format_daily_tasks(existing_today, person, today_label())
                + "\n\nNow what are you working on right now? Reply with a task number, focus label, `edit`, or `cancel`."
            )
            choice = (await bot.wait_for("message", check=check)).content.strip()

            if choice.lower() == "cancel":
                await ctx.send("Okay. I stopped before starting task accounting.")
                return
    
        active_focus = choice
    
        if choice.isdigit():
            idx = int(choice) - 1
            tasks = normalize_daily_items(existing_today)
            if 0 <= idx < len(tasks):
                active_focus = tasks[idx]["text"]
            else:
                await ctx.send("I couldn’t match that task number, so I’ll use it as a focus label.")
    
        session = TimeSession(
            channel_id=ctx.channel.id,
            person=person,
            date_iso=date_key,
            date_label=today_label(),
            last_timestamp=local_now().isoformat(),
            last_activity_timestamp=local_now().isoformat(),
            active_task=active_focus,
            daily_tasks=normalize_daily_items(existing_today),
        )
    
        active_time_sessions[ctx.channel.id] = session
        
        replace_daily_tasks(person, date_key, session.daily_tasks)
    
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
    
        metiche.push_task_summary_json(build_raw_time_payload(session))

        pending_tasks = [
            task for task in normalize_daily_items(existing_today)
            if normalize_task(task.get("text", "")) != normalize_task(active_focus)
        ]
        
        pending_text = "\n".join(
            [f"- {task['text']}" for task in pending_tasks]
        ) or "(nothing else pending)"
        
        session.setup_complete = True
        await save_active_day_state(ctx, session)

        await ctx.send(
            f"🟢 Active focus:\n{active_focus}\n\n"
            f"⏳ Pending:\n{pending_text}\n\n"
            "Task accounting is active now. No extra setup step.\n\n"
            "To check something off, type `done` or `done task name`.\n"
            "Other useful commands: `show`, `add task`, `later task`, `switch task`, `drift label`, `pause reason`, `resume task`, `ping 30`."
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
