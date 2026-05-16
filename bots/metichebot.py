"""
Metichebot refactor: weekly execution engine + daily raw time accounting.

What changed:
- Removed the duplicate metiche_weekly_plans table path that made !mplan look empty.
- Uses fetch_latest_metiche_weekly / insert_metiche_weekly as the single weekly-plan source.
- Rebuilt !mweekly as an operational forecast intake, not just a note collector.
- Added automatic weekly execution interpretation and schedule injection.
- Added !mwakeup for Daniel's morning boot sequence.
- Kept: !mschedule, !mtoday, !mcheckin, !mbodydouble, !mquiet, !mgoals, !mstopday.
- Removed stale generate_daily_schedule/save_weekly_plan/load_weekly_plan compatibility code.
"""

import asyncio
import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple
from urllib import error, request

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

VALID_PEOPLE = ["Heaven", "Daniel", "Handley Man"]
PERSON_TO_CALENDAR_KEY = {
    "Heaven": "heaven",
    "Daniel": "daniel",
    "Handley Man": "handley_man",
}

RAW_TIME_LABEL = "raw_time"
DEFAULT_CALENDAR = {"Heaven": {}, "Daniel": {}, "Handley Man": {}}
DEFAULT_CHILLHOP_URL = os.getenv(
    "DANIEL_MORNING_AUDIO_URL",
    "https://www.youtube.com/results?search_query=chillhop+morning+radio",
)

metiche_instance = None
active_time_sessions: Dict[int, "TimeSession"] = {}
pending_wakeups: Dict[int, Dict[str, Any]] = {}

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
    blocks: List[Dict[str, Any]] = field(default_factory=list)
    daily_tasks: List[Dict[str, Any]] = field(default_factory=list)


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
    return datetime.fromisoformat(ts)

def parse_wakeup_time(raw: str) -> Optional[datetime]:
    raw = raw.strip().lower().replace(".", "")
    today = datetime.now().date()

    for fmt in ("%I:%M %p", "%I %p", "%H:%M", "%H"):
        try:
            parsed = datetime.strptime(raw, fmt).time()
            wake_dt = datetime.combine(today, parsed)

            if wake_dt <= datetime.now():
                wake_dt = wake_dt + timedelta(days=1)

            return wake_dt
        except ValueError:
            continue

    return None


def week_of_monday(d: datetime) -> str:
    monday = d.date() - timedelta(days=d.weekday())
    return monday.isoformat()


def today_iso() -> str:
    return datetime.now().date().isoformat()


def today_label() -> str:
    fmt = "%A, %B %-d" if os.name != "nt" else "%A, %B %#d"
    return datetime.now().strftime(fmt)


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
    today = datetime.now().date()
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

def build_wakeup_message(execution: WeeklyExecution) -> str:
    return (
        "🌅 Daniel morning boot sequence\n"
        f"Audio runway: {DEFAULT_CHILLHOP_URL}\n\n"
        "1. Shower + shave\n"
        "2. Get dressed\n"
        "3. Make sit-down breakfast\n"
        "4. Kids pack snacks/lunch boxes from staged counter snacks\n"
        "5. Confirm first job / first work block\n\n"
        f"Weekly target: {format_money(execution.target_amount)}\n"
        f"Scheduled revenue: {format_money(execution.scheduled_revenue)}\n"
        f"Revenue gap: {format_money(execution.revenue_gap)}\n"
        f"Mode: {execution.primary_mode}\n\n"
        "Priorities:\n"
        + "\n".join([f"- {task}" for task in execution.priority_tasks])
        + "\n\nStart with the shower. No algorithm hole."
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


# ---------- manager ----------

class MeticheManager:
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.channel_id: Optional[int] = None
        self.bodydouble_on = False
        self.next_checkin: Optional[datetime] = None
        self.checkin_interval_hours = 2
        self.data_service_url = os.getenv("DATA_SERVICE_URL", "").rstrip("/")

    def turn_on_bodydouble(self, channel_id: int):
        self.channel_id = channel_id
        self.bodydouble_on = True
        self.next_checkin = datetime.now() + timedelta(hours=self.checkin_interval_hours)

    def turn_off_bodydouble(self):
        self.bodydouble_on = False
        self.next_checkin = None

    def bump_bodydouble_timer(self):
        if self.bodydouble_on:
            self.next_checkin = datetime.now() + timedelta(hours=self.checkin_interval_hours)

    def build_bodydouble_prompt(self) -> str:
        return "¿Qué onda?\nWhat have you been doing since the last time marker?"

    async def start_loop(self):
        while True:
            await asyncio.sleep(30)
            now = datetime.now()

            for channel_id, wakeup in list(pending_wakeups.items()):
                if now >= wakeup["wake_time"]:
                    channel = self.bot.get_channel(channel_id)
                    if channel:
                        week = week_of_monday(datetime.now())
                        _, execution, _, _, _ = current_weekly_context(week)
                        await channel.send(build_wakeup_message(execution))
                    pending_wakeups.pop(channel_id, None)
        
            if not self.channel_id or not self.bodydouble_on or not self.next_checkin:
                continue
            if datetime.now() >= self.next_checkin:
                channel = self.bot.get_channel(self.channel_id)
                if channel:
                    await channel.send(self.build_bodydouble_prompt())
                self.next_checkin = datetime.now() + timedelta(hours=self.checkin_interval_hours)

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
        "last_timestamp": session.last_timestamp,
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

def register_metiche(bot: commands.Bot):
    global metiche_instance
    metiche_instance = MeticheManager(bot)

    async def push_daily_tasks_to_calendar(ctx: commands.Context, session: TimeSession):
        metiche = get_metiche()
        if metiche is None or session.person not in VALID_PEOPLE:
            return

        week = week_of_monday(datetime.now())
        _, execution, calendar_json, quarterly_goals, yearly_goals = current_weekly_context(week)
        person_schedule = calendar_json.get(session.person, {})
        person_schedule[session.date_iso] = normalize_daily_items(session.daily_tasks)
        calendar_json[session.person] = person_schedule

        replace_daily_tasks(session.person, session.date_iso, session.daily_tasks)
        save_weekly_snapshot(
            ctx, week, execution, calendar_json,
            wants_bodydouble=True,
            quarterly_goals=quarterly_goals,
            yearly_goals=yearly_goals,
            raw_time=build_raw_time_payload(session),
        )
        metiche.push_calendar_json(session.person, person_schedule)

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
                last_timestamp=now_iso(),
            )
            active_time_sessions[ctx.channel.id] = session
            await ctx.send("Started raw time accounting. Tell me what you were doing at the next time marker.")
            return None

        activity_text = activity.strip()
        if not activity_text:
            return None

        now = datetime.now()
        duration = max(0, int((now - parse_iso(session.last_timestamp)).total_seconds() // 60))
        block = {
            "date": session.date_iso,
            "start": session.last_timestamp,
            "end": now.isoformat(),
            "duration_minutes": duration,
            "duration_label": minutes_to_label(duration),
            "activity": activity_text,
            "source": source,
        }
        session.blocks.append(block)
        session.last_timestamp = now.isoformat()

        insert_metiche_checkin({
            "ts": now_iso(),
            "discord_user": str(ctx.author),
            "channel_id": str(ctx.channel.id),
            "week_of": week_of_monday(datetime.now()),
            "category": RAW_TIME_LABEL,
            "task": activity_text,
            "energy": None,
        })

        match_idx = find_best_task_match(session.daily_tasks, activity_text)
        if match_idx is not None:
            session.daily_tasks[match_idx]["done"] = True
            await push_daily_tasks_to_calendar(ctx, session)

        push_result = metiche.push_task_summary_json(build_raw_time_payload(session))
        metiche.bump_bodydouble_timer()

        msg = f"⏱️ Logged {minutes_to_label(duration)} — {activity_text}\nTotal accounted today: {build_raw_time_payload(session)['total_label']}"
        if match_idx is not None:
            msg += f"\n✅ Checked off: {session.daily_tasks[match_idx]['text']}"
        if not push_result.get("ok"):
            msg += f"\nSaved, but dashboard push failed: {push_result.get('reason')}"
        await ctx.send(msg)
        return block

    @bot.command(name="metichebot")
    async def metichebot_help(ctx):
        await ctx.send(
            """
🧠 METICHEBOT

Planning
!mweekly — update current operating target and choose the next revenue-gap lever!mplan — show the current weekly execution plan
!mschedule — manually add/change/replace a person schedule
!mgoals — save quarterly and yearly goals

Daily Use
!mwakeup — Daniel morning boot checklist + chillhop link
!mtoday — start today’s working list and raw time accounting
!mcheckin <what you were doing> — manual raw time entry
!mstopday — stop today’s time session

Bodydouble
!mbodydouble — Que onda every 2 hours
!mquiet — turn off Que onda pings
"""
        )

    @bot.command(name="mweekly")
    async def mweekly(ctx: commands.Context):
        active_time_sessions.pop(ctx.channel.id, None)

        def check(m: discord.Message):
            return m.author.id == ctx.author.id and m.channel.id == ctx.channel.id

        week = week_of_monday(datetime.now())
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
        week = week_of_monday(datetime.now())
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
        def check(m: discord.Message):
            return m.author.id == ctx.author.id and m.channel.id == ctx.channel.id
    
        await ctx.send("What time should I run Daniel’s wakeup sequence? Example: `7:00 AM`")
    
        raw_time = (await bot.wait_for("message", check=check)).content.strip()
        wake_time = parse_wakeup_time(raw_time)
    
        if wake_time is None:
            await ctx.send("I couldn’t read that time. Try something like `7:00 AM` or `6:30`.")
            return
    
        pending_wakeups[ctx.channel.id] = {
            "wake_time": wake_time,
            "set_by": str(ctx.author),
        }
    
        await ctx.send(
            f"✅ Daniel’s wakeup sequence is scheduled for {wake_time.strftime('%A, %B %-d at %-I:%M %p')}.\n"
            "Set his actual phone alarm too. I can ping Discord, but I can’t make the phone scream."
        )

    @bot.command(name="mschedule")
    async def mschedule(ctx: commands.Context):
        active_time_sessions.pop(ctx.channel.id, None)
        metiche = get_metiche()
        if metiche is None:
            await ctx.send("Metiche isn’t initialized yet.")
            return

        def check(m: discord.Message):
            return m.author.id == ctx.author.id and m.channel.id == ctx.channel.id

        week = week_of_monday(datetime.now())
        _, execution, calendar_json, quarterly_goals, yearly_goals = current_weekly_context(week)

        await ctx.send("Who’s schedule are we working on?\n(Heaven / Daniel / Handley Man)")
        person_raw = (await bot.wait_for("message", check=check)).content.strip()
        person = next((p for p in VALID_PEOPLE if p.lower() == person_raw.lower()), None)
        if not person:
            await ctx.send("I need one of: Heaven / Daniel / Handley Man")
            return

        person_schedule = calendar_json.get(person, {})
        await ctx.send(
            format_person_schedule(person, person_schedule)
            + "\n\nWhat do you want to do?\n1. Add to schedule\n2. Change specific days\n3. Start over\nReply with 1, 2, or 3"
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

    @bot.command(name="mtoday")
    async def mtoday(ctx: commands.Context):
        active_time_sessions.pop(ctx.channel.id, None)
        metiche = get_metiche()
        if metiche is None:
            await ctx.send("Metiche isn’t initialized yet.")
            return

        def check(m: discord.Message):
            return m.author.id == ctx.author.id and m.channel.id == ctx.channel.id

        week = week_of_monday(datetime.now())
        _, execution, calendar_json, quarterly_goals, yearly_goals = current_weekly_context(week)

        await ctx.send(f"Today is {today_label()}.\nWho are we working as today?\n(Heaven / Daniel / Handley Man)")
        person_raw = (await bot.wait_for("message", check=check)).content.strip()
        person = next((p for p in VALID_PEOPLE if p.lower() == person_raw.lower()), None)
        if not person:
            await ctx.send("I need one of: Heaven / Daniel / Handley Man")
            return

        date_key = today_iso()
        existing_today = load_daily_tasks(person, date_key)
        if not existing_today:
            existing_today = normalize_daily_items(calendar_json.get(person, {}).get(date_key, []))

        await ctx.send(
            f"📅 Today is {today_label()}\n"
            f"💰 Weekly target: {format_money(execution.target_amount)}\n"
            f"🧭 Mode: {execution.primary_mode}\n\n"
            + format_daily_tasks(existing_today, person, today_label())
            + "\n\nAny changes for today? Reply with the new full list, or say `no changes`."
        )
        changes = (await bot.wait_for("message", check=check)).content.strip()
        if changes.lower() not in {"no", "no changes", "same", "keep"}:
            parsed = parse_task_list(changes)
            if parsed:
                existing_today = parsed
            else:
                await ctx.send("I couldn’t read that as a list, so I kept the current list.")

        session = TimeSession(
            channel_id=ctx.channel.id,
            person=person,
            date_iso=date_key,
            date_label=today_label(),
            last_timestamp=now_iso(),
            daily_tasks=normalize_daily_items(existing_today),
        )
        active_time_sessions[ctx.channel.id] = session
        calendar_json[person][date_key] = session.daily_tasks
        replace_daily_tasks(person, date_key, session.daily_tasks)
        save_weekly_snapshot(
            ctx, week, execution, calendar_json,
            wants_bodydouble=True,
            quarterly_goals=quarterly_goals,
            yearly_goals=yearly_goals,
            raw_time=build_raw_time_payload(session),
        )
        metiche.push_calendar_json(person, calendar_json[person])
        metiche.push_task_summary_json(build_raw_time_payload(session))
        metiche.turn_on_bodydouble(ctx.channel.id)

        await ctx.send(
            "Locked for today. Raw time accounting starts now.\n\n"
            + format_daily_tasks(session.daily_tasks, person, today_label())
            + "\n\nWhen I ask `Qué onda?`, tell me what you have been doing since the last time marker."
        )

    @bot.command(name="mstopday")
    async def mstopday(ctx: commands.Context):
        session = active_time_sessions.pop(ctx.channel.id, None)
        metiche = get_metiche()
        if metiche is not None:
            metiche.turn_off_bodydouble()
        if not session:
            await ctx.send("No active time session was running.")
            return
        payload = build_raw_time_payload(session)
        await ctx.send(f"Stopped today’s time session.\nTotal accounted: {payload['total_label']}\nBlocks logged: {payload['blocks_logged']}")

    @bot.command(name="mbodydouble")
    async def mbodydouble(ctx: commands.Context):
        metiche = get_metiche()
        if metiche is None:
            await ctx.send("Metiche isn’t initialized yet.")
            return
        metiche.turn_on_bodydouble(ctx.channel.id)
        await ctx.send(metiche.build_bodydouble_prompt())

    @bot.command(name="mquiet")
    async def mquiet(ctx: commands.Context):
        metiche = get_metiche()
        if metiche is None:
            await ctx.send("Metiche isn’t initialized yet.")
            return
        metiche.turn_off_bodydouble()
        await ctx.send("Okay. I’ll be quiet.")

    @bot.command(name="mgoals")
    async def mgoals(ctx: commands.Context):
        def check(m: discord.Message):
            return m.author.id == ctx.author.id and m.channel.id == ctx.channel.id

        week = week_of_monday(datetime.now())
        _, execution, calendar_json, _, _ = current_weekly_context(week)

        await ctx.send("What are the quarterly goals? Comma-separated, or `none`.")
        quarterly_goals = parse_named_list((await bot.wait_for("message", check=check)).content)
        await ctx.send("What are the yearly goals? Comma-separated, or `none`.")
        yearly_goals = parse_named_list((await bot.wait_for("message", check=check)).content)

        save_weekly_snapshot(ctx, week, execution, calendar_json, quarterly_goals=quarterly_goals, yearly_goals=yearly_goals)
        await ctx.send("Locked. I saved your quarterly and yearly goals.")

    @bot.command(name="mcheckin")
    async def mcheckin(ctx: commands.Context, *, entry: str = ""):
        await log_raw_time_block(ctx, entry, source="manual")

    @bot.listen("on_message")
    async def metiche_time_listener(message: discord.Message):
        if message.author.bot or message.content.startswith("!"):
            return

        metiche = get_metiche()
        if metiche is None:
            return

        ctx = await bot.get_context(message)
        if message.channel.id in active_time_sessions:
            await log_raw_time_block(ctx, message.content, source="active_day")
            return

        if metiche.bodydouble_on and metiche.channel_id == message.channel.id:
            await log_raw_time_block(ctx, message.content, source="que_onda")

