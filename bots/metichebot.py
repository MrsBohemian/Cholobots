import asyncio
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from urllib import request, error

import discord
from discord.ext import commands

from db.database import (
    now_iso,
    insert_metiche_weekly,
    insert_metiche_checkin,
    fetch_latest_metiche_weekly,
)

metiche_instance = None

DAY_NAMES = [
    "monday", "tuesday", "wednesday", "thursday",
    "friday", "saturday", "sunday"
]

VALID_PEOPLE = ["Heaven", "Daniel", "Handley Man"]

DEFAULT_TASK_CATEGORIES = [
    "Revenue",
    "Infrastructure",
    "Outreach",
    "Admin",
    "Drift",
    "Life"
]

# One active daily work session per Discord channel.
# This keeps the "today" checklist alive while you use Metichebot.
active_daily_sessions: Dict[int, "DailySession"] = {}


@dataclass
class DailySession:
    channel_id: int
    person: str
    date_iso: str
    date_label: str
    tasks: List[Dict[str, Any]]


def week_of_monday(d: datetime) -> str:
    monday = d.date() - timedelta(days=d.weekday())
    return monday.isoformat()


def today_iso() -> str:
    return datetime.now().date().isoformat()


def today_label() -> str:
    return datetime.now().strftime("%A, %B %-d") if os.name != "nt" else datetime.now().strftime("%A, %B %#d")


def normalize_task(task: str) -> str:
    return re.sub(r"\s+", " ", task.strip().lower())


def day_to_iso(day_name: str, week_start: str) -> str:
    base = datetime.fromisoformat(week_start)
    offset = DAY_NAMES.index(day_name.lower())
    return (base + timedelta(days=offset)).date().isoformat()


def parse_schedule_block(text: str, week_start: str) -> Dict[str, List[str]]:
    result: Dict[str, List[str]] = {}
    lines = [line.strip() for line in text.splitlines() if line.strip()]

    for line in lines:
        if ":" not in line:
            continue

        day_part, task_part = line.split(":", 1)
        day_name = day_part.strip().lower()

        if day_name not in DAY_NAMES:
            continue

        tasks = [t.strip() for t in task_part.split(",") if t.strip()]
        iso_day = day_to_iso(day_name, week_start)
        result[iso_day] = tasks

    return result


def merge_days(existing: Dict[str, List[str]], incoming: Dict[str, List[str]]) -> Dict[str, List[str]]:
    merged = {k: list(v) for k, v in existing.items()}

    for day, new_tasks in incoming.items():
        current = merged.get(day, [])
        seen = {normalize_task(task) for task in current}

        for task in new_tasks:
            norm = normalize_task(task)
            if norm not in seen:
                current.append(task.strip())
                seen.add(norm)

        merged[day] = current

    return merged


def modify_days(existing: Dict[str, List[str]], incoming: Dict[str, List[str]]) -> Dict[str, List[str]]:
    updated = {k: list(v) for k, v in existing.items()}

    for day, tasks in incoming.items():
        updated[day] = tasks

    return updated


def replace_days(_: Dict[str, List[str]], incoming: Dict[str, List[str]]) -> Dict[str, List[str]]:
    return {k: list(v) for k, v in incoming.items()}


def format_person_schedule(person: str, person_schedule: Dict[str, List[str]]) -> str:
    if not person_schedule:
        return f"{person}: (blank)"

    lines = [f"{person}:"]

    for iso_day in sorted(person_schedule.keys()):
        day_label_str = datetime.fromisoformat(iso_day).strftime("%A")
        tasks = ", ".join(person_schedule[iso_day]) if person_schedule[iso_day] else "(blank)"
        lines.append(f"- {day_label_str} ({iso_day}): {tasks}")

    return "\n".join(lines)


def normalize_daily_items(raw_items: List[Any]) -> List[Dict[str, Any]]:
    normalized = []

    for item in raw_items:
        if isinstance(item, dict):
            text = str(item.get("text", "")).strip()
            done = bool(item.get("done", False))
        else:
            text = str(item).strip()
            done = False

        if text:
            normalized.append({"text": text, "done": done})

    return normalized


def parse_task_list(text: str) -> List[Dict[str, Any]]:
    cleaned = text.strip()

    if not cleaned:
        return []

    # Supports:
    # task one
    # task two
    #
    # or: task one, task two, task three
    if "\n" in cleaned:
        parts = [line.strip("-• 1234567890.").strip() for line in cleaned.splitlines()]
    else:
        parts = [part.strip() for part in cleaned.split(",")]

    return [{"text": part, "done": False} for part in parts if part]


def format_daily_tasks(session: DailySession) -> str:
    if not session.tasks:
        return "No tasks listed for today yet."

    lines = [f"📋 {session.person} — {session.date_label}"]

    for idx, task in enumerate(session.tasks, start=1):
        mark = "✅" if task.get("done") else "⬜"
        lines.append(f"{mark} {idx}. {task.get('text', '')}")

    return "\n".join(lines)


def find_best_task_match(tasks: List[Dict[str, Any]], text: str) -> Optional[int]:
    incoming = normalize_task(text)

    if not incoming:
        return None

    # First: exact-ish containment.
    for idx, task in enumerate(tasks):
        if task.get("done"):
            continue

        task_text = normalize_task(task.get("text", ""))

        if task_text and (task_text in incoming or incoming in task_text):
            return idx

    # Second: shared keyword match.
    incoming_words = {w for w in re.findall(r"[a-zA-Z0-9]+", incoming) if len(w) > 2}

    best_idx = None
    best_score = 0

    for idx, task in enumerate(tasks):
        if task.get("done"):
            continue

        task_words = {
            w for w in re.findall(r"[a-zA-Z0-9]+", normalize_task(task.get("text", "")))
            if len(w) > 2
        }

        score = len(incoming_words & task_words)

        if score > best_score:
            best_score = score
            best_idx = idx

    return best_idx if best_score >= 2 else None


def extract_task_text_from_accounting_entry(entry: str) -> str:
    """
    Supports both:
    - Revenue - sent estimate to Maria - 4
    - sent estimate to Maria

    For task matching, use the middle part when category/energy format is present.
    """
    parts = [p.strip() for p in entry.split("-")]

    if len(parts) >= 3:
        return " - ".join(parts[1:-1]).strip()

    return entry.strip()


class MeticheManager:
    def __init__(self, bot):
        self.bot = bot
        self.channel_id: Optional[int] = None
        self.bodydouble_on = False
        self.next_checkin: Optional[datetime] = None
        self.checkin_interval_hours = 2
        self.data_service_url = os.getenv("DATA_SERVICE_URL", "").rstrip("/")
        self.default_categories = list(DEFAULT_TASK_CATEGORIES)

    def turn_on_bodydouble(self, channel_id: int):
        self.channel_id = channel_id
        self.bodydouble_on = True
        self.next_checkin = datetime.now() + timedelta(hours=self.checkin_interval_hours)

    def turn_off_bodydouble(self):
        self.bodydouble_on = False
        self.next_checkin = None

    def build_bodydouble_prompt(self) -> str:
        categories = " / ".join(self.default_categories)

        return (
            "¿Qué onda?\n"
            "What did you just work on?\n"
            f"Categories: {categories}\n"
            "Reply naturally, or use: Category - Task - Energy(1-5)\n"
            "Example: Revenue - sent estimate to Maria - 4"
        )

    async def start_loop(self):
        while True:
            await asyncio.sleep(30)

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
        data = json.dumps(payload).encode("utf-8")

        req = request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST"
        )

        try:
            with request.urlopen(req, timeout=10) as resp:
                body = resp.read().decode("utf-8")
                return {"ok": True, "status": resp.status, "body": body}

        except error.HTTPError as e:
            return {"ok": False, "reason": f"HTTP {e.code}"}

        except Exception as e:
            return {"ok": False, "reason": str(e)}

    def push_calendar_json(self, week_of: str, person: str, person_schedule: Dict[str, List[Any]]) -> Dict[str, Any]:
        key_map = {
            "Heaven": "heaven",
            "Daniel": "daniel",
            "Handley Man": "handley_man"
        }

        calendar_key = key_map.get(person)

        payload = {
            "calendarKey": calendar_key,
            "schedule": person_schedule
        }

        return self.post_json("calendar", payload)

    def push_task_summary_json(self, week_of: str) -> Dict[str, Any]:
        plan = fetch_latest_metiche_weekly(week_of)

        if not plan:
            return {"ok": False, "reason": "No weekly plan found"}

        task_json = plan.get("task_summary_json") or {
            "Revenue": 0.0,
            "Infrastructure": 0.0,
            "Outreach": 0.0,
            "Admin": 0.0,
            "Drift": 0.0,
            "Life": 0.0,
            "entries": []
        }

        return self.post_json("tasks", task_json)


def get_metiche():
    return metiche_instance


def register_metiche(bot):
    global metiche_instance
    metiche_instance = MeticheManager(bot)

    async def log_task_accounting_entry(ctx: commands.Context, entry: str, quiet: bool = False):
        metiche = get_metiche()

        if metiche is None:
            if not quiet:
                await ctx.send("Metiche isn’t initialized yet.")
            return

        text = entry.strip()

        if not text:
            if not quiet:
                await ctx.send("Use: `Category - Task - Energy(1-5)`")
            return

        parts = [p.strip() for p in text.split("-")]

        if len(parts) >= 3:
            category = parts[0]
            task = " - ".join(parts[1:-1]).strip()
            energy_text = parts[-1]

            try:
                energy = int(re.findall(r"\d+", energy_text)[0])
            except Exception:
                energy = None
        else:
            category = "Uncategorized"
            task = text
            energy = None

        week = week_of_monday(datetime.now())
        plan = fetch_latest_metiche_weekly(week) or {}

        task_summary = plan.get("task_summary_json") or {
            "Revenue": 0.0,
            "Infrastructure": 0.0,
            "Outreach": 0.0,
            "Admin": 0.0,
            "Drift": 0.0,
            "Life": 0.0,
            "entries": []
        }

        task_summary.setdefault("entries", [])
        task_summary.setdefault(category, 0.0)

        task_summary["entries"].append({
            "timestamp": now_iso(),
            "user": str(ctx.author),
            "category": category,
            "task": task,
            "energy": energy,
        })

        # Current assumption: each check-in / bodydouble response = 2 hours.
        if isinstance(task_summary.get(category), (int, float)):
            task_summary[category] += 2.0

        insert_metiche_checkin({
            "ts": now_iso(),
            "discord_user": str(ctx.author),
            "channel_id": str(ctx.channel.id),
            "week_of": week,
            "category": category,
            "task": task,
            "energy": energy,
        })

        weekly_goal = float(plan.get("weekly_goal", 0.0) or 0.0)
        jobs = plan.get("jobs", []) or []
        pending_estimates = plan.get("pending_estimates", []) or []
        invoices_to_send = plan.get("invoices_to_send", []) or []
        calendar_json = plan.get("calendar_json", {}) or {"Heaven": {}, "Daniel": {}, "Handley Man": {}}
        wants_bodydouble = bool(plan.get("wants_bodydouble", True))
        quarterly_goals = plan.get("quarterly_goals", []) or []
        yearly_goals = plan.get("yearly_goals", []) or []

        insert_metiche_weekly({
            "ts": now_iso(),
            "discord_user": str(ctx.author),
            "channel_id": str(ctx.channel.id),
            "week_of": week,
            "weekly_goal": weekly_goal,
            "jobs_json": json.dumps(jobs, ensure_ascii=False),
            "pending_estimates_json": json.dumps(pending_estimates, ensure_ascii=False),
            "invoices_to_send_json": json.dumps(invoices_to_send, ensure_ascii=False),
            "calendar_json": json.dumps(calendar_json, ensure_ascii=False),
            "task_summary_json": json.dumps(task_summary, ensure_ascii=False),
            "wants_bodydouble": wants_bodydouble,
            "quarterly_goals_json": json.dumps(quarterly_goals, ensure_ascii=False),
            "yearly_goals_json": json.dumps(yearly_goals, ensure_ascii=False),
        })

        push_result = metiche.push_task_summary_json(week)

        if not quiet:
            status_line = (
                "Pushed task JSON."
                if push_result.get("ok")
                else f"Saved, but task push failed: {push_result.get('reason')}"
            )

            await ctx.send(f"Logged: {category} - {task} - energy {energy or '?'}\n{status_line}")

    async def mark_daily_task_done(ctx: commands.Context, entry: str):
        session = active_daily_sessions.get(ctx.channel.id)
        if not session:
            return False

        task_text = extract_task_text_from_accounting_entry(entry)
        match_idx = find_best_task_match(session.tasks, task_text)

        if match_idx is None:
            session.tasks.append({"text": task_text, "done": True})
            completed_text = task_text
        else:
            session.tasks[match_idx]["done"] = True
            completed_text = session.tasks[match_idx]["text"]

        await ctx.send(f"✅ {completed_text}")

        # Push checked state to the calendar dashboard.
        metiche = get_metiche()
        if metiche is not None:
            week = week_of_monday(datetime.now())
            plan = fetch_latest_metiche_weekly(week) or {}
            calendar_json = plan.get("calendar_json", {}) or {"Heaven": {}, "Daniel": {}, "Handley Man": {}}
            person_schedule = calendar_json.get(session.person, {}) or {}
            person_schedule[session.date_iso] = session.tasks
            calendar_json[session.person] = person_schedule

            weekly_goal = float(plan.get("weekly_goal", 0.0) or 0.0)
            jobs = plan.get("jobs", []) or []
            pending_estimates = plan.get("pending_estimates", []) or []
            invoices_to_send = plan.get("invoices_to_send", []) or []
            wants_bodydouble = bool(plan.get("wants_bodydouble", True))
            quarterly_goals = plan.get("quarterly_goals", []) or []
            yearly_goals = plan.get("yearly_goals", []) or []
            task_summary = plan.get("task_summary_json") or {
                "Revenue": 0.0,
                "Infrastructure": 0.0,
                "Outreach": 0.0,
                "Admin": 0.0,
                "Drift": 0.0,
                "Life": 0.0,
                "entries": []
            }

            insert_metiche_weekly({
                "ts": now_iso(),
                "discord_user": str(ctx.author),
                "channel_id": str(ctx.channel.id),
                "week_of": week,
                "weekly_goal": weekly_goal,
                "jobs_json": json.dumps(jobs, ensure_ascii=False),
                "pending_estimates_json": json.dumps(pending_estimates, ensure_ascii=False),
                "invoices_to_send_json": json.dumps(invoices_to_send, ensure_ascii=False),
                "calendar_json": json.dumps(calendar_json, ensure_ascii=False),
                "task_summary_json": json.dumps(task_summary, ensure_ascii=False),
                "wants_bodydouble": wants_bodydouble,
                "quarterly_goals_json": json.dumps(quarterly_goals, ensure_ascii=False),
                "yearly_goals_json": json.dumps(yearly_goals, ensure_ascii=False),
            })

            metiche.push_calendar_json(week, session.person, person_schedule)

        return True

    @bot.command(name="metichebot")
    async def metichebot_help(ctx):
        msg = """
🧠 METICHEBOT

Metiche is for daily goals, scheduling, task accounting, and body-doubling.

ACTIVE FUNCTIONS

Planning
!mweekly
Save weekly goal, jobs, pending estimates, and invoices

!mschedule
Build or update schedule for Heaven, Daniel, or Handley Man

!mplan
Show the current saved weekly plan

Daily Use
!mtoday
Start today's working list. Metiche will verify the date, show today's goals, accept changes, and repeat the list with checkmarks as you work.

!mstopday
Stop today’s active checklist if Metiche starts treating setup messages like tasks

Execution
!mbodydouble
Turn on task accounting check-ins. Metichebot will ask “Qué onda?” every 2 hours.

!mquiet
Turn off check-ins

!mcheckin <Category - Task - Energy(1-5)>
Manual task accounting entry

Example:
!mcheckin Revenue - sent estimate to Maria - 4

Strategy
!mgoals
Save quarterly and yearly goals
"""
        await ctx.send(msg)

@bot.command(name="mschedule")
async def mschedule(ctx: commands.Context):
    active_daily_sessions.pop(ctx.channel.id, None)
                
    metiche = get_metiche()
    
    if metiche is None:
        await ctx.send("Metiche isn’t initialized yet.")
        return

        def check(m: discord.Message):
            return (m.author.id == ctx.author.id) and (m.channel.id == ctx.channel.id)

        week = week_of_monday(datetime.now())
        current_plan = fetch_latest_metiche_weekly(week) or {}
        calendar_json: Dict[str, Any] = current_plan.get("calendar_json", {}) or {}

        await ctx.send("Who’s schedule are we working on?\n(Heaven / Daniel / Handley Man)")
        who_msg = await bot.wait_for("message", check=check)

        person_raw = who_msg.content.strip()
        person = next((p for p in VALID_PEOPLE if p.lower() == person_raw.lower()), None)

        if not person:
            await ctx.send("I need one of: Heaven / Daniel / Handley Man")
            return

        person_schedule = calendar_json.get(person, {})

        await ctx.send(
            format_person_schedule(person, person_schedule)
            + "\n\nWhat do you want to do?\n\n"
            "1. Add to schedule (keep everything, add new tasks)\n"
            "2. Change specific days\n"
            "3. Start over\n\n"
            "Reply with 1, 2, or 3"
        )

        mode_msg = await bot.wait_for("message", check=check)
        mode_raw = mode_msg.content.strip().lower()

        if mode_raw in {"cancel", "exit", "stop"}:
            await ctx.send("Okay. Exiting schedule flow.")
            return

        if mode_raw == "1":
            mode = "merge"
        elif mode_raw == "2":
            mode = "modify"
        elif mode_raw == "3":
            mode = "replace"
        else:
            await ctx.send("Reply with 1, 2, or 3 (or cancel).")
            return

        await ctx.send(
            "What does the weekly schedule look like?\n\n"
            "Use format:\n"
            "Monday: task, task\n"
            "Tuesday: task"
        )

        schedule_msg = await bot.wait_for("message", check=check)
        incoming = parse_schedule_block(schedule_msg.content, week)

        if not incoming:
            await ctx.send("I couldn’t parse that. Use lines like `Monday: task, task`.")
            return

        if mode == "merge":
            updated_person_schedule = merge_days(person_schedule, incoming)
        elif mode == "modify":
            updated_person_schedule = modify_days(person_schedule, incoming)
        else:
            updated_person_schedule = replace_days(person_schedule, incoming)

        calendar_json.setdefault("Heaven", {})
        calendar_json.setdefault("Daniel", {})
        calendar_json.setdefault("Handley Man", {})
        calendar_json[person] = updated_person_schedule

        weekly_goal = float(current_plan.get("weekly_goal", 0.0) or 0.0)
        jobs = current_plan.get("jobs", []) or []
        pending_estimates = current_plan.get("pending_estimates", []) or []
        invoices_to_send = current_plan.get("invoices_to_send", []) or []
        wants_bodydouble = bool(current_plan.get("wants_bodydouble", False))
        quarterly_goals = current_plan.get("quarterly_goals", []) or []
        yearly_goals = current_plan.get("yearly_goals", []) or []
        task_summary = current_plan.get("task_summary_json") or {
            "Revenue": 0.0,
            "Infrastructure": 0.0,
            "Outreach": 0.0,
            "Admin": 0.0,
            "Drift": 0.0,
            "Life": 0.0,
            "entries": []
        }

        insert_metiche_weekly({
            "ts": now_iso(),
            "discord_user": str(ctx.author),
            "channel_id": str(ctx.channel.id),
            "week_of": week,
            "weekly_goal": weekly_goal,
            "jobs_json": json.dumps(jobs, ensure_ascii=False),
            "pending_estimates_json": json.dumps(pending_estimates, ensure_ascii=False),
            "invoices_to_send_json": json.dumps(invoices_to_send, ensure_ascii=False),
            "calendar_json": json.dumps(calendar_json, ensure_ascii=False),
            "task_summary_json": json.dumps(task_summary, ensure_ascii=False),
            "wants_bodydouble": wants_bodydouble,
            "quarterly_goals_json": json.dumps(quarterly_goals, ensure_ascii=False),
            "yearly_goals_json": json.dumps(yearly_goals, ensure_ascii=False),
        })

        push_result = metiche.push_calendar_json(week, person, updated_person_schedule)

        status_line = (
            "Pushed to dashboard JSON."
            if push_result.get("ok")
            else f"Saved, but dashboard push failed: {push_result.get('reason')}"
        )

        await ctx.send(format_person_schedule(person, updated_person_schedule) + f"\n\n{status_line}")

    @bot.command(name="mtoday")
    async def mtoday(ctx: commands.Context):
        metiche = get_metiche()

        if metiche is None:
            await ctx.send("Metiche isn’t initialized yet.")
            return

        def check(m: discord.Message):
            return (m.author.id == ctx.author.id) and (m.channel.id == ctx.channel.id)

        week = week_of_monday(datetime.now())
        current_plan = fetch_latest_metiche_weekly(week) or {}
        calendar_json: Dict[str, Any] = current_plan.get("calendar_json", {}) or {}

        await ctx.send(
            f"Today is {today_label()}.\n\n"
            "Who are we working as today?\n"
            "(Heaven / Daniel / Handley Man)"
        )

        who_msg = await bot.wait_for("message", check=check)
        person_raw = who_msg.content.strip()
        person = next((p for p in VALID_PEOPLE if p.lower() == person_raw.lower()), None)

        if not person:
            await ctx.send("I need one of: Heaven / Daniel / Handley Man")
            return

        date_key = today_iso()
        person_schedule = calendar_json.get(person, {}) or {}
        existing_today = normalize_daily_items(person_schedule.get(date_key, []))

        weekly_goal = float(current_plan.get("weekly_goal", 0.0) or 0.0)
        quarterly_goals = current_plan.get("quarterly_goals", []) or []
        yearly_goals = current_plan.get("yearly_goals", []) or []

        goal_lines = [
            f"📅 Today is {today_label()}",
            f"💰 Weekly goal: ${weekly_goal:,.0f}" if weekly_goal else "💰 Weekly goal: not set",
        ]

        if quarterly_goals:
            goal_lines.append("🎯 Quarterly goals: " + ", ".join(quarterly_goals))
        if yearly_goals:
            goal_lines.append("🧭 Yearly goals: " + ", ".join(yearly_goals))

        draft_session = DailySession(
            channel_id=ctx.channel.id,
            person=person,
            date_iso=date_key,
            date_label=today_label(),
            tasks=existing_today,
        )

        await ctx.send(
            "\n".join(goal_lines)
            + "\n\n"
            + format_daily_tasks(draft_session)
            + "\n\n"
            "Any changes for today?\n"
            "Reply with the new full list for today, or say `no changes`."
        )

        changes_msg = await bot.wait_for("message", check=check)
        changes = changes_msg.content.strip()

        if changes.lower() not in {"no", "no changes", "same", "keep"}:
            new_tasks = parse_task_list(changes)
            if new_tasks:
                draft_session.tasks = new_tasks
            else:
                await ctx.send("I couldn’t read that as a list, so I kept the current list.")

        active_daily_sessions[ctx.channel.id] = draft_session

        # Save today's working list back into calendar_json so the dashboard can show it.
        person_schedule[date_key] = draft_session.tasks
        calendar_json[person] = person_schedule

        jobs = current_plan.get("jobs", []) or []
        pending_estimates = current_plan.get("pending_estimates", []) or []
        invoices_to_send = current_plan.get("invoices_to_send", []) or []
        wants_bodydouble = bool(current_plan.get("wants_bodydouble", True))
        task_summary = current_plan.get("task_summary_json") or {
            "Revenue": 0.0,
            "Infrastructure": 0.0,
            "Outreach": 0.0,
            "Admin": 0.0,
            "Drift": 0.0,
            "Life": 0.0,
            "entries": []
        }

        insert_metiche_weekly({
            "ts": now_iso(),
            "discord_user": str(ctx.author),
            "channel_id": str(ctx.channel.id),
            "week_of": week,
            "weekly_goal": weekly_goal,
            "jobs_json": json.dumps(jobs, ensure_ascii=False),
            "pending_estimates_json": json.dumps(pending_estimates, ensure_ascii=False),
            "invoices_to_send_json": json.dumps(invoices_to_send, ensure_ascii=False),
            "calendar_json": json.dumps(calendar_json, ensure_ascii=False),
            "task_summary_json": json.dumps(task_summary, ensure_ascii=False),
            "wants_bodydouble": wants_bodydouble,
            "quarterly_goals_json": json.dumps(quarterly_goals, ensure_ascii=False),
            "yearly_goals_json": json.dumps(yearly_goals, ensure_ascii=False),
        })

        push_result = metiche.push_calendar_json(week, person, person_schedule)

        await ctx.send(
            "Locked for today.\n\n"
            + format_daily_tasks(draft_session)
            + "\n\n"
            "As you do things, just type what you did. I’ll check it off."
        )

        if not push_result.get("ok"):
            await ctx.send(f"Saved, but dashboard push failed: {push_result.get('reason')}")
            
    @bot.command(name="mstopday")
    async def mstopday(ctx: commands.Context):
        active_daily_sessions.pop(ctx.channel.id, None)
        await ctx.send("Okay. I stopped today’s active checklist.")

    @bot.command(name="mplan")
    async def mplan(ctx: commands.Context):
        week = week_of_monday(datetime.now())
        plan = fetch_latest_metiche_weekly(week)

        if not plan:
            await ctx.send("No weekly plan saved yet.")
            return

        calendar_json = plan.get("calendar_json", {}) or {}

        lines = [f"📌 Weekly plan ({week})"]

        for person in VALID_PEOPLE:
            lines.append(format_person_schedule(person, calendar_json.get(person, {})))

        await ctx.send("\n\n".join(lines))

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
            return (m.author.id == ctx.author.id) and (m.channel.id == ctx.channel.id)

        week = week_of_monday(datetime.now())
        current_plan = fetch_latest_metiche_weekly(week) or {}

        await ctx.send("What are the quarterly goals?\nComma-separated, or `none`.")
        q_msg = await bot.wait_for("message", check=check)
        quarterly_goals = [
            x.strip()
            for x in q_msg.content.split(",")
            if x.strip() and x.strip().lower() != "none"
        ]

        await ctx.send("What are the yearly goals?\nComma-separated, or `none`.")
        y_msg = await bot.wait_for("message", check=check)
        yearly_goals = [
            x.strip()
            for x in y_msg.content.split(",")
            if x.strip() and x.strip().lower() != "none"
        ]

        weekly_goal = float(current_plan.get("weekly_goal", 0.0) or 0.0)
        jobs = current_plan.get("jobs", []) or []
        pending_estimates = current_plan.get("pending_estimates", []) or []
        invoices_to_send = current_plan.get("invoices_to_send", []) or []
        calendar_json = current_plan.get("calendar_json", {}) or {"Heaven": {}, "Daniel": {}, "Handley Man": {}}
        wants_bodydouble = bool(current_plan.get("wants_bodydouble", False))
        task_summary = current_plan.get("task_summary_json") or {
            "Revenue": 0.0,
            "Infrastructure": 0.0,
            "Outreach": 0.0,
            "Admin": 0.0,
            "Drift": 0.0,
            "Life": 0.0,
            "entries": []
        }

        insert_metiche_weekly({
            "ts": now_iso(),
            "discord_user": str(ctx.author),
            "channel_id": str(ctx.channel.id),
            "week_of": week,
            "weekly_goal": weekly_goal,
            "jobs_json": json.dumps(jobs, ensure_ascii=False),
            "pending_estimates_json": json.dumps(pending_estimates, ensure_ascii=False),
            "invoices_to_send_json": json.dumps(invoices_to_send, ensure_ascii=False),
            "calendar_json": json.dumps(calendar_json, ensure_ascii=False),
            "task_summary_json": json.dumps(task_summary, ensure_ascii=False),
            "wants_bodydouble": wants_bodydouble,
            "quarterly_goals_json": json.dumps(quarterly_goals, ensure_ascii=False),
            "yearly_goals_json": json.dumps(yearly_goals, ensure_ascii=False),
        })

        await ctx.send("Locked. I saved your quarterly and yearly goals.")

    @bot.command(name="mweekly")
    async def mweekly(ctx: commands.Context):
        def check(m: discord.Message):
            return (m.author.id == ctx.author.id) and (m.channel.id == ctx.channel.id)

        week = week_of_monday(datetime.now())
        current_plan = fetch_latest_metiche_weekly(week) or {}

        await ctx.send("Weekly financial goal (number):")
        goal_msg = await bot.wait_for("message", check=check)

        try:
            weekly_goal = float(goal_msg.content.replace("$", "").replace(",", "").strip())
        except Exception:
            weekly_goal = 0.0

        await ctx.send("Jobs for the week (comma-separated or `none`):")
        jobs_msg = await bot.wait_for("message", check=check)
        jobs = [
            x.strip()
            for x in jobs_msg.content.split(",")
            if x.strip() and x.strip().lower() != "none"
        ]

        await ctx.send("Pending estimates (comma-separated or `none`):")
        est_msg = await bot.wait_for("message", check=check)
        pending_estimates = [
            x.strip()
            for x in est_msg.content.split(",")
            if x.strip() and x.strip().lower() != "none"
        ]

        await ctx.send("Invoices to send (comma-separated or `none`):")
        inv_msg = await bot.wait_for("message", check=check)
        invoices_to_send = [
            x.strip()
            for x in inv_msg.content.split(",")
            if x.strip() and x.strip().lower() != "none"
        ]

        calendar_json = current_plan.get("calendar_json", {}) or {"Heaven": {}, "Daniel": {}, "Handley Man": {}}
        quarterly_goals = current_plan.get("quarterly_goals", []) or []
        yearly_goals = current_plan.get("yearly_goals", []) or []
        wants_bodydouble = bool(current_plan.get("wants_bodydouble", False))
        task_summary = current_plan.get("task_summary_json") or {
            "Revenue": 0.0,
            "Infrastructure": 0.0,
            "Outreach": 0.0,
            "Admin": 0.0,
            "Drift": 0.0,
            "Life": 0.0,
            "entries": []
        }

        insert_metiche_weekly({
            "ts": now_iso(),
            "discord_user": str(ctx.author),
            "channel_id": str(ctx.channel.id),
            "week_of": week,
            "weekly_goal": weekly_goal,
            "jobs_json": json.dumps(jobs, ensure_ascii=False),
            "pending_estimates_json": json.dumps(pending_estimates, ensure_ascii=False),
            "invoices_to_send_json": json.dumps(invoices_to_send, ensure_ascii=False),
            "calendar_json": json.dumps(calendar_json, ensure_ascii=False),
            "task_summary_json": json.dumps(task_summary, ensure_ascii=False),
            "wants_bodydouble": wants_bodydouble,
            "quarterly_goals_json": json.dumps(quarterly_goals, ensure_ascii=False),
            "yearly_goals_json": json.dumps(yearly_goals, ensure_ascii=False),
        })

        await ctx.send("Locked. Weekly execution layer saved.")

    @bot.command(name="mcheckin")
    async def mcheckin(ctx: commands.Context, *, entry: str = ""):
        await log_task_accounting_entry(ctx, entry, quiet=False)
        await mark_daily_task_done(ctx, entry)

    @bot.listen("on_message")
    async def metiche_bodydouble_listener(message: discord.Message):
        metiche = get_metiche()

        if message.author.bot:
            return

        if metiche is None:
            return

        if message.content.startswith("!"):
            return

        ctx = await bot.get_context(message)

        if message.channel.id in active_daily_sessions:
            await log_task_accounting_entry(ctx, message.content, quiet=True)
            await mark_daily_task_done(ctx, message.content)
            return

        if not metiche.bodydouble_on:
            return

        if metiche.channel_id != message.channel.id:
            return

        await log_task_accounting_entry(ctx, message.content, quiet=False)
