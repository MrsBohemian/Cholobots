import asyncio
import json
import os
import random
import re
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
DEFAULT_TASK_CATEGORIES = ["Revenue", "Infrastructure", "Outreach", "Admin", "Drift", "Life"]


def week_of_monday(d: datetime) -> str:
    monday = d.date() - timedelta(days=d.weekday())
    return monday.isoformat()


def monday_of_current_week() -> datetime:
    now = datetime.now()
    monday = now.date() - timedelta(days=now.weekday())
    return datetime.combine(monday, datetime.min.time())


def normalize_task(task: str) -> str:
    return re.sub(r"\s+", " ", task.strip().lower())


def day_to_iso(day_name: str, week_start: str) -> str:
    base = datetime.fromisoformat(week_start)
    offset = DAY_NAMES.index(day_name.lower())
    return (base + timedelta(days=offset)).date().isoformat()


def parse_schedule_block(text: str, week_start: str) -> Dict[str, List[str]]:
    """
    Expected format:
    Monday: task, task
    Tuesday: task
    """
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
        day_label = datetime.fromisoformat(iso_day).strftime("%A")
        tasks = ", ".join(person_schedule[iso_day]) if person_schedule[iso_day] else "(blank)"
        lines.append(f"- {day_label} ({iso_day}): {tasks}")
    return "\n".join(lines)


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
            "Reply: Category - Task - Energy(1-5)"
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
        req = request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with request.urlopen(req, timeout=10) as resp:
                body = resp.read().decode("utf-8")
                return {"ok": True, "status": resp.status, "body": body}
        except error.HTTPError as e:
            return {"ok": False, "reason": f"HTTP {e.code}"}
        except Exception as e:
            return {"ok": False, "reason": str(e)}

    def push_calendar_json(self, week_of: str, person: str, person_schedule: Dict[str, List[str]]) -> Dict[str, Any]:
        plan = fetch_latest_metiche_weekly(week_of)
        calendar_json: Dict[str, Any] = plan.get("calendar_json", {}) if plan else {}
        calendar_json.setdefault("Heaven", {})
        calendar_json.setdefault("Daniel", {})
        calendar_json.setdefault("Handley Man", {})
        calendar_json[person] = person_schedule
        return self.post_json("calendar", calendar_json)

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
            
    @bot.command(name="metichebot")
    async def metichebot_help(ctx):
        msg = """
🧠 METICHEBOT

Metiche is for planning, scheduling, check-ins, and task accounting.

ACTIVE FUNCTIONS

Planning
!mweekly
Save weekly goal, jobs, pending estimates, and invoices

!mschedule
Build or update schedule for Heaven, Daniel, or Handley Man

!mplan
Show the current saved weekly plan

Execution
!mbodydouble
Turn on check-ins

!mquiet
Turn off check-ins

!mcheckin <Category - Task - Energy(1-5)>
Log a manual task check-in

Strategy
!mgoals
Save quarterly and yearly goals
"""
        await ctx.send(msg)

    @bot.command(name="mschedule")
    async def mschedule(ctx: commands.Context):
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
            + "\n\nDo you want to merge, modify, or replace?"
        )
        mode_msg = await bot.wait_for("message", check=check)
        mode = mode_msg.content.strip().lower()
        if mode in {"cancel", "exit", "stop"}:
            await ctx.send("Okay. Exiting schedule flow.")
            return
        
        if mode not in {"merge", "modify", "replace"}:
            await ctx.send("Reply with exactly: merge, modify, or replace, or type cancel.")
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
            "wants_bodydouble": wants_bodydouble,
            "quarterly_goals_json": json.dumps(quarterly_goals, ensure_ascii=False),
            "yearly_goals_json": json.dumps(yearly_goals, ensure_ascii=False),
        })

        push_result = metiche.push_calendar_json(week, person, updated_person_schedule)
        status_line = "Pushed to dashboard JSON." if push_result.get("ok") else f"Saved, but dashboard push failed: {push_result.get('reason')}"
        await ctx.send(format_person_schedule(person, updated_person_schedule) + f"\n\n{status_line}")

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
        await ctx.send("I’m here. What’s the first thing you’re doing?")

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
        quarterly_goals = [x.strip() for x in q_msg.content.split(",") if x.strip() and x.strip().lower() != "none"]

        await ctx.send("What are the yearly goals?\nComma-separated, or `none`.")
        y_msg = await bot.wait_for("message", check=check)
        yearly_goals = [x.strip() for x in y_msg.content.split(",") if x.strip() and x.strip().lower() != "none"]

        weekly_goal = float(current_plan.get("weekly_goal", 0.0) or 0.0)
        jobs = current_plan.get("jobs", []) or []
        pending_estimates = current_plan.get("pending_estimates", []) or []
        invoices_to_send = current_plan.get("invoices_to_send", []) or []
        calendar_json = current_plan.get("calendar_json", {}) or {"Heaven": {}, "Daniel": {}, "Handley Man": {}}
        wants_bodydouble = bool(current_plan.get("wants_bodydouble", False))

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
        jobs = [x.strip() for x in jobs_msg.content.split(",") if x.strip() and x.strip().lower() != "none"]

        await ctx.send("Pending estimates (comma-separated or `none`):")
        est_msg = await bot.wait_for("message", check=check)
        pending_estimates = [x.strip() for x in est_msg.content.split(",") if x.strip() and x.strip().lower() != "none"]

        await ctx.send("Invoices to send (comma-separated or `none`):")
        inv_msg = await bot.wait_for("message", check=check)
        invoices_to_send = [x.strip() for x in inv_msg.content.split(",") if x.strip() and x.strip().lower() != "none"]

        calendar_json = current_plan.get("calendar_json", {}) or {"Heaven": {}, "Daniel": {}, "Handley Man": {}}
        quarterly_goals = current_plan.get("quarterly_goals", []) or []
        yearly_goals = current_plan.get("yearly_goals", []) or []
        wants_bodydouble = bool(current_plan.get("wants_bodydouble", False))

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
            "wants_bodydouble": wants_bodydouble,
            "quarterly_goals_json": json.dumps(quarterly_goals, ensure_ascii=False),
            "yearly_goals_json": json.dumps(yearly_goals, ensure_ascii=False),
        })

        await ctx.send("Locked. Weekly execution layer saved.")

    @bot.command(name="mcheckin")
    async def mcheckin(ctx: commands.Context, *, entry: str = ""):
        metiche = get_metiche()
        if metiche is None:
            await ctx.send("Metiche isn’t initialized yet.")
            return

        text = entry.strip()
        if not text:
            await ctx.send("Use: `!mcheckin Category - Task - Energy(1-5)`")
            return

        parts = [p.strip() for p in text.split("-")]
        if len(parts) < 3:
            await ctx.send("Use: `!mcheckin Category - Task - Energy(1-5)`")
            return

        category = parts[0]
        task = " - ".join(parts[1:-1]).strip()
        energy_text = parts[-1]
        try:
            energy = int(re.findall(r"\d+", energy_text)[0])
        except Exception:
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

        # Lightweight assumption: each manual checkin = 2 hours unless later refined.
        if category in task_summary and isinstance(task_summary[category], (int, float)):
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
        status_line = "Pushed task JSON." if push_result.get("ok") else f"Saved, but task push failed: {push_result.get('reason')}"
        await ctx.send(f"Logged: {category} - {task} - energy {energy or '?'}\n{status_line}")
