import asyncio
import json
import random
from datetime import datetime, time, timedelta
from typing import Optional, Dict, Any

import discord
from discord.ext import commands

from db.database import (
    now_iso,
    insert_metiche_log,
    insert_metiche_weekly,
    insert_metiche_checkin,
    fetch_latest_metiche_weekly,
)

metiche_instance = None


class MeticheManager:
    def __init__(self, bot):
        self.bot = bot
        self.state = {}
        self.channel_id = None
        self.schedule = []
        self.accountant_on = False
        self.next_accountant_check = None
        self.awaiting_accountant_choice = False

    def turn_on(self, channel_id: int):
        self.channel_id = channel_id
        self.accountant_on = True
        self.next_accountant_check = datetime.now() + timedelta(hours=2)

    def turn_off(self):
        self.accountant_on = False
        self.next_accountant_check = None

    def declare_targets(self, revenue, outreach, infra, cash_rule):
        self.state["targets"] = {
            "revenue": revenue,
            "outreach": outreach,
            "infra": infra,
            "cash_rule": cash_rule,
            "declared": datetime.now().isoformat()
        }

    def generate_daily_schedule(self):
        today = datetime.now().date()
        windows = [
            (time(8, 30), time(10, 0)),
            (time(11, 30), time(13, 0)),
            (time(14, 30), time(16, 0)),
            (time(19, 0), time(20, 30)),
        ]

        self.schedule = []
        for start, end in windows:
            start_dt = datetime.combine(today, start)
            end_dt = datetime.combine(today, end)
            delta = int((end_dt - start_dt).total_seconds())
            rand_sec = random.randint(0, delta)
            self.schedule.append(start_dt + timedelta(seconds=rand_sec))

    async def start_loop(self):
        while True:
            now_time = datetime.now().time()

            if now_time >= time(17, 0):
                await asyncio.sleep(1800)
                continue

            if not self.channel_id:
                await asyncio.sleep(60)
                continue

            now = datetime.now()

            for check_time in list(self.schedule):
                if now >= check_time:
                    channel = self.bot.get_channel(self.channel_id)
                    if channel:
                        await channel.send(self.build_checkin_message())
                    self.schedule.remove(check_time)

            await asyncio.sleep(30)

            if self.accountant_on and self.next_accountant_check:
                if datetime.now() >= self.next_accountant_check:
                    channel = self.bot.get_channel(self.channel_id)
                    if channel:
                        await channel.send(
                            "¿Qué onda? Last 2 hours.\n"
                            "Revenue / Infrastructure / Outreach / Admin / Drift?\n"
                            "Reply: Category - Task - Energy(1-5)"
                        )
                    self.next_accountant_check = datetime.now() + timedelta(hours=2)

    def build_checkin_message(self):
        t = self.state.get("targets", {})
        return (
            f"Revenue Target: ${t.get('revenue',0)}\n"
            f"Outreach Target: {t.get('outreach',0)}\n"
            f"Infra Target: {t.get('infra',0)} hrs\n\n"
            "¿Qué onda?\n"
            "Revenue / Infrastructure / Outreach / Admin / Drift?\n"
            "Reply: Category - Task - Energy(1-5)"
        )


def week_of_monday(d: datetime) -> str:
    monday = d.date() - timedelta(days=d.weekday())
    return monday.isoformat()


def get_metiche():
    return metiche_instance


def register_metiche(bot):
    global metiche_instance
    metiche_instance = MeticheManager(bot)

    @bot.command(name="metiche_declare")
    async def metiche_declare(ctx, revenue: float, outreach: int, infra: float, *, cash_rule: str):
        metiche = get_metiche()
        if metiche is None:
            await ctx.send("Metiche isn’t initialized yet. Restart the bot.")
            return

        metiche.channel_id = ctx.channel.id
        metiche.declare_targets(revenue, outreach, infra, cash_rule)
        metiche.generate_daily_schedule()
        metiche.awaiting_accountant_choice = True

        await ctx.send(
            "Targets locked.\n\n"
            "Do you want Metiche to run **task accounting today**?\n"
            "Reply **yes** to enable 2-hour check-ins or **no**."
        )

    @bot.command(name="metiche_times")
    async def metiche_times(ctx):
        metiche = get_metiche()
        if metiche is None:
            await ctx.send("Metiche isn’t initialized yet.")
            return

        if not metiche.schedule:
            await ctx.send("No check-ins scheduled.")
            return

        lines = [t.strftime("%H:%M") for t in metiche.schedule]
        await ctx.send("Metiche check-in times today:\n" + "\n".join(lines))

    @bot.command(name="metiche_ping")
    async def metiche_ping(ctx):
        metiche = get_metiche()
        if metiche is None:
            await ctx.send("Metiche isn’t initialized yet.")
            return
        metiche.channel_id = ctx.channel.id
        await ctx.send(metiche.build_checkin_message())

    @bot.command(name="metiche_log")
    async def metiche_log(ctx: commands.Context, job: str, hours: float, cost: float = 0.0, *, note: str = ""):
        job = (job or "").strip()
        if not job:
            await ctx.send("Use: `!metiche_log <job> <hours> <cost> <note>`")
            return

        row = {
            "ts": now_iso(),
            "discord_user": str(ctx.author),
            "channel_id": str(ctx.channel.id),
            "job": job,
            "kind": "LABOR",
            "hours": float(hours),
            "cost": float(cost),
            "note": note.strip() or None,
        }
        insert_metiche_log(row)
        await ctx.send(f"🧾 Logged labor for `{job}`: {hours:g} hrs, ${cost:,.2f} ({note.strip() or 'no note'})")

    @bot.command(name="metiche_weekly")
    async def metiche_weekly(ctx: commands.Context):
        await ctx.send(
            "Alright. Weekly money + execution plan. Answer each prompt.\n"
            "If something is none, type `none`."
        )

        def check(m: discord.Message):
            return (m.author.id == ctx.author.id) and (m.channel.id == ctx.channel.id)

        week = week_of_monday(datetime.now())

        await ctx.send("1) Weekly financial goal (number):")
        goal_msg = await bot.wait_for("message", check=check)
        try:
            weekly_goal = float(goal_msg.content.replace("$", "").replace(",", "").strip())
        except Exception:
            weekly_goal = 0.0

        await ctx.send("2) Jobs for the week (comma-separated):")
        jobs_msg = await bot.wait_for("message", check=check)
        jobs = [x.strip() for x in jobs_msg.content.split(",") if x.strip() and x.strip().lower() != "none"]

        await ctx.send("3) Pending estimates (comma-separated or `none`):")
        est_msg = await bot.wait_for("message", check=check)
        pending_estimates = [x.strip() for x in est_msg.content.split(",") if x.strip() and x.strip().lower() != "none"]

        await ctx.send("4) Invoices to send (comma-separated or `none`):")
        inv_msg = await bot.wait_for("message", check=check)
        invoices_to_send = [x.strip() for x in inv_msg.content.split(",") if x.strip() and x.strip().lower() != "none"]

        await ctx.send("5) What’s on the schedule for TODAY? (paste a short block):")
        sched_msg = await bot.wait_for("message", check=check)
        todays_schedule = sched_msg.content.strip()

        await ctx.send("6) Do you want Metiche to be the daily task accountant? (yes/no)")
        acc_msg = await bot.wait_for("message", check=check)
        wants_accountant = acc_msg.content.strip().lower().startswith("y")

        insert_metiche_weekly({
            "ts": now_iso(),
            "discord_user": str(ctx.author),
            "channel_id": str(ctx.channel.id),
            "week_of": week,
            "weekly_goal": weekly_goal,
            "jobs_json": json.dumps(jobs, ensure_ascii=False),
            "pending_estimates_json": json.dumps(pending_estimates, ensure_ascii=False),
            "invoices_to_send_json": json.dumps(invoices_to_send, ensure_ascii=False),
            "todays_schedule": todays_schedule,
            "wants_accountant": wants_accountant,
        })

        await ctx.send("Locked. I saved it. If you want the 2-hour check-ins, run `!metiche_on` in this channel.")

    @bot.command(name="metiche_plan")
    async def metiche_plan(ctx: commands.Context):
        week = week_of_monday(datetime.now())
        plan = fetch_latest_metiche_weekly(week)
        if not plan:
            await ctx.send("No weekly plan saved yet. Run `!metiche_weekly`.")
            return

        msg = (
            f"📌 **Weekly plan (week of {plan['week_of']})**\n"
            f"- Goal: ${plan['weekly_goal']:,.2f}\n"
            f"- Jobs: {', '.join(plan['jobs']) or '(none)'}\n"
            f"- Pending estimates: {', '.join(plan['pending_estimates']) or '(none)'}\n"
            f"- Invoices to send: {', '.join(plan['invoices_to_send']) or '(none)'}\n"
            f"- Today: {plan['todays_schedule'] or '(blank)'}\n"
            f"- Accountant mode: {'ON' if plan['wants_accountant'] else 'OFF'}"
        )
        await ctx.send(msg)

    @bot.command(name="metiche_accountant")
    async def metiche_accountant(ctx: commands.Context):
        metiche = get_metiche()
        if metiche is None:
            await ctx.send("Metiche isn’t initialized yet.")
            return

        def check(m):
            return m.author == ctx.author and m.channel == ctx.channel

        await ctx.send("Do you need task accounting? (yes/no)")
        msg = await bot.wait_for("message", check=check)

        if msg.content.lower().startswith("y"):
            metiche.turn_on(ctx.channel.id)
            await ctx.send("Task accounting ON. I’ll check in every 2 hours.")
        else:
            await ctx.send("Okay. No task accounting today.")

    @bot.command(name="metiche_quiet")
    async def metiche_quiet(ctx):
        metiche = get_metiche()
        if metiche:
            metiche.turn_off()
            await ctx.send("Fine. I’ll stop watching you.")

    @bot.command(name="metiche_on")
    async def metiche_on(ctx: commands.Context):
        metiche = get_metiche()
        if metiche is None:
            await ctx.send("Metiche isn’t initialized yet. Restart the bot.")
            return
        metiche.turn_on(ctx.channel.id)
        await ctx.send("Okay. I’m on. I’ll check in every 2 hours. Don’t ghost me.")

    @bot.command(name="metiche_off")
    async def metiche_off(ctx: commands.Context):
        metiche = get_metiche()
        if metiche is None:
            await ctx.send("Metiche isn’t initialized yet.")
            return
        metiche.turn_off()
        await ctx.send("Fine. Accountant mode OFF.")