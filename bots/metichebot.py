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

                prompt = ping.get("prompt") or "¿Qué onda? What changed since the last time marker?"
                await channel.send(prompt)

                advance_ping_schedule(
                    ping["id"],
                    int(ping.get("interval_minutes") or 120),
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

        week = week_of_monday(local_now())
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
        calendar_json.setdefault(session.person, {})
        calendar_json[session.person][session.date_iso] = normalize_daily_items(session.daily_tasks)
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

        await ctx.send("Who’s schedule are we working on?\n(Heaven / Daniel / Handley Man)")
        person_raw = (await bot.wait_for("message", check=check)).content.strip()
        person = next((p for p in VALID_PEOPLE if p.lower() == person_raw.lower()), None)
        if not person:
            await ctx.send("I need one of: Heaven / Daniel / Handley Man")
            return

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
        person = "Heaven"
        
        today_tasks = [{"text": item, "done": False, "source": "mbraindump"} for item in buckets["today"]]
        
        existing_today = load_daily_tasks(person, date_key)
        merged_today = normalize_daily_items(existing_today) + today_tasks
        
        calendar_json.setdefault(person, {})
        calendar_json[person][date_key] = merged_today
        
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
    
        person = "Heaven"
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
            active_task=active_focus,
            daily_tasks=normalize_daily_items(existing_today),
        )
    
        active_time_sessions[ctx.channel.id] = session
        calendar_json.setdefault(person, {})
        calendar_json[person][date_key] = session.daily_tasks
    
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
            person = "Heaven"
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

    @bot.listen("on_message")
    async def metiche_time_listener(message: discord.Message):
        if message.author.bot or message.content.startswith("!"):
            return

        metiche = get_metiche()
        if metiche is None:
            return

        ctx = await bot.get_context(message)
        session = active_time_sessions.get(message.channel.id)

        if session and session.setup_complete:
            handled = await handle_active_day_command(ctx, message.content)
            if handled:
                return
            await log_raw_time_block(ctx, message.content, source="active_day")
            return
