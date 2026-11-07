# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Cholobots is a single Discord bot (discord.py, `!` prefix) that hosts six personas for a small contracting business (Handley Man): Metichebot (planning, daily tasks, time tracking, pings), Chismebot (CRM / leads), Guardabot (inventory), Crudobot (job costing, closeout, receipts), Obi Juan (subcontractor "quests"), Vueltabot (marketplace / delivery gigs). Data lives in Supabase. A sibling repo, `../Cholobots-Mobile`, is a set of static HTML dashboards reading the same Supabase project with a publishable key.

Work is tracked in Taiga project `cholo-bots` (id 39) on https://backlog.growthgarage.org. The roadmap is `Cholobots_48_Hour_Development_Plan.md` at the repo root.

## Running

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt      # unpinned: discord.py, python-dotenv, openai, supabase
python cholobots.py                  # same as the Procfile worker
```

`config.py` runs `load_dotenv(override=True)`, so `.env` wins over the real environment. Required or the process dies at import: `DISCORD_TOKEN`, `OPENAI_API_KEY`, `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY` (chismebot, guardabot and crudobot call `create_client` unguarded at module level; vueltabot does the same but is imported inside `on_ready`, so its failure surfaces there instead, see below; obijuan, metichebot and `db/database.py` degrade to a `None` client). `db/database.py` and vueltabot also accept `SUPABASE_KEY` as a fallback name; the other modules do not. Optional: `GUARDABOT_DB` (SQLite path, default `guardabot.db`), `DATA_SERVICE_URL` (dashboard push; when unset every metichebot time block replies "Saved, but dashboard push failed", crudobot's read of it is silent and unused), `TRANSPORTATION_CHANNEL_ID` (parsed with `int()`, default `0`), `DANIEL_MORNING_AUDIO_URL` (YouTube default), `CRUDOBOT_RECEIPT_MODEL` (default `gpt-5-mini`; chismebot's summary model is hardcoded to the same).

`cholobots.py` calls `bot.run()` at module level with no `__main__` guard: importing it starts the bot. For static checks use `python -m py_compile cholobots.py config.py db/database.py bots/*.py`.

There are no tests, linters or CI for the bot itself yet (Taiga stories #33, #34, #35). The Supabase schema is not in the repo; column names must be inferred from the queries in each bot module.

## Agent pipeline

`.claude/` holds the Claude Code pipeline ported from the you-thon repo (Taiga story #70): skills (`/analyze-story`, `/fully-implement-story`, `/fix-single-bug`, `/triage-bugs`, `/overseer`, ...), agents, shared includes and `.claude/autonomous-pipeline.md`, which is the overview. Everything Taiga-specific (project 39's statuses and ids, the pipeline tags, the MCP tool mapping, the branch and PR conventions) is in `.claude/skills/shared/taiga-project.md`; update that file first when the project configuration changes.

Prerequisites: `python3 scripts/taiga_project_setup.py --check` must exit 0. If it exits 2, Jon runs it with `--apply` (see `scripts/README.md`); the agent account cannot change project settings. The helper scripts in `scripts/` read `TAIGA_API_URL`, `TAIGA_USERNAME`, `TAIGA_PASSWORD` from the shell (the git-ignored `.envrc` exports them). Every `gh` call is `env -u GH_TOKEN gh ... --repo MrsBohemian/Cholobots`; pipeline branches are pushed to the `upstream` remote and PRs open there. Script tests: `pip install -r requirements-dev.txt && python -m pytest tests/scripts -q`.

## Architecture

**Registration happens three different ways** (`cholobots.py`): `register_chisme/metiche/guard/crudo(bot)` run once at import and define commands as closures; `setup_obijuan(bot)` adds a Cog inside `on_ready`; vueltabot is loaded with `bot.load_extension("bots.vueltabot")` inside `on_ready`. The two `on_ready` loads are guarded by `bot.obijuan_loaded`/`bot.vuelta_loaded` because `on_ready` fires on reconnect. `on_ready` order is: SQLite init, Obi Juan, vueltabot, then start Metichebot's 30-second scheduler (`MeticheManager.start_loop`, guarded by a `loop_task.done()` check). Any exception in the vueltabot load (missing env, bad `TRANSPORTATION_CHANNEL_ID`) ends `on_ready` early: the process stays up with no ping/wakeup scheduler, and retries on every reconnect.

**Every bot module creates its own Supabase client** at import with the service-role key, so Row Level Security is bypassed everywhere and nothing is scoped by guild, company or user. There is no tenant column on any table. `db/database.py` is a legacy SQLite layer: only `insert_metiche_checkin` still writes to it; `insert_metiche_weekly`/`fetch_latest_metiche_weekly` live in that file but actually use Supabase; the `guard_events`, `metiche_logs` and `crudo_reports` helpers have no callers. Crudobot's historic reports (`!crudojc`, `!crudoestimate`, `!crudoreport`) read/write local JSON files in the working directory.

**Shared tables are the cross-bot contract.** Metichebot reads `crudo_projects`/`crudo_project_tasks` and inserts `crudo_labor_entries` (one row per time block with duration > 0 when a `!mtoday` session is linked to a Crudo project; insert failures are only printed); Crudobot sums those entries per task in `!crudotime`/`!crudoclose`. Crudobot and Chismebot read and write `chisme_contacts`; Metichebot reads it. Obi Juan's tables (`obijuan_*`, `wtp_*`) and timecards are not linked to any of the above, and Obi Juan silently falls back to in-memory dicts when its Supabase client is `None`. The mobile dashboards read ~15 of these tables plus `chisme_dashboard_contacts`, which nothing in this repo references.

**Conversation state is in-memory.** Multi-step flows keep dicts keyed by `author.id` only (chismebot's six `*_sessions` dicts, crudobot's receipt sessions, metichebot's `mdice_waiting` closure local) or by channel (metichebot's `active_time_sessions`). None of them expire; a session lives until it completes, is cancelled, or the process restarts, while Supabase-backed ping schedules keep firing. `bot.wait_for` prompts have no timeout except in `!mengine` and vueltabot's `ask_user`, so an abandoned prompt hangs forever (≈18 sites in metichebot, crudobot's `ask()` helper).

**Several `on_message` listeners run for every message**, including ones the attachment gate blocks (the gate only skips `process_commands`): metichebot's time listener (once `session.setup_complete`, any non-`!` text from *any* user in that channel becomes a time block under the session's person; the `mdice_waiting` check runs before the `!` filter, so the user's next message anywhere, even a command, becomes the mdice reason), chismebot's session listener (also answers bare `cancel`/`stop`/`nevermind`/`!cancel`/`!stop` from anyone, so `!cancel` gets two replies), and crudobot's receipt listener (`save`/`yes`/`1`/`confirm` or `cancel`/`no`/`discard`/`stop` from that user in any channel). A new interactive flow has to assume its replies may be consumed by one of these.

**Attachment gate.** `cholobots.py:on_message` drops any message with attachments unless its first token is in a hardcoded whitelist, and rejects everything else, including replies to prompts, with a canned message. Any new command that takes an attachment must be added to that whitelist.

**Identity.** `bots/metichebot.py` maps four hardcoded Discord user IDs to person names (`DISCORD_USER_TO_PERSON`); anyone else becomes `"UNKNOWN:<id>"`, which `load_daily_tasks`/`replace_daily_tasks` then map to the `heaven` calendar key, so an unknown user reads and (via delete-then-insert) overwrites Heaven's `daily_tasks`. Timezone is hardcoded to `America/Chicago` (`LOCAL_TZ` in metichebot, `ZoneInfo` in obijuan and chismebot). Each module has its own `now_iso()`: `db.database`, guardabot, crudobot and chismebot are naive local time, obijuan is UTC-aware, so Supabase timestamps are mixed.

**Errors.** There is no `on_command_error`. The try/except around `process_commands` in `cholobots.py` does not catch exceptions raised inside commands, so a failing Supabase call ends the flow silently for the user and prints to stderr.

**Blocking I/O.** The Supabase client is the synchronous one and every `.execute()` runs on the event loop, as do `urllib` calls to `DATA_SERVICE_URL`, SQLite writes, and chismebot's `client.responses.create` summary call. Only the receipt OpenAI call is offloaded with `asyncio.to_thread`. Follow that pattern for anything slow you add.

## Conventions

- Command names are mostly prefixed per bot (`m*`, `c*`, `g*`, `crudo*`, `o*`), but plenty of unprefixed names exist: Obi Juan (`note`, `update`, `accept`, `materials`, `labor`, `payout`, `paid`, `questcreate`...), Vueltabot (`tengo`, `busco`, `vuelta`, `claim`, `tripclaim`), Chismebot (`lead`, `leads`, `chisme`, `hotlist`, `cancel`, aliases `stove`/`oven`/`stovetop`), and `report`/`close` as subcommands of the `!crudo` group. Check for collisions before adding a command.
- Persona voice is Spanglish and in-character; help text is hand-written per bot and is already out of sync with real command names in several places. When you add or rename a command, update that bot's `!<name>` help and the `!queso` text in `cholobots.py`.
- Customer/project lookups are `ilike %text%` substring matches with small limits; a new lookup should follow the existing helper in that module rather than add another variant.
