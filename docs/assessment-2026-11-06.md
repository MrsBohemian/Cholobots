# Cholobots assessment — 2026-11-06

Static review of `Cholobots` (main @ f4d4859) and `Cholobots-Mobile` (main @ 0b4002d) against `Cholobots_48_Hour_Development_Plan.md`, plus the state of Taiga project `cholo-bots` (#39). Nothing was run against Discord, Supabase or OpenAI; every finding cites code. Line numbers refer to the current `main`.

> `Cholobots-Mobile` now lives in `mobile/`: unprefixed `*.html:N` citations below mean `mobile/*.html`, and `0b4002d` is reachable in this repo.

## Verdict

The code does what the `!queso` text promises only for the happy path of a single business with four known people. Three structural facts drive nearly every defect below:

1. **One global data pool, service-role key everywhere, no tenant key on any table.** Multi-company isolation (plan objective 4, acceptance test 6) is not a feature gap; it needs a schema and query change in every bot.
2. **Conversation state is in-memory and listeners overlap.** Flows are lost on redeploy, and three `on_message` listeners compete for every reply. This is the root of the one existing Taiga issue (#1 "Fix Overlapping Discord Pings").
3. **No error surface, no tests, no CI.** Command exceptions go to stderr only (no `on_command_error`); there is no automated way to know whether a flow works.

Objective 1 (audit) can be mostly closed from this document plus a short live test session. Objectives 2–5 are greenfield.

## Repo state

- 222 commits, one author (two name spellings), 2026-03-11 → 2026-09-20 on `main`. No tags, no README, no tests/lint/CI, unpinned `requirements.txt`, no Python version pin (local is 3.14; deps are not installed system-wide).
- **Receipt routing fix merged 2026-11-06** (PR #1, `8f321ea`): the attachment gate now matches the first token against `{"!greceipt", "!crudoreceipt", "!tengo"}` (`cholobots.py:80-83`), so `!crudoreceipt` is reachable. `!greceipt` is still listed but no such command exists; drop it when `!queso` goes.
- No secrets in history; `.env`/`*.db` never committed. The mobile repo ships a Supabase publishable key in six HTML files (expected for that key type; safety depends on RLS, which is in neither repo).
- `db/database.py` SQLite layer is mostly dead (see CLAUDE.md). On a Procfile worker without a volume, `guardabot.db` is wiped on deploy, so `metiche_checkins` (the only live SQLite writer) is not durable anyway.

## Findings by plan objective

### 1. Audit current command flows (12h)

**Task time capture (`!mtoday` → records).** `bots/metichebot.py`
- Durations are persisted only when the session is linked to a Crudo project: `log_raw_time_block` inserts `crudo_labor_entries` (`:2174-2187`). Otherwise time exists only in `session.blocks`/`focus_minutes` (memory) and a blocking `urllib` POST to `DATA_SERVICE_URL/tasks` (`:2200`, `post_json` `:1792-1808`) whose failure is reported to the user but never retried; with the URL unset every time block replies "Saved, but dashboard push failed".
- `!mstopday` (`:3718-3727`) removes the session without logging the final block; it prints totals to Discord but persists no day summary, so the last segment is always lost.
- Drift blocks bypass `log_raw_time_block` (`:2344-2357`, `:2786-2799`) and never produce labor entries even on linked projects.
- SQLite `metiche_checkins` gets activity text but no minutes (`:2190-2198`; table has no duration column).
- `!mdone` (`:3862`) is a weaker duplicate of the in-session `done` verb: no focus-timer close, no Crudo task completion.
- Crudobot's time review ignores labor entries with no `task_id` (`crudobot.py:568`), so project-level time never reaches closeout.

**`!mtoday` ↔ project lists ↔ Daniel's view.**
- `daily_tasks` is the source of truth; `source`/`type`/`priority` are dropped on save (`:429-433`). `metiche_project_tasks` (`!mtasks`) is a separate, never-updated list (`actual_minutes`/`completed` have no writer).
- Any Discord user not in `DISCORD_USER_TO_PERSON` (`:120-125`) reads and overwrites **Heaven's** tasks via `PERSON_TO_CALENDAR_KEY.get(person, "heaven")` (`:420`, `:439`). This also means a first-time external user silently edits Handley Man data.
- The weekly plan is one shared row per week (`db/database.py:261-268`): calendar, financial objective and goals are global, and concurrent `!mschedule`/`!mgoals` overwrite each other (read-modify-write of the whole `calendar_json`).
- Mobile `time.html:122` uses the UTC date while `loadbar.html` uses local; in the evening they disagree on "today".

**Pings / `!mwakeup`.**
- Pings never @mention anyone and go to the stored channel; `user_id` is saved but unused (`:1069`). Any user's activity in the channel suppresses them (`:3935-3937`); `!mquiet` stops every person's schedule in the channel; `sync_ping_focus` re-enables paused schedules.
- Replies to pings after a redeploy are silently ignored because the in-memory session is gone while Supabase schedules keep firing (`:3948`). This is the mechanism behind Taiga #1.
- `!mwakeup` is hardwired to Daniel (`:3146`, `:3175`, `:3185`, message header `:1476`, default routine `:1464-1468`, env var name).
- `mark_wakeup_sent` uses naive `datetime.now()` (`:544`) while scheduling uses aware Chicago time.

**`!mbraindump`.** No AI; user sorts with `T:/W:/H:/I:` lines (`:2075`). `I` items are saved (`:3409`), `T` items are appended to `daily_tasks` without de-dup (`:3418`), `W` and `H` items are only counted in the summary (`:3444-3445`) and never stored; "launch session" prints a placeholder (`:3461`).

**Leads / customer notes (`bots/chismebot.py`).**
- `!lead` keeps notes in memory until `done` (`:1487-1498`); a redeploy loses a half-captured lead.
- Linking a lead to a contact never copies notes into `chisme_notes` or updates `chisme_summary`/`last_contact_date`; lead text lives only in `chisme_leads.raw_notes` / `chisme_opportunities.scope_notes`. Menu option 1 (`:1260-1262`, handler `:1306-1325`) closes the lead without touching the contact. Promotion can create an opportunity with `contact_id=None` (`:685`).
- The only OpenAI use is `synthesize_summary` (`:265`, gpt-5-mini, `max_output_tokens=500` including reasoning), called on every note *and* every system event; empty output falls back silently (`:293-295`) and the fallback truncates at 1800 chars, dropping new notes once the summary is full.
- `!chismebot` help omits `!lead`/`!leads`/`!cancel`; `!stove` (alias of `cactive`, puts a customer in the Oven) reads like Stovetop.
- Phone stored as raw string by `!cset` (`:428`) but digits-only elsewhere, so lookups diverge and duplicate cards appear. Viewing `!hotlist Name` raises any ≤50° contact to 51° (`:1110-1119`).

**Project closeout (`bots/crudobot.py`).**
- Re-running `!crudoclose` / `!crudoproject` option 4 on a project at `payment_pending`/`paid` resets `closeout_status` to `ready` (`mark_project_ready_for_closeout`). The `status != "completed"` guard at `:1600` never fires because `complete_crudo_project` (`:498`) has no callers; option 4 (`:796`) has no guard at all.
- Financial engine computes net materials and completed-task minutes only (`:825-871`); no labor dollars, profit or margin anywhere; `contract_amount` is read (`:1008`, `:1051`) but never written. `money()` returns 0.0 on garbage so a typo records a $0 invoice/payment silently (`:1057`, `:1063`). `amount_collected` is overwritten, not accumulated.
- Receipt SAVE/CANCEL listener (`:1666-1698`) is keyed by user only, no TTL: a later `1` or `yes` to a `!crudoproject` menu silently saves a pending receipt. `sha256` is computed but never stored (`:1660`); dedupe works only when `receipt_number` is present (`:333-345`). Transaction + items inserts are not atomic (`:309-329`). Uploaded PDFs are never deleted from OpenAI files.
- Obi Juan timecards/payouts and `crudo_*` are disjoint; quests never feed job cost.

**`!queso` removal** is the command block at `cholobots.py:161-232` plus the "Type `!queso`" line in `!cholobots` (`:157`); `!cholobots` names Obi Juan only as a teaser and omits Vueltabot entirely.

**Startup fragility.** `on_ready` runs SQLite init → Obi Juan → `load_extension("bots.vueltabot")` → start Metiche scheduler (`cholobots.py:43-65`). Any exception in the vueltabot load (missing Supabase env, non-numeric `TRANSPORTATION_CHANNEL_ID`) ends `on_ready` before the scheduler starts: the bot stays online, answers commands, but never sends pings or wakeups, and retries the failing load on every reconnect.

### 2. Friendlier / natural-language interactions (10h)

Nothing exists yet. Constraints any listener approach must respect: three listeners already consume free text (metiche time listener for any non-`!` text in a session channel; chisme session listener incl. bare `cancel`/`stop`; crudo receipt confirm). Adding a fourth without a single dispatcher will make double-handling worse. The cheapest credible prototype is task initiation: route free text to `!mtoday` focus selection or `!lead`, with a confirmation step, inside one new listener that checks (and yields to) existing session dicts.

### 3. Wearable MVP (6h)

Nothing exists. Pings currently go to a channel without a mention; a wearable path needs a per-person destination (DM or push). The ping schedule already has `user_id` on the row; a DM-based notification is the smallest change.

### 4. Multi-company readiness + onboarding (12h)

- **Isolation:** no `guild_id`/`org_id` on any table, every query global, service-role key in every bot module and in `db/database.py`. The mobile app writes to `chisme_dashboard_contacts` and deletes from `chisme_active` from the browser with the publishable key (`customers.html:1078-1116`, `:1343-1345`); RLS policies are not in either repo and unverified. Acceptance test 6 ("test-company information cannot be viewed by another company") is currently failing by construction.
- **Identity:** hardcoded four-person map; unknown → Heaven. Hardcoded "Daniel"/"Heaven"/"Handley Man"/Chicago/210 area code/Circular SA throughout.
- **Onboarding:** no website, no guild-join handler, no profile table.
- Sessions keyed by `author.id` without guild means a user in two servers shares one flow.

### 5. Quarterly quest experience (8h)

Obi Juan has a `Quest` model with notes/updates/assignments/payouts but no state machine (`!setquest <id> status <anything>`), no KPIs, no per-person aggregates; `created_at`/`updated_at` default to bot start time (`obijuan.py:166-167`); `get_risk_factors` always returns `[]`; eight of the sixteen commands in `!obijuan` help (`oquestcreate`, `oquestinfo`, `osetquest`, `ositevisit`, `onote`, `omaterials`, `oupdate`, `opaid`) are not registered — the real names lack the `o` prefix.

## Cross-cutting risks

| Risk | Where | Effect |
|---|---|---|
| Sync Supabase/urllib/SQLite on the event loop | every bot; `metichebot` 30s loop | one slow call stalls all six bots; LLM summary call blocks per note |
| No `on_command_error` | repo-wide | DB errors invisible to users |
| `bot.wait_for` without timeout | metichebot (≈17 sites), crudobot `ask()` | abandoned prompts leak and eat later messages |
| Non-atomic multi-step writes | `replace_daily_tasks` delete-then-insert; `gmove`; receipt save; closeout | partial failure loses data |
| Naive vs aware timestamps | `now_iso()`, `mark_wakeup_sent` | wrong-day records on UTC hosts |
| Unescaped `ilike` input | crudobot `:356`, obijuan `:341`, chismebot `find_contacts` | `%`/`_` act as wildcards |
| Expiring Discord CDN URLs stored as `photo_url` | vueltabot `:110-123` | marketplace photos rot |
| `!claim` select-then-update race | vueltabot `:665-964` | double claims (`!tripclaim` does it right) |

## Taiga state

Project #39 has one issue (#1 "Fix Overlapping Discord Pings", New, unassigned) and no stories, epics, sprints or wiki. The `slave1-codex` account got a 403 on project health, so it may lack some permissions. None of the five plan objectives is represented.

### Backlog filed (2026-11-06)

- **Issues #4–#20** — one per immediate bug above (Heaven fallback, time loss, closeout regression, receipt confirm hijack, $0 money input, chisme cancel, lead notes, AI summary, `!claim` race, CDN photo URLs, startup fragility, Obi Juan help/timestamps, timezones, phone dedupe, braindump W/H, mobile dashboards, help text). **#1** updated with the ping root cause.
- **Epic #2 (this assessment)** — design stories #21–#30 (error handler, blocking I/O, single dispatcher, session persistence, atomic writes, one Supabase client, one clock, durable time records, financial engine, lookup helper) and testing stories #34 (unit tests), #36 (manual scripts).
- **Epic #46 Developer experience** — #47 Supabase CLI local stack, #48 seed fixtures, #49 migration workflow, #50 bootstrap/task runner, #51 integration harness, #52 docs, plus #31 test Discord/env, #32 baseline migration, #33 pinning/lint, #35 CI. Foundational for everything else.
- **Epic #3 Multi-business isolation** — #37 tenant model decision, #38 migration + backfill, #39 thread the filter, #40 profiles replace the person map, #41 per-business config, #42 off service-role key, #43 mobile RLS, #44 tenant-scoped sessions, #45 fixture second company + isolation test.
- **Objective epics, not yet broken into stories** — #59 natural-language entry point (objective 2), #60 wearable notification MVP (3), #61 website→Discord onboarding (rest of 4), #62 quarterly guild quest (5). Each carries the plan text, what exists today, dependencies and candidate stories.
- Additional issues filed after the first pass: #53–#57; story #58 (repository access and contribution agreement) under epic #46.
- Per-bot deep dives are in the Taiga wiki (`home`, `deep-dive-*`, `repo-hygiene-2026-11-06`).

## Recommended order for Nov 13

1. ~~Merge `fix/crudobot-receipt-routing`; delete the stale `!greceipt` entry; remove `!queso`~~ (done on branch `claude-init-assessment`).
2. Devex first: #47 local Supabase stack and #32 baseline migration, so the rest can be tested without touching production.
3. #21 `on_command_error` and #22 `run_db()` helper, applied to metichebot's loop and the chismebot summary call first.
4. The two data-integrity bugs with external-user impact: #4 Heaven fallback and #6 closeout regression, each with an integration test (#51).
5. #37 tenant-model decision before any multi-business code.
