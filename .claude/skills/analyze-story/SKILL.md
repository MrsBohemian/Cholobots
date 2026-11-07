---
name: analyze-story
description: Analyze a Taiga user story and produce a detailed implementation plan for /implement-story
argument-hint: "<story-ref> or 'new'"
allowed-tools:
  - Bash(scripts/taiga-story.sh *)
  - Bash(scripts/taiga-epic.sh *)
  - Write(//tmp/**)
  - mcp__taiga-py__get_story
  - mcp__taiga-py__create_story
  - mcp__taiga-py__update_story
  - mcp__taiga-py__set_story_status
  - mcp__taiga-py__get_task
  - mcp__taiga-py__create_task
  - mcp__taiga-py__update_task
  - mcp__taiga-py__break_down_story
  - mcp__taiga-py__get_epic_overview
  - mcp__taiga-py__create_epic
  - mcp__taiga-py__browse_backlog
  - mcp__taiga-py__search
  - mcp__taiga-py__get_wiki
  - mcp__taiga-py__add_comment
  - Bash(git diff *)
  - Bash(git log *)
  - Bash(git status *)
  - Bash(git show *)
  - Read
  - Grep
  - Glob
  - Edit
  - WebFetch(https://github.com/*)
  - WebFetch(https://raw.githubusercontent.com/*)
  - WebFetch(https://discordpy.readthedocs.io/*)
  - WebFetch(https://supabase.com/*)
  - WebFetch(https://docs.python.org/*)
---

# Analyze Story $ARGUMENTS

Investigate a user story and produce an implementation plan detailed enough that `/implement-story` can skip investigation and go straight to TDD (or scripted manual verification) for each task.

**THIS SKILL IS PLANNING ONLY.** You MUST NOT write code, edit source files, create branches, run the bot, or make any implementation changes. Your only outputs are: Taiga updates (story description, tasks, epic links, comments), edits to the story's design doc Open Questions section (see Step 4c), and a summary to the user. When you finish Step 10, STOP.

Taiga project, statuses and tags: [`../shared/taiga-project.md`](../shared/taiga-project.md). Every MCP call passes `project=39`. Codebase background: the root `CLAUDE.md` and `docs/assessment-2026-11-06.md`; read both before Step 5.

**Eligibility.** A story is analysis-eligible when it is `Ready for Analysis` (or `New`) and lacks the `llm-analyzed` tag. A story already `Ready` or later may be re-analyzed on request (reconcile, Step 8); a closed story (`Done`, `Archived`) is not.

**Unattended runs.** When this skill runs under an orchestrator (`/fully-analyze-stories`, the overseer) there is no human to ask. Wherever a step below says "ask the user", instead post the question on the story with `add_comment(project=39, ref=<ref>, text="Analysis question: <question>", entity_type="story")`, leave the status and tags unchanged (no `llm-analyzed`), and end with the single line `NEEDS_CLARIFICATION: <question>`.

## Step 1: Get or create the story in Taiga

### If the argument is a ref number (existing story)

1. Find the story and its tasks: `scripts/taiga-story.sh <ref> 39 --with-tasks`. It returns story JSON with a `tasks` array (`id`, `ref`, `subject`, `status`, `status_extra_info`, `is_closed`). Keep the `tasks` array for Step 8.
2. If the story is closed (`Done` or `Archived`), inform the user and stop.
3. Fetch the full story: `get_story(project=39, ref=<ref>)` for the description, tags and epic.
4. Proceed to Step 2.

### If the argument is "new" (or no ref number provided)

1. Ask the user: "What feature or enhancement do you want? Describe what the user should be able to do and why."
2. Create the story: `create_story(project=39, subject=<short summary>, description=<user's description>, tags=[<per-bot tag(s)>])`, then `set_story_status(project=39, ref=<new ref>, status="Ready for Analysis")`.
3. Report the created story ref to the user, then proceed to Step 2.

## Step 2: Evaluate story completeness

Before investigating code, check whether the story gives you enough to work with. A well-defined story needs:

- **What the user can do**: which person, in which persona, with which command or dashboard
- **Why it matters**: the motivation or business need (Handley Man today; other businesses under epic #3)
- **Acceptance criteria**: specific, testable conditions that define "done"
- **Scope boundaries**: what is explicitly NOT included

If any of these are missing or ambiguous, **stop and ask the user** before investigating. Useful questions here:

- "The story says [X] but doesn't describe what success looks like. What are the acceptance criteria?"
- "Which of the four known people use this, and should it work for an unknown Discord user (who today falls back to Heaven's data)?"
- "Should this be scoped to one business, or is it fine as a global Handley Man feature until epic #3 lands?"
- "What should the bot reply if the user abandons the prompt, types `cancel`, or the Supabase call fails?"
- "Is this a modification of an existing command or a new one? Which persona owns it?"

Do NOT guess at missing details. It's faster to ask one clarifying question than to plan the wrong feature.

### Design-tagged stories

Stories tagged `design` (#21 to #30 today) ask for an architecture decision, not a feature: error handling, blocking I/O, the single dispatcher, session persistence, atomic writes, the shared Supabase client, the clock, durable time records, the financial engine, the lookup helper. For these, the plan's Overview states the decision and the alternatives considered (one line each, with the reason for rejecting), and the task breakdown implements the decision in the smallest first slice. If the decision itself needs Heaven or Jon (it changes data shape, user-visible behaviour, or touches epic #3), ask before planning. A design story that is really several slices gets `epic-candidate` (Step 10).

## Step 3: Assign to an epic

1. Read the open epics from [`../shared/taiga-project.md`](../shared/taiga-project.md) § Epics and wiki, and `get_epic_overview(project=39, ref=<epic>)` for any that look relevant (it returns the description and the linked stories). If the story already has an epic, confirm it still fits and skip to 3a.
2. Determine which epic best fits this story's scope.
3. Present your recommendation to the user: name the epic and explain why it fits. If no existing epic is a good match, suggest creating one with `create_epic(project=39, subject=..., description=..., story_refs=[<ref>])`.
4. Once the user confirms, link the story: `update_story(project=39, ref=<ref>, epic=<epic ref>)`.
5. Check the epic's dependency diagram (Step 3a).

### Step 3a: Check the epic dependency diagram

1. Fetch the epic description: `scripts/taiga-epic.sh get <epic ref>` (the `get_epic_overview` result has it too).
2. Look for a `## Dependency Diagram` or `## Dependencies` section.
3. If one exists:
   a. Read the graph (the fenced code block with `#ref` entries and `└─►` arrows).
   b. From the story's description and acceptance criteria, suggest which stories this one depends on and which depend on it. Infrastructure stories are common prerequisites: #47 local Supabase stack, #32 baseline migration, #34 unit tests, #31 test Discord server, #37 tenant-model decision for anything under epic #3.
   c. Present the suggestion: "I'd place #<new_ref> after #<dep_ref> because [reason]. Does that look right?"
   d. If the user confirms, write the full updated epic description (only the diagram's code block changed) to `/tmp/epic-<epic ref>-description.md` and apply it: `scripts/taiga-epic.sh patch <epic ref> --description-file /tmp/epic-<epic ref>-description.md`. The workflow MCP server has no epic-update tool; this script is the route.
   e. If the user declines or wants changes, adjust accordingly.
4. If no dependency section exists and the epic has more than one story, offer to create a `## Dependency Diagram` section the same way (list the stories in a fenced code block and ask the user to confirm the edges). If the user declines, skip.

## Step 4: Gather design context

### 4a. Look for a design document

1. Check `docs/designs/story-<ref>.md` (the convention from `/design-story`).
2. Also check the story description for a `## Design` section or a link to a design file.
3. If no design doc exists, proceed to Step 5 and note in the plan that none was available.

### 4b. Incorporate the design document

If a design doc exists, read it fully. Its sections map to the analysis:

| Design doc section | How to use it in the plan |
|---|---|
| **Story Intent** | Authoritative source for the plan's Overview. Don't re-derive intent. |
| **People and tenancy** | Which of the four known people (and unknown users) the flow serves, and what epic #3 changes. Each person's edge cases become test cases. |
| **Commands** | Determines which bot module and which `register_*`/Cog gets the change, and which names must be checked for collisions (Step 5e). |
| **Transcripts** | The implementer's spec for the reply sequence. Point task descriptions at a transcript ("see `docs/designs/story-<ref>.md` § Transcripts > Happy path") instead of re-describing it. |
| **Interaction Flows** | Each flow's trigger, Supabase reads/writes, cancel and timeout path become implementation detail and acceptance criteria in the relevant task. |
| **Listener ownership** | Which `on_message` listener or `wait_for` consumes each reply; carry it into the task that adds the flow. |
| **Dashboard changes** | Which `../Cholobots-Mobile` page changes and which tables/columns it reads. |
| **Design Decisions** | Constraints the implementer must follow. Copy the relevant decision (with rationale) into each task it affects. |
| **Open Questions** | If any remain unresolved, **stop and ask the user** before planning. Sync resolutions back into the doc (Step 4c). |

### 4c. Sync resolved Open Questions back into the design doc

If this analysis resolves an open question in the design doc (by an explicit user answer, or by a decision the plan settles):

1. Edit `docs/designs/story-<ref>.md` with the `Edit` tool: strike the question and record the resolution inline, e.g. `~~Embed or plain text for the summary?~~ **resolved**: plain text, matches the rest of Crudobot.`
2. If a question can only be answered during implementation, leave it unstruck and annotate it: `<question> **deferred to implementation**: <what still needs deciding and why>`.
3. Leave questions this analysis didn't touch exactly as written.

Leave the edit in the working tree; this skill does not commit. Name the file and the struck/annotated questions in the Step 10 report.

## Step 5: Investigate the codebase

Gather all the context the implementer will need. The implementer should only need to read the files they're modifying, not the files they're calling into.

### 5a. Identify the parts involved

- **Entry point** (`cholobots.py`): bot setup, `on_ready` load order, the attachment gate in `on_message`, `!cholobots` help
- **Persona module** (`bots/<persona>.py`): commands registered in `register_*` closures (chismebot, metichebot, guardabot, crudobot), the Obi Juan Cog, or the vueltabot extension
- **Supabase tables** used through each module's own client; the schema is not in the repo (story #32 adds a baseline migration)
- **Legacy storage**: `db/database.py` (SQLite, mostly dead), Crudobot's local JSON reports
- **Config** (`config.py`): env vars, the shared OpenAI client
- **Dashboards**: static HTML in `../Cholobots-Mobile` (or `mobile/` after story #64) reading Supabase with the publishable key

### 5b. Find existing patterns to follow

For each kind of change, find the closest existing pattern and read the actual code:

1. New command: an existing command in the same persona module, including how it registers and how its help text is written.
2. Multi-step prompt: the module's existing `wait_for` or session-dict flow (note whether it has a timeout).
3. Supabase read/write: the module's existing helper for that table; copy the exact column names it uses.
4. Customer/project lookup: the module's existing `ilike` helper; reuse it rather than adding a variant.
5. Slow call (OpenAI, Supabase in a loop, HTTP): the `asyncio.to_thread` call in `bots/crudobot.py` (receipt parsing, around `:1637`).

Capture the code in the plan; don't just name it.

### 5c. Map changes to files

For each acceptance criterion, start from the user-facing behaviour (the command and its replies, or the dashboard view), follow it to the data layer, and note every file and every Supabase table/column involved.

### 5d. Gather implementation context

For each file that changes, note the exact function signatures, the surrounding conventions (persona voice, reply style), any existing tests, and the imports needed.

### 5e. Cholobots hazard checklist

Answer every item in the plan (Architecture section). Each one has burned this codebase before; cite `file:line`.

1. **Listeners that will see the replies.** `on_message` listeners run for every message, including ones the attachment gate blocks: metichebot's time listener (any non-`!` text in a channel with a set-up `!mtoday` session, from any user; `mdice_waiting` consumes the user's next message anywhere), chismebot's session listener (bare `cancel`/`stop`/`nevermind`/`!cancel`/`!stop` from anyone), crudobot's receipt listener (`save`/`yes`/`1`/`confirm`, `cancel`/`no`/`discard`/`stop` from that user in any channel). Say which of them will also see this flow's replies and how the plan avoids double handling. If the story adds free-text handling, check whether story #23 (single dispatcher) has landed.
2. **Attachment gate.** A command that takes an attachment must be added to `attachment_commands` in `cholobots.py:on_message` (around `:80`); a reply to a prompt that carries an attachment is rejected by the gate.
3. **In-memory sessions.** Session dicts keyed by `author.id` (or channel) never expire and die on restart, and a user in two guilds shares one entry. Say where the new flow keeps state, how it ends (complete, `cancel`, timeout) and what happens after a redeploy. New `bot.wait_for` calls take a `timeout=` (story #24).
4. **Blocking I/O.** Supabase `.execute()`, `urllib`, SQLite and OpenAI calls block the event loop. New slow calls go through `asyncio.to_thread` (or the `run_db()` helper once story #22 lands).
5. **Identity and time.** `DISCORD_USER_TO_PERSON` (`bots/metichebot.py:120`) maps four hardcoded IDs; unknown users become `UNKNOWN:<id>` and read/overwrite Heaven's `daily_tasks`. Timezone is `America/Chicago`; each module has its own `now_iso()` (naive local vs UTC-aware). Say which identity the feature uses and which clock (story #27 for the shared clock).
6. **Tables and columns.** Name every Supabase table and column read or written, each inferred from an existing query (cite it). New tables or columns need a migration: if story #32 (baseline migration) and #49 (migration workflow) have landed, plan a migration file; otherwise the plan says the schema change is applied by hand in Supabase and records the SQL in the task. No table has a tenant column; anything under epic #3 follows the #37 decision.
7. **Help text.** A new or renamed command updates that persona's `!<persona>` help command (`!metichebot`, `!chismebot`, `!guardabot`, `!crudobot`, `!obijuan`, `!vueltabot`) and, if it is a headline feature, `!cholobots` in `cholobots.py`. Keep the Spanglish persona voice.
8. **Command-name collisions.** Grep every `name=` and `aliases=` across `cholobots.py` and `bots/*.py` for the proposed name and aliases. Unprefixed names exist in several personas (`note`, `update`, `accept`, `lead`, `cancel`, `claim`, `report`, `close`...).
9. **Errors.** There is no `on_command_error` (story #21). Say what the user sees when a Supabase call fails mid-flow.
10. **Cross-bot contracts.** If the change touches a shared table (`crudo_projects`, `crudo_project_tasks`, `crudo_labor_entries`, `chisme_contacts`) or a table the dashboards read, list the other readers and confirm they still work.

## Step 6: Determine which tests are needed

Read `docs/testing-strategy.md`, `docs/testing-conventions.md` and `docs/testing-workflow.md` first.

### Documentation-only stories

If the story produces only documentation, note in the plan:

> **Tests**: Not applicable. This story produces documentation only.

Skip the rest of Step 6.

### Code-changing stories

Plan tests at the lowest level that can prove each acceptance criterion:

1. **Unit tests (pytest)** for pure logic: parsers, money and time arithmetic, phone normalisation, lookup-input escaping, message formatting, state transitions.
   - Location `tests/` (bot tests, not `tests/scripts/`, which holds the pipeline script tests). Run with `python -m pytest -q`.
   - Never import `cholobots.py` (importing it starts the bot). Bot modules import `config`, which exits without the four required env vars, and chismebot, guardabot and crudobot call `create_client` at import. Follow the import approach story #34 established (`tests/conftest.py`); if #34 has not landed, either plan a task that moves the helper into a side-effect-free module, or plan a conftest that sets dummy env vars and patches `supabase.create_client` before import. Name which.
   - Time-dependent tests pass a fixed aware `datetime` in `America/Chicago`; no test reads the wall clock.
   - No network: patch the Supabase client and OpenAI.
2. **Integration tests against local Supabase** for queries, multi-step writes and cross-bot table contracts, once story #47 (local Supabase stack) and #51 (integration harness) exist. Until then, list the queries that would be covered here and move their verification to manual steps.
3. **Manual test steps** for anything that needs a live bot: command parsing by discord.py, listener interplay, prompts, embeds, persona replies, and dashboard rendering. Write them as numbered steps for the test Discord server (story #31) with the exact messages to send and the expected replies (and for dashboards, the page in `../Cholobots-Mobile` and what it should show). These steps go into the plan's Verification section and the relevant task, so the `senior-manual-testing-specialist` can run them.

### Step 6b: Validate the test plan against the testing strategy

Check every planned test against the rules in `docs/testing-strategy.md` (suite placement, one authoritative level per business rule, deterministic time, no network in unit tests). Correct the plan where it violates them and note the corrections and their rationale.

## Step 7: Identify documentation updates

- **README.md**: does the feature change setup, required env vars, or the persona overview?
- **CLAUDE.md**: does it add env vars, change registration or listener architecture, add a shared table contract, or change a convention? Update the relevant paragraph.
- **In-bot help**: covered by hazard 7; list it here too so it is not forgotten.
- **`docs/`**: does an existing doc cover this area, or does a new one need creating and linking from README?

If updates are needed, include a dedicated task (or fold the help-text change into the command's task). If nothing needs updating, say so explicitly.

## Step 8: Break down into tasks

### Reconcile against the story's existing tasks

The `tasks` array from Step 1 is a required input. If it is non-empty, this is a **reconcile**, not a fresh breakdown.

1. Compose the ideal breakdown for the story as it stands now.
2. Map each planned unit of work onto the existing tasks **by the work it describes, not by subject string**.
3. Treat closed tasks (`status_extra_info.is_closed` true; fall back to top-level `is_closed`) and tasks at `Ready for test` as completed work; plan around them, do not re-create them.
4. Emit a disposition table before writing anything, one row per existing task and one per planned unit:

| Existing task | Planned unit | Disposition |
|---|---|---|
| #88 Add timeout to crudo ask() | Add timeout to crudo ask() | **update**: same work, refresh description |
| — | Unit tests for money() parsing | **create**: genuinely new |
| #91 Old approach task | — | **flag**: no longer in plan, surface to user |
| #92 Already-closed task | (covered) | **leave**: closed, do not touch |

- **update**: refresh the description in place; keep the subject unless it is actively wrong.
- **create**: no existing task covers this unit.
- **leave**: fine as is, or already closed / `Ready for test`.
- **flag**: no longer belongs to the plan. Surface it; never silently drop it.

When a match is unclear, read the task with `get_task(project=39, ref=<task ref>)` and compare files-to-change. If still ambiguous, **ask the user**. Never leave the story with two tasks covering the same unit of work.

### Task shape

Each task is independently committable (passes `python -m py_compile cholobots.py config.py db/database.py bots/*.py` and the existing tests), roughly 1 to 3 files, one TDD cycle, ordered by dependency.

Common ordering:

1. **Pure helper + unit tests** (parsing, formatting, arithmetic), possibly extracted to an importable module
2. **Schema change** (migration file or recorded SQL, per hazard 6)
3. **Data access** (the module's Supabase helper, offloaded per hazard 4)
4. **Command / flow** (registration, prompts with timeouts, listener guards, attachment-gate entry)
5. **Help text** (`!<persona>`, `!cholobots`)
6. **Dashboard** (`../Cholobots-Mobile` page), if any
7. **Documentation** (README, CLAUDE.md)

Only create tasks for work that actually needs doing.

### Task description format

```
## What

[1-2 sentences describing what this task accomplishes]

## Files to Change

- `bots/<module>.py`: [what changes and why]

## Implementation Details

- Exact function signatures, Supabase table/column names (with the query they were inferred from)
- The pattern to follow, with the actual code from the codebase
- Listener, attachment-gate, timeout and blocking-I/O handling from the hazard checklist that applies to this task

## Tests

- Unit: `tests/<file>.py`: [test cases]
- Manual (test Discord server, #31): [numbered steps with exact messages and expected replies], or "none"

## Acceptance

- [Specific condition that proves this task is done]
```

A task that must stay open after the story's PR merges (gated on a live step, a human, or another epic) adds a `## Gate` section naming what it waits for. `/implement-task` skips it and reports it `GATED:`, and the story's merge-side-effect tags depend on it (see [`../shared/merge-side-effects.md`](../shared/merge-side-effects.md)).

## Step 9: Write the implementation plan

Compose the story-level plan with this template. It goes in the story description. Every section is required.

```markdown
## Implementation Plan

### Design Document

`docs/designs/story-<ref>.md` [Status: Approved/Draft/None]

[If none exists, write "No design document available for this story."]

### Overview

[1-3 sentences. What capability is added/changed and the high-level approach. For a `design`-tagged story: the decision, then one line per rejected alternative.]

### Architecture

[Which modules, commands, listeners and tables are involved and how they connect. Name the files. Then the hazard checklist from Step 5e, one line per item: answer or "n/a".]

### What Does NOT Need to Change

[Bullets: things the implementer might be tempted to modify but shouldn't.]

### Merge Side Effects

- Completes through PR(s): [yes, one PR | yes, N PRs | no: <why>]
- Tasks that stay open after merge: [none | #<task-ref>: <reason>, one per `## Gate` task]
- Post-merge validation: [none | <live check in the test or production Discord server, dashboard check against live data, human acceptance by Heaven or Jon>]
- Taiga tags set: [`close-tasks-on-merge`, `mark-done-on-merge` | `close-tasks-on-merge` only | none], per [`../shared/merge-side-effects.md`](../shared/merge-side-effects.md)

### Task Breakdown

1. [Task subject]: [1-sentence summary]
2. ...

### Tests

#### Unit: `tests/<file>.py` (pytest)

**Import approach:** [conftest from #34 | helper extracted to `<module>` | dummy env + patched `create_client`]

**Test cases to write:**
1. `test_<name>`: [what it verifies]

**Pattern to follow:**
\```python
# From tests/<existing>.py (or the minimal pytest shape if none exists yet):
def test_parses_dollar_amount():
    assert money("$1,234.50") == 1234.50
\```

#### Integration (local Supabase)

[Cases, or "Deferred until #47/#51: <queries that would be covered>"]

### Verification

**Automated:**
- `python -m py_compile cholobots.py config.py db/database.py bots/*.py`
- `python -m pytest -q` for the affected test files (no bot test suite yet → "n/a until #34")
- `ruff check .` once configured (#33)
See `docs/testing-workflow.md` for routing.

**Manual (test Discord server, #31; dashboards via Chrome DevTools):**
1. [Exact message to send] → [expected reply]
2. [Cancel / timeout / unknown-user / Supabase-failure scenario]
```

## Step 10: Update Taiga

### Update the story description

**Replace the description; do not append to it or preserve a copy of the original.** Taiga keeps version history. Carry forward any *fact* from the original the plan still depends on (capability, acceptance criteria, scope) by folding it into the plan.

```
update_story(project=39, ref=<ref>, description="<plan>")
```

### Create or update tasks

Execute the Step 8 disposition table row by row. Do **not** re-derive matches here.

- **update** → `update_task(project=39, ref=<task ref>, description="<task details>")`; add `subject=` only when the existing subject misdescribes the work.
- **create** → `create_task(project=39, story_ref=<ref>, subject=<subject>, description="<task details>", status="New")` (or `break_down_story(project=39, story_ref=<ref>, tasks=[...])` for several at once).
- **leave** → no call.
- **flag** → report it to the user with its ref and subject. The workflow server has no delete; if the user confirms it should go, they delete it in the Taiga UI (unattended: comment on the task with `add_comment(project=39, ref=<task ref>, entity_type="task", text="Flagged by analysis: no longer in the plan.")`).

Then re-fetch with `scripts/taiga-story.sh <ref> 39 --with-tasks` and confirm the task count equals **update** + **create** + **leave** + **flag** rows. If it is higher, a duplicate was created: say so explicitly in the report.

### Tag and advance the story

1. Read the current tags from `get_story` (a list of names).
2. Add `llm-analyzed`.
3. If the story still represents substantial planning rather than a clean implementation slice, also add:
   - `planning-heavy` when significant design, architecture or open questions remain
   - `epic-candidate` when it is likely to split into several stories or become an epic. You may create the epic and child stories (`create_epic`, `create_story(..., epic=<ref>)`); tag the umbrella and children with `planning-heavy` / `epic-candidate` so the structure gets reviewed.
4. Add the merge-side-effect tags the plan names, applying the deciding rule in [`../shared/merge-side-effects.md`](../shared/merge-side-effects.md):
   - `close-tasks-on-merge` iff the story completes through a single PR and no task stays open after merge.
   - `mark-done-on-merge` iff `close-tasks-on-merge` applies and no post-merge validation remains. Until the test Discord server exists (#31), a command-handler change without automated coverage has post-merge validation.
   - Several PRs: neither tag. On a re-analysis, remove either tag the current plan no longer supports.
5. Advance to `Ready` only when the story is now a concrete implementation target. If it remains primarily planning-heavy, do **not** force it forward just because analysis happened.
6. Apply:
   ```
   update_story(project=39, ref=<ref>, tags=[<existing tags...>, "llm-analyzed", ...], status="Ready")
   ```
   Omit `status` if the story is already `Ready` or later (`In progress`, `Ready for review`, `Ready for test`), or stays where it is as planning-heavy.

### Report

Report what you found, that the plan is written, the tasks with their epic, the Step 8 reconcile counts (updated, created, left, flagged) and the post-write task-count check, the tags set (`llm-analyzed`, and `planning-heavy` / `epic-candidate` when applicable), which merge-side-effect tags were set and why, and whether the story moved to `Ready`. If you created an epic and child stories, say so. If Step 3a changed an epic's dependency diagram, say which epic. If Step 4c edited the design doc, name the file and the struck vs. deferred questions. Unattended runs end with `ANALYSIS_COMPLETE` (or `NEEDS_CLARIFICATION: <question>` / `BLOCKED: <reason>`).

## STOP — Do Not Implement

This skill ends here. Do NOT proceed to implementation, code changes, or branch creation. The user will invoke `/implement-story` separately.
