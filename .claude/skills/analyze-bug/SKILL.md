---
name: analyze-bug
description: Analyze a Taiga bug and produce a detailed implementation plan for /fix-bugs
argument-hint: "<issue-ref> or 'new'"
allowed-tools:
  - Bash(scripts/taiga-story.sh *)
  - mcp__taiga-py__get_issue
  - mcp__taiga-py__get_story
  - mcp__taiga-py__search
  - mcp__taiga-py__create_issue
  - mcp__taiga-py__update_issue
  - mcp__taiga-py__add_comment
  - Bash(env -u GH_TOKEN gh pr list *)
  - Bash(git diff *)
  - Bash(git log *)
  - Bash(git status *)
  - Bash(git show *)
  - Bash(git blame *)
  - Read
  - Grep
  - Glob
  - WebFetch(https://github.com/*)
  - WebFetch(https://raw.githubusercontent.com/*)
  - WebFetch(https://discordpy.readthedocs.io/*)
  - WebFetch(https://supabase.com/*)
  - WebFetch(https://docs.python.org/*)
---

# Analyze Bug $ARGUMENTS

Investigate a bug and produce an implementation plan detailed enough that `/fix-bugs` can skip investigation and go straight to TDD (or scripted manual verification).

**THIS SKILL IS PLANNING ONLY.** You MUST NOT write code, edit source files, create branches, run the bot, or make any implementation changes. Your only outputs are: Taiga updates (issue description, tags, comments) and a summary to the user. When you finish Step 7, STOP.

Taiga project, statuses and tags: [`../shared/taiga-project.md`](../shared/taiga-project.md). Every MCP call passes `project=39`. Codebase background: the root `CLAUDE.md` and `docs/assessment-2026-11-06.md`.

**Eligibility.** A bug is analysis-eligible when it is open, tagged `triaged` (see `docs/bug-triage.md`), and lacks `llm-analyzed`. The status stays `New` through analysis; it changes only when a fix starts.

**Unattended runs** (`/fully-analyze-bugs`, the overseer): wherever a step says "ask the user", instead leave the status and tags unchanged, post the question with `add_comment(project=39, ref=<ref>, entity_type="issue", text="Analysis question: <question>")`, and end with the single line `NEEDS_CLARIFICATION: <question>`.

## Step 1: Get or create the bug in Taiga

### If the argument is a ref number (existing bug)

1. Fetch the issue: `get_issue(project=39, ref=<issue-ref>)`. If you need the internal id for a REST script, `scripts/taiga-story.sh <ref> 39 --issue` returns it.
2. If the issue is closed (`Closed` or `Rejected`), inform the user and stop.
3. Proceed to Step 2.

### If the argument is "new" (or no ref number provided)

1. Ask the user: "What's the bug? Describe what's happening and what you expected instead."
2. File it following `.claude/skills/shared/bug-filing.md` (duplicate check, severity and priority per `docs/bug-triage.md`, surface tag plus per-bot tag, source tag `src:jon`, description template built from the user's answer).
3. Report the created issue ref to the user, then proceed to Step 2.

## Step 2: Evaluate the bug description

You need at minimum:

- **Expected behaviour**: what should the bot (or dashboard) do?
- **Actual behaviour**: what does it do instead?
- **How to trigger it**: the command and messages sent, in which channel, by which person, or the dashboard page and data state

If any of these are missing or ambiguous, **stop and ask the user**. Useful questions:

- "The bug says [X] but doesn't describe what should happen instead. What's the expected reply?"
- "Which person sent the messages? An ID outside the four known people behaves differently (falls back to Heaven)."
- "Did this happen right after a redeploy? In-memory sessions are lost on restart while Supabase ping schedules keep firing."
- "Is this a regression (it used to work) or has it never worked?"

Do NOT guess at missing details.

## Step 3: Investigate the codebase

Gather the root cause and all the context the fixer will need. The fixer should only need to read the files they're modifying.

### 3a. Reproduce understanding

Identify the expected and actual behaviour and where the bug likely lives: `cholobots.py` (startup, attachment gate, `on_message`), a persona module in `bots/`, `db/database.py`, `config.py`, or a dashboard page in `../Cholobots-Mobile`.

Reproduction is by reading code, or by sending the messages in the test Discord server (story #31) when it exists and the bug needs a live bot. Never run `python cholobots.py` against production from this skill. When you reproduce live, record the exact messages and replies in the plan.

### 3b. Trace the code path

1. Find the entry point: the command function (grep its `name=` / `aliases=`), the `on_message` listener, the scheduler loop (`MeticheManager.start_loop`), or the dashboard script.
2. Follow the call chain to where behaviour diverges, including every `on_message` listener that sees the same message (see the hazard checklist below).
3. Read all files on the path, not just the one with the bug.

### 3c. Identify the root cause

State a specific, verified hypothesis that cites `file:line`, e.g.:

- "`bots/crudobot.py:1600` guards on `status != "completed"`, but nothing ever sets that status (`complete_crudo_project` at `:498` has no callers), so the guard never fires."
- "The receipt listener at `bots/crudobot.py:1666` matches `1` from the same user in any channel, so it consumes a `!crudoproject` menu reply."

### 3d. Check whether the bug is already fixed or invalid

Verify the bug still exists on `upstream/main` (`git fetch upstream` first if your checkout is stale; read with `git show upstream/main:<path>`):

- **Already fixed**: `git log --oneline upstream/main --grep="<keyword>"`, or search recent commits. If the code already behaves correctly, the bug is resolved.
- **Invalid**: the description is factually incorrect, or the described behaviour is correct.

If either applies, skip to **Step 7b**.

### 3e. Apply the Cholobots hazard checklist

Go through the ten items in `.claude/skills/analyze-story/SKILL.md` Step 5e (listeners, attachment gate, in-memory sessions, blocking I/O, identity and time, tables and columns, help text, command-name collisions, errors, cross-bot contracts). Each one is either part of the root cause, a constraint on the fix, or "n/a". A fix that adds a prompt, a listener branch or a Supabase call must not introduce a new instance of these hazards.

### 3f. Gather implementation context

For each file that changes, note the exact function signatures, the surrounding patterns, the Supabase table/column names (cite the query each was inferred from; the schema is not in the repo), and any related tests. Capture the actual code of any pattern the plan references.

### 3g. Identify prerequisites

A prerequisite is another bug or story that must **merge to `main` upstream before this fix can land or pass**: the fix depends on code it introduces (e.g. the `run_db()` helper from #22, the dispatcher from #23, the shared clock from #27), a new test would fail until it merges, or both touch the same code and this plan assumes the other's version.

- Look at refs named in the bug description, then search for related open items touching the same files: `search(project=39, query="<file path or symbol>")`, one call per file or symbol. Fetch each candidate with `get_issue(project=39, ref=N)` or `get_story(project=39, ref=N)`.
- Record whether each candidate is **merged**: a merged PR exists upstream, `env -u GH_TOKEN gh pr list --repo MrsBohemian/Cholobots --search "<ref>" --state merged --json number,title,baseRefName`. Confirm the hit is this item's fix and that its base was `main` (or a branch since merged to `main`). Taiga status is not the test.
- "Related" or "same area" is not a prerequisite. Only a hard ordering constraint counts. A merged prerequisite no longer constrains anything; leave it out or mark it "(merged)".

## Step 4: Determine which tests are needed

Read `docs/testing-strategy.md`, `docs/testing-conventions.md` and `docs/testing-workflow.md`. TDD applies: plan a failing test first whenever the code under change is testable.

1. **Unit test (pytest)** in `tests/` when the bug lives in or can be moved into pure logic: parsing, `money()`, time and date arithmetic, phone normalisation, lookup escaping, state transitions. Never import `cholobots.py`; bot modules need the four required env vars and three create a Supabase client at import, so follow the import approach story #34 established, or plan to extract the helper into a side-effect-free module, or a conftest with dummy env and a patched `create_client`. Time tests use a fixed aware `America/Chicago` datetime.
2. **Integration test against local Supabase** for query, multi-step write and cross-bot table bugs, once stories #47 and #51 exist. Until then, say "deferred until #47/#51" and cover the behaviour with manual steps.
3. **Manual test steps** for anything that needs a live bot (listener interplay, prompts, discord.py parsing) or a browser (dashboards): numbered steps for the test Discord server (story #31) with the exact messages and expected replies, or the dashboard page and expected display. The fixer records these on the issue when no automated test is possible.

## Step 5: Identify documentation updates

- **README.md**: does the fix change setup or required env vars?
- **CLAUDE.md**: does it change env vars, listener or registration behaviour, a shared table contract, or a statement the file makes about current behaviour (it describes several of these bugs)?
- **In-bot help**: does the fix rename or change a command its `!<persona>` help describes?

If changes are needed, include them in the plan's Changes section; otherwise say so explicitly.

## Step 6: Write the implementation plan

Every section is required. Prerequisites comes first; never put a sequencing constraint anywhere else.

```markdown
## Implementation Plan

### Prerequisites

[Either "None." or one bullet per item that must merge first:]
- #<ref> (<bug|story>, <open|merged>): <why this fix cannot land before it>

### Reproduction

[Exact steps: who sends what, where, in what state; or the code path read. Expected vs actual reply.]

### Root Cause

[1-3 sentences, citing `file:line`. Name the function and what's wrong.]

### Approach

[1-2 sentences. Name the approach and the existing pattern it follows.]

### Hazards

[One line per relevant item from the hazard checklist: how the fix handles it. Omit items that are n/a.]

### What Does NOT Need to Change

[Bullets: things the fixer might be tempted to modify but shouldn't.]

**Closes on merge:** [eligible | not eligible: <post-merge validation that remains: a live check in the test or production Discord server, a dashboard check against live data, a human acceptance step>]

### Changes

#### 1. [File path: description of change]

- Exact function signatures and lines involved (`file:line`)
- Supabase tables/columns touched (with the query they were inferred from)
- Pattern to follow, with the actual code:

\```python
# Pattern from bots/<module>.py:<line>:
...
\```

#### 2. [Next file...]

### Tests

#### Unit: `tests/<file>.py` (pytest)

**Import approach:** [conftest from #34 | helper extracted | dummy env + patched `create_client`]

**Test cases to write (each fails before the fix):**
1. `test_<name>`: [what it verifies]

#### Integration

[Cases, or "Deferred until #47/#51"]

### Verification

**Automated:**
- `python -m py_compile cholobots.py config.py db/database.py bots/*.py`
- `python -m pytest -q` on the affected test files (no bot test suite yet → record "n/a until #34")
- `ruff check .` once configured (#33)

**Manual (test Discord server, #31; dashboards via Chrome DevTools):**
1. [Exact message] → [expected reply]
2. [Regression scenario: the other listener / unknown user / after restart]
```

## Step 7a: Update the Taiga issue (bug confirmed)

**Replace the description; do not append to it or preserve a copy of the original.** Taiga keeps version history. Fold any fact the plan depends on (reproduction steps, environment, symptoms) into the plan.

```
update_issue(project=39, ref=<ref>, description="<plan>")
```

### Tag the issue

1. Read the current tags from `get_issue`.
2. Apply: `update_issue(project=39, ref=<ref>, tags=[<existing tags...>, "llm-analyzed"])`. The status stays `New`.
3. Do **not** add `close-tasks-on-merge` or `mark-done-on-merge`. Bug auto-close is reserved for the unattended sweeps, which read the plan's `**Closes on merge:**` line (see [`../shared/merge-side-effects.md`](../shared/merge-side-effects.md)).

### Block on open prerequisites

The unattended fix orchestrators (`/parallel-fix-bugs`, `/fully-fix-bugs`) skip bugs in the `Blocked` status, so that status is how an open prerequisite keeps this bug out of a fix run.

1. If any prerequisite listed in the plan is still open: `update_issue(project=39, ref=<ref>, status="Blocked")`.
2. If there are no open prerequisites, leave the status unchanged.

### Leave a comment

Post a summary so it appears in the activity feed. When the bug was blocked, append ` Blocked until #<ref>[, #<ref>] merges.`:

```
add_comment(project=39, ref=<ref>, entity_type="issue", text="Analyzed: <1-2 sentence root cause with file:line>. Implementation plan added to description.")
```

Report what you found, that the plan is written, and that the issue is tagged `llm-analyzed`. The bug is ready for `/fix-bugs`, or, if blocked, say which prerequisites it waits on. Unattended runs end with `ANALYSIS_COMPLETE`.

## Step 7b: Reject the bug (already fixed or invalid)

### Update the description

**Replace the description.** The rejection opens by restating what was reported, so it reads standalone:

```markdown
## Rejection: [Already Fixed | Invalid]

### What was reported
[1-2 sentences in your own words, including any reproduction details the finding depends on]

### Finding
[1-3 sentences. Already fixed: commit hash, PR number, what it changed. Invalid: the code (`file:line`) that contradicts the report.]

### Evidence
[Quote the relevant code or commit message]
```

```
update_issue(project=39, ref=<ref>, description="<rejection summary>")
```

### Set status to Rejected and tag

```
update_issue(project=39, ref=<ref>, status="Rejected", tags=[<existing tags...>, "llm-analyzed"])
```

### Leave a comment

```
add_comment(project=39, ref=<ref>, entity_type="issue", text="Rejected: <1-2 sentence summary>. See updated description for full evidence.")
```

Report that the bug was rejected and why, with the commit/PR reference if already fixed.

## STOP — Do Not Implement

This skill ends here. Do NOT proceed to implementation, code changes, or branch creation. The user will invoke `/fix-bugs` separately.
