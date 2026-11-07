---
name: implement-task
description: Implement the next incomplete task from a story — single task, fresh context
argument-hint: "<story-ref> <branch-name> [base-branch]"
allowed-tools:
  - Bash(scripts/taiga-story.sh *)
  - mcp__taiga-py__get_story
  - mcp__taiga-py__update_story
  - mcp__taiga-py__get_task
  - mcp__taiga-py__update_task
  - mcp__taiga-py__set_task_status
  - mcp__taiga-py__add_comment
  - Bash(cd *)
  - Bash(git *)
  - Bash(env -u GH_TOKEN gh *)
  - Bash(python *)
  - Bash(python3 *)
  - Bash(timeout 590 *)
  - Bash(ruff *)
  - Bash(bash tests/scripts/*)
  - Bash(date *)
  - Bash(rm -f /tmp/implement-task-*)
  - Bash(find *)
  - Bash(ls *)
  - Read(//tmp/**)
  - Read
  - Edit
  - Write
  - Grep
  - Glob
  - WebFetch(https://github.com/*)
  - WebFetch(https://raw.githubusercontent.com/*)
  - WebFetch(https://discordpy.readthedocs.io/*)
  - WebFetch(https://supabase.com/*)
  - WebFetch(https://docs.python.org/*)
---

# Implement Task — Story #$ARGUMENTS

Implement the next incomplete task from a story. This skill handles exactly one task, then returns. The orchestrator calls it repeatedly with fresh agents until all tasks are done.

Taiga names: [`../shared/taiga-project.md`](../shared/taiga-project.md). Every MCP call passes `project=39`. Codebase background: the root `CLAUDE.md`.

## Step 1: Set up the branch

Parse arguments from `$ARGUMENTS`:
- First word: story ref number (e.g., `39`)
- Second word: branch name (e.g., `feature/39-tenant-filter`)
- Third word (optional): `<base>`, the branch this story's feature branch was cut from. Orchestrators always pass it. If absent (a manual invocation), use `main`.

**Worktree.** The spawn prompt names the worktree to work in (`Work only in <worktree>`); call it `<wt>`. Your shell's working directory resets between Bash calls, so start every command with `cd <wt> && ...` or use `git -C <wt>`. Never `cd` to the main checkout, never check out or switch to another branch, never run `git reset --hard` or `git stash` (the stash is shared by every worktree). If no worktree was named (a manual run), `<wt>` is `git rev-parse --show-toplevel`.

Check the branch:
1. `git -C <wt> branch --show-current` must print `<branch-name>`. If it does, `git -C <wt> pull --ff-only upstream <branch-name>` when the branch exists on `upstream` (ignore "couldn't find remote ref"; the branch may be local only).
2. If `<wt>` is on a different branch, do not switch it: report `BLOCKED: worktree <wt> is on <other-branch>, expected <branch-name>` and stop.
3. Manual run only, branch missing everywhere: from the checkout you were invoked in, `git fetch upstream <base> && git worktree add -b <branch-name> .claude/worktrees/story-<ref> upstream/<base>` and use that path as `<wt>`. Normally the calling skill (`/fully-implement-story` or `/implement-story`) already created the branch and worktree.

Then write the task lock as its own Bash call so the orchestrator can tell you are still alive:
```bash
date +%s > /tmp/implement-task-<story-ref>-<task-ref>.lock
```
The lock carries a heartbeat timestamp, not a pid. `$PPID` inside a Bash tool call is the long-lived `claude` CLI process shared by every subagent in the session, so its liveness says nothing about whether *this* agent is still working. Refresh the heartbeat with the same command at the start of each later step and immediately before and after any long-running command (test runs). The orchestrator treats a heartbeat older than 15 minutes as a dead agent and respawns. Write the lock as soon as the task ref is known (after Step 2 if it is not known yet).

## Step 2: Fetch story and find next task

1. Find the story and its tasks: `scripts/taiga-story.sh <story-ref> 39 --with-tasks`. It returns story JSON with a `tasks` array (`id`, `ref`, `subject`, `status`, `status_extra_info`, `is_closed`).
2. Fetch the full story for context: `get_story(project=39, ref=<story-ref>)`.
3. Find the first task whose status is not `Closed` (62) and whose description has no `## Gate` section (read each candidate with `get_task(project=39, ref=<task-ref>)`). If none remains, report **"All tasks complete"**, add one `GATED: <task-ref> <reason>` line for each open task that has a `## Gate` section, apply the tag removal from "Gated tasks" below if any are listed, and return.
4. Read the full task: `get_task(project=39, ref=<task-ref>)` for the description and implementation plan.
5. Mark it started: `set_task_status(project=39, ref=<task-ref>, status="In progress")`.

### Gated tasks

A task gated on something outside this branch (a live step, a human, or another epic) stays open after the story's PR merges. If the task you picked turns out mid-work to be impossible to finish for that reason, do not implement or close it:

1. Record why on the task itself, so later runs skip it: `update_task(project=39, ref=<task-ref>, description="<current description>\n\n## Gate\n\n<what it waits for>")`.
2. Remove the `close-tasks-on-merge` and `mark-done-on-merge` tags from the story if present: re-fetch it with `get_story(project=39, ref=<story-ref>)`, then `update_story(project=39, ref=<story-ref>, tags=<every current tag except those two>)`. An open task means neither signal holds (see [`../shared/merge-side-effects.md`](../shared/merge-side-effects.md)).
3. Discard your uncommitted edits for the task with `git -C <wt> checkout -- <file>` per file you touched (never `reset --hard` or `stash`), skip Steps 3 to 6, and report `GATED: <task-ref> <reason>` in Step 7 in place of the commit SHA and Taiga status lines. The orchestrator counts a gated task as finished, not failed.

**ALWAYS use `scripts/taiga-story.sh <story-ref> 39 --with-tasks` to find tasks.** The Taiga project is ALWAYS 39.

**Task statuses (names for MCP, ids as `taiga-story.sh` returns them):** `New` = 59, `In progress` = 60, `Closed` = 62 (use this in Step 6).

## Step 3: Implement the task

**If the task changes a command or reply**, keep the persona's Spanglish voice and update its `!<persona>` help text in the same task when the plan says so. Apply the hazard checklist the plan copied in (listeners, attachment gate, session state, `wait_for` timeouts, blocking I/O via `asyncio.to_thread`, identity, timezone, table/column names, command-name collisions).

1. **If the task description contains a detailed implementation plan**, skip investigation. Read only the files listed in the plan, then go straight to TDD.
2. **If there is no plan**, investigate the codebase as needed.
3. **TDD on testable code** (pure helpers, parsers, money and time functions, formatting, state transitions); every test run follows Step 4:
   a. Write a failing test that validates the desired behaviour
   b. Run it and confirm it fails as expected
   c. Write only enough code to make it pass
   d. Run it and confirm it passes
   e. Refactor while keeping tests green
4. **Code that needs a live bot** (discord.py command parsing, `on_message` interplay, prompts, embeds) or a browser (dashboards) cannot be unit tested yet. Implement it, then post the manual test steps on the task: `add_comment(project=39, ref=<task-ref>, entity_type="task", text="Manual test steps (test Discord server, #31):\n1. <exact message> → <expected reply>\n...")`. Say so in the report.
5. Never import `cholobots.py` in a test (importing it starts the bot). Follow the import approach the plan names (conftest from #34, extracted helper, or dummy env plus patched `supabase.create_client`). Never run `python cholobots.py`.

## Step 4: Run checks and tests

Pick the interpreter once: the main checkout's virtualenv if it exists, else `python3`:
```bash
main=$(dirname "$(git -C <wt> rev-parse --path-format=absolute --git-common-dir)"); ls "$main/.venv/bin/python" 2>/dev/null || echo python3
```
Use the printed path as `<py>` below (it is an absolute path, not a `cd` into the main checkout).

Refresh the task-lock heartbeat immediately before and after each run, each as its own Bash call.

Run each command as a foreground Bash call with the tool's `timeout: 600000` parameter set explicitly, wrapped in a shell `timeout 590`. Never use `run_in_background`, `Monitor`, or end your turn waiting for a notification: this runs headless, so there is no next turn. See [`../shared/synchronous-worker-contract.md`](../shared/synchronous-worker-contract.md).

1. **Compile check (always):**
   ```bash
   cd <wt> && timeout 590 <py> -m py_compile cholobots.py config.py db/database.py bots/*.py; echo "exit=$?"
   ```
2. **Bot unit tests:** if any test file exists under `tests/` outside `tests/scripts/`, run only the affected files: `cd <wt> && timeout 590 <py> -m pytest -q tests/<file>.py; echo "exit=$?"`. Otherwise record "no test suite yet (#34)".
3. **Script tests:** if the task changed `scripts/`, run `cd <wt> && timeout 590 <py> -m pytest tests/scripts -q` and the matching `bash tests/scripts/test-*.sh`.
4. **Lint:** if ruff is configured (`ruff.toml`, `.ruff.toml`, or `[tool.ruff]` in `pyproject.toml`), `cd <wt> && ruff check .`. Otherwise record "no linter configured yet (#33)".

Consult `docs/testing-workflow.md` for routing. Fix every failure, then re-run. **If you believe a failure is unrelated to your changes**, you MUST follow the Test Failure Triage Protocol in `.claude/skills/shared/test-failure-triage.md` before dismissing it.

## Step 5: Commit

Run the compile check from Step 4.1 again immediately before every commit; never commit code that fails it. Stage only the files this task changed and commit with a message referencing the task:

```
git -C <wt> commit -m "<description of what was implemented> (#<story-ref>, task #<task-ref>)

Co-Authored-By: <your session's model> <noreply@anthropic.com>"
```
Use your own session's attribution trailer; see `.claude/skills/shared/commit-attribution.md`.

Do NOT push. The orchestrator pushes after all tasks are done.

## Step 6: Mark task done

**This step is MANDATORY. The orchestrator checks your report for the Taiga status line and treats its absence as a failure.**

1. Close the task: `set_task_status(project=39, ref=<task-ref>, status="Closed")`.
2. **Verify** by re-fetching: `get_task(project=39, ref=<task-ref>)`. Confirm the status is `Closed`. If not, retry once. If it is still not `Closed`, proceed to Step 7 and report the failure (the orchestrator needs the `❌` line).
3. **Your Step 7 report MUST include this exact line:**
   ```
   Task status updated to Closed: ✅
   ```

## Step 7: Report

Before returning (including when reporting BLOCKED), remove the task lock as its own Bash call:
```bash
rm -f /tmp/implement-task-<story-ref>-<task-ref>.lock
```

The report MUST include:

- Which task was implemented (subject and ref)
- Commit SHA
- Files changed
- Check results: compile check, unit tests (pass/fail counts, or "no test suite yet (#34)"), lint (or "#33"), and whether manual test steps were posted on the task
- **`Task status updated to Closed: ✅`**, or `❌` with the specific error
- Whether tasks remain (and how many)

A `GATED: <task-ref> <reason>` report replaces the commit SHA, files, check results and "Task status updated" lines for that task.

Omitting the "Task status updated" line (outside a `GATED:` report) or reporting `❌` marks the run incomplete; the orchestrator may retry.

## Handling tool permission denials

1. Do NOT ask the user to approve permissions: this skill runs headless where no human can respond.
2. If the denied tool is in this skill's `allowed-tools`, try an alternative (e.g. `Write` instead of `Edit`, a different `Bash` pattern).
3. After 2 failed alternatives, report **"BLOCKED: Repeated permission denials on [tool]. Cannot complete task."** and exit. Do NOT mark the task done.

## What this skill does NOT do

- Does NOT push (the orchestrator does)
- Does NOT create PRs (`finalize-story-pr` does)
- Does NOT update story status (the orchestrator does)
- Does NOT implement or close a gated task
- Does NOT implement more than one task per invocation
- Does NOT run the bot
