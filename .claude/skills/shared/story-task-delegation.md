# Story Task Delegation

How a story skill implements its open tasks with fresh agents. Followed by `fully-implement-story` and `implement-story`.

Task statuses (from `.claude/skills/shared/taiga-project.md`): `New` = 59, `In progress` = 60, `Closed` = 62. `implement-task` marks a finished task `Closed` (62), so 62 is the "done" status counted below. `scripts/taiga-story.sh <ref> 39 --with-tasks` returns each task's numeric `status`, `id` and `ref`; MCP writes take the task ref and the status name.

## Caller contract

The calling skill supplies:

- Inputs `<ref>`, `<branch-name>` and `<base-branch>`, and the worktree path the story runs in.
- The *run state*: `preSpawnTip`, `openTaskIds` (tasks with status != 62 at run start), `verifiedTaskIds`, `tasksCompleted`, `tasksTotal` and `noCommitRecoveryAttempted`. The caller says where it keeps them; this procedure only reads and updates them.
- *escalate(<reason>)*: the caller's stop action for an unrecoverable problem. This procedure never writes a phase or names a state file.

Every spawn prompt below carries the worktree path and forbids `cd` to the main checkout, checking out other branches, `git reset --hard` and `git stash` (the stash is shared across worktrees).

## Respawn guard

Apply this before spawning a fresh foreground agent for a task after a foreground spawn returned early or its test run was interrupted. An early return is not proof the subagent is gone:

1. Check the task lock `/tmp/implement-task-<storyRef>-<taskRef>.lock` (written by implement-task). It holds a heartbeat epoch second that the subagent refreshes as it works, never a pid, because `$PPID` inside a Bash tool call is the long-lived `claude` CLI process shared by every subagent, so its liveness would always succeed and the lock of a crashed agent would pin the orchestrator forever. The subagent is still alive only if the heartbeat is under 15 minutes old:
   ```bash
   lock=/tmp/implement-task-<storyRef>-<taskRef>.lock
   [ -f "$lock" ] && [ $(( $(date +%s) - $(cat "$lock") )) -lt 900 ] && echo alive || echo dead
   ```
   While it reports `alive`, poll every 30 s in a single Bash call (`timeout: 600000`) until the lock is gone or the heartbeat goes stale. If it is still `alive` when the poll window ends, `escalate("task #<ref> subagent still running after early return")`; do not respawn.
2. Only when the lock is absent or its heartbeat is stale: `rm -f` any stale lock, then spawn the fresh foreground agent.
3. Additionally refuse to respawn while a test run is still going in the worktree (`pgrep -f "pytest.*<worktree path>"` matches) or the worktree has uncommitted changes (`git -C <worktree> status --porcelain` is non-empty) that no live worker owns: list them in the escalation instead of letting a second agent build on a half-finished edit.

## Task loop

Loop until all tasks are done. If the run state already has `tasksCompleted == tasksTotal` on entry (no open tasks, or every task gated), skip the loop and go straight to **Safety net**.

1. **Record the branch tip, then spawn a fresh `senior-software-developer` agent:**
   - Before spawning: run `git -C <worktree> rev-parse <branch-name>` and record the result in the run state as `preSpawnTip`. Shell variables do not survive across tool calls, so the check below substitutes the literal SHA from the run state.
   - `subagent_type: "senior-software-developer"`
   - `model: "sonnet"`: only this spawn is overridden. The analyzed task supplies a file-level plan and TDD steps, while the Opus `test-quality-reviewer` and Codex review catch mistakes downstream; finalize, rebase, and bug-fix spawns stay on the agent's Opus pin.
   - `run_in_background: false`
   - Prompt: `"Work only in <worktree>. Implement the next task for story #<ref> on branch <branch-name>. Follow the implement-task skill instructions with arguments '<ref> <branch-name> <base-branch>'. Never cd to the main checkout, check out another branch, run git reset --hard or git stash."`

2. **When agent returns**, verify the branch, not the report. Report formatting is known-flaky, so the completion signal is `git -C <worktree> rev-list <preSpawnTip>..<branch-name>` (substitute `<preSpawnTip>` from the run state): non-empty output means the task's commit landed, whatever SHA the report cites; empty output means the agent returned before finishing. Docs/config-only tasks still commit, so a landed commit is always expected.
   - If output contains "BLOCKED:" → the agent hit a permission denial or other unrecoverable issue. Tag the story `escalation-needed` (`update_story(project=39, ref=<ref>, tags=<current tags + escalation-needed>)`) and assign it to Jon (`assign_item(project=39, ref=<ref>, username="Jon Roberts", entity_type="story")`), then `escalate("agent blocked: <details>")` and **STOP**.
   - If output contains one or more `GATED: <task-ref> <reason>` lines → each named task is left open by design and counts as finished, not failed, so never retry it. Resolve each `<task-ref>` to its id by fetching the story's tasks with `scripts/taiga-story.sh <ref> 39 --with-tasks` (here `<ref>` is the story ref) and matching `<task-ref>` against the `ref` field of each entry in the returned `tasks` array; a matched `id` that is not in `openTaskIds` (a stale or incorrect report naming a task closed before this run) is not counted: `escalate("GATED report names task #<task-ref> outside the run's open tasks")` and **STOP**; for every matched `id` in `openTaskIds` and not already in `verifiedTaskIds`, append it to `verifiedTaskIds`, increment `tasksCompleted`, and write the run state. A gated task lands no commit, so when the report gates a task and claims nothing else, skip the `rev-list` checks below and continue the loop. Carry the `GATED:` lines into the caller's final report. See [`merge-side-effects.md`](merge-side-effects.md).
   - If output contains "All tasks complete" **and** the run state has `tasksCompleted == tasksTotal` → break loop, go to **Safety net**. If `tasksCompleted < tasksTotal`, the sentinel is not trusted (a worker closed a task in Taiga without landing its commit), so treat it exactly like an empty `rev-list` and enter the recovery path below.
   - `rev-list` non-empty → the work landed. Re-fetch tasks with `scripts/taiga-story.sh <ref> 39 --with-tasks` and find the task at status 62 whose id is in `openTaskIds` but not yet in `verifiedTaskIds`:
     - Found → read run state, reset `noCommitRecoveryAttempted` to false, append its id to `verifiedTaskIds`, increment `tasksCompleted`, write run state, continue loop.
     - None found → the commit landed but Taiga was not updated (the report carries `Task status updated to Closed (status 62): ❌`, or omits the line). Do **not** re-spawn: the retry would re-implement the task and double-count it. Close it from here using the task ref named in the report: `set_task_status(project=39, ref=<task ref>, status="Closed")`, then `get_task(project=39, ref=<task ref>)` to confirm `Closed`. Confirmed → record it as above and continue. Still not closed → `escalate("set_task_status failed for task #<task ref> after its commit landed")` and **STOP**.
   - `rev-list` empty (or the untrusted sentinel above) → first apply **Respawn guard** before re-spawning (an empty `rev-list` is exactly the early return it covers), checking every `/tmp/implement-task-<ref>-*.lock` present, because the stranded task's ref is unknown when the agent closed nothing. If the guard waited on a live lock, re-run this step against the same `preSpawnTip` instead of recovering: the late worker may have landed its commit or closed its task while the guard waited. Otherwise recover once, tracked by a `noCommitRecoveryAttempted` flag in the run state (false at run start; reset to false each time a task's commit is verified landed). If the flag is already true, do not recover again: `escalate("task agent returned without a landed commit")` and **STOP**. Otherwise set the flag to true and write the run state before recovering. Identify the stranded task from Taiga rather than the report, which on the sentinel route names no task: re-fetch tasks with `scripts/taiga-story.sh <ref> 39 --with-tasks`; every task at status 62 whose id is in `openTaskIds` but not in `verifiedTaskIds` was closed without a landed commit, so reopen each with `set_task_status(project=39, ref=<task ref>, status="New")` so the retry picks it up instead of reporting "All tasks complete". Then record a fresh `preSpawnTip` and re-spawn the task agent. The retry is subject to the same `rev-list` check: an "All tasks complete" reply from the retry counts as a failure (it falls through the `tasksCompleted < tasksTotal` gate above), because the retried task cannot be complete without a landed commit. If the retry also lands nothing, `escalate("task agent returned without a landed commit")` and **STOP**.

3. Each agent gets a completely fresh context window, with no accumulated state from previous tasks.

4. When the loop exits (all tasks done or BLOCKED), proceed to **Safety net** to verify Taiga task statuses.

## Safety net

Safety net for flaky `implement-task` compliance with its Taiga-update step. Before proceeding to test-quality review, confirm every task on the story is `Closed` (62). If any aren't, force-close them now.

1. Re-fetch the story with tasks: `scripts/taiga-story.sh <ref> 39 --with-tasks`
2. For each task in the returned `tasks` array where `status != 62`, except a task reported `GATED:` or whose description has a `## Gate` section (fetch each with `get_task(project=39, ref=<task ref>)`; the `--with-tasks` listing omits descriptions). Those stay open by design; list them in the caller's final report under "Tasks left open by design: #<ref>, #<ref>". Close every other one:
   ```
   set_task_status(project=39, ref=<task ref>, status="Closed")
   ```
   Log each one you closed here, in the caller's final report under "Tasks auto-closed by safety net: #<ref>, #<ref>".
3. **Verify the updates succeeded** by re-fetching the story with tasks: `scripts/taiga-story.sh <ref> 39 --with-tasks`. Confirm every task other than the gated ones now has `status == 62`. If any task is still open after its update, `escalate("safety net set_task_status failed for task #<ref>")` and **STOP**.
4. Proceed to **Test-quality review** regardless of how many tasks needed closing (as long as all but the gated ones are now confirmed Closed).

Finding open stragglers is routine cleanup, not a failure signal: do NOT escalate merely because tasks were found open. But if the status write fails (the task remains open after the update attempt), that IS a failure: escalate. File a separate bug only if the same task keeps reappearing as "open" across multiple runs (which would indicate a deeper issue).

## Test-quality review

After all tasks are complete, review the test changes for strategy compliance before creating the PR. Skip this section, and say "no test changes (no test suite yet, #34)" in the caller's report, when `git -C <worktree> diff --name-only <base-branch>...<branch-name> -- tests/` is empty.

1. **Spawn a fresh `test-quality-reviewer` agent:**
   - `subagent_type: "test-quality-reviewer"`
   - `run_in_background: false`
   - Prompt: `"Work only in <worktree>. Review all test changes on branch <branch-name> against the testing strategy. Follow the review-tests skill instructions with arguments '<branch-name>'."`

2. **When agent returns**, check its output:
   - If the agent made changes (moved/deleted/rewritten tests), the changes are already committed on the branch
   - If the agent reports "No test changes to review" or "All test changes comply", proceed
   - If the agent reports issues it could not auto-fix, **treat this as BLOCKED**: tag the story `escalation-needed` and assign it to `"Jon Roberts"` as above, then `escalate("test-quality-review issues: <summary>")` and **STOP**. Do **not** continue past this section until the issues are resolved.
