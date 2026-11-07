---
name: fully-implement-story
description: Autonomously implement a story end-to-end with agents — implementation, PR monitoring, testing, bug fixing
argument-hint: "<story-ref> [base-branch] [--resume]"
allowed-tools:
  - Agent
  - Bash(scripts/taiga-story.sh *)
  - Bash(python3 scripts/taiga_project_setup.py --check*)
  - mcp__taiga-py__*
  - Bash(env -u GH_TOKEN gh *)
  - Bash(git *)
  - Bash(date *)
  - Bash(cat /tmp/implement-task-*)
  - Bash(rm -f /tmp/implement-task-*)
  - Bash(pgrep *)
  - Bash(sleep *)
  - Bash(python3 scripts/remediate_pr.py *)
  - Bash(GH_REPO=* python3 scripts/remediate_pr.py *)
  - Read(//tmp/**)
  - Edit(//tmp/**)
  - Write(//tmp/**)
  - Read
  - Grep
  - Glob
---

# Fully Implement Story $ARGUMENTS

Autonomously implement a story end-to-end: task-by-task implementation with fresh agents in a git worktree, PR creation, monitoring, manual testing, bug fixing, and retesting.

Taiga names: [`../shared/taiga-project.md`](../shared/taiga-project.md). Every MCP call passes `project=39`. GitHub: branches and PRs live on `upstream` (`MrsBohemian/Cholobots`); every `gh` call is `env -u GH_TOKEN gh ... --repo MrsBohemian/Cholobots`.

## Running Unattended

This skill may run under `/implement-epic` or the overseer. **There is no human present.** You MUST NOT post questions to the user like "what should I do?" or "should I keep retrying?". The skill defines the decision tree; follow it.

**Every Agent spawn in this skill MUST pass `run_in_background: false`.** If a turn ends while a subagent runs in the background, the headless CLI can exit and kill the subagent, losing all story work. See [`../shared/synchronous-worker-contract.md`](../shared/synchronous-worker-contract.md).

The same trap applies to every other way of leaving work in flight: never resume a subagent with `SendMessage` (it runs in the background), never use `Monitor` or a background Bash task to wait for a subagent, and never end a turn expecting a task notification to wake you. If a foreground spawn returns early, apply the **respawn guard** in [`../shared/story-task-delegation.md`](../shared/story-task-delegation.md) before spawning a fresh foreground agent for the same task.

**Worktree isolation.** All story work happens in `<wt>` = `<main checkout>/.claude/worktrees/story-<ref>` (Step 1c). This orchestrator runs git commands against the main checkout only to create or remove worktrees and fetch; it never runs `git checkout`/`git switch` there (other sessions share it). Every spawn prompt names its worktree and forbids `cd` to the main checkout, checking out other branches, `git reset --hard` and `git stash`.

**escalate(<reason>)** means, in this order:
1. Write `phase = "escalated"`, `escalated = true` and `escalationReason = <reason>` to the state file (its own Bash/Write call).
2. `update_story(project=39, ref=<ref>, status="Blocked", tags=[<current tags...>, "escalation-needed"])` (re-read the tags first; Taiga replaces the whole list).
3. `assign_item(project=39, ref=<ref>, username="Jon Roberts", entity_type="story")`.
4. `add_comment(project=39, ref=<ref>, text="Escalated by fully-implement-story: <reason>", entity_type="story")`, and if a PR exists, `env -u GH_TOKEN gh pr comment <prNumber> --repo MrsBohemian/Cholobots --body "Escalating to @bigjonroberts: <reason>"`.
5. Keep the worktree for inspection and **STOP**.

Decision rules when things go wrong:
- CI flaky but under 5 failures: invoke `/remediate-pr` again and let the script run its stability check.
- `/remediate-pr` returns ESCALATED: `escalate("<its reason>")`.
- Agent blocked by a permission denial or tool error: `escalate("<the specific reason>")`.
- Uncertainty about proceeding: `escalate("uncertain: <what>")`. Do not ask.

**Terminal phases**: the state file must end in one of these before you exit:
- `"complete"`: all steps succeeded, PR ready for review
- `"escalated"`: any reason, with populated `escalationReason`

If you would write any other phase (e.g. `"monitoring"`, `"fixing"`, `"testing"`) as your final action, you are doing it wrong: escalate instead. The phase write is its own call, never combined with cleanup such as `git worktree remove`.

## Step 0: Parse arguments

`$ARGUMENTS` is `<ref> [base-branch] [--resume]`. The first token is `<ref>`. Strip `--resume` before deciding whether a second token remains.

If a second token remains, it is `<base-branch>`, the branch this story's feature branch is cut from and the PR targets. If absent, `<base-branch>` is `main`.

**An explicit base is not advisory.** A story stack bases each story on the branch of the story before it, so cutting from or targeting the wrong branch puts the predecessor's whole diff into this story's PR. Carry the base through unchanged and pass it as an argument to `/finalize-story-pr` and `/rebase-branch`; do **not** also patch their commands to compensate.

## Step 0b: Taiga preflight

Run once, before anything else: `python3 scripts/taiga_project_setup.py --check`.
- Exit 0: proceed.
- Exit 2: the project lacks statuses, priorities or tags this pipeline uses. Do not write a state file or touch Taiga. Print the missing list and `Run: python3 scripts/taiga_project_setup.py --env-file scripts/taiga-auth.env --apply` for Jon, and stop.
- Any other exit: report the error and stop.

## Step 1: Validate and initialize

**Resume check: do this before anything else in Step 1.** You are resuming when `--resume` is present, or when `/tmp/fully-implement-<ref>.json` already exists with a non-terminal `phase`. The story is already `In progress` with work on disk: do not start over, and do not ask; resume.

1. Recover the state. If `/tmp/fully-implement-<ref>.json` exists, read it and do not overwrite it. If it does not (the machine was restarted; the worktree or branch survived), rebuild it from git and Taiga:
   - `git fetch upstream`, then `featureBranch`: `git branch -a --format='%(refname:short)' | grep -E '^(upstream/)?feature/<ref>-'`, dropping a leading `upstream/`. The anchored match matters for the reasons given in 1c.
   - `worktreePath`: the path `git worktree list --porcelain` shows for `featureBranch`, or null.
   - Look up any existing PR once: `env -u GH_TOKEN gh pr list --repo MrsBohemian/Cholobots --head <featureBranch> --state all --json number,baseRefName --jq '.[0]'` (null if none).
   - `baseBranch`: the PR's `baseRefName` if a PR was found (it survives a relaunch that passes only `<ref>`); otherwise `<base-branch>` from Step 0.
   - `prNumber`: the PR's `number`, or null.
   - `tasksCompleted` / `tasksTotal`: from `scripts/taiga-story.sh <ref> 39 --with-tasks`; tasks with `status == 62` (`Closed`) are done. Open tasks whose description has a `## Gate` section (`get_task(project=39, ref=<task-ref>)`) also count as done (see [`../shared/merge-side-effects.md`](../shared/merge-side-effects.md)).
   - `phase`: `"monitoring"` if a PR exists and is open, else `"implementing"`.
   - Write the block from 1d with those values.
2. Skip 1a's status validation (the story is no longer `Ready`); just fetch the story and its tasks for context.
3. In 1c, reuse `worktreePath` if it exists; otherwise add a worktree for `featureBranch` **without pulling** (its latest commits may be unpushed). If the branch does not exist at all, nothing was done before the interruption: fall through to a normal Step 1 from 1a (treating `In progress` as valid).
4. Skip 1d (already handled) and 1e (the story is already `In progress`).
5. Jump to the step for the recorded `phase` (see "Resumability" below), continuing at `tasksCompleted`.

### 1a. Fetch and validate the story

1. Find the story and its tasks: `scripts/taiga-story.sh <ref> 39 --with-tasks`. Extract `id` and `tasks`.
2. If the story is closed (`Done` or `Archived`), inform the user and stop.
3. Fetch the full story: `get_story(project=39, ref=<ref>)` and verify it has the `llm-analyzed` tag.
4. If it is not `Ready` or not analyzed, inform the user and stop.

### 1b. Metadata

No lookups needed: status names, the assignee name `"Jon Roberts"` and tags come from [`../shared/taiga-project.md`](../shared/taiga-project.md).

### 1c. Set up branch and worktree

1. `git fetch upstream`, then look for this story's branch: `git branch -a --format='%(refname:short)' | grep -E '^(upstream/)?feature/<ref>-'`. Anchoring on `feature/<ref>-` keeps the match to this story's own branch; an unanchored match on the bare ref also hits longer refs (`39` matches `feature/391-x`) and an integration branch named after the story (`epic/<epic>-integration-<ref>`), which is a base, never this story's branch.
2. `<wt>` = `<main checkout>/.claude/worktrees/story-<ref>` (absolute path), unless `git worktree list --porcelain` already shows the branch checked out elsewhere; then reuse that path.
3. Branch exists locally: `git worktree add <wt> <branch-name>`, then `git -C <wt> pull --ff-only upstream <branch-name>` if it is on upstream. Only on upstream: `git worktree add --track -b <branch-name> <wt> upstream/<branch-name>`.
4. No branch: `git fetch upstream +<base-branch>:refs/remotes/upstream/<base-branch> && git worktree add -b feature/<ref>-<slug> <wt> upstream/<base-branch>` (slug from the story subject, lowercase, hyphens, 3 to 4 words). The explicit refspec creates `upstream/<base-branch>` even for a stacked predecessor branch.

### 1d. Initialize state file

Skip on a resume. Write the initial state to `/tmp/fully-implement-<ref>.json`:

```json
{
  "storyRef": <ref>,
  "storyId": <story id from taiga-story.sh>,
  "phase": "implementing",
  "prNumber": null,
  "prCreatedAt": null,
  "featureBranch": "<branch-name>",
  "baseBranch": "<base-branch>",
  "worktreePath": "<wt>",
  "tasksCompleted": 0,
  "openTaskIds": [<ids of tasks with status != 62>],
  "tasksTotal": <length of openTaskIds>,
  "verifiedTaskIds": [],
  "preSpawnTip": null,
  "noCommitRecoveryAttempted": false,
  "bugs": [],
  "manualTesting": null,
  "testReviewComplete": false,
  "escalated": false,
  "escalationReason": null
}
```

`openTaskIds` (and therefore `tasksTotal`) holds only tasks still open at the start of this run: tasks closed by a prior attempt are not re-implemented, so counting them would make the Step 2 completion gate unsatisfiable.

### 1e. Set story to "In progress"

`set_story_status(project=39, ref=<ref>, status="In progress")`

## Step 2: Task-by-task implementation

Follow **Task loop** in [`../shared/story-task-delegation.md`](../shared/story-task-delegation.md) with the worktree `<wt>` and the run state kept in `/tmp/fully-implement-<ref>.json` (`preSpawnTip`, `openTaskIds`, `verifiedTaskIds`, `tasksCompleted`, `tasksTotal`, `noCommitRecoveryAttempted`). Record `preSpawnTip` from `git -C <wt> rev-parse <branch-name>` before every spawn. *escalate(<reason>)* is the procedure in "Running Unattended".

## Step 2a: Verify all tasks are Closed in Taiga

Follow **Safety net** in [`../shared/story-task-delegation.md`](../shared/story-task-delegation.md) with the same run state and *escalate*.

## Step 2b: Test quality review

Follow **Test-quality review** in [`../shared/story-task-delegation.md`](../shared/story-task-delegation.md) with the same *escalate*.

Update state file: `testReviewComplete = true`.

## Step 3: Finalize PR

1. **Spawn a fresh `senior-software-developer` agent** (`run_in_background: false`) with the prompt: `"Work only in <wt>. Finalize the PR for story #<ref> on branch <branch-name>. Follow the finalize-story-pr skill instructions with arguments '<ref> <branch-name> <base-branch>'. Never cd to the main checkout, check out another branch, run git reset --hard or git stash."`
2. **When the agent returns**, get the PR number from GitHub rather than the report: `env -u GH_TOKEN gh pr list --repo MrsBohemian/Cholobots --head <branch-name> --state open --json number --jq '.[0].number'`. None → `escalate("finalize-story-pr created no PR")`.
3. Update state file: `phase = "monitoring"`, `prNumber = <number>`, `prCreatedAt = <now>`.

## Step 3b: Rebase check

1. `env -u GH_TOKEN gh pr view <prNumber> --repo MrsBohemian/Cholobots --json mergeStateStatus -q .mergeStateStatus`
2. If `"BEHIND"` or `"DIRTY"`:
   a. Spawn `senior-software-developer` (`run_in_background: false`): `"Work only in <wt>. Rebase the PR branch onto <base-branch>. Follow the rebase-branch skill instructions with arguments '<prNumber> <base-branch>'. Never cd to the main checkout, check out another branch, run git reset --hard or git stash."`
   b. If the rebase escalated (conflict too complex): `escalate("rebase conflict escalated")`.
   c. On success, **the full pipeline must re-run**: Step 4 (monitoring), then Step 5 (testing), and so on, even if testing already passed, since new base code may change behaviour. Each `/remediate-pr` invocation starts with a fresh failure count.
3. If `"CLEAN"`, `"HAS_HOOKS"` or `"UNSTABLE"`, proceed to Step 4.

**Also run this check** at the start of Step 7: merged bug-fix sub-PRs may have moved the branch relative to its base.

## Step 4: PR monitoring and remediation

Follow `.claude/skills/remediate-pr/SKILL.md` with arguments:

```
<prNumber> --taiga-id <ref> --taiga-type story --max-failures 5
```

It drives `python3 scripts/remediate_pr.py` (stability monitoring, failure counting, state) and spawns agents only for fixes and comment resolution. Cholobots has no CI yet (story #35): "no CI checks configured" is reported as STABLE with a note, never as a failure or an endless wait. Record the note for the Step 9 report.

- **`STABLE`**: proceed to Step 5.
- **`ESCALATED`**: `escalate("5+ build failures")`.
- **`NEEDS_REBASE`**: go to Step 3b.

## Step 5: Manual testing phase

1. Update state file: `phase = "testing"`.
2. Spawn `senior-manual-testing-specialist` (`run_in_background: false`). The prompt includes:
   - Story ref, subject and acceptance criteria (from the story description), and the plan's manual test steps plus any posted on the tasks
   - PR number and branch, and the worktree `<wt>` to read code from (never `cd` to the main checkout)
   - Where to test: Discord commands in the test Discord server (story #31) against a bot running this branch; dashboards from `../Cholobots-Mobile` (or `mobile/` after story #64) with the Chrome DevTools MCP. A local `python -m http.server` for the dashboards never exits: start it as a background Bash call and kill it at the end.
   - "This is the **first pass**. Test all acceptance criteria and edge cases (cancel, timeout, unknown user, Supabase failure, other listeners in the channel). File bugs in Taiga for any deficiencies found, tagged `src:manual-test`."
   - "If the test environment is not available (no test server or test bot token yet), do not run the bot against production. End your final message with `MANUAL_TEST_UNAVAILABLE: <what is missing>`."
3. Check its output:
   - `MANUAL_TEST_UNAVAILABLE` → record `manualTesting = "skipped: <reason>"`. Manual verification is now post-merge validation, so drop `mark-done-on-merge` (`env -u GH_TOKEN gh pr edit <prNumber> --repo MrsBohemian/Cholobots --remove-label mark-done-on-merge`, and remove the story tag of that name if present), post the manual steps as a PR comment headed "Manual verification pending (test environment, #31)", and proceed to Step 9.
   - No bugs filed → `manualTesting = "passed"`, proceed to Step 9.
   - Bugs filed → extract the bug refs, add each to the state file `bugs` array with status `"filed"`, proceed to Step 6.

## Step 6: Bug fixing phase

1. Update state file: `phase = "fixing"`.
2. For each bug in `state.bugs` with status `"filed"`:

   a. Create a worktree for it on the feature branch tip: `git fetch upstream <featureBranch> && git worktree add --detach <main checkout>/.claude/worktrees/bug-<bug-ref> upstream/<featureBranch>` (the fix skill creates its `fix/` branch there). Call it `<bug-wt>`.

   b. **Spawn `senior-software-developer`** (`run_in_background: false`): `"Work only in <bug-wt>. Fix bug #<bug-ref> on feature branch <featureBranch>. Follow the fix-story-bug skill instructions with arguments '<bug-ref> <featureBranch>'. Never cd to the main checkout, check out another branch, run git reset --hard or git stash."`

   c. When it returns, find the sub-PR on GitHub: `env -u GH_TOKEN gh pr list --repo MrsBohemian/Cholobots --base <featureBranch> --search "<bug-ref>" --json number,headRefName`. None → `escalate("fix-story-bug created no sub-PR for #<bug-ref>")`.

   d. Update the bug in the state file: `status = "sub-pr-monitoring"`, `subPrNumber`, `subBranch = <headRefName>`, `worktreePath = <bug-wt>`.

   e. **Monitor the sub-PR** by following `.claude/skills/remediate-pr/SKILL.md` with `<subPrNumber> --taiga-id <bug-ref> --taiga-type issue --max-failures 5`. If it returns `ESCALATED`: `escalate("sub-PR #<subPrNumber> for bug #<bug-ref> escalated")`.

   f. When remediation returns `STABLE`:
      - Merge the sub-PR: `env -u GH_TOKEN gh pr merge <subPrNumber> --repo MrsBohemian/Cholobots --squash --delete-branch`
      - Bring the story worktree up to date: `git -C <wt> pull --ff-only upstream <featureBranch>`
      - Remove the bug worktree: `git worktree remove <bug-wt>`
      - Update the bug's status to `"merged"` in the state file
      - Make sure the bug is `Ready for test` in Taiga (fix-story-bug sets it): `update_issue(project=39, ref=<bug-ref>, status="Ready for test")`

3. When all bugs are `"merged"`, proceed to Step 7.

## Step 7: Post-fix monitoring

1. Update state file: `phase = "post-fix-monitoring"`.
2. Run Step 3b's rebase check, then follow `.claude/skills/remediate-pr/SKILL.md` with `<prNumber> --taiga-id <ref> --taiga-type story --max-failures 5`.
   - **`STABLE`**: proceed to Step 8.
   - **`ESCALATED`**: `escalate("5+ post-fix build failures")`.

## Step 8: Second-pass testing

1. Update state file: `phase = "retesting"`.
2. Spawn `senior-manual-testing-specialist` (`run_in_background: false`) with the same context as Step 5, plus: "This is the **second pass**. Re-test each fixed bug and the original acceptance criteria." and the list of bug refs to verify.
3. Check its output:
   - **Issues found**: `escalate("second-pass testing found issues")`.
   - **`MANUAL_TEST_UNAVAILABLE`**: handle as in Step 5 (skip with note, drop `mark-done-on-merge`), proceed to Step 9.
   - **No issues**: proceed to Step 9.

## Step 9: Success

1. Update state file: `phase = "complete"` (its own call).
2. **Close verified bugs**: for each bug with status `"merged"` that the second pass verified, `update_issue(project=39, ref=<bug-ref>, status="Closed")` and `add_comment(project=39, ref=<bug-ref>, entity_type="issue", text="Verified fixed: second-pass testing passed. Closed by fully-implement-story.")`. Update the bug to `"closed"` in the state file. Bugs not verified (manual testing skipped) stay `Ready for test`.
3. Make sure the story is `Ready for review` (finalize-story-pr set it): `set_story_status(project=39, ref=<ref>, status="Ready for review")`.
4. Assign the story to Jon: `assign_item(project=39, ref=<ref>, username="Jon Roberts", entity_type="story")`.
5. Make sure the PR link is on the story (finalize-story-pr comments it; add it if missing): `add_comment(project=39, ref=<ref>, text="PR: https://github.com/MrsBohemian/Cholobots/pull/<prNumber>", entity_type="story")`.
6. Post a PR comment:
   ```
   env -u GH_TOKEN gh pr comment <prNumber> --repo MrsBohemian/Cholobots --body "Story implementation complete. Comments resolved, manual testing: <passed | skipped: reason>. CI: none configured yet (#35). Ready for review."
   ```
7. Remove the story worktree (its own Bash call; the branch stays on upstream): `git worktree remove <wt>`.
8. Report success with the PR link, the base branch and whether it came from the argument or the `main` default, the manual-testing result, and the "no CI configured" note.

## Error handling

- If any agent spawn fails, `escalate("<the error>")`.
- The state file at `/tmp/fully-implement-<ref>.json` enables resumability: re-invoking the skill reads it and continues from the current phase.

## Resumability

The resume check at the top of Step 1 decides whether this run resumes and recovers the state. It then jumps to the step for `phase`:
  - `"implementing"` → Step 2 (or Step 2b if all tasks are done but `testReviewComplete` is false)
  - `"monitoring"` → Step 3b (rebase check), then Step 4
  - `"testing"` → Step 5
  - `"fixing"` → Step 6
  - `"post-fix-monitoring"` → Step 7 (which runs Step 3b first)
  - `"retesting"` → Step 8

On resume, `<base-branch>` comes from the state file's `baseBranch`, and `<wt>` from `worktreePath`, not from re-resolving Step 0. This matters because the `"monitoring"` and `"post-fix-monitoring"` entry points both reach Step 3b, which spawns `/rebase-branch`.
