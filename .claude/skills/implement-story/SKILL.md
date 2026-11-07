---
name: implement-story
description: Implement a user story from Taiga project 39 (cholo-bots)
argument-hint: "<story-ref> [base-branch]"
allowed-tools:
  - Agent
  - Bash(scripts/taiga-story.sh *)
  - mcp__taiga-py__get_story
  - mcp__taiga-py__update_story
  - mcp__taiga-py__set_story_status
  - mcp__taiga-py__assign_item
  - mcp__taiga-py__get_task
  - mcp__taiga-py__update_task
  - mcp__taiga-py__set_task_status
  - mcp__taiga-py__add_comment
  - Bash(git *)
  - Bash(env -u GH_TOKEN gh *)
  - Bash(date *)
  - Bash(cat /tmp/implement-task-*)
  - Bash(rm -f /tmp/implement-task-*)
  - Bash(pgrep *)
  - Bash(sleep *)
  - Read(//tmp/**)
  - Read
  - Grep
  - Glob
---

# Implement Story $ARGUMENTS

Taiga names: [`../shared/taiga-project.md`](../shared/taiga-project.md). Every MCP call passes `project=39`. GitHub: branches and PRs live on `upstream` (`MrsBohemian/Cholobots`); every `gh` call is `env -u GH_TOKEN gh ... --repo MrsBohemian/Cholobots`.

## Step 0: Parse arguments

`$ARGUMENTS` is `<ref> [base-branch]`. The first token is `<ref>`, a **ref number** (what the Taiga UI shows). Use `<ref>` alone (never the raw `$ARGUMENTS`) wherever a step calls Taiga tooling; passing the base branch as a second positional argument would make `scripts/taiga-story.sh` misread it as the project id.

If a second token is present, it is `<base>`, the branch to cut this story's feature branch from (a predecessor's branch when the story is part of a stack). If absent, `<base>` is `main`.

## Step 1: Fetch story and tasks from Taiga

1. Find the story and its tasks: `scripts/taiga-story.sh <ref> 39 --with-tasks`. Extract `id` and `tasks`.
2. If the story is closed (`Done` or `Archived`), inform the user and stop.
3. Fetch the full story: `get_story(project=39, ref=<ref>)`.

Record the run's `openTaskIds` = ids of tasks with status != 62 (`Closed`) (the `id` field of each `tasks` entry, not its `ref`) and `tasksTotal` = the length of `openTaskIds`. Closed tasks are not re-implemented, so counting them would make the completion gate unsatisfiable. Copy the story subject and each open task subject into your TodoWrite list. No other investigation is needed.

## Step 2: Set story to In progress

`set_story_status(project=39, ref=<ref>, status="In progress")`

## Step 2.5: Set up a working branch and worktree

Run these from the main checkout; never `git checkout` or `git switch` there (other sessions share it).

1. `git fetch upstream`.
2. Look for this story's branch: `git branch -a --format='%(refname:short)' | grep -E '^(upstream/)?feature/<ref>-'`. Anchoring on the `feature/<ref>-<slug>` convention keeps the match to this story's own branch; an unanchored match on the bare ref also hits longer refs (`39` matches `feature/391-x`) and integration branches named after the story.
3. Pick the worktree path `.claude/worktrees/story-<ref>` (absolute: `<main checkout>/.claude/worktrees/story-<ref>`), call it `<wt>`.
   - If `git worktree list --porcelain` already shows the branch checked out somewhere, reuse that path as `<wt>`.
   - Branch exists locally: `git worktree add <wt> <branch-name>`.
   - Branch exists only on upstream: `git worktree add --track -b <branch-name> <wt> upstream/<branch-name>`.
   - No branch: `git fetch upstream <base> && git worktree add -b feature/<ref>-<short-slug> <wt> upstream/<base>` (slug from the story subject, lowercase, hyphens, 3 to 4 words).
4. Fast-forward a reused branch when upstream is ahead: `git -C <wt> pull --ff-only upstream <branch-name>` (skip if the branch is not on upstream yet).

## Step 3: Implement tasks with fresh agents

Follow [`../shared/story-task-delegation.md`](../shared/story-task-delegation.md): **Task loop** (applying **Respawn guard** before every re-spawn), then **Safety net**, then **Test-quality review**, with `<branch-name>` and `<wt>` from Step 2.5 and `<base>` from Step 0. Every spawn prompt names `<wt>` and forbids `cd` to the main checkout, checking out other branches, `git reset --hard` and `git stash`. Every Agent spawn is foreground (`run_in_background: false`); this skill may itself be running inside an `/overseer` subagent.

- **Run state:** keep it in this conversation. `openTaskIds` / `tasksTotal` come from Step 1; record `preSpawnTip` (the literal SHA from `git -C <wt> rev-parse <branch-name>`) before each spawn; start `tasksCompleted` at 0, `verifiedTaskIds` as `[]` and `noCommitRecoveryAttempted` as false; mark a task's TodoWrite item `completed` when it is added to `verifiedTaskIds`.
- **escalate(<reason>):** set the story `Blocked`, tag it `escalation-needed` and assign Jon (`update_story(project=39, ref=<ref>, status="Blocked", tags=[<current tags...>, "escalation-needed"])`, then `assign_item(project=39, ref=<ref>, username="Jon Roberts", entity_type="story")`), post the reason with `add_comment(project=39, ref=<ref>, text="Escalated: <reason>", entity_type="story")`, keep the worktree for inspection, stop here (do not run Step 4), and report `BLOCKED: <reason>`.

Carry any `GATED:` lines into the Step 4 report.

## Step 4: Finish — set story to "Ready for review"

When all tasks are done:

1. Spawn a fresh `senior-software-developer` agent (`run_in_background: false`) with the prompt: `"Work only in <wt>. Finalize the PR for story #<ref> on branch <branch-name>. Follow the finalize-story-pr skill instructions with arguments '<ref> <branch-name> <base>'. Never cd to the main checkout, check out another branch, run git reset --hard or git stash."` It runs the checks, pushes to `upstream`, creates the PR against `<base>`, applies the merge-side-effect labels (or the fallback tags), runs the Codex review, sets the story to `Ready for review`, assigns Jon and comments the PR link.
2. Confirm from Taiga, not the report: `get_story(project=39, ref=<ref>)` shows `Ready for review`, and `env -u GH_TOKEN gh pr list --repo MrsBohemian/Cholobots --head <branch-name> --json number,url` returns the PR. If either is missing, `escalate("finalize-story-pr did not complete: <what is missing>")`.
3. Assign the story to Jon for review if finalize did not: `assign_item(project=39, ref=<ref>, username="Jon Roberts", entity_type="story")`.
4. **Do NOT set the story to `Done`** yourself. Done happens either by Jon after merging the PR, or by the post-merge sync when the PR carries `mark-done-on-merge` (or the story keeps that tag); see [`../shared/merge-side-effects.md`](../shared/merge-side-effects.md).
5. Remove the worktree; the branch is pushed and stays: `git worktree remove <wt>`.
6. Report what was completed, the PR link, the merge-side-effect labels or tags, the Codex review line (`REVIEW_POSTED`/`REVIEW_SKIPPED`/`REVIEW_PENDING`), the `GATED:` lines, and the "no CI yet (#35)" note.

Remind the user to run `/clear` before invoking `/implement-story` again, to avoid context pollution from the previous story.
