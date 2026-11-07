---
name: reset-epic-stories
description: Reset failed/partial epic story runs — removes story worktrees, deletes branches and state files, resets Taiga statuses, optionally closes PRs
argument-hint: "<epic-ref> <story-refs...> [--close-prs] [--dry-run]"
disable-model-invocation: true  # human-only: destructive sweep of worktrees, branches, state files, and Taiga statuses
allowed-tools:
  - Bash(git *)
  - Bash(env -u GH_TOKEN gh *)
  - Bash(rm *)
  - Bash(cat *)
  - Bash(ls *)
  - Bash(python3 *)
  - Bash(scripts/taiga-story.sh *)
  - Read
  - Grep
  - AskUserQuestion
  - mcp__taiga-py__*
---

# Reset Epic Stories $ARGUMENTS

Fully reset one or more stories from an `/implement-epic` run so they can be re-run from scratch.
This is a destructive cleanup — it removes all artifacts from previous runs. Every `gh` call is
`env -u GH_TOKEN gh ... --repo MrsBohemian/Cholobots`.

## Step 1: Parse arguments

Split `$ARGUMENTS` into `<epic-ref>`, one or more `<story-ref>` numbers, and the optional flags
`--close-prs` (close each story's open PR with a comment) and `--dry-run` (print the plan, change
nothing).

Example: `46 47 48 50` means epic 46, stories 47, 48, 50.

Set `EPIC_DIR=/tmp/epic-<epic-ref>` and `STATE_FILE=/tmp/implement-epic-<epic-ref>.json`.

## Step 2: Build the plan (read-only)

For each story ref, collect:

1. **Worktree**: `.claude/worktrees/story-<ref>` if it exists (`git worktree list`), plus its
   `git -C <worktree> status --short` so uncommitted work is visible before it is destroyed.
2. **Branches**: the story's `branch` from `$STATE_FILE` if recorded, plus local
   `git branch --list "feature/<ref>-*"` and remote `git branch -r --list "upstream/feature/<ref>-*"`
   (after `git fetch upstream`). Include an `epic/<epic-ref>-integration-<ref>` branch if one exists.
3. **State files**: `/tmp/fully-implement-<ref>.json` if present.
4. **PRs**: `env -u GH_TOKEN gh pr list --repo MrsBohemian/Cholobots --state open --search "<ref> in:title" --json number,title,headRefName`, keeping titles that claim the ref.
5. **Taiga**: `get_story(project=39, ref=<ref>)` for its status, tags, and tasks
   (`scripts/taiga-story.sh <ref> 39 --with-tasks`). A story in a closed status (`Done`,
   `Archived`) is **not reset**: mark it skipped in the plan with the reason.

Also note whether `$STATE_FILE` exists and whether any of its stories outside this list are still
`launching`/`active` (another session may be running the epic).

## Step 3: Confirm

Print the plan as a table (story, worktree + dirty files, branches local/remote, PRs, Taiga
changes, skipped reasons). With `--dry-run`, stop here.

If `$STATE_FILE` shows a `"phase": "running"` with `launching`/`active` stories, warn that an
`/implement-epic` session may still be driving this epic; it must be stopped first.

Ask the operator to confirm (`AskUserQuestion`): **reset as planned** / **cancel**. Do nothing
destructive without an explicit yes.

## Step 4: Remove worktrees and local artifacts

For each story ref:
- `git worktree remove --force .claude/worktrees/story-<ref>` (force: the operator confirmed
  discarding its uncommitted work), then `git worktree prune`.
- `rm -f /tmp/fully-implement-<ref>.json`.

Also:
- `rm -f $STATE_FILE` — the epic state file (the next `/implement-epic` run starts fresh and
  re-reads Taiga; stories not in this reset that are already `Ready for review` are no longer
  `Ready`, so they are not re-run).
- `rm -rf $EPIC_DIR` — `stories-input.json`, `description.md`, `deps.json`.

## Step 5: Close PRs (only with `--close-prs`)

For each open PR found in Step 2:
`env -u GH_TOKEN gh pr close <n> --repo MrsBohemian/Cholobots --comment "Closed by /reset-epic-stories: story #<ref> is being re-run from scratch."`

Without `--close-prs`, leave PRs open and list them in the summary; deleting the head branch in
Step 6 would close them anyway, so without the flag skip the remote delete for any branch that
still has an open PR.

## Step 6: Delete branches

For each story ref:
- Local: `git branch -D <branch>` for every local branch found in Step 2.
- Remote: `git push upstream --delete <branch>` for every `upstream/` branch found in Step 2
  (strip the `upstream/` prefix), except a branch whose PR stayed open (Step 5).

## Step 7: Reset Taiga

For each story ref not skipped in Step 2:
1. Read the current tags; write back the list without `escalation-needed`, keeping
   `llm-analyzed` and every other tag (read-modify-write).
2. `update_story(project=39, ref=<ref>, status="Ready", tags=<filtered tags>)` — this also moves a
   `Blocked` story back to `Ready`.
3. For each of the story's tasks not already `New`:
   `set_task_status(project=39, ref=<task ref>, status="New")`.
4. `add_comment(project=39, ref=<ref>, text="Reset by /reset-epic-stories for a fresh /implement-epic run.", entity_type="story")`.

## Step 8: Verify

Print a summary:
- Worktrees removed
- State files and epic directory removed
- PRs closed (or left open)
- Local and remote branches deleted
- Taiga statuses, tags, and tasks reset
- Any items that couldn't be cleaned (with reasons)

The caller can now safely re-run `/implement-epic <epic-ref>` for these stories.
