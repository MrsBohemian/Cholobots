---
name: remediate-pr
description: Monitor a PR for CI stability, auto-fix build failures and resolve review comments, escalate after repeated failures
argument-hint: "<pr-number> [--taiga-id <ref>] [--taiga-type story|issue] [--max-failures 5] [--resume <state-file>]"
allowed-tools:
  - Agent
  - Bash(python3 scripts/remediate_pr.py *)
  - Bash(env -u GH_TOKEN gh *)
  - Bash(git *)
  - mcp__taiga-py__update_story
  - mcp__taiga-py__update_issue
  - mcp__taiga-py__assign_item
  - mcp__taiga-py__get_story
  - mcp__taiga-py__get_issue
  - mcp__taiga-py__add_comment
  - Read(//tmp/**)
  - Read
  - Grep
  - Glob
---

# Remediate PR — $ARGUMENTS

This skill is a thin wrapper around `scripts/remediate_pr.py`. The Python script handles all deterministic operations (stability monitoring via `monitor-pr-stability.sh`, state management, failure counting, action-request writing). This wrapper only invokes Claude agents for build fixes and comment resolution, and handles Taiga escalation (which the script cannot do since it has no MCP access).

The target repo is `MrsBohemian/Cholobots`; every `gh` call here is prefixed `env -u GH_TOKEN` (the script already does this internally). The PR branch is pushed to `upstream`, the same repo the PR is in.

**No CI yet.** Cholobots has no GitHub Actions workflow that builds or tests (story #35 adds one; `.github/workflows/taiga-sync.yml` only syncs Taiga after a merge). When the script finds no CI checks configured it treats the checks as passed, logs a `NOTE: no CI checks configured` line, records `"note": "no CI checks configured"` in its result, and moves on to review comments and the Codex review. That is stable-with-note, not a failure and not an endless wait. Report the note to the caller so they know "STABLE" did not include a CI verdict.

## Step 1: Parse arguments

Parse `$ARGUMENTS`:
- First word: **pr-number** (required, positive integer), unless `--resume` is given
- `--taiga-id <ref>`: Taiga item ref for escalation tagging (optional). The script stores it as `taigaId`; the workflow tools are ref-based, so treat the stored value as the ref.
- `--taiga-type story|issue`: Taiga item type — determines which MCP tools to use for escalation (optional, required if `--taiga-id` is set)
- `--max-failures <n>`: Build failure threshold before escalation (optional, default: 5)
- `--resume <state-file>`: resume an existing run (normally only used internally in Step 4)

## Step 2: Invoke the script

Run the remediation script with the parsed arguments:

```bash
python3 scripts/remediate_pr.py <pr-number> [--max-failures N] [--taiga-id REF] [--taiga-type story|issue]
```

Capture the exit code.

## Step 3: Handle the result

Check the exit code from the script invocation:

### Exit 0 — STABLE

The PR is CI-stable (or has no CI configured, see above) and has no unresolved review comments. Report **STABLE** to the caller, with the "no CI checks configured" note if the script logged one, and stop.

### Exit 13 — Needs rebase

The PR has merge conflicts and cannot be merged without a rebase.

1. **If `rebaseAttempted` is true** (a boolean you track in this conversation, initially false, scoped to this skill invocation), report **NEEDS_REBASE** to the caller and stop. (This guards against a rebase that completed but did not clear the conflicts, and against unbounded rebase loops while the base branch advances.)
2. Get the PR's target branch: `env -u GH_TOKEN gh pr view <pr-number> --repo MrsBohemian/Cholobots --json baseRefName -q .baseRefName` (PRs may target an epic integration branch, not always main).
3. Spawn a `remediation-developer` agent to rebase the branch:
   - `subagent_type: "remediation-developer"`
   - Prompt: `"Rebase PR #<pr-number> onto <base>. Follow the rebase-branch skill instructions with arguments '<pr-number> <base>'. Work only in the worktree you were given; never cd to the main checkout, check out other branches, git reset --hard, or git stash. End your final message with exactly one line: REBASE_RESULT: SUCCEEDED or REBASE_RESULT: ESCALATED."`
4. When the agent returns, set `rebaseAttempted` to true and check the `REBASE_RESULT:` line in its output:
   - **ESCALATED**, or the line is missing (conflict too complex — `rebase-branch` posts the PR comment and updates Taiga itself where applicable): report **NEEDS_REBASE** to the caller and stop.
   - **SUCCEEDED**: go to **Step 4** (resume). The resume re-runs the stability check against the rebased branch.

### Exit 10 — Build fix needed

The script detected failing CI checks and needs agent intervention. (This cannot happen until story #35 adds CI; the path is kept so it works the day CI lands.)

1. Read the action-request JSON from `/tmp/remediate-pr-<pr-number>-action.json`. It contains:
   - `action` — `"fix_build"`
   - `pr.number` — the PR number
   - `details.failing_checks` — list of failing check names
   - `state_file` — path to the state file for resume
2. Spawn a `remediation-developer` agent to fix the build:
   - `subagent_type: "remediation-developer"`
   - Prompt: `"Fix the build failure on PR #<pr-number>. Follow the fix-build-failure skill instructions with argument 'pr-<pr-number>'. Work only in the worktree you were given; never cd to the main checkout, check out other branches, git reset --hard, or git stash."`
3. When the agent returns, go to **Step 4** (resume).

### Exit 11 — Comments found

The script detected unresolved review comments that need resolution.

1. Read the action-request JSON from `/tmp/remediate-pr-<pr-number>-action.json`. It contains:
   - `action` — `"resolve_comments"`
   - `pr.number` — the PR number
   - `details.comment_count` — number of unresolved comment threads
   - `state_file` — path to the state file for resume
2. Spawn a `remediation-developer` agent to resolve comments:
   - `subagent_type: "remediation-developer"`
   - Prompt: `"Resolve comments on PR #<pr-number>. Follow the pr-comments-resolve skill instructions with argument '<pr-number>'. Work only in the worktree you were given; never cd to the main checkout, check out other branches, git reset --hard, or git stash."`
3. When the agent returns, go to **Step 4** (resume).

### Exit 12 — ESCALATED

The script's build failure count reached the threshold. Handle escalation: status `Blocked`, tag `escalation-needed` (keeping the item's other tags), assignee `"Jon Roberts"` (status and tag names are in `.claude/skills/shared/taiga-project.md`; the workflow server resolves assignees by full name, never by Taiga username).

1. Read the state file at `/tmp/remediate-pr-<pr-number>.json` to get `taigaId` and `taigaType`.
2. **If `taigaId` and `taigaType` are present:**
   - **If `taigaType` is `story`:**
     - Fetch the story to get existing tags: `get_story(project=39, ref=<taigaId>)`
     - `update_story(project=39, ref=<taigaId>, status="Blocked", tags=[<existing_tags...>, "escalation-needed"])` (tags are replaced wholesale, so always write back the existing list)
     - `assign_item(project=39, ref=<taigaId>, username="Jon Roberts", entity_type="story")`
     - `add_comment(project=39, ref=<taigaId>, entity_type="story", text="Escalated: PR #<pr-number> hit the build-failure threshold (previous status: <status>). Manual intervention needed.")`
   - **If `taigaType` is `issue`:**
     - Fetch the issue to get existing tags: `get_issue(project=39, ref=<taigaId>)`
     - `update_issue(project=39, ref=<taigaId>, status="Blocked", tags=[<existing_tags...>, "escalation-needed"])`
     - `assign_item(project=39, ref=<taigaId>, username="Jon Roberts", entity_type="issue")`
     - `add_comment(project=39, ref=<taigaId>, entity_type="issue", text="<same text>")`
3. **Post PR comment:**
   ```
   env -u GH_TOKEN gh pr comment <pr-number> --repo MrsBohemian/Cholobots --body "Escalating to @bigjonroberts — build failure threshold reached. Manual intervention needed."
   ```
4. Report **ESCALATED** to the caller and stop.

### Exit 14 — PENDING

CI has not reached a verdict yet, or the Codex review is still running; the script returned within its bounded wait window. (A repo with no CI configured does not produce this exit for the checks; only a running Codex review or in-flight checks do.)

1. Immediately re-invoke the script with `--resume` (see **Step 4**) and go back to **Step 3** to handle the new exit code.

### Exit 1 — Error

The script hit an unexpected error. Read stderr for details and report the error to the caller. Stop.

## Step 4: Resume the script

After agent intervention (build fix or comment resolution), re-invoke the script with `--resume`:

```bash
python3 scripts/remediate_pr.py --resume /tmp/remediate-pr-<pr-number>.json
```

On resume from an `awaiting_fix` or `awaiting_comment_resolution` phase the script first requests a Codex review pass via `scripts/codex-review-pr.sh` (it owns the per-PR pass cap and same-head guard), then runs the stability check. A failing or absent review script is logged and never fatal.

Go back to **Step 3** to handle the new exit code.

## Return values

This skill communicates its result via the final line of output to the caller:

- **`STABLE`** — PR is CI-stable (or has no CI configured, noted) and all review comments have been resolved
- **`ESCALATED`** — Build failures exceeded the threshold; Taiga item set to `Blocked`, tagged `escalation-needed`, assigned to Jon Roberts, and Jon notified on the PR
- **`NEEDS_REBASE`** — a rebase was needed and could not be completed automatically (escalated conflict, or conflicts persisted after a rebase attempt)

## What this skill does NOT do

- Does NOT pre-check `mergeStateStatus` — it rebases only in reaction to the script's exit 13; callers may still pre-check before invoking
- Does NOT merge — the caller decides when and how to merge after receiving STABLE (`env -u GH_TOKEN gh pr merge <n> --repo MrsBohemian/Cholobots --squash`)
- Does NOT start or stop the bot — it never runs `python cholobots.py`
- Does NOT update Taiga story/issue status except to set `Blocked` (with the tag and assignee) on escalation
