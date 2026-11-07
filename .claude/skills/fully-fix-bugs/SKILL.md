---
name: fully-fix-bugs
description: Autonomously fix all open bugs end-to-end - sequential fixing in git worktrees, PR monitoring, remediation, Taiga updates
argument-hint: "[filter flags] [--yes]"
disable-model-invocation: true  # human-only: unattended sweep orchestrator - a model spawning this creates two owners for the same bugs
allowed-tools:
  - Agent
  - Bash(scripts/taiga-story.sh *)
  - Bash(python3 scripts/taiga_select_issues.py *)
  - Bash(python3 scripts/taiga_project_setup.py --check)
  - Bash(python3 scripts/remediate_pr.py *)
  - Bash(scripts/monitor-pr-stability.sh *)
  - Bash(jq *)
  - Bash(grep *)
  - Bash(tail *)
  - Bash(env -u GH_TOKEN gh *)
  - Bash(git *)
  - Bash(sleep *)
  - mcp__taiga-py__*
  - AskUserQuestion
  - Read(//tmp/**)
  - Edit(//tmp/**)
  - Read
  - Grep
  - Glob
---

# Fully Fix Bugs

Autonomously fix all open bugs in Taiga project 39 (`cholo-bots`): sequential bug fixing in isolated git worktrees, PR creation, monitoring, remediation via `scripts/remediate_pr.py`, and Taiga updates.

Each bug runs in its own git worktree, `.claude/worktrees/bug-<ref>`, with one synchronous `Agent` spawn doing the work in it. The worktree stays from fix through monitoring and remediation, then is removed when the bug is complete or escalated. There are no containers.

Conventions used throughout: `REPO_ROOT` is the main checkout (`git rev-parse --show-toplevel`, run once at the start); `WT=$REPO_ROOT/.claude/worktrees/bug-<ref>`. Branches are pushed to `upstream` (`MrsBohemian/Cholobots`) and PRs open inside it (`origin` is only a mirror); every `gh` call is `env -u GH_TOKEN gh ... --repo MrsBohemian/Cholobots`. Taiga names are in `.claude/skills/shared/taiga-project.md`. Run `scripts/` helpers (`remediate_pr.py`, `monitor-pr-stability.sh`) from `REPO_ROOT`.

## Step 0: Preflight and parse arguments

Run `python3 scripts/taiga_project_setup.py --check` once. Exit 0: proceed. Exit 2: stop and print `Run: python3 scripts/taiga_project_setup.py --env-file scripts/taiga-auth.env --apply` for Jon (the project is missing statuses, the `Urgent` priority or pipeline tags this skill relies on).

Parse arguments per [`../shared/bug-sweep-options.md`](../shared/bug-sweep-options.md) (this skill has no positional argument).

## Step 1: Initialize

### 1a. Check for existing state file

Check if `/tmp/fully-fix-bugs.json` exists:
- If it exists and `phase` is not `"complete"`:
  - Read the state file
  - Report current state (how many bugs total, how many done, current phase)
  - Resume from the appropriate step based on `currentBugIndex` and the current bug's `status`:
    - `"pending"` -> Step 3 (fix the bug)
    - `"fixing"` -> Step 3 (re-fix, the worktree may be gone or half-done)
    - `"pr-created"` -> Step 3b (rebase check)
    - `"monitoring"` -> Step 4 (monitor PR)
    - `"stable"` -> Step 5 (update Taiga, advance to next)
    - `"escalated"` -> Step 5 (advance to next bug)
  - **Worktree recovery on resume:** before spawning any agent for the current bug, restore the path from state: `WT=$(jq -r ".bugs[.currentBugIndex].worktreePath" /tmp/fully-fix-bugs.json)`. Check `git worktree list`. If the worktree is gone but the bug has a `branch`, recreate it on that branch (`git worktree add "$WT" <branch>`). If neither exists, start from Step 3 (the worktree is created there).
  - Reuse `options`/`urgentRefs` from state; never re-select or re-prompt
  - Skip Steps 1b and 1c
- If it doesn't exist or `phase` is `"complete"`, proceed with fresh initialization

### 1b. Select bugs

Run `python3 scripts/taiga_select_issues.py --purpose fix <flags>`, then confirm the selection per the include. `Urgent` bugs are in `urgent`: they are excluded from the batch and reported, and the run continues with `selected`. If `selected` is empty, report that (with the Urgent block) and stop.

Status names (`In progress`, `Ready for review`, `Blocked`, `Closed`), the assignee `"Jon Roberts"` (full name) and the `escalation-needed` tag are resolved by name; no id lookup is needed.

### 1b-2. Check for already-merged PRs

For each bug in `selected` from step 1b, check if a merged PR already fixes it:

1. Search GitHub for merged PRs referencing the bug's ref:
   ```bash
   env -u GH_TOKEN gh pr list --repo MrsBohemian/Cholobots --search "<ref>" --state merged --json number,title -q '.[].number'
   ```
   Also check for branch naming patterns (fix-single-bug uses `fix/<ref>-<slug>`):
   ```bash
   env -u GH_TOKEN gh pr list --repo MrsBohemian/Cholobots --search "head:fix/" --state merged --json number,headRefName -q '.[] | select(.headRefName | test("^fix/<ref>-")) | .number'
   ```
   Apply the claim rule (ref in the title as `fix(#<ref>)`/`(bug #<ref>)`, or a `Fixes`/`Closes`/`Resolves #<ref>` body line); a bare mention is not a claim.

2. If a merged PR is found:
   - Close the bug in Taiga: `update_issue(project=39, ref=<ref>, status="Closed")`
   - Log: "Bug #<ref> already fixed by PR #<number> - auto-closing in Taiga"
   - Remove the bug from the list

3. Only bugs **without** merged PRs proceed to the state file in step 1c. If all bugs were already fixed, report that and stop.

### 1c. Initialize state file

Write the initial state to `/tmp/fully-fix-bugs.json`:

```json
{
  "phase": "fixing",
  "currentBugIndex": 0,
  "repoRoot": "<REPO_ROOT>",
  "options": <selector options object>,
  "rawFlags": "<raw flag string>",
  "urgentRefs": [<urgent refs>],
  "bugs": [
    {
      "ref": <ref>,
      "id": <issue_id>,
      "subject": "<subject>",
      "status": "pending",
      "branch": null,
      "prNumber": null,
      "buildFailureCount": 0,
      "escalationReason": null,
      "worktreePath": null
    }
  ]
}
```

Include ALL bugs from `selected` in the array, in `selected` order. (`id` is Taiga's internal id; every call here uses `ref`, and the REST scripts resolve the id themselves.)

## Step 2: Bug loop

Process bugs sequentially starting from `currentBugIndex`. For each bug:

1. If `currentBugIndex >= bugs.length`, go to Step 6 (done).
2. Check the current bug's `status` and route accordingly:
   - `"complete"` or `"escalated"` -> increment `currentBugIndex` and continue to the next bug
   - `"pending"` or `"fixing"` -> proceed to Step 3 (fix the bug)
   - `"pr-created"` -> proceed to Step 3b (rebase check)
   - `"monitoring"` -> proceed to Step 4 (monitor PR)
   - `"stable"` -> proceed to Step 5 (finalize and advance)

## Step 3: Fix the bug

1. Update the current bug's `status` to `"fixing"` in the state file.

2. Update the Taiga issue to `In progress`: `update_issue(project=39, ref=<ref>, status="In progress")`

3. **Create the worktree** from the current upstream tip (detached; `/fix-single-bug` creates the `fix/<ref>-<slug>` branch inside it):
   ```bash
   git fetch upstream
   git worktree add --detach "$REPO_ROOT/.claude/worktrees/bug-<ref>" upstream/main
   ```
   Skip the add if the worktree already exists (resume). Record `worktreePath` in the state file. If this fails, set `status = "escalated"`, `escalationReason = "worktree creation failed"` and go to Step 5.

4. **Spawn the fix agent** (synchronous, foreground; wait for its return):
   - **Agent type**: `senior-software-developer`
   - **Prompt**:
     ```
     Fix Taiga bug #<ref> (<subject>) in project 39 by running /fix-single-bug with the argument '<ref> --mark-done-on-merge'.

     Working directory: <WT> (a git worktree of this repo based on upstream/main). Do all file edits and git commands there (use absolute paths or `git -C <WT>`). Main checkout: <REPO_ROOT>.
     FORBIDDEN: cd to the main checkout, checking out other branches, `git reset --hard`, `git stash` (the stash is shared across worktrees).
     The skills and helper scripts live in the main checkout and may be absent from the worktree: read skill text from <REPO_ROOT>/.claude/skills/ and run helpers as `python3 <REPO_ROOT>/scripts/...` with <WT> as the working directory.
     Never run `python cholobots.py`. Follow .claude/skills/shared/synchronous-worker-contract.md: foreground Bash and Agent calls only, return only after the work is verified complete.

     Finish by returning: the PR number and URL, the branch name, the Codex review line, and the check results. If you cannot produce a PR, return `NO_PR` and the reason.
     ```

5. **Extract the PR number from the agent's report**, then verify it exists on the upstream repo:
   ```bash
   env -u GH_TOKEN gh pr view <number> --repo MrsBohemian/Cholobots --json headRefName,state -q '.headRefName + " " + .state'
   ```
   If the report names no PR, look for one from the worktree's branch:
   ```bash
   BR=$(git -C "$WT" branch --show-current)
   env -u GH_TOKEN gh pr list --repo MrsBohemian/Cholobots --state open --json number,headRefName -q ".[] | select(.headRefName==\"$BR\") | .number"
   ```

6. **Update state file:**
   - If a valid PR was found: set `status = "pr-created"`, `prNumber = <number>`, `branch = <headRefName>`
   - If no PR was created (agent failed or returned `NO_PR`): set `status = "escalated"`, `escalationReason = "agent failed to fix"`, proceed to Step 5

7. Proceed to Step 3b.

## Step 3b: Rebase check

Before monitoring, check if the branch needs a rebase onto `main`:

1. Run: `env -u GH_TOKEN gh pr view <prNumber> --repo MrsBohemian/Cholobots --json mergeStateStatus -q .mergeStateStatus`
2. If result is `"BEHIND"` or `"DIRTY"`:
   a. Spawn an agent (`remediation-developer`, synchronous) in the worktree to run `/rebase-branch <prNumber>`, with the same working-directory and FORBIDDEN lines as Step 3.4.
   b. Check its report:
      - If the rebase was escalated (conflict too complex), set bug `status = "escalated"`, `escalationReason = "rebase conflict escalated"`, proceed to Step 5.
      - If the rebase succeeded, reset `buildFailureCount = 0` for this bug and proceed to Step 4.
3. Otherwise (`"CLEAN"`, `"HAS_HOOKS"`, `"UNSTABLE"` with no checks configured), proceed to Step 4.

## Step 4: PR monitoring and remediation via remediate_pr.py

This step delegates to `scripts/remediate_pr.py` for deterministic CI monitoring and state management. The script runs in the orchestrator session (from `REPO_ROOT`). When it requests agent intervention (exit codes 10/11/13), the fix runs in an `Agent` spawn working in the bug's worktree.

There is no CI configured yet (story #35). The script treats "no checks configured" as a pass-with-note; do not treat it as a failure and do not wait for checks that will never arrive.

1. Update bug `status` to `"monitoring"` in state file.

2. **Run the remediation script:**
   ```bash
   python3 scripts/remediate_pr.py <prNumber> --max-failures 5 --taiga-id <ref> --taiga-type issue
   ```
   Run it as a foreground Bash call with `timeout: 600000`. Capture the exit code.

3. **Handle the exit code:**

### Exit 0: STABLE

The PR is CI-stable (or has no checks configured) and has no unresolved review comments. Update bug `status` to `"stable"` and proceed to Step 5.

### Exit 10: Build fix needed

The script detected failing CI checks and needs agent intervention.

1. Read the action-request JSON from `/tmp/remediate-pr-<prNumber>-action.json` to get details (failing check names, etc.).
2. Spawn an agent (`remediation-developer`, synchronous) in the worktree to run `/fix-build-failure pr-<prNumber>`, with the working-directory and FORBIDDEN lines from Step 3.4.
3. When it returns, **resume the script** and go back to step 4.3 to handle the new exit code:
   ```bash
   python3 scripts/remediate_pr.py --resume /tmp/remediate-pr-<prNumber>.json
   ```

### Exit 11: Comments found

The script detected unresolved review comments that need resolution.

1. Read the action-request JSON from `/tmp/remediate-pr-<prNumber>-action.json` to get details (comment count, etc.).
2. Spawn an agent (`remediation-developer`, synchronous) in the worktree to run `/pr-comments-resolve <prNumber>`, with the working-directory and FORBIDDEN lines from Step 3.4.
3. When it returns, **resume the script** and go back to step 4.3 to handle the new exit code:
   ```bash
   python3 scripts/remediate_pr.py --resume /tmp/remediate-pr-<prNumber>.json
   ```

### Exit 12: ESCALATED

The script's build failure count reached the threshold (5+ failures). Set bug `status = "escalated"`, `escalationReason = "5+ build failures"`. Proceed to Step 5.

### Exit 13: NEEDS_REBASE

Run the rebase agent from Step 3b, then resume the script. If the rebase escalates, set `status = "escalated"`, `escalationReason = "rebase conflict escalated"` and proceed to Step 5.

### Exit 14: PENDING

CI or the Codex review has not reached a verdict within the script's bounded wait. Resume immediately: `python3 scripts/remediate_pr.py --resume /tmp/remediate-pr-<prNumber>.json`, and handle the new exit code.

### Exit 1: Error

The script hit an unexpected error. Set bug `status = "escalated"`, `escalationReason = "remediation script error"`. Proceed to Step 5.

## Step 5: Finalize current bug and advance

Based on the current bug's `status`:

### If `"stable"`

1. Make sure the Taiga issue is `Ready for review` (`/fix-single-bug` sets it; set it again if missing): `update_issue(project=39, ref=<ref>, status="Ready for review")`
2. Assign the issue to Jon: `assign_item(project=39, ref=<ref>, username="Jon Roberts", entity_type="issue")`
3. Update bug `status` to `"complete"` in state file

> **Note:** Bug closure (Taiga status -> Closed) happens when the PR is merged, via the `taiga-sync` GitHub Actions workflow, because this skill passes `--mark-done-on-merge` and `fix-single-bug` labels eligible PRs `mark-done-on-merge`. A PR without the label (plan not eligible) leaves the bug open for Jon. This skill only needs to leave the bug in `Ready for review` and assigned.

### If `"escalated"`

1. Re-read the issue, then `update_issue(project=39, ref=<ref>, status="Blocked", tags=[<existing_tags...>, "escalation-needed"])` (`tags` replaces the whole list).
2. Assign the issue to Jon: `assign_item(project=39, ref=<ref>, username="Jon Roberts", entity_type="issue")`
3. Post a comment on the PR (if one exists): `env -u GH_TOKEN gh pr comment <prNumber> --repo MrsBohemian/Cholobots --body "Escalating to @bigjonroberts: <escalationReason>. Manual intervention needed."`

### In both cases:

1. **Remove the worktree:**
   ```bash
   git worktree remove "$WT"
   ```
   Run it without `--force` (the branch and PR are untouched). If it refuses because the tree is dirty, leave the worktree in place, keep `worktreePath` in the state file and list it in the final report so nothing unpushed is lost.

2. Increment `currentBugIndex` in state file
3. Go back to Step 2 (bug loop)

## Step 6: Complete

1. Update state file: `phase = "complete"`
2. Report summary, starting with the Urgent block (`urgentRefs`, each with `Run /fix-single-bug <ref> now.`):
   - Total bugs processed
   - Bugs completed (with PR numbers and links)
   - Bugs escalated (with reasons)
   - Worktrees left in place (with paths)
   - Bugs remaining (if any were added to Taiga during the run: these are not tracked)

## Error handling

- If worktree creation fails, log the error, set the current bug to `status = "escalated"`, `escalationReason = "worktree creation failed"`, and proceed to Step 5 to advance to the next bug.
- If an agent spawn fails or returns an error, log it, set the current bug to `status = "escalated"`, `escalationReason = "agent execution failed"`, and proceed to Step 5.
- The state file at `/tmp/fully-fix-bugs.json` enables resumability: if the conversation is interrupted, re-invoking the skill reads the state file and continues from the current bug and phase.
- **Worktree recovery on resume:** when resuming, always verify the worktree exists before spawning an agent into it; recreate it if it is gone.
- **Always remove worktrees** in Step 5, regardless of whether the bug was stable or escalated, unless removal is refused because of uncommitted work.
- **Never stop the entire run for a single bug failure**: escalate the bug (`Blocked`, `escalation-needed` tag, assigned to Jon) and move on.
