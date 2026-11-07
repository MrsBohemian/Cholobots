---
name: parallel-fix-bugs
description: Fix all open bugs in parallel across isolated git worktrees - up to N bugs concurrently
argument-hint: "[N] [filter flags] [--yes]"
disable-model-invocation: true  # human-only: unattended sweep orchestrator - a model spawning this creates two owners for the same bugs
allowed-tools:
  - Agent
  - Bash(scripts/taiga-story.sh *)
  - Bash(python3 scripts/taiga_select_issues.py *)
  - Bash(python3 scripts/taiga_project_setup.py --check)
  - Bash(mkdir *)
  - Bash(cat *)
  - Bash(jq *)
  - Bash(git *)
  - Bash(env -u GH_TOKEN gh *)
  - mcp__taiga-py__*
  - AskUserQuestion
  - Read(//tmp/**)
  - Edit(//tmp/**)
  - Read
  - Grep
  - Glob
---

# Parallel Fix Bugs $ARGUMENTS

Fix all open bugs in parallel: select eligible bugs with the shared selector, then run up to `N` bugs at once, each in its own git worktree (`.claude/worktrees/bug-<ref>`) with an `Agent` spawn running the `fix-and-remediate-bug` composite skill. There is no separate orchestrator script and no container: the slot loop below is the orchestrator, and its state lives in `/tmp/parallel-fix-bugs.json`.

Conventions: `REPO_ROOT` is the main checkout (`git rev-parse --show-toplevel`); `WT=$REPO_ROOT/.claude/worktrees/bug-<ref>`. Branches are pushed to `upstream` (`MrsBohemian/Cholobots`) and PRs open inside it (`origin` is only a mirror); every `gh` call is `env -u GH_TOKEN gh ... --repo MrsBohemian/Cholobots`. Taiga names are in `.claude/skills/shared/taiga-project.md`.

## Step 1: Preflight, parse arguments and validate

Run `python3 scripts/taiga_project_setup.py --check` once. Exit 0: proceed. Exit 2: stop and print `Run: python3 scripts/taiga_project_setup.py --env-file scripts/taiga-auth.env --apply` for Jon (the project is missing statuses, the `Urgent` priority or pipeline tags this skill relies on).

If the first whitespace-separated token of `$ARGUMENTS` is an integer, use it as `N` (concurrency limit). Numbers elsewhere (for example `--refs 12` or `--milestone 3`) are filter values, never concurrency. Default to 3 if not provided.

`N` must be an integer in the range **1-5** (inclusive). Validate before doing anything else and report clearly if the value is invalid.

If `N` > 3, warn:
> "Warning: N=<N> may exceed account rate limits. Recommended max is 3."

If `/tmp/parallel-fix-bugs.json` exists with `phase` == `running`, this is a resume: skip Step 2, report the stored `options`, and go to Step 3 (the loop resumes from the state file; filters are not re-applied). Otherwise parse the remaining arguments per [`../shared/bug-sweep-options.md`](../shared/bug-sweep-options.md).

## Step 2: Select bugs

### 2a. Run the selector

Run `python3 scripts/taiga_select_issues.py --purpose fix <flags>`, then confirm the selection per the include. `Urgent` bugs are in `urgent`: they are excluded from the batch and reported, and the run continues with `selected`.

If `selected` is empty, report that (with the Urgent block) and stop.

### 2b. Check for already-merged PRs

For each bug in `selected`, check if a merged PR already fixes it:

1. Search GitHub for merged PRs referencing the bug's ref:
   ```bash
   env -u GH_TOKEN gh pr list --repo MrsBohemian/Cholobots --search "<ref>" --state merged --json number,title -q '.[].number'
   ```
   Also check for branch naming patterns:
   ```bash
   env -u GH_TOKEN gh pr list --repo MrsBohemian/Cholobots --search "head:fix/" --state merged --json number,headRefName -q '.[] | select(.headRefName | test("^fix/<ref>-")) | .number'
   ```
   Apply the claim rule (ref in the title as `fix(#<ref>)`/`(bug #<ref>)`, or a `Fixes`/`Closes`/`Resolves #<ref>` body line); a bare mention is not a claim.

2. If a merged PR is found:
   - Close the bug in Taiga: `update_issue(project=39, ref=<ref>, status="Closed")`
   - Log: "Bug #<ref> already fixed by PR #<number> - auto-closing in Taiga"
   - Remove the bug from the list

3. Only bugs **without** merged PRs proceed. If all bugs were already fixed, report that and stop.

### 2c. Write the state file

Write `/tmp/parallel-fix-bugs.json` (create `/tmp/parallel-fix-bugs/` if you want scratch files):

```json
{
  "phase": "running",
  "concurrency": <N>,
  "repoRoot": "<REPO_ROOT>",
  "options": <selector options object>,
  "rawFlags": "<raw flag string>",
  "urgentRefs": [<urgent refs>],
  "bugs": [
    {
      "ref": <ref>,
      "id": <issue_id>,
      "subject": "<subject>",
      "status": "queued",
      "branch": null,
      "prNumber": null,
      "outcome": null,
      "escalationReason": null,
      "worktreePath": null
    }
  ]
}
```

Include ALL bugs, in `selected` order. Bug status vocabulary: `queued` -> `launching` -> `active` -> `complete` / `escalated` / `error`. The queue is launched in array order, so the order is the only priority signal.

## Step 3: Slot loop

Print `INIT:<total>:<N>` (or `RESUMED:<active>:<queued>:<complete>:<escalated>:<errors>` on resume) and then run the loop until no bug is `queued`, `launching` or `active`.

**Resume handling.** Before the first launch of a resumed run, reconcile each bug still marked `launching` or `active`: its agent died with the previous session. Read `/tmp/fix-and-remediate-<ref>.json` if it exists. A terminal phase (`complete`, `already-closed`, `escalated`) is applied as in the completion handling below. Anything else (or no file) sets the bug back to `queued`, keeping its `worktreePath` and `branch`; the worker resumes on its existing `fix/<ref>-*` branch.

**Launch.** While fewer than `N` bugs are `active` and some are `queued`, take the first queued bug and:

1. Set `status = "launching"`, print `LAUNCHING:<ref>:<subject>`.
2. Set the Taiga issue to `In progress`: `update_issue(project=39, ref=<ref>, status="In progress")`.
3. Create the worktree (one at a time, never concurrently), detached at the upstream tip; the worker creates the `fix/<ref>-<slug>` branch inside it:
   ```bash
   git fetch upstream
   git worktree add --detach "$REPO_ROOT/.claude/worktrees/bug-<ref>" upstream/main
   ```
   Skip the add if the worktree exists (resume). Record `worktreePath`. On failure set `status = "error"`, `escalationReason = "worktree creation failed"`, print `LAUNCH_FAIL:<ref>:<reason>` and go on to the next queued bug.
4. Spawn the worker with the `Agent` tool:
   - **Agent type**: `senior-software-developer`
   - **Prompt**:
     ```
     Fix Taiga bug #<ref> (<subject>) in project 39 by running /fix-and-remediate-bug with the argument '<ref>'.

     Working directory: <WT> (a git worktree of this repo based on upstream/main). Do all file edits and git commands there (use absolute paths or `git -C <WT>`). Main checkout: <REPO_ROOT>.
     FORBIDDEN: cd to the main checkout, checking out other branches, `git reset --hard`, `git stash` (the stash is shared across worktrees).
     The skills and helper scripts live in the main checkout and may be absent from the worktree: read skill text from <REPO_ROOT>/.claude/skills/ and run helpers as `python3 <REPO_ROOT>/scripts/...` with <WT> as the working directory.
     Never run `python cholobots.py`. Follow .claude/skills/shared/synchronous-worker-contract.md: foreground Bash and Agent calls only, return only after the state file /tmp/fix-and-remediate-<ref>.json is in a terminal phase.

     Return: the final phase, the PR number and URL, the branch name, and the escalation reason if any.
     ```
   Launch every free slot's agent in a single message so they run concurrently. Set `status = "active"` and print `LAUNCH_OK:<ref>` once spawned.

**Completion handling.** Workers finish independently and you are notified per completion. As each returns (do not wait for the whole batch):

1. Read `/tmp/fix-and-remediate-<ref>.json` (the authority; the agent's report is only a cross-check). Map the outcome:
   - `complete` -> `status = "complete"`, `outcome = "complete"`, store `prNumber` and `branch`
   - `already-closed` -> `status = "complete"`, `outcome = "already-closed"`, no PR
   - `escalated` -> `status = "escalated"`, store `escalationReason`
   - missing file, non-terminal phase, or an agent error -> `status = "escalated"`, `escalationReason = "worker ended without a terminal phase (<phase>)"`
2. The worker normally does its own Taiga finalization (`Ready for review` and assignee, or `Blocked`, the `escalation-needed` tag and assignee). If it ended without a terminal phase, do the escalation yourself: re-read the issue, `update_issue(project=39, ref=<ref>, status="Blocked", tags=[<existing...>, "escalation-needed"])`, and `assign_item(project=39, ref=<ref>, username="Jon Roberts", entity_type="issue")`.
3. Remove the worktree: `git worktree remove "$WT"` (no `--force`; the branch and PR stay). If it refuses because the tree is dirty, leave it, keep `worktreePath` and list it in the report.
4. Print `COMPLETED:<ref>:<outcome>:<pr>`, update the state file, then run **Launch** again to fill the freed slot with the next queued bug.

Print `WAITING:<active>:<queued>` whenever you are idle waiting for a completion notification. Never launch more than `N` at once. Never stop the whole run for one bug.

When no bug is `queued`, `launching` or `active`: set `phase = "complete"` and print `DONE:<complete>:<escalated>:<errors>`.

## Step 4: Report results

1. Read `/tmp/parallel-fix-bugs.json` for final counts (completed, escalated, errors).
2. Present a summary, starting with the Urgent block from the state's `urgentRefs` (each `#<ref> <subject>: Run /fix-single-bug <ref> now.`):
   - Total bugs processed
   - Bugs completed (with PR numbers and links)
   - Bugs escalated (with reasons and Taiga links)
   - Bugs errored (with reasons)
   - Worktrees left in place (with paths)
3. Remind the user to review PRs and handle escalations.

## Notes

- **Resume**: re-invoke with the same N; filters are not re-applied on resume. The state file is the only resume record.
- **Rate limits**: N=2-3 is safe; N=4+ will throttle.
- **Existing sequential skill**: `/fully-fix-bugs` remains available for sequential processing (one bug at a time). Use this parallel skill when you have multiple bugs to fix concurrently.
- **No CI yet** (story #35): workers treat "no checks configured" as a pass-with-note, so remediation mostly waits on review comments.
- **Merge train**: After all bugs are fixed, use `/merge-train <PR1> <PR2> ...` to sequentially rebase and merge the resulting PRs.
