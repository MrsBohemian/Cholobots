---
name: parallel-fix-bugs
description: Fix all open bugs in parallel - up to N bugs concurrently. N > 1 runs each bug in an isolated devcontainer (default); N = 1 or --worktrees runs each in a git worktree slot
argument-hint: "[N] [--worktrees] [filter flags] [--yes]"
disable-model-invocation: true  # human-only: unattended sweep orchestrator - a model spawning this creates two owners for the same bugs
allowed-tools:
  - Agent
  - Bash(scripts/taiga-story.sh *)
  - Bash(scripts/supabase-local.sh *)
  - Bash(python3 scripts/devcontainer/*)
  - Bash(nohup python3 scripts/devcontainer/*)
  - Bash(scripts/devcontainer/*)
  - Bash(docker info*)
  - Bash(ps -p *)
  - Bash(tail *)
  - Bash(kill *)
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

Fix all open bugs in parallel: select eligible bugs with the shared selector (Steps 1 to 2, shared by both modes), then run up to `N` bugs at once with the `fix-and-remediate-bug` composite skill. Two modes:

- **Container mode** (`N` > 1 without `--worktrees`, so a plain `/parallel-fix-bugs` is container mode): the skill hands the bugs to the Python state machine `scripts/devcontainer/orchestrate_bugs.py` (Step 3'). It runs up to `N` bugs at once, each in its own devcontainer clone with its own Docker-in-Docker daemon and local Supabase stack, and starts the in-container agent through `scripts/devcontainer/exec-skill.sh`. Nothing touches a hosted Supabase project.
- **Worktree mode** (`N` = 1, or `--worktrees`): the skill itself is the orchestrator (Step 3). Each bug gets a git worktree (`.claude/worktrees/bug-<ref>`) and an `Agent` spawn running `fix-and-remediate-bug`; the state lives in `/tmp/parallel-fix-bugs.json`. Each slot also runs its own local Supabase stack (`scripts/supabase-local.sh`).

Both modes write `/tmp/parallel-fix-bugs.json`, with a `mode` key, so a resume never mixes them. Nothing is merged; `/merge-train` does that afterwards.

## Choosing the mode

- **`N` > 1** (default 3): container mode. Each bug is isolated on its own clone, so it suits bugs that touch unrelated code. Each container needs about 1 to 1.5 GB RAM plus its Supabase stack, and the first launch is slow (image pull, stack start).
- **`N` = 1**: worktree mode, one bug at a time, no image pull, no Docker-in-Docker.
- **`N` > 1 with `--worktrees`**: the worktree slot loop with several slots. Pick it on a host without Docker headroom.

Bugs in the same bot module (`bots/metichebot.py`, `bots/chismebot.py`, `bots/crudobot.py`) tend to conflict whichever mode runs them; `/merge-train` rebases the PRs one by one afterwards.

Conventions: `REPO_ROOT` is the main checkout (`git rev-parse --show-toplevel`); in worktree mode `WT=$REPO_ROOT/.claude/worktrees/bug-<ref>`. Branches are pushed to `upstream` (`MrsBohemian/Cholobots`) and PRs open inside it (`origin` is only a mirror); every `gh` call is `env -u GH_TOKEN gh ... --repo MrsBohemian/Cholobots`. Taiga names are in `.claude/skills/shared/taiga-project.md`.

## Step 1: Preflight, parse arguments and validate

Run `python3 scripts/taiga_project_setup.py --check` once. Exit 0: proceed. Exit 2: stop and print `Run: (eval "$(dv export project/taiga-growthgarage-jonroberts)" && python3 scripts/taiga_project_setup.py --apply)` for Jon (the project is missing statuses, the `Urgent` priority or pipeline tags this skill relies on).

If the first whitespace-separated token of `$ARGUMENTS` is an integer, use it as `N` (concurrency limit). Numbers elsewhere (for example `--refs 12` or `--milestone 3`) are filter values, never concurrency. Default to 3 if not provided.

`N` must be an integer in the range **1-5** (inclusive). Validate before doing anything else and report clearly if the value is invalid. The Python orchestrator enforces the same limit.

The run's **mode** is `worktree` when `N` = 1 or the flag `--worktrees` is present, otherwise `container`. `--worktrees` is this skill's own flag: strip it from the arguments before the remaining flags go to the selector, which would reject it.

If `N` > 3, warn:
> "Warning: N=<N> may exceed account rate limits. Recommended max is 3."

**Substrate check, container mode only.** `docker info` must succeed; the five scripts `scripts/devcontainer/{launch,prepare-clone,exec,exec-skill,teardown}.sh` must exist and be executable; and `npx --yes @devcontainers/cli@<DEVCONTAINER_MIN_VERSION from scripts/devcontainer/launch.sh> --version` must succeed. If anything is missing, stop, name it, and suggest re-invoking with `--worktrees`. Never fall back to worktree mode silently. Both modes need `git remote get-url upstream` to name `MrsBohemian/Cholobots`.

**Resume check.** If `/tmp/parallel-fix-bugs.json` exists with `phase` == `running`, this is a resume. Read its `mode` first (a file with no `mode` predates the key and is a container run). If it differs from this invocation's mode, stop and say: `/tmp/parallel-fix-bugs.json was written by <mode> mode; re-invoke with the same mode, or delete the file`. Otherwise skip Step 2, report the stored options (worktree mode: the state file's `options`; container mode: `/tmp/parallel-fix-bugs/sweep-options.json`), and go to Step 3 (worktree mode) or Step 3' (container mode, which re-invokes the orchestrator; it resumes from its own state and ignores the input file). Filters are not re-applied on resume. Otherwise parse the remaining arguments per [`../shared/bug-sweep-options.md`](../shared/bug-sweep-options.md).

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

### 2c. Write the state file (worktree mode only)

In container mode skip this: write the input files in Step 3' instead, and the orchestrator owns `/tmp/parallel-fix-bugs.json` itself.

Write `/tmp/parallel-fix-bugs.json` (create `/tmp/parallel-fix-bugs/` if you want scratch files):

```json
{
  "mode": "worktree",
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

## Step 3 (worktree mode): slot loop

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
   Then start the worktree's own Supabase stack (its own Bash call, run from `REPO_ROOT`, the main checkout): `scripts/supabase-local.sh start "$WT" >/dev/null`. A base without `supabase/config.toml` exits 0 with a note. A non-zero exit is a warning only: print `STACK_WARN:<ref>:<first stderr line>` and continue. Each active slot's stack costs about 1 GB RAM on top of its worker.
4. Spawn the worker with the `Agent` tool:
   - **Agent type**: `senior-software-developer`
   - **Prompt**:
     ```
     Fix Taiga bug #<ref> (<subject>) in project 39 by running /fix-and-remediate-bug with the argument '<ref>'.

     Working directory: <WT> (a git worktree of this repo based on upstream/main). Do all file edits and git commands there (use absolute paths or `git -C <WT>`). Main checkout: <REPO_ROOT>.
     FORBIDDEN: cd to the main checkout, checking out other branches, `git reset --hard`, `git stash` (the stash is shared across worktrees).
     The skills and helper scripts live in the main checkout and may be absent from the worktree: read skill text from <REPO_ROOT>/.claude/skills/ and run helpers as `python3 <REPO_ROOT>/scripts/...` with <WT> as the working directory.
     Local Supabase for this worktree: scripts/supabase-local.sh env <WT> prints its SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY / SUPABASE_DB_URL; make test-integration uses it automatically. Never use another checkout's stack.
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
3. Stop the worktree's stack (its own Bash call; escalated and errored bugs included; failure is a `STACK_WARN:<ref>:<first stderr line>`): `scripts/supabase-local.sh stop "$WT"`. Then remove the worktree: `git worktree remove "$WT"` (no `--force`; the branch and PR stay). If it refuses because the tree is dirty, leave it, keep `worktreePath` and list it in the report.
4. Print `COMPLETED:<ref>:<outcome>:<pr>`, update the state file, then run **Launch** again to fill the freed slot with the next queued bug.

Print `WAITING:<active>:<queued>` whenever you are idle waiting for a completion notification. Never launch more than `N` at once. Never stop the whole run for one bug.

When no bug is `queued`, `launching` or `active`: set `phase = "complete"` and print `DONE:<complete>:<escalated>:<errors>`.

## Step 3' (container mode): prepare input and run the orchestrator

1. `mkdir -p /tmp/parallel-fix-bugs`.
2. Write `selected` to `/tmp/parallel-fix-bugs/bugs-input.json` as a JSON array of `{"ref", "id", "subject"}` objects, in `selected` order. The orchestrator launches queued bugs in input order, so the array order is the only priority signal it receives. Also write `/tmp/parallel-fix-bugs/sweep-options.json` as `{"options", "rawFlags", "urgentRefs"}`.
3. Run the orchestrator **detached via nohup** so it survives session timeouts. Export `BASE_BRANCH=<branch>` first when the bugs' base is not `main` (the orchestrator fetches it from `upstream` into the host repo before each launch, refuses a stale checked-out copy, and passes it to every worker as the second argument of `fix-and-remediate-bug`):
   ```bash
   nohup python3 scripts/devcontainer/orchestrate_bugs.py <N> /tmp/parallel-fix-bugs/bugs-input.json \
     > /tmp/parallel-fix-bugs/orchestrator.log 2>&1 &
   echo "PID: $!"
   ```
   Note the PID. An exit of 1 right away (bad concurrency or a missing input file) is explained in the log. Exit 2 means the state file was written by the other mode, or a signal stopped the run (a signal still emits a `DONE` line).
4. Start monitoring with `/loop` and note the **job ID** it returns:
   ```
   /loop 5m Check orchestrator status: tail -5 /tmp/parallel-fix-bugs/orchestrator.log; wc -c /tmp/parallel-fix-bugs/bug-*.log 2>/dev/null; if ps -p <PID> > /dev/null 2>&1; then echo "orchestrator alive"; else PHASE=$(python3 -c "import json; print(json.load(open('/tmp/parallel-fix-bugs.json')).get('phase','unknown'))" 2>/dev/null); echo "orchestrator DEAD, phase: $PHASE"; [ "$PHASE" != "running" ] && echo "TERMINAL: proceed to Step 4"; fi
   ```
   The shell command only prints; it cannot call `CronDelete`. When the loop prints `TERMINAL: proceed to Step 4`, use `CronDelete` with the job ID, then go to Step 4. A dead orchestrator whose phase is still `running` was killed: report it, do not loop forever, and tell the user a re-invocation resumes.
5. Tell the user:
   - the orchestrator is running detached (PID)
   - watch it: `tail -f -n +1 /tmp/parallel-fix-bugs/orchestrator.log`
   - watch a bug agent: `tail -f -n +1 /tmp/parallel-fix-bugs/bug-<ref>.log | jq .`
   - check state: `python3 -m json.tool /tmp/parallel-fix-bugs.json`
   - stop it: `kill <PID>` (it tears its containers down and emits `DONE`)
6. Cancel the monitoring loop with `CronDelete` and the job ID, also when you reached Step 4 manually rather than through the loop's `TERMINAL` line.

The in-container `fix-and-remediate-bug` does its own Taiga finalization (`Ready for review` and assignee, or `Blocked`, the `escalation-needed` tag and assignee), and the orchestrator itself makes no Taiga calls. For a bug the state file reports `escalated` or `error` without the worker's finalization (the container died, or the agent ended without a terminal phase), do the escalation yourself in Step 4: re-read the issue, `update_issue(project=39, ref=<ref>, status="Blocked", tags=[<existing...>, "escalation-needed"])`, and `assign_item(project=39, ref=<ref>, username="Jon Roberts", entity_type="issue")`. Skip it when the issue is already `Blocked` or `Ready for review`.

## Step 4: Report results

0. Container mode: cancel the monitoring loop with `CronDelete` and the job ID from Step 3'.4, and do the escalation writes Step 3' describes for any bug that lacks them.
1. Read `/tmp/parallel-fix-bugs.json` for final counts (completed, escalated, errors).
2. Present a summary, starting with the Urgent block from the state's `urgentRefs` (each `#<ref> <subject>: Run /fix-single-bug <ref> now.`):
   - Total bugs processed
   - Bugs completed (with PR numbers and links)
   - Bugs escalated (with reasons and Taiga links)
   - Bugs errored (with reasons)
   - Worktree mode: worktrees left in place (with paths). Container mode: the orchestrator tears each clone down as the bug ends, so list any `clonePath` from the state file that still exists (a failed teardown, or a run that was killed) and each bug's log `/tmp/parallel-fix-bugs/bug-<ref>.log`
   - Worktree mode, if any stack `stop` failed: run `scripts/supabase-local.sh prune` to stop stacks whose worktree is gone
3. Remind the user to review PRs and handle escalations.

## Event vocabulary

Two tables, one per mode; the modes emit different events.

### Worktree mode

- `INIT:<total>:<N>` -> Fresh run
- `RESUMED:<active>:<queued>:<complete>:<escalated>:<errors>` -> Resume
- `LAUNCHING:<ref>:<subject>` -> Worktree being prepared
- `STACK_WARN:<ref>:<first stderr line>` -> A Supabase stack `start` or `stop` failed (warning only)
- `LAUNCH_OK:<ref>` -> Agent spawned
- `LAUNCH_FAIL:<ref>:<reason>` -> Worktree creation failed
- `COMPLETED:<ref>:<outcome>:<pr>` -> Bug finished
- `WAITING:<active>:<queued>` -> Idle, waiting for a completion
- `DONE:<complete>:<escalated>:<errors>` -> Final summary

### Container mode (emitted by `orchestrate_bugs.py` into `orchestrator.log`)

- `INIT:<total>:<N>` -> Fresh run
- `RESUMED:<active>:<queued>:<complete>:<escalated>:<errors>` -> Resume
- `LAUNCHING:<ref>:<subject>` -> Preparing the bug's clone and container
- `LAUNCH_OK:<ref>:<pid>` -> Agent started in the container (`<pid>` is the host-side `exec-skill.sh` process)
- `LAUNCH_FAIL:<ref>:<reason>` -> The container or clone could not be prepared; a bug is retried until 3 launch attempts have failed, then it is an error
- `SLOT_FILLED:<active>:<queued>` -> A freed slot took the next queued bug
- `COMPLETED:<ref>:<outcome>:<pr>` -> Bug finished; `<outcome>` is `complete`, `already_closed`, `escalated`, or `error` (a `complete` with no PR is downgraded to `error`/`no_pr_created`)
- `WAITING:<active>:<queued>` -> Polling cycle
- `DONE:<complete>:<escalated>:<errors>` -> Final summary

`orchestrate_bugs.py` has no provider rate-limit handling: an agent stopped by one ends without a terminal phase and is reported `escalated`; re-invoking resumes.

## Notes

- **Resume**: re-invoke in the same mode (same N, or the same `--worktrees`); filters are not re-applied on resume. The state file is the only resume record, and a mode mismatch stops the run.
- **Rate limits**: N=2-3 is safe; N=4+ will throttle. Container mode: the first launch of each container is slow, and each holds about 1 to 1.5 GB RAM plus its Supabase stack.
- **Existing sequential skill**: `/fully-fix-bugs` remains available for sequential processing (one bug at a time). Use this parallel skill when you have multiple bugs to fix concurrently.
- **CI**: workers wait for the `checks` and `integration` runs; a PR whose branch predates the workflows has no runs and counts as passed after a two-minute grace when its base requires no checks, otherwise it stays `PENDING` until rebased; the "no CI checks configured" note appears only for a repo with no workflows at all.
- **Merge train**: After all bugs are fixed, use `/merge-train <PR1> <PR2> ...` to sequentially rebase and merge the resulting PRs.
