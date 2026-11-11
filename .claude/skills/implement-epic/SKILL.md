---
name: implement-epic
description: Orchestrate story implementation across an epic, in dependency order. n = 1 runs one story at a time in a git worktree; n > 1 runs up to N stories at once in isolated devcontainers (--worktrees keeps n > 1 on worktree slots; --dry-run launches and smoke-tests containers without implementing)
argument-hint: "<epic-ref> [<n>] [--worktrees] [--dry-run]"
allowed-tools:
  - Agent
  - Bash(scripts/taiga-story.sh *)
  - Bash(scripts/taiga-epic.sh *)
  - Bash(scripts/supabase-local.sh *)
  - Bash(python3 scripts/devcontainer/*)
  - Bash(nohup python3 scripts/devcontainer/*)
  - Bash(scripts/devcontainer/*)
  - Bash(docker info*)
  - Bash(docker ps *)
  - Bash(ps -p *)
  - Bash(tail *)
  - Bash(kill *)
  - Bash(python3 scripts/taiga_project_setup.py --check)
  - Bash(python3 scripts/lib/parse_roadmap.py *)
  - Bash(mkdir *)
  - Bash(cat *)
  - Bash(jq *)
  - Bash(git *)
  - Bash(env -u GH_TOKEN gh *)
  - Bash(python3 *)
  - Bash(rm -f /tmp/fully-implement-*.json)
  - mcp__taiga-py__*
  # Two things make the obvious spelling Write(/tmp/*) inert. A single leading slash anchors
  # at the settings source, meaning <repo>/tmp, and * does not cross a directory separator, so
  # /tmp/implement-epic-<ref>.json needs //tmp/**. Separately, file permissions are checked
  # against Edit(path) and Read(path) rules only: a path-scoped Write() rule is accepted and
  # then never consulted, so it must be spelled Edit(), which covers the Write tool for new
  # files as well as edits. This skill writes its state file with Write/Read and would
  # otherwise hit a permission prompt mid-run.
  - Read(//tmp/**)
  - Edit(//tmp/**)
  - Read
  - Grep
  - Glob
---

# Implement Epic $ARGUMENTS

Orchestrate story implementation across an epic. Two modes share story discovery and dependency
parsing (Steps 2 to 2c), then diverge:

- **Worktree mode** (`<n>` = 1, or `--worktrees`): the skill itself is the orchestrator. It runs up
  to `<n>` stories at a time, each in a **slot**: a git worktree at
  `.claude/worktrees/story-<ref>` plus one synchronous `senior-software-developer` agent following
  `/fully-implement-story` in that worktree. A finished slot is refilled with the next ready story.
  Each slot also runs its own local Supabase stack (`scripts/supabase-local.sh`), about 1 GB RAM.
- **Container mode** (`<n>` > 1 without `--worktrees`): the skill hands the stories to the Python
  state machine `scripts/devcontainer/orchestrate_epic.py`, which runs up to `<n>` stories at once,
  each in its own devcontainer clone with its own Supabase stack, and relaunches a story after a
  provider rate limit.

Nothing is merged; `/merge-train epic:<ref>` does that afterwards.

## Choosing the mode

**`<n>` defaults to 1: worktree mode, one story at a time.**

- **`<n>` = 1**: worktree mode. Use it when the epic's stories build on each other or edit the same
  files. Every branch is local, there is no image pull and no Docker-in-Docker, so setup is seconds.
- **`<n>` > 1**: container mode, only for a wide dependency diagram whose stories are genuinely
  disjoint on disk.
- **`<n>` > 1 with `--worktrees`**: the worktree slot loop with several slots. Pick it on a host
  without Docker headroom: each container needs about 1 to 1.5 GB plus its own Supabase stack.
  `--dry-run` is not valid with it.

A wide diagram is not on its own a reason to raise `<n>`: what decides is whether stories that
would run beside each other edit the same files. In this repo most behaviour lives in a few large
modules (`bots/metichebot.py`, `bots/chismebot.py`, `bots/crudobot.py`), so two stories touching
the same bot almost always conflict, and N parallel branches off one base each carry their own copy
of those files and a human resolves the same conflict N times. When in doubt, 1: never wrong, only
slower.

## Step 0: Parse arguments and validate

Split `$ARGUMENTS` into `<epic-ref>`, `<n>` (concurrency limit) and the flags `--worktrees` and
`--dry-run`. Default `<n>` to 1 if not provided. If `<n>` is provided but is not a positive integer,
stop and report the error: neither mode applies to zero, a negative value or a non-integer.

The run's **mode** is `worktree` when `<n>` = 1 or `--worktrees` is present, otherwise `container`.
`--dry-run` is only valid in container mode (`<n>` > 1, no `--worktrees`); with `<n>` = 1 or with
`--worktrees`, stop and say so.

In container mode `<n>` must be in the range **1-5**: `scripts/devcontainer/orchestrate_epic.py` exits
with "concurrency must be 1-5" for anything larger, before it creates any state. Validate that here
and stop with the same message rather than launching the orchestrator to fail. Worktree mode has no
such cap.

If `<n>` > 3, warn:
> "Warning: N=<n> may exceed account rate limits. Recommended max is 3."

**Preflight, once:** `python3 scripts/taiga_project_setup.py --check`. Exit 2 means the Taiga
project lacks the pipeline statuses or tags this skill reads and writes: stop and print
`Run: (eval "$(dv export project/taiga-growthgarage-jonroberts)" && python3 scripts/taiga_project_setup.py --apply)` for Jon.

**Substrate check, both modes.** `git remote get-url upstream` must name `MrsBohemian/Cholobots`,
and `env -u GH_TOKEN gh repo view MrsBohemian/Cholobots --json name` must succeed. If either
fails, stop and say exactly what is missing.

**Container substrate, container mode only.** `docker info` must succeed; the five scripts
`scripts/devcontainer/{launch,prepare-clone,exec,exec-skill,teardown}.sh` must exist and be
executable; and `npx --yes @devcontainers/cli@<DEVCONTAINER_MIN_VERSION from
scripts/devcontainer/launch.sh> --version` must succeed. If anything is missing, stop, name it,
and suggest re-invoking with `--worktrees`. Never fall back to worktree mode silently.

## Step 1: Initialize and reconcile the state file

State lives in `/tmp/implement-epic-<epic-ref>.json` (container mode honours
`$ORCHESTRATE_STATE_DIR`; the path is the same unless that is set). Read its `mode` field first.
**A run resumes in the mode that wrote its file**: if the file's `mode` differs from this run's
mode, stop and explain; the operator re-invokes with matching arguments or deletes the file. A
missing `mode` means `"container"`.

In container mode the reconcile is `orchestrate_epic.py`'s (it re-queues `error`, `skipped` and
PR-less `escalated` stories and leaves `complete` and PR-bearing `escalated` alone); do not edit
its file by hand, and skip the rest of this step. The file also records `dryRun`: a dry-run file
resumed without `--dry-run` (or the reverse) makes `orchestrate_epic.py` exit 2 and name the
mismatch; re-invoke with matching flags or delete the file.

In worktree mode, if the file exists and `phase` is not `"complete"`, this is a **resume**:

- `complete` stories are **left alone** — they already have a PR and re-running one opens a second;
- `escalated` stories with a `pr` already recorded are **left alone** too, for the same reason — a
  story can escalate after its PR exists, and re-queuing it would restart a terminal workflow and
  risk a duplicate PR;
- `error`, `skipped`, PR-less `escalated`, `launching` and `active` stories are re-queued — a
  `launching`/`active` story means the session was interrupted mid-story, not that the story failed;
- stories in the epic the file has never seen are added as `queued`;
- emit `RESUMED:<epic-ref>:<total>`, then one `REQUEUED:<ref>` or `ADDED:<ref>` per change.

A re-queued story keeps its recorded `worktree` and `branch`: Step 4b's "if the worktree path
already exists, reuse it" rule picks the existing checkout back up instead of running
`git worktree add` against a path/branch that is already registered, which fails.

If the file's `concurrency` differs from this run's `<n>`, use the new `<n>` and log the change.

Record whether this run is a **resume** or **fresh**; Step 3 branches on it.

## Step 2: Fetch and filter stories

1. `get_epic_overview(project=39, ref=<epic-ref>)` — returns the epic's subject, its full
   `description` (saved for Step 2a) and every linked story.
2. **Confirm with the user:** "Epic #<epic-ref>: <subject>. Is this the right epic?" — wait for
   confirmation before proceeding.
3. For each linked story, `get_story(project=39, ref=<story-ref>)` and keep only stories whose
   status is `Ready` **and** whose tags contain `llm-analyzed`. Record each story's internal `id`
   from `scripts/taiga-story.sh <ref> 39` when you need it for a REST script; it may stay `null`
   otherwise.
4. Sort by ref number (ascending).
5. If no eligible stories: report and stop.

### Step 2a: Parse the dependency diagram and check for cycles

Write the epic description to `/tmp/epic-<epic-ref>/description.md` (`mkdir -p /tmp/epic-<epic-ref>`
first), then parse its `## Dependency Diagram` (or `## Dependencies`) section in legacy bare-int
mode:

```bash
python3 scripts/lib/parse_roadmap.py /tmp/epic-<epic-ref>/description.md > /tmp/epic-<epic-ref>/deps.json
```

The output maps each story ref (a JSON **string** key) to the list of int refs it depends on. The
expected diagram format uses indented trees with `└─►` arrows inside a fenced code block:

```
#100 (Root story)
  └─► #200 (Depends on 100)
        └─► #300 (Depends on 200)
  └─► #400 (Also depends on 100)
```

**Graceful fallback**: no dependency section, or an unparseable one, gives `{}` — proceed without
dependencies (ref order).

**Cycle detection, before anything launches:**

```bash
python3 -c "
import json, sys
sys.path.insert(0, 'scripts/lib')
from dependency_parser import find_dependency_cycle
deps = {int(k): v for k, v in json.load(open('/tmp/epic-<epic-ref>/deps.json')).items()}
print(json.dumps(find_dependency_cycle(deps)))
"
```

A non-`null` result is a cycle: report `ERROR: Circular dependency: #a -> #b -> #a` and stop
without creating any worktree. Fix the epic's diagram and re-invoke.

#### Step 2a-bis: resolve out-of-run dependencies: Done, or merged into the base

Collect the **out-of-run refs**: every dep in the Step 2a map, for an eligible story, whose ref is
not in the eligible story list. Classify them all in one call, with the run's `baseBranch`
(Step 2c). With no out-of-run refs, run it with none; it prints an empty result
(`"satisfied_refs": []`) and makes no Taiga or GitHub call:

```bash
python3 scripts/lib/dependency_satisfaction.py --base <baseBranch> <out-of-run refs...> \
  > /tmp/epic-<epic-ref>/out-of-run.json
```

An out-of-run dependency is **satisfied** when its code is already in the base branch:

1. its story is `Done`; or
2. at least one **merged** PR into `<baseBranch>` claims the ref, no **open** PR (into any base)
   still claims it, **and** every open task of the story has a `## Gate` section. "Claims" is
   `scripts/taiga_closing_refs.py`, the parser `taiga-sync.yml` runs: a `#<ref>:` title prefix, a
   `fix(#<ref>):` scope, a standalone `Fixes #<ref>` body line, or a `feature/<ref>-`/`fix/<ref>-`
   branch. A bare `#<ref>` mention never counts.

A story shipped through several PRs is not satisfied while a later PR is open, and a merged PR
alone cannot tell whether it was the last one: PR #7 claimed #32 but carried only the baseline
dump, leaving agent tasks #93, #96 and #101 `New`. So an open task **without** a `## Gate` section
(the same check `/implement-task` uses to skip a task) keeps the dependency unsatisfied, with a
`reason` such as `merged PR #7 but agent tasks open: #93, #96, #101`. Open gated tasks (human or
post-merge steps) are allowed. A story with no open tasks needs only the merged PR.

It is **never** satisfied when the story is `Archived` or not found, or when a lookup fails
(`taiga-story.sh`, `gh pr list`, or a task's description); the `reason` field says which. The helper reads PRs with
`env -u GH_TOKEN gh pr list --repo MrsBohemian/Cholobots --state all` once per run, and treats a
list that reaches its limit as a failure rather than trusting a truncated page.

The output is `{"satisfied_refs": [...], "dependencies": {"<ref>": {...}}}`. Each dependency
carries `satisfied`, `via` (`done`, `merged` or `null`), `reason`, `status`, `merged_prs`,
`open_prs`, `open_tasks`, and, once a merged PR is found, `gated_tasks` (open tasks with a
`## Gate` section: post-merge checks waiting on Jon or Heaven), `open_agent_tasks` (open tasks
without one, which block) and `gated_tasks_error` when the task descriptions could not be read
(which also blocks). Gated tasks never block a dependent; they only go in the report. If the command itself fails, treat every out-of-run
dependency as not satisfied and record the error as the reason.

Then call `resolve_out_of_run_dependencies(deps, eligible_refs, satisfied_refs)` from
`scripts/lib/dependency_parser.py`, using the same `sys.path.insert(0, 'scripts/lib')` preamble as
the cycle check. Use its `run_deps` from here on. Remove every `excluded` ref from the story list
and record `ref -> blocking_ref` plus the blocking dependency's `reason` for the Step 5 report. If
no stories remain, report the exclusions and stop.

A satisfied out-of-run dependency is dropped from `run_deps`, so a story whose only prerequisites
are satisfied has no in-run dependencies and is cut from `baseBranch`, which already contains the
prerequisite's merged work (Step 2c). An unsatisfied prerequisite (not merged, a PR still open,
agent tasks still open, Archived, or unknown because a lookup failed) keeps the dependent **out of this run**, because
running it would cut its branch from a base that lacks the prerequisite's work.

### Step 2b: Write stories-input.json

Write the filtered stories to `/tmp/epic-<epic-ref>/stories-input.json` as a JSON array, with each
story's `dependencies` set to its `run_deps` entry (`[]` when it has none):

```json
[
  {"ref": 101, "id": 1001, "subject": "Foundation", "dependencies": []},
  {"ref": 102, "id": 1002, "subject": "Depends on 101", "dependencies": [101]},
  {"ref": 103, "id": 1003, "subject": "Also depends on 101", "dependencies": [101]}
]
```

This file is the run's input record; on a resume, Step 1 reconciles the state file against it, and
container mode's `orchestrate_epic.py` reads it directly (Step 4'). The orchestrator resolves each
story's base branch itself, so the file carries only `dependencies`.

**Dry run:** write only the first `<n>` stories, in ref order, whose `dependencies` list is empty.
A dry run proves the launch path, not the dependency chain.

### Step 2c: `baseBranch` is the run's root base, not every story's base

`baseBranch` (default `main`, honouring `$BASE_BRANCH`) is the **run's root base** — the base only
for stories with no dependencies, including stories whose only prerequisites were out-of-run
dependencies satisfied in Step 2a-bis (Done, or merged into `baseBranch`, so their work is already
there). A story with one dependency is cut from that dependency's branch,
so it sees the dependency's work; a story with two or more is cut from its integration branch
(Step 4a). Every branch lives on `upstream` (`MrsBohemian/Cholobots`), so a stacked PR's base is
always a branch in the same repo.

The resolved base is recorded per story as `base` in the state file, alongside its own `branch`.
On resume, the state file's `baseBranch` wins over a conflicting `$BASE_BRANCH` (log the ignored
value). A single dependency that completed without a recorded branch causes its dependents to be
**skipped** (`SKIPPED:<ref>:dependency_failed:<dep>`) rather than falling back to `baseBranch`.

## Step 3 (worktree mode, fresh runs only): write the state file

**Skip on a resume, and skip entirely in container mode.** Rewriting the file on a resume sets every story back to `queued` and
re-implements the finished ones — the exact failure Step 1 exists to prevent.

Compute the run order first: topological sort of the eligible stories by `run_deps`, ties broken by
ascending ref. Then write:

```json
{
  "mode": "worktree",
  "phase": "running",
  "epicRef": 0, "baseBranch": "main", "concurrency": 1,
  "stories": [
    {"ref": 0, "id": null, "subject": "...", "dependencies": [], "status": "queued",
     "branch": null, "worktree": null, "base": null, "baseReason": null,
     "pr": null, "outcome": null, "escalationReason": null, "failureReason": null}
  ]
}
```

`status` is `queued` → `launching` → `active` → `complete` | `escalated` | `error` | `skipped`.
`outcome` refines a terminal status (`complete`, `handed_off`, `no_pr_created`,
`dependency_failed`, `integration_conflict`, `integration_error`, `agent_incomplete`). Stories
appear in run order. **Rewrite the file on every status change** — that is the only thing that
makes an interrupted run resumable.

Emit `INIT:<epic-ref>:<total>:<n>`.

## Step 4 (worktree mode): the slot loop

Repeat until no story is `queued`, `launching` or `active`:

1. **Cascade.** For every `queued` story with a dependency that ended `error`, `escalated` or
   `skipped`, set it `skipped` with outcome `dependency_failed`, emit
   `SKIPPED:<ref>:dependency_failed:<dep-ref>`, and repeat until nothing changes (transitive).
   A failure never ends the run.
2. **Ready set.** `queued` stories whose every dependency is `complete`, in run order.
3. **Fill free slots.** Free slots = `<n>` minus `active` stories. For each of the first that many
   ready stories, do **4a** (base), **4b** (worktree), set `status: "launching"`, emit
   `LAUNCHING:<ref>:<subject>`, rewrite the state file.
4. **Spawn the round.** Set every launching story `active`, rewrite the state file, then spawn one
   agent per launching story per **4c**, all in **one message** as parallel foreground `Agent`
   calls. Emit `LAUNCH_OK:<ref>` for each.
5. **Record.** As the calls return, record each story per **4d** and rewrite the state file.
   Foreground calls sent in one message return together, so a freed slot refills at the next pass
   of this loop; that is the price of never leaving an agent running past the end of a turn.
6. **Stall.** If nothing is ready and nothing is `active` while stories are still `queued`, their
   dependencies can never complete: skip them with `dependency_failed` as in item 1.

### 4a. Choose the base

**A multi-parent story (2+ dependencies) does not call `select_base_branch` at all.** Its base is an
integration branch built here, at the start of its own turn; the readiness rule already guarantees
every dependency reached `complete`, so every parent branch is resolvable.

- **Name it** with `integration_branch_name(epic_ref, story_ref)` from
  `scripts/lib/integration_branch.py` — `epic/<epic-ref>-integration-<story-ref>`. Do not
  hand-spell it.
- **Parent branches** come from the parents' recorded `branch` in the state file, or
  `env -u GH_TOKEN gh pr view <pr> --repo MrsBohemian/Cholobots --json headRefName` — they are
  agent-invented slugs and must never be derived from a ref.
- **Build it** in a scratch worktree, never in the repo root:
  ```bash
  git fetch upstream
  git worktree add -b <name> .claude/worktrees/integration-<ref> upstream/<first parent branch>
  git -C .claude/worktrees/integration-<ref> merge --no-ff --no-edit upstream/<each further parent branch>
  git -C .claude/worktrees/integration-<ref> push -u upstream <name>
  env -u GH_TOKEN gh pr create --repo MrsBohemian/Cholobots --base <baseBranch> --head <name> \
    --title "Epic #<epic-ref>: integration branch for #<ref>" --body "Merges the parents of #<ref>: <parent refs>."
  git worktree remove .claude/worktrees/integration-<ref>
  ```
  The story then branches from `<name>`, and its own PR targets `<name>`, not the root base.
- **Hand-off to `/merge-train`**: the story's PR is a stacked PR there — its base is the
  integration branch — so the train retargets and `--merge`s it once the integration branch lands,
  then closes the integration PR once all its parents have merged. `/merge-train`'s SKILL.md owns
  the stacked-PR rules; they are not restated here.
- **On a conflict**: `git -C .claude/worktrees/integration-<ref> merge --abort`, remove the scratch
  worktree and `git branch -D <name>`. The story is **blocked**, not failed: set it `skipped` with
  outcome `integration_conflict`, emit `BLOCKED:<ref>:integration_conflict:<parent-refs>`, and
  continue; its dependents cascade-skip. There is no PR yet, so `/rebase-branch` (which needs a PR
  number) cannot help here. A failure to resolve, fetch, push, or open the PR is
  `integration_error` instead (`BLOCKED:<ref>:integration_error:<parent-refs>`), so the event does
  not send a reader looking for a conflict that never happened.

Otherwise use the integration branch as `base` with reason `"integration"` and go to 4b.

For a story with zero or one dependency, call `select_base_branch` from
`scripts/lib/base_selector.py`, passing the story's dependencies and a `story_branches` map seeded
from **every** `complete` story in the state file (not only ones finished in this run), using each
story's `branch`. Keys may be ints or the strings a JSON round-trip produces; either resolves:

```bash
python3 -c "
import sys, json
sys.path.insert(0, 'scripts/lib')
from base_selector import select_base_branch, UnresolvedDependencyError
try:
    base, reason = select_base_branch(<ref>, <dependencies>, <story_branches>, default_base='<baseBranch>')
    print(json.dumps({'base': base, 'reason': reason}))
except UnresolvedDependencyError as e:
    print(json.dumps({'error': str(e)}))
"
```

The rule, in prose (the report must explain itself):

- **no dependencies** -> base is `baseBranch`
- **exactly one dependency** -> base is that story's recorded branch
- **more than one dependency** -> the integration branch above; calling `select_base_branch` with
  2+ dependencies raises `MultiParentDependencyError`, which is a skill bug, not a skip
- **`UnresolvedDependencyError`** -> the story does not run: set it `skipped`, emit
  `SKIPPED:<ref>:dependency_failed:<dep-ref>`, and continue. There is **no** fallback to "the last
  branch that did finish".

A dependency's branch is a valid base once that story is `complete` (outcome `complete` or
`handed_off`): its branch is pushed to `upstream` and its PR is open.

### 4b. Create the worktree

`<branch>` is `feature/<ref>-<kebab-cased subject>`, the slug derived from the Taiga subject.

```bash
git fetch upstream
git fetch upstream "+<base>:refs/remotes/upstream/<base>"
git branch <branch> upstream/<base>
git worktree add .claude/worktrees/story-<ref> <branch>
```

Always cut from `upstream/<base>`, never a local `<base>`: a stale local branch would silently win
and cut the story from old code. The explicit second fetch populates `refs/remotes/upstream/<base>`
even when the configured refspec does not track that branch.

**If the worktree path already exists, reuse it — do not recreate it and do not delete it.** A
resumed run must pick the existing worktree back up; say so and carry on. If `<branch>` already
exists without a worktree, skip `git branch` and add the worktree on the existing branch.

Record `branch`, `worktree`, `base` and `baseReason`. If any command fails, set the story `error`
with the git error as `failureReason`, emit `LAUNCH_FAIL:<ref>:<reason>`, and go on to the next
ready story.

**Start the slot's Supabase stack**, as its own Bash call from the main checkout, once the worktree
exists (also when it was reused):

```bash
scripts/supabase-local.sh start <absolute worktree path> >/dev/null
```

A base without `supabase/config.toml` exits 0 with a note. A non-zero exit is a warning only: emit
`STACK_WARN:<ref>:<first stderr line>`, remember that a stop may be needed, and carry on (the
worker's integration tests then skip, or fail under `make test-integration`). Never mark the story
`error` for it.

### 4c. Spawn one agent per slot

**Before spawning**, check `/tmp/fully-implement-<ref>.json`. If it exists with a **terminal**
`phase` (`complete`, `escalated`, `error`), delete it with exactly
`rm -f /tmp/fully-implement-<ref>.json` — that spelling is the one `allowed-tools` grants, and
improvising another one stops an unattended run on a permission prompt. The file is a leftover from
an earlier attempt, and `/fully-implement-story` only overwrites it once it reaches its own state
initialisation; if this attempt's agent dies before then, 4d would misreport the leftover result.
Leave the file alone if `phase` is **non-terminal** — that is the re-queued story case, where
`/fully-implement-story` is meant to resume from it.

A **single** `senior-software-developer` agent per slot, foreground (`run_in_background: false`),
with a prompt in this shape:

```
Work in the git worktree at <absolute worktree path>, on branch <branch>. Every git
command runs inside that worktree. Never cd to the main checkout at <repo root>, never
check out another branch, and never run `git reset --hard` or `git stash` (the stash is
shared across worktrees). Your shell's working directory resets between calls, so cd to
the worktree (or use absolute paths) in every Bash call.

Local Supabase for this worktree: scripts/supabase-local.sh env <absolute worktree path>
prints its SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY / SUPABASE_DB_URL; make test-integration
uses it automatically. Never use another checkout's stack.

Branch <branch> is ALREADY created and checked out there, cut from upstream/<base branch>.
In /fully-implement-story this is the "branch exists" case, but SKIP any `git pull`: the
branch has no upstream tracking yet, so a pull fails with "no tracking information".
Push it with `git push -u upstream <branch>`. Do not create a second branch.

Follow the /fully-implement-story skill with arguments '<ref> <base branch>'. It owns the
entire per-story workflow: the task loop, the commits, the local checks, the push, the PR
against <base branch> on MrsBohemian/Cholobots, the review round and the Taiga status
writes. Do not improvise around it.

This run is unattended. Do not stop to ask the operator a question.
```

Do **not** use the Agent tool's `isolation: "worktree"` — that creates its own throwaway worktree
off the wrong base. Foreground is mandatory: a turn that ends with work in flight can let the CLI
exit and kill the agent.

### 4d. Record the result

Read `/tmp/fully-implement-<ref>.json`, **not** the agent's prose. `/fully-implement-story` writes
that file. "Valid `prNumber`" means a positive integer:

| `phase` | valid `prNumber`? | status / outcome | event |
|---|---|---|---|
| `complete` | yes | `complete` / `complete` | `COMPLETED:<ref>:complete:<pr>` |
| `complete` | no | `error` / `no_pr_created` | `FAILED:<ref>:no_pr_created` |
| `escalated` | -- | `escalated` / `escalated` | `COMPLETED:<ref>:escalated:<pr>` |
| a handoff phase (`monitoring`) | yes | `complete` / `handed_off` (a **success**) | `COMPLETED:<ref>:handed_off:<pr>` |
| anything else | yes | `escalated`, reason `non-terminal phase: <phase>` | `COMPLETED:<ref>:escalated:<pr>` |
| anything else, or file missing/unreadable | no | `error` / `agent_incomplete` | `FAILED:<ref>:agent_incomplete` |

A `complete` phase with no valid PR means the agent exited claiming success without ever opening a
reviewable PR — treat it as an error rather than letting it unblock dependents unreviewed.

**Agent returned without completing → `error`, keep the worktree.** Whatever the agent left
uncommitted is the recovery material; the next run re-queues the story and reuses the worktree.

Also read **`featureBranch`** from the same file and record it as the story's `branch`. That is
the branch that actually exists on `upstream`; it is what feeds `story_branches` for a dependent's
base. Branch names are never derived from a ref.

**Never leave the loop for a single story's failure.** Record it, move on, report it at the end.

## Step 4' (container mode): prepare input and run the orchestrator

1. `mkdir -p /tmp/epic-<epic-ref>`. Step 2b already wrote `stories-input.json` there.
2. Run the orchestrator **detached via nohup** so it survives session timeouts. Export
   `BASE_BRANCH=<baseBranch>` first when the run's root base is not `main` (the state file's
   recorded `baseBranch` wins on a resume):
   ```bash
   nohup python3 scripts/devcontainer/orchestrate_epic.py <epic-ref> <n> /tmp/epic-<epic-ref>/stories-input.json [--dry-run] \
     > /tmp/epic-<epic-ref>/orchestrator.log 2>&1 &
   echo "PID: $!"
   ```
   Note the PID. An exit of 1 right away (bad arguments, or `ERROR: Circular dependency`) is in
   the log; an exit of 2 after a signal still emits a `DONE` line.
3. Start monitoring with `/loop` and note the **job ID** it returns:
   ```
   /loop 5m Check orchestrator status: tail -5 /tmp/epic-<epic-ref>/orchestrator.log; wc -c /tmp/epic-<epic-ref>/story-*.log 2>/dev/null; if ps -p <PID> > /dev/null 2>&1; then echo "orchestrator alive"; else PHASE=$(python3 -c "import json; print(json.load(open('/tmp/implement-epic-<epic-ref>.json')).get('phase','unknown'))" 2>/dev/null); echo "orchestrator DEAD, phase: $PHASE"; [ "$PHASE" != "running" ] && echo "TERMINAL: proceed to Step 5"; fi
   ```
   The shell command only prints; it cannot call `CronDelete`. When the loop prints `TERMINAL:
   proceed to Step 5`, use `CronDelete` with the job ID, then go to Step 5. A dead orchestrator
   whose phase is still `running` was killed: report it, do not loop forever, and tell the user a
   re-invocation resumes.
4. Tell the user:
   - the orchestrator is running detached (PID), and for a dry run that it launches and
     smoke-tests containers without implementing and keeps them
   - watch it: `tail -f -n +1 /tmp/epic-<epic-ref>/orchestrator.log`
   - watch a story agent: `tail -f -n +1 /tmp/epic-<epic-ref>/story-<ref>.log | jq .`
   - check state: `python3 -m json.tool /tmp/implement-epic-<epic-ref>.json`
   - stop it: `kill <PID>` (it tears its containers down and emits `DONE`)
5. Cancel the monitoring loop with `CronDelete` and the job ID, also when you reached Step 5
   manually rather than through the loop's `TERMINAL` line.

## Step 5 — finish, report, epic status

Set `phase` to `"complete"` and emit `DONE:<complete>:<escalated>:<errors>:<skipped>`
(`handed_off` stories count under `<complete>`).

Shared by both modes. In container mode the orchestrator has already set `phase` and tore down
its containers; skip the worktree cleanup below and only report. **Dry run:** the orchestrator
keeps every container and clone. Report each story's kept `clonePath` and `containerId` from the
state file, the teardown command (`scripts/devcontainer/teardown.sh <clonePath>` per clone), the stories skipped as `dry_run_dependency`, and
recommend **no** epic status change.

**Worktree cleanup is worktree mode's**: for a story that ended `complete`, stop its stack first
(`scripts/supabase-local.sh stop <absolute worktree path>`, its own Bash call), then
run `git worktree remove .claude/worktrees/story-<ref>` and `git worktree prune` — the branch is
pushed and the PR is open. **Keep** the worktree for a story that ended `escalated` or `error`: it
may hold uncommitted work and it is the evidence for whoever picks the story up. Run the same
`stop` for kept worktrees too (migrations and seed rebuild the data). A failed `stop` is a
`STACK_WARN:<ref>:<first stderr line>`, never an error; if any stop failed, the final report says
to run `scripts/supabase-local.sh prune`. The scratch `integration-<ref>` worktree gets no stack.

1. Read the state file for final counts.
2. **Epic status.** The workflow MCP server has no epic-status tool, and `scripts/taiga-epic.sh`
   edits descriptions only, so report the status you recommend for Jon to set: `Done` when every
   eligible story completed, otherwise `In progress` so it stays visible for follow-up.
3. Report as a table — ref, subject, outcome, base branch, **the reason string from
   `select_base_branch`** (or `integration`), branch, PR URL
   (`https://github.com/MrsBohemian/Cholobots/pull/<n>`), note — followed by the excluded stories
   (each with the unfinished prerequisite that blocked it), the failures with reasons and their kept
   worktree paths, the dropped out-of-run dependencies, and the reminder that nothing was merged.
   List the dropped dependencies from `/tmp/epic-<epic-ref>/out-of-run.json` in two groups: those
   satisfied as `Done`, and those satisfied through the **merged-PR path** (`via: merged`), each
   with its status, its merged PR number(s) and its open gated tasks (`#<task-ref> <subject>`).
   The merged-PR group tells Jon which stories still have human steps
   waiting; they block nothing. Each excluded story names the dependency that blocked it and that
   dependency's `reason`. `handed_off` stories had CI unverified at agent exit — the merge train
   re-validates before merging. Every PR whose branch has the workflows reports `checks` and `integration`.

## Event vocabulary

Two tables, one per mode; the modes differ and so does what they emit.

### Worktree mode

- `INIT:<epic-ref>:<total>:<n>` → Starting a fresh run
- `RESUMED:<epic-ref>:<total>` → Resuming from existing state
- `REQUEUED:<ref>` → On resume, a previously failed or interrupted story was re-queued
- `ADDED:<ref>` → On resume, a story present in the epic but absent from state was added as queued
- `LAUNCHING:<ref>:<subject>` → Worktree prepared, about to spawn the story agent
- `LAUNCH_OK:<ref>` → Agent spawned
- `LAUNCH_FAIL:<ref>:<reason>` → Worktree creation failed
- `COMPLETED:<ref>:<outcome>:<pr>` → Story finished; `<outcome>` is `complete`, `handed_off`, or `escalated`
- `FAILED:<ref>:<reason>` → The story's outcome is an error (`no_pr_created`, `agent_incomplete`)
- `SKIPPED:<ref>:dependency_failed:<failed_ref>` → Story skipped because a dependency did not succeed
- `BLOCKED:<ref>:integration_conflict:<parent-refs>` → The story's parents conflict when merged; that story alone is skipped and its dependents cascade-skip
- `BLOCKED:<ref>:integration_error:<parent-refs>` → The integration branch failed to build for an operational reason; same skip and cascade
- `DONE:<complete>:<escalated>:<errors>:<skipped>` → Final summary

### Container mode (emitted by `orchestrate_epic.py` into `orchestrator.log`)

- `INIT:<epic-ref>:<total>:<n>` → Starting a fresh run
- `RESUMED:<epic-ref>:<active>:<queued>:<complete>:<escalated>:<errors>` → Resuming from existing state
- `REQUEUED:<ref>` / `ADDED:<ref>` → Resume reconcile, as in worktree mode
- `LAUNCHING:<ref>:<subject>` → Preparing the story's clone and container
- `LAUNCH_OK:<ref>:<pid>` → Agent started inside the container (`<pid>` is the host-side `exec-skill.sh` process)
- `LAUNCH_FAIL:<ref>:<reason>` → The container or clone could not be prepared
- `SLOT_FILLED:<active>:<queued>` → A freed slot took the next ready story
- `COMPLETED:<ref>:<outcome>:<pr>` → Story finished; `<outcome>` is `complete`, `handed_off` (a success) or `escalated`
- `FAILED:<ref>:<reason>` → Error outcome (`no_pr_created`, `agent_incomplete`)
- `SKIPPED:<ref>:dependency_failed:<failed_ref>` → Skipped because a dependency did not succeed
- `SKIPPED:<ref>:dry_run_dependency` → Dry run only: the story has dependencies, so it is not launched
- `BLOCKED:<ref>:<outcome>:<parent-refs>` → Integration branch for a multi-parent story failed; that story is skipped and its dependents cascade-skip
- `RATE_LIMITED:<ref>:<not-before-iso>` → The agent hit a provider rate limit. Not a failure: the story is re-queued with its clone kept and relaunches after the reset time; no other story launches until then. After 3 rate-limited exits it is `escalated` with outcome `rate_limit_exhausted` and its clone is kept
- `WAITING:<active>:<queued>` → Polling cycle; long runs of it are normal while a rate-limit window elapses
- `DONE:<complete>:<escalated>:<errors>:<skipped>` → Final summary (`handed_off` counts under `<complete>`)

## Notes

- **Resume**: re-invoke with the same epic ref and the same mode; Step 1 reconciles the state file. To force a full
  from-scratch re-run, use `/reset-epic-stories` or delete `/tmp/implement-epic-<epic-ref>.json`.
- **Rate limits, per mode**: container mode relaunches a rate-limited story automatically
  (`RATE_LIMITED`, above). Worktree mode does not: an agent killed by one returns without
  completing, so the story ends `error` with its worktree kept, and the next run re-queues it and
  resumes from `/tmp/fully-implement-<ref>.json`.
- **Container mode costs**: the first launch of each container is slow (image pull and stack
  start), and each holds about 1 to 1.5 GB RAM plus its Supabase stack. N = 2 or 3 is safe.
- **Worktrees** live at `.claude/worktrees/story-<ref>` (git-ignored), are reused on a resumed run,
  and are removed by this skill only on `complete`.
- **Merge train**: after the run, use `/merge-train epic:<ref>` to rebase and merge the resulting
  PRs in diagram order.
- **Coexistence**: `/overseer` refuses an epic whose state file here is `running`, and any story
  this file holds in a non-terminal status. This skill does not check overseer state in return.

## What this skill does NOT do

- Does not implement, commit, or open a story PR itself — `/fully-implement-story` does, inside
  each slot. The only branches it creates and pushes itself are story branches (unpushed, for the
  agent) and integration branches.
- Does not merge or close a story — `/merge-train` and Jon's review do that.
- Does not set the epic's status — it reports the recommended one.
