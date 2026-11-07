---
name: implement-epic
description: Orchestrate story implementation across an epic in git worktree slots — up to N stories at a time, each in its own worktree driven by a senior-software-developer running /fully-implement-story, in dependency order
argument-hint: "<epic-ref> [<n>]"
allowed-tools:
  - Agent
  - Bash(scripts/taiga-story.sh *)
  - Bash(scripts/taiga-epic.sh *)
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

Orchestrate story implementation across an epic. The skill itself is the orchestrator: it
discovers the epic's analyzed stories, orders them by the epic's `## Dependency Diagram`, and runs
up to `<n>` of them at a time, each in a **slot** — a git worktree at
`.claude/worktrees/story-<ref>` plus one synchronous `senior-software-developer` agent following
`/fully-implement-story` in that worktree. A finished slot is refilled with the next ready story.
Nothing is merged; `/merge-train epic:<ref>` does that afterwards.

## Choosing `<n>`

**`<n>` defaults to 1** — one story at a time. Raise it only for a wide dependency diagram whose
stories are genuinely disjoint on disk.

A wide diagram is not on its own a reason to raise `<n>`: what decides is whether stories that
would run beside each other edit the same files. In this repo most behaviour lives in a few large
modules (`bots/metichebot.py`, `bots/chismebot.py`, `bots/crudobot.py`), so two stories touching
the same bot almost always conflict, and N parallel branches off one base each carry their own copy
of those files and a human resolves the same conflict N times. When in doubt, 1: never wrong, only
slower.

## Step 0: Parse arguments and validate

Split `$ARGUMENTS` into `<epic-ref>` and `<n>` (concurrency limit). Default `<n>` to 1 if not
provided. If `<n>` is provided but is not a positive integer, stop and report the error.

If `<n>` > 3, warn:
> "Warning: N=<n> may exceed account rate limits. Recommended max is 3."

**Preflight, once:** `python3 scripts/taiga_project_setup.py --check`. Exit 2 means the Taiga
project lacks the pipeline statuses or tags this skill reads and writes: stop and print
`Run: python3 scripts/taiga_project_setup.py --env-file scripts/taiga-auth.env --apply` for Jon.

**Substrate check.** `git remote get-url upstream` must name `MrsBohemian/Cholobots`, and
`env -u GH_TOKEN gh repo view MrsBohemian/Cholobots --json name` must succeed. If either fails,
stop and say exactly what is missing.

## Step 1: Initialize and reconcile the state file

State lives in `/tmp/implement-epic-<epic-ref>.json`. If the file exists and `phase` is not
`"complete"`, this is a **resume**:

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

#### Step 2a-bis: resolve out-of-run dependencies by Done status

Collect the **out-of-run refs**: every dep in the Step 2a map, for an eligible story, whose ref is
not in the eligible story list. For each one, `get_story(project=39, ref=<dep-ref>)` (or
`scripts/taiga-story.sh <dep-ref> 39`) and add it to `done_refs` **only if its status is `Done`**.
Do **not** use `is_closed`: `Archived` is also closed and is not a merge. A lookup that fails, or
returns exit 44, counts as not Done.

Then call `resolve_out_of_run_dependencies(deps, eligible_refs, done_refs)` from
`scripts/lib/dependency_parser.py`, using the same `sys.path.insert(0, 'scripts/lib')` preamble as
the cycle check. Use its `run_deps` from here on. Remove every `excluded` ref from the story list
and record `ref -> blocking_ref` for the Step 5 report. If no stories remain, report the exclusions
and stop.

An out-of-run dependency is dropped **only** when it is Done, i.e. merged — its work is already in
the base branch. An unfinished prerequisite (In progress, Blocked, Ready for review, Ready for test,
not yet analyzed, Archived) keeps the dependent **out of this run**, because running it would cut
its branch from a base that lacks the prerequisite's work.

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

This file is the run's input record; on a resume, Step 1 reconciles the state file against it.

### Step 2c: `baseBranch` is the run's root base, not every story's base

`baseBranch` (default `main`, honouring `$BASE_BRANCH`) is the **run's root base** — the base only
for stories with no dependencies. A story with one dependency is cut from that dependency's branch,
so it sees the dependency's work; a story with two or more is cut from its integration branch
(Step 4a). Every branch lives on `upstream` (`MrsBohemian/Cholobots`), so a stacked PR's base is
always a branch in the same repo.

The resolved base is recorded per story as `base` in the state file, alongside its own `branch`.
On resume, the state file's `baseBranch` wins over a conflicting `$BASE_BRANCH` (log the ignored
value). A single dependency that completed without a recorded branch causes its dependents to be
**skipped** (`SKIPPED:<ref>:dependency_failed:<dep>`) rather than falling back to `baseBranch`.

## Step 3 (fresh runs only): write the state file

**Skip on a resume.** Rewriting the file on a resume sets every story back to `queued` and
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

## Step 4: the slot loop

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

## Step 5 — finish, report, epic status

Set `phase` to `"complete"` and emit `DONE:<complete>:<escalated>:<errors>:<skipped>`
(`handed_off` stories count under `<complete>`).

**Worktree cleanup is this skill's**: for a story that ended `complete`, run
`git worktree remove .claude/worktrees/story-<ref>` then `git worktree prune` — the branch is pushed
and the PR is open. **Keep** the worktree for a story that ended `escalated` or `error`: it may hold
uncommitted work and it is the evidence for whoever picks the story up.

1. Read the state file for final counts.
2. **Epic status.** The workflow MCP server has no epic-status tool, and `scripts/taiga-epic.sh`
   edits descriptions only, so report the status you recommend for Jon to set: `Done` when every
   eligible story completed, otherwise `In progress` so it stays visible for follow-up.
3. Report as a table — ref, subject, outcome, base branch, **the reason string from
   `select_base_branch`** (or `integration`), branch, PR URL
   (`https://github.com/MrsBohemian/Cholobots/pull/<n>`), note — followed by the excluded stories
   (each with the unfinished prerequisite that blocked it), the failures with reasons and their kept
   worktree paths, the dropped out-of-run dependencies (Done prerequisites only), and the reminder
   that nothing was merged. `handed_off` stories had CI unverified at agent exit — the merge train
   re-validates before merging. Until story #35 adds CI, every PR reports "no checks configured".

## Event vocabulary

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

## Notes

- **Resume**: re-invoke with the same epic ref; Step 1 reconciles the state file. To force a full
  from-scratch re-run, use `/reset-epic-stories` or delete `/tmp/implement-epic-<epic-ref>.json`.
- **Rate limits**: there is no automatic requeue on a provider rate limit. An agent killed by one
  returns without completing, so the story ends `error` with its worktree kept, and the next run
  re-queues it and resumes from `/tmp/fully-implement-<ref>.json`.
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
