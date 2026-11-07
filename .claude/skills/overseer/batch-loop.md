# Overseer Batch Loop — shared machinery

The **mode-independent** half of a batch run, shared by `epic-mode.md` (`/overseer <epic-ref>`)
and `sprint-mode.md` (`/overseer sprint <milestone>`).
Each mode file supplies its own enumeration, dependency-source, state-file naming, and report;
everything below is identical for every batch kind and lives here **once**, so a fix lands in one
place.

The machinery below is deliberately written **kind-agnostic** — a batch's units of work are called
**items**, and nothing here assumes an item is a story. In epic mode every item is a story; in
sprint mode an item is a story or a bug.

Each mode file names its own **batch resume ref** — the argument that re-invokes the run
(`<epic-ref>` for epic mode, `sprint <milestoneId>` for sprint mode). Below, "the batch resume
ref" means that value.

## Enumeration completeness (canonical)

The workflow server's batch reads (`get_epic_overview`, `get_sprint_board`) return the whole set
in one call, and `scripts/taiga_select_issues.py` pages issues internally. Never validate an
enumeration against a remembered count; the count is whatever the call returns today. A raw REST
caller follows [`../shared/taiga-pagination.md`](../shared/taiga-pagination.md).

## Fallback announcements (canonical)

Two distinct strings, chosen by what the dependency source parsed to:

- Parse gave `{}` (no diagram section, unparseable content) **or** the graph is cyclic → announce
  **"no dependency graph parsed, falling back to ref-ascending"**.
- Parse gave a non-empty map with **zero edges** → announce
  **"diagram parsed, no dependency edges — ref-ascending IS the correct topological order."**

## Ordering loop (Kahn)

There is no batch-level start gate — the run begins immediately; only the per-item Gate A/Gate B
fire. Every gate answer inside a batch — Gate A/Gate B, the cascade choice, a budget check-in,
adopting a filed item — counts only when Jon types it in the overseer's own session
([`run-discipline.md`](run-discipline.md)).

**Ordering** is an explicit Kahn procedure: build the ready-set of `pending` items whose every
dependency is satisfied (satisfaction is defined per mode — see each mode file's parameters), take
the **lowest ref** (in a mixed-kind batch, lowest ref regardless of kind), run it, recompute. An
item **absent from the parsed dependency map** has no dependencies to satisfy and so is trivially
ready — exclude it from the ready-set anyway while any item **present in the map** is still
`pending`, so a no-diagram item never jumps ahead of a mapped one. Only once every mapped item has
left `pending` does the no-diagram remainder become selectable, lowest ref first.

**Invariant**: after driving a selected item, its row **must** leave `pending` for `complete`,
`escalated`, `skipped`, or `external-complete` — any other outcome is recorded `escalated` with a
reason and the loop continues; otherwise the same item would be reselected forever.

**Liveness**: if the ready-set is empty while at least one item is still `pending`, take the
**Idle exit** below — resumable by re-invoking `/overseer` with the **batch resume ref**. This is
the loop's only liveness guarantee.

Drive each selected item through the normal per-item Steps, including analyze + Gate A when the
`llm-analyzed` tag is absent. `routing.md` **Step 1a** (the advisory roadmap lookup) still runs
per item inside a batch run — it is advisory, silent-skips on any failure, and its output is
surfaced at that item's Gate A exactly as in a single-item run.

An item enumerated in the batch but **absent from the parsed dependency map** is ordered LAST and
flagged in the report as "no diagram node". A dependency ref pointing **outside** the batch is
resolved live: satisfied by its live status, otherwise an unsatisfied dependency that can trigger
the all-blocked pause; one that does not resolve at all is dropped with a report note.

## Idle exit

Before each selection — the first one included, so a run can end without spawning a worker —
classify every `pending` item:

- **agent-actionable** — every dependency is satisfied and nothing outside the run gates it.
- **blocked** — record why in the row's `blockedOn`:
  - `dependency: <refs>` — an unsatisfied dependency;
  - `waiting on Jon: <what>` — the item waits on a Jon decision or a JON-ONLY step: it carries
    `escalation-needed`, sits at Blocked assigned to Jon, has an open gate question awaiting his
    answer, or every open task on it has a `## Gate` section naming Jon, a credential, an admin
    setting, or spend;
  - `waiting on real use: <what>` — every open task's `## Gate` names real-world usage, a live
    event, or post-merge evidence that only time produces.

"Waiting on Jon" and "waiting on real use" count as **blocked, not pending**: those items never
enter the ready-set, and probing, re-verifying, or filing work around them is not progress.

When no `pending` item is agent-actionable, exit idle:

1. Persist `phase: "paused"`, with each pending row's `blockedOn`.
2. Write the hand-off. Epic mode: read-modify-write the epic description, replacing (or appending)
   only its `## Overseer hand-off` section with one line per blocked item
   (`#<ref> <subject> — <blockedOn>`), the run's `filedItems`, and the resume command
   (`/overseer <batch resume ref>`). The workflow MCP server cannot edit an epic description, so
   go through REST: `scripts/taiga-epic.sh get <epic-ref>` for the description and its `version`,
   write the edited description to `/tmp/overseer-epic-<epic-ref>-handoff.md` (touch nothing
   outside that section), then `scripts/taiga-epic.sh patch <epic-ref> --description-file
   /tmp/overseer-epic-<epic-ref>-handoff.md --version <version>`. On a version conflict, re-get and
   re-apply once; a second conflict goes in the report instead. Sprint mode: a milestone has no
   description, so the hand-off lives in the state file and the S6 report.
3. Send Jonlist one status line: the run, `paused (idle)`, and the blocked count
   ([`run-discipline.md`](run-discipline.md), **Jonlist**).
4. Report and stop.

An epic whose only open items are Jon-gated therefore ends `paused`, with the blocking list posted,
without spawning a worker. On resume, re-classify every row live before selecting: a `blockedOn`
is a snapshot, not a verdict.

## Runtime findings

[`run-discipline.md`](run-discipline.md) **Runtime findings** applies to every batch. An item filed
mid-run is recorded in the batch file's `filedItems` and in the hand-off, and is **never added to the
batch's `stories`/`items` array** — re-enumeration on resume included. If a worker linked a filed
story to the epic (or put a filed item in the milestone), the re-enumeration leaves it out unless
its `filedItems` entry carries `"adopted": true`, which you set only when Jon adopts it in the
overseer's own session. An adopted item joins the array as `pending` and is ordered like any other.

## Base selection

Scoped to epic mode: an epic story's dependency-satisfaction definition (E3) and its
`epic/<epic-ref>-integration-<ref>` branch naming below are epic mode's. Sprint mode has its own
dependency-satisfaction definition and base-selection cases (`sprint-mode.md` **S4**), which reuses
the two-or-more-upstream branch-naming case only when a sprint item carries an epic ref, escalates a
multi-parent item that lacks one and a bug with exactly one satisfied-but-unmerged upstream, and
otherwise follows this section unchanged — including a sprint story's single-upstream stacking
case below.

The conductor **never creates a branch** — `SKILL.md`'s tool grant has no `Bash(git *)` in any form
and forbids mutating commands; it only *names* branches. This stacking mechanism applies to a
worker skill that accepts a base-branch argument — among the leaf skills that is
`implement-story`, whose grammar is `<story-ref> [base-branch]` — a single positional argument,
never a repeatable flag. `fix-single-bug` takes no base-branch argument at all, so a sprint bug
with an unmerged upstream is escalated instead of routed through this section — see
`sprint-mode.md` **S4**'s bug override. Every branch lives on `upstream` (`MrsBohemian/Cholobots`),
so a stacked PR's base is always a branch in the same repo:

- All of an item's upstream dependencies are `external-complete`/`complete` (satisfied per the
  mode's E3 parameters) **and merged** — omit the second argument entirely: `implement-story <ref>`
  (the worker bases on `upstream/main` itself).
- Exactly one upstream dependency is satisfied but its PR is not yet merged — pass that item's
  recorded `featureBranch` as the positional base: `implement-story <ref> <that item's
  featureBranch>`.
- Two or more upstream dependencies are satisfied but unmerged — name the integration branch
  `epic/<epic-ref>-integration-<story-ref>` (the `scripts/lib/integration_branch.py` convention
  shared with `implement-epic` and `merge_train.py`) and put this prose in the spawn prompt:
  "Before following implement-story, run `git fetch upstream`, create the integration branch
  `epic/<epic-ref>-integration-<ref>` from `upstream/<first parent branch>` and `git merge --no-ff
  --no-edit upstream/<each further parent branch>` into it, `git push -u upstream` it, open a PR
  for it with `env -u GH_TOKEN gh pr create --repo MrsBohemian/Cholobots --base main --head
  epic/<epic-ref>-integration-<ref>` (a synthetic PR needs no reviewer and its own base needs no
  dependency resolution, since `merge-train` retargets/sequences stacked PRs regardless), then
  follow the implement-story skill with arguments '<ref> epic/<epic-ref>-integration-<ref>'." A
  conflict in that merge is a `BLOCKED: integration merge conflict …` return, handled by
  Escalation. Without this PR, `merge_train.py` has no node for the integration branch at all (it
  orders and merges only branches with an open PR) and the child story is silently skipped;
  **hand off to `/merge-train`**: the story's own PR is a stacked PR there — its base is the
  integration branch — so the train retargets and `--merge`s it once the integration branch
  lands, then closes the integration PR once all its parents have merged.

Whether a satisfied upstream's PR has merged is read live: `env -u GH_TOKEN gh pr view <n> --repo
MrsBohemian/Cholobots --json mergedAt` — a non-null `mergedAt` counts as merged for base-selection
purposes even though the dependency was already satisfied by Taiga status.

**Stacked PRs.** A story's PR targets the base it was cut from — `implement-story` itself
creates the PR against that base, never `main` unconditionally. `/merge-train epic:<epic-ref>`
orders parents before children and retargets a child's PR onto `main` once its parent lands; the
conductor does not do this itself and has no equivalent sweep over the batch's open PRs to run.
`baseBranch`/`featureBranch` live only in the state file — after a total state-file loss the report
lists the batch's open PRs for manual review, and for a stacked PR `baseRefName` **is**
recoverable straight from `env -u GH_TOKEN gh pr view <n> --repo MrsBohemian/Cholobots --json baseRefName` (unlike a flat `main`-only
convention, where every PR's base reads back as `main` and tells you nothing).

## Cascade

On an item's `escalated` outcome, compute its transitive dependents. Ask (`AskUserQuestion`)
whether to skip the dependents and continue independent branches (recommended) or pause the batch.
Skipping writes `skipped` to those dependent rows so a legitimate skip does not degenerate into a
spurious all-blocked pause. Under `overseer-conductor`, `AskUserQuestion` is unavailable to
subagents: persist `phase: "paused"` and stop-and-report instead of asking.

## Resume and the ownership guard

The guard is **batch-kind-agnostic**. `/overseer <story-ref>` or `/overseer <bug-ref>` warns and
stops **only** when a batch file under `~/.local/state/overseer/cholobots-<batchkind>-<ref>.json` whose **own** `phase`
is `running` **or** `paused` **and** whose `"project"` is `39` holds that item in a `pending` or
`running` row — in this repo those are epic files, `~/.local/state/overseer/cholobots-epic-<ref>.json`, and sprint
files, `~/.local/state/overseer/cholobots-sprint-<milestoneId>.json`, so the guard checks **both** globs. `paused` counts
because the cascade and all-blocked pauses both leave un-driven items in `pending` rows and expect
the **batch resume ref** to pick them back up — a bare item resume against one of those rows would
start a second owner while the paused batch still lists it. A `complete` or `escalated` batch file
does **not** own its items, or a crashed run would brick every item it ever enumerated — those two
phases are terminal and never revisit a row. If an item is ever held by more than one nonterminal
batch file, report **both** file paths. The batch conductor itself is exempt from this guard.

An item is also owned by `fully-implement-story` when `/tmp/fully-implement-<ref>.json` exists with
a non-terminal top-level `phase` — the same coexistence rule `state.md` applies to a single-item
run applies inside a batch.

Under `overseer-conductor`, the resume ref after **any** per-item gate inside a batch run **must**
be the **batch resume ref** (the epic ref, or `sprint <milestoneId>`), never the item ref, or the
guard blocks the documented stop-and-report resume path.

A `running` batch file whose `updatedAt` is old is reported by path with an option to ignore it;
terminal batch files (`complete`/`escalated`) may be deleted. Unlike `complete`/`escalated`, a
`paused` batch file is **not** deleted and **is** offered for resume by overseer `SKILL.md` Step 1 —
the all-blocked pause and the cascade pause both depend on re-invoking `/overseer` with the batch
resume ref picking a `paused` file back up; that is the loop's only liveness guarantee.

A batch file whose `"project"` is missing or **not `39`** belongs to another repo's conductor —
`~/.local/state/overseer/` is shared across every repo whose conductor uses it, and refs collide
freely across Taiga projects. Never resume it, never overwrite it ([`state.md`](state.md), **State-file
ownership**). Always write `"project": 39` into every batch state file you create.

On resume, **reconcile first**: re-enumerate the batch's items, re-fetch live statuses, and
re-parse the dependency source. Live Taiga beats the state file, subject to the
`baseBranch`/`featureBranch` recoverability note above.

## Spawn-prompt sentinel

**The sentinel contract lives in the spawn prompt, not in the leaf skill** — the leaf skills do
not emit sentinels on their own, so it must be spelled out every time a batch item is driven
through `senior-software-developer`. The mode fragment supplies which worker skill to name here —
`implement-story` for a story, `fix-single-bug` for a sprint bug (`sprint-mode.md` **S4**) — e.g.
for a story: `"Follow the implement-story skill with arguments '<ref>[ <base>]'. End your final
message with exactly one sentinel line: COMPLETE, NEEDS_CLARIFICATION: <question>, or BLOCKED:
<reason>."` (see `SKILL.md`, **Delegation idiom**).
