SPRINT-MODE-PROCEDURE-V1

# Overseer Sprint Mode

The procedure for running `/overseer sprint <milestone-id | name>` (the keyword `milestone` is
accepted as a synonym) — a Taiga **milestone's** mixed stories-and-issues queue driven as a
sequential, dependency-ordered batch through the unchanged per-item machinery. The dependency
graph comes from a **human-confirmed wiki page**, not from an item description.

The keyword is **mandatory**: milestones have no `ref`, only `id`/`slug`/`name`, so a bare number
cannot route here — `SKILL.md` Step 1's bare-number story → issue → epic fall-through is untouched.

Mode-independent batch machinery — enumeration completeness, fallback announcements, the ordering
loop, base selection, cascade, and the resume/ownership guard — lives in
[`batch-loop.md`](batch-loop.md) and is written kind-agnostic. This file carries only what is
sprint-specific and **defers** rather than restating.

## S0: Preflight

At the **start of every sprint-mode turn** (not once at routing — every turn that touches a sprint
run), `Read` **both** this file **and** [`batch-loop.md`](batch-loop.md), and echo the sentinel
`SPRINT-MODE-PROCEDURE-V1` before any Taiga write or worker spawn.

## S1: Resolve the milestone

`get_sprint_board(project=39, sprint=<arg>, with_tasks=false)` — the workflow server resolves an id
or an exact name and returns the milestone's `id`, name, and its stories.

- **No match** → stop and say so. Project 39 has no milestones yet, so this is today's outcome
  until Jon plans one (`plan_sprint`).
- The workflow server has no milestone-listing tool, so there is no substring match and no
  multiple-match case; ask the human for the exact name when the lookup misses
  (`AskUserQuestion`; under `overseer-conductor`: stop-and-report).

## S2: Enumerate both kinds

Trailing arguments after the milestone are issue filter flags (the grammar in
[`bug-sweep-options`](../shared/bug-sweep-options.md)); record them as `issueFilters` (`{options, rawFlags}`).
Sprint mode does not use that include's confirm prompt or `--yes`.
The routed milestone is the only milestone: if the trailing flags contain `--milestone` (or `--milestone=`),
stop and tell the operator to drop it, because include values OR together and would admit issues from a
second milestone while stories stay on the routed one. `--exclude-milestone` is allowed.

Enumerate **both** (completeness per `batch-loop.md`; defer — do not restate it):

- Stories: the `get_sprint_board` result from S1 (every story in the milestone, one call).
- Issues: `python3 scripts/taiga_select_issues.py --purpose sprint --milestone <id> <issue filter flags>`. Use
  `selected` (every issue type, closed included, no triage gate, so S2.6 skip-complete still sees closed
  issues via each row's `status`). Filters apply to issues only; stories are enumerated as before.
  Exit 2 means stop and show stderr. The selector pages issues internally.

## S2.5: Coexistence check (before spawning any worker)

Sprint mode enumerates a **mixed** story+issue queue, so it must check every orchestrator in
[`state.md`](state.md)'s **Coexistence with the unattended orchestrators** table, not just the
epic-only pair `epic-mode.md`'s E0.5 checks:

1. Glob `/tmp/implement-epic-*.json`. If any file with top-level `"phase": "running"` lists, under
   `stories[].ref`, a story S2 enumerated with a non-terminal `status`, **stop and report**:
   `implement-epic` owns it. Name the file path.
2. Glob `/tmp/fully-implement-*.json`. If any file names a story S2 enumerated and its top-level
   `phase` is non-terminal, stop the same way — `fully-implement-story` owns it.
3. Read `/tmp/parallel-fix-bugs.json`. If it lists, under `bugs[].ref`, an issue S2 enumerated with
   a non-terminal status, stop the same way — `parallel-fix-bugs` owns it.
4. Read `/tmp/fully-fix-bugs.json`. If its top-level `phase` is not `"complete"` and it lists,
   under `bugs[].ref`, an issue S2 enumerated whose `status` is not `"complete"` or
   `"escalated"`, stop the same way — `fully-fix-bugs` owns it.

A file that matches none of the checks above — including a `fully-fix-bugs` file whose top-level
`phase` is already `"complete"`, which is live at `"fixing"` rather than `"running"` — owns
nothing; note it and continue.
Re-run this check **on resume** too — a live run may have started while this sprint was paused.

## S2.6: Resolve statuses

Resolve statuses **live and per type** — stories and issues have separate enums, never cross them:
`get_story(project=39, ref=N)` and `get_issue(project=39, ref=N)` each return the status name and
`is_closed`.

**Skip-complete** records `external-complete` (dependency-satisfying, never entering the per-item
Steps) for:

- a **story** whose live status **NAME** is `Done`, `Ready for review`, or `Ready for test`;
- an **issue** whose live status carries **`is_closed: true`** — match by the flag, never by
  remembered names or ids (`Closed` and `Rejected` today, but the flag is the contract).

## S3: Discover and confirm the wiki page

There is no mechanical milestone → wiki mapping, so the page is **discovered and confirmed**,
never derived.

1. `get_wiki(project=39)` (no slug) lists every page — `home`, the `deep-dive-*` pages,
   `repo-hygiene-2026-11-06` and `cross-project-execution-order` today, so an exhaustive sweep is
   a cheap worst case. Exclude `cross-project-execution-order`: it is the namespaced roadmap
   page, never a sprint diagram.
2. **Rank** candidates by slug-token overlap with the milestone's slug and name.
3. **Verify by coverage**, in rank order: `get_wiki(project=39, slug=<slug>)`, pipe its content
   through `python3 scripts/lib/parse_roadmap.py -` by heredoc in **legacy bare-int mode** — no
   `--default-namespace`, never bare `python3`, never `python3 -c` — and count how many of the
   S2-enumerated refs appear as keys (`str(ref)` lookups, per the parser's output contract).
   **Stop early at ≥50% coverage.**
4. **Confirm with the human**, echoing `issueFilters` in the confirmation text so the filters are visible at this gate (`AskUserQuestion`; under `overseer-conductor`: stop-and-report). This
   gate fires **even at zero coverage** — present the top-ranked candidate with its coverage number
   **and the list of matched refs**, not just a percentage: legacy-mode namespace collapse can
   produce phantom matches by number collision alone. Options: **accept** / **name another page** /
   **proceed with no diagram**.
5. Record `wikiSlug` (and `wikiPageId` when the read returns it) in the state file so a resume
   never re-discovers.

None of the current project-39 pages carries a `## Dependency Diagram` section, so the parse yields
`{}` for all of them and the confirmation gate lands on "proceed with no diagram" until someone
writes a sprint diagram page.

The **fallback announcement** follows the human's choice, using `batch-loop.md`'s strings
verbatim: a `{}` parse, a cyclic graph, or "proceed with no diagram" → the *"no dependency graph
parsed, falling back to ref-ascending"* string; a non-empty map with zero edges → the *"ref-ascending
IS the correct topological order"* string.

Diagram refs pointing **outside** the sprint follow `batch-loop.md`'s out-of-batch dependency rule; issues filtered out by `issueFilters` are simply not batch items and fall under the same rule.
Sprint items **absent from the diagram** are ordered LAST and flagged "no diagram node".

## S4: Loop — `batch-loop.md`'s ordering loop, with these sprint parameters

- **Items** are the **unified** story + issue queue; the lowest-ref tie-break applies **regardless
  of kind**.
- A **dependency is satisfied** per kind: a story dep whose row is `external-complete`/`complete`
  or whose live status NAME is `Ready for review`, `Ready for test`, or `Done`; an issue dep whose
  row is `external-complete`/`complete` or whose live status has `is_closed: true`.
- The **batch resume ref** is `sprint <milestoneId>`.
- A sprint item with 2+ unmerged upstreams has **no merge-train-compatible integration-branch
  convention**: `scripts/lib/integration_branch.py`'s `INTEGRATION_BRANCH_RE` matches only
  `epic/<n>-integration-<n>`, so an invented `sprint/...` name would parse as `None` everywhere
  `merge_train.py` looks for a synthetic integration PR. If the item carries an epic (the story's
  epic link; issues never have one), use that epic's ref and the ordinary
  `epic/<epic-ref>-integration-<ref>` convention from `batch-loop.md`'s **Base selection** section.
  Otherwise this case has no supported branch name — record the item `escalated` with reason
  "multi-parent sprint item has no integration-branch convention" rather than inventing one; see
  **Known limitations**.
- A **bug** with exactly one satisfied-but-unmerged upstream has the same problem one level down:
  `batch-loop.md`'s **Base selection** stacks a story from its dependency's `featureBranch` via
  `implement-story <ref> <base>`'s positional base argument, but `fix-single-bug` takes no such
  argument — it always branches from `upstream/main` and opens its PR against `main`. Record the
  item `escalated` with reason "sprint bug has an unmerged dependency; fix-single-bug has no
  base-branch argument" rather than routing it through `fix-single-bug` anyway; see **Known
  limitations**.

Each selected item drops into the **existing per-item lifecycle unchanged**, with its own Gate A /
Gate B and its own `~/.local/state/overseer/cholobots-<kind>-<ref>.json`:

- a **story** → `senior-software-developer` with `analyze-story`, then `senior-software-developer`
  with `implement-story`;
- a **bug with every upstream merged (or none)** → `senior-software-developer` with `analyze-bug`,
  then `senior-software-developer` with `fix-single-bug` — **never `fix-bugs`**, which takes no
  argument and sweeps the whole project (`SKILL.md` Known limitations).

Base selection, the `gh pr view` merge-state read, and stacked-PR base selection are
`batch-loop.md`'s — do not restate them.

## S5: Cascade — `batch-loop.md`'s cascade, unchanged

Same parameters as S4; the pause writes `phase: "paused"` to
`~/.local/state/overseer/cholobots-sprint-<milestoneId>.json`.

## S6: Report

A per-item outcome table **with a `kind` column**, including `external-complete` and "no diagram
node" rows; the chosen wiki page and its coverage; the fallback announcement if one fired; and any
state-file-loss gap; and every `filedItems` entry with the `spawnCount`. An item closes on
evidence ([`run-discipline.md`](run-discipline.md)); the milestone's own status is Jon's call.

## State file

`~/.local/state/overseer/cholobots-sprint-<milestoneId>.json`. This reuses the `cholobots-<kind>-<ref>.json` naming
shape with `kind: "sprint"` and the **milestone id in the ref slot**, because sprints have no ref.

```json
{
  "kind": "sprint",
  "project": 39,
  "milestoneId": 1,
  "milestoneName": "Sprint 1",
  "wikiPageId": null,
  "wikiSlug": null,
  "issueFilters": { "options": {}, "rawFlags": "" },
  "phase": "running",
  "currentItem": null,
  "deps": {},
  "updatedAt": "2026-11-07T00:00:00Z",
  "spawnCount": 0,
  "filedItems": [],
  "items": [
    {
      "kind": "bug",
      "ref": 4,
      "taigaId": null,
      "status": "pending",
      "blockedOn": null,
      "prNumber": null,
      "featureBranch": null,
      "baseBranch": null,
      "outcome": null
    }
  ],
  "escalationReason": null
}
```

`phase` is `running | paused | complete | escalated`. Each item row's `kind` is `story | bug` and
its `status` is `pending | running | complete | escalated | skipped | external-complete`;
`blockedOn` is `null` or the reason a `pending` row is not agent-actionable (`batch-loop.md`,
**Idle exit**). `spawnCount` and `filedItems` are the run's budgets and runtime findings
([`run-discipline.md`](run-discipline.md)); a filed item without `"adopted": true` is left out of
`items` even when it sits in the milestone.
`updatedAt` is an ISO string so staleness is computable — the conductor has no `ls`/`stat` grant and
cannot read file mtime. `"project": 39` is mandatory — `~/.local/state/overseer/` is shared across every repo whose
conductor uses it and refs collide freely across Taiga projects
([`state.md`](state.md), **State-file ownership**).

Per-item `~/.local/state/overseer/cholobots-<kind>-<ref>.json` files stay authoritative for intra-item state; on
conflict the per-item file plus live Taiga win over the sprint file. `batch-loop.md`'s
`baseBranch`/`featureBranch` recoverability note applies identically — for a stacked PR,
`baseRefName` reads back straight from GitHub after a state-file loss.

## Resume and the ownership guard

`batch-loop.md`'s **Resume and the ownership guard** section applies verbatim, with
`~/.local/state/overseer/cholobots-sprint-<milestoneId>.json` as the batch file and `sprint <milestoneId>` as the batch
resume ref. An item may legitimately sit in **both** an epic batch and a sprint batch — the guard
reports both paths. On resume, reconcile first: re-enumerate both kinds (for issues, re-run the selector with the persisted `issueFilters.rawFlags`, never the bare `sprint <milestoneId>` set), re-fetch live statuses per
type, and re-parse the recorded wiki page (`wikiSlug`) rather than re-running discovery.

## Known limitations

**No milestones and no sprint diagrams yet.** Project 39 has no milestones, so S1 misses until Jon
plans one, and none of its wiki pages carries a `## Dependency Diagram` section, so a real sprint
run lands on the confirmed-fallback (ref-ascending) path. Authoring those pages is separate work.

**Exact milestone names only.** The workflow server resolves a milestone by id or exact name and
has no listing tool, so S1 cannot rank or offer candidates the way a listing would.

**Cost under `overseer-conductor`.** Two human gates per item, times N items, means 2N
stop-and-report run terminations on the delegated path — plus S1's name-miss and S3's
wiki-confirmation gates. A sprint run is not unattended under `overseer-conductor`.

**No integration-branch convention for a multi-parent bug.** `scripts/lib/integration_branch.py`
names branches `epic/<epic-ref>-integration-<ref>`; an issue never carries an epic, so a bug with
2+ unmerged upstreams inside a sprint has no supported integration-branch name at all (S4).
Extending the tooling to cover this is separate work — until then, escalate that item rather than
inventing a branch name `merge_train.py` cannot parse.

**No stacked-base support for a single-parent bug.** `fix-single-bug` — the only bug worker sprint
mode is permitted to spawn — always branches from `upstream/main` and opens its PR against `main`;
it has no positional base-branch argument the way `implement-story` does. A sprint bug with exactly
one satisfied-but-unmerged upstream therefore cannot be stacked on that upstream's `featureBranch`
(S4). Giving `fix-single-bug` a base-branch argument is a leaf-skill change, so escalate that item
rather than running it against `main` anyway.
