EPIC-MODE-PROCEDURE-V1

# Overseer Epic Mode

The procedure for running `/overseer <epic-ref>` — sequential, dependency-ordered epic runs through
the unchanged per-story machinery. Mode-independent batch machinery (enumeration completeness,
fallback announcements, ordering, base selection, cascade, resume/ownership guard) lives in
[`batch-loop.md`](batch-loop.md) and is written kind-agnostic; this file carries only what is
epic-specific.

Both files exist because `SKILL.md` is already close to its 24000 B retention budget — far past the
~5000-token per-skill post-compaction re-attachment window that Claude Code documents. Inlining
E1–E5 there would push Escalation, Taiga discipline, and Report out of that window. This is a
**discipline prompt, not enforcement**: a `Read`-loaded fragment is ordinary transcript content with
no re-attachment guarantee of its own, which is exactly why E0 re-reads it every turn.

## E0: Preflight

At the **start of every epic-mode turn** — not once at routing, every turn that touches an epic run
— `Read` **both** this file **and** [`batch-loop.md`](batch-loop.md), then echo the sentinel
`EPIC-MODE-PROCEDURE-V1` before any Taiga write or worker spawn.

## E0.5: Coexistence check (before anything else)

`implement-epic` is the **unattended, worktree-slot** orchestrator for an epic; epic mode is the
**attended, sequential** one. Never both on one epic, and never manually drive a story
`implement-epic` (or `fully-implement-story`) owns (root `CLAUDE.md`,
[`.claude/autonomous-pipeline.md`](../../autonomous-pipeline.md) § Pipeline State, and
[`state.md`](state.md)'s **Coexistence with the unattended orchestrators** table).

1. Read `/tmp/implement-epic-<ref>.json`. If it exists with top-level `"phase": "running"`,
   **stop and report**: `implement-epic` owns this epic. Name the file path in the report.
2. Glob `/tmp/implement-epic-*.json`. If any file with `"phase": "running"` lists, under
   `stories[].ref`, a story that E1 would enumerate for this epic **with a non-terminal `status`**,
   stop the same way — the other run owns that story even though its epic ref differs.
3. Glob `/tmp/fully-implement-*.json`. If any file names a story E1 would enumerate and its
   top-level `phase` is non-terminal, stop the same way — `fully-implement-story` owns that story.

A `complete`, `escalated`, or otherwise non-`running` `implement-epic`/`fully-implement-story` file
owns nothing; note it and continue.

## Routing detail

`SKILL.md` **Step 1** routes a bare number by falling through `get_story` → `get_issue` →
`get_epic_overview` (each `project=39, ref=<n>`). Each miss on the wrong type **raises** a
not-found error rather than returning null or an empty result — that raise **is** the
miss-try-next signal, not a failure to handle. The explicit `epic <n>` form bypasses the
fall-through entirely. Everything here is ref-based; no internal id is needed.

## E1: Enumerate and parse

1. `get_epic_overview(project=39, ref=<n>)` — returns the epic's description (the dependency
   source) and every linked story with its status, in one call.
2. For each story, `get_story(project=39, ref=<story-ref>)` for its live status **name**, tags and
   description; never trust a remembered status.
3. **Skip-complete**: a story already at status name Done, Ready for review, or Ready for test
   **and** with no prior epic-run history is recorded `external-complete`, counts as
   dependency-satisfying, and never enters Steps 2–6. A row this epic's own run already completed
   keeps status `complete` along with its `prNumber`, `featureBranch`, and `baseBranch`. An
   `external-complete` row still needs its own `prNumber`/`featureBranch` recorded — a dependent's
   base-selection rule (`batch-loop.md`) may need this prerequisite's branch if its PR turns out
   unmerged: look for a `taiga-comment.sh`-posted `PR: .../pull/<n>` comment on the story, or
   `env -u GH_TOKEN gh pr list --repo MrsBohemian/Cholobots --search "<ref> in:title" --state all`,
   then `env -u GH_TOKEN gh pr view <n> --repo MrsBohemian/Cholobots --json
   headRefName,baseRefName,mergedAt` for the branch and merge state. No PR found (a no-PR story)
   is normal — leave both `null` per **Known limitations**.
4. Parse the epic description on stdin via heredoc: `python3 scripts/lib/parse_roadmap.py -` in
   **legacy bare-int mode** — do **not** pass `--default-namespace` (that is `routing.md` Step 1a's
   wiki mode, a separate and incompatible grammar). Never bare `python3`, never `python3 -c`.
5. Output contract: keys are JSON **strings** (look up by `str(ref)`), dep elements are ints, dep
   lists sorted, key order is insertion order, and unparseable input gives **exit 0 with `{}`**. A
   cyclic diagram is caught the same way `implement-epic`'s cycle check catches one — treat it as
   unparseable and trigger `batch-loop.md`'s "no dependency graph parsed" fallback announcement.
6. A story in the batch file's `filedItems` without `"adopted": true` is left out of `stories`
   (`batch-loop.md`, **Runtime findings**), even when it is linked to the epic.
7. A story enumerated in the epic but **absent from the parsed map** is ordered LAST and flagged
   "no diagram node" per `batch-loop.md`.
8. Fallback announcements and the out-of-epic dependency rule are `batch-loop.md`'s — use its strings
   **verbatim** rather than restating them here.

## E2: State file

`~/.local/state/overseer/cholobots-epic-<ref>.json`:

```json
{
  "project": 39,
  "kind": "epic",
  "ref": 46,
  "taigaId": null,
  "phase": "running",
  "currentStory": null,
  "deps": {},
  "updatedAt": "2026-11-07T00:00:00Z",
  "spawnCount": 0,
  "filedItems": [],
  "stories": [
    {
      "ref": 47,
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

`phase` is `running | paused | complete | escalated`. Each story row's `status` is
`pending | running | complete | escalated | skipped | external-complete`; `blockedOn` is `null` or
the reason a `pending` row is not agent-actionable (`batch-loop.md`, **Idle exit**). `spawnCount`
and `filedItems` are the run's budgets and runtime findings ([`run-discipline.md`](run-discipline.md)). `"project": 39` is the first
key and is **mandatory** — `~/.local/state/overseer/` is shared across every repo whose conductor
uses it and refs collide freely across Taiga projects ([`state.md`](state.md), **State-file
ownership**). `updatedAt` (ISO string) makes staleness computable: the conductor has no `ls`/`stat`
grant and cannot read file mtime. `taigaId` fields are optional (`state.md`).

Per-story `~/.local/state/overseer/cholobots-story-<ref>.json` files (each also carrying `"project": 39`) stay
authoritative for intra-story state; on conflict the per-story file plus live Taiga win over the
epic file. `baseBranch`/`featureBranch` recoverability is `batch-loop.md`'s note and applies here —
for a stacked PR, `baseRefName` is recoverable straight from GitHub after a state-file loss, so E5 need
only report open PRs for review rather than claim the gap is unrecoverable.

## E3: Loop — `batch-loop.md`'s ordering loop, with these epic parameters

- **Items** are stories.
- A **dependency is satisfied** when its row is `external-complete`/`complete`, or its live status
  NAME is Ready for test, Ready for review, or Done.
- The **batch resume ref** is the epic ref (`/overseer <epic-ref>`).
- The **integration-branch prefix** is `epic/<epic-ref>-integration-` (so a multi-parent story gets
  `epic/<epic-ref>-integration-<ref>`, per `scripts/lib/integration_branch.py`).

Story spawn follows `batch-loop.md`'s **Base selection** and **Spawn-prompt sentinel** sections —
do not restate them here. The conductor **never creates a branch**; it only names them.

## E4: Cascade — `batch-loop.md`'s cascade, unchanged

Same parameters as E3. The pause writes `phase: "paused"` to `~/.local/state/overseer/cholobots-epic-<ref>.json`. Under
`overseer-conductor`, `AskUserQuestion` is unavailable to subagents, so the cascade choice degrades
to persisting `paused` and stopping-and-reporting.

## E5: Report

A per-story outcome table — including `external-complete` rows and rows flagged "no diagram node" —
plus any E1 fallback announcement. If the state file was lost mid-run, list the epic's open PRs (with their
live `baseRefName`) for manual review rather than claiming the state is unrecoverable — stacked-PR
base branches read back straight from GitHub. List every `filedItems` entry and the `spawnCount`.
A story closes on evidence ([`run-discipline.md`](run-discipline.md)); the epic's own status is
Jon's call (the workflow server has no epic-status tool; report the status you recommend). Suggest
`/merge-train epic:<epic-ref>` as the natural next step once every story in the epic has a stable PR.

`/clear` does not apply mid-epic: the epic run's own state lives in `~/.local/state/overseer/`, but a cleared context
loses the E0 sentinel and every in-flight worker relay.

## Resume and the ownership guard

`batch-loop.md`'s **Resume and the ownership guard** section applies verbatim, with
`~/.local/state/overseer/cholobots-epic-<ref>.json` as the batch file and the epic ref as the batch resume ref. On
resume, **reconcile first**: re-enumerate the epic's stories, re-fetch live statuses, and re-parse
the epic description's diagram. Re-run E0.5 as well — an `implement-epic` or `fully-implement-story`
run may have started while this one was paused.

## Known limitations

**No-PR stories.** `implement-story` always opens a PR, so dependency satisfaction is by Taiga status
alone — a pure decision/research story still needs its status moved to Ready for review / Done by
hand before its dependents unblock. There is no special no-PR mode.

**Cost under `overseer-conductor`.** Two human gates (Gate A, Gate B) per story × N stories means
**2N stop-and-report run terminations** on the delegated path. The epic's "no epic-level start gate,
auto-run" design does not make an epic run unattended under `overseer-conductor`; each gate still
needs a human relay, and each resume must use the **epic ref**, never the story ref.

**One-directional coexistence.** E0.5 stops an epic run that `implement-epic` or
`fully-implement-story` owns, but neither of those orchestrators checks for a `running`
`~/.local/state/overseer/cholobots-epic-<ref>.json`. If that bites, file a follow-up `pipeline` story rather than working
around it here.
