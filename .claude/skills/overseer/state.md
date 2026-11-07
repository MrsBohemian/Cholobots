# Overseer State Machine

`SKILL.md`'s **phase state machine** — state-file paths, the JSON schema, per-kind phase
sequences, the terminal-phase rule, cross-repo state-file ownership, and coexistence with the
unattended orchestrators. Extracted so `SKILL.md` stays inside the post-compaction
re-attachment window (see its **Retention budget** note). `Read` this before writing or
resuming any `~/.local/state/overseer/cholobots-*.json`.

## The phase state machine

State lives under `~/.local/state/overseer/`, not `/tmp`, so a WSL or host restart does not wipe a
run mid-flight. Each file is named `cholobots-<kind>-<ref>.json`: the `cholobots` prefix (the roadmap
namespace for project 39) keeps this repo's files apart from other repos' conductors that share the
directory. For an epic run the epic description's `## Overseer hand-off` section
([`batch-loop.md`](batch-loop.md), **Idle exit**) is the durable hand-off; the state file is the
working copy.

Persist a state file at `~/.local/state/overseer/cholobots-<kind>-<ref>.json` (`<kind>` is `story`, `bug`, `epic`, or
`sprint` — epic schema in [`epic-mode.md`](epic-mode.md), sprint schema in
[`sprint-mode.md`](sprint-mode.md); the sprint file also carries `issueFilters` (`{options, rawFlags}`)). For a `new`/free-form request the ref is unknown until the
planner creates the Taiga item — keep state in `~/.local/state/overseer/cholobots-pending.json` first, then write the
full state directly to the final `~/.local/state/overseer/cholobots-<kind>-<ref>.json` path once you have the ref (no
rename — the tool surface has no `mv`; the pending file is superseded and ignored from then on).

The pending path is **fixed and unqualified**, so it holds exactly one un-reffed run at a time.
That is fine for `/overseer` in the main thread (one run per session — see **Report**), but a
spawner that runs two `overseer-conductor` agents concurrently on two `new` requests would have
them overwrite each other. If your spawn prompt supplies a pending-state path, use that instead;
otherwise, before writing `~/.local/state/overseer/cholobots-pending.json`, check whether it already exists with a
non-terminal `phase` and `"project": 39` — if it does, another un-reffed run is live: stop and
report rather than clobber it.

State schema:

```json
{
  "project": 39,
  "kind": "story",
  "ref": 21,
  "taigaId": null,
  "phase": "implementing",
  "prNumber": null,
  "featureBranch": null,
  "remediateStateFile": null,
  "rebaseAttempted": false,
  "buildFailureCount": 0,
  "escalationAttempts": 0,
  "qaBugRefs": [],
  "qaFixPRs": [],
  "escalationReason": null,
  "roadmapDeps": null,
  "verifyCycles": 0,
  "spawnCount": 0,
  "filedItems": []
}
```

`taigaId` is the internal id `scripts/taiga-story.sh` returns, needed only by
`scripts/taiga-comment.sh`; the MCP tools are ref-based, so it may stay `null` until a script call
fills it.

`verifyCycles`, `spawnCount`, and `filedItems` carry [`run-discipline.md`](run-discipline.md)'s
budgets and runtime findings: `verifyCycles` counts verify → bug → fix cycles on this item (the
third failure escalates to Jon), `spawnCount` counts every worker spawned for this run, and each
`filedItems` entry is `{ref, kind, filedBy, reason, adopted}` for an item filed mid-run and left in
the backlog. Inside an epic or sprint run, `spawnCount` and `filedItems` live in the batch file and
the per-item file keeps only `verifyCycles`.

`roadmapDeps` is written by **Step 1a** — `null` when the roadmap read was gracefully skipped or
the ref is not on the master roadmap (the common case), an empty array `[]` when the ref is on the
roadmap and every upstream dep is closed, or a non-empty array of `{ref, subject, statusName,
isClosed}` entries when there are unmet/unknown upstreams to surface at Gate A.

`remediateStateFile` is `/tmp/remediate-pr-<prNumber>.json` once Step 4 starts — it is
`scripts/remediate_pr.py`'s own state file, not one you write; you only record its path so a
resumed conductor run knows to pass `--resume` instead of starting fresh. `rebaseAttempted`
mirrors the script's exit-13 handling in [`remediation.md`](remediation.md): once a rebase has
been attempted for this PR, a second `NEEDS_REBASE` escalates instead of looping.
`buildFailureCount` is a read-only mirror of the script's own `buildFailureCount` — the script owns the
counter and its dedup logic, so you never increment it yourself. `qaFixPRs` is written only on a QA
`FAIL` that enters another remediation round — each entry is `{bugRef, prNumber}` for a sub-PR
opened against `featureBranch` (Step 5); it stays populated through `awaiting-qa-merge` so a resume
can re-check every sub-PR's merge status live rather than trusting the phase alone.

**Story phases:** `routing → analyzing → (gate A: awaiting-gate-a under overseer-conductor) → implementing → remediating → (gate B: awaiting-gate-b under overseer-conductor) → [qa → remediating → awaiting-qa-merge] → complete | escalated`
**Bug phases:** `routing → analyzing → (gate A: awaiting-gate-a under overseer-conductor) → fixing → remediating → (gate B: awaiting-gate-b under overseer-conductor) → [qa → remediating → awaiting-qa-merge] → complete | escalated`
**Epic phases:** `running → paused | complete | escalated` (E0–E5 procedure + state schema: [`epic-mode.md`](epic-mode.md); shared machinery: [`batch-loop.md`](batch-loop.md)).
**Sprint phases:** `running → paused | complete | escalated` (S0–S6 procedure + state schema: [`sprint-mode.md`](sprint-mode.md); shared machinery: [`batch-loop.md`](batch-loop.md)).

The two human gates are identical for stories and bugs: Gate A after analysis (plan approval before any implementation), Gate B after the PR is stable (hand-back vs. optional QA).

`qa` is optional — entered only when the human opts into a `senior-manual-testing-specialist` pass at Gate B; otherwise Gate B advances straight to finalize. A QA `FAIL` may loop back to `remediating` for another round before a second pass (Step 5).

**Terminal phases** — the state file MUST end in one of these:
- `"complete"` — work implemented, PR open and stable, comments resolved, story/bug handed back for review.
- `"escalated"` — any blocker, with a populated `escalationReason`.
- `"paused"` — **epic and sprint runs only**, resumable by re-invoking `/overseer <epic-ref>` / `/overseer sprint <id|name>`; see [`epic-mode.md`](epic-mode.md) / [`sprint-mode.md`](sprint-mode.md).
- `"awaiting-gate-a"` — **story and bug runs under `overseer-conductor` only**, parked when Step 2's plan is ready (`ANALYSIS_COMPLETE`) but Gate A's approve/request-changes/cancel decision has not been made; resumable by re-invoking `/overseer <ref>`, which re-presents the captured plan and `roadmapDeps` advisory at Gate A instead of re-spawning the planner.
- `"awaiting-gate-b"` — **story and bug runs under `overseer-conductor` only**, parked when Step 4 reached `STABLE` but Gate B's hand-back/QA decision has not been made; resumable by re-invoking `/overseer <ref>`, which re-presents the recorded PR outcome at Gate B instead of re-driving Step 4.
- `"awaiting-qa-merge"` — **story and bug runs only**, parked after a QA-round sub-PR (or several, tracked in `qaFixPRs`) reached stable but has not been confirmed merged into `featureBranch`; resumable by re-invoking `/overseer <ref>`, which re-checks every `qaFixPRs` entry live before proceeding (Step 5).

If your final action would leave `phase` at any in-progress value (`analyzing`, `implementing`, `fixing`, `remediating`, `qa`, `running`, …), you are doing it wrong — **escalate instead** (an epic/sprint parks at `paused`, a story/bug awaiting a Gate A/B decision under `overseer-conductor` parks at `awaiting-gate-a`/`awaiting-gate-b`, a story/bug awaiting a QA-fix merge parks at `awaiting-qa-merge`, none of these is left `running`/`remediating`/`analyzing`).

**State-file ownership (cross-repo collision).** `~/.local/state/overseer/` is shared across every repo whose conductor uses it (the internal-infra and homelab conductors among them), and refs collide freely across Taiga projects, so a `~/.local/state/overseer/cholobots-<kind>-<ref>.json` you did not write may still belong to another repo's overseer run if that repo reused the `cholobots` prefix. Before resuming any state file, check its `"project"` field: if it is missing or **not `39`**, it is not yours — do not resume it, do not overwrite it, and route the ref through fresh routing (Step 1) instead. Always write `"project": 39` into every state file you create — the epic and sprint files follow the same rule.

**Coexistence with the unattended orchestrators.** `implement-epic` / `fully-fix-bugs` / `parallel-fix-bugs` / `fully-implement-story` track their own runs in separate state files (all of them run in git worktrees here). Before routing a ref, read each of these:

| Orchestrator | State file | Ref list |
|---|---|---|
| `implement-epic` | `/tmp/implement-epic-<epic-ref>.json` (glob `/tmp/implement-epic-*.json`) | `stories[].ref` |
| `parallel-fix-bugs` | `/tmp/parallel-fix-bugs.json` | `bugs[].ref` |
| `fully-fix-bugs` | `/tmp/fully-fix-bugs.json` | `bugs[].ref` |
| `fully-implement-story` | `/tmp/fully-implement-<ref>.json` | that single `ref`, owned while its top-level `phase` is non-terminal |

A ref is **owned** when the orchestrator's file shows a non-terminal, in-progress state for that
ref (a `"phase": "running"` top-level file with the ref in a non-terminal-status list entry, or a
single-ref file with a non-terminal `phase`). `fully-fix-bugs` uses its own vocabulary: it is live
while its top-level `phase` is anything but `"complete"` (it writes `"phase": "fixing"`), and a
bug in it is owned while its `bugs[].status` is not `"complete"` or `"escalated"`. If the ref is owned, tell the human and stop — never
manually drive a story one of these orchestrators owns. An **epic ref** is likewise owned when
`/tmp/implement-epic-<ref>.json` carries `phase: "running"` — epic mode refuses it (E0.5 in
[`epic-mode.md`](epic-mode.md)); a **sprint's milestone** is checked the same way against every
story/bug it would enumerate (S2.5 in [`sprint-mode.md`](sprint-mode.md)).
