# Autonomous Story Implementation

## Skills

| Skill | Description | Usage |
|---|---|---|
| `/design-story <ref>` | Interactive Discord command UX and dashboard design clarification | Produces reply-flow sketches, persona voice, role analysis, phone-width notes for dashboards. Run before `/analyze-story` |
| `/fully-design-stories <epic-ref>` | Sequential interactive design for all design/ui stories in an epic | Fetches tagged stories, invokes `/design-story` one-by-one with user interaction, state file for resumption |
| `/fully-implement-story <ref> [base-branch]` | Autonomous end-to-end story implementation | Story must be in `Ready` status with `llm-analyzed` tag. `base-branch` defaults to `main`; pass a base to stack this story on a predecessor branch |
| `/implement-task <ref> <branch>` | Implement a single task from a story | Used by orchestrator, one task per fresh agent. If `<branch>` doesn't exist yet, it is created from the story's base branch, not from `main` |
| `/finalize-story-pr <ref> <branch> [base-branch]` | Run affected checks, push branch, create PR, update Taiga | Used by orchestrator after all tasks complete. The PR targets `base-branch`; defaults to `main` |
| `/pr-comments-resolve <pr>` | Resolve PR review comments autonomously | Evaluates, fixes, replies, resolves threads, and captures worthy out-of-scope discoveries as Taiga stories/bugs |
| `/pr-converse <pr>` | Address feedback on a PR thoughtfully | Attended counterpart of `/pr-comments-resolve` |
| `/rebase-branch <pr> [target]` | Rebase a PR branch onto target (default: the PR's own base branch) | Resolves conflicts using story/epic context, escalates when stuck |
| `/fix-story-bug <ref> <branch>` | Fix a bug with a sub-PR to a feature branch | Adds automated regression tests, or manual test steps for handler wiring |
| `/triage-bugs [--dry-run] [--refs ...]` | Sweep untriaged bugs per `docs/bug-triage.md` | Sets severity, priority, surface/source tags and `triaged`; routes to Needs Info / Rejected; reports Urgent bugs. Runs before analysis and fixing |
| `/analyze-bug <ref>` / `/analyze-story <ref>` | Root-cause analysis / implementation plan | Sets `llm-analyzed`; stories move to `Ready` |
| `/analyze-build-failure pr-<number>` | Diagnose a failing check without fixing it | Read-only companion to `/fix-build-failure` |
| `/fully-analyze-bugs [concurrency] [filter flags] [--yes]` | Autonomously analyze all open unanalyzed bugs | Parallel analysis (default 3), question queue for missing info; only `triaged` bugs |
| `/fully-analyze-stories <epic-ref> [N]` | Autonomously analyze all stories in an epic | Parallel analysis (default 3), question queue for missing info |
| `/fully-fix-bugs [filter flags] [--yes]` | Autonomously fix all open bugs end-to-end | Sequential fixing, one worktree per bug, PR monitoring, remediation, Taiga updates |
| `/fix-bugs [filter flags] [--yes]` | Fix open bugs one at a time, each on its own branch/PR | Selection via the shared bug selector |
| `/fix-single-bug <ref> [--mark-done-on-merge]` | Fix a single bug on its own branch off `upstream/main` | Used by fully-fix-bugs orchestrator |
| `/parallel-fix-bugs [N] [filter flags] [--yes]` | Fix all open bugs in parallel across worktree slots | Up to N concurrent slots (default 3), each runs fix-and-remediate-bug |
| `/fix-and-remediate-bug <ref>` | Fix a single bug with TDD + PR remediation | Composite skill used by parallel-fix-bugs |
| `/fix-build-failure pr-<number>` | Fix a failing check on a PR branch | Today `py_compile` and script tests; CI once story #35 lands |
| `/implement-epic <ref> [n]` | Parallel story implementation across worktree slots | Processes all analyzed stories in an epic, up to n concurrently (default 1), respecting dependency ordering |
| `/implement-story <ref>` | Attended single-story implementation | Same task delegation as `/fully-implement-story`, stops at the PR |
| `/remediate-pr <pr> [--taiga-id <ref>] [--taiga-type story\|issue]` | Monitor PR stability, auto-fix checks, resolve comments, escalate | Delegates to `scripts/remediate_pr.py`; shared loop used by /fully-implement-story, /fully-fix-bugs, /merge-train; also re-triggers a Codex review pass on resume from `awaiting_fix`/`awaiting_comment_resolution` |
| `/merge-train <PRs... \| epic:<ref>>` | Sequential PR rebase, validate, and merge | Processes a queue of PRs or discovers them from an epic's completed stories |
| `/codex-review-pr <pr>` | One Codex review pass on a PR | Called by every PR-creating skill per `.claude/skills/shared/codex-review.md` |
| `/validate-test-plan <ref>` | Validate story test plan against testing strategy | Checks level placement, ownership rules, redundancy; corrects and comments on story |
| `/review-tests <branch\|pr>` | Review test code changes against testing strategy | Fixes placement, removes redundancy, rewrites anti-patterns; comments on PR/story with WHY |
| `/research-spike <ref>` | Time-boxed investigation that ends in a written finding | Comments the result on the item |
| `/reset-epic-stories <epic-ref> <story-refs...> [--close-prs] [--dry-run]` | Reset failed or partial epic story runs | Removes story worktrees, branches and state files, resets Taiga statuses, optionally closes PRs |
| `/remediate-taiga-audit <manifest-path> [--audit <report-path>]` | Apply reviewed Taiga fixes from a manifest built from the `scripts/taiga_audit.py` report | Milestone assignments and epic links only |
| `/overseer <request \| story-ref \| bug-ref \| epic-ref \| epic <n> \| sprint <id\|slug> [filter flags] \| new>` | Interactive, human-gated conductor for the full idea → PR → hand-back lifecycle | Routes to analyze → Gate A (plan approval) → implement/fix → PR → remediate → Gate B (hand-back vs. optional QA); epic-ref and sprint runs batch a dependency-ordered set of stories/bugs; attended and sequential: never shares an epic or bug queue with `/implement-epic`, `/parallel-fix-bugs`, or `/fully-fix-bugs` |
| `/roadmap-wiki-update <ref> [--depends-on <ns#ref>,...]` | Append a `cholobots#N` node to the `cross-project-execution-order` wiki page in project 39 | Append-only, idempotent, optimistic-concurrency with one retry; invoked by `/overseer` when it mints a new story/bug ref at Gate A |

`/cut-release` is not ported: Cholobots has no release pipeline (the bot deploys from `main` via the `Procfile` worker); release automation belongs under epic #46.

Procedures meant to run unattended live in `docs/` and are written agent-agnostically: Taiga REST calls and decision tables that a non-Claude agent framework can execute. The Claude Code skill is a thin wrapper that cites the doc, as `/triage-bugs` does with `docs/bug-triage.md`.

## Prerequisites

- **Taiga setup.** The pipeline needs statuses (`Ready for Analysis`, `Ready for review`, `Blocked`), the `Urgent` priority, the pipeline tags and the roadmap wiki page that project 39 did not start with. Jon adds them once with `(eval "$(dv export project/taiga-growthgarage-jonroberts)" && python3 scripts/taiga_project_setup.py --apply)`. Orchestrating skills (overseer, fully-*, implement-epic, merge-train, triage-bugs, fix-bugs, parallel-fix-bugs) run `python3 scripts/taiga_project_setup.py --check` at startup and stop with that command for Jon on exit 2.
- **GitHub.** Branches are pushed to `upstream` (`MrsBohemian/Cholobots`) and PRs open inside it; `origin` is only a mirror. The `upstream` remote must use SSH (`git remote set-url upstream git@github.com:MrsBohemian/Cholobots.git` once per clone), because git over HTTPS picks up the shell's `GH_TOKEN`, which cannot push there. Every `gh` call is `env -u GH_TOKEN gh ... --repo MrsBohemian/Cholobots`.
- **Checks.** `python -m py_compile cholobots.py config.py db/database.py bots/*.py` is the only mandatory check today; pytest (story #34), ruff (#33) and CI (#35) are conditional until they land. "No checks configured" on a PR counts as a pass with a note. Details: `docs/testing-workflow.md`.

## Taiga status mapping

Skills use the full status set (`.claude/skills/shared/taiga-project.md` is the single reference for names, ids, tags, assignees and both access routes). Stories run `New`/`Ready for Analysis` → `Ready` + `llm-analyzed` → `In progress` → `Ready for review` → `Ready for test` → `Done`; bugs run `New` → `triaged` → `llm-analyzed` → `In progress` → `Ready for review` → `Closed`. Escalation is `Blocked` + `escalation-needed` + assigned `"Jon Roberts"` for stories and bugs alike; assignees resolve by full name, never by Taiga username.

## Agents

| Agent | Model | Preloaded Skills | Description |
|---|---|---|---|
| `senior-software-developer` | Opus | implement-task, fix-single-bug, fix-story-bug, fix-build-failure, finalize-story-pr, rebase-branch, analyze-story | Implements tasks and fixes bugs via TDD on pure helpers, records manual verification steps for handler wiring; analyzes stories |
| `remediation-developer` | Sonnet | pr-comments-resolve, fix-build-failure, rebase-branch, remediate-pr | Resolves PR comments and fixes failing checks |
| `senior-manual-testing-specialist` | Opus | analyze-bug | Tests Discord commands in the test server (story #31) and the dashboards via Chrome DevTools MCP |
| `test-plan-reviewer` | Sonnet | validate-test-plan | Reviews test plans in story analyses against the testing strategy |
| `test-quality-reviewer` | Opus | review-tests | Reviews test code changes for placement, redundancy, and quality; fixes issues and documents WHY |
| `overseer-conductor` | model pinned in its frontmatter | overseer | Non-interactive/delegated variant of `/overseer`: never edits files or runs checks; routes all state through Taiga, delegates each unit of work to the right worker agent, and runs the escalation ladder. Human gates degrade to stop-and-report since a subagent cannot use `AskUserQuestion` |
| `escalation-orchestrator` | Opus | (none: inline playbook) | Deep-reasoning escalation tier for the overseer, handed a single `BLOCKED` or failure-threshold item; diagnoses, re-plans, re-delegates to leaf workers, or escalates to Jon. Never touches the repo directly |

Worker agents (`senior-software-developer`, `remediation-developer`, `test-quality-reviewer`, `senior-manual-testing-specialist`) are synchronous: they follow `.claude/skills/shared/synchronous-worker-contract.md` (foreground-only Bash and Agent calls, return only after verified completion), and orchestrators treat their return as completion. `fully-implement-story` and `implement-story` additionally verify that the branch advanced past the pre-spawn tip (`git rev-list <preSpawnTip>..<branch>`) before counting a task as done, rather than trusting the SHA the agent reports; both follow `.claude/skills/shared/story-task-delegation.md`.

`fully-implement-story` and `implement-story` override the implement-task spawn of `senior-software-developer` to Sonnet via the Agent tool's `model` parameter (see `.claude/skills/shared/story-task-delegation.md`); all other `senior-software-developer` spawns use the agent's Opus pin.

## Bug Triage

Bugs flow New → `triaged` → `llm-analyzed` → fixed. `docs/bug-triage.md` defines severity (impact), priority (derived from severity × surface, overridable with a comment), the required surface tag (`product`, `devx`, `pipeline`, `ci`, with a per-bot tag alongside `product`) and source tag (`src:user`, `src:monitor`, `src:manual-test`, `src:pr-review`, `src:agent`, `src:jon`), response targets, and the sweep procedure. Every filing path applies the rubric at creation via `.claude/skills/shared/bug-filing.md`; only `/triage-bugs` sets the `triaged` tag. Analysis and fix orchestrators consider only `triaged` bugs and order them by priority, then severity, then ref. Fix batches exclude Urgent bugs and report them as `Run /fix-single-bug <ref> now`, and the batch continues. Bug selection and its filter flags come from `scripts/taiga_select_issues.py` (purposes `triage`, `analyze`, `fix`, `sprint`) via the shared include `.claude/skills/shared/bug-sweep-options.md`. The rubric is written for any agent with Taiga REST access, so an external triage agent can run the same sweep.

## Post-Merge Taiga Sync

The `taiga-sync.yml` GitHub Actions workflow runs on every merged PR. It acts only on the Taiga items the PR *claims* (a `#<ref>:` title prefix, a `fix(#<ref>):` scope, a trailing `(bug #<ref>)`, a standalone `Fixes`/`Closes`/`Resolves #<ref>` body line, or a `feature/`/`fix/` branch suffix), never a bare `#<ref>` mention in prose. Every claimed ref is processed, not just the first. For each claimed ref the two side effects are opt-in, each enabled by a PR label or a same-named Taiga tag on the item (read live at merge): `mark-done-on-merge` sets a story to Done (status 80) or a bug/issue to Closed (status 74), and `close-tasks-on-merge` closes the story's open tasks (status 62). With neither, the item only gets a "Merged in PR" comment. [`skills/shared/merge-side-effects.md`](skills/shared/merge-side-effects.md) defines the rules and which skill sets which signal. Dependents in `/implement-epic` unblock when a story is Done, so a story merged without `mark-done-on-merge` unblocks them when Jon marks it Done. The workflow delegates to `scripts/taiga-update-on-merge.sh`, which is also usable standalone (`--dry-run` supported).

**Not live yet:** the workflow needs repository secrets `TAIGA_API_URL`, `TAIGA_USERNAME` and `TAIGA_PASSWORD` on `MrsBohemian/Cholobots`, and the two labels must be created on the repo (the `gh label create` step in `merge-side-effects.md`). Until the secrets exist, whoever merges runs `scripts/taiga-update-on-merge.sh` by hand.

## Worktrees

Cholobots has no container fleet. Every story or bug runs in a git worktree of the shared clone:

```bash
git fetch upstream
git branch <branch> upstream/main            # or the story's base branch; skip when resuming
git worktree add .claude/worktrees/<story|bug>-<ref> <branch>
```

`.claude/worktrees/` is git-ignored. In the main session use `EnterWorktree` with `path:` (`name:` creates a fresh branch from the default remote, which is wrong when resuming an existing branch); for subagents pass the path in the prompt, or spawn with `isolation: "worktree"` when the skill does not need a specific branch. Other sessions drive the shared clone and have destroyed in-flight work there with `git reset --hard` and checkouts, so every worker prompt forbids `cd` to the main checkout, checking out other branches, `git reset --hard` and `git stash` (the stash is shared across worktrees). Teardown is `git worktree remove <path>`, plus `git branch -D <branch>` only when the skill says to discard. There is no app or service to stop: automation never starts the bot.

## Pipeline State

The `/fully-implement-story` orchestrator tracks state in `/tmp/fully-implement-<ref>.json`. Phases: implementing → monitoring → testing → fixing → post-fix-monitoring → retesting → complete/escalated. The state file also records `baseBranch` and `worktreePath`, so a resumed run keeps targeting the same base in the same worktree.

Escalation triggers:
- 5+ check failures on any PR
- Second-pass testing finds unresolved issues

Escalation actions: story moved to `Blocked`, tagged `escalation-needed`, assigned to `"Jon Roberts"`, all work stops.

The `/fully-fix-bugs` orchestrator tracks state in `/tmp/fully-fix-bugs.json`. Each bug fix runs in its own worktree (`.claude/worktrees/bug-<ref>`), created before the fix and kept through remediation so the fixing and remediating agents share one checkout. Remediation delegates to `scripts/remediate_pr.py`, which drives `monitor-pr-stability.sh` and emits action-request exit codes; the orchestrator then spawns `remediation-developer` for `fix-build-failure` or `pr-comments-resolve` with the worktree path. The state file tracks per-bug `worktreePath` alongside the usual status and PR data. Bugs with 5+ check failures are moved to `Blocked`, tagged `escalation-needed`, assigned to Jon, and skipped (the run continues with the next bug). The worktree is removed when the bug completes or escalates (the branch and PR stay on `upstream`); only a worktree with uncommitted work is kept and listed in the report.

The `/parallel-fix-bugs` skill runs the slot loop itself (in the skill prose; there is no Python orchestrator). Each bug gets a worktree plus an `Agent` spawn running the `fix-and-remediate-bug` composite skill, which wraps fix-single-bug + PR remediation into a single agent session. State is tracked in `/tmp/parallel-fix-bugs.json`. Bugs progress through: queued → launching → active → complete/escalated/error. Up to N bugs run concurrently (default 3). When a bug completes or escalates, its worktree is removed (kept only when it holds uncommitted work) and its slot is filled with the next queued bug. Re-invoking with the same args resumes from the existing state file.

The `/fully-analyze-stories` orchestrator tracks state in `/tmp/fully-analyze-stories-<epic-ref>.json`. Fetches all stories in the specified epic, spawns parallel `senior-software-developer` agents (up to N, default 3) running `/analyze-story` in unattended mode. Stories progress through: pending → analyzing → complete/rejected/needs-clarification/failed. Clarification questions are collected and presented to the operator in batch. Re-invoking with the same args resumes from the existing state file.

The `/implement-epic` orchestrator tracks state in `/tmp/implement-epic-<ref>.json`. The skill fetches eligible stories with `get_epic_overview(project=39, ref=<ref>)`, writes them to `/tmp/epic-<ref>/stories-input.json`, and runs the slot loop in the skill prose: each slot is a worktree (`.claude/worktrees/story-<ref>`) plus an `Agent` spawn running `/fully-implement-story <ref> <base-branch>` in that worktree. Stories progress through: queued → launching → active → complete/escalated/error/skipped. Up to N stories run concurrently. When a story escalates, its terminal status remains `escalated`, its worktree is kept, and its slot is immediately filled with the next queued story. There is no automatic requeue on a provider rate limit: an agent killed by one returns without completing, so the story ends `error` with its worktree kept, and the next run of the skill re-queues it and `/fully-implement-story` resumes from `/tmp/fully-implement-<ref>.json` in the preserved worktree. When dependencies are defined, stories with failed prerequisites are transitively skipped. Re-invoking with the same args resumes from the existing state file.

The `/merge-train` skill delegates to `scripts/merge_train.py`, which handles all deterministic operations (argument parsing, git rebase, CI monitoring via `scripts/monitor-pr-stability.sh`, Taiga checks via `scripts/taiga-story.sh`, state management, squash-merge with `env -u GH_TOKEN gh pr merge --repo MrsBohemian/Cholobots`). The SKILL.md wrapper invokes Claude agents for conflict resolution (via `/rebase-branch`), check remediation (via `/fix-build-failure`), and comment resolution (via `/pr-comments-resolve`), with per-PR failure counting and escalation at 5 failures. State is tracked in `/tmp/merge-train-<timestamp>-<random>.json` (created with `mkstemp`, so concurrent trains never share a file). PRs progress through: queued → in_progress → merged/skipped/failed. The script communicates with the wrapper via structured exit codes: 0 (complete), 10 (conflicts need resolution; action-request JSON written next to the state file as `/tmp/merge-train-<ts>-<random>-action.json`), 11 (remediation needed; action-request JSON written, `action` field is `remediate_build` or `resolve_comments`). After agent intervention, the wrapper re-invokes with `--resume <state-file-path>`. A pre-rebase stability check verifies checks pass (or none are configured) and no review comments are unresolved before attempting rebase. The train orders an integration PR after its parents and before the child, and closes an integration PR whose tree already matches the target once its parents land rather than reporting it as failed; the child story's PR is a *stacked* PR whose base is the integration branch, so the train retargets it and merges it with `--merge`.

The `/overseer` skill tracks state at `~/.local/state/overseer/cholobots-<kind>-<ref>.json` (`kind` = `story|bug|epic|sprint`; a sprint run uses the milestone id in the ref slot) and, for an un-reffed `new`/free-form request, at `~/.local/state/overseer/cholobots-pending.json` until the planner mints a ref. State lives outside `/tmp` so a host restart does not wipe a run; for an epic run the epic description's `## Overseer hand-off` section (written with `scripts/taiga-epic.sh`) is the durable hand-off. Every state file carries `"project": 39`, because `~/.local/state/overseer/` is shared with other repos' conductors and refs collide freely across Taiga projects: a file missing that field, or carrying a different project id, is never resumed or overwritten. Story/bug phases run `routing → analyzing → (gate A) → implementing|fixing → remediating → (gate B) → [qa] → complete | escalated`; epic and sprint phases are `running → paused | complete | escalated`. A batch run exits idle at `paused`, writing its blocking list to the epic description's `## Overseer hand-off` section, when no pending item is agent-actionable; items waiting on Jon, Heaven or real use count as blocked. Bugs and stories filed mid-run stay in the backlog (`filedItems`) unless Jon adopts them. Each item gets at most 2 verify → bug → fix cycles before escalating to Jon, and a run checks in with Jon every 40 worker spawns or 5 filed items. Gate approvals count only when Jon types them in the overseer's own session; relayed ones are information. The overseer marks a story Done or an issue Closed once the closing evidence is posted on the item; JON-ONLY and spend items still go to Jon. Rules: `.claude/skills/overseer/run-discipline.md`. Step 4 drives `scripts/remediate_pr.py` by exit code without `--taiga-id`, so the escalation write (Blocked status, tag and assignment) stays with the conductor rather than the script. Coexistence is one-directional: before routing a ref, the overseer checks `/tmp/implement-epic-*.json`, `/tmp/parallel-fix-bugs.json`, `/tmp/fully-fix-bugs.json`, and `/tmp/fully-implement-<ref>.json` for a non-terminal, in-progress claim on that ref and refuses to drive it manually if one is found; those orchestrators do not check for `~/.local/state/overseer/cholobots-*.json` in return.

The `/remediate-pr` skill delegates to `scripts/remediate_pr.py`, which handles all deterministic operations (stability monitoring via `scripts/monitor-pr-stability.sh`, failure counting, escalation checks, state management). The SKILL.md wrapper invokes Claude agents for check fixes (via `/fix-build-failure`) and comment resolution (via `/pr-comments-resolve`), following the same thin-wrapper pattern as `/merge-train`. CLI: `python3 scripts/remediate_pr.py <pr-number> [--max-failures N] [--taiga-id ID] [--taiga-type story|issue] [--resume <state-file>]`. State is tracked in `/tmp/remediate-pr-<pr>.json`. Exit codes: 0 (STABLE: checks passing or none configured, no unresolved comments), 10 (build fix needed; action-request JSON written to `/tmp/remediate-pr-<pr>-action.json`), 11 (comment resolution needed; action-request JSON written), 12 (ESCALATED: failure count reached threshold), 13 (NEEDS_REBASE: the wrapper drives `/rebase-branch` via a `remediation-developer` agent and resumes, returning NEEDS_REBASE only when the rebase escalates or the need recurs), 14 (PENDING: no verdict within the bounded wait window; re-invoke with `--resume`), 1 (unexpected error). After agent intervention, the wrapper re-invokes with `--resume <state-file-path>`. Default escalation threshold is 5 failures. Until CI exists (story #35) a PR with no checks reaches STABLE as soon as its review threads are resolved.

### Codex Review Pipeline

Reviews are performed by Codex CLI via `scripts/codex-review-pr.sh <pr>`, initiated by our skills; GitHub runs no review or CI on push. `CODEX_REVIEW_REASONING_EFFORT` defaults to `xhigh`.

- **Pass 1** runs right after `gh pr create` in every PR-creating skill, per `.claude/skills/shared/codex-review.md`. **Passes 2 and 3** are requested by `scripts/remediate_pr.py` when it resumes from `awaiting_fix` or `awaiting_comment_resolution`.
- **Cap: 3 passes per PR**, and one review per head SHA, both enforced inside the script, not by callers. A resolve-without-push yields `REVIEW_SKIPPED:same-head` and burns no pass.
- **Stdout contract** (last line): `REVIEW_POSTED:<pass>/<max>:<inline>:<body_only>`, `REVIEW_SKIPPED:cap`, `REVIEW_SKIPPED:same-head`, `REVIEW_PENDING:<pass>/<max>`. Exit 1 means nothing was posted.
- **Detached worker**: a pass on a large PR can outlast the 10-minute Bash tool cap. The script launches Codex and the posting step in a `setsid nohup` worker, tracked in `${CODEX_REVIEW_STATE_DIR:-${TMPDIR:-/tmp}/codex-review-state}/pr-<pr>-<head>-<dry-run>/`, and returns `REVIEW_PENDING:<pass>/<max>` once its bounded wait (`CODEX_REVIEW_WAIT_SECONDS`, default 540s) expires. Re-invoking the same command joins the running worker. The worker posts on its own regardless of whether anything re-invokes.
- **Dead-worker reclaim**: if the worker dies without its own cleanup (SIGKILL, OOM), the next invocation sweeps the worker's session with `pgrep`/`pkill`, removes the worktree it was handed and the job dir, then reports the failure.
- `scripts/remediate_pr.py` maps a `REVIEW_PENDING` last line to exit 14 (PENDING) without changing phase, so `--resume` re-runs the review hook and joins the worker.
- Findings become real GitHub review threads authored by the token owner, so `monitor-pr-stability.sh` treats them as blocking and remediation dispatches `/pr-comments-resolve` until they are resolved.
- Review failures are logged and non-fatal everywhere; they never block PR creation or remediation. Every `gh` call the script makes uses `env -u GH_TOKEN` against `MrsBohemian/Cholobots`.

## Epic Dependency Ordering

Epic descriptions can include a `## Dependency Diagram` or `## Dependencies` section that defines inter-story dependencies (written with `scripts/taiga-epic.sh patch`, since the workflow MCP server cannot edit epic descriptions). The `/implement-epic` skill and `scripts/merge_train.py` both parse this section via `parse_dependency_diagram()` from `scripts/lib/dependency_parser.py`. The `/implement-epic` skill passes dependency info to its slot loop via `stories-input.json`.

**Dependency diagram format:**
```
#101 (Foundation story)
  └─► #102 (Depends on 101)
        └─► #104, #105 (Both depend on 102)
  └─► #103 (Also depends on 101)

#106 (Independent, can run in parallel)
```

Parsing rules:
- `#<number>` identifies story refs
- Indented `└─►` arrows indicate the parent (less-indented) story is a prerequisite
- Comma-separated refs on the same arrow line share the same dependency
- Stories not mentioned in the diagram have no dependencies
- The diagram must be inside a fenced code block within the section
- Node refs must lead the line (right after the arrow for child lines, or at the start of the line for roots); trailing annotation text is ignored and never creates edges
- A wrapped continuation line that does not start with `#NNN` is ignored entirely and does not reset the ancestry tree
- A story whose diagram lists 2+ parents is based on an integration branch `epic/<epic-ref>-integration-<story-ref>`, named by `integration_branch_name()` in `scripts/lib/integration_branch.py` (shared with `merge_train.py`); the branch merges every parent with `--no-ff`, is pushed to `upstream`, and gets a PR of its own targeting `main`

**stories-input.json format** (with optional `dependencies` field):
```json
[
  {"ref": 101, "id": 1001, "subject": "Foundation", "dependencies": []},
  {"ref": 102, "id": 1002, "subject": "Depends on 101", "dependencies": [101]},
  {"ref": 103, "id": 1003, "subject": "Also depends on 101", "dependencies": [101]}
]
```

**Slot loop behavior**:
- Stories with unmet dependencies stay queued until all prerequisites reach `complete` status
- If a prerequisite fails (error/escalated), all transitive dependents are cascade-skipped with `dependency_failed` outcome
- An unresolvable integration merge blocks that story only, with outcome `integration_conflict`, leaving the rest of the epic running. An integration branch that fails for an operational reason (unresolvable parent branch, fetch/push failure, PR-create failure) blocks the same way with outcome `integration_error`
- If no dependency section exists or parsing fails, the loop falls back to ref-number ordering
- Circular dependencies are detected; if found, the skill stops with the cycle named before creating any worktree, rather than silently reordering the graph

`/overseer` epic and sprint mode parse the same `## Dependency Diagram` section through `scripts/lib/parse_roadmap.py`: epic mode pipes the epic description through `python3 scripts/lib/parse_roadmap.py -` in **legacy bare-int mode** (no `--default-namespace`), while the overseer's roadmap advisory reads the `cross-project-execution-order` wiki page in project 39 in **namespaced mode** (`--default-namespace cholobots`). The two modes use incompatible grammars and are never mixed. Refs in other namespaces are recorded as unknown and never looked up.
