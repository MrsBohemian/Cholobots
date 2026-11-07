# Bug Triage

How bugs in Taiga project 39 (`cholo-bots`) get a severity, a priority and a place in the queue.
This is the single source of truth for Claude Code skills, other agents and humans, and it is
written so an agent with only Taiga REST access can execute it. Names, ids and tags are listed in
`.claude/skills/shared/taiga-project.md`; the pipeline statuses and the `Urgent` priority exist
once Jon has run `scripts/taiga_project_setup.py --apply`.

## Principles

- **Severity is impact. Priority is scheduling.** Severity says what the bug does to the people
  using the bots (or to the pipeline) and never changes because of deadlines. Priority says when we
  fix it.
- **Default, then override with a reason.** Priority comes from the matrix below. Any override,
  and any later change to severity or priority, gets a comment explaining why.
- **Triaged is a checkable state.** Fix orchestrators only pull bugs tagged `triaged`.

## Fields and tags

| Field | Values (low to high) |
|---|---|
| Type | `Bug` (this document applies), `Question`, `Enhancement` (not triaged as bugs) |
| Severity | `Wishlist`, `Minor`, `Normal`, `Important`, `Critical` |
| Priority | `Low`, `Normal`, `High`, `Urgent` |
| Status | `New`, `In progress`, `Ready for review`, `Ready for test`, `Closed`, `Blocked`, `Needs Info`, `Rejected`, `Postponed` |

**Surface** (exactly one): `product` (a bot command, listener, scheduler or dashboard as users
experience it; add every per-bot tag that applies, one or more of `metichebot`, `chismebot`,
`guardabot`, `crudobot`, `obijuan`, `vueltabot`, `mobile`; when no bot can be determined, add none
and say so in the triage comment), `devx` (local setup, venv, env vars, Supabase local stack, seeding,
docs for those), `pipeline` (skills, agents, scripts, Taiga and GitHub automation), `ci` (GitHub
Actions and failures that only show there; story #35).

**Source** (exactly one): `src:user` (Heaven, Daniel or the crew), `src:monitor` (an automated
production signal), `src:manual-test`, `src:pr-review`, `src:agent`, `src:jon`. An existing source
tag stands. **Default when none is set:** bugs filed from the 2026-11-06 assessment (bug refs 1 to
57) are `src:jon` (Jon directed the assessment); any other bug with no source tag is `src:user`,
because every agent filing path sets its source tag at creation (`bug-filing.md`), so an untagged
bug was filed by a person in the Taiga UI. Triage never guesses beyond that.

**State:** `triaged`, `llm-analyzed`, `pre-existing-failure` (only alongside `ci` or `devx`),
`escalation-needed`. Keep existing topic tags (`listeners`, `data-loss`, `timezone`, ...).

## Severity

Judge impact only: who is affected, is a record lost or wrong, is there a workaround?

| Level | `product` | `devx`, `pipeline`, `ci` |
|---|---|---|
| **Critical** | Records silently lost or overwritten for real users, or one person or business sees or edits another's data. The bot down or a core flow (time capture, leads, closeout, receipts, pings) broken for everyone. Any secret or key exposure. Example: #4, unknown Discord users read and overwrite Heaven's daily tasks. | Every automated run fails or corrupts shared state (git, Taiga) with no workaround. `main` does not compile. |
| **Important** | A core flow loses or corrupts data in a common path, or is broken for a subset of users with no good workaround; money recorded wrong but recoverable. Examples: #5 time lost at `!mstopday`, #6 closeout reset to `ready`, #7 receipt saved by an unrelated `yes`, #8 `$0` invoice on a typo, #10 lead notes never reach the card, #14 pings never start if Vueltabot fails to load. | An orchestrator path reliably fails or silently does the wrong thing (skips work, wrong status, loses a PR). A clean clone cannot be set up or checked. |
| **Normal** | A non-core function broken, or a real workaround exists; wrong-day records from timezone mix-ups; races that need two users at once. Examples: #9 Chismebot answers anyone's `cancel`, #12 `!claim` double claim, #16 mixed timestamps, #53 `!hotlist Name` raises temperature. | Degraded with a known workaround; flaky tests; docs wrong in a way that costs an agent time. |
| **Minor** | Help text or persona copy wrong, cosmetic output, log noise. Examples: #20 help text out of sync, #56 Guardabot audit rows without a user. | Stale hints, cosmetic output. |
| **Wishlist** | Behaviour that works as built but could be nicer; no defect. | Same. |

Overrides: any confirmed data exposure across people or businesses is Critical; "affected users"
means all, some, one and reproducible, or one and not reproducible, never percentages; the filer's
severity is provisional and the triager confirms it.

## Priority

| Severity | `product` | `devx`, `pipeline`, `ci` |
|---|---|---|
| Critical | **Urgent** | High |
| Important | High | Normal |
| Normal | Normal | Normal |
| Minor | Low | Low |
| Wishlist | Low | Low |

Modifiers: source `src:user` or `src:monitor` never below Normal; blocks an item in flight (the
issue says so, or the blocked item names it): one level up; only reproducible on a dev-only path
with a documented workaround: one level down.

**Urgent bugs are reported, not batch-fixed.** Every sweep and fix batch lists them first as
`Run /fix-single-bug <ref> now`, and the batch continues without them.

## Response targets

| Priority | Triaged within | Work starts | Target close |
|---|---|---|---|
| Urgent | same day | same day, `/fix-single-bug` off `upstream/main` | 48 hours |
| High | 1 business day | this week | current milestone |
| Normal | 1 week | next fix sweep | next milestone |
| Low | 1 week | when convenient | none; reviewed at 90 days |

Only "triaged within" (from `created_date`) is checked by the sweep; the rest is reviewed by Jon.
The MCP `get_issue` returns no dates or version; read `created_date`, `modified_date` and `version`
with `scripts/taiga-story.sh <ref> 39 --issue`.

## Routing rules

- **Needs Info**: the description fails the completeness test (Definition of triaged, item 2).
  Comment which part is missing. No reply within 14 days → `Rejected` with a comment; reopen when it
  arrives.
- **Rejected**: duplicate, not a bug (works as designed; consider filing an `Enhancement`), or stale
  Needs Info. Two bugs are duplicates when they share the root cause at the same code location; the
  same symptom from different causes is not a duplicate. The later ref is Rejected with the comment
  `Duplicate of #<earlier ref>`.
- **Closed**: a merged PR claims the bug. Claims come from GitHub only (Taiga comments are not
  scanned) and use the grammar in `scripts/taiga_closing_refs.py`: a `#N:` title prefix, a
  `fix(#N):` scope, a trailing `(bug #N)`, a standalone `Fixes`/`Closes`/`Resolves #N` body line,
  or, when none of those is present, the branch name. A bare `#N` mention is never a claim. Comment
  `Closed by PR #N`.
- **Postponed**: Jon's call only, usually for a Low bug at its 90-day review or one that waits on an
  epic (for example multi-business work under #3). The sweep lists candidates; it never postpones.

## Definition of triaged

1. Type is `Bug`.
2. The description is complete: it contains (a) a reproduction or a `file:line` citation and
   (b) an observed-versus-expected statement in any wording. The `What happens / Repro / Fix
   direction` form of the assessment issues counts, as does the filing template below. Only when
   (a) or (b) is missing: Needs Info.
3. Duplicates were searched and none found, or the issue was closed as a duplicate.
4. Severity is set per the table.
5. Exactly one surface tag and exactly one source tag are set (plus every per-bot tag that applies
   for `product`, or none with a comment saying no bot could be determined).
6. Priority is set per the matrix and modifiers, or overridden with a comment.
7. The `triaged` tag is applied.

## Filing

Every filing path applies this document at creation: provisional severity, derived priority,
surface and source tags, and the description template (`**Expected:**`, `**Actual:**`,
`**Reproduce / observed at:**`, `**Preliminary cause:**`, `**Evidence:**`). Claude Code skills
follow `.claude/skills/shared/bug-filing.md`. Only the sweep sets `triaged`.

## Triage sweep

Runs on demand or on a schedule (Claude Code `/triage-bugs`, or any agent following these steps),
with a dry-run mode that prints proposed changes without writing. Base URL
`https://backlog.growthgarage.org/api/v1`; authenticate with `POST /auth`
(`{"type":"normal","username":...,"password":...}`) and send `Authorization: Bearer <token>`.

1. Resolve ids: `GET /issue-types?project=39`, `/severities?project=39`, `/priorities?project=39`,
   `/issue-statuses?project=39`. Ids of the pre-existing values are in `taiga-project.md`;
   resolve `Urgent`, `Ready for review` and `Blocked` by name.
2. Candidates: `GET /issues?project=39&status__is_closed=false&type=50` with header
   `x-disable-pagination: True`, keep those without the `triaged` tag. (Same set as
   `python3 scripts/taiga_select_issues.py --purpose triage`.)
3. For each candidate, in ref order:
   1. Completeness test (item 2 above). Fails → status `Needs Info` + comment; next issue.
   2. Duplicates and prior fixes: `GET /search?project=39&text=<token>` with one or two distinctive
      tokens at a time; apply the duplicate rule under "Routing rules". Duplicate → `Rejected` +
      comment; next. PR claims: fetch the PR list once per sweep (`gh pr list --repo
      MrsBohemian/Cholobots --state all --limit 200 --json number,title,body,headRefName,state`)
      and match each bug with the claim grammar under "Routing rules"; never read Taiga comments for
      PR markers. A merged PR claims it → `Closed` + comment; next. An open PR claims it → keep the
      status, continue tagging.
   3. Surface tag (for `product`, every per-bot tag that applies), then source tag (the default
      under "Fields and tags").
   4. Confirm or correct severity; derive priority.
   5. One write: `PATCH /issues/<id>` with `version`, `severity`, `priority`, `tags` (existing tags
      plus surface, source, `triaged`; drop any second surface tag among `product`/`devx`/
      `pipeline`/`ci`, keeping topic tags such as `devex`) and a `comment` stating the
      severity and priority and any override reason. Through the MCP tools this is two calls,
      because `update_issue` has no comment parameter: `update_issue(...)` then
      `add_comment(project=39, ref=, text=, entity_type="issue")`. `get_issue` returns tags as
      `[name, color]` pairs; `update_issue(tags=[...])` takes plain names.
4. Expire Needs Info older than 14 days (`modified_date`) to `Rejected` with a comment. List,
   without moving, Low bugs untouched for 90 days (`modified_date`).
5. Report in this order: Urgent bugs by the priority this sweep proposes, not the current Taiga
   priority (each with `Run /fix-single-bug <ref> now`), High bugs,
   counts by priority and surface, bugs past their triage window (skip on the first sweep of an
   untriaged backlog), overrides, issues closed or rejected, issues sent to Needs Info, Low bugs
   due for review.

## Fix selection

All bug sweeps select with `scripts/taiga_select_issues.py` (`--purpose triage|analyze|fix|sprint`
plus filter flags; see `.claude/skills/shared/bug-sweep-options.md`). Order: priority descending,
then severity descending, then ref ascending. The `fix` purpose excludes `Ready for review` and
`Blocked` bugs and moves Urgent bugs out of the batch.

## Weekly human pass

Jon reads the sweep report, confirms or overrides anything Urgent or High, and decides Postponed
or Rejected for the 90-day Low list. Overrides go in a comment, same as for agents.
