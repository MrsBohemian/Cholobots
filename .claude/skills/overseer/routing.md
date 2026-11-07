# Overseer Routing

`SKILL.md` **Step 1** — argument parsing, kind detection, the design gate, resume rules, and the
advisory roadmap lookup (Step 1a). Extracted so `SKILL.md` stays inside the post-compaction
re-attachment window (see its **Retention budget** note). `Read` this before routing any
`$ARGUMENTS`.

## Step 1: Route

Parse `$ARGUMENTS`:

- **`epic <n>`** → **Epic mode** (`SKILL.md` § Epic mode). Skips the rest of this Step.
- **`sprint <arg>`** or **`milestone <arg>`** → **Sprint mode** (`SKILL.md` § Sprint mode). Skips the rest of this Step. Trailing flags after the milestone are issue filters, handled by sprint mode.
- **A bare number** (e.g. `21`) → resume an existing item. Detect story vs bug vs epic by falling through `get_story` → `get_issue` → `get_epic_overview` (`project=39, ref=<n>`). Each miss **raises** a not-found error — that is miss-try-next, not a failure. On an epic hit, go to **Epic mode** instead of the rest of this Step — its own resume rule offers a `paused` epic file for resume, unlike other terminal files.

  **Inverse ownership guard**: a bare story/bug ref held in a `pending`/`running` row by a `running` or `paused` `~/.local/state/overseer/cholobots-epic-*.json` or `~/.local/state/overseer/cholobots-sprint-*.json` carrying `"project": 39` warns and stops (`batch-loop.md`'s resume-and-guard section) — do not drive a ref that a live (or pause-parked) epic/sprint run already owns.

  **Design gate** (before anything else, for a story hit only): if the story carries tag `design` or `ui` without `design-reviewed`, **or** its description links `docs/designs/story-<ref>.md` that does not exist in the repo, stop and report: "Story #<ref> needs design first. Run `/design-story <ref>` in this main thread (it is interactive and writes `docs/designs/story-<ref>.md`, which the conductor never does), then re-invoke `/overseer <ref>`." Write no state file. Design here means Discord command UX (prompts, embeds, reply flows, Spanglish persona voice) or dashboard layout. Under `overseer-conductor` this is an ordinary stop-and-report. Inside a batch run (epic/sprint), item-row `status` has no `paused` value (`pending | running | complete | escalated | skipped | external-complete` — `batch-loop.md`'s ordering invariant), so the item row records `escalated` with reason `design needed`, per that invariant's "any other outcome is recorded escalated" rule; the batch loop continues past it via the cascade.

  For a story/bug hit that clears the design gate: read its status and tags. If a `~/.local/state/overseer/cholobots-<kind>-<ref>.json` already exists, carries `"project": 39`, and its phase is anything other than `complete` or `escalated`, offer to resume from its phase — this deliberately includes the terminal-but-resumable `awaiting-gate-a`, `awaiting-gate-b`, and `awaiting-qa-merge` parks (`state.md`'s **Terminal phases**) (a state file at `qa` resumes at Step 5, passing any recorded `qaBugRefs` as prior-pass refs; a state file at `awaiting-qa-merge` resumes at Step 5's merge re-check, live-verifying every `qaFixPRs` entry before proceeding — never re-driving Step 4 or re-offering QA on the strength of the phase alone; a state file at `awaiting-gate-a` resumes by re-presenting Gate A with the plan and `roadmapDeps` advisory already captured in the file — never re-spawning Step 2's planner; a state file at `awaiting-gate-b` resumes by re-presenting Gate B with the PR outcome already captured in the file — never re-driving Step 4); otherwise resume at the phase implied by status+tags.

  Find PRs with `env -u GH_TOKEN gh pr list --repo MrsBohemian/Cholobots --state <open|merged> --search "<ref> in:title" --json number,title,headRefName`, keeping only a title that claims the ref (`#<ref>:` prefix for a story; a trailing `(bug #<ref>)` for a bug, `fix-single-bug`'s PR title format).

  - **Story:**
    - no `llm-analyzed` tag → `analyzing`
    - `Ready` + `llm-analyzed`, not yet In progress → **Gate A** (the plan was never approved, so re-present it)
    - `In progress` → `implementing`
    - `Ready for review` or `Ready for test` with an open PR → `remediating` (record its number as `prNumber`)
    - `Ready for review` or `Ready for test` with a merged PR → **close on evidence** ([`run-discipline.md`](run-discipline.md)) once the plan's post-merge proof exists; otherwise tell the human what is missing and stop
  - **Bug:** nothing on this path moves an issue to `Ready` (not an issue status) or `Ready for test` — `analyze-bug` only adds the tag, Gate A's approval moves it to `In progress`, and `fix-single-bug` sets `Ready for review` once its PR is open. A bug is therefore `In progress` while `fixing` and `Ready for review` while `remediating`; check the PR too, because a worker that died after `gh pr create` leaves an `In progress` bug with an open PR.
    - no `llm-analyzed` tag → `analyzing`
    - `New` + `llm-analyzed` → **Gate A** (the plan was never approved, so re-present it)
    - `Ready for review` or `In progress` with an open PR → `remediating` (record its number as `prNumber`)
    - `In progress` with no open PR → `fixing` — never on status alone, which would re-spawn `fix-single-bug` and open a duplicate PR off `upstream/main`
    - `Ready for review` with a merged PR → **close on evidence** ([`run-discipline.md`](run-discipline.md)) once the plan's post-merge proof exists; otherwise tell the human what is missing and stop
    - any other status (`Ready for review` with neither an open nor a merged PR, `Blocked` after an escalation, `Ready for test`, `Needs Info`, `Postponed`) → tell the human the status and stop; re-entering Gate A or `fixing` would re-drive a handed-back or escalated bug

  A story at `Blocked` was escalated: tell the human and stop. QA is opt-in at Gate B, so status+tags never imply `qa`. If the item is already Done/Closed, tell the user and stop.
- **`new`** or **free-form prose** → this is a brand-new request. Ask the human whether it is a **feature** (user story) or a **bug** (`AskUserQuestion`). Then delegate the analyze phase with `"new"` and the request text so the planner creates the Taiga item and returns its ref.

Write the initial state file with `phase: "routing"` → advance to `analyzing`.

### Step 1a: Advisory roadmap lookup (silent-skip on any failure)

(Runs per story inside an epic or sprint run, unchanged.)

Once the ref is resolved (skip entirely for a `new`/free-form request — no ref exists yet; the roadmap read attaches to the ref that Step 2 mints, and this same block re-runs at that point before Gate A), fetch the cross-project execution-order wiki page and surface any unmet upstream dependencies for **this item** as advisory context at Gate A. **Advisory only** — never blocks, never auto-cancels, never changes Gate A's option set. Jon decides.

Persist the result in the state file as `roadmapDeps` (an array of `{ref, subject, statusName, isClosed}` objects — empty when nothing is unmet, `null` when the read was skipped) so Gate A and any resumed run can present the advisory without re-fetching.

The page may not yet contain any `cholobots` nodes; until `roadmap-wiki-update` adds one, every lookup lands on the silent `null` path below.

**Lookup procedure:**

1. `get_wiki(project=39, slug="cross-project-execution-order")` — the cross-project execution-order page, created in this project by `scripts/taiga_project_setup.py`. `mcp__taiga-py__get_wiki` is already inside this skill's `mcp__taiga-py__*` grant.

   If the MCP is unavailable or that call fails, try REST before giving up:

   ```bash
   scripts/taiga-wiki.sh get --slug cross-project-execution-order --project-id 39 --out /tmp/roadmap-wiki.md
   ```

   MCP-first is deliberate here and inverted in
   [`roadmap-wiki-update`](../roadmap-wiki-update/SKILL.md), which *writes*
   the page and so goes REST-first to keep the page off the model's output
   path. This lookup is a read-only advisory with no byte-exactness stake, so
   REST is just one more rung before the skip. Do not harmonise the two.
2. Parse the page content with the namespaced mode:

   When the page came from the script it is already on disk, so pass the path:

   ```bash
   python3 scripts/lib/parse_roadmap.py --default-namespace cholobots /tmp/roadmap-wiki.md
   ```

   The MCP path yields the content in-context, so it feeds the same wrapper
   over stdin:

   ```bash
   python3 scripts/lib/parse_roadmap.py --default-namespace cholobots - <<'ROADMAP_WIKI_EOF'
   <wiki page content>
   ROADMAP_WIKI_EOF
   ```

   The `Bash(python3 scripts/lib/parse_roadmap.py *)` row in this skill's
   `allowed-tools` covers both invocations (and `roadmap-wiki-update`'s
   identical idempotency parse). The grant is script-path-scoped rather than
   `python3 -c` because `-c` is arbitrary code execution — the wrapper is a
   read-only parser CLI (stdin/file in, JSON out), so the boundary stays in
   the matcher.

3. Look up the key `"cholobots#<ref>"` — Cholobots items are project 39, namespace `cholobots`. **If the ref is not a key, stop silently and set `roadmapDeps = null`.** Most Cholobots items are not on the master roadmap, and this repo's `.claude/` pipeline tooling is excluded by design, so "not present" is the common case and must produce **no output at all**.
4. For each dep of that node, resolve its live status. A `cholobots#N` dep resolves in project 39: refs are project-wide across item types, so try in order `get_story` → `get_issue` → `get_epic_overview` (`project=39, ref=N`). Treat a dep as **unmet** if the resolved item's status is not closed (`is_closed`), and as **unknown** if all three lookups miss. A dep in any other namespace is recorded as **unknown** and never looked up.
5. At Gate A (in `SKILL.md`), surface only unmet/unknown deps as advisory text, e.g.: `Roadmap (cross-project execution order): cholobots#21 has 1 unmet upstream — cholobots#47 "…" (In progress). This is advisory; the plan may still be sound to implement now.` If every dep is met, one line saying so, or nothing.

**Cost:** bounded at 1 wiki read + one Taiga read per `cholobots` dep (typically 0–5).

**Graceful skip — the important half.** Take the silent no-op path (write `roadmapDeps = null`, produce **no output**, **no error, no retry loop, no escalation**) when any of these hold: neither wiki path is available — the taiga-py MCP is unavailable **and** `scripts/taiga-wiki.sh get` also fails (including when the `TAIGA_API_URL` / `TAIGA_USERNAME` / `TAIGA_PASSWORD` env contract is unset); the page is missing; the page has no `## Dependency Diagram` section (`parse_dependency_diagram` returns `{}`); the parse yields an empty graph; the ref is not a node; or a dep-status lookup fails. Roadmap awareness is a convenience — it must never be able to stall a conductor run.

#### Gate A advisory rendering

`SKILL.md` **Gate A** renders the advisory this way. Read `roadmapDeps` from the state file (Step 1a wrote it). It is one of:

- `null` → **omit the advisory entirely** (the ref is not on the master roadmap, or the read was gracefully skipped). Most Cholobots items are not on it, so noise here would train the human to ignore it.
- `[]` (empty array) → optionally: `Roadmap (cross-project execution order): all upstream deps met.` One line, or nothing.
- Non-empty array → surface each entry as a one-liner, e.g. `Roadmap (cross-project execution order): cholobots#<ref> has N unmet upstream — <ns>#<n> "<subject>" (<statusName>). This is advisory; the plan may still be sound to implement now.`

The advisory is **advisory only** — it never blocks, never auto-cancels, and never changes Gate A's option set (approve / request-changes / cancel). Jon decides.
