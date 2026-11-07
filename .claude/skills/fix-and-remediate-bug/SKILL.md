---
name: fix-and-remediate-bug
description: Fix a single bug with TDD, create PR, then monitor and remediate until stable or escalated. Used by parallel-fix-bugs orchestrator.
argument-hint: "<bug-ref>"
allowed-tools:
  - mcp__taiga-py__*
  - Bash(python -m py_compile *)
  - Bash(python -m pytest *)
  - Bash(ruff *)
  - Bash(git *)
  - Bash(find *)
  - Bash(xargs grep *)
  - Bash(env -u GH_TOKEN gh *)
  - Bash(scripts/codex-review-pr.sh *)
  - Bash(python3 scripts/remediate_pr.py *)
  - Bash(scripts/monitor-pr-stability.sh *)
  - Read(//tmp/**)
  - Edit(//tmp/**)
  - Read
  - Edit
  - Write
  - Grep
  - Glob
  - WebFetch(https://github.com/*)
  - WebFetch(https://raw.githubusercontent.com/*)
  - WebFetch(https://discordpy.readthedocs.io/*)
  - WebFetch(https://supabase.com/docs/*)
  - WebFetch(https://pypi.org/*)
---

# Fix and Remediate Bug $ARGUMENTS

Composite skill that fixes a single bug, creates a PR, then monitors and remediates the PR until CI-stable or escalated. It runs in a git worktree (`.claude/worktrees/bug-<ref>`) inside an `Agent` spawn made by the `parallel-fix-bugs` orchestrator, which waits for it synchronously.

State is tracked in `/tmp/fix-and-remediate-<ref>.json` so the orchestrator can read the outcome directly after you return.

Repo facts: branches are pushed to `upstream` (`MrsBohemian/Cholobots`) and PRs open inside it; `origin` is only a mirror. Every `gh` call is `env -u GH_TOKEN gh ... --repo MrsBohemian/Cholobots`. Taiga names are in `.claude/skills/shared/taiga-project.md`. Never `cd` to the main checkout, check out other branches, `git reset --hard` or `git stash`.

## Running Unattended

This skill runs in a subagent spawned by `/parallel-fix-bugs`. **There is no human present.** You MUST NOT post questions to the user like "what should I do?" or "should I keep retrying?". The skill defines the decision tree; follow it.

Decision rules when things go wrong:
- CI flaky but under 5 failures: invoke `python3 scripts/remediate_pr.py` again. Let the script run its stability check.
- `python3 scripts/remediate_pr.py` returns ESCALATED (exit 12): write `phase = "escalated"` with `escalationReason` and STOP.
- Agent blocked by a permission denial or tool error: write `phase = "escalated"` with the specific reason and STOP.
- Uncertainty about proceeding: write `phase = "escalated"` with `escalationReason = "uncertain: <what>"` and STOP. Do not ask.

**Terminal phases**: the state file must end in one of these before you exit:
- `"complete"`: all steps succeeded, PR ready for review
- `"escalated"`: any reason, with populated `escalationReason`
- `"already-closed"`: bug was already closed when fetched

If you would write any other phase (e.g., `"remediating"`, `"fixing"`, `"pr-created"`) as your final action, you are doing it wrong: escalate instead.

## Step 1: Initialize state file

Extract the bug ref from `$ARGUMENTS` (e.g., `12`).

Write the initial state file at `/tmp/fix-and-remediate-<ref>.json`:
```json
{
  "phase": "fixing",
  "ref": <ref>,
  "prNumber": null,
  "branch": null,
  "buildFailureCount": 0,
  "escalationReason": null
}
```

Every later "update state to `{...}`" below sets only the named fields; keep the rest (`ref`, `prNumber`, `branch`), because `/parallel-fix-bugs` reads them from this file.

## Step 2: Fix the bug (fix-single-bug steps)

Follow the fix-single-bug workflow:

### 2a. Fetch bug details

Fetch the issue: `get_issue(project=39, ref=<ref>)`. Note the `subject`, tags and status.
If the issue is already `Closed` or `Rejected`, update state to `{"phase": "already-closed"}` and stop. The orchestrator treats this as terminal success with no PR, distinct from `phase: "complete"` which requires a PR to have been created.
If the bug lives only in `../Cholobots-Mobile` and this repo has no `mobile/` directory yet (story #64), write `phase = "escalated"` with `escalationReason = "out of scope: lives in ../Cholobots-Mobile"` and go to Step 4.

### 2b. Setup branch

1. `git fetch upstream`
2. If the current branch is already `fix/<ref>-*`, keep it (resume). Otherwise create the branch from the fetched tip: `git switch -c fix/<ref>-<short-slug> upstream/main` (slug from the issue subject, 3-4 words).
3. Set the issue to `In progress`: `update_issue(project=39, ref=<ref>, status="In progress")`

### 2c. Investigate and fix

**If the fix changes Discord reply UX**, keep the bot's Spanglish persona voice and follow the module's existing reply patterns (there is no `docs/design-system.md`); update that bot's `!<name>` help text if a command is added or renamed.

**If the bug description contains a detailed implementation plan**, skip investigation. Read only the files listed in the plan, then go straight to implementation.

Otherwise, follow the systematic debugging process: root cause investigation, pattern analysis, then a single tested hypothesis. Keep the root `CLAUDE.md` gotchas in mind (competing `on_message` listeners, never-expiring in-memory sessions, synchronous Supabase client, the attachment gate whitelist).

### 2d. Implement with tests

Every check run below follows the headless foreground rule in Step 2e. Follow [docs/testing-workflow.md](../../../docs/testing-workflow.md).

- **Testable code** (pure helpers, parsers, money and time functions) and a `tests/` suite exists (story #34): TDD. Write a failing test that reproduces the bug, confirm it fails, fix with the smallest change, confirm it passes, refactor while green. Never import `cholobots.py` in a test (it starts the bot); bot modules create Supabase clients at import, so set dummy env vars first.
- **Command handlers that need a live bot or Supabase, or no suite yet**: make the smallest fix and post numbered manual test steps on the Taiga issue (`add_comment(project=39, ref=<ref>, text=..., entity_type="issue")`) for the test-server run (story #31). Record "no test suite yet (#34)" when that is the reason.

Never run `python cholobots.py`.

### 2e. Verify

Run as foreground Bash calls (never `run_in_background`, `Monitor`, or end your turn waiting for a task notification: this runs headless, so there is no next turn and the run dies with you):

1. `python -m py_compile cholobots.py config.py db/database.py bots/*.py` (mandatory before every commit)
2. `python -m pytest -q` if `tests/` holds a suite, else record "no test suite yet (#34)"
3. `ruff check .` if ruff is configured (story #33), else skip

Fix every failure, then re-run. For a failure you believe is unrelated, follow `.claude/skills/shared/test-failure-triage.md`.

### 2f. Commit, push, create PR

1. Commit the changes:
   ```
   git commit -m "fix(#<ref>): <description>

   Co-Authored-By: <your session's model> <noreply@anthropic.com>"
   ```
   Use your own session's attribution trailer, see `.claude/skills/shared/commit-attribution.md`.
2. Push the branch to upstream: `git push -u upstream fix/<ref>-<slug>`
3. Create a PR:
   ```
   env -u GH_TOKEN gh pr create --repo MrsBohemian/Cholobots --base main --head <branch> --title "fix(#<ref>): <bug subject>" --body-file <body> [--label mark-done-on-merge]
   ```
   - Add `--label mark-done-on-merge` iff the bug's plan says `**Closes on merge:** eligible` (a missing line counts as not eligible). This skill runs only under the unattended `/parallel-fix-bugs`, so it needs no flag to opt in. See [`../shared/merge-side-effects.md`](../shared/merge-side-effects.md). If the repo rejects the label, create the PR without it.
   - Body: same format as fix-single-bug (Bug context, Fix summary, Regression tests; for manual-only fixes list the manual test steps posted on the issue). Issue links use `https://backlog.growthgarage.org/project/cholo-bots/issue/<ref>`.
4. Mark the issue ready for review: `update_issue(project=39, ref=<ref>, status="Ready for review")`.
5. Add the PR link to the Taiga bug as a comment:
   `add_comment(project=39, ref=<ref>, text="PR: https://github.com/MrsBohemian/Cholobots/pull/<pr_number>", entity_type="issue")`
6. Run the Codex review on the new PR following `.claude/skills/shared/codex-review.md`. A review failure is reported, not fatal; continue with the remaining steps.

### 2g. Update state

Update `/tmp/fix-and-remediate-<ref>.json`:
```json
{
  "phase": "pr-created",
  "ref": <ref>,
  "prNumber": <pr_number>,
  "branch": "<branch>",
  "buildFailureCount": 0,
  "escalationReason": null
}
```

If no PR was created (agent failed), update state to `{"phase": "escalated", "escalationReason": "failed to create PR"}` and go to Step 4.

## Step 3: Remediate the PR

### 3a. Run remediation script

Update state phase to `"remediating"`.

Run:
```bash
python3 scripts/remediate_pr.py <prNumber> --max-failures 5 --taiga-id <ref> --taiga-type issue
```

Run this as a normal foreground Bash call with `timeout: 600000`. **NEVER use `run_in_background`**: in headless mode, ending your turn kills background tasks, leaving the state file at a non-terminal phase that the orchestrator downgrades to escalated. The script bounds its own CI wait and returns exit 14 (PENDING) well within the Bash timeout; re-invoke on 14 instead of backgrounding.

There is no CI configured yet (story #35). The script treats "no checks configured" as a pass-with-note, so a PR with only review comments or a clean merge state still reaches exit 0 or 11. Do not treat missing checks as a failure and do not wait for them.

Capture the exit code.

### 3b. Handle exit codes

#### Exit 0: STABLE

The PR is CI-stable (or has no checks configured) and has no unresolved review comments.

1. Update state to `{"phase": "complete"}`
2. Go to Step 4 (finalize)

#### Exit 10: Build fix needed

1. Read `/tmp/remediate-pr-<prNumber>-action.json` for details
2. Fix the build inline (follow fix-build-failure approach):
   - Read the failing check details from the action-request
   - Investigate the failure, fix the code
   - Re-run `python -m py_compile cholobots.py config.py db/database.py bots/*.py`, commit and push
3. Resume remediation:
   ```bash
   python3 scripts/remediate_pr.py --resume /tmp/remediate-pr-<prNumber>.json
   ```
4. Go back to Step 3b to handle the new exit code

#### Exit 11: Comments found

1. Read `/tmp/remediate-pr-<prNumber>-action.json` for details
2. Resolve comments inline (follow pr-comments-resolve approach):
   - Read each unresolved comment thread
   - Evaluate, fix if needed, reply, resolve
3. Resume remediation:
   ```bash
   python3 scripts/remediate_pr.py --resume /tmp/remediate-pr-<prNumber>.json
   ```
4. Go back to Step 3b to handle the new exit code

#### Exit 12: ESCALATED

Build failure count reached threshold (5+ failures).

1. Update state to `{"phase": "escalated", "escalationReason": "5+ build failures"}`
2. Go to Step 4 (finalize)

#### Exit 13: NEEDS_REBASE

1. Rebase the branch onto upstream main:
   ```bash
   git fetch upstream main
   git rebase upstream/main
   git push --force-with-lease upstream HEAD
   ```
   If rebase conflicts are too complex to resolve, update state to `{"phase": "escalated", "escalationReason": "rebase conflict"}` and go to Step 4.
2. Resume remediation:
   ```bash
   python3 scripts/remediate_pr.py --resume /tmp/remediate-pr-<prNumber>.json
   ```
3. Go back to Step 3b

#### Exit 14: PENDING

CI has not reached a verdict yet, or the Codex review is still running; the script returned within its bounded wait window.

1. Immediately resume remediation:
   ```bash
   python3 scripts/remediate_pr.py --resume /tmp/remediate-pr-<prNumber>.json
   ```
2. Go back to Step 3b to handle the new exit code

#### Exit 1: Error

Unexpected error from the remediation script.

1. Update state to `{"phase": "escalated", "escalationReason": "remediation script error"}`
2. Go to Step 4

## Step 4: Finalize

### If phase is "complete"

1. Make sure the issue is in `Ready for review` (Step 2f.4 set it; set it again if missing).
2. Assign the issue to Jon: `assign_item(project=39, ref=<ref>, username="Jon Roberts", entity_type="issue")`
3. Do not close the issue: taiga-sync closes the bug on merge only when the PR carries `mark-done-on-merge`.

### If phase is "escalated"

1. Re-read the issue, then set the status and tag: `update_issue(project=39, ref=<ref>, status="Blocked", tags=[<existing...>, "escalation-needed"])` (`tags` replaces the whole list).
2. Assign the issue to Jon: `assign_item(project=39, ref=<ref>, username="Jon Roberts", entity_type="issue")`
3. If a PR exists, post a comment: `env -u GH_TOKEN gh pr comment <prNumber> --repo MrsBohemian/Cholobots --body "Escalating to @bigjonroberts: <escalationReason>. Manual intervention needed."` and mirror it with `add_comment(project=39, ref=<ref>, text=..., entity_type="issue")`.

## Step 5: Report

Return:
- Bug ref and subject
- Final phase (complete or escalated)
- PR number and URL (if created)
- Codex review result (the `REVIEW_POSTED`/`REVIEW_SKIPPED` line, if a PR was created)
- Root cause (one sentence, if fixed)
- Escalation reason (if escalated)
