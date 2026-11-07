---
name: fix-bugs
description: Cycle through all open bugs in Taiga project 39 (cholo-bots), fixing each on its own branch/PR
argument-hint: "[filter flags] [--yes]"
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
  - Bash(python3 scripts/taiga_select_issues.py *)
  - Bash(python3 scripts/taiga_project_setup.py --check)
  - AskUserQuestion
  - Read(//tmp/**)
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

# Fix Bugs

Repo and workflow facts (branches and PRs live on `upstream` = `MrsBohemian/Cholobots`, every `gh` call is `env -u GH_TOKEN gh ... --repo MrsBohemian/Cholobots`, status and tag names) are in the root `CLAUDE.md` and `.claude/skills/shared/taiga-project.md`.

## Step 1: Fetch all open bugs from Taiga

First run `python3 scripts/taiga_project_setup.py --check` once. Exit 0: proceed. Exit 2: stop and print `Run: python3 scripts/taiga_project_setup.py --env-file scripts/taiga-auth.env --apply` for Jon (the project is missing statuses, the `Urgent` priority or pipeline tags this skill relies on).

Parse arguments and select bugs per [`../shared/bug-sweep-options.md`](../shared/bug-sweep-options.md) by running `python3 scripts/taiga_select_issues.py --purpose fix <flags>` (it defaults to project 39). `/fix-bugs` keeps no state file, so the include's "Record" step means listing the effective options in the final report, and Resume does not apply. Report the `Urgent` block first; those bugs go to `/fix-single-bug`, not this sweep.

Reviewer assignment goes to `"Jon Roberts"` (the workflow server resolves assignees by full name, and status, severity and priority by name), so no id lookups are needed.

Add every bug in `selected` to TodoWrite by subject in that order. Pick the first one to work on.

## Step 2: Fix one bug

### 2a. Setup

1. Mark the bug as `in_progress` in TodoWrite
2. Make sure the working tree is clean, then branch from the fetched upstream: `git fetch upstream && git switch -c fix/<ref>-<short-slug> upstream/main` (slug from the issue subject, lowercase, hyphens, 3-4 words). Local `main` is not the base; `upstream` is Heaven's repo.
3. Fetch the full issue details: `get_issue(project=39, ref=<ref>)` for the description
4. Set the issue to `In progress`: `update_issue(project=39, ref=<ref>, status="In progress")`

### 2b. Investigate and fix

**If the fix changes Discord reply UX** (prompts, embeds, persona voice), keep the bot's Spanglish persona voice and follow the reply patterns already in that module. If a command is added or renamed, update that bot's `!<name>` help text. There is no `docs/design-system.md`.

**IMPORTANT: If the bug description contains a detailed implementation plan (root cause, specific files, specific changes), skip investigation entirely.** Read only the files listed in the plan, then go straight to implementation. Do NOT launch exploratory agents or re-investigate what the plan already explains.

If there is **no** implementation plan, follow the systematic debugging process:

1. **Root cause investigation**: read error messages, reproduce the issue, check recent changes
2. **Pattern analysis**: find working examples, compare against references, identify differences
3. **Hypothesis and testing**: form a single hypothesis, test minimally, verify before continuing

Keep the root `CLAUDE.md` gotchas in mind: several `on_message` listeners consume replies, in-memory sessions never expire, the Supabase client is synchronous, and the attachment gate whitelists commands.

**Implementation.** Follow [docs/testing-workflow.md](../../../docs/testing-workflow.md) and [docs/testing-conventions.md](../../../docs/testing-conventions.md):

- **Testable code** (pure helpers, parsers, money and time functions): TDD. Write a failing test that reproduces the bug, run it to confirm it fails, make the smallest fix, run it to confirm it passes, refactor while green. This applies once a `tests/` suite exists (story #34); until then record "no test suite yet (#34)" on the issue and fall through to the next bullet.
- **Command handlers that need a live Discord bot or Supabase**: no automated test. Write numbered manual test steps (command to run, expected reply) and post them on the Taiga issue with `add_comment(project=39, ref=<ref>, text=..., entity_type="issue")` for the test-server run (story #31). Say so in the PR body.

Never run `python cholobots.py` from this skill.

### 2c. Verify

Run, as foreground Bash calls (no `run_in_background`, this runs headless):

1. `python -m py_compile cholobots.py config.py db/database.py bots/*.py` (mandatory)
2. `python -m pytest -q` if `tests/` holds a suite, else record "no test suite yet (#34)"
3. `ruff check .` if ruff is configured (story #33), else skip

Fix every failure, then re-run.

### 2d. Commit, PR, and mark in review

1. Commit the changes (checks must pass first). Use the trailer from `.claude/skills/shared/commit-attribution.md`.
2. Push the branch to upstream: `git push -u upstream fix/<ref>-<short-slug>`
3. Create the PR: `env -u GH_TOKEN gh pr create --repo MrsBohemian/Cholobots --base main --head fix/<ref>-<short-slug> --title "fix(#<ref>): <subject>"`
4. Run the Codex review on the new PR following `.claude/skills/shared/codex-review.md`. A review failure is reported, not fatal; continue with the remaining steps.
5. Update Taiga: set the status to `Ready for review`: `update_issue(project=39, ref=<ref>, status="Ready for review")`.
6. Comment the PR link: `add_comment(project=39, ref=<ref>, text="PR: https://github.com/MrsBohemian/Cholobots/pull/<n>", entity_type="issue")`
7. Assign the issue to Jon for review: `assign_item(project=39, ref=<ref>, username="Jon Roberts", entity_type="issue")`
8. **Do NOT set the issue to "Closed"**: only Jon closes issues after merging the PR.
9. Mark the bug as `completed` in TodoWrite

## Step 3: Next bug

Report what was fixed and how many open bugs remain.

Remind the user to run `/clear` before invoking `/fix-bugs` again for the next bug. Each bug gets its own clean context to avoid confusion.
