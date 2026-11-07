---
name: fix-story-bug
description: Fix a single bug from manual testing, creating a sub-PR to a feature branch with regression tests (or recorded manual test steps)
argument-hint: "<bug-ref> <feature-branch>"
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

# Fix Story Bug $ARGUMENTS

Fix a single bug found during manual testing, creating a sub-PR that targets a feature branch (not `main`). Regression tests are automated where the code is testable; otherwise manual test steps go on the Taiga issue.

Repo facts: branches are pushed to `upstream` (`MrsBohemian/Cholobots`) and PRs open inside it (including this sub-PR, whose base is the feature branch); `origin` is only a mirror. Every `gh` call is `env -u GH_TOKEN gh ... --repo MrsBohemian/Cholobots`. Taiga names are in `.claude/skills/shared/taiga-project.md`.

## Step 1: Parse arguments

Extract the bug ref and feature branch from `$ARGUMENTS`:
- First word: bug ref number (e.g., `12`)
- Remaining words: feature branch name (e.g., `feature/34-test-harness`)

Example: `12 feature/34-test-harness`

## Step 2: Fetch bug details

1. `get_issue(project=39, ref=<bug-ref>)`. Note the `subject`, tags and status.
2. If the issue is already `Closed` or `Rejected`, inform the caller and stop.

## Step 3: Setup branch

Work in the current checkout (a git worktree when an orchestrator spawned you). Never `cd` to the main checkout, check out unrelated branches, `git reset --hard` or `git stash`.

1. Fetch and base the sub-branch on the feature branch tip: `git fetch upstream <feature-branch>`, then
   ```
   git switch -c fix/<bug-ref>-<short-slug> upstream/<feature-branch>
   ```
   Use a short slug derived from the issue subject (lowercase, hyphens, ~3-4 words max). If you are already on a `fix/<bug-ref>-*` branch, keep it.
2. Set the issue to `In progress`: `update_issue(project=39, ref=<bug-ref>, status="In progress")`

## Step 4: Investigate and fix

**If the fix changes Discord reply UX** (prompts, embeds, persona voice), keep the bot's Spanglish persona voice and follow the reply patterns already in that module. If a command is added or renamed, update that bot's `!<name>` help text. There is no `docs/design-system.md`.

**If the bug description contains a detailed implementation plan** (root cause, specific files, specific changes), skip investigation entirely. Read only the files listed in the plan, then go straight to implementation.

If there is **no** implementation plan, follow the systematic debugging process:

1. **Root cause investigation**: read error messages, reproduce the issue, check recent changes
2. **Pattern analysis**: find working examples, compare against references, identify differences
3. **Hypothesis and testing**: form a single hypothesis, test minimally, verify before continuing

Keep the root `CLAUDE.md` gotchas in mind: several `on_message` listeners consume replies, in-memory sessions never expire, the Supabase client is synchronous, and the attachment gate whitelists commands.

## Step 5: Regression tests

Follow [docs/testing-workflow.md](../../../docs/testing-workflow.md). Choose per fix:

- **Testable code** (pure helpers, parsers, money and time functions) and a `tests/` suite exists (story #34): TDD. The regression test must specifically cover the bug scenario.
  1. Write a failing test that reproduces the bug
  2. Run it to confirm it fails as expected
  3. Fix the bug with the smallest reasonable change
  4. Run it to confirm the fix works
  5. Refactor if needed while keeping tests green

  Never import `cholobots.py` in a test (it calls `bot.run()` at module level). Bot modules create Supabase clients at import, so tests set dummy env vars first.
- **Command handlers that need a live bot or Supabase, or no suite yet**: make the smallest fix, then post numbered manual test steps (command, expected reply, the bug scenario first) on the Taiga issue with `add_comment(project=39, ref=<bug-ref>, text=..., entity_type="issue")` for the test-server run (story #31). Record "no test suite yet (#34)" when that is the reason.

Never run `python cholobots.py` from this skill.

## Step 6: Verify

Run as foreground Bash calls (never `run_in_background`; this runs headless):

1. `python -m py_compile cholobots.py config.py db/database.py bots/*.py` (mandatory before every commit)
2. `python -m pytest -q` if `tests/` holds a suite, else record "no test suite yet (#34)"
3. `ruff check .` if ruff is configured (story #33), else skip

Fix every failure, then re-run.

**If you believe a failure is unrelated to your changes**, you MUST follow the Test Failure Triage Protocol in `.claude/skills/shared/test-failure-triage.md` before dismissing it.

## Step 7: Commit, push, create sub-PR

1. Commit the changes with a descriptive message:
   ```
   git commit -m "fix(#<ref>): <description of what was fixed>

   Co-Authored-By: <your session's model> <noreply@anthropic.com>"
   ```
   Use your own session's attribution trailer, see `.claude/skills/shared/commit-attribution.md`.
2. Push the sub-branch to upstream:
   ```
   git push -u upstream fix/<slug>-<ref>
   ```
3. Extract bug context from the issue details fetched in Step 2:
   a. **Bug description excerpt**: first paragraph of the issue description (or the first 2-3 sentences), under ~400 characters. If it has a `## Steps to Reproduce` or similar section, extract that separately.
   b. **Root cause**: from the implementation plan if present, otherwise one sentence from your investigation.
   c. **Reproduction steps**: brief steps from the bug description if present. Omit if not available.
4. Create a sub-PR targeting the **feature branch** (not `main`):
   ```
   env -u GH_TOKEN gh pr create --repo MrsBohemian/Cholobots --base <feature-branch> --head <branch> --title "fix(#<ref>): <bug subject>" --body-file <body>
   ```
   - No `--reviewer`. Add no merge-side-effect label. The bug is verified through `Ready for test` (Step 8), so merging this sub-PR must not close it. See [`../shared/merge-side-effects.md`](../shared/merge-side-effects.md).
   - Body format:
     ```
     Fixes bug [#<ref>](https://backlog.growthgarage.org/project/cholo-bots/issue/<ref>)

     ## Bug context

     **[#<ref>](https://backlog.growthgarage.org/project/cholo-bots/issue/<ref>): <subject>**

     <Bug description excerpt from Taiga: first paragraph or 2-3 sentences>

     **Root cause:** <one-sentence root cause from implementation plan or investigation>

     **Reproduction:** <brief reproduction steps from bug description. If not available, DELETE THIS ENTIRE LINE FROM THE PR BODY>

     ## Fix summary

     <what was changed and why>

     ## Regression tests

     <list of regression tests added, or "Manual test steps posted on the Taiga issue (no automated suite for this code yet)">

     🤖 Generated with [Claude Code](https://claude.com/claude-code)
     ```

   Never include empty subsections. If reproduction steps are not available, omit the **Reproduction:** line entirely.
5. Add the sub-PR link to the Taiga bug as a comment:
   `add_comment(project=39, ref=<ref>, text="Sub-PR: <full PR URL>", entity_type="issue")`
6. Run the Codex review on the new PR following `.claude/skills/shared/codex-review.md`. A review failure is reported, not fatal; continue with the remaining steps.

## Step 8: Update Taiga

Update the bug to `Ready for test`: `update_issue(project=39, ref=<ref>, status="Ready for test")`.

**Do NOT close the issue**: the orchestrator or Jon handles that after verification. A bug whose PR is open normally sits in `Ready for review`, but here the sub-PR lands on a feature branch and verification happens on that branch, so `Ready for test` is correct.

## Step 9: Report

Return:
- Which bug was fixed (subject and ref)
- Root cause (one sentence)
- What regression tests were added, or where the manual test steps were posted
- Sub-PR number and URL
- Codex review result (the `REVIEW_POSTED`/`REVIEW_SKIPPED` line)
- Check results (py_compile, and pytest/ruff pass/fail or "not configured")
