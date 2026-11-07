---
name: fix-single-bug
description: Fix a single bug from Taiga on its own branch off upstream/main, creating a PR. Used by the fully-fix-bugs orchestrator.
argument-hint: "<bug-ref> [--mark-done-on-merge]"
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

# Fix Single Bug $ARGUMENTS

Fix a single bug on its own branch off `upstream/main`, with TDD where the code is testable, and open a PR inside Heaven's repo (`MrsBohemian/Cholobots`). This skill is used by the `fully-fix-bugs` orchestrator. It sets the Taiga issue to `In progress` while fixing and to `Ready for review` once the PR is up; the orchestrator does the rest (monitoring, remediation, assignment, escalation) after PR monitoring.

Repo facts: branches are pushed to `upstream` (`MrsBohemian/Cholobots`) and PRs open inside it; `origin` is only a mirror. The shell's `GH_TOKEN` cannot see that repo, so every `gh` call is `env -u GH_TOKEN gh ... --repo MrsBohemian/Cholobots`. Taiga names are in `.claude/skills/shared/taiga-project.md`.

## Step 1: Parse argument and fetch bug details

1. Extract the bug ref from `$ARGUMENTS` (e.g., `12`). If `$ARGUMENTS` also contains the token `--mark-done-on-merge`, note it: only then may Step 6 label the PR to close the bug on merge. `/fully-fix-bugs` passes it; overseer and manual runs do not.
2. Fetch the issue: `get_issue(project=39, ref=<bug-ref>)`. Note the `subject`, tags and status.
3. If the issue is already `Closed` or `Rejected`, inform the caller and stop.
4. If the bug lives only in the sibling dashboards repo (`../Cholobots-Mobile`) and this repo has no `mobile/` directory yet (story #64), stop and report "out of scope: lives in ../Cholobots-Mobile". Make no changes.

## Step 2: Setup branch

Work in the current checkout, which is a git worktree when the orchestrator spawned you (`.claude/worktrees/bug-<ref>`). When nothing created one (an overseer spawn or a manual run in the main checkout), create it first and work only there: `git fetch upstream && git worktree add -b fix/<bug-ref>-<short-slug> <main checkout>/.claude/worktrees/bug-<bug-ref> upstream/main` (skip the add when that worktree already exists). Never `cd` to the main checkout, check out other branches, `git reset --hard` or `git stash` (the stash is shared across worktrees).

1. `git fetch upstream`
2. If the current branch is already `fix/<bug-ref>-*`, keep it (resume). Otherwise the tree must be clean; create the branch from the fetched upstream tip: `git switch -c fix/<bug-ref>-<short-slug> upstream/main`. Slug from the issue subject (lowercase, hyphens, 3-4 words max).
3. Set the issue to `In progress`: `update_issue(project=39, ref=<bug-ref>, status="In progress")`

## Step 3: Investigate and fix

**If the fix changes Discord reply UX** (prompts, embeds, persona voice), keep the bot's Spanglish persona voice and follow the reply patterns already in that module. If a command is added or renamed, update that bot's `!<name>` help text. There is no `docs/design-system.md`.

**IMPORTANT: If the bug description contains a detailed implementation plan (root cause, specific files, specific changes), skip investigation entirely.** Read only the files listed in the plan, then go straight to implementation. Do NOT launch exploratory agents or re-investigate what the plan already explains.

If there is **no** implementation plan, follow the systematic debugging process:

1. **Root cause investigation**: read error messages, reproduce the issue, check recent changes
2. **Pattern analysis**: find working examples, compare against references, identify differences
3. **Hypothesis and testing**: form a single hypothesis, test minimally, verify before continuing

Keep the root `CLAUDE.md` gotchas in mind: several `on_message` listeners consume replies, in-memory sessions never expire, the Supabase client is synchronous, and the attachment gate whitelists commands.

## Step 4: Implement with tests

Follow [docs/testing-workflow.md](../../../docs/testing-workflow.md) and [docs/testing-conventions.md](../../../docs/testing-conventions.md). Cholobots has no test suite today (story #34), so choose per fix:

- **Testable code** (pure helpers, parsers, money and time functions) and a `tests/` suite exists: TDD. The regression test must specifically cover the bug scenario.
  1. Write a failing test that reproduces the bug
  2. Run it to confirm it fails as expected
  3. Fix the bug with the smallest reasonable change
  4. Run it to confirm the fix works
  5. Refactor if needed while keeping tests green

  Never import `cholobots.py` in a test (it calls `bot.run()` at module level). Bot modules create Supabase clients at import, so tests set dummy env vars first.
- **Command handlers that need a live bot or Supabase, or no suite yet**: make the smallest fix, then write numbered manual test steps (command to run, expected reply, the bug scenario first) and post them on the Taiga issue with `add_comment(project=39, ref=<bug-ref>, text=..., entity_type="issue")`. They are run in the test Discord server (story #31) or against the dashboards with the Chrome DevTools MCP. Record "no test suite yet (#34)" when that is the reason.

Never run `python cholobots.py` from this skill.

## Step 5: Verify

Run, as foreground Bash calls (never `run_in_background`, `Monitor`, or end your turn waiting for a task notification: this runs headless, so there is no next turn and the run dies with you):

1. `python -m py_compile cholobots.py config.py db/database.py bots/*.py` (mandatory before every commit)
2. `python -m pytest -q` if `tests/` holds a suite, else record "no test suite yet (#34)"
3. `ruff check .` if ruff is configured (story #33), else skip

Fix every failure, then re-run.
- **If you believe a failure is unrelated to your changes**, you MUST follow the Test Failure Triage Protocol in `.claude/skills/shared/test-failure-triage.md` before dismissing it.

## Step 6: Commit, push, create PR

1. Commit the changes with a descriptive message:
   ```
   git commit -m "fix(#<ref>): <description of what was fixed>

   Co-Authored-By: <your session's model> <noreply@anthropic.com>"
   ```
   Use your own session's attribution trailer, see `.claude/skills/shared/commit-attribution.md`.
2. Fix any failures, re-run, and re-commit.
3. Push the branch to upstream:
   ```
   git push -u upstream fix/<ref>-<slug>
   ```
4. Extract bug context from the issue details fetched in Step 1.2:
   a. **Bug description excerpt**: the first paragraph of the issue description (or the first 2-3 sentences). If the description has a `## Steps to Reproduce` or similar section, extract that separately. Keep the excerpt under ~400 characters.
   b. **Root cause**: the root cause from the implementation plan if present, otherwise one sentence from your investigation.
   c. **Reproduction steps**: brief reproduction steps from the bug description if present. Omit this subsection if not available.
5. Create a PR:
   ```
   env -u GH_TOKEN gh pr create --repo MrsBohemian/Cholobots --base main --head <branch> --title "fix(#<ref>): <bug subject>" --body-file <body> [--label mark-done-on-merge]
   ```
   - No `--reviewer` and no `--head owner:` prefix; the branch lives in the same repo.
   - Add `--label mark-done-on-merge` iff Step 1 saw `--mark-done-on-merge` **and** the bug's plan says `**Closes on merge:** eligible`. A missing line counts as not eligible. See [`../shared/merge-side-effects.md`](../shared/merge-side-effects.md). If the label is rejected by the repo, create the PR without it and say so in the report.
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

   Never include empty subsections. If reproduction steps are not available in the bug description, omit the **Reproduction:** line entirely.
6. Mark the issue ready for review: `update_issue(project=39, ref=<ref>, status="Ready for review")`.
7. Add the PR link to the Taiga bug as a comment:
   `add_comment(project=39, ref=<ref>, text="PR: https://github.com/MrsBohemian/Cholobots/pull/<pr_number>", entity_type="issue")`
8. Run the Codex review on the new PR following `.claude/skills/shared/codex-review.md`. A review failure is reported, not fatal; continue with the remaining steps.

## Step 7: Report

Return:
- Which bug was fixed (subject and ref)
- Root cause (one sentence)
- What regression tests were added, or where the manual test steps were posted
- PR number and URL, and the branch name
- Codex review result (the `REVIEW_POSTED`/`REVIEW_SKIPPED` line)
- Check results (py_compile, and pytest/ruff pass/fail or "not configured")
- Merge side effects: whether the PR carries `mark-done-on-merge`, and why

**Do NOT close the issue or set `Ready for test`.** The orchestrator handles assignment (`"Jon Roberts"`) and escalation (`Blocked` + `escalation-needed`) after PR monitoring confirms stability. The bug moves to `Closed` on merge: taiga-sync does it when the PR carries `mark-done-on-merge`, otherwise Jon (or the overseer's close-on-evidence rule) does.
