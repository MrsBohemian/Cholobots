---
name: finalize-story-pr
description: Push a story branch, run checks, create PR, and update Taiga status
argument-hint: "<story-ref> <branch-name> [base-branch]"
allowed-tools:
  - Bash(scripts/taiga-story.sh *)
  - Bash(scripts/codex-review-pr.sh *)
  - mcp__taiga-py__get_story
  - mcp__taiga-py__update_story
  - mcp__taiga-py__set_story_status
  - mcp__taiga-py__assign_item
  - mcp__taiga-py__get_task
  - mcp__taiga-py__add_comment
  - Bash(cd *)
  - Bash(git *)
  - Bash(env -u GH_TOKEN gh *)
  - Bash(python *)
  - Bash(python3 *)
  - Bash(timeout 590 *)
  - Bash(ruff *)
  - Bash(bash tests/scripts/*)
  - Read(//tmp/**)
  - Write(//tmp/**)
  - Read
  - Edit
  - Write
  - Grep
  - Glob
---

# Finalize Story PR — $ARGUMENTS

Push a story branch, run the checks, create a PR on `MrsBohemian/Cholobots`, and update Taiga. This skill runs after all tasks have been implemented and committed locally.

Taiga names: [`../shared/taiga-project.md`](../shared/taiga-project.md). Every MCP call passes `project=39`. Every `gh` call is `env -u GH_TOKEN gh ... --repo MrsBohemian/Cholobots` (the exported `GH_TOKEN` cannot see that repo).

## Step 1: Verify branch state

Parse arguments from `$ARGUMENTS`:
- First word: story ref number
- Second word: branch name
- Third word (optional): the base branch this PR targets. `/fully-implement-story` and `/implement-story` pass their own base straight through. When absent, `<base-branch>` is `main`. Getting it wrong silently attributes a predecessor branch's commits to this story.

**Worktree.** The spawn prompt names the worktree (`Work only in <worktree>`); call it `<wt>` and prefix every command with `cd <wt> &&` or use `git -C <wt>`. Never `cd` to the main checkout, check out another branch, `git reset --hard` or `git stash`. A manual run without a named worktree uses `git rev-parse --show-toplevel`.

1. Verify the branch: `git -C <wt> branch --show-current` must print `<branch-name>`. If not, report `BLOCKED: worktree <wt> is on <other-branch>` and stop (do not switch it).
2. Verify there are commits ahead of the PR's base:
   ```bash
   git -C <wt> fetch -q upstream +<base-branch>:refs/remotes/upstream/<base-branch>
   git -C <wt> log upstream/<base-branch>..<branch-name> --oneline
   ```
   Measure against `upstream/<base-branch>`, never a local branch, which is routinely behind. The explicit refspec creates the `upstream/<base-branch>` ref even for a stacked predecessor branch.
   - If there are no commits, report "No changes to push" and stop.

## Step 1b: Test quality review

If the branch changes anything under `tests/` (`git -C <wt> diff --name-only upstream/<base-branch>...<branch-name> -- tests/`), follow the `/review-tests` skill with argument `<branch-name>`: read `.claude/skills/review-tests/SKILL.md` and execute it in this conversation (no subagent). Otherwise record "no test changes".

## Step 2: Run checks

Cholobots has no CI yet (story #35), so these local checks are the only gate before review. Consult `docs/testing-workflow.md` for routing.

Pick the interpreter: `main=$(dirname "$(git -C <wt> rev-parse --path-format=absolute --git-common-dir)"); ls "$main/.venv/bin/python" 2>/dev/null || echo python3`; call the result `<py>`.

Run each as a foreground Bash call with `timeout: 600000`, wrapped in `timeout 590`. Never use `run_in_background`, `Monitor`, or end your turn waiting for a notification: this runs headless.

1. **Compile check (mandatory):** `cd <wt> && timeout 590 <py> -m py_compile cholobots.py config.py db/database.py bots/*.py; echo "exit=$?"`
2. **Bot unit tests:** if any test file exists under `tests/` outside `tests/scripts/`, `cd <wt> && timeout 590 <py> -m pytest -q --ignore=tests/scripts; echo "exit=$?"`. Otherwise record "no test suite yet (#34)".
3. **Script tests:** if the branch changes `scripts/` or `tests/scripts/`, `cd <wt> && timeout 590 <py> -m pytest tests/scripts -q` and each `bash tests/scripts/test-*.sh` that covers a changed script.
4. **Lint:** if ruff is configured (`ruff.toml`, `.ruff.toml`, or `[tool.ruff]` in `pyproject.toml`), `cd <wt> && ruff check .`. Otherwise record "no linter configured yet (#33)".
5. On a failure: fix it (this agent has Edit/Write), re-run the check, and commit the fix (re-run the compile check before the commit; trailer per `.claude/skills/shared/commit-attribution.md`). **If you believe a failure is unrelated to the story's changes**, you MUST follow the Test Failure Triage Protocol in `.claude/skills/shared/test-failure-triage.md` before dismissing it.

## Step 3: Push and create PR

1. Push the branch: `git -C <wt> push -u upstream <branch-name>`.
2. Fetch the story: `get_story(project=39, ref=<story-ref>)` for subject, description and tags.
3. Extract story context from the description for the PR body:
   a. **Overview**: the text between `### Overview` and the next `###` heading (2 to 3 sentences). If absent, the first paragraph after a `---` separator; if neither exists, the story subject.
   b. **Acceptance Criteria**: a `## Acceptance Criteria` bullet list, verbatim. If absent, omit the subsection.
   c. **Scope**: `### What Does NOT Need to Change` or `## Scope Boundaries`, summarised to 1 to 2 lines. If absent, omit.
   d. **Architecture**: `### Architecture`, summarised to which modules, commands and tables changed. If absent, omit.
   e. **Manual test steps**: the plan's `### Verification` manual steps, plus any steps implementers posted on tasks. Keep the Story Context section under ~800 characters; never include empty subsections.
4. Decide the merge side effects, applying the deciding rule in [`../shared/merge-side-effects.md`](../shared/merge-side-effects.md) to the story as implemented:
   a. Re-fetch the tasks: `scripts/taiga-story.sh <story-ref> 39 --with-tasks`. A task still open stays open after merge when it has a `## Gate` section (`get_task(project=39, ref=<task-ref>)`), the plan's `### Merge Side Effects` section lists it, or an implementer reported it `GATED:`.
   b. `close-tasks-on-merge` applies iff no task stays open after merge. `mark-done-on-merge` applies iff `close-tasks-on-merge` applies **and** the plan says `Post-merge validation: none` **and** implementation found none the plan missed (a command-handler change verified only by manual steps that have not been run in the test Discord server is post-merge validation). A plan without that section gets no `mark-done-on-merge`.
   c. If the story carries a `close-tasks-on-merge` or `mark-done-on-merge` tag that no longer applies, remove it: `update_story(project=39, ref=<story-ref>, tags=<full current list minus that tag>)`. A stale tag would fire at merge on its own.
5. Write the PR body to `/tmp/pr-body-<story-ref>.md` and create the PR:
   ```bash
   env -u GH_TOKEN gh pr create --repo MrsBohemian/Cholobots --base <base-branch> --head <branch-name> \
     --title "#<story-ref>: <story subject>" --body-file /tmp/pr-body-<story-ref>.md
   ```
   Body template:
   ```markdown
   ## Story Context

   **[#<ref>](https://backlog.growthgarage.org/project/cholo-bots/us/<ref>): <subject>**

   <extracted overview, 2-3 sentences max>

   **Acceptance criteria:**
   <extracted bullet list; DELETE THIS HEADING AND THE LIST if the story has none>

   **Scope:** <summary; DELETE THIS LINE if absent>

   **Architecture:** <summary; DELETE THIS LINE if absent>

   ## Tasks completed

   - <task 1 subject>
   - ...

   ## Test plan

   - [x] `python -m py_compile cholobots.py config.py db/database.py bots/*.py`
   - [ ] Unit tests: <N passed | no test suite yet (#34)>
   - [ ] Lint: <clean | no linter configured yet (#33)>
   - [ ] Manual steps in the test Discord server (#31):
     1. <exact message> → <expected reply>

   **CI:** none configured yet (story #35); the checks above are the only automated gate.

   ## Known pre-existing failures

   <"- #NNN: <test name>" for failures verified via the Test Failure Triage Protocol; omit the section if none>

   🤖 Generated with [Claude Code](https://claude.com/claude-code)
   ```
   Take the PR number from the URL `gh pr create` prints.
6. Apply the merge-side-effect labels decided in step 4, one call per label:
   ```bash
   env -u GH_TOKEN gh pr edit <pr-number> --repo MrsBohemian/Cholobots --add-label close-tasks-on-merge
   ```
   If a call fails because the label does not exist, create it once (`env -u GH_TOKEN gh label create <name> --repo MrsBohemian/Cholobots`) and retry. If it still fails, fall back to the same-named Taiga tag: re-fetch the story and `update_story(project=39, ref=<story-ref>, tags=[<current tags...>, "<name>"])`. Either signal is enough at merge. Record which route each signal took.
7. Comment the PR link on the story: `add_comment(project=39, ref=<story-ref>, text="PR: https://github.com/MrsBohemian/Cholobots/pull/<pr-number>", entity_type="story")`.
8. Run the Codex review on the new PR following `.claude/skills/shared/codex-review.md` (pass 1). A review failure is reported, not fatal; continue.

## Step 4: Update Taiga

1. Set the story to `Ready for review`: `set_story_status(project=39, ref=<story-ref>, status="Ready for review")`.
2. Assign it to Jon: `assign_item(project=39, ref=<story-ref>, username="Jon Roberts", entity_type="story")`.

**Do NOT set the story to `Done`** yourself. Done happens either by Jon after merging the PR, or by the post-merge sync when the PR is labelled `mark-done-on-merge` (or the story keeps that tag).

## Step 5: Report

Return:
- PR number and URL
- The base branch the PR targets
- Check results (compile, unit tests or "#34", lint or "#33", script tests if run) and the "no CI yet (#35)" note
- Codex review result (the `REVIEW_POSTED`/`REVIEW_SKIPPED`/`REVIEW_PENDING` line)
- Merge side effects: which labels the PR carries, which fell back to Taiga tags, which tags were removed, and why
- Story status update confirmation (`Ready for review`, assigned to Jon Roberts)

## What this skill does NOT do

- Does NOT implement any tasks (implement-task does that)
- Does NOT monitor the PR after creation (the orchestrator does that)
- Does NOT mark the story `Done`
