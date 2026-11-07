---
name: rebase-branch
description: Rebase a PR branch onto a target branch, resolve conflicts using story/issue/epic context, verify, and force-push. Escalates to Jon when stuck.
argument-hint: "<PR-number> [target-branch]"
allowed-tools:
  - mcp__taiga-py__get_story
  - mcp__taiga-py__get_epic_overview
  - mcp__taiga-py__update_story
  - mcp__taiga-py__get_issue
  - mcp__taiga-py__update_issue
  - mcp__taiga-py__assign_item
  - mcp__taiga-py__add_comment
  - Bash(python -m py_compile *)
  - Bash(python -m pytest *)
  - Bash(ruff *)
  - Bash(git *)
  - Bash(env -u GH_TOKEN gh *)
  - Read(//tmp/**)
  - Read
  - Edit
  - Write
  - Grep
  - Glob
---

# Rebase Branch — PR #$ARGUMENTS

Rebase a PR branch onto a target branch, resolve merge conflicts intelligently using story/issue and epic context, verify, and force-push to the PR branch on `upstream`. Escalates to Jon when a conflict resolution requires human judgment.

Remotes: `upstream` = `MrsBohemian/Cholobots` holds `main`, every PR branch and every PR; fetch and push there. `origin` is only a mirror and is never used here. Every `gh` call is `env -u GH_TOKEN gh ... --repo MrsBohemian/Cholobots`.

## Running checks

Every check in this skill (Step 6) runs as a foreground Bash call with the tool's `timeout: 600000` parameter set explicitly, wrapped in a shell `timeout 590` so it ends inside the call. Never use `run_in_background`, `Monitor`, or end your turn waiting for a task notification — this runs headless, so there is no next turn and the run dies with you. On exit 124, run a narrower target per [docs/testing-workflow.md](../../../docs/testing-workflow.md) instead of a longer timeout. Never run the bot (`python cholobots.py`).

## Step 1: Parse arguments and gather PR context

Parse `$ARGUMENTS`:
- First word: PR number
- Second word (optional): target branch. Do not fall back to `main` without checking. When the caller omits this argument, use the PR's own base instead:
  ```bash
  env -u GH_TOKEN gh pr view <PR> --repo MrsBohemian/Cholobots --json baseRefName -q .baseRefName
  ```
  Step 1's `gh pr view` call below already fetches `baseRefName`, so this is the same value read from that response, not a second lookup. A stacked PR targets a `feature/*` or `epic/*` branch, not `main` — rebasing it onto `main` would replay its commits over every sibling it was stacked behind, turning a two-file conflict into the whole stack's diff. When `/fully-implement-story` invokes this skill it always passes the base explicitly, so this fallback only fires on direct invocations and `/remediate-pr`.

Gather information in parallel:

1. **PR details**: `env -u GH_TOKEN gh pr view <PR> --repo MrsBohemian/Cholobots --json title,body,headRefName,baseRefName,state,number,url`
2. **Current branch**: `git branch --show-current`

If the PR is closed or merged, report and stop.

## Step 2: Identify the associated Taiga item

Look for a Taiga numeric ref in the PR title, body, or branch name. Accept any of these patterns and extract the numeric portion as `<ref>`:

- `#NNN` anywhere in the PR title, body, or branch name
- Branch names matching `feature/<ref>-<slug>` or `fix/<ref>-<slug>` (e.g., `fix/12-ping-timezone`)
- Title/body text like `bug <ref>` (e.g., `bug 12`)

Use the first confident match you find. If a ref is found:

1. First try a story lookup: `get_story(project=39, ref=<ref>)`.
   - If it returns a result, fetch the epic if linked via `get_epic_overview(project=39, ref=<epic ref>)`. Save the story subject, description, epic description (if any), the current `status` and `tags` list. Record `taigaType = "story"` and `taigaRef = <ref>`.
2. If the story lookup fails (no result or error), try an issue lookup: `get_issue(project=39, ref=<ref>)`.
   - If it returns a result, save the subject, description, current `status`, and `tags` list. Record `taigaType = "issue"` and `taigaRef = <ref>`. Issues don't have epics, so skip the epic lookup.
3. If neither lookup returns a result, proceed without Taiga context (resolve conflicts on code merit alone).

The saved subject + description serve as your **resolution context** regardless of type — they tell you what the PR is trying to accomplish so you can make informed conflict resolution decisions. `taigaType` and `taigaRef` are needed later in Step 5c for escalation updates. (The workflow tools are ref-based; no internal id is needed. If a REST script needs one, `scripts/taiga-story.sh <ref> 39 [--issue]` returns it.)

## Step 3: Prepare for rebase

1. Fetch from `upstream`, with an explicit destination for the target branch so the remote-tracking ref exists even in single-branch clones:
   ```
   git fetch upstream
   git fetch upstream +<target-branch>:refs/remotes/upstream/<target-branch>
   ```
   Call the resulting ref `<target-ref>` (`upstream/<target-branch>`, normally `upstream/main`). Fetching without the `:refs/remotes/...` destination would only populate `FETCH_HEAD`, not the ref every command below consumes.

2. Check out the PR branch (if not already on it):
   ```
   git checkout <head-branch>
   git pull upstream <head-branch>
   ```

3. Preview what will happen — identify files that will conflict. Use `<target-ref>` throughout, not the bare local name (a normal checkout has no local ref for a stacked branch, and local `main` can lag):
   ```
   git diff --name-only <head-branch>...<target-ref>
   ```
   This shows files changed on the target since the branches diverged. Cross-reference with files changed on the PR branch:
   ```
   git diff --name-only $(git merge-base <head-branch> <target-ref>)..<head-branch>
   ```
   Files appearing in both lists are **potential conflict zones**. Note them.

4. Record the pre-rebase HEAD for recovery:
   ```
   git rev-parse HEAD
   ```
   Save this SHA — if the rebase goes badly, `git rebase --abort` restores to this state.

## Step 4: Start the rebase

```
git rebase <target-ref>
```

If the rebase completes with no conflicts, skip to **Step 6**.

If the rebase stops with conflicts, proceed to **Step 5**.

## Step 5: Resolve conflicts

For each conflict round (the rebase may stop multiple times):

### 5a. Identify conflicting files

```
git diff --name-only --diff-filter=U
```

### 5b. For each conflicting file, analyze and resolve

1. **Read the file** to see the conflict markers (`<<<<<<<`, `=======`, `>>>>>>>`).

2. **Understand both sides in a rebase conflict:**
   - **Upstream (HEAD)** (above `=======`, from `<<<<<<< HEAD`): the target branch state you're rebasing onto — what was merged since the PR branched off
   - **Rebased commit (PR branch)** (below `=======`, from `>>>>>>> <commit>`): the commit from the PR branch currently being replayed — what this story intended

3. **Determine resolution strategy** using this priority order:

   **a. Additive / non-overlapping changes** (most common for this codebase):
   Both sides add new content (e.g., new commands appended to the same bot module, new entries in the attachment-gate whitelist or the `on_ready` load order in `cholobots.py`, new lines in a bot's hand-written `!<name>` help text, new packages in `requirements.txt`, new rows in the same doc). **Keep both.** Combine the additions, keeping the file's existing ordering (alphabetical for requirements and imports, logical grouping for commands and help text).

   **b. Epic boundary rules** (if epic context is available):
   The epic description defines which story "owns" which code. If the conflict involves code owned by a specific story, that story's version wins. Example: if the epic says "#51 owns the Supabase client factory in `db/`", and this PR is for #47, use #51's version of the factory.

   **c. Semantic analysis** (when both sides modify the same code):
   - If the target branch refactored/renamed something and the PR branch used the old name, **adopt the target's refactoring** and update the PR's usage.
   - If the PR branch intentionally changed behavior that the target also changed differently, this requires judgment — apply the **escalation check** below.

   **d. Formatting / whitespace conflicts**:
   Accept whichever side has more context. If `ruff` is configured (story #33), `ruff check --fix` on the touched files may clean up; otherwise leave formatting as the target has it.

4. **Escalation check** — escalate if ANY of these are true:
   - Both sides intentionally changed the same logic/behavior in different ways
   - The resolution would change the functional intent of either story
   - You're uncertain which side is correct after reading both stories
   - The conflict involves a Supabase table or column used across bots (`crudo_*`, `chisme_*`, `obijuan_*`, `wtp_*`; the schema is not in the repo, so column names can only be checked against queries), a shared timestamp convention, or a schema migration file
   - The conflict involves the `on_message` listeners, the attachment-gate whitelist logic, or `bot.wait_for` flows where both sides changed control flow
   - More than 10 files have non-trivial conflicts in a single rebase round

   If escalation is needed, proceed to **Step 5c** (Escalate).

5. **Apply the resolution**: Edit the file to remove conflict markers and produce the correct merged content. Do NOT leave any conflict markers (`<<<<<<<`, `=======`, `>>>>>>>`) in the file.

6. **Stage the resolved file**: `git add <file>`

### 5c. Escalate (when stuck)

If you hit an escalation trigger:

1. **Abort the rebase** to leave the branch in a clean state:
   ```
   git rebase --abort
   ```

2. **Document what happened** in a PR comment:
   ```bash
   env -u GH_TOKEN gh pr comment <PR> --repo MrsBohemian/Cholobots --body "$(cat <<'EOF'
   ## Rebase Conflict — Needs Human Resolution

   **Target branch:** <target-branch>
   **Conflicting files:**
   - `path/to/file1.py` — <brief description of conflict>
   - `path/to/file2.py` — <brief description of conflict>

   **Why this needs human judgment:**
   <1-3 sentences explaining why automated resolution isn't safe>

   **Story/Issue context:**
   - This PR: #<ref> — <subject>
   - Conflicting with changes from: <commit range or PR that introduced the other side>

   ---
   cc @bigjonroberts
   EOF
   )"
   ```

3. **Update the Taiga item** (if one was identified in Step 2). Escalation is status `Blocked`, tag `escalation-needed` (plus `rebase-conflict`), and assignment to `"Jon Roberts"` (names in `.claude/skills/shared/taiga-project.md`; the workflow server resolves assignees by full name, not by Taiga username). Tags are replaced wholesale, so write back the saved list plus the new tags. Save the item's current status first and put it in the comment so whoever unblocks it can restore it.

   **If `taigaType` is `"story"`:**
   - `update_story(project=39, ref=<taigaRef>, status="Blocked", tags=<existing_tags> + ["escalation-needed", "rebase-conflict"])`
   - `assign_item(project=39, ref=<taigaRef>, username="Jon Roberts", entity_type="story")`
   - `add_comment(project=39, ref=<taigaRef>, entity_type="story", text="Rebase of PR #<PR> onto <target-branch> hit a conflict that needs human judgment: <one-line reason>. Previous status: <status>. See the PR comment.")`

   **If `taigaType` is `"issue"`:**
   - `update_issue(project=39, ref=<taigaRef>, status="Blocked", tags=<existing_tags> + ["escalation-needed", "rebase-conflict"])`
   - `assign_item(project=39, ref=<taigaRef>, username="Jon Roberts", entity_type="issue")`
   - `add_comment(project=39, ref=<taigaRef>, entity_type="issue", text="<same text>")`

   **If no Taiga item was identified:** skip Taiga updates.

4. **Report** the escalation and stop. Do NOT proceed with a partial or uncertain resolution.

### 5d. Continue the rebase

After resolving all conflicts in the current round:

```
git rebase --continue
```

If the rebase stops again with more conflicts, repeat Step 5. If it completes, proceed to Step 6.

## Step 6: Verify the rebase

### 6a. Sanity check

1. Verify no conflict markers remain anywhere in tracked files:
   ```
   git grep -l '<<<<<<<' || echo "No conflict markers found"
   ```
   If any are found, resolve them before continuing.

2. Verify the commit history looks right:
   ```
   git log --oneline <target-ref>..HEAD
   ```
   The PR's commits should be replayed on top of the target branch.

The compile check in 6b and the tests in 6c follow the foreground rule in "Running checks" above.

### 6b. Compile check

Catch syntax and import-level errors introduced by the rebase:

```
python -m py_compile cholobots.py config.py db/database.py bots/*.py
```

If it fails:
1. Read the error carefully.
2. These are likely due to the rebase combining changes that are valid independently but not together (e.g., a helper was renamed on main and the PR calls it by the old name, or a duplicated function definition after an additive merge).
3. Fix the errors. If you're still in the middle of the rebase, stage the fixes and run `git rebase --continue`. If the rebase has completed, commit the fixes as a new commit (do **not** amend rebased commits).
4. If the errors are too complex to resolve confidently, **escalate** per Step 5c.

`py_compile` does not catch a wrong name or a missing import. Also `git diff <target-ref>...HEAD --stat` and skim each resolved file for references to symbols that either side renamed. If `ruff` is configured, `ruff check .` catches undefined names; otherwise do this by reading.

### 6c. Run tests

If `tests/` exists, run the suites most likely affected by the conflicting files, per [docs/testing-workflow.md](../../../docs/testing-workflow.md): `python -m pytest -q tests/unit` first, then `tests/integration` (needs the local Supabase stack, story #47) or `tests/scripts` when the conflicts touched those areas. If `tests/` does not exist, record "no test suite yet (#34)" in the report; the compile check and the read-through above are the verification.

If tests fail:
1. You MUST follow the Test Failure Triage Protocol in `.claude/skills/shared/test-failure-triage.md` to determine whether the failure is due to the rebase resolution or a pre-existing issue. Do NOT assume a failure is pre-existing without verifying on the base branch.
2. If due to the rebase: fix the issue, commit the fix as a new commit (not amending a rebased commit).
3. If verified as pre-existing: file a Taiga bug per the triage protocol, note it in the report, and don't block on it.
4. Re-run the affected suite to verify.

## Step 7: Force-push

Once the compile check passes and tests are green (or absent, or failures are pre-existing):

```
git push --force-with-lease upstream <head-branch>
```

The PR picks the new head up automatically. Use `--force-with-lease` (not `--force`) to avoid overwriting changes pushed by someone else since we fetched.

If force-with-lease is rejected, someone else pushed to the branch. Fetch, check what changed, and re-evaluate.

## Step 8: Report

Post a comment on the PR summarizing the rebase:

```bash
env -u GH_TOKEN gh pr comment <PR> --repo MrsBohemian/Cholobots --body "$(cat <<'EOF'
## Rebase Complete

**Rebased onto:** `<target-branch>` (<short SHA>)

### Conflicts Resolved
- `path/to/file.py` — <brief description of resolution>

### Resolution Strategy
<1-2 sentences: e.g., "Additive changes combined" or "Adopted main's refactoring of X, updated PR's usage">

### Verification
<py_compile result; pytest result or "no test suite yet (#34)">

---
🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Tell the caller:
- Whether the rebase succeeded or was escalated
- Which target branch was rebased onto, and whether it came from the argument or from `baseRefName`
- How many conflicts were resolved and how
- Verification summary
- The force-push SHA
