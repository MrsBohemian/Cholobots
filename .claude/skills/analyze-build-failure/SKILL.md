---
name: analyze-build-failure
description: Analyze a failed GitHub Actions CI build (or, until CI exists, a locally reproduced check failure) from a PR or run ID, diagnose the failure, and post findings as a PR comment
argument-hint: "pr-<number> or build-<run-id>"
allowed-tools:
  - Bash
  - Read
  - Grep
  - Glob
  - WebFetch(https://github.com/*)
---

# Analyze Build Failure $ARGUMENTS

Investigate a failed build, diagnose the root cause, and post the analysis as a PR comment for review.

All GitHub calls target `MrsBohemian/Cholobots` and are prefixed `env -u GH_TOKEN` (the shell's `GH_TOKEN` cannot see that repo). Never run the bot (`python cholobots.py`); importing it starts the Discord client.

## CI status today

GitHub Actions does not build or test this repo yet. Story #35 adds a CI workflow; its expected shape is three jobs: **compile** (`python -m py_compile cholobots.py config.py db/database.py bots/*.py`), **lint** (`ruff check .`, once story #33 configures ruff) and **test** (`python -m pytest -q`, once story #34 adds `tests/`). `.github/workflows/taiga-sync.yml` is the only workflow that exists and it is a post-merge Taiga sync, not a build; a failure there is not a build failure (report it as a sync failure and stop).

So this skill has two modes:

- **CI mode** (workflow runs exist for the PR or run ID): diagnose from the Actions logs, Steps 1 to 4.
- **Local reproduction mode** (no workflow runs): reproduce the checks yourself on the PR's head commit, Step 1b, then continue at Step 4. Say in the report that the diagnosis is from a local reproduction, not a CI run.

## Step 1: Parse the argument and locate the failure

### If the argument starts with `pr-` (PR number)

1. Extract the PR number from the argument (e.g., `pr-172` → `172`).
2. Get the PR's head SHA and find failed workflow runs (uses the Actions API, compatible with fine-grained PATs):
   ```
   PR_SHA=$(env -u GH_TOKEN gh pr view <number> --repo MrsBohemian/Cholobots --json headRefOid -q .headRefOid)
   env -u GH_TOKEN gh run list --repo MrsBohemian/Cholobots --commit "$PR_SHA" --json name,status,conclusion,databaseId,createdAt --jq '
     group_by(.name) | map(sort_by(.createdAt) | last) | .[]
     | select(.status == "completed" and .conclusion != "success" and .conclusion != "skipped")
   '
   ```
3. If the list is empty, check whether any runs exist at all (`env -u GH_TOKEN gh run list --repo MrsBohemian/Cholobots --commit "$PR_SHA"`).
   - Runs exist and all passed: inform the user and stop: "PR #<number> has no failed workflow runs."
   - No runs at all (today's normal case): go to **Step 1b** (local reproduction).
4. Extract the run database ID from the output.
5. Proceed to Step 2 with the run ID(s). If multiple runs failed, analyze the build/CI workflow run, not `taiga-sync`.

### If the argument starts with `build-` (run database ID)

1. Extract the run ID from the argument (e.g., `build-22463874342` → `22463874342`).
2. Verify the run exists and check its status:
   ```
   env -u GH_TOKEN gh run view <run-id> --repo MrsBohemian/Cholobots --json status,conclusion,headBranch,event,name,number,url
   ```
3. If the run succeeded, inform the user and stop: "Build <run-id> succeeded — no failure to analyze."
4. If the run is still in progress, inform the user and stop: "Build <run-id> is still running. Wait for it to finish."
5. Proceed to Step 2.

### If the argument doesn't match either pattern

Ask the user: "I need either `pr-<number>` (e.g., `pr-172`) or `build-<run-id>` (e.g., `build-22463874342`). Which build should I analyze?"

### Step 1b: Local reproduction (no CI runs exist)

Use an isolated worktree so the main checkout is untouched (do not check out the PR branch there):

```
BRANCH=$(env -u GH_TOKEN gh pr view <number> --repo MrsBohemian/Cholobots --json headRefName -q .headRefName)
git fetch upstream "$BRANCH"
git worktree add --detach .claude/worktrees/analyze-pr-<number> "upstream/$BRANCH"
```

In that worktree, run each check as a foreground Bash call with `timeout: 600000` wrapped in `timeout 590`; never `run_in_background`:

1. `python -m py_compile cholobots.py config.py db/database.py bots/*.py` — always.
2. `ruff check .` — only if ruff is configured (`ruff.toml` or `[tool.ruff]` in `pyproject.toml`); else note "no linter yet (#33)".
3. `python -m pytest -q` — only if `tests/` exists; else note "no test suite yet (#34)". `tests/integration` needs the local Supabase stack (story #47); if it is not running, record that as an environment limitation, not a failure.

If a venv is needed: `python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt` inside the worktree.

If every check passes, report "PR #<number> passes all locally runnable checks" and stop. Otherwise take the failing command's output as your log and continue at Step 3. Remove the worktree when done: `git worktree remove --force .claude/worktrees/analyze-pr-<number>`.

## Step 2: Get the failure details (CI mode)

### 2a. Identify the failed step

```
env -u GH_TOKEN gh run view <run-id> --repo MrsBohemian/Cholobots --json jobs --jq '.jobs[] | {name, conclusion, steps: [.steps[] | select(.conclusion == "failure") | {name, number}]}'
```

Note the failed job name and step name. The expected CI jobs that can fail are:
- **Compile** — `py_compile` syntax errors
- **Lint** — ruff violations
- **Test** — pytest failures (unit, integration, or script tests)

If the workflow differs from this shape, use whatever steps it actually defines.

### 2b. Get the failed step logs

```
env -u GH_TOKEN gh run view <run-id> --repo MrsBohemian/Cholobots --log-failed
```

This returns the logs from only the failed step(s). Read the output carefully — the error details are in here.

**IMPORTANT:** The log output can be very large. Focus on:
- Lines containing `error`, `Error`, `FAIL`, `failed`, `FAILED`, `Traceback`, `exception`, `Exception`
- The last 100 lines of output (these usually contain the summary and stack traces)
- For test failures: the test id (`tests/unit/test_x.py::test_name`), assertion message, and expected vs actual values
- For compile failures: the file, line number, and `SyntaxError` message
- For lint failures: the rule codes and files

### 2c. If the logs are truncated or unclear

Use the full log archive:
```
env -u GH_TOKEN gh api repos/MrsBohemian/Cholobots/actions/runs/<run-id>/logs -H "Accept: application/vnd.github+json" > /tmp/build-logs.zip && unzip -o /tmp/build-logs.zip -d /tmp/build-logs/
```
Then read the relevant log file from `/tmp/build-logs/`.

## Step 3: Find the associated PR

If you started from a `build-` argument, find the PR:

```
env -u GH_TOKEN gh run view <run-id> --repo MrsBohemian/Cholobots --json headBranch,event --jq '{headBranch, event}'
```

Then find the PR for that branch:
```
env -u GH_TOKEN gh pr list --repo MrsBohemian/Cholobots --state all --head <branch-name> --json number,state --jq '.[0]'
```

If no PR exists (e.g., a push to main), note this — you'll skip the PR comment step.

## Step 4: Diagnose the root cause

Classify the failure using the categories in `.claude/skills/shared/test-failure-triage.md`, then investigate:

### Compile failures

1. Parse the error(s): file path, line number, `SyntaxError`/`IndentationError` message.
2. Read the offending file(s) at the relevant lines.
3. Identify the root cause — a bad merge leaving a duplicate `def`, a Python-version feature (the repo targets Python 3.14), an unclosed bracket, etc.
4. Check `git log --oneline -5` on the PR branch to see what recent changes may have caused it.
5. Form a specific diagnosis: "File X, line Y has error Z because..."

### Lint failures

1. The log lists rule codes per file. Undefined names (`F821`) and unused imports (`F401`) after a rebase are the common ones.
2. If ruff is configured, `ruff check --fix` handles the auto-fixable rules; the diagnosis only needs to name the files and codes.

### Test failures

Determine which suite failed (`tests/unit`, `tests/integration`, `tests/scripts`; layout in `docs/testing-strategy.md`).

#### Structured test results (preferred)

If the CI workflow uploads a test-results artifact (JUnit XML from `pytest --junitxml`), download it:
```
env -u GH_TOKEN gh run download <run-id> --repo MrsBohemian/Cholobots -n test-results -D /tmp/test-results
```
and read the failing cases from it. This is faster and more reliable than parsing console logs. If the artifact isn't available, fall back to the console logs.

#### Console log analysis (fallback)

- Find the pytest short summary (`FAILED tests/...::test_name - AssertionError: ...`).
- For unit failures: read the failing test, then the helper under test, and compare expected vs actual.
- For integration failures: check first whether the local Supabase stack or its seed data was available in that job; a connection error is an environment problem, not a code regression.
- For script-test failures: read the shell or Python script under test and the fixture it feeds.
- A failure at import time that mentions `DISCORD_TOKEN`, `OPENAI_API_KEY`, `SUPABASE_URL` or `SUPABASE_SERVICE_ROLE_KEY` means a test imported a bot module (`config.py` dies at import without them, and `cholobots.py` starts the bot). That is a test-design bug: tests of pure helpers must not import those modules at module level without stubbing the environment.

Then investigate:
1. Read the failing test file to understand what it expects.
2. Read the code under test to understand what's actually happening.
3. Check recent commits on the PR branch to see what changed.
4. Form a root cause hypothesis. If you suspect the failure is pre-existing, verify it on the base branch per `.claude/skills/shared/test-failure-triage.md` before saying so.

## Step 5: Compose the analysis report

Write a structured analysis using this template:

```markdown
## Build Failure Analysis

**Run:** [<run-url>](<run-url>)   (or "local reproduction on <short SHA>; no CI configured yet (#35)")
**Failed step:** <step-name>
**Failed at:** <timestamp or "N/A">

### What Failed

[1-3 sentences. Name the specific test/compile/lint error. Be precise — include the test id, error message, or rule code.]

### Root Cause

[1-3 sentences. Explain WHY it failed. Name the specific file, function, and what's wrong. If you can't determine root cause with certainty, state your best hypothesis and what's uncertain.]

### Suggested Fix

[Specific, actionable steps to fix the failure. Include:]
- Which file(s) to change
- What the change should be
- If it's an auto-fixable lint issue, just say "run `ruff check --fix .`"

### Relevant Code

[If applicable, show the problematic code snippet with file path and line numbers]

---
cc @bigjonroberts
```

## Step 6: Post the analysis

### If there's an associated PR

Post the analysis as a PR comment, using a heredoc to avoid shell escaping issues:
```
env -u GH_TOKEN gh pr comment <pr-number> --repo MrsBohemian/Cholobots --body "$(cat <<'EOF'
<analysis content here>
EOF
)"
```

### If there's no PR (push to main)

Print the analysis to the user directly — there's no PR to comment on. Suggest they file a bug (`.claude/skills/shared/bug-filing.md`) or a fix commit as appropriate.

## Step 7: Report

Tell the user:
- Which build (or local reproduction) you analyzed and what failed
- The root cause (one sentence)
- That the analysis has been posted to PR #<number> (or printed, if no PR)
- Link to the PR comment if posted
