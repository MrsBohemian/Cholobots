---
name: fix-build-failure
description: Analyze a failed GitHub Actions CI build (or, until CI exists, a locally reproduced check failure) from a PR or run ID, diagnose the failure, implement fixes on the PR branch, and push
argument-hint: "pr-<number> or build-<run-id>"
allowed-tools:
  - Bash(python -m py_compile *)
  - Bash(python -m pytest *)
  - Bash(python -m venv *)
  - Bash(pip install *)
  - Bash(ruff *)
  - Bash(git *)
  - Bash(find *)
  - Bash(env -u GH_TOKEN gh *)
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
---

# Fix Build Failure $ARGUMENTS

Investigate a failed build, diagnose the root cause, implement fixes on the PR branch, and push to the PR branch on `upstream`.

All GitHub calls target `MrsBohemian/Cholobots` and are prefixed `env -u GH_TOKEN` (the shell's `GH_TOKEN` cannot see that repo). Never run the bot (`python cholobots.py`); importing it starts the Discord client.

## CI status today

GitHub Actions does not build or test this repo yet. Story #35 adds a CI workflow; its expected shape is three jobs: **compile** (`python -m py_compile cholobots.py config.py db/database.py bots/*.py`), **lint** (`ruff check .`, once story #33 configures ruff) and **test** (`python -m pytest -q`, once story #34 adds `tests/`). `.github/workflows/taiga-sync.yml` is the only workflow that exists and it is a post-merge Taiga sync, not a build.

So this skill has two modes:

- **CI mode** (workflow runs exist for the PR or run ID): diagnose from the Actions logs, Steps 1 to 4.
- **Local reproduction mode** (no workflow runs): check out the PR branch and run the checks yourself, Step 1b, then continue at Step 4. Say so in the PR comment.

## Critical constraints

- You MUST NOT create any new branches. All work happens on the PR's existing head branch.
- You MUST NOT create any new pull requests.
- You MUST NOT use `git checkout -b` at any point during this skill.
- Before every `git push`, verify `git branch --show-current` matches the PR's `headRefName`.
- Do not `git reset --hard` or `git stash`, and stay in the working directory you were given (it may be a git worktree; never `cd` to the main checkout).

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
   - No runs at all (today's normal case): go to **Step 1b**.
4. Extract the run database ID from the output.
5. Proceed to Step 2 with the run ID(s). If multiple runs failed, work on the build/CI workflow run, not `taiga-sync`.

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

Complete Step 3 first (check out the PR branch), then run each check as a foreground Bash call with `timeout: 600000` wrapped in `timeout 590`; never `run_in_background`:

1. `python -m py_compile cholobots.py config.py db/database.py bots/*.py` — always.
2. `ruff check .` — only if ruff is configured (`ruff.toml` or `[tool.ruff]` in `pyproject.toml`); else note "no linter yet (#33)".
3. `python -m pytest -q` — only if `tests/` exists; else note "no test suite yet (#34)". `tests/integration` needs the local Supabase stack (story #47); if it is not running, that is an environment limitation, not a failure.

If a venv is needed: `python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt`.

If every check passes, report "PR #<number> passes all locally runnable checks; nothing to fix" and stop. Otherwise use the failing command's output as your log and continue at Step 4.

## Step 2: Get the failure details (CI mode)

### 2a. Identify the failed step

```
env -u GH_TOKEN gh run view <run-id> --repo MrsBohemian/Cholobots --json jobs --jq '.jobs[] | {name, conclusion, steps: [.steps[] | select(.conclusion == "failure") | {name, number}]}'
```

The expected CI jobs that can fail are:
- **Compile** — `py_compile` syntax errors
- **Lint** — ruff violations
- **Test** — pytest failures (unit, integration, or script tests)

If the workflow differs from this shape, use whatever steps it actually defines.

### 2b. Get the failed step logs

```
env -u GH_TOKEN gh run view <run-id> --repo MrsBohemian/Cholobots --log-failed
```

**IMPORTANT:** The log output can be very large. Focus on:
- Lines containing `error`, `Error`, `FAIL`, `failed`, `FAILED`, `Traceback`, `exception`, `Exception`
- The last 100 lines of output (these usually contain the summary and stack traces)
- For test failures: the test id (`tests/unit/test_x.py::test_name`), assertion message, and expected vs actual values
- For compile failures: the file, line number, and `SyntaxError` message
- For lint failures: the rule codes and files

### 2c. If the logs are truncated or unclear

```
env -u GH_TOKEN gh api repos/MrsBohemian/Cholobots/actions/runs/<run-id>/logs -H "Accept: application/vnd.github+json" > /tmp/build-logs.zip && unzip -o /tmp/build-logs.zip -d /tmp/build-logs/
```
Then read the relevant log file from `/tmp/build-logs/`.

## Step 3: Find the associated PR and check out its branch

### 3a. Find the PR

If you started from a `build-` argument, find the PR:

```
env -u GH_TOKEN gh run view <run-id> --repo MrsBohemian/Cholobots --json headBranch,event --jq '{headBranch, event}'
```

Then find the PR for that branch:
```
env -u GH_TOKEN gh pr list --repo MrsBohemian/Cholobots --state all --head <branch-name> --json number,state,headRefName --jq '.[0]'
```

If no PR exists (e.g., a push to main), inform the user and stop — this skill requires a PR branch to push fixes to.

### 3b. Check out the PR branch

The PR branch lives on `upstream` (the same repo the PR is in):

```
git fetch upstream <branch-name>
git checkout <branch-name>
git pull upstream <branch-name>
```

If you were given a worktree that already has the branch checked out, skip the checkout and just pull.

## Step 4: Diagnose the root cause

Classify the failure using the categories in `.claude/skills/shared/test-failure-triage.md`, then investigate:

### Compile failures

1. Parse the error(s): file path, line number, `SyntaxError`/`IndentationError` message.
2. Read the offending file(s) at the relevant lines.
3. Identify the root cause — a bad merge leaving a duplicate `def`, a Python-version feature (the repo targets Python 3.14), an unclosed bracket, etc.
4. Check `git log --oneline -5` on the PR branch to see what recent changes may have caused it.
5. Form a specific diagnosis: "File X, line Y has error Z because..."

### Lint failures

The log lists rule codes per file. Undefined names (`F821`) and unused imports (`F401`) after a rebase are the common ones.

### Test failures

Determine which suite failed (`tests/unit`, `tests/integration`, `tests/scripts`; layout in `docs/testing-strategy.md`).

#### Structured test results (preferred)

If the CI workflow uploads a test-results artifact (JUnit XML from `pytest --junitxml`), download it:
```
env -u GH_TOKEN gh run download <run-id> --repo MrsBohemian/Cholobots -n test-results -D /tmp/test-results
```
If the artifact isn't available, fall back to the console logs.

#### Console log analysis (fallback)

- Find the pytest short summary (`FAILED tests/...::test_name - AssertionError: ...`).
- For integration failures, first rule out an environment problem (local Supabase stack or seed data missing in that job).
- An import-time failure mentioning `DISCORD_TOKEN`, `OPENAI_API_KEY`, `SUPABASE_URL` or `SUPABASE_SERVICE_ROLE_KEY` means a test imported a bot module (`config.py` dies at import without them, and `cholobots.py` starts the bot). Fix the test to stub the environment or import only the pure helper; do not weaken `config.py`.

Then investigate:
1. Read the failing test file to understand what it expects.
2. Read the code under test to understand what's actually happening.
3. Check recent commits on the PR branch to see what changed.
4. Form a root cause hypothesis.

## Step 5: Implement the fix

Follow the systematic debugging process from the root `CLAUDE.md` and TDD principles: when the fix is for a regression in testable code (pure helpers, parsers, money or time functions), add or confirm the failing test first. Discord command handlers that need a live bot cannot be unit tested; write the manual verification steps (test server, story #31) into the PR comment instead and say so.

Run each check as a foreground Bash call with the tool's `timeout: 600000` parameter set explicitly, wrapped in a shell `timeout 590` so it ends inside the call. Never use `run_in_background`, `Monitor`, or end your turn waiting for a task notification — this runs headless, so there is no next turn and the run dies with you. On exit 124, run a narrower target per [docs/testing-workflow.md](../../../docs/testing-workflow.md) instead of a longer timeout.

### For compile failures

1. Read the offending file(s) at the relevant lines.
2. Make the smallest reasonable change to fix the error(s).
3. Run `python -m py_compile cholobots.py config.py db/database.py bots/*.py` to verify.

### For lint failures

Run `ruff check --fix .` for auto-fixable rules and fix the rest by hand, then re-run `ruff check .`. Do not edit the ruff configuration to silence a rule.

### For test failures

1. Read the failing test(s) and the code under test.
2. Fix the root cause — do NOT fix symptoms or add workarounds.
3. Run **only the directly affected test target(s)** (`python -m pytest -q tests/unit/test_x.py`), then the whole directory, not the full tree unless the change is cross-cutting. Consult [docs/testing-workflow.md](../../../docs/testing-workflow.md) for the local-vs-CI policy.
4. Fix every failure, then re-run to verify.
5. **If you believe a failure is unrelated to the PR's changes**, you MUST follow the Test Failure Triage Protocol in `.claude/skills/shared/test-failure-triage.md` before dismissing it.

## Step 6: Commit and push

1. Stage and commit the fix with a descriptive message referencing the failure (stage specific files rather than `git add -A` if untracked scratch files exist):
   ```
   git add <files>
   git commit -m "fix: <description of what was fixed>

   Co-Authored-By: <your session's model> <noreply@anthropic.com>"
   ```
   Use your own session's attribution trailer — see `.claude/skills/shared/commit-attribution.md`.
2. Verify you are still on the PR's head branch: `git branch --show-current` must match the PR's `headRefName`. If it does not, STOP and report an error.
3. Push to the PR branch (using `HEAD` to push exactly the verified branch):
   ```
   git push upstream HEAD
   ```

## Step 7: Post a summary comment on the PR

Post a concise comment on the PR describing the fix:

```
env -u GH_TOKEN gh pr comment <pr-number> --repo MrsBohemian/Cholobots --body "$(cat <<'EOF'
## Build Fix

**Run:** [<run-url>](<run-url>)   (or "local reproduction; no CI configured yet (#35)")
**Failed step:** <step-name>

### What Failed

[1-3 sentences. Name the specific test/compile/lint error.]

### Fix Applied

[1-3 sentences. What was changed and why.]

### Files Changed

- `path/to/file.py` — description of change

---
cc @bigjonroberts
EOF
)"
```

## Step 8: Report

Tell the user:
- Which build (or local reproduction) you analyzed and what failed
- The root cause (one sentence)
- What fix was applied and pushed
- Link to the PR
