---
name: review-tests
description: Review test code changes against the testing strategy — fix placement, remove redundancy, improve quality
argument-hint: "<branch-name-or-pr-number>"
allowed-tools:
  - Bash(git *)
  - Bash(env -u GH_TOKEN gh *)
  - Bash(python -m py_compile *)
  - Bash(python -m pytest *)
  - Bash(ruff *)
  - Bash(scripts/taiga-comment.sh *)
  - mcp__taiga-py__get_story
  - mcp__taiga-py__get_issue
  - mcp__taiga-py__add_comment
  - Read
  - Edit
  - Write
  - Grep
  - Glob
---

# Review Tests — $ARGUMENTS

Review test code changes on a branch or PR against the project testing strategy. Identify and fix test placement errors, redundancy, and quality anti-patterns. Document every change with WHY.

All GitHub calls target `MrsBohemian/Cholobots` and are prefixed `env -u GH_TOKEN`. Never run the bot (`python cholobots.py`).

## Step 1: Load the testing strategy

Read `docs/testing-strategy.md` completely. This is your authority for all decisions. In outline it defines:

- `tests/unit/` — pure helpers (parsers, money and time functions, name and identity mapping, formatting), no network, no Discord, no Supabase (story #34)
- `tests/integration/` — queries and shared-table contracts run against the local Supabase stack, with discord.py test doubles for the Discord side and a faked Supabase client where the real stack is not the subject (stories #47, #51)
- `tests/scripts/` — tests of the helper scripts under `scripts/` (pytest and `test-*.sh`)
- `docs/manual-tests/` — manual test scripts for flows that need a live bot in the test Discord server (story #36) and for the static dashboards

Also read `docs/testing-conventions.md` for conventions (clock handling, fixtures, naming, file layout) and `docs/testing-workflow.md` for how and when to run each suite. If this skill ever conflicts with those documents, the documents win.

If `tests/` does not exist yet on this branch, say so: the compile check (`python -m py_compile cholobots.py config.py db/database.py bots/*.py`) is the only automated floor, and the review below applies to any `tests/` or `docs/manual-tests/` files the branch adds.

## Step 2: Identify test changes

1. Determine the comparison base:
   - If argument looks like a PR number (digits only): `env -u GH_TOKEN gh pr view <number> --repo MrsBohemian/Cholobots --json headRefName,baseRefName`, then `git fetch upstream <headRefName> <baseRefName>` and diff `upstream/<baseRefName>...upstream/<headRefName>`
   - If argument is a branch name: `git fetch upstream` and diff `upstream/main...<branch>`
2. List all changed test files:
   ```bash
   git diff --name-only upstream/main...<branch> -- 'tests/**' 'docs/manual-tests/**'
   ```
3. Also list all changed production source files:
   ```bash
   git diff --name-only upstream/main...<branch> -- cholobots.py config.py 'db/**/*.py' 'bots/**/*.py' 'scripts/**' ':!scripts/__pycache__'
   ```
4. If no test files changed:
   - If production source files also changed, report an ERROR: "Production code changed on this branch with no corresponding test changes. Tests are required for feature and bugfix changes. For Discord command handlers that need a live bot, a `docs/manual-tests/` entry is required instead." Name the changed files and which of the two applies to each.
   - If no production source files changed either (config-only or docs-only PR), report "No test changes to review" and return.

## Step 3: Review each changed test file

For each changed test file, read the full file and check:

### 3a. Suite placement

- Determine which suite this file is in (from its directory path)
- Read the test cases and determine what they are testing
- Cross-reference against the suite definitions in `docs/testing-strategy.md`:
  - Pure helper logic (no I/O) -> `tests/unit/`
  - Anything that needs a Supabase round trip, a shared-table contract between bots, or a bot handler driven through discord.py test doubles -> `tests/integration/`
  - Behavior of a file under `scripts/` -> `tests/scripts/`
  - A flow that only a human in the test Discord server can exercise -> `docs/manual-tests/`
- If a test is in the wrong suite, flag it for movement

### 3b. Redundancy

- For each test case, check if the same behavior is already tested at the authoritative level:
  - Use Grep to search for existing tests of the same function or command in other test directories
  - If the authoritative level already has coverage, this test should be at most a contract check (one happy path and one sad path)
  - If this IS the authoritative level, full coverage is appropriate
- Flag tests that duplicate coverage beyond what the strategy allows

### 3c. Quality anti-patterns

Check for these specific anti-patterns:

- **Importing a bot entry module at test time**: `import cholobots` starts the bot (module-level `bot.run()`), and `config.py` / chismebot / guardabot / crudobot exit at import without `DISCORD_TOKEN`, `OPENAI_API_KEY`, `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`. Tests must import only the pure helper (or the module under a stubbed environment and faked client), never `cholobots.py`.
- **Real external calls in unit tests**: any network, OpenAI, Discord or Supabase call from `tests/unit/`. Use a stub or the faked Supabase client.
- **Mock-chain assertions**: `assert_called_with` on a `MagicMock` chain such as `.table().select().eq().execute()`. Prefer the faked Supabase client (in-memory tables) and assert on resulting rows or return values.
- **Production logic reimplementation**: test helpers that mirror the production function's branches (timezone conversion, rounding, scheduling math). Tests should call the production function with specific inputs and assert specific outputs.
- **Wall-clock time**: `datetime.now()`, `datetime.utcnow()`, `date.today()`, `time.time()` in test code or in a test's setup. Use a fixed or injected clock per `docs/testing-conventions.md`, and pin timezone-sensitive tests to `America/Chicago`. Exception: tests whose subject is the clock helper itself.
- **Persona copy assertions**: asserting a whole Spanglish reply string or embed text that is hand-written flavor. Assert on the data the reply carries (amounts, names, ids, command routing) or a stable keyword.
- **Leaked in-memory state**: chismebot's `*_sessions`, metichebot's `active_time_sessions` and crudobot's receipt sessions are module-level dicts keyed by author or channel. A test that fills one must reset it (fixture), or later tests pass or fail by order.
- **Unbounded waits and sleeps**: `time.sleep`, `asyncio.sleep` used to wait for something, or a `bot.wait_for` double with no timeout. Drive the coroutine directly or advance the fake clock.
- **Integration tests aimed at a real project**: any `tests/integration/` fixture that reads `SUPABASE_URL` / `SUPABASE_SERVICE_ROLE_KEY` from the normal environment without asserting it points at the local stack. The service-role key bypasses Row Level Security; a wrong URL means writes to production.
- **Manual test scripts without steps or cleanup**: a `docs/manual-tests/` file that lacks prerequisites, exact commands to type, expected replies, and how to undo the data it creates.

## Step 4: Fix issues

For each issue found:

### Suite movement

1. Move the test file to the correct directory (`git mv`)
2. Fix imports and any fixture dependencies (shared fakes and fixtures live where `docs/testing-conventions.md` says; move them if both suites need them)
3. If moving from `tests/integration/` to `tests/unit/`, remove the Supabase dependency by passing in the faked client or the plain data; if that is not possible, the test is not a unit test and should stay
4. Verify the test still passes

### Redundancy removal

1. Delete redundant test cases (keep at most one contract check at non-authoritative levels)
2. If an entire file becomes empty after removal, delete the file

### Quality fixes

1. Replace wall-clock calls with the fixed or injected clock
2. Replace mock-chain assertions with the faked Supabase client
3. Replace production logic reimplementation with direct calls to production functions
4. Replace persona-copy assertions with assertions on the carried data
5. Add the missing reset fixture or timeout/guard

## Step 5: Verify changes

Run `python -m py_compile cholobots.py config.py db/database.py bots/*.py`, then the affected suites per [docs/testing-workflow.md](../../../docs/testing-workflow.md): `python -m pytest -q <path>` for each touched directory (`tests/integration` only if the local Supabase stack is running; otherwise record that it was not run and why). If ruff is configured, run `ruff check .` on the touched files. Run each as a foreground Bash call with the tool's `timeout: 600000` parameter set explicitly, wrapped in a shell `timeout 590` so it ends inside the call. Never use `run_in_background`, `Monitor`, or end your turn waiting for a task notification — this runs headless, so there is no next turn and the run dies with you. On exit 124, run a narrower target instead of a longer timeout. If any test fails due to the review changes, fix it before proceeding; for failures you think are unrelated, follow `.claude/skills/shared/test-failure-triage.md`.

## Step 6: Commit and document

1. Commit all changes (if there are any):
   ```bash
   git commit -am "$(cat <<'EOF'
   test: review and fix test quality issues

   [List each change and why, citing docs/testing-strategy.md or docs/testing-conventions.md]

   Co-Authored-By: <your session's model> <noreply@anthropic.com>
   EOF
   )"
   ```
   Use your own session's attribution trailer — see `.claude/skills/shared/commit-attribution.md`. Verify `git branch --show-current` is the PR's head branch, then `git push upstream HEAD`.

2. If a PR number was provided, comment on the PR:
   ```bash
   env -u GH_TOKEN gh pr comment <number> --repo MrsBohemian/Cholobots --body "$(cat <<'EOF'
   ## Test Quality Review

   [For each change:]
   - **[File]**: [What was changed] -- [Why, citing the specific rule from the testing docs]

   [If no issues found: "All test changes comply with the testing strategy."]
   EOF
   )"
   ```

3. If a Taiga story ref is available (from the PR description or a branch name like `feature/<ref>-<slug>`), comment on it:
   ```
   add_comment(project=39, ref=<ref>, entity_type="story", text="Test quality review: [summary]")
   ```
   (Verify the ref is a story with `get_story(project=39, ref=<ref>)` first; for a bug ref use `entity_type="issue"`. `scripts/taiga-comment.sh <id> "<text>" [--issue]` is the REST fallback; it takes the internal `id` from `scripts/taiga-story.sh <ref> 39`.)

## Step 7: Return

Report:
- Number of issues found and fixed
- Summary of changes (movements, deletions, rewrites)
- Test suite results after fixes (or "no test suite yet (#34)")
- Whether any issues could not be auto-fixed (explain why)
