---
name: validate-test-plan
description: Validate a story's test plan against the testing strategy
argument-hint: "<story-ref>"
allowed-tools:
  - Bash(scripts/taiga-story.sh *)
  - mcp__taiga-py__get_story
  - mcp__taiga-py__update_story
  - mcp__taiga-py__get_issue
  - mcp__taiga-py__update_issue
  - mcp__taiga-py__add_comment
  - Bash(scripts/taiga-comment.sh *)
  - Read
  - Grep
  - Glob
---

# Validate Test Plan — Story #$ARGUMENTS

Validate a story's test plan section against the project testing strategy (`docs/testing-strategy.md`). This skill checks that planned tests are in the correct suites, respect ownership rules, avoid redundancy, and follow cross-cutting conventions.

## Step 1: Load the testing strategy

Read `docs/testing-strategy.md` completely, plus `docs/testing-conventions.md` and `docs/testing-workflow.md`. Those documents are the single source of truth for suite definitions, ownership rules, and conventions. If anything in this skill ever conflicts with them, the documents win.

From the strategy, extract and internalize at least:

- The **suite definitions** and what belongs at each level:
  - `tests/unit/` for pure helpers (parsers, money and time functions, formatting, identity mapping), story #34
  - `tests/integration/` for Supabase round trips and shared-table contracts against the local stack (stories #47, #51), with discord.py test doubles and a faked Supabase client
  - `tests/scripts/` for the helper scripts under `scripts/`
  - `docs/manual-tests/` for flows that need a live bot in the test Discord server (story #36) and the static dashboards
- The **business rule ownership** rules: each business rule has exactly one authoritative test level, with at most a single contract check at other levels.
- The **cross-cutting rules** for duplication, deterministic time (fixed or injected clock, `America/Chicago` for timezone-sensitive behavior), production logic reimplementation, and clean test output.
- The **handler testing guidance**: how a command handler is driven with discord.py test doubles, how Supabase is faked, and when a handler is manual-test only.

If `tests/` does not exist yet, a plan that lists automated tests is still validated against the intended layout; note in the report that the suites are not in place yet.

## Step 2: Fetch the story

1. Fetch the story: `get_story(project=39, ref=$ARGUMENTS)`. Read the `description` field and the story's current `tags` and `status`. (`scripts/taiga-story.sh $ARGUMENTS 39` returns the same JSON plus the internal `id`, which the REST fallback `scripts/taiga-comment.sh` needs.)
2. If no story has that ref, try `get_issue(project=39, ref=$ARGUMENTS)` and treat it as a bug with `entity_type="issue"` in the calls below.

## Step 3: Extract the test plan

Find the `### Tests` section in the implementation plan (within the story description). Parse out:

- Which test suites are planned (by file path or suite name)
- What test cases are listed
- What each test verifies

If the story has no `### Tests` section, check for a `## Tests` section or test information embedded in task descriptions. If no test plan exists at all, report that and stop.

If the test plan says "Not applicable" (documentation-only or config-only story), verify this claim by checking the `### Architecture` section for any source code changes (`cholobots.py`, `config.py`, `db/`, `bots/`, `scripts/`). If the claim is valid, report compliance and stop. If source code IS changing, flag this as an ERROR.

## Step 4: Validate each planned test against the strategy

For each planned test case, check:

### 4a. Suite placement

Is the test planned for the correct suite per the suite definitions?

Common misplacements to catch:
- A pure helper tested through a Discord command handler (belongs in `tests/unit/` against the helper)
- A Supabase query or shared-table contract (for example `crudo_labor_entries` rows written by metichebot and summed by crudobot) tested in `tests/unit/` with mocks instead of in `tests/integration/`
- A flow that needs a live bot (multi-step `bot.wait_for` prompts, attachment uploads, reactions) planned as an automated test instead of a `docs/manual-tests/` script
- A script's behavior tested from `tests/unit/` instead of `tests/scripts/`

### 4b. Ownership compliance

Does the plan respect the business rule ownership rules?

- The authoritative level should have full coverage (multiple edge cases, parametrized or property-based tests where appropriate)
- Other levels should have at most one contract check per rule
- Flag any plan that puts exhaustive edge-case testing at a non-authoritative level

### 4c. Redundancy check

Are any test cases duplicating coverage that already exists?

1. For each planned test, use Grep to search for existing tests of the same function or behavior across `tests/` and `docs/manual-tests/`
2. If existing coverage is found at the authoritative level, flag planned duplicates at other levels (unless they qualify as intentional duplication per the strategy)

### 4d. Handler test quality

If handler or reply tests are planned:
- Do they assert on the data the reply carries (amounts, names, routing) rather than whole Spanglish persona strings?
- Do they use discord.py test doubles for context, author, channel and `bot.wait_for`, and a faked Supabase client rather than mock chains?
- Do they say how module-level session dicts are reset between tests?
- Flag any plan that imports `cholobots.py` (starts the bot) or relies on real Discord, OpenAI or Supabase credentials

### 4e. Integration and manual-test setup

If integration tests are planned:
- Do they target the local Supabase stack, with a guard that the URL is local (the service-role key bypasses RLS)?
- Do they seed preconditions through helpers or fixtures instead of driving full command flows when setup is not the behavior under test?

If manual test scripts are planned:
- Do they name prerequisites, exact commands, expected replies, and cleanup of created rows?

### 4f. Deterministic time

If any planned test involves time-dependent behavior (pings, wakeups, daily tasks, time blocks, `America/Chicago` boundaries, naive vs UTC timestamps):
- Does the plan mention a fixed or injected clock per `docs/testing-conventions.md`?
- Flag any plan that doesn't specify deterministic time for time-dependent tests

### 4g. Manual-test necessity and TDD

- For each bug fix in the plan: is there a failing-test-first step for testable code (pure helpers, parsers, money and time functions)? If the code is a handler that needs a live bot, the plan must contain manual test steps instead (WARNING if missing, ERROR if the plan has neither).
- **ERROR** if the plan puts a live-bot flow in `tests/` with no way to run it without Discord.
- Suggested correction: push the logic into a pure helper covered at `tests/unit/`, and keep a thin manual script for the handler.

## Step 5: Report findings

For each issue found, report:

1. **What is wrong** -- describe the specific problem
2. **Which rule it violates** -- cite the relevant part of `docs/testing-strategy.md` or `docs/testing-conventions.md` (for example, the suite definition for `tests/unit/`, or the business rule ownership rule)
3. **Suggested correction** -- concrete action (for example, "Move this test from `tests/integration/` to `tests/unit/` by testing `parse_amount` directly", "Remove redundant edge cases from the handler test -- the helper's unit tests already cover them")
4. **Severity**:
   - **ERROR**: Wrong suite placement, ownership violation, production logic reimplementation, missing tests for changing source code, live-bot flow planned as an automated test
   - **WARNING**: Potential redundancy, missing clock mention, persona-copy assertions, missing cleanup in a manual script

## Step 6: Update the story

Use `entity_type="story"` (or `"issue"` for a bug) in the comment calls.

### If errors were found:

1. Update the story description's `### Tests` section with the corrected test plan. Preserve the rest of the description. Use `update_story(project=39, ref=$ARGUMENTS, description="<updated description>")` (or `update_issue` for a bug).
2. Add a Taiga comment explaining the changes:
   ```
   add_comment(project=39, ref=$ARGUMENTS, entity_type="story", text="Test plan review: [summary of changes]. Rules cited: [list of strategy sections referenced].")
   ```

### If only warnings were found:

1. Add a Taiga comment with the warnings (do not modify the description):
   ```
   add_comment(project=39, ref=$ARGUMENTS, entity_type="story", text="Test plan review: plan is acceptable with minor notes. [list warnings]. No changes made to description.")
   ```

### If no issues were found:

1. Add a Taiga comment confirming compliance:
   ```
   add_comment(project=39, ref=$ARGUMENTS, entity_type="story", text="Test plan review: plan complies with testing strategy. No changes needed.")
   ```

## Step 7: Return

Report the findings summary to the caller:
- Number of errors and warnings found
- Whether the story description was updated
- Brief list of the most significant findings (if any)
