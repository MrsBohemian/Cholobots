---
name: research-spike
description: Execute research spike issues systematically, running defined test methodologies and producing deliverables
argument-hint: "<spike-issue-ref>"
allowed-tools:
  - mcp__taiga-py__get_issue
  - mcp__taiga-py__get_story
  - mcp__taiga-py__update_issue
  - mcp__taiga-py__add_comment
  - mcp__taiga-py__search
  - mcp__taiga-py__get_wiki
  - mcp__taiga-py__upsert_wiki
  - Read
  - Write
  - Edit
  - Grep
  - Glob
  - TodoWrite
  - Bash(git *)
  - Bash(env -u GH_TOKEN gh *)
  - Bash(python *)
  - Bash(python3 *)
  - Bash(ls *)
  - Bash(pwd)
  - Bash(mkdir *)
  - Bash(rm *)
  - Bash(chmod *)
  - Bash(echo *)
  - Bash(timeout *)
  - Bash(cat *)
  - Bash(date)
  - Bash(whoami)
  - Agent
---

# Research Spike — $ARGUMENTS

You are executing a research spike issue in Taiga project 39 (`cholo-bots`). Follow these steps
systematically to ensure all test methodologies are executed and deliverables are produced.

## Step 1: Parse spike issue

1. Fetch the spike: `get_issue(project=39, ref=$ARGUMENTS)` (fall back to
   `get_story(project=39, ref=$ARGUMENTS)` if the spike was filed as a story).
2. Extract from the description:
   - Goal
   - Testing Methodology (test files, commands, pass/fail criteria)
   - Test Matrix dimensions
   - Deliverables list
   - Time box
3. Choose the deliverable slug: `spike-<ref>-<kebab-cased short goal>` (e.g.
   `spike-37-tenant-model`). It names both the wiki page and the repo file.
4. Create a TodoWrite list with major phases from the testing methodology

## Step 2: Set up test environment

1. Create a working directory: `mkdir -p /tmp/spike-$ARGUMENTS`
2. Work there with absolute paths (`/tmp/spike-$ARGUMENTS/...`); the shell's working directory
   resets between calls.
3. Create any test files specified in the methodology (scratch scripts, sample payloads, fixture
   rows). Never point an experiment at production Supabase, Discord or OpenAI resources unless the
   spike explicitly authorizes it; use the local Supabase stack (story #47) or the test Discord
   server (story #31) when they exist, and record which one you used.
4. Initialize results file: `echo "# Research Spike Results - Issue #$ARGUMENTS" > /tmp/spike-$ARGUMENTS/results.md`
5. Add the results recording template to results.md

## Step 3: Execute test phases

For each test phase in the methodology:

1. Mark the phase as "in_progress" in TodoWrite
2. Set up the specific test environment as documented
3. Execute each test in the phase:
   - Run the exact commands specified
   - Capture full output (stdout and stderr)
   - Apply pass/fail criteria
   - Record results in the table format
4. After each test, append results to results.md
5. Mark the phase as "completed" in TodoWrite

### Critical: Variable isolation

- Change ONLY ONE variable at a time between tests
- Reset environment between tests when needed (cleanup commands)
- Document any deviations from the test plan

## Step 4: Analyze results

1. Review the completed test matrix
2. Identify patterns:
   - Which combinations consistently pass/fail?
   - What environmental factors correlate with failures?
   - Are there version-specific issues (Python, discord.py, supabase-py, openai)?
3. Write analysis section in results.md

## Step 5: Produce deliverables

For each deliverable listed in the spike issue:

1. **Results matrix**: Format the results table properly with markdown
2. **Root cause hypothesis / decision**: Based on test results, write a clear hypothesis or
   recommendation
3. **Recommended approach or workaround**: Document the configuration or design that works
4. **Updated related items**: `add_comment(project=39, ref=<ref>, text=..., entity_type="story"|"issue")`
   on each related story or issue with the findings that affect it
5. **External bug reports**: If specified, prepare comment text (but don't post)

Then publish the write-up in both places:

- **Taiga wiki**: `upsert_wiki(project=39, slug=<deliverable slug>, content=<full write-up>)`.
  Check first with `get_wiki(project=39, slug=<deliverable slug>)`; if a page already exists from
  an earlier run, update it rather than creating a variant slug.
- **Repo**: write the same content to `docs/research/<deliverable slug>.md` (create
  `docs/research/` if missing). Do not commit unless the spike says to; report the path so the
  caller can include it in a PR.

The write-up is current-state: goal, method, results matrix, analysis, recommendation, open
questions. Never link a local `/tmp` path from it.

## Step 6: Finalize spike

1. Add summary to results.md with:
   - Total tests executed
   - Pass/fail/error counts
   - Key findings
   - Recommended next steps
2. Update the spike issue:
   - Add a comment containing the results summary and matrix, plus the wiki page slug and
     `docs/research/<deliverable slug>.md` (paste the summary directly — never link to a local
     `/tmp` path)
   - Set its status per [`../shared/taiga-project.md`](../shared/taiga-project.md): `Closed` when
     every deliverable landed, otherwise leave it `In progress` and list what is missing
   - Tag with `spike-complete` if not already tagged (read-modify-write the tag list)
3. If within time box, document any incomplete items

## Step 7: Report completion

Output a summary including:
- Number of tests executed
- Key findings (2-3 bullets)
- Location of full results (wiki slug and `docs/research/` path)
- Any blockers encountered
- Recommended next steps

## Error handling

- If a test might hang, prefix it with `timeout <seconds> <cmd>` to enforce a time limit
- If environment setup fails, document and skip dependent tests
- If spike exceeds time box, document progress and recommendations
- Never run `python cholobots.py` from a spike: importing or running it connects the live bot

## Notes

- Maintain scientific rigor: one variable at a time
- Document everything, even "boring" passes
- Use exact commands from the methodology
- Preserve all output, even if verbose
- Follow the specified results template format exactly
