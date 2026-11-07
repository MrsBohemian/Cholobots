---
name: fully-analyze-bugs
description: Autonomously analyze all open unanalyzed bugs in parallel — investigation, root cause, implementation plans, Taiga updates
argument-hint: "[concurrency] [filter flags] [--yes]"
allowed-tools:
  - Agent
  - Bash(scripts/taiga-story.sh *)
  - Bash(python3 scripts/taiga_select_issues.py *)
  - Bash(python3 scripts/taiga_project_setup.py --check)
  - mcp__taiga-py__*
  - Read(//tmp/**)
  - Edit(//tmp/**)
  - Read
  - Grep
  - Glob
  - AskUserQuestion
---

# Fully Analyze Bugs

Autonomously analyze all open unanalyzed bugs in Taiga project 39 (`cholo-bots`): parallel analysis with agents, question queue for missing information, and Taiga updates. Analysis is read-only for the repo (it reads code, queries Taiga and writes the plan to the issue), so the agents share the main checkout and need no worktrees. Taiga names are in `.claude/skills/shared/taiga-project.md`; every Taiga call is ref-based with `project=39`.

## Step 0: Preflight and parse arguments

Run `python3 scripts/taiga_project_setup.py --check` once. Exit 0: proceed. Exit 2: stop and print `Run: (eval "$(dv export project/taiga-growthgarage-jonroberts)" && python3 scripts/taiga_project_setup.py --apply)` for Jon (the project is missing statuses, the `Urgent` priority or pipeline tags this skill relies on).

If the first whitespace-separated token of `$ARGUMENTS` is an integer, use it as the concurrency level (max parallel agents). Numbers elsewhere (for example `--refs 123` or `--milestone 12`) are filter values, never concurrency. Default: 3.

Parse the remaining arguments per [`../shared/bug-sweep-options.md`](../shared/bug-sweep-options.md).

## Step 1: Initialize

### 1a. Check for existing state file

Check if `/tmp/fully-analyze-bugs.json` exists:
- If it exists and `phase` is not `"complete"`:
  - Read the state file
  - Report current state (how many bugs total, how many analyzed, how many need clarification)
  - Resume from the appropriate phase, reusing `options` from the state file; do not re-run the selector or re-prompt (see the include)
- If it doesn't exist or `phase` is `"complete"`, proceed with fresh initialization

### 1b. Select the bugs

Run `python3 scripts/taiga_select_issues.py --purpose analyze <flags>`, then confirm the selection per the include. `Urgent` bugs stay in the batch for analysis, so `urgent` is always empty for this purpose.

If `selected` is empty, report that and stop.

### 1c. Initialize state file

Write the initial state to `/tmp/fully-analyze-bugs.json`:

```json
{
  "phase": "analyzing",
  "concurrency": <N>,
  "options": {...},
  "rawFlags": "...",
  "bugs": [
    {
      "ref": <ref>,
      "id": <issue_id>,
      "subject": "<subject>",
      "status": "pending",
      "question": null,
      "answer": null,
      "failureReason": null
    }
  ]
}
```

Include ALL `selected` bugs in the array, in `selected` order.

## Step 2: Analysis loop

Process bugs in parallel batches of up to `concurrency` agents at a time.

### 2a. Select next batch

Collect up to `concurrency` bugs with status `"pending"`, `"re-queued"`, or `"analyzing"` (stuck from a previous interrupted run). If none remain, go to Step 3.

### 2b. Launch agents in parallel

For each bug in the batch, spawn an agent **in parallel** (all in a single message with multiple Agent tool calls):

- **Agent type**: senior-manual-testing-specialist (has analyze-bug skill preloaded)
- **Prompt**:
  ```
  Analyze bug #<ref> (<subject>) in Taiga project 39 (cholo-bots).

  Follow the /analyze-bug skill instructions with argument '<ref>'.

  CRITICAL — Taiga lookup: The bug ref is <ref>. In Step 1 of the analyze-bug skill, use `get_issue(project=39, ref=<ref>)` to fetch the full issue details directly. Do NOT use `search` to find it. Never run `python cholobots.py`; analysis reads code and Taiga only.

  IMPORTANT MODIFICATION: You are running as an unattended agent. If Step 2 of the analyze-bug skill requires you to ask the user for clarification (because the bug description is missing expected behavior, actual behavior, or reproduction steps), do NOT wait for an answer. Instead, STOP immediately and return your response in this exact format:

  NEEDS_CLARIFICATION
  QUESTION: <your question(s) for the operator>

  If the bug description is clear enough to proceed, complete the full analysis through Step 7a or 7b, then return one of:

  ANALYSIS_COMPLETE
  <brief summary of root cause and plan>

  ANALYSIS_REJECTED
  REASON: <why the bug is already fixed or invalid — include commit/PR if applicable>

  If you encounter an error that prevents analysis, return:

  ANALYSIS_FAILED
  REASON: <what went wrong>
  ```
  If the bug has a non-null `answer` field (from a previous clarification round), append to the prompt:
  ```

  The operator previously answered your clarification question. Their answer:
  <answer>

  Use this information and proceed with the analysis — do not ask for clarification again.
  ```

Update each bug's `status` to `"analyzing"` in the state file before launching.

### 2c. Process results

When all agents in the batch return, process each result:

- **If output contains `ANALYSIS_COMPLETE`**: set bug `status` to `"complete"`
- **If output contains `ANALYSIS_REJECTED`**: set bug `status` to `"rejected"`, store reason in `failureReason`
- **If output contains `NEEDS_CLARIFICATION`**: set bug `status` to `"needs-clarification"`, extract the question and store in `question` field
- **If output contains `ANALYSIS_FAILED`** or agent errors: set bug `status` to `"failed"`, store reason in `failureReason`
- **If output doesn't match any pattern**: treat as `"failed"` with reason `"unexpected agent output"`

Update the state file after processing the batch.

### 2d. Continue or proceed to questions

If there are more `"pending"` bugs, go back to 2a for the next batch. Otherwise, proceed to Step 3.

## Step 3: Question queue

Check if any bugs have status `"needs-clarification"`.

If none, skip to Step 4.

If there are questions, present them ALL to the operator at once in a numbered list:

```
The following bugs need clarification before analysis can continue:

1. **Bug #<ref>**: <subject>
   Question: <question>

2. **Bug #<ref>**: <subject>
   Question: <question>

...

For each bug, you can:
- Answer the question (I'll re-run analysis with your answer)
- Skip it (it will remain unanalyzed)

Please respond with answers by number, or "skip <number>" for any you want to skip.
You can also say "skip all" to finish without analyzing these.
```

For each answer the operator provides:
- Store the answer in the bug's `answer` field
- Set bug `status` to `"re-queued"`

For skipped bugs, leave status as `"needs-clarification"`.

Update the state file, then go back to Step 2 to process re-queued bugs.

## Step 4: Complete

1. Update state file: `phase = "complete"`
2. Report summary:
   - The options used (from the state file's `options`)
   - Total bugs processed
   - Bugs analyzed successfully (refs and subjects)
   - Bugs rejected as invalid or already fixed (refs, subjects, and reasons)
   - Bugs needing clarification (still unresolved, with their questions)
   - Bugs that failed (with reasons)
   - Remind: analyzed bugs now have the `llm-analyzed` tag and are ready for `/fully-fix-bugs`

## Error handling

- If an agent spawn fails, set the bug to `status = "failed"`, `failureReason = "agent spawn failed"`, and continue with the remaining bugs.
- The state file at `/tmp/fully-analyze-bugs.json` enables resumability — if interrupted, re-invoke to resume.
- **Never stop the entire run for a single bug failure** — mark it as failed and continue.
