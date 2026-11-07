---
name: fully-analyze-stories
description: Autonomously analyze all stories in an epic in parallel - investigation, implementation plans, Taiga updates
argument-hint: "<epic-ref> [concurrency]"
allowed-tools:
  - Agent
  - Bash(scripts/taiga-story.sh *)
  - Bash(python3 scripts/taiga_project_setup.py --check)
  - mcp__taiga-py__*
  - Read(//tmp/**)
  - Edit(//tmp/**)
  - Read
  - Grep
  - Glob
  - AskUserQuestion
---

# Fully Analyze Stories

Autonomously analyze all stories in a Taiga epic (project 39, `cholo-bots`): parallel analysis with agents, question queue for missing information, and Taiga updates. Analysis only reads code and writes to Taiga (plus the design-doc Open Questions edit noted below), so the agents share the main checkout and need no worktrees. Taiga names are in `.claude/skills/shared/taiga-project.md`; every Taiga call is ref-based with `project=39`. Codebase background for the agents is the root `CLAUDE.md`.

## Step 0: Preflight and parse arguments

Run `python3 scripts/taiga_project_setup.py --check` once. Exit 0: proceed. Exit 2: stop and print `Run: (eval "$(dv export project/taiga-growthgarage-jonroberts)" && python3 scripts/taiga_project_setup.py --apply)` for Jon (the project is missing statuses, the `Urgent` priority or pipeline tags this skill relies on).

Split `$ARGUMENTS` into two parts:

1. `<epic-ref>` (required): the epic reference number (for example `46` for developer experience)
2. `[N]` (optional): concurrency level (max parallel agents), default: 3

If no epic-ref is provided, ask the user for it.

## Step 1: Initialize

### 1a. Check for existing state file

Check if `/tmp/fully-analyze-stories-<epic-ref>.json` exists:
- If it exists and `phase` is not `"complete"`:
  - Read the state file
  - Report current state (how many stories total, how many analyzed, how many need clarification)
  - Resume from the appropriate phase
- If it doesn't exist or `phase` is `"complete"`, proceed with fresh initialization

### 1b. Fetch epic and stories

1. `get_epic_overview(project=39, ref=<epic-ref>)`: returns the epic (with its description) and every story linked to it, with per-status counts. Note the epic `subject`.
2. **Confirm with the user:** "Epic #<epic-ref>: <subject>. Is this the right epic?" and wait for confirmation before proceeding.
3. If the overview rows do not include each story's tags, fetch them with `get_story(project=39, ref=<story ref>)`.
4. Keep only **analysis-eligible** stories: status `Ready for Analysis`, or status `New`, and in both cases **without** the `llm-analyzed` tag. Stories at `Ready` or later, closed stories (`Done`, `Archived`) and `Blocked` stories are left alone.
5. If there are none, report that (with the status counts from the overview) and stop.

### 1c. Initialize state file

Write the initial state to `/tmp/fully-analyze-stories-<epic-ref>.json`:

```json
{
  "phase": "analyzing",
  "epicRef": <epic-ref>,
  "concurrency": <N>,
  "stories": [
    {
      "ref": <ref>,
      "id": <story_id>,
      "subject": "<subject>",
      "status": "pending",
      "question": null,
      "answer": null,
      "failureReason": null,
      "tagFailure": false,
      "designDocEdited": null
    }
  ]
}
```

Include ALL eligible stories in the array, ordered by ref (ascending). (`id` is Taiga's internal id, informational only; every call uses `ref`.)

## Step 2: Analysis loop

Process stories in parallel batches of up to `concurrency` agents at a time.

### 2a. Select next batch

Collect up to `concurrency` stories with status `"pending"`, `"re-queued"`, or `"analyzing"` (stuck from a previous interrupted run). If none remain, go to Step 3.

### 2b. Launch agents in parallel

For each story in the batch, spawn an agent **in parallel** (all in a single message with multiple Agent tool calls):

- **Agent type**: senior-software-developer (has analyze-story skill preloaded)
- **Prompt**:
  ```
  Analyze story #<ref> (<subject>) in Taiga project 39 (cholo-bots).

  Follow the /analyze-story skill instructions with argument '<ref>'.

  CRITICAL - Taiga lookup: In Step 1, follow the /analyze-story Step 1 exactly: use `scripts/taiga-story.sh <ref> 39 --with-tasks` to load the story and its current tasks, then call `get_story(project=39, ref=<ref>)` to fetch the full story description. Do NOT use `search` to find it.

  You are running unattended. Never run `python cholobots.py`; analysis reads code and Taiga only.

  IMPORTANT MODIFICATIONS for unattended mode:

  1. SKIP Step 3 (epic assignment) and Step 3a (dependency diagram) entirely - the story is already linked to its epic.

  2. At any point during the /analyze-story workflow where you would normally ask the operator for clarification or confirmation - including but not limited to: Step 2 (story completeness), design-doc "Open Questions", or any later blocking question - do NOT ask interactively. Instead STOP immediately and return:
     NEEDS_CLARIFICATION
     QUESTION(S):
     - <question 1>
     - <question 2, if any>

  3. If the story is a duplicate, out of scope, or otherwise not analyzable, STOP and return:
     ANALYSIS_REJECTED
     REASON: <why this story cannot be analyzed as-is>

  4. If you complete the full analysis through Step 10, return:
     ANALYSIS_COMPLETE
     <brief summary of the plan and number of tasks created>
     If Step 4c edited the design doc's Open Questions section, also add a line:
     DESIGN_DOC_EDITED: docs/designs/story-<ref>.md

  5. If you encounter an error, or the skill ends with `BLOCKED: <reason>`, return:
     ANALYSIS_FAILED
     REASON: <what went wrong>
  ```

  If the story has a non-null `answer` field (from a previous clarification round), append to the prompt:
  ```

  The operator previously answered your clarification question. Their answer:
  <answer>

  Use this information and proceed with the analysis - do not ask for clarification again.
  ```

  If the story already has tasks (the `--with-tasks` output is non-empty), append to the prompt:
  ```

  The story already has tasks, so Step 8 is a reconcile: produce the disposition table against the existing task list rather than deciding preservation ad hoc. You are running unattended, so the reconcile's "ask the user" branch is unavailable - an ambiguous match must resolve to **update**, or be reported as unresolved, never to **create**.
  ```

Update each story's `status` to `"analyzing"` in the state file before launching.

### 2c. Process results

When all agents in the batch return, process each result:

- **If output contains `ANALYSIS_COMPLETE`**: set story `status` to `"complete"`. If it also contains a `DESIGN_DOC_EDITED: <path>` line, store that path in the story's `designDocEdited` field: this flags an uncommitted working-tree change the operator needs to review.
- **If output contains `ANALYSIS_REJECTED`**: set story `status` to `"rejected"`, store reason in `failureReason`. Then tag the story in Taiga:
    1. Fetch the story to get its current tags: `get_story(project=39, ref=<ref>)`
    2. Apply (the list replaces all tags, so send every existing one): `update_story(project=39, ref=<ref>, tags=[<existing tags...>, "analysis-rejected"])`
    3. If tagging fails (tool error), record `tagFailure = true` in the story's state entry, log a warning, and continue: the story is still marked rejected; tagging is best-effort.
- **If output contains `NEEDS_CLARIFICATION`**: set story `status` to `"needs-clarification"`, extract the question and store in `question` field
- **If output contains `ANALYSIS_FAILED`** or agent errors: set story `status` to `"failed"`, store reason in `failureReason`
- **If output doesn't match any pattern**: treat as `"failed"` with reason `"unexpected agent output"`

Update the state file after processing the batch.

### 2d. Continue or proceed to questions

If there are still stories in an analysis-eligible status (`"pending"`, `"re-queued"`, or `"analyzing"`), go back to 2a for the next batch. Otherwise, proceed to Step 3.

## Step 3: Question queue

Check if any stories have status `"needs-clarification"`.

If none, skip to Step 4.

If there are questions, present them ALL to the operator at once in a numbered list:

```
The following stories need clarification before analysis can continue:

1. **Story #<ref>**: <subject>
   Question: <question>

2. **Story #<ref>**: <subject>
   Question: <question>

...

For each story, you can:
- Answer the question (I'll re-run analysis with your answer)
- Skip it (it will remain unanalyzed)

Please respond with answers by number, or "skip <number>" for any you want to skip.
You can also say "skip all" to finish without analyzing these.
```

For each answer the operator provides:
- Store the answer in the story's `answer` field
- Set story `status` to `"re-queued"`

For skipped stories, leave status as `"needs-clarification"`.

After processing all operator responses:
- If at least one story was marked `"re-queued"`, update the state file and go back to Step 2 to process re-queued stories.
- If no stories were marked `"re-queued"` (for example, the operator said "skip all" or skipped every question), update the state file and proceed directly to Step 4, leaving unresolved stories in `"needs-clarification"`.

## Step 4: Complete

1. Update state file: `phase = "complete"`
2. Report summary:
   - Total stories processed
   - Stories analyzed successfully (refs and subjects)
   - Stories rejected (refs, subjects, reasons, and whether the `analysis-rejected` tag was applied or failed to apply)
   - Stories needing clarification (still unresolved, with their questions)
   - Stories that failed (with reasons)
   - Remind: analyzed stories now have the `llm-analyzed` tag
   - For any story tagged `planning-heavy` or `epic-candidate`, note explicitly that it may still need higher-level planning review rather than immediate implementation
   - Stories with a non-null `designDocEdited`: list the refs and file paths, and note these design docs have uncommitted Open-Questions resolutions in the working tree that still need to be committed

## Error handling

- If an agent spawn fails, set the story to `status = "failed"`, `failureReason = "agent spawn failed"`, and continue with the remaining stories.
- The state file at `/tmp/fully-analyze-stories-<epic-ref>.json` enables resumability: if interrupted, re-invoke to resume.
- **Never stop the entire run for a single story failure**: mark it as failed and continue.
