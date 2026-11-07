---
name: fully-design-stories
description: Sequentially design all design-tagged stories in an epic - interactive Discord interaction design (commands, transcripts, listener ownership, persona voice, dashboard specs) via /design-story
argument-hint: "<epic-ref>"
allowed-tools:
  - Bash(scripts/taiga-story.sh *)
  - Bash(python3 scripts/taiga_project_setup.py --check)
  - mcp__taiga-py__*
  - Read(//tmp/**)
  - Edit(//tmp/**)
  - Read
  - Grep
  - Glob
  - Skill
  - AskUserQuestion
---

# Fully Design Stories

Sequentially design all `design`-tagged stories in a Taiga epic (project 39, `cholo-bots`): fetch eligible stories, then invoke `/design-story` for each one interactively in the main conversation thread. `/design-story` designs Discord command UX (syntax, prompts, embeds, reply flows, listener ownership, Spanglish persona voice) and the static dashboards; there is no React design system or `docs/design-system.md`.

**This skill runs in the main conversation thread.** Each `/design-story` invocation is interactive: the user participates in the scope checks, transcripts and design decisions. Stories are processed one at a time.

**Context window management.** Each `/design-story` run consumes significant context. After completing each story's design, remind the user to restart the conversation (re-invoke `/fully-design-stories <epic-ref>`) to get a fresh context window. The state file enables seamless resumption.

## Step 0: Preflight and parse arguments

Run `python3 scripts/taiga_project_setup.py --check` once. Exit 0: proceed. Exit 2: stop and print `Run: (eval "$(dv export project/taiga-growthgarage-jonroberts)" && python3 scripts/taiga_project_setup.py --apply)` for Jon (the project is missing statuses, the `Urgent` priority or pipeline tags this skill relies on).

`$ARGUMENTS` must contain an `<epic-ref>` (required): the epic reference number. If no epic-ref is provided, ask the user for it.

## Step 1: Initialize

### 1a. Check for existing state file

Check if `/tmp/fully-design-stories-<epic-ref>.json` exists:
- If it exists and `phase` is not `"complete"`:
  - Read the state file
  - Report current state (how many stories total, how many designed, how many skipped, how many remain)
  - Proceed to Step 2 (the loop will pick up the next pending story)
- If it doesn't exist or `phase` is `"complete"`, proceed with fresh initialization

### 1b. Fetch epic and stories

1. `get_epic_overview(project=39, ref=<epic-ref>)`: returns the epic (with its description) and every story linked to it, with per-status counts. Note the epic `subject`.
2. **Confirm with the user:** "Epic #<epic-ref>: <subject>. Is this the right epic?" and wait for confirmation before proceeding.
3. If the overview rows do not include each story's tags, fetch them with `get_story(project=39, ref=<story ref>)`.

### 1c. Filter stories

From the fetched stories, select those that match ALL of:
- **Not closed** (status is not `Done` or `Archived`)
- **Tagged `design`** (the real tag in project 39)
- **Pre-analysis status**: `New` or `Ready for Analysis`

For stories that have the `design-reviewed` tag (already designed), separate them into a "previously designed" list: these are candidates for revisiting but not automatically included.

Note for the operator: `/design-story` itself says that `design` stories which are architecture decisions (error handling, I/O, dispatcher, sessions, clock) go straight to `/analyze-story` unless the decision changes what users see. The user can skip those in Step 2b.

If no eligible stories remain (after filtering), check if there are previously-designed stories. If so, ask the user whether they want to revisit any of them. If not, report that there's nothing to design and stop.

### 1d. Handle previously-designed stories

If any stories already have the `design-reviewed` tag, present them to the user:

```
The following stories in this epic are tagged design but already have design-reviewed:

1. **Story #<ref>**: <subject>
2. **Story #<ref>**: <subject>

Would you like to revisit any of these? (Enter numbers, "all", or "none")
```

Add any the user selects to the eligible list. Mark the rest as `"skipped"` with reason `"already designed, user chose not to revisit"`.

### 1e. Initialize state file

Write the initial state to `/tmp/fully-design-stories-<epic-ref>.json`:

```json
{
  "phase": "designing",
  "epicRef": <epic-ref>,
  "stories": [
    {
      "ref": <ref>,
      "id": <story_id>,
      "subject": "<subject>",
      "status": "pending",
      "previouslyDesigned": <bool>,
      "skipReason": null
    }
  ]
}
```

Include ALL matching stories (both fresh and user-selected revisits), ordered by ref (ascending). Stories the user chose not to revisit should have `status: "skipped"` and a `skipReason`.

## Step 2: Design loop

Process stories **one at a time, sequentially**.

### 2a. Select next story

Find the first story in the state file with `status` = `"pending"`. If none remain, go to Step 3.

### 2b. Present the story

Tell the user which story is next and how many remain:

```
## Next: Story #<ref> - <subject> (<N> of <total>, <remaining> remaining)
```

If `previouslyDesigned` is true, note:
```
This story was previously designed. We're revisiting it per your request.
```

Ask the user: **"Ready to design this one, or skip it?"**

- If the user says skip: set `status` to `"skipped"`, `skipReason` to `"user skipped"`, update state file, go back to 2a.
- If ready: proceed to 2c.

### 2c. Invoke /design-story

Update the story's `status` to `"designing"` in the state file.

Invoke the design-story skill:
```
/design-story <ref>
```

### 2d. Record result

After `/design-story` completes (the user has gone through the full interactive design flow):

- Set story `status` to `"complete"` in the state file
- Update the state file

### 2e. Context window checkpoint

After completing the design, tell the user:

```
Design complete for Story #<ref>.

Progress: <completed>/<total> stories designed (<remaining> remaining).

To keep context fresh, restart this conversation and re-invoke:
  /fully-design-stories <epic-ref>

The state file will resume from Story #<next-ref>.
```

**Do NOT automatically proceed to the next story.** Wait for the user to either:
- Say they want to continue in this session (proceed to 2a)
- Restart the conversation (they'll re-invoke and resume from state file)

## Step 3: Complete

1. Update state file: `phase = "complete"`
2. Report summary:
   - Total stories in scope
   - Stories designed successfully (refs and subjects)
   - Stories skipped (refs, subjects, and reasons)
   - Stories that were revisits vs fresh designs
   - Remind: designed stories now have the `design-reviewed` tag and are in `Ready for Analysis` status, and their design docs are at `docs/designs/story-<ref>.md` (uncommitted until the operator commits them)
   - Suggest running `/fully-analyze-stories <epic-ref>` next for implementation planning

## Error handling

- If `/design-story` fails or the user abandons mid-design, set the story to `status = "pending"` (so it can be retried on resume) and update the state file.
- The state file at `/tmp/fully-design-stories-<epic-ref>.json` enables resumability: if interrupted, re-invoke to resume.
- **Never lose progress**: always update the state file after any status change.
