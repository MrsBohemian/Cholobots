# Bug Filing Protocol

Follow this whenever a skill or agent creates a Taiga bug. The rubric lives in
`docs/bug-triage.md`; this file is only the procedure for applying it at filing time. Field and
tag names come from `.claude/skills/shared/taiga-project.md`.

## Step 1: Names, not ids

The `taiga-py` workflow server resolves names, so there is nothing to fetch. Use type `Bug`,
status `New`, severities `Wishlist` / `Minor` / `Normal` / `Important` / `Critical`, priorities
`Low` / `Normal` / `High` / `Urgent`.

## Step 2: Check for duplicates

`search(project=39, query="<key terms>")`, one or two distinctive tokens at a time. If a matching
open issue exists, do not create a new one: reference the existing ref in your output and, if you
have new evidence, add it with `add_comment(project=39, ref=<ref>, text="<evidence>", entity_type="issue")`.

## Step 3: Classify per `docs/bug-triage.md`

1. **Surface** (exactly one): `product`, `devx`, `pipeline`, `ci`. A `product` bug also gets every
   per-bot tag that applies (one or more of `metichebot`, `chismebot`, `guardabot`, `crudobot`,
   `obijuan`, `vueltabot`, `mobile` for the dashboards); when no bot can be determined, add none
   and say so in the description. Add any existing topic tag that fits (`listeners`, `data-loss`,
   `timezone`, ...).
2. **Source** (exactly one): `src:user`, `src:monitor`, `src:manual-test`, `src:pr-review`,
   `src:agent`, `src:jon`. Skills filing on their own behalf use `src:agent`; the manual-testing
   agent uses `src:manual-test`; PR comment resolution uses `src:pr-review`.
3. **Severity**: pick from the severity table for that surface. This is provisional; the triage
   sweep confirms it.
4. **Priority**: read it off the severity × surface matrix, then apply the modifiers. If you
   deviate from the matrix, say why in the description.

Do not apply the `triaged` tag. Only the triage sweep sets it.

## Step 4: Create the issue

```
create_issue(
    project=39,
    subject="<concise title>",
    description="<template below>",
    issue_type="Bug",
    severity="<name>",
    priority="<name>",
    tags=["<surface>", "<source>", "<per-bot tag>", ...]
)
```

The issue starts in status `New`. If `create_issue` drops the tags, set them with
`update_issue(project=39, ref=<ref>, tags=[...])`.

Description template (fill every line; write "unknown" rather than omitting a line):

```
**Expected:** ...
**Actual:** ...
**Reproduce / observed at:** <command and channel, or file:line>
**Preliminary cause:** ...
**Evidence:** <links, log lines, PR, commit hash, Discord message link>
```

Add any context the filing skill already requires (for example "Discovered during PR #N review"
with file and line) above the template.

## Step 5: Report the ref

Return the created ref number to the caller or user, and follow any skill-specific follow-up
(PR comment, PR description note). An `Urgent` bug is never batched: say
`Run /fix-single-bug <ref> now` in the report.
