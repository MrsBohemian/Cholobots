---
name: remediate-taiga-audit
description: Apply reviewed Taiga milestone and epic-link fixes from a manifest generated from the taiga audit report (project 39, cholo-bots)
argument-hint: "<manifest-path> [--audit <report-path>]"
allowed-tools:
  - Bash(python3 scripts/taiga_audit.py *)
  - Bash(cat *)
  - Bash(ls *)
  - mcp__taiga-py__get_story
  - mcp__taiga-py__get_epic_overview
  - mcp__taiga-py__update_story
  - mcp__taiga-py__move_to_sprint
  - mcp__taiga-py__get_sprint_board
  - Read
  - Grep
---

# Remediate Taiga Audit $ARGUMENTS

Apply only reviewed changes from a manifest, to Taiga project 39 (`cholo-bots`). The audit script (`scripts/taiga_audit.py`) stays read-only and is the source of truth for validation. Names of statuses, tags and fields are in `.claude/skills/shared/taiga-project.md`.

## Step 1: Resolve paths

Parse `$ARGUMENTS` as `<manifest-path> [--audit <report-path>]`.

Defaults:
- manifest: `output/taiga-audit/remediation-manifest.json`
- audit report: `output/taiga-audit/report.json`

If the audit report is missing, run:

```bash
python3 scripts/taiga_audit.py
```

(It needs `TAIGA_API_URL`, `TAIGA_USERNAME` and `TAIGA_PASSWORD` in the shell; the repo `.envrc` exports them through direnv.)

## Step 2: Read and validate input

Read the manifest and audit JSON.

Manifest format:

```json
{"actions":[...]}
```

Allowed action types:
- `assign_milestone` (fields `story_ref`, `milestone_slug`)
- `link_epic` (fields `story_ref`, `epic_ref`)

Before mutating Taiga, validate each action against the audit report:
- `assign_milestone` must match a story listed in `project39.no_milestone.expected_assignment` or `project39.no_milestone.manual_review`, and `milestone_slug` must be one of the milestones listed under `summary.project_39.milestones`. No milestones (sprints) exist in project 39 yet, so until Jon creates one this action type fails validation.
- `link_epic` must match a record in `candidate_missing_epic_links` (same `story_ref` and `epic_ref`; both in project 39)
- Any other action type is rejected
- If any action does not match the report, stop and report the mismatch before changing anything

## Step 3: Apply milestone assignments

Group all `assign_milestone` actions by `milestone_slug`.

For each group, confirm the sprint exists with `get_sprint_board(project=39, sprint="<milestone_slug>")`, then apply one move:

```text
move_to_sprint(project=39, story_refs=[<ref1>, <ref2>, ...], sprint="<milestone_slug>")
```

## Step 4: Apply epic links

For each `link_epic` action:
1. Confirm the epic exists: `get_epic_overview(project=39, ref=<epic_ref>)`
2. Call:

```text
update_story(project=39, ref=<story_ref>, epic=<epic_ref>)
```

## Step 5: Verify and report

Re-read each changed story with `get_story(project=39, ref=<story_ref>)` and confirm:
- milestone assignment matches the requested target
- epic link now exists

Print a summary with:
- applied milestone changes
- applied epic links
- skipped actions
- any validation failures

Do not invent or auto-approve changes that are not explicitly listed in the manifest. Findings in the report's `project39.zero_epic` and `project39.multi_epic_context` sections are for human review and are never applied from here.
