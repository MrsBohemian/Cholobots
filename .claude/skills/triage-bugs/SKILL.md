---
name: triage-bugs
description: Sweep untriaged Taiga bugs - validate severity and priority against docs/bug-triage.md, set surface/source tags, route to Needs Info or Rejected, report Urgent bugs
argument-hint: "[--dry-run] [--refs <ref,ref,...>]"
disable-model-invocation: true  # human-only or scheduled: an unattended sweep - a model spawning this mid-task creates two owners for the same bugs
allowed-tools:
  - mcp__taiga-py__*
  - Bash(python3 scripts/taiga_select_issues.py *)
  - Bash(python3 scripts/taiga_project_setup.py --check)
  - Bash(scripts/taiga-story.sh *)
  - Bash(env PR_BRANCH=* python3 scripts/taiga_closing_refs.py)
  - Bash(env -u GH_TOKEN gh pr list *)
  - Bash(env -u GH_TOKEN gh pr view *)
  - Read(//tmp/**)
  - Write(//tmp/**)
  - Edit(//tmp/**)
  - Read
  - Grep
  - Glob
---

# Triage Bugs $ARGUMENTS

Apply the rubric in `docs/bug-triage.md` to every open bug in Taiga project 39 (`cholo-bots`) that is not yet `triaged`. This skill is a thin wrapper: the rules, the definition of triaged, the sweep steps, and the report format are all in that document. Read it in full before starting and follow its "Triage sweep" section step by step. Nothing below repeats the rubric. The canonical status, severity, priority and tag names are in `.claude/skills/shared/taiga-project.md`.

**THIS SKILL DOES NOT FIX BUGS.** It only reads and updates Taiga issues.

Names this project uses (the workflow server resolves them, so no id lookups):

- Severities: `Wishlist`, `Minor`, `Normal`, `Important`, `Critical`
- Priorities: `Low`, `Normal`, `High`, `Urgent`
- Statuses used here: `New`, `Needs Info`, `Rejected`, `Closed`
- Tags: exactly one surface (`product`, `devx`, `pipeline`, `ci`), exactly one source (`src:user`, `src:monitor`, `src:manual-test`, `src:pr-review`, `src:agent`, `src:jon`), then `triaged`. A bot command or dashboard behaviour is surface `product`. For `product`, add every per-bot tag that applies (one or more of `metichebot`, `chismebot`, `guardabot`, `crudobot`, `obijuan`, `vueltabot`, `mobile`), keeping any already present; when no bot can be determined, add none and say so in the triage comment. Keep other topic tags.
- Source: use the default in `docs/bug-triage.md` ("Fields and tags", **Source**) exactly as written: bugs filed from the 2026-11-06 assessment (bug refs 1 to 57) are `src:jon`; any other bug with no source tag is `src:user`, because agent filing paths always set their source tag at creation. An existing source tag stands. Never guess beyond that rule.

## Step 1: Preflight and parse arguments

- `--dry-run`: compute every decision and print the report, but make no Taiga writes.
- `--refs <ref,ref,...>`: limit the sweep to those refs (still skips any that already carry `triaged` unless the ref is listed explicitly, in which case re-triage it and comment on what changed).
- No arguments: full sweep.

Run `python3 scripts/taiga_project_setup.py --check` once. It checks the statuses, the Urgent priority, the pipeline/surface/source tags and the roadmap wiki page. Exit 0: proceed. Exit 2 on a real run: stop, print the missing list and `Run: (eval "$(dv export project/taiga-growthgarage-jonroberts)" && python3 scripts/taiga_project_setup.py --apply)` for Jon. Exit 2 with `--dry-run`: print the missing list and the same apply command as a warning at the top of the report, then continue (no writes happen, so the missing names only affect what a real run could apply).

## Step 2: Fetch candidates

Follow "Triage sweep" steps 1 and 2 in `docs/bug-triage.md`. Select the candidates with `python3 scripts/taiga_select_issues.py --purpose triage` (read-only; it prints the same JSON shape as the other purposes). Its `selected` list is the open Bug-type issues that lack the `triaged` tag; with `--refs <ref,...>` it narrows to those refs. Exit 2 is a bad option (show stderr and stop), exit 1 is a Taiga error (report and stop). Question and Enhancement issues are never selected. A ref listed with `--refs` that already carries `triaged` shows up in `skippedRefs`; add it to the candidates anyway (explicit refs are re-triaged, per Step 1).

Then fetch PR claims once for the whole sweep: `env -u GH_TOKEN gh pr list --repo MrsBohemian/Cholobots --state all --limit 200 --json number,title,body,headRefName,state`. Never search GitHub per bug.

State file `/tmp/triage-bugs.json`. If it exists, its `project` is `39` and its `phase` is not `"complete"`, resume it: skip bugs whose `status` is `"written"`, and on a real run redo bugs left `"decided"` by an earlier dry run. Otherwise (no file, a different `project`, or `phase` `"complete"`) overwrite it with:

```json
{"project": 39, "phase": "triaging", "dryRun": <true with --dry-run, else false>, "bugs": [{"ref": 0, "id": 0, "subject": "", "status": "pending", "decision": null}]}
```

On resume, set `dryRun` from this run's flag. Per-bug `status`: `pending`, `decided` (dry run, nothing written) or `written`.

## Step 3: Triage each candidate

For each bug, in ref order, fetch it with `get_issue(project=39, ref=<ref>)` (never rely on a list payload for the description) and apply "Triage sweep" step 3 from `docs/bug-triage.md`: completeness test, duplicate rule (`search(project=39, query="<key terms>")`), surface, source, severity, priority, write. `get_issue` returns no dates or version; read `created_date`, `modified_date` and `version` with `scripts/taiga-story.sh <ref> 39 --issue` (read-only) for the triage window and Step 4. A candidate already in `Needs Info` whose description still fails the completeness test gets no new comment; Step 4 handles it.

**PR claims (GitHub only; Taiga comments are not scanned).** A PR claims a bug only where `scripts/taiga_closing_refs.py` finds the ref: a `#<ref>:` title prefix, a `fix(#<ref>):` scope, a trailing `(bug #<ref>)`, a standalone `Fixes|Closes|Resolves #<ref>` body line, or (only when none of those is present) the branch. A bare `#<ref>` mention is never a claim. For each PR from Step 2 whose title, body or branch contains the ref, confirm with `env PR_BRANCH="<headRefName>" PR_TITLE="<title>" PR_BODY="<body>" python3 scripts/taiga_closing_refs.py` (prints the claimed refs, space-separated). Use `env -u GH_TOKEN gh pr view <n> --repo MrsBohemian/Cholobots` only when one PR needs a closer look. A `MERGED` claim: `update_issue(project=39, ref=<ref>, status="Closed")` plus `add_comment(..., text="Closed by PR #N")`, no tags added. An `OPEN` claim: keep the status and continue tagging.

**Write.** `update_issue` has no comment parameter, so each bug written is two Taiga API calls: `update_issue(project=39, ref=<ref>, severity="<name>", priority="<name>", tags=[...], status="Needs Info" | "Rejected" (only when routing))`, then `add_comment(project=39, ref=<ref>, text="<comment>", entity_type="issue")` with the rationale. `tags` replaces the whole list (read-modify-write): `get_issue` returns tags as `[name, color]` pairs, but `update_issue(tags=[...])` takes plain strings, so flatten to the names, then send every tag you keep plus the surface, source and `triaged` tags. In dry-run mode, record the decision in the state file (`status: "decided"`) instead of writing.

Record each decision in `/tmp/triage-bugs.json` as you go (`status: "written"` after both calls succeed).

## Step 4: Expire stale Needs Info

"Triage sweep" step 4 in `docs/bug-triage.md`. List the bugs with `python3 scripts/taiga_select_issues.py --purpose triage --status "Needs Info"` (and `--purpose fix --status "Needs Info"` for triaged ones) and the Low review list with `python3 scripts/taiga_select_issues.py --purpose fix --max-priority Low`; read each `modified_date` with `scripts/taiga-story.sh <ref> 39 --issue`. In dry-run mode, list them instead.

## Step 5: Report

Produce the report exactly as "Triage sweep" step 5 in `docs/bug-triage.md` orders it. The Urgent section lists every bug whose **proposed** priority (this sweep's decision) is `Urgent`, not the selector's `urgent` list (that reflects the current Taiga priority and stays empty until the setup script has added `Urgent`). Each gets the line:

> Run `/fix-single-bug <ref>` now.

Set `phase` to `"complete"` in the state file. In dry-run mode, end the report with the count of writes that would have been made, where a write is one Taiga API call (each triaged, routed, closed or expired bug is two: `update_issue` plus `add_comment`), and the command to apply them (`/triage-bugs` with the same `--refs`, without `--dry-run`).
