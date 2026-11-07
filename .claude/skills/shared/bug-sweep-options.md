# Bug Sweep Options

Every bug sweep skill (`/triage-bugs`, `/fully-analyze-bugs`, `/parallel-fix-bugs`, `/fully-fix-bugs`, `/fix-bugs`) selects its bugs the same way: it runs `scripts/taiga_select_issues.py`, shows the result, confirms it, and records the options. The skill does not restate the selection rules; the script is authoritative for them.

## 1. Arguments

The skill takes its own positional argument (concurrency, where one exists) and `--yes`. Every other flag is passed verbatim to the selector:

```
python3 scripts/taiga_select_issues.py --purpose <triage|analyze|fix> [flags...]
```

| Flag | Meaning |
|---|---|
| `--surface` / `--exclude-surface` | Keep / drop bugs by surface tag (`product`, `devx`, `pipeline`, `ci`) |
| `--source` / `--exclude-source` | Keep / drop bugs by source tag (`src:user`, `src:monitor`, `src:manual-test`, `src:pr-review`, `src:agent`, `src:jon`) |
| `--min-priority` / `--max-priority` | Priority floor / ceiling (Low, Normal, High, Urgent), compared by Taiga `order` |
| `--min-severity` / `--max-severity` | Severity floor / ceiling (Wishlist, Minor, Normal, Important, Critical), compared by Taiga `order` |
| `--refs` / `--exclude-refs` | Narrow to / remove specific bug refs |
| `--tag` / `--exclude-tag` | Keep / drop bugs by any tag, for example a per-bot tag (`--tag crudobot`) |
| `--milestone` / `--exclude-milestone` | Keep / drop bugs by milestone (id or slug); no milestones exist yet |
| `--status` / `--exclude-status` | Replace the purpose's default status set / add statuses to its exclusions |

The project defaults to 39. `python3 scripts/taiga_select_issues.py --help` is the authoritative grammar. Semantics:

- Values are comma lists, and flags may repeat.
- Include values are OR'd within a dimension and AND'd across dimensions.
- Any exclude match removes the bug.
- `--status` replaces the default status set; `--exclude-status` adds to it.
- `--refs` only narrows. A listed ref that fails eligibility or a filter is never forced in; it appears in `skippedRefs` with the reason.

## 2. Purposes

The selector defines default eligibility once per purpose, applied before filters:

- `triage`: open Bugs without the `triaged` tag. Used only by `/triage-bugs`.
- `analyze`: open Bugs, `triaged`, not `llm-analyzed`. Urgent bugs stay in the batch, since analysis helps the hotfix.
- `fix`: open Bugs, `triaged`, status not `Ready for review` or `Blocked`. Filters run first; Urgent bugs that survive them are moved out of the batch into `urgent`.

Sweep skills never pass `--purpose sprint`; `/overseer sprint` documents its own use. The statuses and the `Urgent` priority come from `scripts/taiga_project_setup.py`; the sweep skill's own preflight (`--check`) guarantees they exist.

## 3. Run and handle exit codes

Run the selector before anything else. It is read-only and prints one JSON object: `{purpose, options, ids, selected, urgent, skippedRefs, counts}`.

- Exit 0: continue (an empty selection is still exit 0).
- Exit 2: a bad option. Show stderr, which lists the valid values. An attended run re-asks for flags; a `--yes` run stops.
- Exit 1: a Taiga error. Report it and stop.

## 4. Confirm

Render one table:

- The effective `options` as `key: value` lines.
- An **Urgent (not batched)** block, listed first, with one line per bug: `#<ref> <subject>: Run /fix-single-bug <ref> now.`
- The `selected` rows as `#ref · priority · severity · surface · subject`.
- `skippedRefs` with their reasons.

Then:

- With `--yes`, print the table and proceed.
- Otherwise, use AskUserQuestion with **Accept** / **Cancel**. Other means the operator types replacement flags: re-run the selector and re-confirm.
- If AskUserQuestion is unavailable (a subagent or headless run) and `--yes` is absent, abort with `Re-run with --yes to accept these options unattended.` Never proceed silently.
- An empty `selected` means print the table (including the Urgent block) and stop.

## 5. Record

Store the selector's `options` object, the raw flag string, and the `urgent` refs in the run's state file under `options`, `rawFlags` and `urgentRefs`.

**Resume** means the state file already exists and is not complete. A resume never re-runs the selector and never re-prompts; it reports the stored options and continues.

## 6. Report

The final report starts with the Urgent block again, so it is not lost in a long run.
