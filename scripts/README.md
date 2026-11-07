# Pipeline helper scripts

Helpers the `.claude/skills` pipeline calls for Taiga (project 39, `cholo-bots`, on
https://backlog.growthgarage.org) and GitHub (`MrsBohemian/Cholobots`). They are stdlib-only
Python 3.14 and bash (`curl`, `jq`, `git`, `gh`; `codex` for reviews). None of them runs the bot.

## Environment

| Variable | Used by | Notes |
|---|---|---|
| `TAIGA_API_URL`, `TAIGA_USERNAME`, `TAIGA_PASSWORD` | every Taiga script | Exported by the repo `.envrc` (direnv). `taiga_project_setup.py` also reads `--env-file` and prompts on a TTY |
| `GH_REPO` | monitor, codex review, merge train | Default `MrsBohemian/Cholobots`. Every `gh` call runs as `env -u GH_TOKEN gh ... --repo "$GH_REPO"`, because the exported `GH_TOKEN` cannot see that repo |
| `MERGE_TRAIN_REMOTE`, `CODEX_REVIEW_REMOTE` | merge train, codex review | git remote holding PR branches, default `upstream` |
| `MONITOR_ADVISORY_CHECKS`, `MONITOR_ADVISORY_REVIEWERS`, `MONITOR_RUN_REGISTRATION_GRACE_SECONDS` | monitor | See `--help` |
| `CODEX_REVIEW_*` | codex review | See `--help` |

## Taiga

| Script | Purpose |
|---|---|
| `taiga_project_setup.py [--check\|--apply] [--env-file F]` | Adds the pipeline's statuses, `Urgent` priority, tags and the `cross-project-execution-order` wiki page; renumbers status order. Dry run by default; `--check` exits 2 when anything is missing. Copy `taiga-auth.env.example` to `taiga-auth.env` (git-ignored) for an admin run |
| `taiga-story.sh <ref> [39] [--issue\|--epic] [--with-tasks]` | Fetch a story/issue/epic as JSON; exit 44 when the ref does not exist |
| `taiga-epic.sh get <ref>` / `patch <ref> --description-file F [--version N]` | Read or replace an epic description (dependency diagram, hand-off); exit 3 on a version conflict, 44 when missing |
| `taiga-comment.sh <id> "<text>" [--issue]` | Comment on a story or issue by internal id |
| `taiga-wiki.sh get\|patch <slug\|page-id> ...` | Read a wiki page into a `/tmp` file or PATCH it from a file; exit 3 on a version conflict |
| `taiga_select_issues.py --purpose triage\|analyze\|fix\|sprint ...` | Read-only issue selector; prints JSON (`selected`, `urgent`, `skippedRefs`) |
| `taiga_audit.py [--out-dir D] [--roadmap F]` | Milestone and epic-link audit; writes `output/taiga-audit/report.{json,md}` and a manifest template |
| `taiga-update-on-merge.sh <ref> <pr> <url>` | Post-merge sync run by `.github/workflows/taiga-sync.yml` (story Done 80, issue Closed 74, task Closed 62), gated on the `mark-done-on-merge` / `close-tasks-on-merge` label or tag |
| `taiga_closing_refs.py` | The refs a merged PR claims (title prefix, `fix(#N)`, `Fixes #N` line, `feature/`/`fix/` branch suffix) |
| `taiga_find_duplicate_tasks.py`, `taiga_strip_duplicated_description.py`, `taiga_html_to_markdown.py` | Cleanup tools for agent-created tasks and descriptions |

## Pull requests

| Script | Purpose |
|---|---|
| `monitor-pr-stability.sh <pr> <quiet-min> [--max-seconds N]` | One stdout line: `STABLE`, `NEEDS_REBASE`, `CHECKS_FAILING:..`, `COMMENTS_FOUND:N`, `PENDING`. With no CI configured (zero workflows, until story #35) it prints `STABLE` and a `NOTE: no CI checks configured ...` line on stderr |
| `remediate_pr.py <pr> [--resume F]` | Exit-code driven remediation loop around the monitor; records the no-CI note in its state file |
| `codex-review-pr.sh <pr> [--dry-run]` | Detached Codex review posted as PR review threads; worktrees go under `.claude/worktrees/` |
| `merge_train.py <prs...\|epic:<ref>> [--resume F]` | Sequential rebase, check, merge. Post-rebase check is `python -m py_compile cholobots.py config.py db/database.py bots/*.py`, plus pytest once unit tests exist (story #34) |
| `lib/` | Shared Taiga client, dependency-diagram parser (`parse_roadmap.py --default-namespace cholobots`), base-branch selector, integration-branch names, stability parser, 0600 JSON writer for state files |

## Tests

```bash
python -m venv .venv && . .venv/bin/activate && pip install -r requirements-dev.txt
python -m pytest tests/scripts -q
for t in tests/scripts/test-*.sh; do bash "$t" || echo "FAILED: $t"; done
```

The tests mock `curl`, `gh` and `codex`; none of them needs network access or credentials.
