# Pipeline helper scripts

Helpers the `.claude/skills` pipeline calls for Taiga (project 39, `cholo-bots`, on
https://backlog.growthgarage.org) and GitHub (`MrsBohemian/Cholobots`). They are stdlib-only
Python 3.14 and bash (`curl`, `jq`, `git`, `gh`; `codex` for reviews), except
`check_local_env.py` (see [Local development](#local-development)). None of them runs the bot.

## Environment

| Variable | Used by | Notes |
|---|---|---|
| `TAIGA_API_URL`, `TAIGA_USERNAME`, `TAIGA_PASSWORD` | every Taiga script | Read from the shell only. See [Credentials](#credentials) |
| `GH_REPO` | monitor, codex review, merge train | Default `MrsBohemian/Cholobots`. Every `gh` call runs as `env -u GH_TOKEN gh ... --repo "$GH_REPO"`, because the exported `GH_TOKEN` cannot see that repo |
| `MERGE_TRAIN_REMOTE`, `CODEX_REVIEW_REMOTE` | merge train, codex review | git remote holding PR branches, default `upstream` |
| `MONITOR_ADVISORY_CHECKS`, `MONITOR_ADVISORY_REVIEWERS`, `MONITOR_RUN_REGISTRATION_GRACE_SECONDS` | monitor | See `--help` |
| `CODEX_REVIEW_*` | codex review | See `--help` |

### Credentials

The scripts read the Taiga credentials from the shell and never from a file in this repo. The
checkout's git-ignored `.envrc` loads them with direnv from the `project/taiga-growthgarage-slave1-codex` scope of
Jon's SOPS vault, `~/work/utils/jon-dev-env`. That scope holds the agent account, `slave1-codex`,
which the scripts and the Taiga MCP server use day to day:

```bash
watch_file "$HOME/work/utils/jon-dev-env/secrets/project/taiga-growthgarage-slave1-codex.enc.json"
_dev_env_exports="$("$HOME/work/utils/jon-dev-env/scripts/envtool" export project/taiga-growthgarage-slave1-codex)" || exit 1
eval "$_dev_env_exports"; unset _dev_env_exports
```

On a new machine, set up `jon-dev-env` as its README describes. That means restoring the age
identity, installing `sops` and `direnv`, and hooking direnv into Bash. Then create the `.envrc`
above in this checkout and run `direnv allow`.

`taiga_project_setup.py --apply` changes project settings, which the agent account cannot do. Jon
stores his admin account once in a second scope, typing each value at a hidden prompt. `dv` is the
vault's own command; a shell that has loaded the `jon-dev-env` profile has it on `PATH`, and
`$DEV_ENV_HOME/scripts/envtool` is the same tool for a script that runs without that profile:

```bash
dv set project/taiga-growthgarage-jonroberts TAIGA_API_URL   # https://backlog.growthgarage.org
dv set project/taiga-growthgarage-jonroberts TAIGA_USERNAME  # jonroberts
dv set project/taiga-growthgarage-jonroberts TAIGA_PASSWORD
```

The dry run and `--check` work with the agent account. The apply loads the admin scope into a
subshell, so the admin password stays out of the interactive shell:

```bash
python3 scripts/taiga_project_setup.py
(eval "$(dv export project/taiga-growthgarage-jonroberts)" && python3 scripts/taiga_project_setup.py --apply)
python3 scripts/taiga_project_setup.py --check
```

The dry run and the apply print the account they logged in as. If the apply reports
`slave1-codex`, the admin scope is missing or incomplete, because `dv export` prints nothing
for a scope that does not exist.

## Taiga

| Script | Purpose |
|---|---|
| `taiga_project_setup.py [--check\|--apply]` | Adds the pipeline's statuses, `Urgent` priority, tags and the `cross-project-execution-order` wiki page; renumbers status order. Dry run by default; `--check` exits 2 when anything is missing. `--apply` needs the admin account, see [Credentials](#credentials) |
| `taiga-story.sh <ref> [39] [--issue\|--epic] [--with-tasks]` | Fetch a story/issue/epic as JSON; exit 44 when the ref does not exist |
| `taiga-epic.sh get <ref>` / `patch <ref> --description-file F [--version N]` | Read or replace an epic description (dependency diagram, hand-off); exit 3 on a version conflict, 44 when missing |
| `taiga-comment.sh <id> "<text>" [--issue]` | Comment on a story or issue by internal id |
| `epic_dependency_diagram.py --epic-json F --deps F --out F` | Merges reported story dependencies into an epic description's diagram (existing edges kept, out-of-epic refs as `[outside epic]` roots, soft orderings below the block), validates it with the shared parser and writes the new description to `--out`; prints a JSON status (`written`/`unchanged`/`invalid`). Exit 0 ok, 2 invalid (nothing written), 1 usage or I/O error. `--fallback-state <state.json> --epic-refs 1,2` instead prints which unreported dependency categories (`upstream`, `soft`) each in-epic story must take from its description |
| `taiga-wiki.sh get\|patch <slug\|page-id> ...` | Read a wiki page into a `/tmp` file or PATCH it from a file; exit 3 on a version conflict |
| `taiga_select_issues.py --purpose triage\|analyze\|fix\|sprint ...` | Read-only issue selector; prints JSON (`selected`, `urgent`, `skippedRefs`) |
| `taiga_audit.py [--out-dir D] [--roadmap F]` | Milestone and epic-link audit; writes `output/taiga-audit/report.{json,md}` and a manifest template |
| `[PR_LABELS="..."] taiga-update-on-merge.sh [--skip-if-synced] <ref> <pr> <url>` | Post-merge sync for one ref (story Done 80, issue Closed 74, task Closed 62), gated on the `mark-done-on-merge` / `close-tasks-on-merge` label or tag. `--skip-if-synced` skips the status/comment step when the item already has a `Merged in PR #N (` or `Closed by PR #N (` comment; a story's still-open tasks are closed when `close-tasks-on-merge` applies. Driven by `taiga_sync_from_push.py` in the fork's `.github/workflows/taiga-sync.yml` |
| `taiga_sync_from_push.py [--before SHA] [--after SHA] [--dry-run]` | The fork workflow's driver: resolves each commit pushed to the fork's `main` to its merged upstream PR, skips PRs below `TAIGA_SYNC_MIN_UPSTREAM_PR` (default 8), unmerged or not based on `main`, and runs `taiga-update-on-merge.sh --skip-if-synced` once per claimed ref with the PR's labels. Needs `GITHUB_TOKEN` and `TAIGA_*`; exit 1 when any update failed |
| `taiga_closing_refs.py` | The refs a merged PR claims (title prefix, `fix(#N)`, `Fixes #N` line, `feature/`/`fix/` branch suffix) |
| `taiga_find_duplicate_tasks.py`, `taiga_strip_duplicated_description.py`, `taiga_html_to_markdown.py` | Cleanup tools for agent-created tasks and descriptions |

## Pull requests

| Script | Purpose |
|---|---|
| `monitor-pr-stability.sh <pr> <quiet-min> [--max-seconds N]` | One stdout line: `STABLE`, `NEEDS_REBASE`, `CHECKS_FAILING:..`, `COMMENTS_FOUND:N`, `PENDING`. A PR with no runs (a branch that predates `checks.yml`/`integration.yml`) counts as passed after the run-registration grace (`MONITOR_RUN_REGISTRATION_GRACE_SECONDS`, default 120) when its base requires no checks; otherwise missing required runs stay `PENDING` until the branch is rebased onto a base that has the workflows. Only a repo with zero workflows gets `STABLE` at once plus a `NOTE: no CI checks configured ...` line on stderr |
| `remediate_pr.py <pr> [--resume F]` | Exit-code driven remediation loop around the monitor; records the no-CI note in its state file |
| `codex-review-pr.sh <pr> [--dry-run]` | Detached Codex review posted as PR review threads; worktrees go under `.claude/worktrees/` |
| `merge_train.py <prs...\|epic:<ref>> [--resume F]` | Sequential rebase, check, merge. Post-rebase check is `python -m py_compile cholobots.py config.py db/database.py bots/*.py`, plus pytest once unit tests exist (story #34) |
| `lib/dependency_satisfaction.py [--base B] <ref>...` | `/implement-epic` Step 2a-bis: classifies out-of-run dependencies as satisfied when Done, or when a merged PR into the base (default `$BASE_BRANCH` or `main`) claims the ref, no open PR does (claims per `taiga_closing_refs.py`) and every open task has a `## Gate` section; Archived, an open non-gated task, or a failed Taiga/gh/task lookup is never satisfied. Prints `satisfied_refs` plus per-ref reason, PRs, gated tasks and open agent tasks |
| `lib/` | Shared Taiga client, dependency-diagram parser (`parse_roadmap.py --default-namespace cholobots`), base-branch selector, integration-branch names, stability parser, 0600 JSON writer for state files |

## Database schema

| Script | Purpose |
|---|---|
| `check_schema_coverage.py [--repo-root .] [--migrations supabase/migrations] [--mobile-dir D] [--json]` | Lists every table/view the bots (`.table("x")` in `bots/*.py`, `db/database.py`) and dashboards (`.from("x")` in `D/*.html`, `D/assets/*.js`) reference, and whether `supabase/migrations/*.sql` defines it, with kind, RLS state and policy count; objects nobody references are listed separately. `--mobile-dir` defaults to `mobile/`, else `../Cholobots-Mobile` (pass it explicitly from a worktree). Exit 0 all present, 1 something missing, 2 a directory not found. A dashboard-read table with RLS off is a `WARN` line, not a failure. Reads files only; never connects to Supabase |

## Local development

| Script | Purpose |
|---|---|
| `check_local_env.py [--env-file F]` | Preflight `make run` calls before `cholobots.py`. Resolves `.env` over the shell like `config.py` and exits 1, one line per problem on stderr, unless `SUPABASE_URL` is on `127.0.0.1`, `localhost` or `::1`, `DISCORD_TOKEN`, `OPENAI_API_KEY` and `SUPABASE_SERVICE_ROLE_KEY` are non-empty, and `TRANSPORTATION_CHANNEL_ID` (when set) is an integer. Has no override flag. Needs python-dotenv, so it runs with `.venv/bin/python` |
| `devcontainer/write_env.py ENV_FILE` | Run by `.devcontainer/post-start.sh`; stdlib-only (it imports `is_local_url` from `check_local_env.py`). Reads `supabase status -o env` on stdin and merges `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `DISCORD_TOKEN` (from `CHOLOBOTS_TEST_DISCORD_TOKEN`) and `OPENAI_API_KEY` into the given `.env`, leaving every other line alone. Refuses a `SUPABASE_URL` that is not local, never prints secret values, writes the file with mode 0600. Tests: `tests/scripts/test_devcontainer_write_env.py` and `test-devcontainer-lifecycle.sh` |

## Tests

```bash
python -m venv .venv && . .venv/bin/activate && pip install -r requirements-dev.txt
python -m pytest tests/scripts -q
for t in tests/scripts/test-*.sh; do bash "$t" || echo "FAILED: $t"; done
```

The tests mock `curl`, `gh` and `codex`; none of them needs network access or credentials. The `.env`-file cases in `test_check_local_env.py` skip unless python-dotenv is installed (`requirements.txt`).
