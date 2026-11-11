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
| `ORCHESTRATE_REMOTE` | `devcontainer/orchestrate_*.py` | git remote holding base and integration branches, default `upstream` (`GH_REPO` names the repo) |
| `ORCHESTRATE_SCRIPT_DIR`, `ORCHESTRATE_STATE_DIR`, `ORCHESTRATE_REPO_ROOT`, `BASE_BRANCH` | `devcontainer/orchestrate_*.py` | Directory holding `launch.sh`, `prepare-clone.sh`, `exec-skill.sh`, `exec.sh` and `teardown.sh` (default: the orchestrator's own directory; tests point it at fakes), state file directory (default `/tmp`), host repo (default: the checkout containing the script), default base branch (default `main`) |
| `ORCHESTRATE_TEST_HOST_STATE` | `devcontainer/orchestrate_*.py` | Test-only (`1`): read a per-item outcome file from the state dir when the in-container read fails, which is how the fake `exec-skill.sh` in the tests reports a result. Unset in real runs, because that path is shared with worktree-mode runs and a stale file there would be taken for this run's outcome |
| `POLL_INTERVAL`, `ORCHESTRATE_LAUNCH_TIMEOUT`, `ORCHESTRATE_TEARDOWN_TIMEOUT`, `ORCHESTRATE_TRANSIENT_BACKOFF` | `devcontainer/orchestrate_*.py` | Seconds: child poll interval (30) and container launch (1800), both orchestrators; `ORCHESTRATE_TEARDOWN_TIMEOUT` (180) and `ORCHESTRATE_TRANSIENT_BACKOFF` (900, the wait before relaunching after a provider rate limit) are read by `orchestrate_epic.py` only |
| `MONITOR_ADVISORY_CHECKS`, `MONITOR_ADVISORY_REVIEWERS`, `MONITOR_RUN_REGISTRATION_GRACE_SECONDS` | monitor | See `--help` |
| `CODEX_REVIEW_*` | codex review | See `--help` |
| `SUPABASE_CLI` | `supabase-local.sh` | Supabase CLI command, default `npx --yes supabase@2.121.0` (the Makefile exports the same value) |
| `SUPABASE_LOCAL_EXCLUDE` | `supabase-local.sh` | Comma-separated services to skip on `start`; default none in the main checkout and a lighter set in a worktree. Empty value starts everything |
| `CHOLOBOTS_CHECKOUT` | `supabase-local.sh` | Checkout to act on when no path argument is given, default the current directory |
| `CHOLOBOTS_DEVCONTAINER` | `supabase-local.sh` | `1` (set by the devcontainer) makes the checkout use the default ports and project id |
| `CHOLOBOTS_CONTAINER_GH_TOKEN` | `devcontainer/launch.sh` | Token the container uses for `gh` and git pushes to `MrsBohemian/Cholobots`; the host `GH_TOKEN` is never passed in. Export it via direnv, never from a file in the repo. Unset: ssh-agent forwarding, else no push access |
| `GH_TOKEN_FOR_PACKAGES` | `devcontainer/launch.sh` | Host-side `docker login ghcr.io` for the pre-built image (needs `read:packages`); falls back to `GH_TOKEN`, then `GITHUB_TOKEN` |
| `CHOLOBOTS_DEVCONTAINER_IMAGE` | `devcontainer/launch.sh` | Pre-built image to pull, default `ghcr.io/mrsbohemian/cholobots/devcontainer:latest`. The fallback local build is tagged per workspace (`cholobots-devcontainer[-base]:local-<key>`); `teardown.sh` removes it |
| `SSH_AUTH_SOCK` | `devcontainer/launch.sh` | Forwarded into the container when no container token is set (Linux) |
| `XDG_CACHE_HOME` | `devcontainer/launch.sh`, `teardown.sh` | Parent of `cholobots-devcontainer/<hash>/`, where each workspace's override `devcontainer.json` lives (default `~/.cache`) |

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
| `supabase-local.sh start\|stop\|status\|env\|prune [checkout-path]` (core: `supabase_local.py`) | Manages per-checkout local Supabase stacks. The main checkout and the devcontainer use the default ports. Each worktree gets project id `cholobots-<name>-<hash>` (sanitized directory name plus 8 hex digits of the full path's hash) and a slot `n`, recorded in the git common dir; every port is the default plus `10*n` (API `54321+10n`, DB `54322+10n`), the inspector 8083 plus `n`. `start` mirrors `supabase/` into the worktree's git-ignored `.supabase-local/`; `stop` frees the slot; `env` prints `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `SUPABASE_DB_URL`; `prune` stops stacks of removed worktrees, including deleted directories git still lists. Forwards only `db reset\|diff\|lint`, `migration new\|list\|up` and `test db`, with the checkout path right after the command words (`db reset <path>`), and appends `--local` to those that choose a target. Refuses everything else, hosted options (`--linked`, `--db-url`, `--project-ref`, `--password`, `--local=false`, with or without `=value`) and `--workdir`/`--project-id`. Needs Docker and Node. Tests: `tests/scripts/test_supabase_local.py`, and `tests/scripts/test-supabase-local.sh` (real stacks; runs only with `SUPABASE_LOCAL_LIVE=1` and Docker) |
| `devcontainer/launch.sh <path> [<branch>] [--worktree [<base>]]` | Starts an isolated devcontainer for a checkout and prints `CONTAINER_ID=<id>`. Without `--worktree` a missing `<path>` becomes a shallow clone of this checkout (`<branch>` defaults to `main` and must exist locally; `origin` and `upstream` point at GitHub; a `.git/cholobots-managed-clone` marker is written). With `--worktree` a missing `<path>` becomes a git worktree (`<branch>` required; `<base>` is the start point for a new branch) and the common git dir is mounted. An existing path is reused. Refuses a main checkout (a `.git` directory without the marker), because the container rewrites its `.env`. Needs Docker, `jq` and the `devcontainer` CLI 0.84.1+. Pulls the pre-built GHCR image, else builds `.devcontainer/Dockerfile` locally. **GitHub access:** the container gets only `CHOLOBOTS_CONTAINER_GH_TOKEN` (`gh auth login` and `gh auth setup-git`), never the host `GH_TOKEN`. Without it the host ssh-agent socket is forwarded (Linux); with neither, pushes fail and the script warns. The host `GH_TOKEN`/`GITHUB_TOKEN` (or `GH_TOKEN_FOR_PACKAGES`) is used only for the host's `docker login ghcr.io`. Never write a token into a file or an argument |
| `tools/pytaiga-mcp/install.sh` | Installs the pinned workflow-mode `pytaiga-mcp` (Alterna-Dev-Studio fork) into `PYTAIGA_MCP_HOME` (default `~/work/utils/pytaiga-mcp`; the devcontainer image uses `/opt/pytaiga-mcp`). The only place the commit is pinned; a marker written last makes a rerun a no-op and a failed install never look complete. Runs `uv sync` with `UV_CONCURRENT_DOWNLOADS=1` because parallel git fetches can fail with TLS errors on Docker bridge networks. Usable on the host as an alternative to jon-dev-env's `setup-taiga-mcp`; needs `git` and `uv`. `--print-marker` prints the marker. Tests: `tests/scripts/test-pytaiga-mcp-install.sh` |
| `devcontainer/pins.sh [root]` | Prints every devcontainer tool pin as `KEY='value'` lines, parsed from `Dockerfile`, `Dockerfile.prebuild` and `install.sh`. Read by `smoke-test.sh` and the build workflow's version check. Read pins from here, never copy them into docs. Tests: `tests/scripts/test-devcontainer-pins.sh` |
| `devcontainer/exec.sh <path> <command...>` | Runs one command in the container whose `devcontainer.local_folder` label is the physical (`pwd -P`) path, as `vscode` in `/workspaces/<dir name>`, with `.venv/bin` first on `PATH` when it exists. Exits 1 when no container, or more than one, matches (run `teardown.sh` first). The command string reaches the inner shell as a single argument |
| `devcontainer/exec-skill.sh <path> <skill> <arguments> [--provider claude\|codex] [--log F] [--dry-run]` | Pipes `.claude/skills/<skill>/SKILL.md` (frontmatter stripped, `$ARGUMENTS` substituted) into `claude --dangerously-skip-permissions -p -` (or `codex exec ...`) inside that container and streams the JSON events to the log (default `/tmp/exec-skill-<skill>-<timestamp>.log`). `--dry-run` prints the command and the first 5 prompt lines. Same container lookup and exit-1 rules as `exec.sh` |
| `devcontainer/orchestrate_epic.py <epic-ref> <concurrency> <stories-json> [--dry-run]` | Host-side state machine behind `/implement-epic <ref> <n>` with n > 1. Per story: launch a container clone, run `/fully-implement-story` through `exec-skill.sh`, read the in-container state file, relaunch after a rate limit, tear down. Writes `${ORCHESTRATE_STATE_DIR}/implement-epic-<epic-ref>.json` with `"mode": "container"` and per-story `clonePath`, `containerId`, `logFile`; prints `LAUNCHING`, `LAUNCH_FAIL`, `RATE_LIMITED` and `DONE` events. `--dry-run` smoke-tests each stack and keeps every container and clone |
| `devcontainer/orchestrate_bugs.py <concurrency> <bugs-json>` | Same for `/parallel-fix-bugs <n>` with n > 1: runs `fix-and-remediate-bug` per bug, state in `${ORCHESTRATE_STATE_DIR}/parallel-fix-bugs.json`, clones under `/tmp/parallel-fix-bugs/bug-<ref>`, torn down per bug. No `--dry-run` |
| `devcontainer/prepare-clone.sh <path>` | Writes the clone's `.claude/settings.local.json` (allowlisted tools, a hook rejecting self-matching `pgrep`/`pkill`) and `.mcp.json` (taiga-py and chrome-devtools) for in-container agent sessions, reading the browser and taiga paths from the clone's own Dockerfiles. Run it before `exec-skill.sh` |
| `devcontainer/teardown.sh <path> [--keep-clone]` | Stops and removes every container labelled for `<path>` (the physical path), then its `cholobots-venv-*` and DinD volumes and the cached override config, and fails if any container survives. Then removes `<path>` only if it is a `launch.sh`-managed clone: it refuses a worktree (remove it with `git worktree remove <path>` from its main checkout), anything without the marker, `/` and `$HOME`; `--keep-clone` skips the removal |
| `devcontainer/worktree-add.sh <repo-root> <path> <branch> [<base>]` and `--relativize <path>` | Used by `launch.sh`. Adds a worktree (reusing `<branch>` when it exists locally) with a relative `.git` pointer so the devcontainer CLI can mount the common dir; `--relativize` fixes an existing worktree |
| `hooks/reject-self-matching-pgrep.sh` | PreToolUse hook (written into a clone's `settings.local.json` by `prepare-clone.sh`) that blocks `pgrep -f` / `pkill -f`, which match the agent's own Bash shell. Test: `tests/scripts/test-reject-self-matching-pgrep.sh` |
| `devcontainer/start-registry-mirror.sh`, `devcontainer/smoke-test.sh` | The first starts the local `registry-mirror` Docker container (pull-through cache for the in-container Docker); `launch.sh` calls it and ignores failure. `smoke-test.sh` launches a clone (of the current branch, or of a temporary branch it makes and removes when HEAD is detached), checks the toolset and in-container Supabase stack, and tears it down (needs a Docker daemon) |

**Clone or worktree.** A clone (`launch.sh <path> <branch>`) has its own object store and config, so an autonomous run cannot touch your checkout or the shared `.git`; `teardown.sh` deletes it. A worktree (`launch.sh <path> <branch> --worktree`) shares objects, branches, `.git/config` and the stash with the main checkout, like the worktrees under `.claude/worktrees/`: cheaper and faster, and `teardown.sh` never deletes it. Tests: `tests/scripts/test-devcontainer-*.sh`, `test-launch-*.sh`, `test-prepare-clone.sh` and `test-worktree-add.sh`, all against a stub `docker`.

## Tests

```bash
python -m venv .venv && . .venv/bin/activate && pip install -r requirements-dev.txt
python -m pytest tests/scripts -q
for t in tests/scripts/test-*.sh; do bash "$t" || echo "FAILED: $t"; done
```

The tests mock `curl`, `gh` and `codex`; none of them needs network access or credentials. The `.env`-file cases in `test_check_local_env.py` skip unless python-dotenv is installed (`requirements.txt`).
