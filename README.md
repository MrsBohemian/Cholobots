# Cholobots

A Discord bot that hosts six Spanglish-speaking personas for running a small contracting business from chat:

| Persona | Help command | Does |
|---|---|---|
| Metichebot | `!metichebot` | weekly plan, daily tasks, time tracking, check-in pings |
| Chismebot | `!chismebot` | leads, customer cards, follow-ups (CRM) |
| Guardabot | `!guardabot` | inventory and garage zones |
| Crudobot | `!crudobot` | job costing, closeout, receipt scanning |
| Obi Juan | `!obijuan` | subcontractor "quests", clock-in/out, payouts |
| Vueltabot | `!vueltabot` | marketplace listings and delivery gigs |

Data lives in Supabase. `mobile/` holds static dashboards that read the same Supabase project ([mobile/README.md](mobile/README.md)), published at https://mrsbohemian.github.io/Cholobots/.

## Quickstart

From a fresh clone to the bot answering in your own test Discord server, against a Supabase stack on your machine, never the hosted project. `make help` lists every target.

1. **Prerequisites.** git, GNU make, [uv](https://docs.astral.sh/uv/) at the version `scripts/devcontainer/pins.sh` prints as `UV_VERSION` (declared in `.devcontainer/Dockerfile.prebuild`; install it with `brew install uv` or the [astral installer](https://docs.astral.sh/uv/getting-started/installation/), and `make check-tools` names it when missing), Python 3.14 (the version in `.python-version`; uv can fetch it for you), Docker running (Docker Engine on Linux, Docker Desktop or OrbStack on macOS) and Node 18+ for `npx`. The shell tests also need `jq`, `curl`, GNU `timeout` and `setsid` (`make setup` and `make test` name any that is missing). On macOS: `brew install jq coreutils util-linux`, then put `$(brew --prefix coreutils)/libexec/gnubin` and `$(brew --prefix util-linux)/bin` on your `PATH`. On Debian or Ubuntu: `apt install jq curl coreutils util-linux`. On Windows, use WSL2 with Docker Desktop's WSL integration and run everything from the WSL shell. The Makefile runs the Supabase CLI as `npx --yes supabase@2.121.0`; if you installed that version with Homebrew (`brew install supabase/tap/supabase`) or Scoop, add `SUPABASE=supabase` to each `make db-*` command. `supabase --version` should print 2.121.0.
2. **Install.**
   ```bash
   git clone https://github.com/MrsBohemian/Cholobots.git && cd Cholobots
   make setup
   ```
   This creates `.venv`, installs the locked dependencies from `uv.lock` with `uv sync --locked`, installs the pre-commit hook, and copies [.env.example](.env.example) to `.env` (an existing `.env` is left alone).
3. **Start the database.** `make db-up` (the first run pulls images and takes several minutes). It runs Postgres, Auth, Storage, the REST API and Studio in Docker: the API is at `http://127.0.0.1:54321` and Studio at `http://127.0.0.1:54323`. Edge Functions are off in `supabase/config.toml` because the repo has none. Then `make db-reset` rebuilds the database from the baseline schema in `supabase/migrations/` plus the fictitious seed data described under [Seed data](#seed-data).
4. **Point `.env` at it.** `make db-env` prints a `SUPABASE_URL=` and a `SUPABASE_SERVICE_ROLE_KEY=` line; paste both into `.env`, replacing the lines already there. `.env` is loaded with `override=True`, so it silently wins over your shell: keep the hosted URL, the hosted service-role key and the production Discord token out of it. A wrong or empty value shows up as a `create_client` error when the process starts.
5. **Discord.** Create your own test application, bot and server as [docs/test-discord-server.md](docs/test-discord-server.md) describes. Turn on the bot's **Message Content Intent** (without it no `!` command is seen) and put its token in `DISCORD_TOKEN`. Never the production token.
6. **OpenAI.** Any non-empty `OPENAI_API_KEY` lets the bot start; receipt scanning and Chismebot summaries need a real key.
7. **Run.** `make run`. It first checks `.env` and refuses to start, listing each problem, when `SUPABASE_URL` is not the local stack, a required secret is empty, or `TRANSPORTATION_CHANNEL_ID` is not a whole number. The console then shows `✅ Logged in as <your test bot>`. In the test server, `!cholobots` answers with the persona overview, and `!cshow Olive` shows a seeded contact card.
8. **Day to day.** `make check` (compile, `ruff` and the tests) before you push, `make test-integration` for the integration tests against the local stack, `make db-down` when you are done. Every pull request runs the same checks in GitHub Actions: `checks` (what `make check` runs) and `integration` (what `make test-integration` runs, on a throwaway local stack).

**Several worktrees at once.** Run `make db-up` in each worktree (or `scripts/supabase-local.sh start`): every worktree gets its own stack on its own ports, so they do not collide. `make db-env` prints this checkout's `SUPABASE_URL` and key.

Two local quirks: your test accounts are not in the bot's hardcoded user map, so Metichebot files them under Heaven's daily tasks (harmless locally, and the seed relies on it, see [Seed data](#seed-data)), and once `!mtoday` is set up in a channel, any message there that does not start with `!` becomes a time block.

Required variables: `DISCORD_TOKEN`, `OPENAI_API_KEY`, `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`. The full list, including optional variables and which ones crash the process at import, is in [CLAUDE.md](CLAUDE.md).

**Without make.** The raw commands work too: `uv sync --locked && .venv/bin/pre-commit install`, `npx --yes supabase@2.121.0 start` / `db reset` / `status -o env` / `stop`, and the static checks `.venv/bin/python -m py_compile cholobots.py config.py db/database.py bots/*.py` `.venv/bin/ruff check .` and `uv run --frozen ty check` (or activate the environment first with `. .venv/bin/activate`). `.venv/bin/python cholobots.py` skips the local-stack check that `make run` does, so double-check `.env` first.

### Devcontainer

`.devcontainer/` builds one container with Python 3.14, the Supabase CLI 2.121.0, `gh`, Node 22, `psql`, Chrome (amd64 only) and its own Docker daemon (Docker-in-Docker). Each container runs a private Supabase stack on the default ports, so containers on one machine do not collide with each other or with a stack on the host.

**Pre-built image.** `scripts/devcontainer/launch.sh` pulls `ghcr.io/mrsbohemian/cholobots/devcontainer:latest` instead of building. It adds the volatile tools (`claude`, `codex`, `chrome-devtools-mcp`, `uv`, and the pinned `pytaiga-mcp` in `/opt/pytaiga-mcp`) and a warm uv cache to the base image. Set `GH_TOKEN_FOR_PACKAGES` to a classic PAT with `read:packages` (keep it in your credentials vault, never in the repo); `launch.sh` falls back to `GH_TOKEN`, then `GITHUB_TOKEN`. Without pull access, or on Apple Silicon (the image is amd64 only), `launch.sh` builds both stages locally, which is slow, and tags them per workspace (`cholobots-devcontainer:local-<key>`). VS Code "Reopen in Container" builds only the base image, so it has no `claude` or `codex`.

**Bumping a pin.** Every tool version is declared once: an `ARG` in `.devcontainer/Dockerfile.prebuild`, or the commit in `tools/pytaiga-mcp/install.sh`. `scripts/devcontainer/pins.sh` prints them all. Edit that one place and merge to `main`; `.github/workflows/build-devcontainer.yml` then rebuilds and publishes `:base`, `:latest` and `:<sha>`. Pull requests, the weekly schedule and manual dispatch only build and check the installed versions against the pins.

Prerequisites: Docker, and either the pinned CLI or VS Code with the Dev Containers extension ("Reopen in Container"):

```bash
npx --yes @devcontainers/cli@0.89.0 up --workspace-folder .
npx --yes @devcontainers/cli@0.89.0 exec --workspace-folder . python -m pytest tests/unit tests/scripts -q
```

Host environment it reads (all optional for starting the container; the bot needs the first two):

- `CHOLOBOTS_TEST_DISCORD_TOKEN`: the **test** application token from [docs/test-discord-server.md](docs/test-discord-server.md). It becomes `DISCORD_TOKEN` inside the container. Never use the production token.
- `OPENAI_API_KEY`, `TAIGA_API_URL`, `TAIGA_USERNAME`, `TAIGA_PASSWORD`, `GH_TOKEN`.

What happens:

- **Create (once).** `post-create.sh` builds `.venv`, a Docker volume separate from any `.venv` on the host, and runs `uv sync --locked` into it (installing the pinned uv first on a base-only image that lacks it).
- **Every start.** `post-start.sh` waits for the Docker daemon, runs `supabase start`, then **`supabase db reset`: fresh migrations and seed data on every container start, so stop the bot first and expect local edits to the data to be gone.** It then runs `scripts/devcontainer/write_env.py`, which sets `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` to the in-container stack and `DISCORD_TOKEN` and `OPENAI_API_KEY` to the passed values, and leaves every other `.env` line alone. `.env` is the same file as in the host checkout. The script never starts the bot.
- **Slow or odd hosts.** On a host kernel with nftables only, `post-start.sh` switches `iptables` and `ip6tables` to the nft backend and restarts `dockerd`. After a container restart the stack needs a moment to become healthy, so `supabase start` and `db reset` are retried. Tunables, set on the host or in the container: `DOCKER_WAIT_LIMIT` (seconds to wait for dockerd, default 60), `SUPABASE_RETRY_ATTEMPTS` (default 30) and `SUPABASE_RETRY_DELAY` (seconds, default 2).
- **Ports.** VS Code forwards 54321 (API) and 54323 (Studio); the headless CLI forwards nothing.
- **Memory.** About 1 GB per container for the Supabase stack. `post-start.sh` skips `imgproxy`, `logflare`, `vector` and `mailpit`; set `SUPABASE_START_EXCLUDE=studio,imgproxy,...` to trim more, or `SUPABASE_START_EXCLUDE=` (empty) to start everything.
- **Registry mirror.** `/etc/docker/daemon.json` points at `host.docker.internal:6000`; without a mirror running, Docker pulls from Docker Hub (`scripts/devcontainer/start-registry-mirror.sh` starts one). The pre-built image sets `SUPABASE_INTERNAL_IMAGE_REGISTRY=docker.io` so the Supabase images are pulled from Docker Hub and the mirror can cache them; they are not baked into the image. Known limitation: on Linux hosts the container cannot reach the host mirror because `host.docker.internal` is not mapped, so the Supabase images come straight from Docker Hub there (Taiga bug #202).
- **Headless commands.** Inside the container the `make` targets use the installed CLI (`SUPABASE=supabase` is set).

Safety: nothing in the container talks to the hosted Supabase project. Never put hosted credentials or the production Discord token in the container's environment or in `.env`. As in the Quickstart, test accounts fall back to Heaven's `daily_tasks` key, which is harmless in the local database.

### Seed data

`supabase/seed.sql` loads one obviously fictitious contracting business, and `make db-reset` (`supabase db reset`) runs it after the migrations (`[db.seed]` in `supabase/config.toml`). The reset is one command against the local Docker stack only, never with `--linked`:

```bash
make db-reset    # or: npx --yes supabase@2.121.0 db reset
```

It takes about 30 s. Stop the bot first: its in-memory sessions would point at rows the reset deleted.

What it seeds:

- Chismebot: `Fern Fridgeman (Test)` (Fridge), `Stella Stovetop (Test)` (Stovetop, 80 degrees) and `Olive Ovenmitt (Test)` (Oven, burner 1) with a journal and notes. `chisme_dashboard_contacts` is a view, so it fills from these contacts.
- Crudobot: two projects for Olive (`Olive kitchen faucet (Test)`, `Olive deck rail (Test)`) with tasks, labor entries and material purchases.
- Guardabot: garage zones `Electrical Shelf (Test)` and `Plumbing Bin (Test)` with wire nuts and PEX elbows.
- Metichebot: this week's `metiche_weekly` row and three `daily_tasks`.
- Obi Juan: quest `test-gate-latch` with one assignment. Vueltabot: `Used Ladder (Test)` and a buyer interest in a ladder.

`!mtoday` works out of the box. There is no profiles table yet, so any test account reaches Metichebot as `UNKNOWN:<id>`, which reads the `heaven` calendar key, and the seed puts today's tasks under that key. "Today" is fixed at reset time in America/Chicago, so run the reset again on a new day.

Editing the seed: names end in ` (Test)`, phones are `210555 01xx`, emails are `@example.com`, Discord IDs are the placeholders `100000000000000001` to `100000000000000004`, and real customers or the production Discord IDs never appear. `tests/unit/test_seed_contract.py` enforces these rules.

If a reset fails on a seed statement, the seed has drifted from the migrations: fix the seed.

### Linking to the hosted project (owner only)

Owner only, once per machine. `supabase login` stores the access token in the CLI's own home-directory or keychain storage (or export `SUPABASE_ACCESS_TOKEN` through direnv); it never goes in the repo. Then `supabase link --project-ref <hosted project ref>` (it prompts for the database password). The link state lives in `supabase/.temp/`, which is git-ignored. Do not run `supabase db push` or any other write against the hosted project; migrations reach it only through the gated workflow in [docs/database-migrations.md](docs/database-migrations.md).

## Running the bot

`make run` checks that `.env` points at the local Supabase stack (`scripts/check_local_env.py`), then starts the bot; `python cholobots.py` skips the check. Either way it connects to Discord with the token in `.env`, so use your own test bot and never point a local `.env` at the hosted Supabase project or the production Discord token. Coding agents never start the bot.

To browse the dashboards locally: `python -m http.server 8000 --directory mobile` (open http://localhost:8000/).

## Dependencies

Dependencies are declared in `pyproject.toml` (runtime `dependencies`, dev tools in the `dev` group) and locked in `uv.lock`. `make setup` and CI install them with `uv sync --locked`, which fails when the lock is stale.

To add or upgrade a package, edit its `==` pin in `pyproject.toml` (or `uv add <pkg>==<ver>`, `uv add --dev <pkg>==<ver>`), or for a transitive package run `uv lock --upgrade-package <pkg>`. Then run `make lock` and commit `pyproject.toml`, `uv.lock` and `requirements.txt` in one pull request. CI's `make lock-check` fails otherwise. Use the pinned uv version so the lock does not churn. ty is pinned in the dev group, so a ty upgrade goes in its own pull request that fixes every diagnostic the new version reports. A pyrefly upgrade is also its own pull request; it does not have to clear the errors, but the PR notes the count change. When ruff changes, bump the `rev` in `.pre-commit-config.yaml` too; `tests/unit/test_dependency_contract.py` checks it.

## Production

Railway (Heaven's account) deploys the `main` branch with the `Procfile` (`worker: python cholobots.py`) and the Python from `.python-version`. Its build installs either `requirements.txt` with pip or `uv.lock` with uv; both hold the same locked tree. `requirements.txt` is generated by `make lock`: never edit it by hand, and do not delete it while Railway may build from it.

## Resetting data

`make db-reset` (or `npx --yes supabase@2.121.0 db reset`) rebuilds the local database from `supabase/migrations/` and reloads the fictitious [seed data](#seed-data). It only touches the local Docker stack. Stop the bot first.

## Running tests

Pick what to run from [docs/testing-workflow.md](docs/testing-workflow.md); which level a test belongs at is in [docs/testing-strategy.md](docs/testing-strategy.md).

| Command | Runs |
|---|---|
| `make check` | compile check, `ruff check .`, `make typecheck`, `make lock-check`, then `make test` |
| `make typecheck` | `ty check` on the whole project; any diagnostic fails |
| `make typecheck-pyrefly` | pyrefly (spike #216), informational: reports ty's blind spots, never part of `make check`, non-failing step in CI; configured in `pyproject.toml` `[tool.pyrefly]` |
| `make test` | `pytest tests/unit tests/scripts` and every `tests/scripts/test-*.sh` |
| `make test-integration` | `tests/integration` against the local stack (`make db-up` first); refuses any non-local Supabase URL |
| `python -m pytest tests/unit -q` | fast contract and guard tests; no network, no Supabase |
| `python -m pytest tests/scripts -q` | tests for the pipeline scripts in `scripts/` |
| `python -m py_compile cholobots.py config.py db/database.py bots/*.py` | compile check of the bot modules |
| `ruff check .` | lint (configured in `ruff.toml`) |
| `uv run --frozen ty check` | type check, any diagnostic fails (configured in `pyproject.toml` `[tool.ty]`; also the pre-commit hook) |

The bots' command logic has no unit tests yet; `tests/integration` drives commands against the local stack.

## Work tracking

Issues, stories and epics live in the Taiga project `cholo-bots` (id 39) at <https://backlog.growthgarage.org/project/cholo-bots>. Start with epic #2 (the codebase assessment, with per-bot deep dives in the project wiki), epic #46 (developer experience: local Supabase stack, migrations, CI) and epic #3 (multi-business isolation). The roadmap is [Cholobots_48_Hour_Development_Plan.md](Cholobots_48_Hour_Development_Plan.md).

## Development environment

**Secrets stay out of the repo.** `.env`, `.envrc` and `.envrc.local` are git-ignored; never commit any of them. The bot's variables live in `.env`, which wins over anything your shell or direnv exports. A [direnv](https://direnv.net/) `.envrc` is for shell-only variables, such as the `TAIGA_*` credentials the pipeline scripts read: it can `dotenv_if_exists .env.local` or export from an encrypted store (Growth Garage collaborators use a SOPS/age vault, see [scripts/README.md](scripts/README.md#credentials) for the exact lines), so nothing secret is written to disk in plaintext inside the checkout. Run `direnv allow` after creating or changing it.

**Schema changes.** Never edit the database schema in the Supabase dashboard or SQL editor. Every change is a new file under `supabase/migrations/` (`supabase migration new <name>`) reviewed in a PR; the production push is gated off. See [docs/database-migrations.md](docs/database-migrations.md).

**Claude Code and Codex.** `CLAUDE.md` holds the architecture notes a session needs; Codex reads it through the `AGENTS.md` symlink. To let Claude read and update Taiga from this checkout, register the Taiga MCP server at project scope so it targets the Growth Garage instance rather than any personal default:

```bash
claude mcp add -s local taiga-py -- <path-to-taiga-mcp-launcher> workflow project/taiga-growthgarage-slave1-codex
```

The launcher reads `TAIGA_API_URL`, `TAIGA_USERNAME` and `TAIGA_PASSWORD` from the encrypted store; nothing is stored in the repo. Verify with a read-only `list_projects` call before filing anything.

**Agent pipeline.** `.claude/` contains the skills, agents and shared includes that let Claude Code take a Taiga story or bug through analysis, implementation, PR, review and merge; `.claude/autonomous-pipeline.md` is the overview and `.claude/skills/shared/taiga-project.md` pins this project's Taiga configuration. The supporting scripts live in `scripts/` (see `scripts/README.md`) with tests in `tests/scripts/`:

```bash
make setup   # or: uv sync --locked && .venv/bin/pre-commit install (pytest, ruff and the ruff-check commit hook)
.venv/bin/python -m pytest tests/scripts -q
.venv/bin/python scripts/taiga_project_setup.py --check   # exit 0 when the Taiga project has the statuses and tags the pipeline needs
```

The setup script is run once by a project admin with `--apply`. Like the other helper scripts it reads the `TAIGA_*` variables from the shell, which direnv loads from Jon's dev-env vault; `scripts/README.md` has the admin command.

## Contributing

Access, the branch and pull-request flow, database changes, the command checklist and commit attribution are in [CONTRIBUTING.md](CONTRIBUTING.md). Ownership and licensing terms are still pending. Architecture notes and known hazards are in [CLAUDE.md](CLAUDE.md).

## Layout

- `cholobots.py` — the single `discord.py` bot; registers every persona and the attachment gate
- `bots/` — one module per persona
- `db/database.py` — legacy SQLite helpers plus the Supabase `metiche_weekly` accessors
- `config.py` — env loading and the shared OpenAI client
- `mobile/` — static dashboards, published to GitHub Pages by `.github/workflows/pages.yml`. For a page's full history use `git log -- mobile/<file> <file>` (commits before the subtree import use the old path).
- `supabase/` — Supabase CLI project: `config.toml` for the local stack and `migrations/`, the database schema of record, starting from a baseline dumped from the hosted project
- `Makefile` — setup, local stack, run, check and test targets (`make help`)
- `tests/` — `unit/` (no network), `integration/` (local Supabase stack), `scripts/` (pipeline script tests)
- `scripts/` — Taiga and GitHub helper scripts for the agent pipeline, plus `check_local_env.py` and the schema checks
- `.claude/` — Claude Code skills, agents and shared includes for the agent pipeline
- `CLAUDE.md` — architecture notes for people and coding agents; `AGENTS.md` is a symlink to it
- `CONTRIBUTING.md` — how changes get to `main`
- `docs/` — assessments and design notes; `Cholobots_48_Hour_Development_Plan.md` is the current roadmap
