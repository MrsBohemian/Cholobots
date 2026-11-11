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

Data lives in Supabase. The companion repo [Cholobots-Mobile](https://github.com/MrsBohemian/Cholobots-Mobile) is a set of static dashboards that read the same Supabase project.

## Local development

Run the bot against a Supabase stack on your own machine, never against the hosted project.

1. **Prerequisites.**
   - Python: `python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt`
   - Docker: Docker Engine on Linux; Docker Desktop (or OrbStack) on macOS; on Windows use WSL2 with Docker Desktop's WSL integration and run everything from the WSL shell.
   - Supabase CLI 2.121.0: via Node 18+ (`alias supabase='npx --yes supabase@2.121.0'`), or Homebrew (`brew install supabase/tap/supabase`) or Scoop. `supabase --version` should print 2.121.0.
2. **Start the stack.** `supabase start` (the first run pulls images and takes several minutes). It runs Postgres, Auth, Storage, the REST API and Studio in Docker and prints the API URL (`http://127.0.0.1:54321`), Studio (`http://127.0.0.1:54323`) and the keys; `supabase status -o env` prints them again. Edge Functions are off in `supabase/config.toml` because the repo has none.
3. **Write `.env`** in the repo root with `SUPABASE_URL=<API_URL>` and `SUPABASE_SERVICE_ROLE_KEY=<SERVICE_ROLE_KEY>` copied from `supabase status -o env`, plus `DISCORD_TOKEN` (a **test** bot token, never the production one) and `OPENAI_API_KEY`. `.env` is loaded with `override=True`, so it silently wins over your shell: keep the hosted URL, the hosted service-role key and the production Discord token out of it. A wrong or empty value shows up as a `create_client` error when the process starts.
4. **Know what is in the database.** The local database has the baseline schema (`supabase/migrations/`) and no data, so commands that read rows find nothing. `supabase db reset` rebuilds it from the migrations.
5. **Run.** `python cholobots.py` (only with the test token), and `supabase stop` when you are done.

Required variables: `DISCORD_TOKEN`, `OPENAI_API_KEY`, `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`. The full list, including optional variables and which ones crash the process at import, is in [CLAUDE.md](CLAUDE.md). There is no test suite yet; `python -m py_compile cholobots.py config.py db/database.py bots/*.py` is the only static check.

### Linking to the hosted project (owner only)

Owner only, once per machine. `supabase login` stores the access token in the CLI's own home-directory or keychain storage (or export `SUPABASE_ACCESS_TOKEN` through direnv); it never goes in the repo. Then `supabase link --project-ref <hosted project ref>` (it prompts for the database password). The link state lives in `supabase/.temp/`, which is git-ignored. Do not run `supabase db push` or any other write against the hosted project.

## Development environment

**Secrets stay out of the repo.** `.env`, `.envrc` and `.envrc.local` are git-ignored. The recommended pattern is [direnv](https://direnv.net/): a project `.envrc` that exports variables from an encrypted store (Growth Garage collaborators use a SOPS/age vault with `scripts/envtool export project/<name>`), so nothing secret is ever written to disk in plaintext inside the checkout. Run `direnv allow` after creating or changing it.

**Schema changes.** Never edit the database schema in the Supabase dashboard or SQL editor. Every change is a new file under `supabase/migrations/` (`supabase migration new <name>`) reviewed in a PR; the production push is gated off. See [docs/database-migrations.md](docs/database-migrations.md).

**Work tracking.** Issues, stories and epics live in the Taiga project `cholo-bots` on <https://backlog.growthgarage.org>. Start with epic #2 (the 2026-11-06 codebase assessment, with per-bot deep dives in the project wiki), epic #46 (developer experience: local Supabase stack, migrations, CI) and epic #3 (multi-business isolation).

**Claude Code.** `CLAUDE.md` holds the architecture notes a session needs. To let Claude read and update Taiga from this checkout, register the Taiga MCP server at project scope so it targets the Growth Garage instance rather than any personal default:

```bash
claude mcp add -s local taiga-py -- <path-to-taiga-mcp-launcher> workflow project/taiga-growthgarage-slave1-codex
```

The launcher reads `TAIGA_API_URL`, `TAIGA_USERNAME` and `TAIGA_PASSWORD` from the encrypted store; nothing is stored in the repo. Verify with a read-only `list_projects` call before filing anything.

**Agent pipeline.** `.claude/` contains the skills, agents and shared includes that let Claude Code take a Taiga story or bug through analysis, implementation, PR, review and merge; `.claude/autonomous-pipeline.md` is the overview and `.claude/skills/shared/taiga-project.md` pins this project's Taiga configuration. The supporting scripts live in `scripts/` (see `scripts/README.md`) with tests in `tests/scripts/`:

```bash
pip install -r requirements-dev.txt
python -m pytest tests/scripts -q
python3 scripts/taiga_project_setup.py --check   # exit 0 when the Taiga project has the statuses and tags the pipeline needs
```

The setup script is run once by a project admin with `--apply`. Like the other helper scripts it reads the `TAIGA_*` variables from the shell, which direnv loads from Jon's dev-env vault; `scripts/README.md` has the admin command.

**Contributing.** Branch from `main`, open a pull request against `MrsBohemian/Cholobots`, keep each change in its own commit. Pipeline branches are pushed to the upstream repo directly so stacked pull requests work; the `Alterna-Dev-Studio` fork is a mirror. The access model and licensing are being settled under Taiga story #58.

## Layout

- `cholobots.py` — the single `discord.py` bot; registers every persona and the attachment gate
- `bots/` — one module per persona
- `db/database.py` — legacy SQLite helpers plus the Supabase `metiche_weekly` accessors
- `config.py` — env loading and the shared OpenAI client
- `supabase/` — Supabase CLI project: `config.toml` for the local stack and `migrations/`, the database schema of record, starting from a baseline dumped from the hosted project
- `docs/` — assessments and design notes; `Cholobots_48_Hour_Development_Plan.md` is the current roadmap
