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

## Quickstart

From a fresh clone to the bot answering in your own test Discord server. Everything runs against a Supabase stack on your machine, never the hosted project. `make help` lists every target.

1. **Prerequisites.** git, GNU make, Python 3.14 (the version in `.python-version`), Docker running (Docker Engine on Linux, Docker Desktop or OrbStack on macOS) and Node 18+ for `npx`. On Windows, use WSL2 with Docker Desktop's WSL integration and run everything from the WSL shell. The Makefile runs the Supabase CLI as `npx --yes supabase@2.121.0`; if you installed that same version with Homebrew (`brew install supabase/tap/supabase`) or Scoop, add `SUPABASE=supabase` to each `make db-*` command.
2. **Install.**
   ```bash
   git clone https://github.com/MrsBohemian/Cholobots.git && cd Cholobots
   make setup
   ```
   This creates `.venv`, installs the pinned requirements and the pre-commit hook, and copies [.env.example](.env.example) to `.env` (an existing `.env` is left alone).
3. **Start the database.** `make db-up` (the first run pulls images and takes several minutes; Studio is at `http://127.0.0.1:54323`), then `make db-reset` to rebuild it from `supabase/migrations/` and `supabase/seed.sql`. Until the seed data lands (story #48) the tables exist but are empty, so commands that read rows find nothing.
4. **Point `.env` at it.** `make db-env` prints a `SUPABASE_URL=` and a `SUPABASE_SERVICE_ROLE_KEY=` line; paste both into `.env`, replacing the lines already there.
5. **Discord.** Create your own test application, bot and server as [docs/test-discord-server.md](docs/test-discord-server.md) describes, so nobody has to invite you. Turn on the bot's **Message Content Intent** (without it no `!` command is seen) and put its token in `DISCORD_TOKEN`. Never the production token.
6. **OpenAI.** Any non-empty `OPENAI_API_KEY` lets the bot start; receipt scanning and Chismebot summaries need a real key.
7. **Run.** `make run`. It first checks `.env` and refuses to start, listing each problem, when `SUPABASE_URL` is not the local stack, a required secret is empty, or `TRANSPORTATION_CHANNEL_ID` is not a whole number. The console then shows `✅ Logged in as <your test bot>`. In the test server, `!cholobots` answers with the persona overview, and `!cshow <a seeded contact>` shows a contact card once the seed has landed.
8. **Day to day.** `make check` (compile, `ruff`, unit tests, script pytest tests and the `test-*.sh` shell tests) before you push, `make test-integration` once story #51 adds those tests, and `make db-down` when you are done.

Two local quirks: your test accounts are not in the bot's hardcoded user map, so Metichebot files them under Heaven's daily tasks (harmless locally; story #40 replaces the map), and once `!mtoday` is set up in a channel, any message there that does not start with `!` becomes a time block.

Required variables: `DISCORD_TOKEN`, `OPENAI_API_KEY`, `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`. The full list, including optional variables and which ones crash the process at import, is in [CLAUDE.md](CLAUDE.md). `.env` is loaded with `override=True`, so it wins over your shell.

**Without make.** The targets are thin wrappers, so the raw commands work too: `python3.14 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt -r requirements-dev.txt && pre-commit install`, `npx --yes supabase@2.121.0 start` / `db reset` / `status -o env` / `stop`, and the static checks `python -m py_compile cholobots.py config.py db/database.py bots/*.py` and `ruff check .`. `python cholobots.py` skips the local-stack check that `make run` does, so double-check `.env` first.

### Linking to the hosted project (owner only)

Owner only, once per machine. `supabase login` stores the access token in the CLI's own home-directory or keychain storage (or export `SUPABASE_ACCESS_TOKEN` through direnv); it never goes in the repo. Then `supabase link --project-ref <hosted project ref>` (it prompts for the database password). The link state lives in `supabase/.temp/`, which is git-ignored. Do not run `supabase db push` or any other write against the hosted project.

## Development environment

**Secrets stay out of the repo.** `.env`, `.envrc` and `.envrc.local` are git-ignored; never commit any of them. The bot's variables live in `.env`, and because `config.py` loads it with `override=True` it wins over anything your shell or direnv exports. A [direnv](https://direnv.net/) `.envrc` is for shell-only variables, such as the `TAIGA_*` credentials the pipeline scripts read: it can `dotenv_if_exists .env.local` or export from an encrypted store (Growth Garage collaborators use a SOPS/age vault, see [scripts/README.md](scripts/README.md#credentials) for the exact lines), so nothing secret is written to disk in plaintext inside the checkout. Run `direnv allow` after creating or changing it.

**Work tracking.** Issues, stories and epics live in the Taiga project `cholo-bots` on <https://backlog.growthgarage.org>. Start with epic #2 (the 2026-11-06 codebase assessment, with per-bot deep dives in the project wiki), epic #46 (developer experience: local Supabase stack, migrations, CI) and epic #3 (multi-business isolation).

**Claude Code.** `CLAUDE.md` holds the architecture notes a session needs. To let Claude read and update Taiga from this checkout, register the Taiga MCP server at project scope so it targets the Growth Garage instance rather than any personal default:

```bash
claude mcp add -s local taiga-py -- <path-to-taiga-mcp-launcher> workflow project/taiga-growthgarage-slave1-codex
```

The launcher reads `TAIGA_API_URL`, `TAIGA_USERNAME` and `TAIGA_PASSWORD` from the encrypted store; nothing is stored in the repo. Verify with a read-only `list_projects` call before filing anything.

**Agent pipeline.** `.claude/` contains the skills, agents and shared includes that let Claude Code take a Taiga story or bug through analysis, implementation, PR, review and merge; `.claude/autonomous-pipeline.md` is the overview and `.claude/skills/shared/taiga-project.md` pins this project's Taiga configuration. The supporting scripts live in `scripts/` (see `scripts/README.md`) with tests in `tests/scripts/`:

```bash
pip install -r requirements-dev.txt && pre-commit install   # pytest, ruff and the ruff-check commit hook
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
- `docs/` — assessments and design notes; `Cholobots_48_Hour_Development_Plan.md` is the current roadmap
