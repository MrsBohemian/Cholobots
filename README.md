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

## Running locally

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # once it exists; see the env list in CLAUDE.md
python cholobots.py
```

Required: `DISCORD_TOKEN`, `OPENAI_API_KEY`, `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`. The full list, including optional variables and which ones crash the process at import, is in [CLAUDE.md](CLAUDE.md). `.env` is loaded with `override=True`, so it wins over your shell. There is no test suite yet; `python -m py_compile cholobots.py config.py db/database.py bots/*.py` is the only static check.

## Development environment

**Secrets stay out of the repo.** `.env`, `.envrc` and `.envrc.local` are git-ignored. The recommended pattern is [direnv](https://direnv.net/): a project `.envrc` that exports variables from an encrypted store (Growth Garage collaborators use a SOPS/age vault with `scripts/envtool export project/<name>`), so nothing secret is ever written to disk in plaintext inside the checkout. Run `direnv allow` after creating or changing it.

**Work tracking.** Issues, stories and epics live in the Taiga project `cholo-bots` on <https://backlog.growthgarage.org>. Start with epic #2 (the 2026-11-06 codebase assessment, with per-bot deep dives in the project wiki), epic #46 (developer experience: local Supabase stack, migrations, CI) and epic #3 (multi-business isolation).

**Claude Code.** `CLAUDE.md` holds the architecture notes a session needs. To let Claude read and update Taiga from this checkout, register the Taiga MCP server at project scope so it targets the Growth Garage instance rather than any personal default:

```bash
claude mcp add -s local taiga-py -- <path-to-taiga-mcp-launcher> workflow project/growthgarage
```

The launcher reads `TAIGA_API_URL`, `TAIGA_USERNAME` and `TAIGA_PASSWORD` from the encrypted store; nothing is stored in the repo. Verify with a read-only `list_projects` call before filing anything.

**Agent pipeline.** `.claude/` contains the skills, agents and shared includes that let Claude Code take a Taiga story or bug through analysis, implementation, PR, review and merge; `.claude/autonomous-pipeline.md` is the overview and `.claude/skills/shared/taiga-project.md` pins this project's Taiga configuration. The supporting scripts live in `scripts/` (see `scripts/README.md`) with tests in `tests/scripts/`:

```bash
pip install -r requirements-dev.txt
python -m pytest tests/scripts -q
python3 scripts/taiga_project_setup.py --check   # exit 0 when the Taiga project has the statuses and tags the pipeline needs
```

The setup script is run once by a project admin with `--apply`; it reads credentials from `--env-file scripts/taiga-auth.env` (copy `scripts/taiga-auth.env.example`), the `TAIGA_*` shell variables, or an interactive prompt.

**Contributing.** Branch from `main`, open a pull request against `MrsBohemian/Cholobots`, keep each change in its own commit. Pipeline branches are pushed to the upstream repo directly so stacked pull requests work; the `Alterna-Dev-Studio` fork is a mirror. The access model and licensing are being settled under Taiga story #58.

## Layout

- `cholobots.py` — the single `discord.py` bot; registers every persona and the attachment gate
- `bots/` — one module per persona
- `db/database.py` — legacy SQLite helpers plus the Supabase `metiche_weekly` accessors
- `config.py` — env loading and the shared OpenAI client
- `docs/` — assessments and design notes; `Cholobots_48_Hour_Development_Plan.md` is the current roadmap
