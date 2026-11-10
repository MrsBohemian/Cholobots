# Cholobots-Mobile

Static, phone-first dashboards for the [Cholobots](https://github.com/MrsBohemian/Cholobots) Discord bot. Each page is a single self-contained HTML file that reads the same Supabase project the bot writes to.

| Page | Title | Reads |
|---|---|---|
| `index.html` | Handley Man Cholobots | menu linking to the pages below |
| `money.html` | Financial Engine | Metichebot's weekly money plan (`metiche_weekly`, `metiche_important_items`) |
| `jobcost.html` | Crudobot Job Costing | `crudo_projects`, tasks, labor entries, material transactions, quest assignments |
| `customers.html` | Chismebot Customer Cards | `chisme_*` contacts, notes, journals plus linked Crudo projects |
| `inventory.html` | Guardabot Garage Map | `inventory_items` by `garage_zones` |
| `time.html` | Time | weekly time blocks from `metiche_weekly` |
| `loadbar.html` | Task Progress | `daily_tasks`, `metiche_project_tasks`, active customers |

There is no build step, framework or package manager. The only dependency is `@supabase/supabase-js@2` from the jsDelivr CDN, loaded with a `<script>` tag in each page.

## Running locally

Any static file server works. From the checkout:

```bash
python -m http.server 8000
# open http://localhost:8000/
```

Opening the files directly with `file://` also works; the pages use plain scripts and the Supabase client over HTTPS, so no server-side behaviour is involved.

Every page declares its own `SUPABASE_URL` and `SUPABASE_ANON_KEY` constants near the top of its `<script>` block, so each page connects on its own; there is no shared config file. The key is Supabase's publishable key, which is designed to be shipped to browsers. It only protects data if Row Level Security policies exist on the tables; see the Cholobots repo's assessment and Taiga epic #3 (multi-business isolation) for the current state.

## Development environment

**Secrets stay out of the repo.** The publishable key in these pages is public by design, but nothing more sensitive belongs here. If a page ever needs a service-role key or any other secret, it has to move behind a server; do not commit it. For local tooling, Growth Garage collaborators use [direnv](https://direnv.net/) with a project `.envrc` that exports variables from an encrypted SOPS/age vault (`scripts/envtool export project/<name>`). `.env`, `.envrc` and `.envrc.local` are git-ignored.

**Work tracking.** Issues, stories and epics for both repos live in the Taiga project `cholo-bots` on <https://backlog.growthgarage.org>. The mobile dashboards are covered by the `deep-dive-mobile` wiki page there, and by epic #2 (the 2026-11-06 codebase assessment), epic #46 (developer experience) and epic #3 (multi-business isolation).

**Claude Code.** To let Claude read and update Taiga from this checkout, register the Taiga MCP server at project scope so it targets the Growth Garage instance rather than any personal default:

```bash
claude mcp add -s local taiga-py -- <path-to-taiga-mcp-launcher> workflow project/taiga-growthgarage-slave1-codex
```

The launcher reads `TAIGA_API_URL`, `TAIGA_USERNAME` and `TAIGA_PASSWORD` from the encrypted store; nothing is stored in the repo. Verify with a read-only `list_projects` call before filing anything.

**Contributing.** Direct pushes to `MrsBohemian/Cholobots-Mobile` are restricted. Fork, branch, and open a pull request; keep each change in its own commit. The access model and licensing are being settled under Taiga story #58.

## Layout

- `index.html` — landing menu
- `*.html` — one dashboard per page, each with inline CSS, markup and script
- `assets/` — icons for the menu tiles and the garage map image
- `assets/cholo-icon2.png` — the app icon
