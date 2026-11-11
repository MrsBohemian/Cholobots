# Cholobots dashboards (`mobile/`)

Static, phone-first dashboards for the [Cholobots](https://github.com/MrsBohemian/Cholobots) Discord bot. Each page is a single self-contained HTML file that reads the same Supabase project the bot writes to. Published at https://mrsbohemian.github.io/Cholobots/.

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

Any static file server works. From the repo root:

```bash
python -m http.server 8000 --directory mobile
# open http://localhost:8000/
```

Opening the files directly with `file://` also works; the pages use plain scripts and the Supabase client over HTTPS, so no server-side behaviour is involved.

Every page declares its own `SUPABASE_URL` and `SUPABASE_ANON_KEY` constants near the top of its `<script>` block, so each page connects on its own; there is no shared config file. The key is Supabase's publishable key, which is designed to be shipped to browsers. It only protects data if Row Level Security policies exist on the tables; see the Cholobots repo's assessment and Taiga epic #3 (multi-business isolation) for the current state.

## Development environment

See the repo's [README](../README.md).

## Layout

- `index.html` — landing menu
- `*.html` — one dashboard per page, each with inline CSS, markup and script
- `assets/` — icons for the menu tiles and the garage map image
- `assets/cholo-icon2.png` — the app icon
