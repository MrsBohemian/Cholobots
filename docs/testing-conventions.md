# Testing Conventions

How tests are laid out, named and wired. Placement and ownership rules are in
`docs/testing-strategy.md`; when to run what is in `docs/testing-workflow.md`.

## Layout and naming

```
tests/
  conftest.py            # shared: env isolation, import guards, clock fixtures
  unit/
    conftest.py          # fake Supabase client, fake OpenAI
    test_crudobot_money.py
  integration/
    conftest.py          # local Supabase client, seed/cleanup, fake Discord context
    test_crudobot_closeout.py
  scripts/               # pipeline script tests (exist today)
```

- Files: `test_<module>_<topic>.py`. Bash script tests: `tests/scripts/test-<script>.sh`.
- Functions: `test_<behaviour>_<condition>`, naming what the code should do, e.g.
  `test_parse_duration_minutes_accepts_hours_and_minutes`.
- One behaviour per test; use `pytest.mark.parametrize` for input tables.
- New `.py` files start with two `# ABOUTME:` lines saying what they cover, matching `tests/scripts/`.

## Importing bot modules safely

Importing the wrong thing has side effects:

- `cholobots.py` calls `bot.run()` at import. **Never import it in a test.**
- `config.py` runs `load_dotenv(override=True)`, so a developer's `.env` beats the test
  environment, and it raises without `DISCORD_TOKEN`/`OPENAI_API_KEY`.
- `bots/chismebot.py`, `bots/guardabot.py` and `bots/crudobot.py` call `create_client` at import;
  `bots/vueltabot.py` does too when loaded.

The root `tests/conftest.py` therefore, before any bot module is imported: sets dummy values for
`DISCORD_TOKEN`, `OPENAI_API_KEY`, `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`; patches
`dotenv.load_dotenv` to a no-op; and patches `supabase.create_client` to return the fake client
(unit) or the local-stack client (integration). Import bot modules inside tests or fixtures, after
those patches, never at the top of a test file.

Module-level state (`*_sessions` dicts, `active_time_sessions`, receipt sessions) leaks between
tests: an autouse fixture clears the dicts the test module touches.

## Faking Supabase (unit level)

The client is synchronous and every call is a builder chain ending in `.execute()`:
`supabase.table("crudo_projects").select("*").eq("id", pid).limit(1).execute().data`.

- Fake at the client boundary with a small `FakeTable` whose builder methods (`select`, `insert`,
  `update`, `delete`, `eq`, `ilike`, `order`, `limit`, ...) record the call and return `self`, and
  whose `execute()` returns an object with `.data` from a canned list.
- Assert on recorded calls (table, filters, payload) and on the function's result, not on how the
  chain was built.
- Do not emulate PostgREST filtering in the fake. If a test needs real filtering, it belongs at the
  integration level.
- Monkeypatch the module attribute (`monkeypatch.setattr(crudobot, "supabase", fake)`), not the
  `supabase` package.

## Integration against local Supabase

- Requires the local stack from story #47 (`supabase start`) and the baseline migration (#32).
  Tests are marked `@pytest.mark.integration` and skip with a clear reason when
  `SUPABASE_URL` does not point at `localhost`/`127.0.0.1`, so production can never be hit.
- Seed through fixtures (story #48) that insert rows and delete them on teardown; give every seeded
  row a unique marker (for example a `test-<uuid>` name) so cleanup cannot touch other data.

## Driving a discord.py command without a gateway

- Commands are closures registered by `register_<bot>(bot)` on a `commands.Bot`. Build a bot with
  `commands.Bot(command_prefix="!", intents=discord.Intents.default())`, call the register
  function, then fetch the command with `bot.get_command("crudoclose")` and await its callback:
  `await cmd.callback(ctx, *args)`. Never call `bot.run()` or `bot.start()`.
- `ctx` is a fake with `author` (with `id`, `name`), `channel` (with `id`), `guild`, and an async
  `send()` that records messages and embeds. `ctx.bot` is the test bot.
- Prompts that call `bot.wait_for("message", check=...)`: monkeypatch `bot.wait_for` with an async
  function that pops scripted replies (fake messages with `content`, `author`, `channel`,
  `attachments=[]`) and applies the `check` to each, so a reply from the wrong user or channel is
  rejected as it would be live.
- Listeners (`on_message`) are tested by calling the registered listener coroutine with a fake
  message. Overlap between listeners stays a manual-test concern until story #23.

## Time and timezones

- Fixtures `chicago_now` and `utc_now` return fixed aware datetimes; provide an evening instant
  (for example 21:30 America/Chicago, which is already tomorrow in UTC) for day-boundary tests.
- Code that reads the clock gets it patched (`monkeypatch.setattr(module, "now_iso", lambda: ...)`
  or a clock argument once story #27 adds one). No `freezegun` unless a story adds it to
  requirements.
- Assert stored timestamps with their offset; never compare naive and aware values.

## Async tests

- Use `pytest-asyncio` (added with story #34) in `auto` mode; command callbacks are plain
  `async def` tests.
- Slow work offloaded with `asyncio.to_thread` runs for real in tests; fake the function it calls.
- Never `asyncio.sleep` for real; patch it or the scheduler interval.

## Other rules

- No network in unit tests: OpenAI calls are faked at `config.client` / the module's client
  attribute; `urllib` calls to `DATA_SERVICE_URL` are patched.
- SQLite (`db/database.py`) tests use a `tmp_path` database via `GUARDABOT_DB`.
- Crudobot's JSON report files are written in the working directory: run those tests with
  `monkeypatch.chdir(tmp_path)`.
