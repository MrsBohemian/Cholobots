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

- `cholobots.py` guards `bot.run()` with `if __name__ == "__main__":`, but importing it still imports
  `config` and every bot module and builds the bot. **Never import it in a test**; read its source
  with `ast` instead (`tests/unit/test_cholobots_entrypoint.py`).
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

- Needs the local stack with migrations and seed (`make db-up`). Run `make test-integration`: it
  exports `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY` and `SUPABASE_DB_URL` from `supabase status`
  (values you already exported win), sets `CHOLOBOTS_INTEGRATION_REQUIRED=1` and runs
  `pytest -m integration tests/integration`.
- `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` are required; `SUPABASE_DB_URL` defaults to
  `postgresql://postgres:postgres@127.0.0.1:54322/postgres`. `tests/integration/local_guard.py`
  refuses any non-loopback host, so the hosted project is never reached. Without the env the tests
  skip; with `CHOLOBOTS_INTEGRATION_REQUIRED=1` a missing env or non-local URL fails the session.
- Before each test the harness truncates every `public` table (`RESTART IDENTITY CASCADE`) over a
  direct Postgres connection and replays `supabase/seed.sql`, so tests do not clean up after
  themselves. Create the rows a test needs with the `factories` fixture; do not rely on seed rows
  beyond what the smoke test pins.
- An autouse fixture monkeypatches the `supabase` attribute of each bot module and `db.database`
  to one local-stack client. Bot modules are imported with dummy `DISCORD_TOKEN`/`OPENAI_API_KEY`,
  `dotenv.load_dotenv` is a no-op, `urllib.request.urlopen` raises, and OpenAI is faked by
  patching `crudobot.parse_receipt_with_openai`.
- `metichebot.local_now` is fixed to 2031-01-15 10:00 America/Chicago so seeded "today" rows never
  collide; call the `chicago_clock` fixture with another datetime to move it.
- Open bugs are strict xfails: `@pytest.mark.xfail(strict=True, raises=AssertionError,
  reason="bug #N open: ...")`. The fix makes the test XPASS and fail, so the fix PR removes the
  marker. `raises=AssertionError` keeps harness errors from being counted as the expected failure.
  Each module also has a non-xfail contract test so a broken harness cannot hide behind xfails.
- Helpers are in `tests/integration/fakes.py`.

## Driving a discord.py command without a gateway

- Commands are closures registered by `register_<bot>(bot)` on a `commands.Bot`. Build a bot with
  `commands.Bot(command_prefix="!", intents=discord.Intents.default())`, call the register
  function, then fetch the command with `bot.get_command("crudoclose")` and await its callback:
  `await cmd.callback(ctx, *args)`. Never call `bot.run()` or `bot.start()`.
- `ctx` is a `FakeContext` (from the `make_ctx(author_id, channel_id)` fixture) with `author`,
  `channel`, `guild` and an async `send()` that records messages and embeds. Vueltabot is a Cog:
  `await bot.add_cog(VueltaBot(bot))`, then `cmd.callback(cog, ctx, ...)`.
- For prompts that call `bot.wait_for("message", check=...)`, `script_replies(bot, ctx, ["done", "7"])`
  replaces `bot.wait_for` with a queue of fake replies and applies `check` to each, so a reply from
  the wrong user or channel is rejected as it would be live. Contract tests assert the returned
  queue is empty. A prompt with no reply left raises `UnscriptedPrompt`, not `TimeoutError`
  (Vueltabot's `ask_user` swallows that) nor `AssertionError` (a strict xfail would). Script
  `fakes.TIMEOUT` to test a timeout path; it is refused when `wait_for` has no timeout.
- Listeners (`on_message`) are tested by calling the coroutine from
  `get_listener(bot, "<function name>")` with a fake message. Overlap between listeners is tested
  manually.

## Time and timezones

- Fixtures `chicago_now` and `utc_now` return fixed aware datetimes; provide an evening instant
  (for example 21:30 America/Chicago, which is already tomorrow in UTC) for day-boundary tests.
- Code that reads the clock gets it patched (`monkeypatch.setattr(module, "now_iso", lambda: ...)`
  or a clock argument once story #27 adds one). No `freezegun` unless a story adds it to
  requirements.
- Assert stored timestamps with their offset; never compare naive and aware values.

## Async tests

- Use `pytest-asyncio`. Integration tests use strict mode (`pytest.mark.asyncio`,
  `pytest_asyncio.fixture`); if a root pytest config sets `asyncio_mode = auto`, follow it.
- Slow work offloaded with `asyncio.to_thread` runs for real in tests; fake the function it calls.
- Never `asyncio.sleep` for real; patch it or the scheduler interval.

## Other rules

- No network in unit tests: OpenAI calls are faked at `config.client` / the module's client
  attribute; `urllib` calls to `DATA_SERVICE_URL` are patched.
- SQLite (`db/database.py`) tests use a `tmp_path` database via `GUARDABOT_DB`.
- Crudobot's JSON report files are written in the working directory: run those tests with
  `monkeypatch.chdir(tmp_path)`.
