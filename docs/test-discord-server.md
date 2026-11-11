# Test Discord server

How to run Cholobots against a separate Discord server and the local Supabase stack, so commands can be tried without touching production.

## 1. Discord application (one-time, Jon)

1. In the Discord Developer Portal, create a separate application and bot (not the production one).
2. Under Bot, enable the privileged **Message Content Intent**. `cholobots.py` sets `intents.message_content = True`; without the toggle no `!` command is seen.
3. OAuth2 URL: scope `bot`, permissions Send Messages, Read Message History, Embed Links, Attach Files, Add Reactions, View Channels. Open it and add the bot to the test server.
4. Store the token in Jon's dev-env vault (`~/work/utils/jon-dev-env`), never in the repo. Bring it into `.env` or the shell through direnv, as described in README "Development environment".

## 2. Server layout

| Channel | Purpose | Id |
|---|---|---|
| `#general` | everyday commands | TODO (fill in) |
| `#metiche-time` | `!mtoday` time sessions | TODO (fill in) |
| `#transportation` | Vueltabot trips; its id goes in `TRANSPORTATION_CHANNEL_ID` | TODO (fill in) |
| `#receipts` | Crudobot receipt flow | TODO (fill in) |

Server id: TODO (fill in). Ids are not secrets.

## 3. Test accounts

One account per seed person. List each Discord id next to the seed row it maps to:

| Person (seed row) | Discord id |
|---|---|
| TODO | TODO |

`DISCORD_USER_TO_PERSON` (`bots/metichebot.py:120`) only knows the four production ids, so test accounts become `UNKNOWN:<id>` and Metichebot falls back to Heaven's `daily_tasks` key. That is harmless locally.

## 4. Rules

- Never invite the production bot to the test server. Two bots would answer every command, one against production data.
- Never put the production token or the hosted Supabase URL or key in a local `.env`.

## 5. Run it

> **Steps 1 and 2 need `supabase/config.toml` (#47), the schema migrations (#32) and seed data (#48).** Without them, skip to step 3 and point `SUPABASE_URL` and the key at a non-production Supabase project you own, never the hosted production project.

1. `supabase start`
2. `supabase db reset`
3. `cp .env.example .env`, then fill `DISCORD_TOKEN`, `OPENAI_API_KEY` and `SUPABASE_SERVICE_ROLE_KEY` (the `SERVICE_ROLE_KEY` line of `supabase status -o env`).
4. `python cholobots.py`
5. Expect these console lines: `✅ ObiJuan loaded`, `✅ Vueltabot loaded`, `✅ Logged in as ...`.
6. Smoke check in `#general`: `!cshow <seeded contact>`.

## 6. Troubleshooting

- `RuntimeError: DISCORD_TOKEN is missing` or `OPENAI_API_KEY is missing`: set them in `.env` (`config.py`).
- Supabase client errors at import: `SUPABASE_URL` or the key is empty. Chismebot, Guardabot and Crudobot create their client unguarded.
- `ValueError` from an empty `TRANSPORTATION_CHANNEL_ID` ends `on_ready` early, so no pings. Use an integer (`0` if unused).
- Commands ignored: the Message Content Intent is off.
- A value you exported in the shell is ignored: `.env` wins (`load_dotenv(override=True)`).

## 7. Listener caveat for testers

Once `!mtoday` is set up in a channel, any non-`!` text there from any account becomes a time block. Use a separate channel for other flows.
