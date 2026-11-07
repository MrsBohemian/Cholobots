# Testing Strategy

The target test pyramid for Cholobots: which tests belong at each level, what each level owns, and
the rules that keep levels from duplicating each other. `docs/testing-conventions.md` covers naming,
fixtures and fakes; `docs/testing-workflow.md` covers when to run what.

Most of this pyramid does not exist yet. Stories #34 (unit tests), #47 (local Supabase stack),
#48 (seed fixtures), #51 (integration harness) and #36 (manual scripts) build it. Until a level
exists, the rules in "Before the tests exist" apply.

## Pyramid

| Level | Location | Tooling | Owns |
|---|---|---|---|
| 1. Unit | `tests/unit/` | pytest | Pure helpers: parsers, money and duration helpers, formatters, lookup escaping, session state transitions extracted from handlers |
| 2. Integration | `tests/integration/` | pytest + local Supabase (#47), discord.py test doubles | A command or listener end to end against real tables: what it reads, writes, and replies |
| 3. Scripts | `tests/scripts/` | pytest + bash | Pipeline scripts in `scripts/` (selectors, remediation, merge train, Taiga helpers) |
| 4. Manual | `docs/manual-tests/` | a person or the manual-testing agent in the test Discord server (#31) | What needs a live Discord client: listener overlap, attachments, timing, persona voice, dashboards |

### Unit (`tests/unit/`)

**In scope:** `money()`, `number()`, `parse_duration_minutes()`, `parse_punch_list()`,
`project_progress()`, `format_minutes()` in `bots/crudobot.py`; phone normalisation and contact
matching in chismebot; time-block arithmetic, daily-task mapping and ping scheduling maths in
metichebot; any `ilike` escaping helper (story #30); timestamp helpers (story #27).

**Out of scope:** anything that needs a Supabase row to exist, or a Discord message to arrive.

When the logic you need to test sits inside a handler, extract it into a pure function in the same
module and test that. This is the default move, not a workaround.

### Integration (`tests/integration/`)

**In scope:** one test module per command family. A test seeds rows, invokes the command callback
with a fake context, and asserts on the replies and on the rows written. Examples: `!crudoclose` on
a `paid` project must not reset `closeout_status` (#6); an unknown Discord user must not touch
Heaven's `daily_tasks` (#4); a receipt `yes` from another flow must not save the pending receipt
(#7); a second business must not see the first one's rows (#45).

**Out of scope:** exhaustive input variants (unit level owns them); gateway behaviour.

**Data:** only the local Supabase stack. Never the production project.

### Scripts (`tests/scripts/`)

**In scope:** every script in `scripts/` and `scripts/lib/`: argument parsing, Taiga payloads (with
a fake HTTP layer), git operations against a throwaway repo (`tests/scripts/conftest.py`), exit
codes and stdout contracts.

### Manual (`docs/manual-tests/`)

One script per command (story #36): preconditions, numbered steps (what to type, which account,
which channel), the expected reply, and the rows to check. Manual scripts own behaviour no fake can
show honestly: three `on_message` listeners competing for one reply, the attachment gate, reply
timing and abandoned prompts, persona voice, and the dashboards in `../Cholobots-Mobile` read
alongside the bot.

## Ownership

Each rule has one authoritative level. Other levels get at most one contract check.

| Rule | Authoritative | Contract check allowed at |
|---|---|---|
| Parsing and arithmetic (money, durations, punch lists, phone numbers) | Unit | Integration: one happy and one bad input reaching the reply |
| Which rows a command reads and writes; idempotence; no cross-person or cross-business leak | Integration | Manual: one walkthrough |
| Session flow state (prompt order, cancel, completion) | Unit (extracted state machine) | Integration: one full flow |
| Listener precedence, attachment gate, real timing | Manual | Integration once a dispatcher (story #23) makes it testable |
| Dashboard rendering of bot data | Manual | none |
| Script behaviour | Scripts | none |

## Rules

1. **No production logic reimplementation.** A test calls the production function with specific
   inputs and asserts specific outputs; it never re-derives the expected value with the same
   branching as the code under test.
2. **Intentional duplication only.** The same assertion at three levels means two should go.
3. **Deterministic time.** Tests pass fixed, timezone-aware instants or freeze the clock; they
   never call `datetime.now()`. A test that involves "today" states whether it means
   America/Chicago or UTC and covers the evening hours where the two disagree.
4. **No network below integration.** Unit and script tests touch no Discord, Supabase, OpenAI or
   `DATA_SERVICE_URL`. Integration tests touch only the local Supabase stack; OpenAI is always faked.
5. **Clean output.** A test that exercises an error path captures and asserts the printed or logged
   error instead of letting it leak into the run.

## What not to test

- discord.py itself: command parsing, prefix handling, `wait_for` mechanics, Cog loading.
- Supabase or PostgREST behaviour: filters, ordering, RLS semantics. Test our query's effect on
  seeded rows, not the client.
- OpenAI output quality. Fake the response; test what we do with it, including empty or malformed
  output.
- Exact persona wording. Assert the fact a reply must carry (a total, a name, the choices), not the
  whole Spanglish sentence.

## Before the tests exist

- `python -m py_compile cholobots.py config.py db/database.py bots/*.py` is the floor for every
  change.
- A change to a pure helper adds the first unit test for it in `tests/unit/`, with whatever minimal
  `conftest.py` that needs, even before #34 lands in full.
- A change to handler wiring that cannot be tested yet records manual verification steps on the
  Taiga item and in the PR, and the PR says "no test suite yet (#34)".
- Script changes are always tested in `tests/scripts/`; that level exists today.
