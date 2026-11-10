# ABOUTME: Smoke tests proving the integration harness resets the database, swaps clients and scripts wait_for.
# ABOUTME: Run with the local Supabase stack up (make test-integration).
import asyncio

import pytest

from .fakes import TIMEOUT, FakeMessage, FakeUser, UnscriptedPrompt, make_bot
from .local_guard import is_local_url

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_every_module_client_points_at_local_stack(bot_modules, db, settings):
    assert is_local_url(settings.url)
    for name, module in bot_modules.items():
        assert module.supabase is db, name


async def test_reset_restores_seed_rows(db, reset_database):
    seeded = len(db.table("chisme_contacts").select("id").execute().data)
    db.table("chisme_contacts").insert({"name": "Harness Smoke Contact"}).execute()
    assert len(db.table("chisme_contacts").select("id").execute().data) == seeded + 1

    reset_database()

    rows = db.table("chisme_contacts").select("name").execute().data
    assert len(rows) == seeded
    assert "Harness Smoke Contact" not in [r["name"] for r in rows]


def _own_messages(ctx):
    def check(m):
        return m.author.id == ctx.author.id

    return check


async def test_scripted_wait_for_rejects_reply_from_other_user(make_ctx, script_replies):
    bot = make_bot()
    ctx = make_ctx(author_id=900000000000000001, channel_id=910000000000000001)
    other = FakeMessage("intruder", FakeUser(900000000000000002), ctx.channel, ctx.guild)
    queue = script_replies(bot, ctx, [other, "mine"])

    got = await bot.wait_for("message", check=_own_messages(ctx), timeout=5)

    assert got.content == "mine"
    assert queue == []


@pytest.mark.parametrize("timeout", [None, 120])
async def test_scripted_wait_for_fails_loudly_when_replies_run_out(make_ctx, script_replies, timeout):
    # Not TimeoutError: Vueltabot's ask_user catches that and returns quietly, hiding a missing reply.
    bot = make_bot()
    ctx = make_ctx(author_id=900000000000000001, channel_id=910000000000000001)
    script_replies(bot, ctx, [])

    with pytest.raises(UnscriptedPrompt, match="ran out of scripted replies"):
        await bot.wait_for("message", check=_own_messages(ctx), timeout=timeout)


async def test_scripted_timeout_raises_timeout_error_when_caller_set_a_timeout(make_ctx, script_replies):
    bot = make_bot()
    ctx = make_ctx(author_id=900000000000000001, channel_id=910000000000000001)
    script_replies(bot, ctx, [TIMEOUT])

    with pytest.raises(asyncio.TimeoutError):
        await bot.wait_for("message", check=_own_messages(ctx), timeout=120)


async def test_scripted_timeout_is_refused_when_caller_waits_forever(make_ctx, script_replies):
    bot = make_bot()
    ctx = make_ctx(author_id=900000000000000001, channel_id=910000000000000001)
    script_replies(bot, ctx, [TIMEOUT])

    with pytest.raises(UnscriptedPrompt, match="wait forever"):
        await bot.wait_for("message", check=_own_messages(ctx))
