# ABOUTME: Smoke tests proving the integration harness resets the database, swaps clients and scripts wait_for.
# ABOUTME: Run with the local Supabase stack up (make test-integration).
import asyncio

import pytest

from .fakes import FakeMessage, FakeUser
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


async def test_scripted_wait_for_rejects_reply_from_other_user(crudo_bot, make_ctx, script_replies):
    ctx = make_ctx(author_id=900000000000000001, channel_id=910000000000000001)
    other = FakeMessage("intruder", FakeUser(900000000000000002), ctx.channel, ctx.guild)
    script_replies(crudo_bot, ctx, [other, "mine"])

    def check(m):
        return m.author.id == ctx.author.id

    got = await crudo_bot.wait_for("message", check=check, timeout=5)
    assert got.content == "mine"
    with pytest.raises(asyncio.TimeoutError):
        await crudo_bot.wait_for("message", check=check, timeout=5)
