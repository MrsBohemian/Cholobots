# ABOUTME: Regression tests for Metichebot's daily_tasks identity handling against the local Supabase stack.
# ABOUTME: Drives !mbraindump through its callback with a fake context and scripted replies.
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

DATE = "2031-01-15"
HEAVEN_ID = 823352347715174421
UNKNOWN_ID = 900000000000000004
CHANNEL_ID = 910000000000000004


def _heaven_rows(db):
    rows = (
        db.table("daily_tasks")
        .select("task,completed")
        .eq("calendar_key", "heaven")
        .eq("date", DATE)
        .execute()
        .data
    )
    return sorted((r["task"], r["completed"]) for r in rows)


def _seed_heaven(factories):
    factories.daily_tasks("heaven", DATE, ["Heaven seeded task A"])
    factories.daily_tasks("heaven", DATE, ["Heaven seeded task B"], completed=True)


async def _braindump(metiche_bot, make_ctx, script_replies, author_id):
    ctx = make_ctx(author_id=author_id, channel_id=CHANNEL_ID)
    queue = script_replies(metiche_bot, ctx, ["fix the gate, buy screws", "T: 1", "later"])
    await metiche_bot.get_command("mbraindump").callback(ctx)
    return queue


@pytest.mark.xfail(strict=True, raises=AssertionError,
                   reason="bug #4 open: unknown Discord users read and overwrite Heaven's daily tasks")
async def test_unknown_user_braindump_leaves_heaven_tasks_untouched(
    metiche_bot, make_ctx, script_replies, factories, db
):
    _seed_heaven(factories)

    await _braindump(metiche_bot, make_ctx, script_replies, UNKNOWN_ID)

    assert _heaven_rows(db) == [("Heaven seeded task A", False), ("Heaven seeded task B", True)]


async def test_heaven_braindump_appends_today_items(metiche_bot, make_ctx, script_replies, factories, db):
    _seed_heaven(factories)

    unused = await _braindump(metiche_bot, make_ctx, script_replies, HEAVEN_ID)

    assert unused == []
    assert _heaven_rows(db) == sorted([
        ("Heaven seeded task A", False),
        ("Heaven seeded task B", True),
        ("fix the gate", False),
    ])
