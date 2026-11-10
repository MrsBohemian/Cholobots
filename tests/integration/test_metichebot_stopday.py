# ABOUTME: Regression tests for Metichebot's !mstopday labor accounting against the local Supabase stack.
# ABOUTME: Builds a TimeSession directly, then drives !mstopday through its callback.
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

CHICAGO = ZoneInfo("America/Chicago")
START = datetime(2031, 1, 15, 10, 0, tzinfo=CHICAGO)
STOP = datetime(2031, 1, 15, 10, 20, tzinfo=CHICAGO)
CHANNEL_ID = 910000000000000005
AUTHOR_ID = 823352347715174421


def _linked_session(bot_modules, project, task):
    session = bot_modules["metichebot"].TimeSession(
        channel_id=CHANNEL_ID,
        person="Heaven",
        date_iso="2031-01-15",
        date_label="Jan 15",
        last_timestamp=START.isoformat(),
        setup_complete=True,
    )
    session.crudo_project_id = project["id"]
    session.crudo_project_name = project["project_name"]
    session.crudo_task_id = task["id"]
    session.crudo_task_name = task["task_name"]
    bot_modules["metichebot"].active_time_sessions[CHANNEL_ID] = session
    return session


def _first_task(db, project):
    return db.table("crudo_project_tasks").select("*").eq("project_id", project["id"]).execute().data[0]


@pytest.mark.xfail(strict=True, raises=AssertionError,
                   reason="bug #5 open: !mstopday loses the final time block")
async def test_mstopday_logs_final_block_as_labor_entry(
    metiche_bot, bot_modules, make_ctx, chicago_clock, factories, db
):
    project = factories.crudo_project(name="Test Faucet 51", status="active", tasks_pending=1)
    task = _first_task(db, project)
    _linked_session(bot_modules, project, task)
    chicago_clock(STOP)
    ctx = make_ctx(author_id=AUTHOR_ID, channel_id=CHANNEL_ID)

    await metiche_bot.get_command("mstopday").callback(ctx)

    entries = db.table("crudo_labor_entries").select("*").eq("project_id", project["id"]).execute().data
    assert len(entries) == 1
    assert entries[0]["task_id"] == task["id"]
    assert entries[0]["actual_minutes"] == 20


async def test_mstopday_reports_totals_and_ends_session(
    metiche_bot, bot_modules, make_ctx, chicago_clock, factories, db
):
    project = factories.crudo_project(name="Test Faucet 51", status="active", tasks_pending=1)
    task = _first_task(db, project)
    session = _linked_session(bot_modules, project, task)
    session.blocks.append({
        "date": "2031-01-15",
        "start": START.isoformat(),
        "end": START.isoformat(),
        "duration_minutes": 15,
        "duration_label": "15m",
        "activity": "earlier work",
        "source": "test",
    })
    chicago_clock(STOP)
    ctx = make_ctx(author_id=AUTHOR_ID, channel_id=CHANNEL_ID)

    await metiche_bot.get_command("mstopday").callback(ctx)

    assert CHANNEL_ID not in bot_modules["metichebot"].active_time_sessions
    assert "Blocks logged:" in ctx.sent_text
    assert "15m" in ctx.sent_text
