# ABOUTME: Regression tests for Crudobot closeout against the local Supabase stack.
# ABOUTME: Drives !crudoclose through its callback with a fake context and scripted replies.
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

AUTHOR_ID = 900000000000000001
CHANNEL_ID = 910000000000000001


def _project_row(db, project):
    return db.table("crudo_projects").select("*").eq("id", project["id"]).execute().data[0]


async def _run_crudoclose(crudo_bot, make_ctx, script_replies, name):
    ctx = make_ctx(author_id=AUTHOR_ID, channel_id=CHANNEL_ID)
    # "done" leaves the existing task times alone, "7" exits the closeout menu.
    script_replies(crudo_bot, ctx, ["done", "7"])
    await crudo_bot.get_command("crudoclose").callback(ctx, project_query=name)
    return ctx


@pytest.mark.xfail(strict=True, raises=AssertionError,
                   reason='bug #6 open: re-running closeout resets closeout_status to "ready"')
async def test_crudoclose_keeps_paid_closeout_status(crudo_bot, make_ctx, script_replies, factories, db):
    project = factories.crudo_project(
        name="Test Deck Rail 51", status="closeout", closeout_status="paid",
        tasks_completed=2, actual_minutes=30,
    )

    await _run_crudoclose(crudo_bot, make_ctx, script_replies, "Test Deck Rail 51")

    assert _project_row(db, project)["closeout_status"] == "paid"


async def test_crudoclose_marks_finished_project_ready(crudo_bot, make_ctx, script_replies, factories, db):
    project = factories.crudo_project(
        name="Test Deck Rail 51", status="active", tasks_completed=2, actual_minutes=30,
    )

    await _run_crudoclose(crudo_bot, make_ctx, script_replies, "Test Deck Rail 51")

    row = _project_row(db, project)
    assert row["status"] == "closeout"
    assert row["closeout_status"] == "ready"
