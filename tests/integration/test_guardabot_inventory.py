# ABOUTME: Characterisation tests for Guardabot's inventory commands against the local Supabase stack.
# ABOUTME: Drives each command callback with a fake context and checks replies and inventory rows.
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

AUTHOR_ID = 900000000000000001
CHANNEL_ID = 910000000000000001

ITEM = "conduit straps"


def _items(db, name, location):
    return (
        db.table("inventory_items").select("*")
        .eq("item_name", name).eq("location", location).execute().data
    )


def _events(db, name, event_type):
    return (
        db.table("material_events").select("*")
        .eq("item_name", name).eq("event_type", event_type).execute().data
    )


def _ctx(make_ctx):
    return make_ctx(author_id=AUTHOR_ID, channel_id=CHANNEL_ID)


@pytest.fixture
def stock(factories):
    """50 conduit straps at A3, created here rather than borrowed from seed.sql."""
    return factories.inventory_item(
        ITEM, 50, "A3", unit="each", category="electrical", notes="Factory note."
    )


async def test_gadd_restocks_existing_row(guard_bot, make_ctx, db, stock):
    ctx = _ctx(make_ctx)
    await guard_bot.get_command("gadd").callback(ctx, text=f"{ITEM} qty:5 loc:a3")

    rows = _items(db, ITEM, "A3")
    assert len(rows) == 1
    assert float(rows[0]["quantity"]) == 55
    assert ITEM in ctx.sent_text
    assert "A3" in ctx.sent_text
    assert "55" in ctx.sent_text
    events = _events(db, ITEM, "add")
    assert len(events) == 1
    assert float(events[0]["quantity"]) == 5
    assert events[0]["location"] == "A3"


async def test_gadd_creates_row_with_metadata(guard_bot, make_ctx, db):
    ctx = _ctx(make_ctx)
    await guard_bot.get_command("gadd").callback(
        ctx, text="zip ties qty:100 loc:c3 category:electrical unit:each"
    )

    rows = _items(db, "zip ties", "C3")
    assert len(rows) == 1
    assert rows[0]["category"] == "electrical"
    assert rows[0]["unit"] == "each"
    assert float(rows[0]["quantity"]) == 100


@pytest.mark.parametrize(
    ("qty", "complaint"),
    [("abc", "needs to be a number"), ("0", "greater than zero")],
)
async def test_gadd_rejects_bad_qty_without_writing(guard_bot, make_ctx, db, qty, complaint):
    ctx = _ctx(make_ctx)
    await guard_bot.get_command("gadd").callback(ctx, text=f"zip ties qty:{qty}")

    assert complaint in ctx.sent_text
    assert db.table("inventory_items").select("*").eq("item_name", "zip ties").execute().data == []
    assert _events(db, "zip ties", "add") == []


async def test_gremove_reduces_stock(guard_bot, make_ctx, db, stock):
    ctx = _ctx(make_ctx)
    await guard_bot.get_command("gremove").callback(ctx, text=f"{ITEM} qty:10 loc:A3")

    assert float(_items(db, ITEM, "A3")[0]["quantity"]) == 40
    assert "Removed" in ctx.sent_text
    assert ITEM in ctx.sent_text
    events = _events(db, ITEM, "remove")
    assert len(events) == 1
    assert float(events[0]["quantity"]) == 10


async def test_gremove_short_deletes_row_and_reports_shortfall(guard_bot, make_ctx, db, stock):
    ctx = _ctx(make_ctx)
    await guard_bot.get_command("gremove").callback(ctx, text=f"{ITEM} qty:60 loc:A3")

    assert _items(db, ITEM, "A3") == []
    assert "Removed 50 of requested 60" in ctx.sent_text
    assert "short by 10" in ctx.sent_text
    events = _events(db, ITEM, "remove")
    assert [float(e["quantity"]) for e in events] == [50]


async def test_gremove_unknown_item_writes_nothing(guard_bot, make_ctx, db):
    ctx = _ctx(make_ctx)
    await guard_bot.get_command("gremove").callback(ctx, text="unobtainium qty:1")

    assert "couldn't find `unobtainium`" in ctx.sent_text
    assert _events(db, "unobtainium", "remove") == []


async def test_gmove_moves_stock_and_keeps_metadata(guard_bot, make_ctx, db, stock):
    ctx = _ctx(make_ctx)
    await guard_bot.get_command("gmove").callback(ctx, text=f"{ITEM} qty:20 from:A3 to:D2")

    assert float(_items(db, ITEM, "A3")[0]["quantity"]) == 30
    dest = _items(db, ITEM, "D2")
    assert len(dest) == 1
    assert float(dest[0]["quantity"]) == 20
    assert dest[0]["unit"] == "each"
    assert dest[0]["category"] == "electrical"
    assert dest[0]["notes"] == "Factory note."
    assert "Moved" in ctx.sent_text
    assert "A3" in ctx.sent_text and "D2" in ctx.sent_text
    events = _events(db, ITEM, "move")
    assert len(events) == 1
    assert events[0]["location"] == "D2"


async def test_gmove_refuses_when_short(guard_bot, make_ctx, db, stock):
    ctx = _ctx(make_ctx)
    await guard_bot.get_command("gmove").callback(ctx, text=f"{ITEM} qty:80 from:A3 to:D2")

    assert "only see 50" in ctx.sent_text
    assert "did not move" in ctx.sent_text
    assert float(_items(db, ITEM, "A3")[0]["quantity"]) == 50
    assert _items(db, ITEM, "D2") == []
    assert _events(db, ITEM, "move") == []


async def test_gcheck_lists_matches(guard_bot, make_ctx, stock):
    ctx = _ctx(make_ctx)
    await guard_bot.get_command("gcheck").callback(ctx, item_name="conduit")

    line = next(ln for ln in ctx.sent_text.splitlines() if ITEM in ln)
    assert "loc: A3" in line
    assert "category: electrical" in line


async def test_gmap_shows_zones(guard_bot, make_ctx, db, factories):
    factories.garage_zone("D4", "Paint Rack")
    db.table("garage_zones").delete().eq("cell", "E5").execute()

    ctx = _ctx(make_ctx)
    await guard_bot.get_command("gmap").callback(ctx)

    assert "D4: Paint Rack" in ctx.sent_text
    assert "E5: empty" in ctx.sent_text


async def test_gzone_labels_a_range(guard_bot, make_ctx, db):
    ctx = _ctx(make_ctx)
    await guard_bot.get_command("gzone").callback(ctx, cells="C1-C3", zone_name="Paint Rack")

    rows = db.table("garage_zones").select("*").in_("cell", ["C1", "C2", "C3"]).execute().data
    assert sorted(r["cell"] for r in rows) == ["C1", "C2", "C3"]
    assert {r["zone_name"] for r in rows} == {"Paint Rack"}
    assert "C1, C2, C3" in ctx.sent_text


async def test_gused_strips_job_and_removes(guard_bot, make_ctx, db, stock):
    ctx = _ctx(make_ctx)
    await guard_bot.get_command("gused").callback(ctx, text=f"{ITEM} qty:5 loc:A3 job:olive")

    assert float(_items(db, ITEM, "A3")[0]["quantity"]) == 45
    assert "Removed" in ctx.sent_text
    assert "job:olive" not in ctx.sent_text
