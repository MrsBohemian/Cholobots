# ABOUTME: Characterisation tests for Guardabot's inventory commands against the local Supabase stack.
# ABOUTME: Drives each command callback with a fake context and checks replies and inventory rows.
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

AUTHOR_ID = 900000000000000001
CHANNEL_ID = 910000000000000001


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


async def test_gadd_restocks_existing_row(guard_bot, make_ctx, db):
    ctx = _ctx(make_ctx)
    await guard_bot.get_command("gadd").callback(ctx, text="wire nuts qty:5 loc:a1")

    rows = _items(db, "wire nuts", "A1")
    assert len(rows) == 1
    assert float(rows[0]["quantity"]) == 55
    assert "✅ Inventory updated: wire nuts +5" in ctx.sent_text
    assert "Now on hand there: 55" in ctx.sent_text
    events = _events(db, "wire nuts", "add")
    assert len(events) == 1
    assert float(events[0]["quantity"]) == 5
    assert events[0]["location"] == "A1"


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


async def test_gadd_rejects_bad_qty(guard_bot, make_ctx, db):
    ctx = _ctx(make_ctx)
    await guard_bot.get_command("gadd").callback(ctx, text="zip ties qty:abc")
    assert "`qty:` needs to be a number." in ctx.sent_text

    ctx = _ctx(make_ctx)
    await guard_bot.get_command("gadd").callback(ctx, text="zip ties qty:0")
    assert "needs to be greater than zero" in ctx.sent_text

    assert db.table("inventory_items").select("*").eq("item_name", "zip ties").execute().data == []


async def test_gremove_reduces_stock(guard_bot, make_ctx, db):
    ctx = _ctx(make_ctx)
    await guard_bot.get_command("gremove").callback(ctx, text="wire nuts qty:10 loc:A1")

    assert float(_items(db, "wire nuts", "A1")[0]["quantity"]) == 40
    assert "✅ Removed: wire nuts x 10 from A1" in ctx.sent_text
    events = _events(db, "wire nuts", "remove")
    assert len(events) == 1
    assert float(events[0]["quantity"]) == 10


async def test_gremove_short_deletes_row_and_reports_shortfall(guard_bot, make_ctx, db):
    ctx = _ctx(make_ctx)
    await guard_bot.get_command("gremove").callback(ctx, text="wire nuts qty:60 loc:A1")

    assert _items(db, "wire nuts", "A1") == []
    assert "Removed 50 of requested 60 wire nuts" in ctx.sent_text
    assert "short by 10" in ctx.sent_text


async def test_gremove_unknown_item(guard_bot, make_ctx):
    ctx = _ctx(make_ctx)
    await guard_bot.get_command("gremove").callback(ctx, text="unobtainium qty:1")
    assert "⚠️ I couldn't find `unobtainium`" in ctx.sent_text


async def test_gmove_moves_stock_and_keeps_metadata(guard_bot, make_ctx, db):
    ctx = _ctx(make_ctx)
    await guard_bot.get_command("gmove").callback(ctx, text="wire nuts qty:20 from:A1 to:D2")

    assert float(_items(db, "wire nuts", "A1")[0]["quantity"]) == 30
    dest = _items(db, "wire nuts", "D2")
    assert len(dest) == 1
    assert float(dest[0]["quantity"]) == 20
    assert dest[0]["unit"] == "each"
    assert dest[0]["category"] == "electrical"
    assert dest[0]["notes"] == "Seed item."
    assert "✅ Moved: wire nuts x 20 — A1 → D2" in ctx.sent_text
    events = _events(db, "wire nuts", "move")
    assert len(events) == 1
    assert events[0]["location"] == "D2"


async def test_gmove_refuses_when_short(guard_bot, make_ctx, db):
    ctx = _ctx(make_ctx)
    await guard_bot.get_command("gmove").callback(ctx, text="wire nuts qty:80 from:A1 to:D2")

    assert "I only see 50 wire nuts at A1; I did not move anything." in ctx.sent_text
    assert float(_items(db, "wire nuts", "A1")[0]["quantity"]) == 50
    assert _items(db, "wire nuts", "D2") == []


async def test_gcheck_lists_matches(guard_bot, make_ctx):
    ctx = _ctx(make_ctx)
    await guard_bot.get_command("gcheck").callback(ctx, item_name="wire")

    assert "📦 Inventory check for: wire" in ctx.sent_text
    line = next(ln for ln in ctx.sent_text.splitlines() if "wire nuts" in ln)
    assert "loc: A1" in line
    assert "category: electrical" in line


async def test_gmap_shows_zones(guard_bot, make_ctx):
    ctx = _ctx(make_ctx)
    await guard_bot.get_command("gmap").callback(ctx)

    assert "A1: Electrical Shelf (Test)" in ctx.sent_text
    assert "B2: Plumbing Bin (Test)" in ctx.sent_text
    assert "C1: empty" in ctx.sent_text


async def test_gzone_labels_a_range(guard_bot, make_ctx, db):
    ctx = _ctx(make_ctx)
    await guard_bot.get_command("gzone").callback(ctx, cells="C1-C3", zone_name="Paint Rack")

    rows = db.table("garage_zones").select("*").in_("cell", ["C1", "C2", "C3"]).execute().data
    assert sorted(r["cell"] for r in rows) == ["C1", "C2", "C3"]
    assert {r["zone_name"] for r in rows} == {"Paint Rack"}
    assert "✅ Updated map: C1, C2, C3 = Paint Rack" in ctx.sent_text


async def test_gused_strips_job_and_removes(guard_bot, make_ctx, db):
    ctx = _ctx(make_ctx)
    await guard_bot.get_command("gused").callback(ctx, text="wire nuts qty:5 loc:A1 job:olive")

    assert float(_items(db, "wire nuts", "A1")[0]["quantity"]) == 45
    assert "✅ Removed: wire nuts x 5 from A1" in ctx.sent_text
