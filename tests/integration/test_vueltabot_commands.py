# ABOUTME: Characterisation tests for Vueltabot's commands against the local Supabase stack.
# ABOUTME: Drives each Cog callback with a fake context and checks replies, embeds and vuelta/transport rows.
import types
from typing import cast

import discord
import pytest
from discord.ext import commands
from discord.ext.commands.view import StringView

from . import fakes
from .fakes import script_replies

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

SELLER_ID = "100000000000000004"
BUYER_ID = 100000000000000001
AUTHOR_ID = 900000000000000031
CHANNEL_ID = 910000000000000031
TRANSPORT_CHANNEL_ID = 920000000000000001
GIG_MESSAGE_ID = 930000000000000001


def _seed_item(db):
    return db.table("vuelta_items").select("*").eq("item_name", "Used Ladder (Test)").execute().data[0]


def _embeds(ctx):
    return [m["embed"] for m in ctx.sent if isinstance(m, dict) and "embed" in m]


def _field_map(embed):
    return {f.name: f.value for f in embed.fields}


class GigChannel:
    """Stands in for the Transportation channel: records posts and serves one stored message."""

    def __init__(self, id=TRANSPORT_CHANNEL_ID, message=None):
        self.id = id
        self.posted = []
        self.message = message

    async def send(self, **kwargs):
        self.posted.append(kwargs)
        return types.SimpleNamespace(id=GIG_MESSAGE_ID)

    async def fetch_message(self, message_id):
        assert message_id == GIG_MESSAGE_ID
        return self.message


class ParsedMessage(fakes.FakeMessage):
    """A FakeMessage that commands.Context accepts (Context reads message._state)."""

    _state = None


class GigMessage:
    def __init__(self, embed):
        self.embeds = [embed]
        self.edited = []

    async def edit(self, **kwargs):
        self.edited.append(kwargs)


def _route_bot_channels(monkeypatch, bot, channel):
    monkeypatch.setattr(bot, "get_channel", lambda cid: channel if cid == TRANSPORT_CHANNEL_ID else None)


def _insert_gig(db, **overrides):
    row = {
        "item_id": str(_seed_item(db)["id"]),
        "buyer_discord_id": "900000000000000041",
        "seller_discord_id": SELLER_ID,
        "pickup_area": "Walzem / 78218",
        "dropoff_area": "Downtown / 78205",
        "delivery_offer": 15,
        "status": "open",
        "gig_channel_id": str(TRANSPORT_CHANNEL_ID),
        "gig_message_id": str(GIG_MESSAGE_ID),
    }
    row.update(overrides)
    return db.table("transport_gigs").insert(row).execute().data[0]


async def test_vueltabot_help_lists_the_commands(vuelta_bot, make_ctx):
    bot, cog = vuelta_bot
    ctx = make_ctx(author_id=AUTHOR_ID, channel_id=CHANNEL_ID)

    await bot.get_command("vueltabot").callback(cog, ctx)

    embeds = _embeds(ctx)
    assert len(embeds) == 1
    assert embeds[0].title == "♻️ Qué onda, I’m Vueltabot."
    assert [f.name for f in embeds[0].fields] == [
        "📦 !tengo", "🔎 !busco", "🤝 !claim", "🚗 !tripclaim", "🛣️ !vuelta",
    ]


async def test_tengo_lists_item_and_alerts_matching_buyer(vuelta_bot, make_ctx, db):
    bot, cog = vuelta_bot
    ctx = make_ctx(author_id=AUTHOR_ID, channel_id=CHANNEL_ID)

    await bot.get_command("tengo").callback(cog, ctx, description="aluminum ladder $25")

    items = db.table("vuelta_items").select("*").eq("item_name", "aluminum ladder").execute().data
    assert len(items) == 1
    item = items[0]
    assert float(item["asking_price"]) == 25
    assert item["status"] == "available"
    assert item["seller_discord_id"] == str(AUTHOR_ID)
    assert item["source_system"] == "circular_sa_discord"
    assert item["photo_url"] is None
    routes = db.table("vuelta_routes").select("*").eq("item_id", item["id"]).execute().data
    assert len(routes) == 1
    assert routes[0]["route_type"] == "local_marketplace"
    assert routes[0]["status"] == "listed"
    assert float(routes[0]["listed_price"]) == 25
    embed = _embeds(ctx)[0]
    assert embed.title == f"📦 Item #{item['id']}"
    assert _field_map(embed)["Price"] == "$25.00"
    assert f"<@{BUYER_ID}>" in ctx.sent_text
    assert "Looking for a ladder" in ctx.sent_text


async def test_tengo_stores_first_image_attachment(vuelta_bot, make_ctx, db):
    bot, cog = vuelta_bot
    attachments = [
        fakes.FakeAttachment("notes.txt", "text/plain"),
        fakes.FakeAttachment("pic.png", "image/png"),
    ]
    ctx = make_ctx(author_id=AUTHOR_ID, channel_id=CHANNEL_ID, attachments=attachments)

    await bot.get_command("tengo").callback(cog, ctx, description="paint roller")

    item = db.table("vuelta_items").select("*").eq("item_name", "paint roller").execute().data[0]
    assert item["photo_url"] == "https://example.invalid/pic.png"
    embeds = _embeds(ctx)
    assert len(embeds) == 1
    assert embeds[0].image.url == "https://example.invalid/pic.png"
    assert "Possible matches" not in ctx.sent_text


async def test_tengo_without_text_shows_usage(vuelta_bot, make_ctx, db):
    bot, cog = vuelta_bot
    ctx = make_ctx(author_id=AUTHOR_ID, channel_id=CHANNEL_ID)

    await bot.get_command("tengo").callback(cog, ctx, description=None)

    assert "¿Qué tienes?" in ctx.sent_text
    assert len(db.table("vuelta_items").select("id").execute().data) == 1


async def test_busco_saves_search_and_shows_matches(vuelta_bot, make_ctx, db):
    bot, cog = vuelta_bot
    seed = _seed_item(db)
    ctx = make_ctx(author_id=AUTHOR_ID, channel_id=CHANNEL_ID)

    await bot.get_command("busco").callback(cog, ctx, description="fiberglass ladder")

    interest = (
        db.table("vuelta_interests").select("*").eq("discord_user_id", str(AUTHOR_ID)).execute().data
    )
    assert len(interest) == 1
    assert interest[0]["match_keywords"] == "fiberglass ladder"
    assert interest[0]["status"] == "active"
    assert interest[0]["alert_enabled"] is True
    assert "Saved your search for **fiberglass ladder**" in ctx.sent_text
    assert "I found **1 possible match**:" in ctx.sent_text
    embed = _embeds(ctx)[0]
    assert embed.title == f"📦 Item #{seed['id']}"
    fields = _field_map(embed)
    assert fields["Price"] == "$40.00"
    assert fields["Condition"] == "used"
    assert fields["Pickup"] == "Testville"
    assert embed.footer.text == f"Seller: Test Seller • Claim with !claim {seed['id']}"


async def test_busco_without_matches(vuelta_bot, make_ctx):
    bot, cog = vuelta_bot
    ctx = make_ctx(author_id=AUTHOR_ID, channel_id=CHANNEL_ID)

    await bot.get_command("busco").callback(cog, ctx, description="unicycle")

    assert "📭 Nothing currently available matched." in ctx.sent_text
    assert _embeds(ctx) == []


async def test_vuelta_shows_item_and_route_history(vuelta_bot, make_ctx, db):
    bot, cog = vuelta_bot
    seed = _seed_item(db)
    db.table("vuelta_routes").insert({
        "item_id": seed["id"], "route_type": "external_marketplace", "platform_name": "Test Market",
        "status": "sold", "listed_price": 45, "sold_price": 38,
    }).execute()
    ctx = make_ctx(author_id=AUTHOR_ID, channel_id=CHANNEL_ID)

    await bot.get_command("vuelta").callback(cog, ctx, seed["id"])

    fields = _field_map(_embeds(ctx)[0])
    assert fields["Status"] == "available"
    assert fields["Price"] == "$40.00"
    assert fields["Category"] == "tools"
    assert "• **local_marketplace** — recommended" in ctx.sent_text
    assert "• **Test Market** — sold — listed $45.00 — sold $38.00" in ctx.sent_text


async def test_vuelta_unknown_and_missing_id(vuelta_bot, make_ctx):
    bot, cog = vuelta_bot
    ctx = make_ctx(author_id=AUTHOR_ID, channel_id=CHANNEL_ID)

    await bot.get_command("vuelta").callback(cog, ctx, 999999)
    await bot.get_command("vuelta").callback(cog, ctx, None)

    assert "❌ No item found with ID #999999." in ctx.sent_text
    assert "`!vuelta 57`" in ctx.sent_text


async def test_claim_with_delivery_posts_the_gig(vuelta_bot, bot_modules, make_ctx, db, monkeypatch):
    bot, cog = vuelta_bot
    seed = _seed_item(db)
    channel = GigChannel()
    monkeypatch.setattr(bot_modules["vueltabot"], "TRANSPORTATION_CHANNEL_ID", TRANSPORT_CHANNEL_ID)
    _route_bot_channels(monkeypatch, bot, channel)
    ctx = make_ctx(author_id=AUTHOR_ID, channel_id=CHANNEL_ID)
    unused = script_replies(bot, ctx, ["yes", "15", "Walzem / 78218", "Downtown / 78205"])

    await bot.get_command("claim").callback(cog, ctx, seed["id"])

    assert unused == []
    gig = db.table("transport_gigs").select("*").execute().data
    assert len(gig) == 1
    gig = gig[0]
    transaction = db.table("vuelta_transactions").select("*").eq("item_id", seed["id"]).execute().data[0]
    assert gig["item_id"] == str(seed["id"])
    assert gig["claim_id"] == str(transaction["id"])
    assert gig["buyer_discord_id"] == str(AUTHOR_ID)
    assert gig["seller_discord_id"] == SELLER_ID
    assert gig["pickup_area"] == "Walzem / 78218"
    assert gig["dropoff_area"] == "Downtown / 78205"
    assert float(gig["delivery_offer"]) == 15
    assert gig["status"] == "open"
    assert gig["gig_channel_id"] == str(TRANSPORT_CHANNEL_ID)
    assert gig["gig_message_id"] == str(GIG_MESSAGE_ID)
    assert transaction["transport_required"] is True
    assert f"Trip Gig #{gig['id']} has been opened for **$15.00**" in ctx.sent_text
    posted = channel.posted[0]["embed"]
    assert posted.title == f"🚗 Trip Gig #{gig['id']}"
    assert (posted.fields[4].name, posted.fields[4].value) == ("Status", "OPEN")
    assert posted.footer.text == f"Claim this trip with !tripclaim {gig['id']}"


async def test_claim_delivery_without_channel_config(vuelta_bot, make_ctx, db):
    bot, cog = vuelta_bot
    seed = _seed_item(db)
    ctx = make_ctx(author_id=AUTHOR_ID, channel_id=CHANNEL_ID)
    script_replies(bot, ctx, ["yes", "15", "Walzem / 78218", "Downtown / 78205"])

    await bot.get_command("claim").callback(cog, ctx, seed["id"])

    assert "hasn't been configured yet" in ctx.sent_text
    gig = db.table("transport_gigs").select("*").execute().data
    assert len(gig) == 1
    assert gig[0]["gig_channel_id"] is None


async def test_claim_prompt_timeout_keeps_claim(vuelta_bot, make_ctx, db):
    bot, cog = vuelta_bot
    seed = _seed_item(db)
    ctx = make_ctx(author_id=AUTHOR_ID, channel_id=CHANNEL_ID)
    script_replies(bot, ctx, [fakes.TIMEOUT])

    await bot.get_command("claim").callback(cog, ctx, seed["id"])

    assert "⌛ Transportation request timed out. The item is still claimed." in ctx.sent_text
    assert db.table("vuelta_items").select("status").eq("id", seed["id"]).execute().data[0]["status"] == "claimed"
    assert db.table("transport_gigs").select("id").execute().data == []


async def test_claim_refusals(vuelta_bot, make_ctx, factories, db):
    bot, cog = vuelta_bot
    seed = _seed_item(db)
    taken = factories.vuelta_item(title="Taken Drill", status="claimed")

    own = make_ctx(author_id=int(SELLER_ID), channel_id=CHANNEL_ID)
    await bot.get_command("claim").callback(cog, own, seed["id"])
    assert "😂 Homie, that's your own item." in own.sent_text

    already = make_ctx(author_id=AUTHOR_ID, channel_id=CHANNEL_ID)
    await bot.get_command("claim").callback(cog, already, taken["id"])
    assert "is already **claimed**" in already.sent_text

    unknown = make_ctx(author_id=AUTHOR_ID, channel_id=CHANNEL_ID)
    await bot.get_command("claim").callback(cog, unknown, 999999)
    assert "❌ I can't find Item #999999." in unknown.sent_text

    assert db.table("vuelta_transactions").select("id").execute().data == []


async def test_tripclaim_claims_open_gig_and_updates_the_post(vuelta_bot, make_ctx, db, monkeypatch):
    bot, cog = vuelta_bot
    gig = _insert_gig(db)
    embed = discord.Embed(title=f"🚗 Trip Gig #{gig['id']}")
    for name, value in [("Pickup", "a"), ("Dropoff", "b"), ("Delivery Offer", "$15.00"),
                        ("Item", "#1"), ("Status", "OPEN")]:
        embed.add_field(name=name, value=value, inline=True)
    message = GigMessage(embed)
    _route_bot_channels(monkeypatch, bot, GigChannel(message=message))
    ctx = make_ctx(author_id=900000000000000042, channel_id=CHANNEL_ID)

    await bot.get_command("tripclaim").callback(cog, ctx, gig["id"])

    row = db.table("transport_gigs").select("*").eq("id", gig["id"]).execute().data[0]
    assert row["status"] == "claimed"
    assert row["driver_discord_id"] == "900000000000000042"
    assert row["claimed_at"] is not None
    assert f"Trip Gig #{gig['id']} claimed!" in ctx.sent_text
    assert "Pickup: **Walzem / 78218**" in ctx.sent_text
    assert "Pay: **$15.00**" in ctx.sent_text
    edited = message.edited[0]["embed"]
    assert (edited.fields[4].name, edited.fields[4].value) == ("Status", "CLAIMED by tester")
    assert edited.footer.text == "This trip has been claimed."


async def test_tripclaim_refusals(vuelta_bot, make_ctx, db):
    bot, cog = vuelta_bot
    gig = _insert_gig(db)
    claimed = _insert_gig(db, status="claimed")

    own = make_ctx(author_id=900000000000000041, channel_id=CHANNEL_ID)
    await bot.get_command("tripclaim").callback(cog, own, gig["id"])
    assert "you can't claim your own delivery gig" in own.sent_text

    already = make_ctx(author_id=AUTHOR_ID, channel_id=CHANNEL_ID)
    await bot.get_command("tripclaim").callback(cog, already, claimed["id"])
    assert "is already **claimed**" in already.sent_text

    unknown = make_ctx(author_id=AUTHOR_ID, channel_id=CHANNEL_ID)
    await bot.get_command("tripclaim").callback(cog, unknown, 999999)
    assert "❌ I can't find Trip Gig #999999." in unknown.sent_text

    missing = make_ctx(author_id=AUTHOR_ID, channel_id=CHANNEL_ID)
    await bot.get_command("tripclaim").callback(cog, missing, None)
    assert "`!tripclaim 7`" in missing.sent_text


@pytest.mark.parametrize("name", ["vuelta", "claim", "tripclaim"])
async def test_numeric_ids_reject_non_numbers(vuelta_bot, name):
    bot, _cog = vuelta_bot
    message = ParsedMessage(f"!{name} abc", fakes.FakeUser(AUTHOR_ID), fakes.FakeChannel(CHANNEL_ID),
                            fakes.FakeGuild())
    view = StringView("abc")
    ctx = commands.Context(message=cast(discord.Message, message), bot=bot, view=view, prefix="!")

    with pytest.raises(commands.BadArgument):
        await bot.get_command(name).invoke(ctx)
