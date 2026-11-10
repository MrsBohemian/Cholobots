# ABOUTME: Regression tests for Vueltabot !claim against the local Supabase stack.
# ABOUTME: Simulates a rival claim landing between the command's read and its write.
import pytest

from .fakes import script_replies

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

CLAIMANT_ID = "900000000000000011"
RIVAL_ID = "900000000000000012"
SELLER_ID = "100000000000000009"


class RivalClaimClient:
    """Delegates to the real client, but commits a rival claim on the first call after the item read."""

    def __init__(self, real, item_id):
        self._real = real
        self._item_id = item_id
        self._read_seen = False
        self._rival_done = False

    def table(self, name):
        if self._read_seen and not self._rival_done:
            self._rival_done = True
            self._commit_rival_claim()
        if name == "vuelta_items":
            self._read_seen = True
        return self._real.table(name)

    def _commit_rival_claim(self):
        self._real.table("vuelta_items").update({"status": "claimed"}).eq("id", self._item_id).execute()
        self._real.table("vuelta_transactions").insert({
            "item_id": self._item_id,
            "seller_discord_id": SELLER_ID,
            "buyer_discord_id": RIVAL_ID,
            "buyer_discord_name": "rival",
            "agreed_price": None,
            "transaction_type": "giveaway",
            "transport_required": False,
            "status": "claimed",
        }).execute()

    def __getattr__(self, name):
        return getattr(self._real, name)


def _transactions(db, item_id):
    return db.table("vuelta_transactions").select("*").eq("item_id", item_id).execute().data


@pytest.mark.xfail(strict=True, raises=AssertionError,
                   reason="bug #12 open: !claim allows two users to claim the same item")
async def test_claim_fails_when_item_claimed_after_read(
    vuelta_bot, bot_modules, make_ctx, factories, db, monkeypatch
):
    bot, cog = vuelta_bot
    item = factories.vuelta_item(title="Test Ladder 51", status="available",
                                 seller_discord_id=SELLER_ID, asking_price=None)
    monkeypatch.setattr(bot_modules["vueltabot"], "supabase", RivalClaimClient(db, item["id"]))
    ctx = make_ctx(author_id=int(CLAIMANT_ID), channel_id=910000000000000011)
    script_replies(bot, ctx, ["no"])

    await bot.get_command("claim").callback(cog, ctx, item["id"])

    rows = _transactions(db, item["id"])
    claimed = [r for r in rows if r["status"] == "claimed"]
    assert len(claimed) == 1
    assert claimed[0]["buyer_discord_id"] == RIVAL_ID


async def test_claim_marks_available_item_claimed(vuelta_bot, make_ctx, factories, db):
    bot, cog = vuelta_bot
    item = factories.vuelta_item(title="Test Ladder 51", status="available",
                                 seller_discord_id=SELLER_ID, asking_price=None)
    ctx = make_ctx(author_id=int(CLAIMANT_ID), channel_id=910000000000000011)
    script_replies(bot, ctx, ["no"])

    await bot.get_command("claim").callback(cog, ctx, item["id"])

    row = db.table("vuelta_items").select("status").eq("id", item["id"]).execute().data[0]
    assert row["status"] == "claimed"
    rows = _transactions(db, item["id"])
    assert len(rows) == 1
    assert rows[0]["buyer_discord_id"] == CLAIMANT_ID
    embed_titles = [m["embed"].title for m in ctx.sent if isinstance(m, dict) and "embed" in m]
    assert any(f"#{item['id']}" in title for title in embed_titles)
