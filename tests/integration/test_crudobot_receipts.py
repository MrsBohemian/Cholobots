# ABOUTME: Regression tests for Crudobot receipt confirmation against the local Supabase stack.
# ABOUTME: Drives !crudoreceipt through its callback, then feeds replies to the confirmation listener.
import pytest

from .fakes import FakeAttachment, FakeMessage, get_listener as find_listener

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

AUTHOR_ID = 900000000000000002
CHANNEL_A = 910000000000000002
CHANNEL_B = 910000000000000003
VENDOR = "Fake Hardware Supply 51"
PARSED = {
    "vendor": VENDOR,
    "transaction_type": "purchase",
    "transaction_date": "2031-01-15",
    "receipt_number": None,
    "subtotal": 10.0,
    "tax": 0.83,
    "total": 10.83,
    "notes": None,
    "items": [{
        "sku": None, "description": "wire nuts", "quantity": 1, "unit": "each",
        "unit_price": 10.0, "line_total": 10.0, "category": "electrical",
    }],
}


async def _parse_receipt(crudo_bot, bot_modules, make_ctx, factories, monkeypatch):
    factories.crudo_project(name="Test Receipt Job 51", status="active", tasks_pending=1)
    monkeypatch.setattr(
        bot_modules["crudobot"], "parse_receipt_with_openai",
        lambda filename, content_type, data: dict(PARSED),
    )
    attachment = FakeAttachment("r.pdf", "application/pdf", b"%PDF-1.4 test")
    ctx = make_ctx(author_id=AUTHOR_ID, channel_id=CHANNEL_A, attachments=[attachment])
    await crudo_bot.get_command("crudoreceipt").callback(ctx, project_query="Test Receipt Job 51")
    assert AUTHOR_ID in bot_modules["crudobot"].receipt_parse_sessions
    return ctx


def _reply(ctx, text, channel):
    return FakeMessage(text, ctx.author, channel, ctx.guild)


def _transactions(db):
    return db.table("crudo_material_transactions").select("*").eq("vendor", VENDOR).execute().data


@pytest.mark.xfail(strict=True, raises=AssertionError,
                   reason='bug #7 open: pending receipt is saved by an unrelated "yes" from another channel')
async def test_receipt_yes_from_other_channel_does_not_save(
    crudo_bot, bot_modules, make_ctx, factories, db, monkeypatch
):
    ctx = await _parse_receipt(crudo_bot, bot_modules, make_ctx, factories, monkeypatch)
    listener = find_listener(crudo_bot, "handle_crudobot_receipt_confirmation")
    other_channel = type(ctx.channel)(CHANNEL_B)

    await listener(_reply(ctx, "yes", other_channel))

    assert _transactions(db) == []


async def test_receipt_save_in_same_channel_saves_transaction_and_items(
    crudo_bot, bot_modules, make_ctx, factories, db, monkeypatch
):
    ctx = await _parse_receipt(crudo_bot, bot_modules, make_ctx, factories, monkeypatch)
    listener = find_listener(crudo_bot, "handle_crudobot_receipt_confirmation")

    await listener(_reply(ctx, "save", ctx.channel))

    transactions = _transactions(db)
    assert len(transactions) == 1
    assert float(transactions[0]["total"]) == 10.83
    items = (
        db.table("crudo_material_transaction_items").select("*")
        .eq("transaction_id", transactions[0]["id"]).execute().data
    )
    assert [i["item_name"] for i in items] == ["wire nuts"]
    assert AUTHOR_ID not in bot_modules["crudobot"].receipt_parse_sessions
