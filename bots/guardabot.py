import os
import re
from datetime import datetime
from typing import Optional

from discord.ext import commands
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

VALID_ROWS = ["A", "B", "C", "D", "E"]
VALID_COLS = ["1", "2", "3", "4", "5"]


def now_iso():
    return datetime.now().isoformat(timespec="seconds")


def normalize_cell(cell: str) -> Optional[str]:
    cell = (cell or "").strip().upper()
    if re.fullmatch(r"[A-E][1-5]", cell):
        return cell
    return None


def parse_cells(cell_text: str):
    """Accept A1, A1-A2, or A1,A2,B3."""
    cell_text = (cell_text or "").strip().upper().replace(" ", "")

    if "-" in cell_text:
        start, end = cell_text.split("-", 1)
        start = normalize_cell(start)
        end = normalize_cell(end)
        if not start or not end:
            return []

        row1, col1 = start[0], int(start[1])
        row2, col2 = end[0], int(end[1])
        if row1 != row2:
            return [start, end]

        low, high = sorted([col1, col2])
        return [f"{row1}{c}" for c in range(low, high + 1)]

    cells = []
    for part in cell_text.split(","):
        cell = normalize_cell(part)
        if cell:
            cells.append(cell)
    return cells


def get_arg(text: str, key: str, default=None):
    match = re.search(rf"\b{re.escape(key)}:(\S+)", text, flags=re.IGNORECASE)
    return match.group(1).strip() if match else default


def remove_arg_tokens(text: str):
    return re.sub(
        r"\b(qty|loc|location|from|to|category|unit):\S+",
        "",
        text,
        flags=re.IGNORECASE,
    ).strip()


def find_inventory(item_name: str, location: str = ""):
    query = (
        supabase.table("inventory_items")
        .select("*")
        .ilike("item_name", f"%{item_name}%")
    )
    if location:
        query = query.eq("location", location.upper())
    return query.execute().data or []


def add_or_update_inventory(item_name, qty, location="", category="", unit="", notes=""):
    location = (location or "").upper()
    response = (
        supabase.table("inventory_items")
        .select("*")
        .eq("item_name", item_name)
        .eq("location", location)
        .execute()
    )
    existing = response.data or []

    if existing:
        row = existing[0]
        new_qty = float(row.get("quantity") or 0) + float(qty or 0)
        supabase.table("inventory_items").update({
            "quantity": new_qty,
            "category": category or row.get("category", ""),
            "unit": unit or row.get("unit", ""),
            "notes": notes or row.get("notes", ""),
            "updated_at": now_iso(),
        }).eq("id", row["id"]).execute()
        return row["id"], new_qty

    result = supabase.table("inventory_items").insert({
        "item_name": item_name,
        "quantity": float(qty or 0),
        "unit": unit,
        "location": location,
        "category": category,
        "notes": notes,
        "updated_at": now_iso(),
    }).execute().data or []
    return (result[0].get("id") if result else None), float(qty or 0)


def inventory_available(item_name: str, location: str = "") -> float:
    return sum(float(r.get("quantity") or 0) for r in find_inventory(item_name, location))


def subtract_inventory(item_name, qty, location=""):
    rows = find_inventory(item_name, location)
    if not rows:
        return False, "not_found", 0.0

    requested = float(qty or 0)
    remaining = requested

    for row in rows:
        if remaining <= 0:
            break
        current_qty = float(row.get("quantity") or 0)
        take = min(current_qty, remaining)
        if take <= 0:
            continue
        new_qty = current_qty - take
        remaining -= take
        supabase.table("inventory_items").update({
            "quantity": new_qty,
            "updated_at": now_iso(),
        }).eq("id", row["id"]).execute()

    removed = requested - remaining
    if remaining > 0:
        return True, f"partial_short_by_{remaining:g}", removed
    return True, "ok", removed


def log_inventory_event(event_type, item_name, qty=0, location="", unit="", notes=""):
    """Keep a lightweight physical-inventory audit trail; no job-cost data lives here."""
    try:
        supabase.table("material_events").insert({
            "event_type": event_type,
            "item_name": item_name,
            "quantity": float(qty or 0),
            "unit": unit or "",
            "location": location or "",
            "job": "",
            "vendor": "",
            "cost": 0,
            "notes": notes or "",
        }).execute()
    except Exception:
        # Inventory is the source of truth. An audit-log failure should not undo
        # an otherwise successful physical inventory update.
        pass


def register_guard(bot: commands.Bot):

    @bot.command(name="guardabot")
    async def guardabot_help(ctx):
        await ctx.send(
            "📦 GUARDABOT\n\n"
            "Guardabot tracks what Handley Man physically has on hand, how much, and where it is.\n\n"
            "`!gmap` — show the garage map\n"
            "`!gzone A1-A2 camping equipment` — label garage zones\n"
            "`!gcheck fan box` — find inventory and locations\n"
            "`!gadd wire nuts qty:50 loc:E2 category:electrical unit:each` — add/restock inventory\n"
            "`!gremove wire nuts qty:10 loc:E2` — remove inventory when it leaves storage\n"
            "`!gmove wire nuts qty:25 from:E2 to:D2` — move inventory to another location\n\n"
            "Receipts, purchases, returns, and job costing now belong to Crudobot."
        )

    @bot.command(name="gmap")
    async def gmap(ctx):
        response = supabase.table("garage_zones").select("*").execute()
        rows = response.data or []
        zone_by_cell = {row["cell"]: row["zone_name"] for row in rows}

        lines = ["🗺️ GARAGE MAP"]
        for r in VALID_ROWS:
            row_parts = []
            for c in VALID_COLS:
                cell = f"{r}{c}"
                row_parts.append(f"{cell}: {zone_by_cell.get(cell, 'empty')}")
            lines.append(" | ".join(row_parts))
        await ctx.send("```" + "\n".join(lines) + "```")

    @bot.command(name="gzone")
    async def gzone(ctx, cells: str = "", *, zone_name: str = ""):
        parsed = parse_cells(cells)
        zone_name = zone_name.strip()
        if not parsed or not zone_name:
            await ctx.send("Use: `!gzone A1-A2 camping equipment`")
            return

        for cell in parsed:
            supabase.table("garage_zones").upsert({
                "cell": cell,
                "zone_name": zone_name,
                "updated_at": now_iso(),
            }).execute()
        await ctx.send(f"✅ Updated map: {', '.join(parsed)} = {zone_name}")

    @bot.command(name="gadd")
    async def gadd(ctx, *, text: str = ""):
        if not text.strip():
            await ctx.send("Use: `!gadd wire nuts qty:50 loc:E2 category:electrical unit:each`")
            return

        try:
            qty = float(get_arg(text, "qty", 1))
        except ValueError:
            await ctx.send("`qty:` needs to be a number.")
            return
        if qty <= 0:
            await ctx.send("`qty:` needs to be greater than zero.")
            return

        loc = (get_arg(text, "loc", get_arg(text, "location", "")) or "").upper()
        category = get_arg(text, "category", "") or ""
        unit = get_arg(text, "unit", "") or ""
        item_name = remove_arg_tokens(text)

        if not item_name:
            await ctx.send("I need an item name.")
            return

        _, new_qty = add_or_update_inventory(item_name, qty, location=loc, category=category, unit=unit)
        log_inventory_event("add", item_name, qty, location=loc, unit=unit, notes="inventory added/restocked")
        await ctx.send(
            f"✅ Inventory updated: {item_name} +{qty:g}\n"
            f"Location: {loc or '?'}\n"
            f"Now on hand there: {new_qty:g} {unit}".rstrip()
        )

    @bot.command(name="gcheck")
    async def gcheck(ctx, *, item_name: str = ""):
        item_name = item_name.strip()
        if not item_name:
            await ctx.send("Use: `!gcheck fan box`")
            return

        rows = find_inventory(item_name)
        if not rows:
            await ctx.send(f"⚠️ I don’t see `{item_name}` in inventory yet.")
            return

        lines = [f"📦 Inventory check for: {item_name}"]
        for row in rows[:12]:
            lines.append(
                f"- {row['item_name']} | qty: {row['quantity']} {row.get('unit') or ''} "
                f"| loc: {row.get('location') or '?'} | category: {row.get('category') or '?'}"
            )
        await ctx.send("\n".join(lines))

    @bot.command(name="gremove")
    async def gremove(ctx, *, text: str = ""):
        if not text.strip():
            await ctx.send("Use: `!gremove wire nuts qty:10 loc:E2`")
            return

        try:
            qty = float(get_arg(text, "qty", 1))
        except ValueError:
            await ctx.send("`qty:` needs to be a number.")
            return
        if qty <= 0:
            await ctx.send("`qty:` needs to be greater than zero.")
            return

        loc = (get_arg(text, "loc", get_arg(text, "location", "")) or "").upper()
        item_name = remove_arg_tokens(text)
        if not item_name:
            await ctx.send("I need an item name.")
            return

        found, status, removed = subtract_inventory(item_name, qty, location=loc)
        if not found:
            await ctx.send(f"⚠️ I couldn't find `{item_name}`{f' at {loc}' if loc else ''}.")
            return

        log_inventory_event("remove", item_name, removed, location=loc, notes="inventory removed from storage")
        if status != "ok":
            await ctx.send(
                f"⚠️ Removed {removed:g} of requested {qty:g} {item_name}. "
                f"Inventory was short by {qty - removed:g}."
            )
            return
        await ctx.send(f"✅ Removed: {item_name} x {removed:g}{f' from {loc}' if loc else ''}")

    @bot.command(name="gmove")
    async def gmove(ctx, *, text: str = ""):
        if not text.strip():
            await ctx.send("Use: `!gmove wire nuts qty:25 from:E2 to:D2`")
            return

        try:
            qty = float(get_arg(text, "qty", 1))
        except ValueError:
            await ctx.send("`qty:` needs to be a number.")
            return
        if qty <= 0:
            await ctx.send("`qty:` needs to be greater than zero.")
            return

        source = (get_arg(text, "from", "") or "").upper()
        destination = (get_arg(text, "to", "") or "").upper()
        item_name = remove_arg_tokens(text)

        if not item_name or not source or not destination:
            await ctx.send("Use: `!gmove wire nuts qty:25 from:E2 to:D2`")
            return
        if source == destination:
            await ctx.send("Source and destination are the same.")
            return

        source_rows = find_inventory(item_name, source)
        available = sum(float(r.get("quantity") or 0) for r in source_rows)
        if available < qty:
            await ctx.send(
                f"⚠️ I only see {available:g} {item_name} at {source}; "
                f"I did not move anything."
            )
            return

        # Preserve useful metadata from the source row when creating/updating destination stock.
        category = source_rows[0].get("category") or "" if source_rows else ""
        unit = source_rows[0].get("unit") or "" if source_rows else ""
        notes = source_rows[0].get("notes") or "" if source_rows else ""

        _, status, removed = subtract_inventory(item_name, qty, location=source)
        if status != "ok" or removed != qty:
            await ctx.send("⚠️ I couldn't complete that move cleanly, so I did not add anything to the destination.")
            return

        add_or_update_inventory(item_name, qty, location=destination, category=category, unit=unit, notes=notes)
        log_inventory_event("move", item_name, qty, location=destination, unit=unit, notes=f"moved from {source} to {destination}")
        await ctx.send(f"✅ Moved: {item_name} x {qty:g} — {source} → {destination}")

    # Backward-compatible alias. This now means only "remove from physical inventory".
    @bot.command(name="gused")
    async def gused(ctx, *, text: str = ""):
        cleaned = re.sub(r"\bjob:\S+", "", text, flags=re.IGNORECASE).strip()
        await gremove(ctx, text=cleaned)
