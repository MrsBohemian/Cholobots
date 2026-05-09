import json
import re
#import sqlite3
import os
from supabase import create_client
from datetime import datetime
from pathlib import Path
from typing import Optional

from discord.ext import commands
#from config import GUARDABOT_DB

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ----------------------------
# Helpers
# ----------------------------

VALID_ROWS = ["A", "B", "C", "D", "E"]
VALID_COLS = ["1", "2", "3", "4", "5"]


def now_iso():
    return datetime.now().isoformat(timespec="seconds")

def normalize_cell(cell: str) -> Optional[str]:
    cell = cell.strip().upper()
    if re.fullmatch(r"[A-E][1-5]", cell):
        return cell
    return None


def parse_cells(cell_text: str):
    """
    Accepts:
    A1
    A1-A2
    A1,A2,B3
    """
    cell_text = cell_text.strip().upper().replace(" ", "")

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
    """
    Finds values like:
    qty:4
    loc:A1
    job:Gardina
    cost:45.98
    vendor:HomeDepot
    """
    match = re.search(rf"{key}:(\S+)", text, flags=re.IGNORECASE)
    return match.group(1).strip() if match else default


def remove_arg_tokens(text: str):
    return re.sub(r"\b(qty|loc|location|job|cost|vendor|category):\S+", "", text, flags=re.IGNORECASE).strip()


def init_guardabot_tables():
    with db_connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS garage_zones (
                cell TEXT PRIMARY KEY,
                zone_name TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS inventory_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_name TEXT NOT NULL,
                quantity REAL DEFAULT 0,
                unit TEXT DEFAULT '',
                location TEXT DEFAULT '',
                category TEXT DEFAULT '',
                notes TEXT DEFAULT '',
                updated_at TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS material_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                item_name TEXT NOT NULL,
                quantity REAL DEFAULT 0,
                unit TEXT DEFAULT '',
                location TEXT DEFAULT '',
                job TEXT DEFAULT '',
                vendor TEXT DEFAULT '',
                cost REAL DEFAULT 0,
                notes TEXT DEFAULT '',
                ts TEXT NOT NULL
            )
        """)

        conn.commit()


def find_inventory(item_name: str):
    response = supabase.table("inventory_items") \
        .select("*") \
        .ilike("item_name", f"%{item_name}%") \
        .execute()

    return response.data or []
    

def add_or_update_inventory(item_name, qty, location="", category="", unit="", notes=""):
    response = supabase.table("inventory_items") \
        .select("*") \
        .eq("item_name", item_name) \
        .eq("location", location) \
        .execute()

    existing = response.data

    if existing:
        row = existing[0]

        new_qty = float(row["quantity"] or 0) + float(qty or 0)

        supabase.table("inventory_items").update({
            "quantity": new_qty,
            "category": category or row.get("category", ""),
            "unit": unit or row.get("unit", ""),
            "notes": notes or row.get("notes", ""),
            "updated_at": now_iso()
        }).eq("id", row["id"]).execute()

    else:
        supabase.table("inventory_items").insert({
            "item_name": item_name,
            "quantity": qty,
            "unit": unit,
            "location": location,
            "category": category,
            "notes": notes,
            "updated_at": now_iso()
        }).execute()


def subtract_inventory(item_name, qty):
    rows = find_inventory(item_name)

    if not rows:
        return False, "not_found"

    remaining = float(qty or 0)

    for row in rows:
        if remaining <= 0:
            break

        current_qty = float(row["quantity"] or 0)

        take = min(current_qty, remaining)

        new_qty = current_qty - take

        remaining -= take

        supabase.table("inventory_items").update({
            "quantity": new_qty,
            "updated_at": now_iso()
        }).eq("id", row["id"]).execute()

    if remaining > 0:
        return True, f"partial_short_by_{remaining}"

    return True, "ok"


def log_event(event_type, item_name, qty=0, location="", job="", vendor="", cost=0, unit="", notes=""):
    supabase.table("material_events").insert({
        "event_type": event_type,
        "item_name": item_name,
        "quantity": qty,
        "unit": unit,
        "location": location,
        "job": job,
        "vendor": vendor,
        "cost": float(cost or 0),
        "notes": notes
    }).execute()


# ----------------------------
# Discord Commands
# ----------------------------

def register_guard(bot):

    # init_guardabot_tables()

    @bot.command(name="guardabot")
    async def guardabot_help(ctx):
        msg = """
📦 GUARDABOT

Guardabot tracks the garage map, inventory, purchases, and materials used on jobs.

CORE FLOW

1. MAP
`!gmap`
Show the 5x5 garage map.

`!gzone A1-A2 camping equipment`
Set a garage zone.

2. CHECK BEFORE BUYING
`!gcheck fan box`
See what we already have before buying.

3. BOUGHT / USED
`!gbought ceiling fan box qty:2 cost:45.98 loc:B2 vendor:HomeDepot`
Log purchased materials and add them to inventory.

`!gused ceiling fan box qty:1 job:Gardina`
Log materials used on a job and subtract from inventory.

4. ADD INVENTORY DIRECTLY
`!gadd wire nuts qty:50 loc:B1 category:electrical`
Add inventory without purchase info.
"""
        await ctx.send(msg)

    @bot.command(name="gmap")
    async def gmap(ctx):
        #init_guardabot_tables()

        response = supabase.table("garage_zones").select("*").execute()
        rows = response.data or []

        zone_by_cell = {row["cell"]: row["zone_name"] for row in rows}

        lines = ["🗺️ GARAGE MAP"]
        for r in VALID_ROWS:
            row_parts = []
            for c in VALID_COLS:
                cell = f"{r}{c}"
                zone = zone_by_cell.get(cell, "empty")
                row_parts.append(f"{cell}: {zone}")
            lines.append(" | ".join(row_parts))

        await ctx.send("```" + "\n".join(lines) + "```")

    @bot.command(name="gzone")
    async def gzone(ctx, cells: str = "", *, zone_name: str = ""):
        #init_guardabot_tables()

        parsed = parse_cells(cells)
        zone_name = zone_name.strip()

        if not parsed or not zone_name:
            await ctx.send("Use: `!gzone A1-A2 camping equipment`")
            return

        for cell in parsed:
            supabase.table("garage_zones").upsert({
                "cell": cell,
                "zone_name": zone_name,
                "updated_at": now_iso()
            }).execute()

        await ctx.send(f"✅ Updated map: {', '.join(parsed)} = {zone_name}")

    @bot.command(name="gadd")
    async def gadd(ctx, *, text: str = ""):
        #init_guardabot_tables()

        if not text.strip():
            await ctx.send("Use: `!gadd wire nuts qty:50 loc:B1 category:electrical`")
            return

        qty = float(get_arg(text, "qty", 1))
        loc = get_arg(text, "loc", get_arg(text, "location", ""))
        category = get_arg(text, "category", "")
        item_name = remove_arg_tokens(text)

        if not item_name:
            await ctx.send("I need an item name. Example: `!gadd wire nuts qty:50 loc:B1 category:electrical`")
            return

        add_or_update_inventory(item_name, qty, location=loc, category=category)
        log_event("add", item_name, qty=qty, location=loc, notes="manual inventory add")

        await ctx.send(f"✅ Added: {item_name} x {qty} at {loc or 'unknown location'}")

    @bot.command(name="gcheck")
    async def gcheck(ctx, *, item_name: str = ""):
        #init_guardabot_tables()

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
                f"- {row['item_name']} | qty: {row['quantity']} {row['unit'] or ''} | loc: {row['location'] or '?'} | category: {row['category'] or '?'}"
            )

        await ctx.send("\n".join(lines))

    @bot.command(name="gbought")
    async def gbought(ctx, *, text: str = ""):
        #init_guardabot_tables()

        if not text.strip():
            await ctx.send("Use: `!gbought ceiling fan box qty:2 cost:45.98 loc:B2 vendor:HomeDepot`")
            return

        qty = float(get_arg(text, "qty", 1))
        cost = float(get_arg(text, "cost", 0))
        loc = get_arg(text, "loc", get_arg(text, "location", ""))
        vendor = get_arg(text, "vendor", "")
        category = get_arg(text, "category", "")
        item_name = remove_arg_tokens(text)

        if not item_name:
            await ctx.send("I need an item name.")
            return

        add_or_update_inventory(item_name, qty, location=loc, category=category)
        log_event("bought", item_name, qty=qty, location=loc, vendor=vendor, cost=cost)

        await ctx.send(
            f"🧾 Bought + added:\n"
            f"{item_name} x {qty}\n"
            f"Location: {loc or '?'}\n"
            f"Vendor: {vendor or '?'}\n"
            f"Cost: ${cost:.2f}"
        )

    @bot.command(name="gused")
    async def gused(ctx, *, text: str = ""):
        #init_guardabot_tables()

        if not text.strip():
            await ctx.send("Use: `!gused ceiling fan box qty:1 job:Gardina`")
            return

        qty = float(get_arg(text, "qty", 1))
        job = get_arg(text, "job", "")
        item_name = remove_arg_tokens(text)

        if not item_name:
            await ctx.send("I need an item name.")
            return

        found, status = subtract_inventory(item_name, qty)
        log_event("used", item_name, qty=qty, job=job)

        if not found:
            await ctx.send(
                f"⚠️ Logged usage for job costing, but I couldn’t find `{item_name}` in inventory."
            )
            return

        if status.startswith("partial"):
            await ctx.send(
                f"⚠️ Logged usage of {item_name} x {qty} for {job or 'unknown job'}, "
                f"but inventory may be short: {status.replace('_', ' ')}"
            )
            return

        await ctx.send(f"✅ Used: {item_name} x {qty} for job: {job or '?'}")
