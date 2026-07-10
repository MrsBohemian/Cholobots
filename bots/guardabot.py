import asyncio
import base64
import hashlib
import json
import re
#import sqlite3
import os
from supabase import create_client
from datetime import datetime
from pathlib import Path
from typing import Optional

from discord.ext import commands

try:
    from config import client as openai_client
except ImportError:
    from openai import OpenAI
    openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
#from config import GUARDABOT_DB

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)



# Pending receipt previews. Data is not saved until the user replies SAVE.
receipt_parse_sessions = {}
RECEIPT_MODEL = os.getenv("GUARDABOT_RECEIPT_MODEL", "gpt-5-mini")
SUPPORTED_RECEIPT_TYPES = {
    "application/pdf",
    "image/jpeg",
    "image/png",
    "image/webp",
}

RECEIPT_JSON_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "vendor": {"type": "string"},
        "transaction_date": {"type": ["string", "null"]},
        "receipt_number": {"type": ["string", "null"]},
        "transaction_type": {
            "type": "string",
            "enum": ["purchase", "return", "exchange", "reimbursement"]
        },
        "subtotal": {"type": "number"},
        "tax": {"type": "number"},
        "total": {"type": "number"},
        "currency": {"type": "string"},
        "notes": {"type": ["string", "null"]},
        "items": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "line_type": {
                        "type": "string",
                        "enum": ["purchase", "return"]
                    },
                    "sku": {"type": ["string", "null"]},
                    "description": {"type": "string"},
                    "quantity": {"type": "number"},
                    "unit": {"type": "string"},
                    "unit_price": {"type": "number"},
                    "line_total": {"type": "number"},
                    "category": {"type": "string"},
                },
                "required": [
                    "line_type", "sku", "description", "quantity", "unit",
                    "unit_price", "line_total", "category"
                ],
            },
        },
    },
    "required": [
        "vendor", "transaction_date", "receipt_number", "transaction_type",
        "subtotal", "tax", "total", "currency", "notes", "items"
    ],
}

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
# Job-cost receipt helpers
# ----------------------------


def clean_receipt_payload(payload):
    payload = dict(payload or {})
    payload["vendor"] = (payload.get("vendor") or "Unknown vendor").strip()
    payload["transaction_type"] = payload.get("transaction_type") or "purchase"
    payload["subtotal"] = round(abs(money(payload.get("subtotal"))), 2)
    payload["tax"] = round(abs(money(payload.get("tax"))), 2)
    payload["total"] = round(abs(money(payload.get("total"))), 2)
    payload["currency"] = payload.get("currency") or "USD"

    cleaned_items = []
    for raw in payload.get("items") or []:
        quantity = abs(float(raw.get("quantity") or 0))
        unit_price = abs(money(raw.get("unit_price")))
        line_total = abs(money(raw.get("line_total"), quantity * unit_price))
        description = (raw.get("description") or "").strip()
        if not description or quantity == 0:
            continue
        cleaned_items.append({
            "line_type": raw.get("line_type") if raw.get("line_type") in {"purchase", "return"} else "purchase",
            "sku": (raw.get("sku") or "").strip() or None,
            "description": description,
            "quantity": quantity,
            "unit": (raw.get("unit") or "each").strip(),
            "unit_price": round(unit_price, 2),
            "line_total": round(line_total, 2),
            "category": (raw.get("category") or "uncategorized").strip(),
        })
    payload["items"] = cleaned_items
    return payload


def parse_receipt_with_openai(filename, content_type, file_bytes):
    prompt = (
        "Extract this contractor receipt into the required schema. Capture every purchasable "
        "material line, including SKU, quantity, purchasing unit, unit price, line total, and a "
        "practical category useful for future construction estimates. Preserve returns as "
        "line_type=return and transaction_type=return or exchange. Do not invent missing values. "
        "Use 0 for unknown numeric values and null for unknown identifiers or dates. The receipt "
        "total must reflect the amount charged or credited, including tax. Tools and equipment "
        "should still be captured and categorized accurately."
    )

    if content_type == "application/pdf":
        uploaded = openai_client.files.create(
            file=(filename, file_bytes, content_type),
            purpose="user_data",
        )
        receipt_input = {"type": "input_file", "file_id": uploaded.id}
    else:
        encoded = base64.b64encode(file_bytes).decode("ascii")
        receipt_input = {
            "type": "input_image",
            "image_url": f"data:{content_type};base64,{encoded}",
            "detail": "high",
        }

    response = openai_client.responses.create(
        model=RECEIPT_MODEL,
        input=[{
            "role": "user",
            "content": [
                receipt_input,
                {"type": "input_text", "text": prompt},
            ],
        }],
        text={
            "format": {
                "type": "json_schema",
                "name": "contractor_receipt",
                "strict": True,
                "schema": RECEIPT_JSON_SCHEMA,
            }
        },
    )
    raw = response.output_text
    if not raw:
        raise ValueError("The receipt parser returned no data.")
    return clean_receipt_payload(json.loads(raw))


def format_receipt_preview(project, parsed, filename):
    tx_word = parsed.get("transaction_type", "purchase").upper()
    lines = [
        f"🧾 **RECEIPT PREVIEW — {project}**",
        f"File: `{filename}`",
        f"Vendor: **{parsed.get('vendor')}**",
        f"Date: {parsed.get('transaction_date') or 'unknown'}",
        f"Receipt/order: {parsed.get('receipt_number') or 'unknown'}",
        f"Type: {tx_word}",
        "",
        "**Harvested materials**",
    ]
    for i, item in enumerate(parsed.get("items") or [], start=1):
        sign = "−" if item.get("line_type") == "return" else ""
        lines.append(
            f"{i}. {item.get('quantity'):g} {item.get('unit')} · "
            f"{item.get('description')} · {sign}${item.get('line_total', 0):,.2f}"
            + (f" · SKU {item.get('sku')}" if item.get("sku") else "")
        )
    if not parsed.get("items"):
        lines.append("⚠️ No usable material lines were found.")
    lines.extend([
        "",
        f"Subtotal: ${parsed.get('subtotal', 0):,.2f}",
        f"Tax: ${parsed.get('tax', 0):,.2f}",
        f"Total: **${parsed.get('total', 0):,.2f}**",
        "",
        "Reply **SAVE** to write this receipt and its materials to the project.",
        "Reply **CANCEL** to discard it. Nothing has been saved yet.",
    ])
    return "\n".join(lines)


def save_parsed_receipt(session, created_by):
    parsed = session["parsed"]
    project = session["project"]
    transaction_payload = {
        "project_name": project,
        "vendor": parsed.get("vendor") or "Unknown vendor",
        "transaction_type": parsed.get("transaction_type") or "purchase",
        "transaction_date": parsed.get("transaction_date") or datetime.now().date().isoformat(),
        "subtotal": parsed.get("subtotal") or 0,
        "tax": parsed.get("tax") or 0,
        "total": parsed.get("total") or 0,
        "receipt_number": parsed.get("receipt_number"),
        "verification_status": "ai_parsed_user_confirmed",
        "notes": parsed.get("notes"),
        "created_by": created_by,
        "source_filename": session.get("filename"),
        "source_attachment_url": session.get("attachment_url"),
        "source_file_sha256": session.get("sha256"),
        "parser_model": RECEIPT_MODEL,
        "raw_parser_json": parsed,
    }
    rows = supabase.table("guard_job_transactions").insert(transaction_payload).execute().data or []
    if not rows:
        raise RuntimeError("Transaction insert returned no row.")
    tx = rows[0]

    item_rows = []
    for item in parsed.get("items") or []:
        item_rows.append({
            **item,
            "transaction_id": tx["id"],
            "project_name": project,
            "vendor": parsed.get("vendor") or "Unknown vendor",
        })
    if item_rows:
        supabase.table("guard_job_items").insert(item_rows).execute()
    return tx, item_rows


def parse_pipe_fields(raw: str):
    parts = [p.strip() for p in (raw or "").split("|") if p.strip()]
    if not parts:
        return "", {}
    project = parts[0]
    fields = {}
    aliases = {
        "vendor": "vendor", "date": "transaction_date", "type": "transaction_type",
        "total": "total", "amount": "total", "subtotal": "subtotal", "tax": "tax",
        "receipt": "receipt_number", "order": "receipt_number", "status": "verification_status",
        "notes": "notes", "note": "notes",
    }
    for p in parts[1:]:
        if ":" not in p:
            continue
        k, v = p.split(":", 1)
        mapped = aliases.get(k.strip().lower())
        if mapped:
            fields[mapped] = v.strip()
    return project.strip(), fields


def money(value, default=0.0):
    cleaned = re.sub(r"[^0-9.\-]", "", str(value or ""))
    try:
        return float(cleaned)
    except ValueError:
        return float(default)


def parse_receipt_item_line(line: str):
    """
    Format:
      purchase | 2 | each | 2.98 | 1.5 inch utility brush | sku:455441 | category:paint supplies
      return   | 12 | piece | 3.38 | Marmo Nero trim | sku:101458388

    line_type may be purchase, return, or use. Amounts are stored positive;
    line_type controls whether Crudobot treats them as a cost or credit.
    """
    parts = [p.strip() for p in line.split("|")]
    if len(parts) < 5:
        return None

    line_type = parts[0].lower()
    if line_type not in {"purchase", "return", "use"}:
        return None

    try:
        quantity = float(parts[1])
        unit_price = money(parts[3])
    except ValueError:
        return None

    unit = parts[2]
    description = parts[4]
    sku = ""
    category = ""
    line_total = quantity * unit_price

    for extra in parts[5:]:
        if ":" not in extra:
            continue
        k, v = extra.split(":", 1)
        k = k.strip().lower()
        v = v.strip()
        if k == "sku":
            sku = v
        elif k == "category":
            category = v
        elif k in {"line_total", "total"}:
            line_total = money(v, line_total)

    return {
        "line_type": line_type,
        "description": description,
        "sku": sku or None,
        "quantity": quantity,
        "unit": unit,
        "unit_price": unit_price,
        "line_total": round(line_total, 2),
        "category": category or None,
    }


def create_job_transaction(project, fields, created_by):
    tx_type = (fields.get("transaction_type") or "purchase").strip().lower()
    if tx_type not in {"purchase", "return", "exchange", "reimbursement", "planned"}:
        raise ValueError("type must be purchase, return, exchange, reimbursement, or planned")

    payload = {
        "project_name": project,
        "vendor": fields.get("vendor") or "Unknown vendor",
        "transaction_type": tx_type,
        "transaction_date": fields.get("transaction_date") or datetime.now().date().isoformat(),
        "subtotal": money(fields.get("subtotal")),
        "tax": money(fields.get("tax")),
        "total": money(fields.get("total")),
        "receipt_number": fields.get("receipt_number") or None,
        "verification_status": fields.get("verification_status") or "verified",
        "notes": fields.get("notes") or None,
        "created_by": created_by,
    }
    rows = supabase.table("guard_job_transactions").insert(payload).execute().data or []
    return rows[0] if rows else None


def add_transaction_items(transaction_id, project, vendor, item_lines):
    rows = []
    for line in item_lines:
        parsed = parse_receipt_item_line(line)
        if not parsed:
            continue
        parsed.update({
            "transaction_id": transaction_id,
            "project_name": project,
            "vendor": vendor,
        })
        rows.append(parsed)
    if rows:
        supabase.table("guard_job_items").insert(rows).execute()
    return rows


def signed_transaction_total(row):
    total = float(row.get("total") or 0)
    return -total if row.get("transaction_type") == "return" else total


def signed_item_total(row):
    total = float(row.get("line_total") or 0)
    return -total if row.get("line_type") == "return" else total


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

5. PARSE A RECEIPT INTO A PROJECT
`!greceipt Michael Lawrence` + attach a PDF/photo
Guardabot harvests vendor, totals, SKUs, quantities, units, and unit costs.
Review the preview, then reply `SAVE` or `CANCEL`.
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

    @bot.command(name="greceipt")
    async def greceipt_attachment(ctx, *, project: str = ""):
        """Parse one attached receipt and wait for SAVE before writing to Supabase."""
        project = project.strip()
        if not project:
            await ctx.send("Use: `!greceipt Michael Lawrence` and attach one PDF or receipt photo.")
            return
        if len(ctx.message.attachments) != 1:
            await ctx.send("Attach exactly one PDF or receipt image to `!greceipt Project Name`.")
            return

        attachment = ctx.message.attachments[0]
        content_type = (attachment.content_type or "").split(";", 1)[0].lower()
        if content_type not in SUPPORTED_RECEIPT_TYPES:
            await ctx.send("I can parse PDF, JPG, PNG, or WEBP receipts.")
            return
        if attachment.size and attachment.size > 15 * 1024 * 1024:
            await ctx.send("That receipt is larger than 15 MB. Please use a smaller PDF or image.")
            return

        await ctx.send(f"🔎 Reading `{attachment.filename}` and harvesting material quantities...")
        try:
            file_bytes = await attachment.read()
            file_hash = hashlib.sha256(file_bytes).hexdigest()
            existing = (
                supabase.table("guard_job_transactions")
                .select("id,project_name,vendor,total")
                .eq("source_file_sha256", file_hash)
                .limit(1)
                .execute()
            ).data or []
            if existing:
                row = existing[0]
                await ctx.send(
                    f"⚠️ This exact receipt is already saved under **{row.get('project_name')}** "
                    f"for {row.get('vendor')} (${float(row.get('total') or 0):,.2f})."
                )
                return

            parsed = await asyncio.to_thread(
                parse_receipt_with_openai,
                attachment.filename,
                content_type,
                file_bytes,
            )
        except Exception as exc:
            await ctx.send(f"I could not parse that receipt: `{type(exc).__name__}: {exc}`")
            return

        receipt_parse_sessions[ctx.author.id] = {
            "project": project,
            "parsed": parsed,
            "filename": attachment.filename,
            "attachment_url": attachment.url,
            "sha256": file_hash,
        }
        await send_long(ctx, format_receipt_preview(project, parsed, attachment.filename))

    @bot.listen("on_message")
    async def handle_receipt_confirmation(message):
        if message.author.bot or message.content.startswith("!"):
            return
        session = receipt_parse_sessions.get(message.author.id)
        if not session:
            return

        answer = message.content.strip().lower()
        if answer in {"cancel", "no", "discard", "stop"}:
            receipt_parse_sessions.pop(message.author.id, None)
            await message.channel.send("🗑️ Receipt discarded. Nothing was saved.")
            return
        if answer not in {"save", "yes", "1", "confirm"}:
            await message.channel.send("Reply `SAVE` to store it or `CANCEL` to discard it.")
            return

        try:
            tx, items = save_parsed_receipt(session, str(message.author))
        except Exception as exc:
            await message.channel.send(f"Could not save the receipt: `{type(exc).__name__}: {exc}`")
            return

        receipt_parse_sessions.pop(message.author.id, None)
        await message.channel.send(
            f"✅ Saved **{tx.get('vendor')}** receipt to **{session['project']}**.\n"
            f"Total: ${float(tx.get('total') or 0):,.2f}\n"
            f"Material lines harvested for Crudobot: {len(items)}"
        )

    @bot.command(name="gmanualreceipt")
    async def gmanualreceipt(ctx, *, raw: str = ""):
        """
        Save one receipt plus its material quantities.

        First line:
        !gmanualreceipt Michael Lawrence | vendor: Home Depot | date: 2026-07-06 | total: 71.45 | tax: 5.45 | receipt: 580-62-2785

        Following lines:
        purchase | 2 | each | 2.98 | 1.5 inch utility brush | sku:455441 | category:paint supplies
        purchase | 1 | pack | 11.48 | mini roller covers | sku:1001287489 | category:paint supplies
        """
        if not raw.strip():
            await ctx.send(
                "Use a header plus one material per line. Example:\n"
                "`!gmanualreceipt Michael Lawrence | vendor: Home Depot | total: 71.45 | tax: 5.45`\n"
                "`purchase | 2 | each | 2.98 | utility brush | sku:455441`"
            )
            return

        lines = [line.strip() for line in raw.splitlines() if line.strip()]
        project, fields = parse_pipe_fields(lines[0])
        if not project or not fields.get("total"):
            await ctx.send("I need a project name and receipt total.")
            return

        try:
            tx = create_job_transaction(project, fields, str(ctx.author))
        except Exception as exc:
            await ctx.send(f"Could not save receipt: `{exc}`")
            return

        item_rows = add_transaction_items(
            tx["id"], project, tx.get("vendor") or "Unknown vendor", lines[1:]
        )
        parsed_total = sum(signed_item_total(row) for row in item_rows)

        await ctx.send(
            f"🧾 Saved receipt for **{project}**\n"
            f"Vendor: {tx.get('vendor')}\n"
            f"Receipt total: ${float(tx.get('total') or 0):,.2f}\n"
            f"Material lines harvested: {len(item_rows)}\n"
            f"Line-item net before tax: ${parsed_total:,.2f}"
        )

    @bot.command(name="gtransaction")
    async def gtransaction(ctx, *, raw: str = ""):
        """Save an unitemized purchase, return, reimbursement, or planned cost."""
        project, fields = parse_pipe_fields(raw)
        if not project or not fields.get("total"):
            await ctx.send(
                "Use: `!gtransaction Michael Lawrence | vendor: Floor & Decor | total: 1436.63 | type: purchase | status: missing_receipt`"
            )
            return
        try:
            tx = create_job_transaction(project, fields, str(ctx.author))
        except Exception as exc:
            await ctx.send(f"Could not save transaction: `{exc}`")
            return
        await ctx.send(
            f"✅ Saved {tx.get('transaction_type')} for **{project}**: "
            f"{tx.get('vendor')} ${float(tx.get('total') or 0):,.2f}"
        )

    @bot.command(name="gproject")
    async def gproject(ctx, *, project: str = ""):
        project = project.strip()
        if not project:
            await ctx.send("Use: `!gproject Michael Lawrence`")
            return

        txs = (
            supabase.table("guard_job_transactions")
            .select("*")
            .ilike("project_name", project)
            .order("transaction_date")
            .execute()
        ).data or []
        items = (
            supabase.table("guard_job_items")
            .select("*")
            .ilike("project_name", project)
            .execute()
        ).data or []

        if not txs:
            await ctx.send(f"No material transactions saved for **{project}**.")
            return

        actual = sum(
            signed_transaction_total(t)
            for t in txs
            if t.get("transaction_type") != "planned"
        )
        planned = sum(
            float(t.get("total") or 0)
            for t in txs
            if t.get("transaction_type") == "planned"
        )

        lines = [
            f"📦 **{project} — MATERIAL LEDGER**",
            "",
            f"Actual net materials: **${actual:,.2f}**",
            f"Planned additional materials: **${planned:,.2f}**",
            f"Projected materials: **${actual + planned:,.2f}**",
            f"Receipts/transactions: {len(txs)}",
            f"Harvested material lines: {len(items)}",
            "",
            "**Transactions**",
        ]
        for t in txs:
            sign = "−" if t.get("transaction_type") == "return" else "+"
            lines.append(
                f"{t.get('transaction_date')} · {t.get('vendor')} · "
                f"{t.get('transaction_type')} · {sign}${float(t.get('total') or 0):,.2f}"
            )

        if items:
            lines.extend(["", "**Materials harvested for future estimating**"])
            for item in items[:25]:
                prefix = "RETURN" if item.get("line_type") == "return" else "USE"
                lines.append(
                    f"• {prefix}: {item.get('quantity'):g} {item.get('unit') or ''} "
                    f"{item.get('description')} @ ${float(item.get('unit_price') or 0):,.2f}"
                )
        await send_long(ctx, "\n".join(lines))

    @bot.command(name="gmaterials")
    async def gmaterials(ctx, *, project: str = ""):
        """Show quantity and unit-cost history that Crudobot can reuse."""
        project = project.strip()
        if not project:
            await ctx.send("Use: `!gmaterials Michael Lawrence`")
            return
        items = (
            supabase.table("guard_job_items")
            .select("*")
            .ilike("project_name", project)
            .order("description")
            .execute()
        ).data or []
        if not items:
            await ctx.send(f"No harvested material quantities for **{project}** yet.")
            return
        lines = [f"🧱 **{project} — MATERIAL QUANTITIES**"]
        for item in items:
            action = "RETURN" if item.get("line_type") == "return" else "PURCHASE"
            lines.append(
                f"{action} · {item.get('quantity'):g} {item.get('unit') or ''} · "
                f"{item.get('description')} · ${float(item.get('unit_price') or 0):,.2f}/unit · "
                f"{item.get('vendor')}"
            )
        await send_long(ctx, "\n".join(lines))
