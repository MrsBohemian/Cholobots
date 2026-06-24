import os
import re
import traceback
from datetime import date, datetime, timedelta

from config import client
from supabase import create_client


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def today_date():
    return date.today().isoformat()


def now_iso():
    return datetime.now().isoformat()


def short(text, limit=220):
    text = (text or "").replace("\n", " · ").strip()
    return text[:limit] + ("..." if len(text) > limit else "")


async def send_long(ctx, text, limit=1900):
    text = str(text or "").strip() or "(empty)"
    for i in range(0, len(text), limit):
        await ctx.send(text[i:i + limit])


def phone_digits(text):
    m = re.search(r"(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})", text or "")
    return re.sub(r"\D", "", m.group(1)) if m else None


def extract_followup_date(text):
    text = text or ""

    m = re.search(r"follow[\s-]*up\s*(?:on)?\s*(\d{4}-\d{2}-\d{2})", text, re.I)
    if m:
        return m.group(1)

    if "tomorrow" in text.lower():
        return (date.today() + timedelta(days=1)).isoformat()

    if "next week" in text.lower():
        return (date.today() + timedelta(days=7)).isoformat()

    return None


def infer_lookup(raw):
    raw = (raw or "").strip()
    phone = phone_digits(raw)

    if phone:
        return phone

    if "|" in raw:
        return raw.split("|", 1)[0].strip()

    if "-" in raw:
        return raw.split("-", 1)[0].strip()

    words = raw.split()
    if len(words) >= 2:
        return " ".join(words[:2])

    return raw


def find_contact(lookup):
    lookup = (lookup or "").strip()
    if not lookup:
        return None

    phone = phone_digits(lookup)

    if phone:
        rows = (
            supabase.table("chisme_contacts")
            .select("*")
            .ilike("phone", f"%{phone}%")
            .limit(1)
            .execute()
        ).data or []
        if rows:
            return rows[0]

    rows = (
        supabase.table("chisme_contacts")
        .select("*")
        .ilike("name", f"%{lookup}%")
        .limit(1)
        .execute()
    ).data or []

    return rows[0] if rows else None


def create_contact_stub(lookup, raw_note=""):
    phone = phone_digits(lookup) or phone_digits(raw_note)
    name = lookup.strip() or phone or "Unknown customer"

    payload = {
        "name": name,
        "source": "chismebot",
        "source_customer_name": name,
        "phone": phone,
        "status": "lead",
        "active_communication": True,
        "active_reason": raw_note[:300] if raw_note else "Needs reconstruction",
        "active_since": today_date(),
        "active_priority": 50,
        "next_followup_date": today_date(),
        "next_contact_date": today_date(),
        "chisme_summary": f"Placeholder contact created from Chismebot note: {raw_note[:500]}",
        "last_outcome": raw_note[:500],
        "updated_at": now_iso(),
    }

    rows = (
        supabase.table("chisme_contacts")
        .insert(payload)
        .execute()
    ).data or []

    return rows[0] if rows else None


def find_or_create_contact(raw):
    lookup = infer_lookup(raw)
    contact = find_contact(lookup)
    if contact:
        return contact
    return create_contact_stub(lookup, raw)


def interaction_type(raw):
    t = (raw or "").lower()
    if "text" in t:
        return "text"
    if "estimate" in t or "quote" in t:
        return "estimate"
    if "paid" in t or "payment" in t:
        return "payment"
    if "invoice" in t:
        return "invoice"
    if "call" in t or "called" in t or "voicemail" in t:
        return "call"
    return "chisme"


def synthesize_summary(contact, raw_note):
    existing = contact.get("chisme_summary") or ""

    try:
        resp = client.responses.create(
            model="gpt-5-mini",
            reasoning={"effort": "low"},
            input=[
                {
                    "role": "system",
                    "content": (
                        "Update a practical contractor CRM customer summary. "
                        "Use only the facts provided. Do not invent. "
                        "Keep it useful for future calls, estimates, and subcontractor handoffs. "
                        "If the contact is incomplete, say what still needs reconstruction."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Customer/contact label: {contact.get('name')}\n\n"
                        f"Existing summary:\n{existing}\n\n"
                        f"New note:\n{raw_note}"
                    ),
                },
            ],
            max_output_tokens=500,
        )

        text = getattr(resp, "output_text", "") or ""
        return text.strip()[:1800] if text.strip() else (existing + "\n" + raw_note)[:1800]

    except Exception:
        return (existing + "\n" + raw_note).strip()[:1800]


def save_interaction(contact, raw_note, created_by=None, counts_today=True):
    followup = extract_followup_date(raw_note)

    payload = {
        "contact_id": contact["id"],
        "interaction_date": today_date(),
        "interaction_type": interaction_type(raw_note),
        "notes": raw_note,
        "outcome": raw_note[:500],
        "next_action": None,
        "next_contact_date": followup,
        "next_followup_date": followup,
        "created_by": created_by,
        "touch_counts_for_today": counts_today,
    }

    return (
        supabase.table("chisme_interactions")
        .insert(payload)
        .execute()
    )


def update_contact_from_chisme(contact, raw_note):
    followup = extract_followup_date(raw_note)
    summary = synthesize_summary(contact, raw_note)

    updates = {
        "chisme_summary": summary,
        "last_contact_date": today_date(),
        "last_outcome": raw_note[:500],
        "active_communication": True,
        "active_reason": raw_note[:300],
        "active_since": contact.get("active_since") or today_date(),
        "daily_touch_date": today_date(),
        "daily_touch_counted": True,
        "updated_at": now_iso(),
    }

    phone = phone_digits(raw_note)
    if phone and not contact.get("phone"):
        updates["phone"] = phone

    if followup:
        updates["next_followup_date"] = followup
        updates["next_contact_date"] = followup

    (
        supabase.table("chisme_contacts")
        .update(updates)
        .eq("id", contact["id"])
        .execute()
    )


def parse_cset(raw):
    parts = [p.strip() for p in raw.split("|") if p.strip()]
    if not parts:
        return "", {}

    lookup = parts[0]
    updates = {}

    field_map = {
        "name": "name",
        "phone": "phone",
        "email": "email",
        "address": "address",
        "source": "source",
        "status": "status",
        "preferred": "preferred_contact_method",
        "preferred contact": "preferred_contact_method",
        "next": "next_action",
        "next action": "next_action",
        "followup": "next_followup_date",
        "follow up": "next_followup_date",
    }

    for p in parts[1:]:
        if ":" in p:
            key, value = p.split(":", 1)
        elif " " in p:
            key, value = p.split(" ", 1)
        else:
            continue

        key = key.strip().lower()
        value = value.strip()

        col = field_map.get(key)
        if col and value:
            updates[col] = value

    updates["updated_at"] = now_iso()
    return lookup, updates


def get_recent_interactions(contact_id, limit=8):
    return (
        supabase.table("chisme_interactions")
        .select("*")
        .eq("contact_id", contact_id)
        .order("created_at", desc=True)
        .limit(limit)
        .execute()
    ).data or []


def get_active_contacts():
    return (
        supabase.table("chisme_contacts")
        .select("*")
        .eq("active_communication", True)
        .order("active_priority", desc=False)
        .order("next_followup_date", desc=False)
        .execute()
    ).data or []


def get_today_touch_count():
    res = (
        supabase.table("chisme_interactions")
        .select("id", count="exact")
        .eq("interaction_date", today_date())
        .eq("touch_counts_for_today", True)
        .execute()
    )
    return res.count or 0


def register_chisme(bot):

    @bot.command(name="chismebot")
    async def chismebot_help(ctx):
        await ctx.send(
            "💬 **CHISMEBOT**\n\n"
            "`!chisme <messy customer note>`\n"
            "Capture anything: name, phone number, job note, reminder, source, or reconstruction clue. "
            "Chismebot finds/creates a Rolodex card, saves the journal note, and keeps it active.\n"
            "Example: `!chisme 2107235619 TV mount`\n\n"
            "`!cset <lookup> | field: value | field: value`\n"
            "Complete or update the Rolodex card.\n"
            "Example: `!cset 2107235619 | name: John Dreese | phone: 2102887136 | address: 5230 Sagerock Pass`\n\n"
            "`!clist`\n"
            "Show active customer communication list.\n\n"
            "`!cshow <lookup>`\n"
            "Show Rolodex card plus recent chisme notes.\n\n"
            "`!cremove <lookup> | follow up YYYY-MM-DD | note`\n"
            "Remove from active list and schedule future follow-up.\n\n"
            "`!cprogress 10`\n"
            "Show today’s customer communication progress."
        )

    @bot.command(name="chisme")
    async def chisme(ctx, *, raw=""):
        if not raw.strip():
            await ctx.send("Use: `!chisme <messy customer note>`")
            return

        try:
            contact = find_or_create_contact(raw)
            if not contact:
                await ctx.send("⚠️ Could not create/find Rolodex card.")
                return

            save_interaction(contact, raw, created_by=str(ctx.author), counts_today=True)
            update_contact_from_chisme(contact, raw)

            await ctx.send(
                f"✅ Chisme captured.\n"
                f"Rolodex: **{contact.get('name')}**\n"
                f"Active card: yes\n"
                f"Note: {short(raw, 300)}"
            )

        except Exception:
            print("=== CHISME ERROR ===")
            traceback.print_exc()
            await ctx.send("⚠️ Chismebot error. Check Railway logs.")

    @bot.command(name="cset")
    async def cset(ctx, *, raw=""):
        if not raw.strip():
            await ctx.send("Use: `!cset <lookup> | name: X | phone: X | address: X`")
            return

        try:
            lookup, updates = parse_cset(raw)
            contact = find_contact(lookup)

            if not contact:
                contact = create_contact_stub(lookup, f"Created during cset: {raw}")

            if not updates:
                await ctx.send("I found/created the card, but I didn’t see fields to update.")
                return

            (
                supabase.table("chisme_contacts")
                .update(updates)
                .eq("id", contact["id"])
                .execute()
            )

            save_interaction(
                contact,
                f"Rolodex updated: {raw}",
                created_by=str(ctx.author),
                counts_today=False,
            )

            await ctx.send(f"✅ Rolodex updated for **{updates.get('name') or contact.get('name')}**")

        except Exception:
            print("=== CSET ERROR ===")
            traceback.print_exc()
            await ctx.send("⚠️ Could not update Rolodex card.")

    @bot.command(name="clist")
    async def clist(ctx):
        contacts = get_active_contacts()

        if not contacts:
            await ctx.send("No active customer communication right now.")
            return

        lines = ["📇 **Active Customer Communication**\n"]

        for i, c in enumerate(contacts, 1):
            lines.append(
                f"{i}. **{c.get('name') or 'Unknown'}**\n"
                f"   Reason: {c.get('active_reason') or 'Needs reconstruction'}\n"
                f"   Phone: {c.get('phone') or 'not saved'}\n"
                f"   Follow-up: {c.get('next_followup_date') or c.get('next_contact_date') or 'not set'}\n"
                f"   Chisme: {short(c.get('chisme_summary'), 160)}\n"
            )

        await send_long(ctx, "\n".join(lines))

    @bot.command(name="cshow")
    async def cshow(ctx, *, lookup=""):
        if not lookup.strip():
            await ctx.send("Use: `!cshow <name or phone>`")
            return

        contact = find_contact(lookup)

        if not contact:
            await ctx.send(f"No Rolodex card found for: {lookup}")
            return

        interactions = get_recent_interactions(contact["id"])

        lines = [
            f"📇 **{contact.get('name')}**",
            f"Phone: {contact.get('phone') or 'not saved'}",
            f"Email: {contact.get('email') or 'not saved'}",
            f"Address: {contact.get('address') or 'not saved'}",
            f"Source: {contact.get('source') or 'not saved'}",
            f"Status: {contact.get('status') or 'unknown'}",
            f"Active: {contact.get('active_communication')}",
            f"Reason: {contact.get('active_reason') or 'none'}",
            f"Next follow-up: {contact.get('next_followup_date') or contact.get('next_contact_date') or 'not set'}",
            "",
            "**Chisme summary:**",
            contact.get("chisme_summary") or "No summary yet.",
            "",
            "**Recent journal:**",
        ]

        if not interactions:
            lines.append("No chisme notes yet.")
        else:
            for item in interactions:
                lines.append(
                    f"- {item.get('interaction_date')}: "
                    f"{item.get('interaction_type') or 'note'} — {short(item.get('notes'), 180)}"
                )

        await send_long(ctx, "\n".join(lines))

    @bot.command(name="cremove")
    async def cremove(ctx, *, raw=""):
        if not raw.strip():
            await ctx.send("Use: `!cremove <lookup> | follow up YYYY-MM-DD | note`")
            return

        parts = [p.strip() for p in raw.split("|") if p.strip()]
        lookup = parts[0]
        note = " | ".join(parts[1:]) if len(parts) > 1 else "Removed from active communication."
        followup = extract_followup_date(note) or (date.today() + timedelta(days=30)).isoformat()

        contact = find_contact(lookup)
        if not contact:
            await ctx.send(f"No Rolodex card found for: {lookup}")
            return

        (
            supabase.table("chisme_contacts")
            .update({
                "active_communication": False,
                "active_reason": None,
                "next_followup_date": followup,
                "next_contact_date": followup,
                "last_outcome": note[:500],
                "updated_at": now_iso(),
            })
            .eq("id", contact["id"])
            .execute()
        )

        save_interaction(
            contact,
            f"Removed from active list. {note}",
            created_by=str(ctx.author),
            counts_today=False,
        )

        await ctx.send(
            f"✅ Removed from active list: **{contact.get('name')}**\n"
            f"Next follow-up: {followup}"
        )

    @bot.command(name="cprogress")
    async def cprogress(ctx, target: int = 10):
        count = get_today_touch_count()
        pct = min(100, round((count / target) * 100)) if target else 0
        filled = max(0, min(10, round(pct / 10)))
        bar = "█" * filled + "░" * (10 - filled)

        await ctx.send(
            f"📞 **Customer Communication Progress**\n\n"
            f"{bar} {count} / {target}\n"
            f"{pct}% complete today"
        )
