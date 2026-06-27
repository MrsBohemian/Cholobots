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


def split_lookup_note(raw):
    raw = (raw or "").strip()
    if "|" in raw:
        lookup, note = raw.split("|", 1)
        return lookup.strip(), note.strip()
    return raw, None


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


def find_contacts(lookup, limit=5):
    lookup = (lookup or "").strip()
    if not lookup:
        return []

    phone = phone_digits(lookup)

    if phone:
        rows = (
            supabase.table("chisme_contacts")
            .select("*")
            .ilike("phone", f"%{phone}%")
            .limit(limit)
            .execute()
        ).data or []
        if rows:
            return rows

    return (
        supabase.table("chisme_contacts")
        .select("*")
        .ilike("name", f"%{lookup}%")
        .limit(limit)
        .execute()
    ).data or []


def create_contact_stub(label, raw_note=""):
    phone = phone_digits(label) or phone_digits(raw_note)
    name = label.strip() or phone or "Unknown customer"

    rows = (
        supabase.table("chisme_contacts")
        .insert({
            "name": name,
            "source": "chismebot",
            "source_customer_name": name,
            "phone": phone,
            "status": "lead",
            "temperature": 25,
            "chisme_summary": f"Placeholder Rolodex card created from: {raw_note or label}",
            "updated_at": now_iso(),
        })
        .execute()
    ).data or []

    contact = rows[0] if rows else None
    if contact:
        ensure_journal(contact["id"])
    return contact


def choose_one_contact(matches):
    if len(matches) == 1:
        return matches[0]
    return None


def format_match_list(matches):
    lines = ["I found multiple possible Rolodex cards:\n"]
    for i, c in enumerate(matches, 1):
        lines.append(
            f"{i}. **{c.get('name')}** — "
            f"{c.get('phone') or 'no phone'} — "
            f"{c.get('address') or c.get('source') or 'no clue'}"
        )
    lines.append("\nUse a more specific lookup, like phone number or address clue.")
    return "\n".join(lines)


def ensure_journal(contact_id):
    existing = (
        supabase.table("chisme_journals")
        .select("*")
        .eq("contact_id", contact_id)
        .limit(1)
        .execute()
    ).data or []

    if existing:
        return existing[0]

    rows = (
        supabase.table("chisme_journals")
        .insert({"contact_id": contact_id})
        .execute()
    ).data or []

    return rows[0] if rows else None


def get_journal(contact_id):
    journal = ensure_journal(contact_id)
    if not journal:
        return None, []
    notes = (
        supabase.table("chisme_notes")
        .select("*")
        .eq("journal_id", journal["id"])
        .order("created_at", desc=True)
        .limit(10)
        .execute()
    ).data or []
    return journal, notes


def synthesize_summary(contact, note):
    existing = contact.get("chisme_summary") or ""

    try:
        resp = client.responses.create(
            model="gpt-5-mini",
            reasoning={"effort": "low"},
            input=[
                {
                    "role": "system",
                    "content": (
                        "Update a practical contractor customer summary. "
                        "Use only known facts. Do not invent. "
                        "Make it useful for future calls, estimates, and subcontractor handoffs."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Customer: {contact.get('name')}\n"
                        f"Old summary:\n{existing}\n\n"
                        f"New chisme note:\n{note}"
                    ),
                },
            ],
            max_output_tokens=500,
        )
        text = getattr(resp, "output_text", "") or ""
        return text.strip()[:1800] if text.strip() else (existing + "\n" + note)[:1800]
    except Exception:
        return (existing + "\n" + note).strip()[:1800]


def add_note(contact, note, created_by=None, note_type="chisme"):
    journal = ensure_journal(contact["id"])
    if not journal:
        return None

    rows = (
        supabase.table("chisme_notes")
        .insert({
            "journal_id": journal["id"],
            "contact_id": contact["id"],
            "note_date": today_date(),
            "note_type": note_type,
            "note": note,
            "created_by": created_by,
        })
        .execute()
    ).data or []

    summary = synthesize_summary(contact, note)
    followup = extract_followup_date(note)

    updates = {
        "chisme_summary": summary,
        "last_contact_date": today_date(),
        "last_outcome": note[:500],
        "updated_at": now_iso(),
    }

    phone = phone_digits(note)
    if phone and not contact.get("phone"):
        updates["phone"] = phone

    if followup:
        updates["next_followup_date"] = followup
        updates["next_contact_date"] = followup

    supabase.table("chisme_contacts").update(updates).eq("id", contact["id"]).execute()
    return rows[0] if rows else None


def set_active(contact, reason=None, burner_position=4, owner="Daniel"):
    followup = extract_followup_date(reason or "")
    existing = (
        supabase.table("chisme_active")
        .select("*")
        .eq("contact_id", contact["id"])
        .limit(1)
        .execute()
    ).data or []

    payload = {
        "contact_id": contact["id"],
        "active_reason": reason or "Active customer communication",
        "burner_position": burner_position,
        "next_followup_date": followup,
        "active_owner": owner,
        "updated_at": now_iso(),
    }

    if existing:
        supabase.table("chisme_active").update(payload).eq("contact_id", contact["id"]).execute()
    else:
        supabase.table("chisme_active").insert(payload).execute()


def parse_fields(raw):
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
        "temp": "temperature",
        "temperature": "temperature",
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

        if not col or not value:
            continue

        if col == "temperature":
            try:
                value = max(0, min(100, int(value)))
            except ValueError:
                continue

        updates[col] = value

    updates["updated_at"] = now_iso()
    return lookup, updates


def get_active():
    return (
        supabase.table("chisme_active")
        .select("*, chisme_contacts(*)")
        .order("burner_position", desc=False)
        .order("next_followup_date", desc=False)
        .execute()
    ).data or []


def register_chisme(bot):

    @bot.command(name="chismebot")
    async def chismebot_help(ctx):
        await ctx.send(
            "💬 **CHISMEBOT**\n\n"
            "`!chisme Name`\n"
            "Show that customer’s chisme journal.\n\n"
            "`!chisme Name | note`\n"
            "Add a chisme note to that customer’s journal.\n\n"
            "`!cset Name | phone: 210... | address: ... | temp: 75`\n"
            "Complete or update the Rolodex card.\n\n"
            "`!cactive Name | burner: 1 | reason: estimate tomorrow`\n"
            "Put customer on active communication list. Burner 1 is hottest active spot; 4 is least active.\n\n"
            "`!clist`\n"
            "Show active customer communication list by burner position.\n\n"
            "`!cremove Name | follow up 2026-07-01 | note`\n"
            "Remove from active list and leave follow-up note.\n\n"
            "`!cshow Name`\n"
            "Show Rolodex card, temperature, active status, and recent chisme."
        )

    @bot.command(name="chisme")
    async def chisme(ctx, *, raw=""):
        if not raw.strip():
            await ctx.send("Use: `!chisme Name` or `!chisme Name | note`")
            return

        lookup, note = split_lookup_note(raw)
        matches = find_contacts(lookup)

        if not matches:
            if note is None:
                await ctx.send(
                    f"No Rolodex card found for **{lookup}**.\n"
                    f"To create one, use: `!chisme {lookup} | <note>`"
                )
                return
            contact = create_contact_stub(lookup, note)
        elif len(matches) > 1:
            await send_long(ctx, format_match_list(matches))
            return
        else:
            contact = matches[0]

        if note is None:
            journal, notes = get_journal(contact["id"])
            lines = [
                f"📓 **Chisme journal: {contact.get('name')}**",
                f"Phone: {contact.get('phone') or 'not saved'}",
                f"Temperature: {contact.get('temperature') or 0}",
                "",
            ]

            real_notes = [n for n in notes if n.get("note_type") != "journal_anchor" and (n.get("note") or "").strip()]

            if not real_notes:
                lines.append("No chisme notes yet.")
                lines.append(f"Add one with: `!chisme {contact.get('name')} | <note>`")
            else:
                for n in real_notes[:8]:
                    lines.append(f"- {n.get('note_date')}: {short(n.get('note'), 250)}")

            await send_long(ctx, "\n".join(lines))
            return

        add_note(contact, note, created_by=str(ctx.author))
        set_active(contact, reason=note, burner_position=4)

        await ctx.send(
            f"✅ Chisme saved for **{contact.get('name')}**.\n"
            f"Added to active list on burner 4."
        )

    @bot.command(name="cset")
    async def cset(ctx, *, raw=""):
        lookup, updates = parse_fields(raw)

        if not lookup:
            await ctx.send("Use: `!cset Name | phone: ... | address: ... | temp: 75`")
            return

        matches = find_contacts(lookup)

        if len(matches) > 1:
            await send_long(ctx, format_match_list(matches))
            return

        contact = matches[0] if matches else create_contact_stub(lookup, raw)

        if not updates:
            await ctx.send("No update fields found.")
            return

        supabase.table("chisme_contacts").update(updates).eq("id", contact["id"]).execute()
        ensure_journal(contact["id"])
        add_note(contact, f"Rolodex updated: {raw}", created_by=str(ctx.author), note_type="rolodex_update")

        await ctx.send(f"✅ Rolodex updated for **{updates.get('name') or contact.get('name')}**")

    @bot.command(name="cactive")
    async def cactive(ctx, *, raw=""):
        if not raw.strip():
            await ctx.send("Use: `!cactive Name | burner: 1 | reason: estimate tomorrow`")
            return

        parts = [p.strip() for p in raw.split("|") if p.strip()]
        lookup = parts[0]
        burner = 4
        reason = "Active customer communication"

        for p in parts[1:]:
            lower = p.lower()
            if lower.startswith("burner"):
                m = re.search(r"([1-4])", lower)
                if m:
                    burner = int(m.group(1))
            elif lower.startswith("reason"):
                reason = p.split(":", 1)[1].strip() if ":" in p else p
            else:
                reason = p

        matches = find_contacts(lookup)

        if len(matches) > 1:
            await send_long(ctx, format_match_list(matches))
            return

        contact = matches[0] if matches else create_contact_stub(lookup, reason)
        set_active(contact, reason=reason, burner_position=burner)
        ensure_journal(contact["id"])

        await ctx.send(f"✅ Active: **{contact.get('name')}** on burner {burner} — {reason}")

    @bot.command(name="clist")
    async def clist(ctx):
        rows = get_active()
        if not rows:
            await ctx.send("No active customer communication right now.")
            return

        lines = ["🔥 **Active Customer Communication**\n"]

        for row in rows:
            c = row.get("chisme_contacts") or {}
            lines.append(
                f"Burner {row.get('burner_position')}: **{c.get('name')}**\n"
                f"  Reason: {row.get('active_reason') or 'none'}\n"
                f"  Temp: {c.get('temperature') or 0}\n"
                f"  Phone: {c.get('phone') or 'not saved'}\n"
            )

        await send_long(ctx, "\n".join(lines))

    @bot.command(name="cshow")
    async def cshow(ctx, *, lookup=""):
        matches = find_contacts(lookup)

        if not matches:
            await ctx.send(f"No Rolodex card found for **{lookup}**.")
            return

        if len(matches) > 1:
            await send_long(ctx, format_match_list(matches))
            return

        c = matches[0]
        journal, notes = get_journal(c["id"])

        active = (
            supabase.table("chisme_active")
            .select("*")
            .eq("contact_id", c["id"])
            .limit(1)
            .execute()
        ).data or []

        lines = [
            f"📇 **{c.get('name')}**",
            f"Phone: {c.get('phone') or 'not saved'}",
            f"Email: {c.get('email') or 'not saved'}",
            f"Address: {c.get('address') or 'not saved'}",
            f"Source: {c.get('source') or 'not saved'}",
            f"Temperature: {c.get('temperature') or 0}",
            f"Status: {c.get('status') or 'unknown'}",
            f"Summary: {c.get('chisme_summary') or 'none'}",
            "",
        ]

        if active:
            a = active[0]
            lines.extend([
                f"🔥 Active burner: {a.get('burner_position')}",
                f"Reason: {a.get('active_reason')}",
                "",
            ])

        lines.append("Recent chisme:")
        real_notes = [n for n in notes if n.get("note_type") != "journal_anchor" and (n.get("note") or "").strip()]
        if not real_notes:
            lines.append("No chisme notes yet.")
        else:
            for n in real_notes[:8]:
                lines.append(f"- {n.get('note_date')}: {short(n.get('note'), 220)}")

        await send_long(ctx, "\n".join(lines))

    @bot.command(name="cremove")
    async def cremove(ctx, *, raw=""):
        if not raw.strip():
            await ctx.send("Use: `!cremove Name | follow up 2026-07-01 | note`")
            return

        parts = [p.strip() for p in raw.split("|") if p.strip()]
        lookup = parts[0]
        note = " | ".join(parts[1:]) if len(parts) > 1 else "Removed from active list."
        followup = extract_followup_date(note)

        matches = find_contacts(lookup)
        if len(matches) > 1:
            await send_long(ctx, format_match_list(matches))
            return
        if not matches:
            await ctx.send(f"No Rolodex card found for **{lookup}**.")
            return

        c = matches[0]

        supabase.table("chisme_active").delete().eq("contact_id", c["id"]).execute()

        updates = {"updated_at": now_iso()}
        if followup:
            updates["next_followup_date"] = followup
            updates["next_contact_date"] = followup
        supabase.table("chisme_contacts").update(updates).eq("id", c["id"]).execute()

        add_note(c, f"Removed from active list. {note}", created_by=str(ctx.author), note_type="active_removed")

        await ctx.send(f"✅ Removed **{c.get('name')}** from active list.")
