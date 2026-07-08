import os
import re
import traceback
from datetime import date, datetime, timedelta

from config import client
from supabase import create_client


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Multi-step Discord sessions
cremove_sessions = {}
hotlist_note_sessions = {}


# ------------------------------------------------------------
# Basic helpers
# ------------------------------------------------------------

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


def parse_followup_response(text):
    text = (text or "").strip().lower()

    if not text or text in {"none", "no", "no follow-up", "no followup", "5"}:
        return None

    if text == "1" or "tomorrow" in text:
        return (date.today() + timedelta(days=1)).isoformat()

    if text == "2" or "next week" in text:
        return (date.today() + timedelta(days=7)).isoformat()

    if text == "3" or "two weeks" in text or "2 weeks" in text:
        return (date.today() + timedelta(days=14)).isoformat()

    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            pass

    return None


# ------------------------------------------------------------
# Hot List 2.0 workflow
# ------------------------------------------------------------

HOTLIST_STAGES = {
    "sitevisit": {
        "temp": 60,
        "stage": "Site Visit Completed",
        "next_action": "Write estimate notes",
        "journal": "Completed site visit.\n\nNext step:\nWrite estimate notes.",
        "prompt_for_notes": True,
    },
    "notes": {
        "temp": 70,
        "stage": "Estimate Ready",
        "next_action": "Send estimate",
        "journal": "Estimate notes completed.\n\nNext step:\nSend estimate.",
        "prompt_for_notes": False,
    },
    "sent": {
        "temp": 80,
        "stage": "Estimate Sent",
        "next_action": "Follow up",
        "journal": "Estimate sent.\n\nNext step:\nFollow up.",
        "prompt_for_notes": False,
    },
    "followup": {
        "temp": 90,
        "stage": "Follow-up Completed",
        "next_action": "Await approval",
        "journal": "Follow-up completed.\n\nNext step:\nAwait approval.",
        "prompt_for_notes": False,
    },
    "approved": {
        "temp": 100,
        "stage": "Estimate Approved",
        "next_action": "Move to Stovetop",
        "journal": "Estimate approved.\n\nCustomer is ready to become an active project.",
        "prompt_for_notes": False,
    },
}


def temp_bar(temp):
    filled = max(0, min(10, int(temp or 0) // 10))
    return "█" * filled + "□" * (10 - filled)


def increment_customer_communication_loadbar(created_by=None):
    # TODO: connect this to your existing daily load bar table.
    # Rule: only call this when a hotlist workflow stage advances.
    pass


# ------------------------------------------------------------
# Rolodex / contact helpers
# ------------------------------------------------------------

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
            "phone": phone,
            "status": "lead",
            "hotlist_temperature": 50,
            "hotlist_stage": "New Lead",
            "next_action": "Contact customer / schedule site visit",
            "chisme_summary": f"Placeholder Rolodex card created from: {raw_note or label}",
            "updated_at": now_iso(),
        })
        .execute()
    ).data or []

    contact = rows[0] if rows else None
    if contact:
        ensure_journal(contact["id"])
    return contact


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


# ------------------------------------------------------------
# Journal helpers
# ------------------------------------------------------------

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


def get_journal(contact_id, limit=10):
    journal = ensure_journal(contact_id)
    if not journal:
        return None, []

    notes = (
        supabase.table("chisme_notes")
        .select("*")
        .eq("journal_id", journal["id"])
        .order("created_at", desc=True)
        .limit(limit)
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
            "note_text": note,
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


# ------------------------------------------------------------
# Stovetop helpers
# ------------------------------------------------------------

def get_active():
    return (
        supabase.table("chisme_active")
        .select("*, chisme_contacts(*)")
        .order("burner_position", desc=False)
        .order("next_followup_date", desc=False)
        .execute()
    ).data or []


def next_available_burner():
    rows = get_active()
    used = {r.get("burner_position") for r in rows}
    for burner in [1, 2, 3, 4]:
        if burner not in used:
            return burner
    return 4


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


# ------------------------------------------------------------
# Field parsing / manual updates
# ------------------------------------------------------------

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

        updates[col] = value

    updates["updated_at"] = now_iso()
    return lookup, updates


def derive_temperature_from_removal(reason_key, current_temp):
    if reason_key == "not_ready":
        return 50
    if reason_key == "not_responding":
        return 50
    if reason_key == "chose_someone_else":
        return 0
    if reason_key == "changed_mind":
        return 0
    if reason_key == "job_completed":
        return 25
    return current_temp


# ------------------------------------------------------------
# Hot List advancement
# ------------------------------------------------------------

async def advance_hotlist_customer(ctx, lookup, step):
    if not lookup:
        await ctx.send("Use: `!hotlist Customer Name sitevisit|notes|sent|followup|approved`")
        return

    matches = find_contacts(lookup)

    if not matches:
        contact = create_contact_stub(lookup, "")
    elif len(matches) > 1:
        await send_long(ctx, format_match_list(matches))
        return
    else:
        contact = matches[0]

    stage = HOTLIST_STAGES[step]

    add_note(
        contact,
        stage["journal"],
        created_by=str(ctx.author),
        note_type="hotlist_progress",
    )

    increment_customer_communication_loadbar(created_by=str(ctx.author))

    if step == "approved":
        burner = next_available_burner()

        supabase.table("chisme_contacts").update({
            "hotlist_temperature": 100,
            "status": "active_project",
            "next_action": "Active project on stovetop",
            "hotlist_stage": stage["stage"],
            "last_outcome": stage["stage"],
            "updated_at": now_iso(),
        }).eq("id", contact["id"]).execute()

        set_active(
            contact,
            reason="Estimate approved. Active project created from Hot List.",
            burner_position=burner,
            owner="Daniel",
        )

        await ctx.send(
            f"🔥 **{contact.get('name')}** moved to the Stovetop.\n"
            f"Burner: {burner}\n"
            f"Customer Communication +1"
        )
        return

    supabase.table("chisme_contacts").update({
        "hotlist_temperature": stage["temp"],
        "status": "lead",
        "next_action": stage["next_action"],
        "hotlist_stage": stage["stage"],
        "last_outcome": stage["stage"],
        "updated_at": now_iso(),
    }).eq("id", contact["id"]).execute()

    if step == "sitevisit":
        hotlist_note_sessions[ctx.author.id] = {
            "contact_id": contact["id"],
            "contact_name": contact.get("name"),
            "step": "sitevisit_notes",
        }

        await ctx.send(
            f"✅ Site visit completed for **{contact.get('name')}**.\n"
            f"🌡 60°\n"
            f"Next: Write estimate notes.\n\n"
            "What did you learn during the site visit?\n\n"
            "I’ll save anything useful for the estimate.\n\n"
            "Type `cancel` to pause this for later."
        )
        return

    await ctx.send(
        f"✅ **{contact.get('name')}** advanced.\n"
        f"🌡 {stage['temp']}°\n"
        f"Next: {stage['next_action']}\n"
        f"Customer Communication +1"
    )
    
def clear_user_sessions(user_id):
    cleared = []

    if hotlist_note_sessions.pop(user_id, None):
        cleared.append("Hot List notes")

    if cremove_sessions.pop(user_id, None):
        cleared.append("Stovetop removal")

    return cleared

# ------------------------------------------------------------
# Discord command registration
# ------------------------------------------------------------

def register_chisme(bot):

    @bot.command(name="chismebot")
    async def chismebot_help(ctx):
        await ctx.send(
            "💬 **CHISMEBOT**\n\n"

            "📓 **Chisme Log**\n"
            "`!chisme Name`\n"
            "Show that customer’s journal.\n\n"
            "`!chisme Name | note`\n"
            "Add a note to that customer’s journal.\n\n"

            "📇 **Rolodex**\n"
            "`!cshow Name`\n"
            "Show customer contact info, summary, and recent chisme.\n\n"
            "`!cset Name | phone: 210... | address: ... | email: ...`\n"
            "Update customer contact info.\n\n"

            "🌡 **Hot List**\n"
            "`!hotlist`\n"
            "Show customers moving toward an estimate or project.\n\n"
            "`!hotlist Name`\n"
            "Show one customer’s stage, next action, and recent activity.\n\n"
            "`!hotlist Name sitevisit|notes|sent|followup|approved`\n"
            "Move the customer forward.\n\n"

            "🔥 **Stovetop**\n"
            "`!stovetop`\n"
            "Show active projects by burner.\n\n"
            "`!cactive Name | burner: 1 | reason: project needs attention`\n"
            "Manually put a customer on the Stovetop.\n\n"
            "`!cremove Name`\n"
            "Take a project off the Stovetop with follow-up notes."
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
                f"Hot List: {contact.get('hotlist_temperature') or 50}° — {contact.get('hotlist_stage') or 'New Lead'}",
                "",
            ]

            real_notes = [
                n for n in notes
                if n.get("note_type") != "journal_anchor"
                and (n.get("note_text") or "").strip()
            ]

            if not real_notes:
                lines.append("No chisme notes yet.")
                lines.append(f"Add one with: `!chisme {contact.get('name')} | <note>`")
            else:
                for n in real_notes[:8]:
                    lines.append(f"- {n.get('note_date')}: {short(n.get('note_text'), 250)}")

            await send_long(ctx, "\n".join(lines))
            return

        add_note(contact, note, created_by=str(ctx.author), note_type="chisme")

        await ctx.send(
            f"✅ Chisme saved for **{contact.get('name')}**.\n"
            f"Use `!hotlist {contact.get('name')}` to see their customer workflow."
        )

    @bot.command(name="cset")
    async def cset(ctx, *, raw=""):
        lookup, updates = parse_fields(raw)

        if not lookup:
            await ctx.send("Use: `!cset Name | phone: ... | address: ... | email: ...`")
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

    @bot.command(name="cactive", aliases=["stove"])
    async def cactive(ctx, *, raw=""):
        if not raw.strip():
            await ctx.send("Use: `!cactive Name | burner: 1 | reason: project needs attention`")
            return

        parts = [p.strip() for p in raw.split("|") if p.strip()]
        lookup = parts[0]
        burner = 4
        reason = "Project on stovetop"

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

        await ctx.send(f"✅ Stovetop: **{contact.get('name')}** on burner {burner} — {reason}")

    @bot.command(name="clist", aliases=["stovetop"])
    async def clist(ctx):
        rows = get_active()
        if not rows:
            await ctx.send("No projects on the stovetop right now.")
            return

        lines = ["🔥 **Stovetop / Active Projects**\n"]

        for row in rows:
            c = row.get("chisme_contacts") or {}
            lines.append(
                f"Burner {row.get('burner_position')}: **{c.get('name')}**\n"
                f"  Reason: {row.get('active_reason') or 'none'}\n"
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
            f"Hot List: {c.get('hotlist_temperature') or 50}° — {c.get('hotlist_stage') or 'New Lead'}",
            f"Next Action: {c.get('next_action') or 'Contact customer / schedule site visit'}",
            f"Status: {c.get('status') or 'unknown'}",
            f"Summary: {c.get('chisme_summary') or 'none'}",
            "",
        ]

        if active:
            a = active[0]
            lines.extend([
                f"🔥 Stovetop burner: {a.get('burner_position')}",
                f"Reason: {a.get('active_reason')}",
                "",
            ])

        lines.append("Recent chisme:")
        real_notes = [
            n for n in notes
            if n.get("note_type") != "journal_anchor"
            and (n.get("note_text") or "").strip()
        ]
        if not real_notes:
            lines.append("No chisme notes yet.")
        else:
            for n in real_notes[:8]:
                lines.append(f"- {n.get('note_date')}: {short(n.get('note_text'), 220)}")

        await send_long(ctx, "\n".join(lines))

    @bot.command(name="hotlist")
    async def hotlist(ctx, *, raw=""):
        raw = (raw or "").strip()

        if not raw:
            active_rows = (
                supabase.table("chisme_active")
                .select("contact_id")
                .execute()
            ).data or []
            active_ids = {r["contact_id"] for r in active_rows}

            rows = (
                supabase.table("chisme_contacts")
                .select("*")
                .gte("hotlist_temperature", 50)
                .lt("hotlist_temperature", 100)
                .order("hotlist_temperature", desc=True)
                .execute()
            ).data or []

            rows = [c for c in rows if c["id"] not in active_ids]

            if not rows:
                await ctx.send("No hot leads right now.")
                return

            lines = ["🌡 **HOT LIST**", ""]

            for c in rows[:15]:
                temp = c.get("hotlist_temperature") or 50
                name = c.get("name") or "Unknown"
                next_action = c.get("next_action") or "Contact customer / schedule site visit"

                lines.append(
                    f"**{temp}° {name}**\n"
                    f"Next:\n{next_action}\n"
                )

            await send_long(ctx, "\n".join(lines))
            return

        parts = raw.split()
        possible_step = parts[-1].lower()

        if possible_step in HOTLIST_STAGES:
            lookup = " ".join(parts[:-1]).strip()
            await advance_hotlist_customer(ctx, lookup, possible_step)
            return

        lookup = raw
        matches = find_contacts(lookup)

        if not matches:
            await ctx.send(f"No Rolodex card found for **{lookup}**.")
            return

        if len(matches) > 1:
            await send_long(ctx, format_match_list(matches))
            return

        c = matches[0]
        journal, notes = get_journal(c["id"])

        temp = c.get("hotlist_temperature") or 50
        next_action = c.get("next_action") or "Contact customer / schedule site visit"
        stage = c.get("hotlist_stage") or "New Lead"

        lines = [
            f"**{c.get('name')}**",
            "",
            f"🌡 **{temp}°**",
            "",
            temp_bar(temp),
            "",
            "**Current Stage**",
            stage,
            "",
            "**Next Action**",
            next_action,
            "",
            "**Recent Activity**",
        ]

        real_notes = [
            n for n in notes
            if n.get("note_type") in {"hotlist_progress", "site_visit_notes", "estimate_notes", "chisme"}
            and (n.get("note_text") or "").strip()
        ]

        if not real_notes:
            lines.append("No recent activity yet.")
        else:
            for n in real_notes[:5]:
                lines.append(f"• {short(n.get('note_text'), 180)}")

        await send_long(ctx, "\n".join(lines))

    @bot.command(name="cremove")
    async def cremove(ctx, *, lookup=""):
        if not lookup.strip():
            await ctx.send("Use: `!cremove Name`")
            return

        matches = find_contacts(lookup)

        if len(matches) > 1:
            await send_long(ctx, format_match_list(matches))
            return

        if not matches:
            await ctx.send(f"No Rolodex card found for **{lookup}**.")
            return

        contact = matches[0]

        cremove_sessions[ctx.author.id] = {
            "contact": contact,
            "step": "reason",
        }

        await ctx.send(
            f"Why are we taking **{contact.get('name')}** off the stovetop?\n\n"
            "1. Not ready / needs to reschedule\n"
            "2. Not responding\n"
            "3. Chose someone else\n"
            "4. Changed mind\n"
            "5. Job completed\n"
            "6. Other"
        )

    @bot.listen("on_message")
    async def handle_cremove_session(message):
        if message.author.bot:
            return
    
        content = message.content.strip()
    
        if content.lower() in {"cancel", "!cancel", "stop", "!stop", "nevermind", "never mind"}:
            cleared = clear_user_sessions(message.author.id)
    
            if cleared:
                await message.channel.send(
                    "👍 Workflow cancelled.\n\n"
                    "Nothing was deleted. You can pick it back up later."
                )
            else:
                await message.channel.send("👍 Nothing active to cancel.")
    
            return
    
        if message.content.startswith("!"):
            return

        if message.content.startswith("!"):
            return

        # Hotlist site visit note capture
        hotlist_session = hotlist_note_sessions.get(message.author.id)
        if hotlist_session:
            content = message.content.strip()

            contact_rows = (
                supabase.table("chisme_contacts")
                .select("*")
                .eq("id", hotlist_session["contact_id"])
                .limit(1)
                .execute()
            ).data or []

            if not contact_rows:
                del hotlist_note_sessions[message.author.id]
                await message.channel.send("I lost that customer card.")
                return

            contact = contact_rows[0]

            add_note(
                contact,
                content,
                created_by=str(message.author),
                note_type="site_visit_notes",
            )

            del hotlist_note_sessions[message.author.id]

            await message.channel.send(
                f"📝 Site visit notes saved for **{contact.get('name')}**.\n"
                f"When the estimate notes are complete, use:\n"
                f"`!hotlist {contact.get('name')} notes`"
            )
            return

        # Stovetop removal workflow
        session = cremove_sessions.get(message.author.id)
        if not session:
            return

        content = message.content.strip()
        contact = session["contact"]

        reason_map = {
            "1": ("not_ready", "Not ready / needs to reschedule"),
            "2": ("not_responding", "Not responding"),
            "3": ("chose_someone_else", "Chose someone else"),
            "4": ("changed_mind", "Changed mind"),
            "5": ("job_completed", "Job completed"),
            "6": ("other", "Other"),
        }

        if session["step"] == "reason":
            if content not in reason_map:
                await message.channel.send("Reply with 1, 2, 3, 4, 5, or 6.")
                return

            reason_key, reason_label = reason_map[content]

            if content == "6":
                session["reason_key"] = "other"
                session["step"] = "custom_reason"
                await message.channel.send("What is the reason?")
                return

            session["reason_key"] = reason_key
            session["reason_label"] = reason_label
            session["base_temp"] = contact.get("hotlist_temperature") or 0
            session["step"] = "followup"

            await message.channel.send(
                f"When should we follow up with **{contact.get('name')}**?\n\n"
                "1. 1 week\n"
                "2. 2 weeks\n"
                "3. 1 month\n"
                "4. Custom date like `7/3/2026`\n"
                "5. No follow-up"
            )
            return

        if session["step"] == "custom_reason":
            session["reason_label"] = content
            session["base_temp"] = contact.get("hotlist_temperature") or 0
            session["step"] = "followup"

            await message.channel.send(
                f"When should we follow up with **{contact.get('name')}**?\n\n"
                "1. 1 week\n"
                "2. 2 weeks\n"
                "3. 1 month\n"
                "4. Custom date like `7/3/2026`\n"
                "5. No follow-up"
            )
            return

        if session["step"] == "followup":
            followup_date = None

            if content == "1":
                followup_date = (date.today() + timedelta(days=7)).isoformat()
            elif content == "2":
                followup_date = (date.today() + timedelta(days=14)).isoformat()
            elif content == "3":
                followup_date = (date.today() + timedelta(days=30)).isoformat()
            elif content == "4":
                session["step"] = "custom_date"
                await message.channel.send("Type the follow-up date like `7/3/2026`.")
                return
            elif content == "5":
                followup_date = None
            else:
                await message.channel.send("Reply with 1, 2, 3, 4, or 5.")
                return

            session["followup_date"] = followup_date
            session["step"] = "note"
            await message.channel.send("Add a note for the chisme log:")
            return

        if session["step"] == "custom_date":
            parsed_date = parse_followup_response(content)
            if not parsed_date:
                await message.channel.send("Use a date like `7/3/2026` or `2026-07-03`.")
                return

            session["followup_date"] = parsed_date
            session["step"] = "note"
            await message.channel.send("Add a note for the chisme log:")
            return

        if session["step"] == "note":
            followup_date = session.get("followup_date")
            reason_label = session["reason_label"]
            reason_key = session["reason_key"]
            base_temp = session.get("base_temp", contact.get("hotlist_temperature") or 0)
            new_temp = derive_temperature_from_removal(reason_key, base_temp)
            user_note = content

            supabase.table("chisme_active").delete().eq("contact_id", contact["id"]).execute()

            updates = {
                "hotlist_temperature": new_temp,
                "last_outcome": f"Took off stovetop: {reason_label}",
                "updated_at": now_iso(),
            }

            if reason_key in {"chose_someone_else", "changed_mind", "job_completed"}:
                updates["status"] = "closed"
                updates["next_action"] = "None"
            else:
                updates["status"] = "lead"
                updates["hotlist_stage"] = "New Lead"
                updates["next_action"] = "Follow up" if followup_date else "Contact customer / schedule site visit"

            if followup_date:
                updates["next_followup_date"] = followup_date
                updates["next_contact_date"] = followup_date

            supabase.table("chisme_contacts").update(updates).eq("id", contact["id"]).execute()

            add_note(
                contact,
                (
                    f"Project moved off the Stovetop.\n\n"
                    f"Reason: {reason_label}\n"
                    f"Follow-up: {followup_date or 'none'}\n"
                    f"Note: {user_note}"
                ),
                created_by=str(message.author),
                note_type="active_removed",
            )

            del cremove_sessions[message.author.id]

            await message.channel.send(
                f"✅ Took **{contact.get('name')}** off the Stovetop.\n"
                f"Reason: {reason_label}\n"
                f"Follow-up: {followup_date or 'none'}"
            )
