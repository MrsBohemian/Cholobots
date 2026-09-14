import os
import re
import traceback
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from config import client
from supabase import create_client


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Multi-step Discord sessions
cremove_sessions = {}
hotlist_note_sessions = {}
stovetop_sessions = {}
chisme_sessions = {}
lead_sessions = {}
lead_processing_sessions = {}


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
        "next_action": "Move to Oven",
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
            "pipeline_stage": "New Lead",
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
# Oven helpers
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
        await ctx.send(
            f"No Rolodex card found for **{lookup}**.\n"
            "I did not create a new customer card. Check the customer name and try again."
        )
        return
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

    if step == "approved":
        burner = next_available_burner()

        supabase.table("chisme_contacts").update({
            "hotlist_temperature": 100,
            "status": "active_project",
            "next_action": "Active project in oven",
            "pipeline_stage": stage["stage"],
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
            f"🔥 **{contact.get('name')}** moved to the Oven.\n"
            f"Burner: {burner}\n"
            f"Stage: {stage['stage']}"
        )
        return

    supabase.table("chisme_contacts").update({
        "hotlist_temperature": stage["temp"],
        "status": "lead",
        "next_action": stage["next_action"],
        "pipeline_stage": stage["stage"],
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
        f"Stage: {stage['stage']}"
    )
    
def clear_user_sessions(user_id):
    cleared = []

    if lead_processing_sessions.pop(user_id, None):
        cleared.append("Lead processing")

    if lead_sessions.pop(user_id, None):
        cleared.append("Lead capture")

    if chisme_sessions.pop(user_id, None):
        cleared.append("Chisme")

    if stovetop_sessions.pop(user_id, None):
        cleared.append("Stovetop")

    if hotlist_note_sessions.pop(user_id, None):
        cleared.append("Hot List notes")

    if cremove_sessions.pop(user_id, None):
        cleared.append("Stovetop removal")

    return cleared

# ------------------------------------------------------------
# Lead Inbox helpers
# ------------------------------------------------------------

def get_unprocessed_leads(limit=50):
    return (
        supabase.table("chisme_leads")
        .select("*")
        .eq("status", "unprocessed")
        .order("captured_at", desc=False)
        .limit(limit)
        .execute()
    ).data or []


def get_lead_by_id(lead_id):
    rows = (
        supabase.table("chisme_leads")
        .select("*")
        .eq("id", lead_id)
        .limit(1)
        .execute()
    ).data or []
    return rows[0] if rows else None


def append_lead_note(lead, note, label="Processing note"):
    existing = (lead.get("raw_notes") or "").strip()
    stamp = datetime.now(ZoneInfo("America/Chicago")).strftime("%Y-%m-%d %H:%M")
    addition = f"[{stamp}] {label}: {note.strip()}"
    combined = f"{existing}\n\n{addition}".strip()
    supabase.table("chisme_leads").update({
        "raw_notes": combined,
        "updated_at": now_iso(),
    }).eq("id", lead["id"]).execute()
    return combined


def lead_preview(lead, limit=120):
    raw = (lead.get("raw_notes") or "").strip()
    if not raw:
        return "No notes"
    first = " ".join(raw.split())
    return first[:limit] + ("..." if len(first) > limit else "")


def try_link_lead_to_existing_contact(lead):
    """Auto-link an unlinked lead when its raw capture contains a unique phone/email match."""
    if not lead or lead.get("contact_id"):
        return lead

    raw = lead.get("raw_notes") or ""
    raw_lower = raw.lower()

    emails = set(re.findall(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", raw, flags=re.I))
    phone_candidates = set()
    for match in re.findall(r"(?:\+?1[\s.\-]?)?(?:\(?\d{3}\)?[\s.\-]?)\d{3}[\s.\-]?\d{4}", raw):
        digits = re.sub(r"\D", "", match)
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
        if len(digits) == 10:
            phone_candidates.add(digits)

    if not emails and not phone_candidates:
        return lead

    contacts = (
        supabase.table("chisme_contacts")
        .select("id,name,phone,email")
        .execute()
    ).data or []

    matches = []
    for contact in contacts:
        email = (contact.get("email") or "").strip().lower()
        phone = re.sub(r"\D", "", contact.get("phone") or "")
        if len(phone) == 11 and phone.startswith("1"):
            phone = phone[1:]

        email_match = email and email in {e.lower() for e in emails}
        phone_match = phone and phone in phone_candidates
        if email_match or phone_match:
            matches.append(contact)

    unique = {c["id"]: c for c in matches}
    if len(unique) != 1:
        return lead

    contact = next(iter(unique.values()))
    supabase.table("chisme_leads").update({
        "contact_id": contact["id"],
        "updated_at": now_iso(),
    }).eq("id", lead["id"]).execute()

    refreshed = get_lead_by_id(lead["id"])
    return refreshed or lead


def lead_menu_text(lead):
    lead = try_link_lead_to_existing_contact(lead)
    callback = lead.get("callback_date")
    linked = lead.get("contact_id")
    customer_label = "Not linked"

    if linked:
        rows = (
            supabase.table("chisme_contacts")
            .select("name")
            .eq("id", linked)
            .limit(1)
            .execute()
        ).data or []
        customer_label = rows[0].get("name") if rows else "Linked"

    return (
        f"📥 **PROCESS LEAD**\n\n"
        f"{lead.get('raw_notes') or '(no notes)'}\n\n"
        f"**Current next action:** {lead.get('next_action') or 'Not set'}\n"
        f"**Callback:** {callback or 'None'}\n"
        f"**Customer card:** {customer_label}\n\n"
        "**What do you want to do?**\n"
        "1. 📞 Contact customer / log outcome\n"
        "2. 📇 Create or match customer card\n"
        "3. 📅 Record scheduling / estimate plan\n"
        "4. 🔎 Research something\n"
        "5. 🤝 Ask colleague / subcontractor\n"
        "6. ⏳ Waiting / set callback\n"
        "7. 📝 Add note\n"
        "8. ✅ Mark processed → create opportunity\n"
        "9. 🧹 Close / remove lead\n"
        "0. ↩️ Back to Lead Inbox"
    )

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
            "Open that customer’s workspace: recent chisme, callbacks, temperature, and Fridge/Stovetop/Oven moves.\n\n"
            "`!chisme Name | note`\n"
            "Add a note to that customer’s journal.\n\n"

            "📇 **Rolodex**\n"
            "`!cshow Name`\n"
            "Show customer contact info, summary, and recent chisme.\n\n"
            "`!cset Name | phone: 210... | address: ... | email: ...`\n"
            "Update customer contact info.\n\n"

            "🍲 **Stovetop / Warm Opportunities**\n"
            "`!stovetop` (or `!hotlist`)\n"
            "Show customers moving toward an estimate or project.\n\n"
            "`!stovetop Name`\n"
            "Show one customer’s stage, next action, and recent activity.\n\n"
            "`!stovetop Name sitevisit|notes|sent|followup|approved`\n"
            "Move the customer forward.\n\n"

            "🔥 **Oven / Active Projects**\n"
            "`!oven`\n"
            "Show active projects.\n\n"
            "`!cactive Name | burner: 1 | reason: project needs attention`\n"
            "Manually put a customer in the Oven.\n\n"
            "`!cremove Name`\n"
            "Take a project out of the Oven with follow-up notes."
        )

    @bot.command(name="leads")
    async def leads(ctx):
        clear_user_sessions(ctx.author.id)
        rows = get_unprocessed_leads()
        if not rows:
            await ctx.send("📥 **Lead Inbox is clear.** No unprocessed leads.")
            return
        lead_processing_sessions[ctx.author.id] = {"step": "select_lead", "leads": rows}
        lines = [f"📥 **LEAD INBOX — {len(rows)} unprocessed**", ""]
        for i, lead in enumerate(rows, 1):
            lines.append(f"**{i}.** {lead_preview(lead)}\n   Next: {lead.get('next_action') or 'Process lead'}")
        lines.extend(["", "Which lead do you want to work on?", "Reply with a number, or `cancel`."])
        await send_long(ctx, "\n".join(lines))

    @bot.command(name="lead")
    async def lead(ctx, *, raw=""):
        clear_user_sessions(ctx.author.id)
        notes = [raw.strip()] if raw.strip() else []
        lead_sessions[ctx.author.id] = {"notes": notes, "started_at": now_iso()}

        if notes:
            await ctx.send(
                "📥 **Lead capture started.**\n"
                "First note captured. Keep sending anything else you learn.\n\n"
                "Type `done` to save the lead or `cancel` to throw it away."
            )
        else:
            await ctx.send(
                "📥 **Lead capture started.**\n"
                "Send me whatever you know — job notes, name, phone, source, event, "
                "or even just what you need to remember.\n\n"
                "Keep sending messages. Type `done` when you're finished or `cancel` to throw it away."
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
            # Chisme is the general-purpose customer workspace, regardless of
            # whether the customer is in the Fridge, on the Stovetop, or in the Oven.
            clear_user_sessions(ctx.author.id)

            journal, notes = get_journal(contact["id"])
            real_notes = [
                n for n in notes
                if n.get("note_type") != "journal_anchor"
                and (n.get("note_text") or "").strip()
            ]

            chisme_sessions[ctx.author.id] = {
                "step": "customer_menu",
                "contact_id": contact["id"],
                "contact_name": contact.get("name") or "Unknown",
            }

            callback = contact.get("next_followup_date") or contact.get("next_contact_date")
            temp = int(contact.get("hotlist_temperature") or 0)
            location = "Oven" if temp >= 100 else ("Stovetop" if temp >= 51 else "Fridge")

            lines = [
                f"📓 **{contact.get('name')}**",
                f"📍 {location} · 🌡 {temp}°",
                f"Stage: {contact.get('pipeline_stage') or 'Stage not set'}",
                f"Next: {contact.get('next_action') or 'No next action set'}",
                f"📅 Callback: {callback or 'None scheduled'}",
                "",
                "**Recent chisme**",
            ]

            if not real_notes:
                lines.append("No chisme notes yet.")
            else:
                for n in real_notes[:5]:
                    lines.append(f"• {n.get('note_date')}: {short(n.get('note_text'), 220)}")

            lines.extend([
                "",
                "**What do you want to do?**",
                "1. 📝 Add chisme",
                "2. 📅 Set / change callback",
                "3. 🌡 Change temperature",
                "4. 🍲 Move to Stovetop",
                "5. 🧊 Move to Fridge",
                "6. 🔥 Move to Oven",
                "7. ✅ Done",
            ])

            await send_long(ctx, "\n".join(lines))
            return

        add_note(contact, note, created_by=str(ctx.author), note_type="chisme")

        await ctx.send(
            f"✅ Chisme saved for **{contact.get('name')}**.\n"
            f"Use `!stovetop {contact.get('name')}` to see their customer workflow."
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
        reason = "Project in oven"

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

        await ctx.send(f"✅ Oven: **{contact.get('name')}** on burner {burner} — {reason}")

    @bot.command(name="clist", aliases=["oven"])
    async def clist(ctx):
        rows = get_active()
        if not rows:
            await ctx.send("No projects in the oven right now.")
            return

        lines = ["🔥 **Oven / Active Projects**\n"]

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
            f"Stovetop: {c.get('hotlist_temperature') or 50}° — {c.get('pipeline_stage') or 'New Lead'}",
            f"Next Action: {c.get('next_action') or 'Contact customer / schedule site visit'}",
            f"Status: {c.get('status') or 'unknown'}",
            f"Summary: {c.get('chisme_summary') or 'none'}",
            "",
        ]

        if active:
            a = active[0]
            lines.extend([
                f"🔥 Oven slot: {a.get('burner_position')}",
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

    @bot.command(name="hotlist", aliases=["stovetop"])
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
                .gte("hotlist_temperature", 51)
                .lt("hotlist_temperature", 100)
                .order("hotlist_temperature", desc=True)
                .execute()
            ).data or []

            rows = [c for c in rows if c["id"] not in active_ids][:15]

            if not rows:
                await ctx.send("No warm customers on the Stovetop right now.")
                return

            stovetop_sessions[ctx.author.id] = {
                "step": "select_customer",
                "customers": rows,
            }

            lines = ["🍲 **STOVETOP / WARM OPPORTUNITIES**", ""]

            for i, c in enumerate(rows, 1):
                temp = c.get("hotlist_temperature") or 51
                name = c.get("name") or "Unknown"
                stage = c.get("pipeline_stage") or "Stage not set"
                next_action = c.get("next_action") or "No next action set"

                lines.append(
                    f"**{i}. {name} — {temp}° — {stage}**\n"
                    f"   Next: {next_action}"
                )

            lines.extend([
                "",
                "**Which customer do you want to work with?**",
                "Reply with a number, or `cancel`."
            ])

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

        if int(c.get("hotlist_temperature") or 0) <= 50:
            supabase.table("chisme_contacts").update({
                "hotlist_temperature": 51,
                "status": "lead",
                "pipeline_stage": c.get("pipeline_stage") or "New Lead",
                "next_action": c.get("next_action") or "Contact customer / schedule site visit",
                "updated_at": now_iso(),
            }).eq("id", c["id"]).execute()
        
            c["hotlist_temperature"] = 51
            
        journal, notes = get_journal(c["id"])

        temp = c.get("hotlist_temperature") or 50
        next_action = c.get("next_action") or "Contact customer / schedule site visit"
        stage = c.get("pipeline_stage") or "New Lead"

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
            f"Why are we taking **{contact.get('name')}** out of the oven?\n\n"
            "1. Not ready / needs to reschedule\n"
            "2. Not responding\n"
            "3. Chose someone else\n"
            "4. Changed mind\n"
            "5. Job completed\n"
            "6. Other"
        )

    @bot.command(name="cancel")
    async def cancel_workflow(ctx):
        cleared = clear_user_sessions(ctx.author.id)
        if cleared:
            await ctx.send(
                "👍 Workflow cancelled.\n\n"
                "Nothing was deleted. You can pick it back up later."
            )
        else:
            await ctx.send("👍 Nothing active to cancel.")

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

        # Interactive Lead Inbox / processing workflow
        processing_session = lead_processing_sessions.get(message.author.id)
        if processing_session:
            content = message.content.strip()
            step = processing_session.get("step")

            if step == "select_lead":
                if not content.isdigit():
                    await message.channel.send("Reply with a lead number, or `cancel`.")
                    return
                index = int(content) - 1
                leads_list = processing_session.get("leads") or []
                if index < 0 or index >= len(leads_list):
                    await message.channel.send("That number isn't in the Lead Inbox. Try again.")
                    return
                lead = get_lead_by_id(leads_list[index]["id"])
                if not lead or lead.get("status") != "unprocessed":
                    lead_processing_sessions.pop(message.author.id, None)
                    await message.channel.send("That lead is no longer unprocessed. Run `!leads` to refresh the inbox.")
                    return
                processing_session["lead_id"] = lead["id"]
                processing_session["step"] = "lead_menu"
                await send_long(message.channel, lead_menu_text(lead))
                return

            lead = get_lead_by_id(processing_session.get("lead_id"))
            if not lead:
                lead_processing_sessions.pop(message.author.id, None)
                await message.channel.send("I lost that lead. Run `!leads` and try again.")
                return

            if step == "lead_menu":
                if content == "1":
                    processing_session["step"] = "contact_outcome"
                    await message.channel.send("📞 What happened when you contacted them? Tell me the outcome and what needs to happen next.")
                    return
                if content == "2":
                    processing_session["step"] = "customer_lookup"
                    await message.channel.send("📇 Type a customer name or phone number to search the Rolodex, or type `new` to create a customer card.")
                    return
                if content == "3":
                    processing_session["step"] = "schedule_note"
                    await message.channel.send("📅 What was scheduled, or what needs to be scheduled?")
                    return
                if content == "4":
                    processing_session["step"] = "research_task"
                    await message.channel.send("🔎 What do you need to research for this lead?")
                    return
                if content == "5":
                    processing_session["step"] = "network_task"
                    await message.channel.send("🤝 Who do you need to ask, and what do you need from them?")
                    return
                if content == "6":
                    processing_session["step"] = "waiting_note"
                    await message.channel.send("⏳ What are you waiting on? Include a callback date if you know it.")
                    return
                if content == "7":
                    processing_session["step"] = "add_lead_note"
                    await message.channel.send("📝 Add the note:")
                    return
                if content == "8":
                    processing_session["step"] = "opportunity_name"
                    await message.channel.send("✅ Give this opportunity a short name, like `Replace 3 vanity lights` or `New mailbox`.")
                    return
                if content == "9":
                    processing_session["step"] = "close_reason"
                    await message.channel.send("🧹 Why are we removing this from the Lead Inbox?\n\n1. Not responding / went cold\n2. Customer no longer interested\n3. Not a fit for Handley Man\n4. Duplicate\n5. Already handled elsewhere\n6. Other")
                    return
                if content == "0":
                    rows = get_unprocessed_leads()
                    processing_session.clear(); processing_session.update({"step":"select_lead","leads":rows})
                    if not rows:
                        lead_processing_sessions.pop(message.author.id, None)
                        await message.channel.send("📥 Lead Inbox is clear.")
                        return
                    lines=[f"📥 **LEAD INBOX — {len(rows)} unprocessed**",""]
                    for i,item in enumerate(rows,1):
                        lines.append(f"**{i}.** {lead_preview(item)}\n   Next: {item.get('next_action') or 'Process lead'}")
                    lines.extend(["","Reply with a number, or `cancel`."])
                    await send_long(message.channel,"\n".join(lines)); return
                await message.channel.send("Reply with 1–9, or 0 to go back.")
                return

            if step == "contact_outcome":
                append_lead_note(lead, content, "Contact outcome")
                supabase.table("chisme_leads").update({"next_action": content[:500], "updated_at": now_iso()}).eq("id", lead["id"]).execute()
                processing_session["step"]="lead_menu"
                await message.channel.send("📞 Contact outcome saved.")
                await send_long(message.channel, lead_menu_text(get_lead_by_id(lead["id"])))
                return

            if step == "customer_lookup":
                if content.lower() == "new":
                    processing_session["step"] = "new_customer"
                    await message.channel.send("📇 Enter: `Name | phone: 210... | email: ... | address: ...`\nOnly include what you know.")
                    return
                matches = find_contacts(content)
                if not matches:
                    processing_session["step"] = "customer_lookup_no_match"
                    await message.channel.send(f"No Rolodex card found for **{content}**.\n\n1. Create new customer\n2. Search again\n3. Go back")
                    return
                if len(matches) == 1:
                    contact=matches[0]
                    supabase.table("chisme_leads").update({"contact_id":contact["id"],"updated_at":now_iso()}).eq("id",lead["id"]).execute()
                    append_lead_note(lead,f"Linked to Rolodex customer: {contact.get('name')}","Customer")
                    processing_session["step"]="lead_menu"
                    await message.channel.send(f"📇 Lead linked to **{contact.get('name')}**.")
                    await send_long(message.channel,lead_menu_text(get_lead_by_id(lead["id"])))
                    return
                processing_session["step"]="customer_choose"; processing_session["customer_matches"]=matches
                lines=["I found multiple possible customer cards:",""]
                for i,c in enumerate(matches,1): lines.append(f"{i}. **{c.get('name')}** — {c.get('phone') or 'no phone'} — {c.get('address') or 'no address'}")
                lines.append("\nReply with the customer number, or `0` to search again.")
                await send_long(message.channel,"\n".join(lines)); return

            if step == "customer_lookup_no_match":
                if content == "1":
                    processing_session["step"]="new_customer"; await message.channel.send("📇 Enter: `Name | phone: 210... | email: ... | address: ...`"); return
                if content == "2":
                    processing_session["step"]="customer_lookup"; await message.channel.send("Type a customer name or phone number:"); return
                if content == "3":
                    processing_session["step"]="lead_menu"; await send_long(message.channel,lead_menu_text(lead)); return
                await message.channel.send("Reply with 1, 2, or 3."); return

            if step == "customer_choose":
                if content == "0":
                    processing_session["step"]="customer_lookup"; await message.channel.send("Type a customer name or phone number:"); return
                if not content.isdigit(): await message.channel.send("Reply with a customer number, or 0 to search again."); return
                matches=processing_session.get("customer_matches") or []; index=int(content)-1
                if index<0 or index>=len(matches): await message.channel.send("That customer number isn't in the list."); return
                contact=matches[index]
                supabase.table("chisme_leads").update({"contact_id":contact["id"],"updated_at":now_iso()}).eq("id",lead["id"]).execute()
                append_lead_note(lead,f"Linked to Rolodex customer: {contact.get('name')}","Customer")
                processing_session["step"]="lead_menu"; processing_session.pop("customer_matches",None)
                await message.channel.send(f"📇 Lead linked to **{contact.get('name')}**.")
                await send_long(message.channel,lead_menu_text(get_lead_by_id(lead["id"])))
                return

            if step == "new_customer":
                lookup, updates = parse_fields(content)
                if not lookup:
                    await message.channel.send("Give me at least a customer name or phone number."); return
                matches=find_contacts(lookup)
                if len(matches)==1:
                    contact=matches[0]
                elif len(matches)>1:
                    processing_session["step"]="customer_choose"; processing_session["customer_matches"]=matches
                    lines=["I found existing possible matches instead of creating a duplicate:",""]
                    for i,c in enumerate(matches,1): lines.append(f"{i}. **{c.get('name')}** — {c.get('phone') or 'no phone'} — {c.get('address') or 'no address'}")
                    lines.append("\nReply with the customer number, or `0` to search again.")
                    await send_long(message.channel,"\n".join(lines)); return
                else:
                    contact=create_contact_stub(lookup,lead.get("raw_notes") or "")
                    if not contact: await message.channel.send("I couldn't create the customer card."); return
                if updates:
                    supabase.table("chisme_contacts").update(updates).eq("id",contact["id"]).execute()
                    refreshed=(supabase.table("chisme_contacts").select("*").eq("id",contact["id"]).limit(1).execute()).data or []
                    if refreshed: contact=refreshed[0]
                supabase.table("chisme_leads").update({"contact_id":contact["id"],"updated_at":now_iso()}).eq("id",lead["id"]).execute()
                append_lead_note(lead,f"Linked to Rolodex customer: {contact.get('name')}","Customer")
                processing_session["step"]="lead_menu"
                await message.channel.send(f"📇 Customer card linked: **{contact.get('name')}**.")
                await send_long(message.channel,lead_menu_text(get_lead_by_id(lead["id"])))
                return

            if step == "schedule_note":
                append_lead_note(lead,content,"Scheduling")
                supabase.table("chisme_leads").update({"next_action":content[:500],"updated_at":now_iso()}).eq("id",lead["id"]).execute()
                processing_session["step"]="lead_menu"; await message.channel.send("📅 Scheduling plan saved.")
                await send_long(message.channel,lead_menu_text(get_lead_by_id(lead["id"]))); return

            if step == "research_task":
                append_lead_note(lead,content,"Research")
                supabase.table("chisme_leads").update({"next_action":f"Research: {content}"[:500],"updated_at":now_iso()}).eq("id",lead["id"]).execute()
                processing_session["step"]="lead_menu"; await message.channel.send("🔎 Research task saved.")
                await send_long(message.channel,lead_menu_text(get_lead_by_id(lead["id"]))); return

            if step == "network_task":
                append_lead_note(lead,content,"Network / subcontractor")
                supabase.table("chisme_leads").update({"next_action":f"Follow up with network: {content}"[:500],"updated_at":now_iso()}).eq("id",lead["id"]).execute()
                processing_session["step"]="lead_menu"; await message.channel.send("🤝 Network/subcontractor action saved.")
                await send_long(message.channel,lead_menu_text(get_lead_by_id(lead["id"]))); return

            if step == "waiting_note":
                # A customer card may have been created after the original lead brain dump.
                # Re-check the raw capture now so callbacks follow the customer automatically.
                lead = try_link_lead_to_existing_contact(lead)
                callback = parse_followup_response(content)
                append_lead_note(lead, content, "Waiting")
                payload = {
                    "next_action": f"Waiting: {content}"[:500],
                    "updated_at": now_iso(),
                }
                if callback:
                    payload["callback_date"] = callback
                supabase.table("chisme_leads").update(payload).eq("id", lead["id"]).execute()

                if callback and lead.get("contact_id"):
                    supabase.table("chisme_contacts").update({
                        "next_followup_date": callback,
                        "next_contact_date": callback,
                    }).eq("id", lead["contact_id"]).execute()

                processing_session["step"] = "lead_menu"
                if callback and lead.get("contact_id"):
                    await message.channel.send(
                        f"⏳ Waiting note saved. Callback: {callback}\n"
                        "📇 Customer callback updated too."
                    )
                elif callback:
                    await message.channel.send(f"⏳ Waiting note saved. Callback: {callback}")
                else:
                    await message.channel.send("⏳ Waiting note saved. No callback date detected.")
                await send_long(message.channel, lead_menu_text(get_lead_by_id(lead["id"])))
                return

            if step == "add_lead_note":
                append_lead_note(lead,content); processing_session["step"]="lead_menu"
                await message.channel.send("📝 Note added."); await send_long(message.channel,lead_menu_text(get_lead_by_id(lead["id"]))); return

            if step == "opportunity_name":
                opportunity_name=content.strip()[:160]
                if not opportunity_name: await message.channel.send("Give the opportunity a short name."); return
                rows=(supabase.table("chisme_opportunities").insert({
                    "lead_id":lead["id"],"contact_id":lead.get("contact_id"),"opportunity_name":opportunity_name,
                    "scope_notes":lead.get("raw_notes") or "","pipeline_stage":"New Opportunity","temperature":50,
                    "next_action":lead.get("next_action"),"callback_date":lead.get("callback_date"),"status":"open","updated_at":now_iso(),
                }).execute()).data or []
                if not rows: await message.channel.send("I couldn't create the opportunity, so I left the lead unprocessed."); return
                supabase.table("chisme_leads").update({"status":"processed","next_action":"Opportunity created","updated_at":now_iso()}).eq("id",lead["id"]).execute()
                lead_processing_sessions.pop(message.author.id,None)
                await message.channel.send(f"✅ **Lead processed.**\nOpportunity created: **{opportunity_name}**\nThis lead is now out of the Lead Inbox.")
                return

            if step == "close_reason":
                reason_map={"1":"Not responding / went cold","2":"Customer no longer interested","3":"Not a fit for Handley Man","4":"Duplicate","5":"Already handled elsewhere"}
                if content == "6": processing_session["step"]="close_custom_reason"; await message.channel.send("What is the reason?"); return
                reason=reason_map.get(content)
                if not reason: await message.channel.send("Reply with 1, 2, 3, 4, 5, or 6."); return
                append_lead_note(lead,reason,"Lead closed")
                supabase.table("chisme_leads").update({"status":"closed","next_action":f"Closed: {reason}","callback_date":None,"updated_at":now_iso()}).eq("id",lead["id"]).execute()
                lead_processing_sessions.pop(message.author.id,None)
                await message.channel.send(f"🧹 Lead removed from the inbox.\nReason: **{reason}**"); return

            if step == "close_custom_reason":
                reason=content.strip()[:500] or "Other"; append_lead_note(lead,reason,"Lead closed")
                supabase.table("chisme_leads").update({"status":"closed","next_action":f"Closed: {reason}"[:500],"callback_date":None,"updated_at":now_iso()}).eq("id",lead["id"]).execute()
                lead_processing_sessions.pop(message.author.id,None)
                await message.channel.send(f"🧹 Lead removed from the inbox.\nReason: **{reason}**"); return

        # Lead capture scratchpad
        lead_session = lead_sessions.get(message.author.id)
        if lead_session:
            content = message.content.strip()

            if content.lower() == "done":
                notes = [n.strip() for n in lead_session.get("notes", []) if n.strip()]
                if not notes:
                    lead_sessions.pop(message.author.id, None)
                    await message.channel.send(
                        "📥 Lead capture closed. Nothing was saved because there were no notes."
                    )
                    return

                raw_notes = "\n".join(notes)
                rows = (
                    supabase.table("chisme_leads")
                    .insert({
                        "raw_notes": raw_notes,
                        "captured_at": lead_session.get("started_at") or now_iso(),
                        "captured_by": str(message.author),
                        "status": "unprocessed",
                        "next_action": "Process lead",
                        "updated_at": now_iso(),
                    })
                    .execute()
                ).data or []

                lead_sessions.pop(message.author.id, None)

                if rows:
                    await message.channel.send(
                        f"📥 **Lead saved.**\n"
                        f"{len(notes)} note{'s' if len(notes) != 1 else ''} captured.\n"
                        "Status: **Unprocessed**\n"
                        "Next: Process lead."
                    )
                else:
                    await message.channel.send(
                        "I tried to save that lead, but Supabase didn't return a saved row."
                    )
                return

            lead_session.setdefault("notes", []).append(content)
            count = len(lead_session["notes"])
            await message.channel.send(
                f"📝 Added to lead capture ({count} note{'s' if count != 1 else ''}). "
                "Keep going, or type `done`."
            )
            return

        # Interactive Chisme customer workspace
        chisme_session = chisme_sessions.get(message.author.id)
        if chisme_session:
            content = message.content.strip()
            step = chisme_session.get("step")

            contact_rows = (
                supabase.table("chisme_contacts")
                .select("*")
                .eq("id", chisme_session.get("contact_id"))
                .limit(1)
                .execute()
            ).data or []

            if not contact_rows:
                chisme_sessions.pop(message.author.id, None)
                await message.channel.send("I lost that customer card. Run `!chisme Name` and try again.")
                return

            contact = contact_rows[0]

            if step == "customer_menu":
                if content == "1":
                    chisme_session["step"] = "add_chisme"
                    await message.channel.send(
                        f"Tell me the chisme about **{contact.get('name')}**.\n"
                        "I'll save your next message to their journal."
                    )
                    return

                if content == "2":
                    chisme_session["step"] = "callback"
                    current_callback = contact.get("next_followup_date") or contact.get("next_contact_date")
                    await message.channel.send(
                        f"📅 Callback for **{contact.get('name')}**\n"
                        f"Current: {current_callback or 'None scheduled'}\n\n"
                        "1. Tomorrow\n"
                        "2. Next week\n"
                        "3. Two weeks\n"
                        "4. One month\n"
                        "5. Enter a date\n"
                        "6. No callback"
                    )
                    return

                if content == "3":
                    chisme_session["step"] = "temperature"
                    await message.channel.send(
                        f"What temperature should **{contact.get('name')}** be?\n"
                        "Enter a whole number from 0 to 100."
                    )
                    return

                if content == "4":
                    new_temp = max(51, min(99, int(contact.get("hotlist_temperature") or 51)))
                    # If this customer was in the Oven, remove the active-project row.
                    supabase.table("chisme_active").delete().eq("contact_id", contact["id"]).execute()
                    supabase.table("chisme_contacts").update({
                        "hotlist_temperature": new_temp,
                        "status": "lead",
                        "pipeline_stage": contact.get("pipeline_stage") or "New Lead",
                        "next_action": contact.get("next_action") or "Contact customer / schedule site visit",
                        "last_outcome": "Moved to Stovetop from Chisme workspace",
                        "updated_at": now_iso(),
                    }).eq("id", contact["id"]).execute()
                    add_note(
                        contact,
                        f"Moved to Stovetop at {new_temp}°.",
                        created_by=str(message.author),
                        note_type="chisme_movement",
                    )
                    chisme_sessions.pop(message.author.id, None)
                    await message.channel.send(
                        f"🍲 **{contact.get('name')}** moved to the Stovetop at {new_temp}°."
                    )
                    return

                if content == "5":
                    supabase.table("chisme_active").delete().eq("contact_id", contact["id"]).execute()
                    supabase.table("chisme_contacts").update({
                        "hotlist_temperature": 50,
                        "status": "lead",
                        "last_outcome": "Moved to Fridge from Chisme workspace",
                        "updated_at": now_iso(),
                    }).eq("id", contact["id"]).execute()
                    add_note(
                        contact,
                        "Moved to Fridge at 50°.",
                        created_by=str(message.author),
                        note_type="chisme_movement",
                    )
                    chisme_sessions.pop(message.author.id, None)
                    await message.channel.send(
                        f"🧊 **{contact.get('name')}** moved to the Fridge at 50°."
                    )
                    return

                if content == "6":
                    burner = next_available_burner()
                    supabase.table("chisme_contacts").update({
                        "hotlist_temperature": 100,
                        "status": "active_project",
                        "pipeline_stage": "Active Project",
                        "next_action": "Active project in oven",
                        "last_outcome": "Moved to Oven from Chisme workspace",
                        "updated_at": now_iso(),
                    }).eq("id", contact["id"]).execute()
                    set_active(
                        contact,
                        reason="Moved to Oven from Chisme workspace.",
                        burner_position=burner,
                        owner="Daniel",
                    )
                    add_note(
                        contact,
                        "Moved to Oven from Chisme workspace.",
                        created_by=str(message.author),
                        note_type="chisme_movement",
                    )
                    chisme_sessions.pop(message.author.id, None)
                    await message.channel.send(
                        f"🔥 **{contact.get('name')}** moved to the Oven.\n"
                        f"Oven slot: {burner}"
                    )
                    return

                if content == "7":
                    chisme_sessions.pop(message.author.id, None)
                    await message.channel.send("✅ Chisme workspace closed.")
                    return

                await message.channel.send("Reply with 1, 2, 3, 4, 5, 6, or 7.")
                return

            if step == "add_chisme":
                add_note(
                    contact,
                    content,
                    created_by=str(message.author),
                    note_type="chisme",
                )
                chisme_session["step"] = "customer_menu"
                await message.channel.send(
                    f"📝 Chisme saved for **{contact.get('name')}**.\n\n"
                    "1. Add more chisme\n"
                    "2. Set / change callback\n"
                    "3. Change temperature\n"
                    "4. Move to Stovetop\n"
                    "5. Move to Fridge\n"
                    "6. Move to Oven\n"
                    "7. Done"
                )
                return

            if step == "callback":
                if content == "5":
                    chisme_session["step"] = "callback_custom"
                    await message.channel.send("Type the callback date like `10/1/2026` or `2026-10-01`.")
                    return

                if content == "6":
                    supabase.table("chisme_contacts").update({
                        "next_followup_date": None,
                        "next_contact_date": None,
                        "updated_at": now_iso(),
                    }).eq("id", contact["id"]).execute()
                    add_note(
                        contact,
                        "Callback cleared.",
                        created_by=str(message.author),
                        note_type="callback",
                    )
                    chisme_sessions.pop(message.author.id, None)
                    await message.channel.send(
                        f"📅 No callback scheduled for **{contact.get('name')}**."
                    )
                    return

                if content == "4":
                    callback_date = (date.today() + timedelta(days=30)).isoformat()
                else:
                    callback_date = parse_followup_response(content)

                if not callback_date:
                    await message.channel.send("Reply with 1, 2, 3, 4, 5, or 6.")
                    return

                supabase.table("chisme_contacts").update({
                    "next_followup_date": callback_date,
                    "next_contact_date": callback_date,
                    "updated_at": now_iso(),
                }).eq("id", contact["id"]).execute()
                add_note(
                    contact,
                    f"Callback scheduled for {callback_date}.",
                    created_by=str(message.author),
                    note_type="callback",
                )
                chisme_sessions.pop(message.author.id, None)
                await message.channel.send(
                    f"📅 Callback set for **{contact.get('name')}**: {callback_date}"
                )
                return

            if step == "callback_custom":
                callback_date = parse_followup_response(content)
                if not callback_date:
                    await message.channel.send("Use a date like `10/1/2026` or `2026-10-01`.")
                    return

                supabase.table("chisme_contacts").update({
                    "next_followup_date": callback_date,
                    "next_contact_date": callback_date,
                    "updated_at": now_iso(),
                }).eq("id", contact["id"]).execute()
                add_note(
                    contact,
                    f"Callback scheduled for {callback_date}.",
                    created_by=str(message.author),
                    note_type="callback",
                )
                chisme_sessions.pop(message.author.id, None)
                await message.channel.send(
                    f"📅 Callback set for **{contact.get('name')}**: {callback_date}"
                )
                return

            if step == "temperature":
                try:
                    new_temp = int(content)
                except ValueError:
                    await message.channel.send("Enter a whole number from 0 to 100.")
                    return

                if new_temp < 0 or new_temp > 100:
                    await message.channel.send("Enter a number from 0 to 100.")
                    return

                updates = {
                    "hotlist_temperature": new_temp,
                    "updated_at": now_iso(),
                }

                if new_temp >= 100:
                    updates.update({
                        "status": "active_project",
                        "pipeline_stage": "Active Project",
                        "next_action": "Active project in oven",
                    })
                    burner = next_available_burner()
                    set_active(
                        contact,
                        reason="Temperature changed to 100° from Chisme workspace.",
                        burner_position=burner,
                        owner="Daniel",
                    )
                    destination = "Oven"
                elif new_temp >= 51:
                    supabase.table("chisme_active").delete().eq("contact_id", contact["id"]).execute()
                    updates["status"] = "lead"
                    destination = "Stovetop"
                else:
                    supabase.table("chisme_active").delete().eq("contact_id", contact["id"]).execute()
                    updates["status"] = "lead"
                    destination = "Fridge"

                supabase.table("chisme_contacts").update(updates).eq("id", contact["id"]).execute()
                add_note(
                    contact,
                    f"Temperature changed to {new_temp}°.",
                    created_by=str(message.author),
                    note_type="chisme_temperature",
                )
                chisme_sessions.pop(message.author.id, None)
                await message.channel.send(
                    f"🌡 **{contact.get('name')}** is now {new_temp}° — {destination}."
                )
                return

        # Interactive Stovetop workspace
        stovetop_session = stovetop_sessions.get(message.author.id)
        if stovetop_session:
            content = message.content.strip()
            step = stovetop_session.get("step")

            if step == "select_customer":
                if not content.isdigit():
                    await message.channel.send("Reply with the customer number, or `cancel`.")
                    return

                index = int(content) - 1
                customers = stovetop_session.get("customers") or []
                if index < 0 or index >= len(customers):
                    await message.channel.send("That number isn't on the Stovetop. Try again.")
                    return

                contact = customers[index]
                stovetop_session["contact_id"] = contact["id"]
                stovetop_session["contact_name"] = contact.get("name") or "Unknown"
                stovetop_session["step"] = "customer_menu"

                await message.channel.send(
                    f"🍲 **{contact.get('name')}**\n"
                    f"🌡 {contact.get('hotlist_temperature') or 51}° — "
                    f"{contact.get('pipeline_stage') or 'Stage not set'}\n"
                    f"Next: {contact.get('next_action') or 'No next action set'}\n\n"
                    "**What do you want to do?**\n"
                    "1. 📝 Add chisme\n"
                    "2. ➡️ Update pipeline stage\n"
                    "3. 📅 Set a callback\n"
                    "4. 🌡 Change temperature\n"
                    "5. 🧊 Move to Fridge\n"
                    "6. 🔥 Move to Oven\n"
                    "7. ❌ Cancel"
                )
                return

            contact_rows = (
                supabase.table("chisme_contacts")
                .select("*")
                .eq("id", stovetop_session.get("contact_id"))
                .limit(1)
                .execute()
            ).data or []

            if not contact_rows:
                stovetop_sessions.pop(message.author.id, None)
                await message.channel.send("I lost that customer card. Run `!stovetop` and try again.")
                return

            contact = contact_rows[0]

            if step == "customer_menu":
                if content == "1":
                    stovetop_session["step"] = "add_chisme"
                    await message.channel.send(
                        f"Tell me the chisme about **{contact.get('name')}**.\n"
                        "I'll save your next message to their journal."
                    )
                    return

                if content == "2":
                    stovetop_session["step"] = "pipeline_stage"
                    await message.channel.send(
                        f"Where is **{contact.get('name')}** right now?\n\n"
                        "1. Site Visit Completed\n"
                        "2. Estimate Ready\n"
                        "3. Estimate Sent\n"
                        "4. Follow-up Completed\n"
                        "5. Work Paused\n"
                        "6. Other"
                    )
                    return

                if content == "3":
                    stovetop_session["step"] = "callback"
                    await message.channel.send(
                        f"When should we contact **{contact.get('name')}** again?\n\n"
                        "1. Tomorrow\n"
                        "2. Next week\n"
                        "3. Two weeks\n"
                        "4. Type a date like `10/1/2026`\n"
                        "5. No callback"
                    )
                    return

                if content == "4":
                    stovetop_session["step"] = "temperature"
                    await message.channel.send(
                        f"What temperature should **{contact.get('name')}** be?\n"
                        "Enter a number from 0 to 99."
                    )
                    return

                if content == "5":
                    supabase.table("chisme_contacts").update({
                        "hotlist_temperature": 50,
                        "status": "lead",
                        "last_outcome": "Moved to Fridge",
                        "updated_at": now_iso(),
                    }).eq("id", contact["id"]).execute()
                    add_note(
                        contact,
                        "Moved from Stovetop to Fridge.",
                        created_by=str(message.author),
                        note_type="stovetop_movement",
                    )
                    stovetop_sessions.pop(message.author.id, None)
                    await message.channel.send(
                        f"🧊 **{contact.get('name')}** moved to the Fridge at 50°."
                    )
                    return

                if content == "6":
                    burner = next_available_burner()
                    supabase.table("chisme_contacts").update({
                        "hotlist_temperature": 100,
                        "status": "active_project",
                        "pipeline_stage": "Active Project",
                        "next_action": "Active project in oven",
                        "last_outcome": "Moved to Oven",
                        "updated_at": now_iso(),
                    }).eq("id", contact["id"]).execute()
                    set_active(
                        contact,
                        reason="Moved to Oven from interactive Stovetop.",
                        burner_position=burner,
                        owner="Daniel",
                    )
                    add_note(
                        contact,
                        "Moved from Stovetop to Oven.",
                        created_by=str(message.author),
                        note_type="stovetop_movement",
                    )
                    stovetop_sessions.pop(message.author.id, None)
                    await message.channel.send(
                        f"🔥 **{contact.get('name')}** moved to the Oven.\n"
                        f"Oven slot: {burner}"
                    )
                    return

                if content == "7":
                    stovetop_sessions.pop(message.author.id, None)
                    await message.channel.send("👍 Stovetop closed. Nothing changed.")
                    return

                await message.channel.send("Reply with 1, 2, 3, 4, 5, 6, or 7.")
                return

            if step == "add_chisme":
                add_note(
                    contact,
                    content,
                    created_by=str(message.author),
                    note_type="chisme",
                )
                stovetop_sessions.pop(message.author.id, None)
                await message.channel.send(
                    f"📝 Chisme saved for **{contact.get('name')}**.\n"
                    "Run `!stovetop` when you're ready for the next thing."
                )
                return

            if step == "pipeline_stage":
                stage_map = {
                    "1": ("Site Visit Completed", 60, "Write estimate notes"),
                    "2": ("Estimate Ready", 70, "Send estimate"),
                    "3": ("Estimate Sent", 80, "Follow up"),
                    "4": ("Follow-up Completed", 90, "Await approval"),
                    "5": ("Work Paused", contact.get("hotlist_temperature") or 60,
                          "Check in with customer about resuming work"),
                }

                if content == "6":
                    stovetop_session["step"] = "custom_pipeline_stage"
                    await message.channel.send("What stage should I use?")
                    return

                if content not in stage_map:
                    await message.channel.send("Reply with 1, 2, 3, 4, 5, or 6.")
                    return

                stage_name, temp, next_action = stage_map[content]
                supabase.table("chisme_contacts").update({
                    "pipeline_stage": stage_name,
                    "hotlist_temperature": temp,
                    "next_action": next_action,
                    "last_outcome": f"Pipeline stage updated: {stage_name}",
                    "updated_at": now_iso(),
                }).eq("id", contact["id"]).execute()
                add_note(
                    contact,
                    f"Pipeline stage updated to {stage_name}. Next: {next_action}.",
                    created_by=str(message.author),
                    note_type="hotlist_progress",
                )
                stovetop_sessions.pop(message.author.id, None)
                await message.channel.send(
                    f"✅ **{contact.get('name')}** updated.\n"
                    f"🌡 {temp}° — {stage_name}\n"
                    f"Next: {next_action}"
                )
                return

            if step == "custom_pipeline_stage":
                stage_name = content[:120]
                supabase.table("chisme_contacts").update({
                    "pipeline_stage": stage_name,
                    "last_outcome": f"Pipeline stage updated: {stage_name}",
                    "updated_at": now_iso(),
                }).eq("id", contact["id"]).execute()
                add_note(
                    contact,
                    f"Pipeline stage updated to {stage_name}.",
                    created_by=str(message.author),
                    note_type="hotlist_progress",
                )
                stovetop_sessions.pop(message.author.id, None)
                await message.channel.send(
                    f"✅ **{contact.get('name')}** stage set to **{stage_name}**.\n"
                    f"Next action left as: {contact.get('next_action') or 'not set'}"
                )
                return

            if step == "callback":
                callback_date = parse_followup_response(content)

                if content == "5":
                    supabase.table("chisme_contacts").update({
                        "next_followup_date": None,
                        "next_contact_date": None,
                        "updated_at": now_iso(),
                    }).eq("id", contact["id"]).execute()
                    add_note(
                        contact,
                        "Callback cleared.",
                        created_by=str(message.author),
                        note_type="callback",
                    )
                    stovetop_sessions.pop(message.author.id, None)
                    await message.channel.send(
                        f"📅 No callback scheduled for **{contact.get('name')}**."
                    )
                    return

                if not callback_date:
                    await message.channel.send(
                        "I couldn't read that date. Use `MM/DD/YYYY` or `YYYY-MM-DD`, "
                        "or reply 1, 2, 3, or 5."
                    )
                    return

                supabase.table("chisme_contacts").update({
                    "next_followup_date": callback_date,
                    "next_contact_date": callback_date,
                    "updated_at": now_iso(),
                }).eq("id", contact["id"]).execute()
                add_note(
                    contact,
                    f"Callback scheduled for {callback_date}.",
                    created_by=str(message.author),
                    note_type="callback",
                )
                stovetop_sessions.pop(message.author.id, None)
                await message.channel.send(
                    f"📅 Callback set for **{contact.get('name')}**: {callback_date}"
                )
                return

            if step == "temperature":
                try:
                    new_temp = int(content)
                except ValueError:
                    await message.channel.send("Enter a whole number from 0 to 99.")
                    return

                if new_temp < 0 or new_temp > 99:
                    await message.channel.send("Enter a number from 0 to 99.")
                    return

                supabase.table("chisme_contacts").update({
                    "hotlist_temperature": new_temp,
                    "updated_at": now_iso(),
                }).eq("id", contact["id"]).execute()
                add_note(
                    contact,
                    f"Temperature changed to {new_temp}°.",
                    created_by=str(message.author),
                    note_type="stovetop_temperature",
                )
                stovetop_sessions.pop(message.author.id, None)

                destination = "Stovetop" if new_temp >= 51 else "Fridge"
                await message.channel.send(
                    f"🌡 **{contact.get('name')}** is now {new_temp}° — {destination}."
                )
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
                f"`!stovetop {contact.get('name')} notes`"
            )
            return

        # Oven removal workflow
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
                "last_outcome": f"Took out of oven: {reason_label}",
                "updated_at": now_iso(),
            }

            if reason_key in {"chose_someone_else", "changed_mind", "job_completed"}:
                updates["status"] = "closed"
                updates["next_action"] = "None"
            else:
                updates["status"] = "lead"
                updates["pipeline_stage"] = "New Lead"
                updates["next_action"] = "Follow up" if followup_date else "Contact customer / schedule site visit"

            if followup_date:
                updates["next_followup_date"] = followup_date
                updates["next_contact_date"] = followup_date

            supabase.table("chisme_contacts").update(updates).eq("id", contact["id"]).execute()

            add_note(
                contact,
                (
                    f"Project moved out of the Oven.\n\n"
                    f"Reason: {reason_label}\n"
                    f"Follow-up: {followup_date or 'none'}\n"
                    f"Note: {user_note}"
                ),
                created_by=str(message.author),
                note_type="active_removed",
            )

            del cremove_sessions[message.author.id]

            await message.channel.send(
                f"✅ Took **{contact.get('name')}** out of the Oven.\n"
                f"Reason: {reason_label}\n"
                f"Follow-up: {followup_date or 'none'}"
            )
