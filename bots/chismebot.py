import os
import re
import traceback
from datetime import date, datetime, timedelta

from config import client
from supabase import create_client


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

cremove_sessions = {}

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
            "lead_temperature": 25,
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
            "note_text": note,   # ✅ correct
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
        "temp": "lead_temperature",
        "temperature": "lead_temperature",
        "lead_temperature": "lead_temperature",
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

        if col == "lead_temperature":
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

def update_temperature(contact, event_type):
    temp = contact.get("lead_temperature", 0)

    if event_type == "customer_inbound_message":
        temp += 15

    elif event_type == "quote_accepted":
        temp = 100

    elif event_type == "quote_sent_no_response":
        temp -= 10

    elif event_type == "reschedule":
        temp -= 10

    elif event_type == "followup_missed":
        temp -= 15

    elif event_type == "job_completed":
        temp = max(temp, 20)

    elif event_type == "reengaged":
        temp += 20

    # clamp values
    temp = max(0, min(100, temp))

    supabase.table("chisme_contacts") \
        .update({"lead_temperature": temp}) \
        .eq("id", contact["id"]) \
        .execute()

    return temp


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
            "`!cactive Name | burner: 1 | reason: project needs attention`\n"
            "Put active project on the stovetop. Burner 1 is front burner; 4 is back burner.\n\n"
            "`!stovetop`\n"
            "Show stovetop / active projects by burner position.\n\n"
            "`!cremove Name`\n"
            "Take a project off the stovetop with guided follow-up prompts.\n\n"
            "`!cshow Name`\n"
            "Show Rolodex card, lead_temperature, active status, and recent chisme."
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
                f"Temperature: {contact.get('lead_temperature') or 0}",
                "",
            ]

            real_notes = [n for n in notes if n.get("note_type") != "journal_anchor" and (n.get("note_text") or "").strip()]

            if not real_notes:
                lines.append("No chisme notes yet.")
                lines.append(f"Add one with: `!chisme {contact.get('name')} | <note>`")
            else:
                for n in real_notes[:8]:
                    lines.append(f"- {n.get('note_date')}: {short(n.get('note_text'), 250)}")

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

    @bot.command(name="cactive", aliases=["stove"])
    async def cactive(ctx, *, raw=""):
        if not raw.strip():
            await ctx.send("Use: `!stove Name | burner: 1 | reason: project needs attention`")
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
                f"  Temp: {c.get('lead_temperature') or 0}\n"
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
            f"Temperature: {c.get('lead_temperature') or 0}",
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
        real_notes = [n for n in notes if n.get("note_type") != "journal_anchor" and (n.get("note_text") or "").strip()]
        if not real_notes:
            lines.append("No chisme notes yet.")
        else:
            for n in real_notes[:8]:
                lines.append(f"- {n.get('note_date')}: {short(n.get('note_text'), 220)}")

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
            "step": "reason"
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
        
        if message.content.startswith("!"):
            return

        session = cremove_sessions.get(message.author.id)
        if not session:
            return

        content = message.content.strip()
        contact = session["contact"]

        reason_map = {
            "1": ("not_ready", "Not ready / needs to reschedule", 50),
            "2": ("not_responding", "Not responding", 40),
            "3": ("chose_someone_else", "Chose someone else", 10),
            "4": ("changed_mind", "Changed mind", 20),
            "5": ("job_completed", "Job completed", 25),
            "6": ("other", "Other", contact.get("lead_temperature") or 0),
        }

        if session["step"] == "reason":
            if content not in reason_map:
                await message.channel.send("Reply with 1, 2, 3, 4, 5, or 6.")
                return

            if content == "6":
                session["step"] = "custom_reason"
                await message.channel.send("What is the reason?")
                return
            
            reason_key, reason_label, new_temp = reason_map[content]
            session["reason_key"] = reason_key
            session["reason_label"] = reason_label
            session["new_temp"] = new_temp
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
            session["reason_key"] = "other"
            session["reason_label"] = content
            session["new_temp"] = contact.get("lead_temperature") or 0
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
            try:
                parsed = datetime.strptime(content, "%m/%d/%Y").date()
                session["followup_date"] = parsed.isoformat()
                session["step"] = "note"
                await message.channel.send("Add a note for the chisme log:")
            except ValueError:
                await message.channel.send("Use date format like `7/3/2026`.")
            return

        if session["step"] == "note":
            followup_date = session.get("followup_date")
            reason_label = session["reason_label"]
            new_temp = session["new_temp"]
            user_note = content

            supabase.table("chisme_active").delete().eq("contact_id", contact["id"]).execute()

            updates = {
                "lead_temperature": new_temp,
                "last_outcome": f"Took off stovetop: {reason_label}",
                "updated_at": now_iso(),
            }

            if followup_date:
                updates["next_followup_date"] = followup_date
                updates["next_contact_date"] = followup_date

            supabase.table("chisme_contacts").update(updates).eq("id", contact["id"]).execute()

            add_note(
                contact,
                (
                    f"Took off stovetop.\n"
                    f"Reason: {reason_label}\n"
                    f"Follow-up: {followup_date or 'none'}\n"
                    f"Lead temperature set to: {new_temp}\n"
                    f"Note: {user_note}"
                ),
                created_by=str(message.author),
                note_type="active_removed"
            )

            del cremove_sessions[message.author.id]

            await message.channel.send(
                f"✅ Took **{contact.get('name')}** off the stovetop.\n"
                f"Reason: {reason_label}\n"
                f"Follow-up: {followup_date or 'none'}\n"
                f"Temperature: {new_temp}"
            )
