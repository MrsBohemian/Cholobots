import os
import re
import traceback
from datetime import date, datetime, timedelta

from config import client
from supabase import create_client


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ---------- BASIC HELPERS ----------

def now_iso():
    return datetime.now().isoformat()


def today_date():
    return date.today().isoformat()


def short_text(text: str, limit: int = 220) -> str:
    text = (text or "").replace("\n", " · ").strip()
    return text[:limit] + ("..." if len(text) > limit else "")


async def send_long(ctx, text: str, limit: int = 1900):
    text = str(text or "").strip() or "(empty)"
    for i in range(0, len(text), limit):
        await ctx.send(text[i:i + limit])


def safe_text_from_openai_response(resp) -> str:
    text = getattr(resp, "output_text", None)
    if isinstance(text, str) and text.strip():
        return text.strip()

    try:
        if hasattr(resp, "output") and resp.output:
            first = resp.output[0]
            if hasattr(first, "content") and first.content:
                c0 = first.content[0]
                if hasattr(c0, "text") and isinstance(c0.text, str):
                    return c0.text.strip()
                if isinstance(c0, dict) and isinstance(c0.get("text"), str):
                    return c0["text"].strip()
    except Exception:
        pass

    return ""


def split_name_payload(raw: str):
    """
    Preferred format:
    !chisme Gail Thompson | talked about birds and pressure washing

    Also supports:
    !chisme Gail Thompson talked about birds...
    """
    raw = (raw or "").strip()

    if "|" in raw:
        name, payload = raw.split("|", 1)
        return name.strip(), payload.strip()

    words = raw.split()
    if len(words) >= 3:
        return " ".join(words[:2]).strip(), " ".join(words[2:]).strip()

    return raw.strip(), ""


def extract_followup_date(text: str):
    text = text or ""

    # follow up 2026-07-01 / follow-up on 2026-07-01
    match = re.search(r"follow[\s-]*up\s*(?:on)?\s*(\d{4}-\d{2}-\d{2})", text, re.I)
    if match:
        return match.group(1)

    if "tomorrow" in text.lower():
        return (date.today() + timedelta(days=1)).isoformat()

    if "next week" in text.lower():
        return (date.today() + timedelta(days=7)).isoformat()

    if "last week of month" in text.lower() or "last week of the month" in text.lower():
        today = date.today()
        if today.month == 12:
            first_next_month = date(today.year + 1, 1, 1)
        else:
            first_next_month = date(today.year, today.month + 1, 1)

        last_day = first_next_month - timedelta(days=1)
        return (last_day - timedelta(days=5)).isoformat()

    return None


def extract_money_amount(text: str):
    match = re.search(r"\$?\s*(\d+(?:\.\d{1,2})?)", text or "")
    return float(match.group(1)) if match else None


def infer_contact_status_from_notes(notes: str):
    lowered = (notes or "").lower()

    if any(x in lowered for x in ["scheduled", "booked", "work starts", "on calendar"]):
        return "active_project"

    if any(x in lowered for x in ["estimate sent", "quote sent", "quoted", "proposal sent"]):
        return "estimate_sent"

    if any(x in lowered for x in ["new lead", "called about", "asked about", "request"]):
        return "lead"

    return None


def infer_interaction_type(notes: str):
    lowered = (notes or "").lower()

    if "text" in lowered:
        return "text"
    if "estimate" in lowered or "quote" in lowered:
        return "estimate"
    if "site visit" in lowered:
        return "site_visit"
    if "invoice" in lowered:
        return "invoice"
    if "paid" in lowered or "payment" in lowered:
        return "payment"
    if any(x in lowered for x in ["called", "call", "voicemail", "no answer"]):
        return "call"

    return "chisme"


# ---------- SUPABASE HELPERS ----------

def find_contact(name: str):
    name = (name or "").strip()
    if not name:
        return None

    rows = (
        supabase.table("chisme_contacts")
        .select("*")
        .ilike("name", f"%{name}%")
        .limit(1)
        .execute()
    ).data or []

    return rows[0] if rows else None


def find_or_create_contact(name: str):
    existing = find_contact(name)
    if existing:
        return existing

    response = (
        supabase.table("chisme_contacts")
        .insert({
            "name": name,
            "source": "chismebot",
            "source_customer_name": name,
            "status": "lead",
            "active_communication": False,
            "active_priority": 50,
            "next_followup_date": today_date(),
            "next_contact_date": today_date(),
            "chisme_summary": "Created from Chismebot.",
            "updated_at": now_iso(),
        })
        .execute()
    )

    rows = response.data or []
    return rows[0] if rows else None


def get_recent_interactions(contact_id: int, limit: int = 8):
    return (
        supabase.table("chisme_interactions")
        .select("*")
        .eq("contact_id", contact_id)
        .order("created_at", desc=True)
        .limit(limit)
        .execute()
    ).data or []


def synthesize_chisme_summary(contact, new_note: str):
    old_summary = contact.get("chisme_summary") or ""

    try:
        resp = client.responses.create(
            model="gpt-5-mini",
            reasoning={"effort": "low"},
            input=[
                {
                    "role": "system",
                    "content": (
                        "You update a contractor CRM customer summary.\n"
                        "Keep it practical, grounded, and useful for future calls, estimates, "
                        "and subcontractor handoffs.\n\n"
                        "Return two sections:\n"
                        "SUMMARY: a concise updated customer record.\n"
                        "DELTA: what changed or was learned from the newest note.\n\n"
                        "Do not invent facts."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Customer: {contact.get('name')}\n\n"
                        f"Existing summary:\n{old_summary}\n\n"
                        f"New note:\n{new_note}"
                    ),
                },
            ],
            max_output_tokens=700,
        )

        text = safe_text_from_openai_response(resp)
        if not text:
            raise ValueError("No OpenAI text returned")

        summary = text
        delta = ""

        if "DELTA:" in text:
            parts = text.split("DELTA:", 1)
            summary = parts[0].replace("SUMMARY:", "").strip()
            delta = parts[1].strip()
        else:
            summary = text.replace("SUMMARY:", "").strip()
            delta = new_note

        return summary[:1800], delta[:1000], text

    except Exception:
        fallback = (old_summary + "\n" + new_note).strip()
        return fallback[:1800], new_note[:1000], fallback[:1800]


def insert_interaction(contact_id: int, interaction_type: str, notes: str, outcome: str = None,
                       next_action: str = None, next_followup_date: str = None,
                       created_by: str = None, summary_delta: str = None,
                       touch_counts_for_today: bool = True):
    response = (
        supabase.table("chisme_interactions")
        .insert({
            "contact_id": contact_id,
            "interaction_date": today_date(),
            "interaction_type": interaction_type,
            "notes": notes,
            "outcome": outcome,
            "next_action": next_action,
            "next_contact_date": next_followup_date,
            "next_followup_date": next_followup_date,
            "created_by": created_by,
            "summary_delta": summary_delta,
            "touch_counts_for_today": touch_counts_for_today,
        })
        .execute()
    )

    rows = response.data or []
    return rows[0] if rows else None


def update_contact_after_note(contact, note: str, summary: str):
    followup_date = extract_followup_date(note)
    estimate_value = extract_money_amount(note)
    inferred_status = infer_contact_status_from_notes(note)

    updates = {
        "chisme_summary": summary[:1800],
        "last_contact_date": today_date(),
        "last_outcome": note[:500],
        "daily_touch_date": today_date(),
        "daily_touch_counted": True,
        "updated_at": now_iso(),
    }

    if followup_date:
        updates["next_followup_date"] = followup_date
        updates["next_contact_date"] = followup_date

    if estimate_value is not None:
        updates["estimate_value"] = estimate_value

    if inferred_status:
        updates["status"] = inferred_status

    (
        supabase.table("chisme_contacts")
        .update(updates)
        .eq("id", contact["id"])
        .execute()
    )


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
    response = (
        supabase.table("chisme_interactions")
        .select("id", count="exact")
        .eq("interaction_date", today_date())
        .eq("touch_counts_for_today", True)
        .execute()
    )

    return response.count or 0


# ---------- DISCORD COMMANDS ----------

def register_chisme(bot):

    @bot.command(name="chismebot")
    async def chismebot_help(ctx):
        await ctx.send(
            "💬 **CHISMEBOT COMMANDS**\n\n"
            "`!chisme Name | notes`\n"
            "Add customer notes, save an interaction, and update the customer chisme summary.\n"
            "Example: `!chisme Gail Thompson | looked at bird droppings, recommend tree trimming and $100 pressure wash`\n\n"
            "`!cactive Name | reason`\n"
            "Put a customer on the active communication index-card list.\n"
            "Example: `!cactive Michael Lawrence | bathroom estimate follow-up`\n\n"
            "`!clist`\n"
            "Show active customer communication list.\n\n"
            "`!cremove Name | follow up YYYY-MM-DD | notes`\n"
            "Remove from active list and schedule future follow-up.\n"
            "Example: `!cremove Gail Thompson | follow up 2026-07-01 | waiting on tree trimming`\n\n"
            "`!ccontact Name`\n"
            "Show phone, email, and address.\n\n"
            "`!cshow Name`\n"
            "Show full customer chisme record and recent notes.\n\n"
            "`!cprogress 10`\n"
            "Show today’s customer communication progress against a daily target."
        )

    @bot.command(name="chisme")
    async def chisme(ctx, *, raw: str = ""):
        if not raw.strip():
            await ctx.send("Use: `!chisme Name | notes`")
            return

        name, note = split_name_payload(raw)

        if not name or not note:
            await ctx.send("Use: `!chisme Name | notes`")
            return

        await ctx.send("…saving chisme…")

        try:
            contact = find_or_create_contact(name)

            if not contact:
                await ctx.send("⚠️ Could not find or create customer record.")
                return

            summary, delta, full_ai_text = synthesize_chisme_summary(contact, note)
            interaction_type = infer_interaction_type(note)
            followup_date = extract_followup_date(note)

            insert_interaction(
                contact_id=contact["id"],
                interaction_type=interaction_type,
                notes=note,
                outcome=note[:500],
                next_followup_date=followup_date,
                created_by=str(ctx.author),
                summary_delta=delta,
                touch_counts_for_today=True,
            )

            update_contact_after_note(contact, note, summary)

            await send_long(
                ctx,
                f"✅ Chisme saved for **{contact.get('name')}**\n\n"
                f"**Updated summary:**\n{summary}\n\n"
                f"**Delta:**\n{delta}"
            )

        except Exception:
            print("=== CHISME ERROR ===")
            traceback.print_exc()
            await ctx.send("⚠️ Chismebot error. Check Railway logs.")

    @bot.command(name="cactive")
    async def cactive(ctx, *, raw: str = ""):
        if not raw.strip():
            await ctx.send("Use: `!cactive Name | reason`")
            return

        name, reason = split_name_payload(raw)
        reason = reason or "Active customer communication"

        try:
            contact = find_or_create_contact(name)

            updates = {
                "active_communication": True,
                "active_reason": reason,
                "active_since": today_date(),
                "active_owner": "Daniel",
                "updated_at": now_iso(),
            }

            (
                supabase.table("chisme_contacts")
                .update(updates)
                .eq("id", contact["id"])
                .execute()
            )

            insert_interaction(
                contact_id=contact["id"],
                interaction_type="active_added",
                notes=f"Added to active communication: {reason}",
                outcome="active_communication",
                created_by=str(ctx.author),
                summary_delta=f"Active communication reason: {reason}",
                touch_counts_for_today=False,
            )

            await ctx.send(f"✅ Active communication added: **{contact.get('name')}** — {reason}")

        except Exception:
            print("=== CACTIVE ERROR ===")
            traceback.print_exc()
            await ctx.send("⚠️ Could not add active communication. Check Railway logs.")

    @bot.command(name="clist")
    async def clist(ctx):
        try:
            contacts = get_active_contacts()

            if not contacts:
                await ctx.send("No active customer communication right now.")
                return

            lines = ["📇 **Active Customer Communication**\n"]

            for idx, c in enumerate(contacts, start=1):
                name = c.get("name") or "Unknown"
                reason = c.get("active_reason") or c.get("next_action") or "No reason listed"
                followup = c.get("next_followup_date") or c.get("next_contact_date") or "not set"
                phone = c.get("phone") or ""
                summary = short_text(c.get("chisme_summary") or "", 140)

                lines.append(
                    f"{idx}. **{name}**\n"
                    f"   Reason: {reason}\n"
                    f"   Follow-up: {followup}\n"
                    f"   Phone: {phone or 'not saved'}\n"
                    f"   Chisme: {summary or 'none'}\n"
                )

            await send_long(ctx, "\n".join(lines))

        except Exception:
            print("=== CLIST ERROR ===")
            traceback.print_exc()
            await ctx.send("⚠️ Could not load active communication list.")

    @bot.command(name="cremove")
    async def cremove(ctx, *, raw: str = ""):
        if not raw.strip():
            await ctx.send("Use: `!cremove Name | follow up YYYY-MM-DD | notes`")
            return

        parts = [p.strip() for p in raw.split("|")]
        name = parts[0]
        detail = " | ".join(parts[1:]).strip() if len(parts) > 1 else ""

        contact = find_contact(name)

        if not contact:
            await ctx.send(f"No customer found for: {name}")
            return

        followup_date = extract_followup_date(detail) or (date.today() + timedelta(days=30)).isoformat()
        notes = detail or f"Removed from active communication. Follow up {followup_date}."

        try:
            (
                supabase.table("chisme_contacts")
                .update({
                    "active_communication": False,
                    "active_reason": None,
                    "active_priority": 50,
                    "next_followup_date": followup_date,
                    "next_contact_date": followup_date,
                    "last_outcome": notes[:500],
                    "updated_at": now_iso(),
                })
                .eq("id", contact["id"])
                .execute()
            )

            insert_interaction(
                contact_id=contact["id"],
                interaction_type="active_removed",
                notes=notes,
                outcome="removed_from_active",
                next_followup_date=followup_date,
                created_by=str(ctx.author),
                summary_delta=f"Removed from active communication. Next follow-up: {followup_date}.",
                touch_counts_for_today=False,
            )

            await ctx.send(
                f"✅ Removed from active communication: **{contact.get('name')}**\n"
                f"Next follow-up: `{followup_date}`"
            )

        except Exception:
            print("=== CREMOVE ERROR ===")
            traceback.print_exc()
            await ctx.send("⚠️ Could not remove active communication. Check Railway logs.")

    @bot.command(name="ccontact")
    async def ccontact(ctx, *, name: str = ""):
        if not name.strip():
            await ctx.send("Use: `!ccontact Name`")
            return

        contact = find_contact(name)

        if not contact:
            await ctx.send(f"No customer found for: {name}")
            return

        lines = [
            f"☎️ **{contact.get('name')}**",
            f"Phone: {contact.get('phone') or 'not saved'}",
            f"Email: {contact.get('email') or 'not saved'}",
            f"Address: {contact.get('address') or 'not saved'}",
            f"Preferred contact: {contact.get('preferred_contact') or 'not saved'}",
            f"Status: {contact.get('status') or 'unknown'}",
        ]

        await ctx.send("\n".join(lines))

    @bot.command(name="cshow")
    async def cshow(ctx, *, name: str = ""):
        if not name.strip():
            await ctx.send("Use: `!cshow Name`")
            return

        contact = find_contact(name)

        if not contact:
            await ctx.send(f"No customer found for: {name}")
            return

        interactions = get_recent_interactions(contact["id"], limit=8)

        lines = [
            f"💬 **{contact.get('name')}**",
            "",
            f"Status: {contact.get('status') or 'unknown'}",
            f"Active: {contact.get('active_communication')}",
            f"Reason: {contact.get('active_reason') or 'none'}",
            f"Next follow-up: {contact.get('next_followup_date') or contact.get('next_contact_date') or 'not set'}",
            f"Last contact: {contact.get('last_contact_date') or 'not set'}",
            f"Estimate value: {contact.get('estimate_value') or 'not set'}",
            "",
            "**Chisme summary:**",
            contact.get("chisme_summary") or "No summary yet.",
            "",
            "**Recent interactions:**",
        ]

        if not interactions:
            lines.append("No interactions yet.")
        else:
            for item in interactions:
                lines.append(
                    f"- {item.get('interaction_date') or item.get('created_at')}: "
                    f"{item.get('interaction_type') or 'note'} — {short_text(item.get('notes'), 180)}"
                )

        await send_long(ctx, "\n".join(lines))

    @bot.command(name="cprogress")
    async def cprogress(ctx, target: int = 10):
        try:
            count = get_today_touch_count()
            pct = round((count / target) * 100) if target else 0
            pct = min(100, pct)

            filled = max(0, min(10, round(pct / 10)))
            bar = "█" * filled + "░" * (10 - filled)

            await ctx.send(
                f"📞 **Customer Communication Progress**\n\n"
                f"{bar} {count} / {target}\n"
                f"{pct}% complete today"
            )

        except Exception:
            print("=== CPROGRESS ERROR ===")
            traceback.print_exc()
            await ctx.send("⚠️ Could not calculate progress.")
