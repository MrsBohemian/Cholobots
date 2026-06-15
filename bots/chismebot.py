import traceback
import json
from pathlib import Path
from datetime import datetime
from config import client
import os
from urllib import request, error
from supabase import create_client
from datetime import date, timedelta

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

DATA_SERVICE_URL = os.getenv("DATA_SERVICE_URL", "").rstrip("/")

# Chisme = long-term narrative/sociological database.
CHISME_FILE = Path("chisme.json")

# Follow-ups = short-term operational call/action list for Command Center.

def now_iso():
    return datetime.now().isoformat()


def load_json(path: Path, default):
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default


def save_json(path: Path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_chisme():
    return load_json(CHISME_FILE, [])


def save_chisme(items):
    save_json(CHISME_FILE, items)

def load_followups():
    response = (
        supabase.table("chisme_followups")
        .select("*")
        .neq("status", "done")
        .order("created_at")
        .execute()
    )
    return response.data or []

def today_date():
    return date.today().isoformat()


def get_due_chisme_contacts(limit: int = 20):
    response = (
        supabase.table("chisme_contacts")
        .select("*")
        .lte("next_contact_date", today_date())
        .in_("status", ["past_customer", "vip_customer", "repeat_customer", "lead", "estimate_sent", "active_project"])
        .order("next_contact_date")
        .order("job_count", desc=True)
        .limit(limit)
        .execute()
    )
    return response.data or []


def build_queue_reason(contact):
    status = contact.get("status") or "past_customer"

    if status == "active_project":
        return "Active workflow communication"
    if status == "lead":
        return "New lead follow-up"
    if status == "estimate_sent":
        return "Estimate follow-up"
    if status == "vip_customer":
        return "VIP customer check-in"
    if status == "repeat_customer":
        return "Repeat customer relationship touchpoint"

    return "Past customer seasonal check-in"


def build_script_hint(contact):
    name = contact.get("name") or "there"
    status = contact.get("status") or "past_customer"

    if status in ["past_customer", "vip_customer", "repeat_customer"]:
        return (
            f"Hi {name}, this is Daniel with Handley Man. "
            "We worked on your house a while back, and I wanted to check in. "
            "How has everything been holding up? Is there anything on your project list "
            "you'd like us to take a look at?"
        )

    if status == "estimate_sent":
        return (
            f"Hi {name}, this is Daniel with Handley Man. "
            "I'm following up on the estimate we sent over. "
            "Did you have any questions or would you like us to look at scheduling?"
        )

    if status == "active_project":
        return (
            f"Hi {name}, this is Daniel with Handley Man. "
            "I'm checking in on the next step for your current project."
        )

    return (
        f"Hi {name}, this is Daniel with Handley Man. "
        "I'm following up on your request to see how we can help."
    )


def create_chisme_daily_queue(target_count: int = 20, assigned_to: str = "Daniel"):
    # Avoid duplicating today's queue.
    existing = (
        supabase.table("chisme_communication_queue")
        .select("*")
        .eq("queue_date", today_date())
        .neq("status", "done")
        .execute()
    ).data or []

    if existing:
        return existing

    contacts = get_due_chisme_contacts(target_count)

    rows = []
    for idx, contact in enumerate(contacts, start=1):
        rows.append({
            "queue_date": today_date(),
            "contact_id": contact["id"],
            "bucket": contact.get("status") or "past_customer",
            "reason": build_queue_reason(contact),
            "script_hint": build_script_hint(contact),
            "priority": idx,
            "status": "queued",
            "assigned_to": assigned_to,
        })

    if not rows:
        return []

    response = supabase.table("chisme_communication_queue").insert(rows).execute()
    return response.data or []


def get_today_queue():
    response = (
        supabase.table("chisme_communication_queue")
        .select("*, chisme_contacts(*)")
        .eq("queue_date", today_date())
        .order("priority")
        .execute()
    )
    return response.data or []


def get_next_queued_item():
    response = (
        supabase.table("chisme_communication_queue")
        .select("*, chisme_contacts(*)")
        .eq("queue_date", today_date())
        .eq("status", "queued")
        .order("priority")
        .limit(1)
        .execute()
    )
    data = response.data or []
    return data[0] if data else None

def push_followups_to_dashboard(items):
    if not DATA_SERVICE_URL:
        return {"ok": False, "reason": "DATA_SERVICE_URL not set"}

    url = f"{DATA_SERVICE_URL}/chisme_followups.json"
    data = json.dumps(items).encode("utf-8")

    req = request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    try:
        with request.urlopen(req, timeout=10) as resp:
            return {"ok": True, "status": resp.status}
    except Exception as e:
        return {"ok": False, "reason": str(e)}

def add_followup(item):
    supabase.table("chisme_followups").insert(item).execute()


def update_followup(item_id, updates):
    (
        supabase.table("chisme_followups")
        .update(updates)
        .eq("id", item_id)
        .execute()
    )

def safe_text_from_openai_response(resp) -> str:
    """
    Tries the most common places text appears in the OpenAI SDK response.
    Falls back to a safe message if needed.
    """
    text = getattr(resp, "output_text", None)
    if isinstance(text, str) and text.strip():
        return text

    try:
        if hasattr(resp, "output") and resp.output:
            first = resp.output[0]
            if hasattr(first, "content") and first.content:
                c0 = first.content[0]
                if hasattr(c0, "text") and isinstance(c0.text, str):
                    return c0.text
                if isinstance(c0, dict) and "text" in c0 and isinstance(c0["text"], str):
                    return c0["text"]
    except Exception:
        pass

    return "(No visible text returned — model may have been truncated.)"


async def send_long(ctx, text: str, limit: int = 1900):
    """
    Discord hard limit is 2000 characters. Use 1900 to be safe.
    Splits long text across multiple messages.
    """
    if not isinstance(text, str):
        text = str(text)

    text = text.strip() or "(empty response)"
    for i in range(0, len(text), limit):
        await ctx.send(text[i : i + limit])


def short_text(text: str, limit: int = 180) -> str:
    text = (text or "").replace("\n", " · ").strip()
    return text[:limit] + ("..." if len(text) > limit else "")


def find_followup_index(items, query: str):
    q = query.strip().lower()
    if not q:
        return None

    # Allow numeric selection from list.
    if q.isdigit():
        idx = int(q) - 1
        open_items = [i for i, item in enumerate(items) if item.get("status") != "done"]
        if 0 <= idx < len(open_items):
            return open_items[idx]

    # Match by name/reason/raw text.
    for i, item in enumerate(items):
        if item.get("status") == "done":
            continue

        haystack = " ".join([
            str(item.get("name", "")),
            str(item.get("reason", "")),
            str(item.get("raw_note", "")),
        ]).lower()

        if q in haystack:
            return i

    return None


# ---------- CHISMEBOT COMMANDS ----------

def register_chisme(bot):

    @bot.command(name="chismebot")
    async def chismebot_help(ctx):
        await ctx.send(
            "💬 CHISMEBOT COMMANDS\n\n"
            "Chismebot has two separate jobs:\n\n"
            "1. `!chisme <note>`\n"
            "Save narrative data about customers, leads, people, and your network.\n"
            "This is long-term context for later analysis and opportunity mining.\n\n"
            "2. `!followup <name/reason>`\n"
            "Add a short-term action item to the Command Center customer follow-up list.\n\n"
            "Other commands:\n"
            "`!chismelist` — show recent saved chisme notes\n"
            "`!followuplist` — show active follow-ups\n"
            "`!followupdone <name or number>` — mark a follow-up done\n\n"
            "Examples:\n"
            "`!chisme Gail Thompson wants kitchen light fixtures and mentioned budget concerns.`\n"
            "`!followup Gail Thompson — call about kitchen light fixture job`"
        )

    @bot.command(name="chisme")
    async def chisme(ctx, *, note: str = ""):
        """
        Save long-term narrative/customer/network data.
        This does NOT automatically become a follow-up.
        """
        if not note.strip():
            await ctx.send("Tell me chisme like: `!chisme Gail wants light fixtures and mentioned budget concerns.`")
            return

        await ctx.send("…thinking…")

        try:
            resp = client.responses.create(
                model="gpt-5-mini",
                reasoning={"effort": "low"},
                input=[
                    {
                        "role": "system",
                        "content": (
                            "You are Chismebot. Turn messy customer/network notes into a clean narrative data card.\n"
                            "Do NOT turn this into a to-do list unless the user explicitly says it is a follow-up.\n\n"
                            "Use these fields:\n"
                            "Name\n"
                            "Org / Relationship\n"
                            "Contact Info Mentioned\n"
                            "Context\n"
                            "What They Need / Want\n"
                            "Relevant Details\n"
                            "Potential Opportunity\n"
                            "Tags\n"
                            "Original Meaning / Why It Matters\n\n"
                            "Keep it grounded in the user's note. Do not invent facts."
                        ),
                    },
                    {"role": "user", "content": note},
                ],
                max_output_tokens=800,
            )

            text = safe_text_from_openai_response(resp)

            items = load_chisme()
            entry = {
                "timestamp": now_iso(),
                "raw_note": note,
                "narrative_card": text,
                "type": "chisme_note"
            }
            items.append(entry)
            save_chisme(items)

            await send_long(ctx, text)
            await ctx.send("✅ Saved to Chismebot narrative database.")

        except Exception:
            print("=== FULL ERROR TRACEBACK ===")
            traceback.print_exc()
            print("=== END TRACEBACK ===")
            await ctx.send("⚠️ Error. Check the terminal traceback.")

    @bot.command(name="chismelist")
    async def chismelist(ctx):
        """
        Show recent narrative notes. This is not the Command Center follow-up list.
        """
        items = load_chisme()

        if not items:
            await ctx.send("No Chismebot notes saved yet.")
            return

        lines = ["💬 Recent Chismebot narrative notes:\n"]

        for idx, item in enumerate(items[-10:], start=1):
            card = item.get("narrative_card") or item.get("raw_note") or "No details"
            lines.append(f"{idx}. {short_text(card)}")

        await send_long(ctx, "\n".join(lines))

    @bot.command(name="cqueue")
    async def cqueue(ctx, target: int = 20):
        """
        Build today's Chismebot customer communication queue.
        """
        items = create_chisme_daily_queue(target_count=target, assigned_to=str(ctx.author))

        queue = get_today_queue()
        total = len(queue)
        done = len([x for x in queue if x.get("status") == "done"])

        buckets = {}
        for item in queue:
            bucket = item.get("bucket") or "unknown"
            buckets[bucket] = buckets.get(bucket, 0) + 1

        bucket_lines = "\n".join([f"- {k}: {v}" for k, v in buckets.items()]) or "No calls due."

        await ctx.send(
            f"📞 Chisme communication queue ready.\n\n"
            f"Today: {done} / {total} complete\n\n"
            f"{bucket_lines}\n\n"
            f"Use `!cnext` to pull the next customer."
        )


    @bot.command(name="cnext")
    async def cnext(ctx):
        """
        Show the next queued customer communication.
        """
        item = get_next_queued_item()

        if not item:
            await ctx.send("No queued customer communication left for today.")
            return

        contact = item.get("chisme_contacts") or {}

        await ctx.send(
            f"📞 Next customer communication\n\n"
            f"**{contact.get('name', 'Unknown')}**\n"
            f"Bucket: {item.get('bucket')}\n"
            f"Reason: {item.get('reason')}\n\n"
            f"Last job date: {contact.get('last_job_date') or 'unknown'}\n"
            f"Job count: {contact.get('job_count') or 0}\n"
            f"Chisme: {contact.get('chisme_summary') or 'No chisme yet.'}\n\n"
            f"Script:\n{item.get('script_hint')}\n\n"
            f"When done, use:\n"
            f"`!cdone <what happened>`"
        )


    @bot.command(name="cdone")
    async def cdone(ctx, *, notes: str = ""):
        """
        Mark the current next queued communication as done.
        """
        item = get_next_queued_item()

        if not item:
            await ctx.send("No queued customer communication left for today.")
            return

        contact = item.get("chisme_contacts") or {}
        contact_id = item.get("contact_id")
        contact_name = contact.get("name", "Unknown")

        notes = notes.strip() or "completed"

        supabase.table("chisme_communication_queue").update({
            "status": "done",
            "completed_at": now_iso(),
            "outcome": notes,
            "notes": notes,
        }).eq("id", item["id"]).execute()

        supabase.table("chisme_interactions").insert({
            "contact_id": contact_id,
            "interaction_type": "call",
            "notes": notes,
            "outcome": notes,
            "created_by": str(ctx.author),
        }).execute()

        # Default next contact rhythm for now.
        next_date = (date.today() + timedelta(days=60)).isoformat()

        supabase.table("chisme_contacts").update({
            "last_contact_date": today_date(),
            "next_contact_date": next_date,
            "last_outcome": notes,
        }).eq("id", contact_id).execute()

        queue = get_today_queue()
        total = len(queue)
        done = len([x for x in queue if x.get("status") == "done"])

        await ctx.send(
            f"✅ Completed: {contact_name}\n"
            f"Outcome: {notes}\n\n"
            f"Progress: {done} / {total}"
        )
        
    @bot.command(name="followup")
    async def followup(ctx, *, note: str = ""):
        """
        Save short-term operational follow-up item.
        This is what feeds the Command Center customer follow-up panel.
        """
        if not note.strip():
            await ctx.send("Add a follow-up like: `!followup Gail Thompson — call about light fixtures`")
            return

        # Keep this intentionally simple and operational.
        # No AI needed unless we decide later.
        if "—" in note:
            name, reason = note.split("—", 1)
        elif "-" in note:
            name, reason = note.split("-", 1)
        else:
            name = note
            reason = "follow up"

        item = {
            "user_id": str(ctx.author.id),
            "channel_id": str(ctx.channel.id),
            "name": name.strip(),
            "reason": reason.strip(),
            "raw_note": note.strip(),
            "status": "open"
        }
        
        add_followup(item)
        
        items = load_followups()
        push_followups_to_dashboard(items)
        
        await ctx.send(f"✅ Added follow-up: {item['name']} — {item['reason']}")

    @bot.command(name="followuplist")
    async def followuplist(ctx):
        """
        Show active follow-ups. This should match the Command Center customer follow-up panel.
        """
        items = load_followups()
        open_items = [item for item in items if item.get("status") != "done"]

        if not open_items:
            await ctx.send("No open follow-ups.")
            return

        lines = ["📋 Active customer follow-ups:\n"]

        for idx, item in enumerate(open_items[-10:], start=1):
            name = item.get("name", "Unknown")
            reason = item.get("reason", "follow up")
            lines.append(f"{idx}. {name} — {reason}")

        await send_long(ctx, "\n".join(lines))

    @bot.command(name="followupdone")
    async def followupdone(ctx, *, query: str = ""):
        """
        Mark a follow-up done by number or name.
        Example:
        !followupdone 1
        !followupdone Gail
        """
        if not query.strip():
            await ctx.send("Mark done like: `!followupdone 1` or `!followupdone Gail`")
            return

        items = load_followups()
        idx = find_followup_index(items, query)

        if idx is None:
            await ctx.send("I couldn’t find an open follow-up matching that.")
            return

        update_followup(
            items[idx]["id"],
            {
                "status": "done",
                "completed_at": now_iso()
            }
        )
        
        items = load_followups()
        push_followups_to_dashboard(items)
        
        name = items[idx].get("name", "Follow-up")
        reason = items[idx].get("reason", "")
        await ctx.send(f"✅ Marked done: {name} — {reason}")

    # Alias, because earlier help mentioned chismedone.
    @bot.command(name="chismedone")
    async def chismedone(ctx, *, query: str = ""):
        await followupdone(ctx, query=query)
