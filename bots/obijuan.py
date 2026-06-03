"""
ObiJuan.py

Quest lifecycle coordinator for the Homie Guild / Cholobots system.

ObiJuan is NOT the estimator, inventory manager, CRM, or scheduler.
He is the field guide who moves a quest through the real workflow:

Quest opportunity -> acceptance -> Chisme unlock -> site visit notes -> estimate prep -> work updates -> completion / payout tracking

Expected integration:
- Import setup_obijuan(bot) from cholobots.py or your main Discord bot file.
- This file assumes discord.py commands extension.
- Database calls are isolated behind helper functions so you can wire them to Supabase next.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

import discord
from discord.ext import commands
import os
from zoneinfo import ZoneInfo
from supabase import create_client

LOCAL_TZ = ZoneInfo("America/Chicago")

def local_now():
    return dt.datetime.now(LOCAL_TZ)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY) if SUPABASE_URL and SUPABASE_KEY else None

def require_supabase() -> bool:
    return supabase is not None

# -----------------------------------------------------------------------------
# Temporary in-memory storage
# Replace these helpers with Supabase calls once the flow feels right.
# -----------------------------------------------------------------------------

QUESTS: Dict[str, Dict[str, Any]] = {}
QUEST_NOTES: List[Dict[str, Any]] = []
QUEST_UPDATES: List[Dict[str, Any]] = []
SITE_VISITS: Dict[str, Dict[str, Any]] = {}


def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def slugify(text: str) -> str:
    return text.strip().lower().replace(" ", "-").replace("_", "-")


@dataclass
class Quest:
    quest_id: str
    customer_name: str
    title: str
    status: str = "open"
    location: Optional[str] = None
    customer_budget: Optional[str] = None
    customer_willingness: Optional[str] = None
    job_summary: Optional[str] = None
    accepted_by_user_id: Optional[int] = None
    accepted_by_name: Optional[str] = None
    created_at: str = now_iso()
    updated_at: str = now_iso()


# -----------------------------------------------------------------------------
# Database-ish helpers
# -----------------------------------------------------------------------------

async def save_quest(quest: Quest) -> Dict[str, Any]:
    data = asdict(quest)
    QUESTS[quest.quest_id] = data
    return data


async def get_quest(quest_id: str) -> Optional[Dict[str, Any]]:
    return QUESTS.get(slugify(quest_id))


async def update_quest(quest_id: str, **fields: Any) -> Optional[Dict[str, Any]]:
    quest_id = slugify(quest_id)
    quest = QUESTS.get(quest_id)
    if not quest:
        return None
    quest.update(fields)
    quest["updated_at"] = now_iso()
    QUESTS[quest_id] = quest
    return quest


async def add_note(quest_id: str, author: discord.User | discord.Member, note_type: str, body: str) -> Dict[str, Any]:
    note = {
        "quest_id": slugify(quest_id),
        "author_id": author.id,
        "author_name": author.display_name,
        "note_type": note_type,
        "body": body,
        "created_at": now_iso(),
    }
    QUEST_NOTES.append(note)
    return note


async def add_update(quest_id: str, author: discord.User | discord.Member, body: str) -> Dict[str, Any]:
    update = {
        "quest_id": slugify(quest_id),
        "author_id": author.id,
        "author_name": author.display_name,
        "body": body,
        "created_at": now_iso(),
    }
    QUEST_UPDATES.append(update)
    return update


async def get_notes_for_quest(quest_id: str) -> List[Dict[str, Any]]:
    quest_id = slugify(quest_id)
    return [note for note in QUEST_NOTES if note["quest_id"] == quest_id]


async def get_updates_for_quest(quest_id: str) -> List[Dict[str, Any]]:
    quest_id = slugify(quest_id)
    return [update for update in QUEST_UPDATES if update["quest_id"] == quest_id]


# -----------------------------------------------------------------------------
# Specialist bot placeholder functions
# Wire these to chismebot.py, crudobot.py, guardabot.py, metichebot.py later.
# -----------------------------------------------------------------------------

async def unlock_chisme(customer_name: str) -> str:
    """
    Placeholder. Later: query Chismebot / Supabase customer_chisme table.
    """
    return (
        f"Chisme unlocked for {customer_name}:\n"
        "- Add customer communication style here.\n"
        "- Add known priorities here.\n"
        "- Add budget/willingness-to-pay context here.\n"
        "- Add property/job history here."
    )


async def guardabot_material_memory(quest_id: str) -> str:
    """
    Placeholder. Later: query Guardabot inventory + historical purchase/material data.
    """
    return (
        "Guardabot material memory placeholder:\n"
        "- Similar past jobs: not connected yet.\n"
        "- Inventory on hand: not connected yet.\n"
        "- Common missing items: not connected yet."
    )


async def crudobot_estimate_brain(quest: Dict[str, Any], notes: List[Dict[str, Any]]) -> str:
    """
    Placeholder. Later: send structured site notes + material memory to Crudobot estimate logic.
    """
    note_lines = "\n".join(f"- [{n['note_type']}] {n['body']}" for n in notes[-12:]) or "- No notes yet."
    return (
        f"Estimate prep for {quest['title']}\n\n"
        f"Customer: {quest['customer_name']}\n"
        f"Location: {quest.get('location') or 'Not recorded yet'}\n"
        f"Customer budget: {quest.get('customer_budget') or 'Not recorded yet'}\n"
        f"Willingness to pay: {quest.get('customer_willingness') or 'Not recorded yet'}\n\n"
        "Recent notes:\n"
        f"{note_lines}\n\n"
        "Estimate buckets to fill before Housecall Pro:\n"
        "1. Base scope\n"
        "2. Hidden risks / contingencies\n"
        "3. Materials likely needed\n"
        "4. Labor hours\n"
        "5. Sub payout / Jesse compensation\n"
        "6. Good / better / best options if budget is uncertain"
    )

async def save_timecard_clockin(channel_id, discord_user_id, person, customer, project):
    if not require_supabase():
        return {"ok": False, "reason": "Supabase not configured"}

    response = (
        supabase.table("obijuan_timecards")
        .insert({
            "channel_id": str(channel_id),
            "discord_user_id": str(discord_user_id),
            "person": person,
            "customer": customer,
            "project": project,
            "clock_in_at": local_now().isoformat(),
            "status": "open",
        })
        .execute()
    )
    return {"ok": True, "data": response.data}


async def fetch_open_timecard(discord_user_id):
    if not require_supabase():
        return None

    response = (
        supabase.table("obijuan_timecards")
        .select("*")
        .eq("discord_user_id", str(discord_user_id))
        .eq("status", "open")
        .order("clock_in_at", desc=True)
        .limit(1)
        .execute()
    )

    rows = response.data or []
    return rows[0] if rows else None


async def close_timecard(timecard_id):
    if not require_supabase():
        return {"ok": False, "reason": "Supabase not configured"}

    response = (
        supabase.table("obijuan_timecards")
        .update({
            "clock_out_at": local_now().isoformat(),
            "status": "closed",
        })
        .eq("id", timecard_id)
        .execute()
    )
    return {"ok": True, "data": response.data}
# -----------------------------------------------------------------------------
# Discord Cog
# -----------------------------------------------------------------------------

class ObiJuan(commands.Cog):
    """Quest guide / field operations coordinator."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.command(name="oclockin")
    async def oclockin(self, ctx: commands.Context, customer: str, *, project: str = ""):
        existing = await fetch_open_timecard(ctx.author.id)
        if existing:
            await ctx.send("You already have an open timecard. Use `!oclockout` first.")
            return

        result = await save_timecard_clockin(
            channel_id=ctx.channel.id,
            discord_user_id=ctx.author.id,
            person=ctx.author.display_name,
            customer=customer,
            project=project or "general work",
        )

        if result.get("ok"):
            await ctx.send(f"🟢 Clocked in: {customer} — {project or 'general work'}")
        else:
            await ctx.send(f"Could not clock in: {result.get('reason')}")


    @commands.command(name="oclockout")
    async def oclockout(self, ctx: commands.Context):
        card = await fetch_open_timecard(ctx.author.id)

        if not card:
            await ctx.send("No open timecard found.")
            return

        result = await close_timecard(card["id"])

        if result.get("ok"):
            await ctx.send(f"🔴 Clocked out: {card.get('customer')} — {card.get('project') or 'general work'}")
        else:
            await ctx.send(f"Could not clock out: {result.get('reason')}")

    @commands.command(name="questcreate")
    async def questcreate(
        self,
        ctx: commands.Context,
        quest_id: str,
        customer_name: str,
        *,
        title: str,
    ):
        """
        Create a quest opportunity.

        Example:
        !questcreate kellie-mays "Kellie Mays" Deck stabilization and estimate visit
        """
        quest_id = slugify(quest_id)
        quest = Quest(
            quest_id=quest_id,
            customer_name=customer_name.strip('"'),
            title=title,
        )
        await save_quest(quest)

        await ctx.send(
            f"🧭 **ObiJuan posted a new quest opportunity.**\n"
            f"**Quest:** `{quest_id}`\n"
            f"**Customer:** {quest.customer_name}\n"
            f"**Title:** {quest.title}\n"
            f"**Status:** open\n\n"
            f"Use `!accept {quest_id}` to claim it."
        )

    @commands.command(name="accept")
    async def accept(self, ctx: commands.Context, quest_id: str):
        """
        Accept a quest and unlock basic customer chisme.

        Example:
        !accept kellie-mays
        """
        quest = await get_quest(quest_id)
        if not quest:
            await ctx.send(f"No encuentro that quest, homie: `{quest_id}`")
            return

        if quest.get("accepted_by_user_id"):
            await ctx.send(
                f"That quest is already accepted by **{quest.get('accepted_by_name')}**."
            )
            return

        quest = await update_quest(
            quest_id,
            status="accepted",
            accepted_by_user_id=ctx.author.id,
            accepted_by_name=ctx.author.display_name,
        )

        chisme = await unlock_chisme(quest["customer_name"])
        await ctx.send(
            f"🧭 **Quest accepted. Ándale, {ctx.author.display_name}.**\n"
            f"**Quest:** `{quest['quest_id']}`\n"
            f"**Customer:** {quest['customer_name']}\n"
            f"**Next step:** Heaven confirms scheduling with the customer.\n\n"
            f"🗣️ **Chismebot says:**\n{chisme}"
        )

    @commands.command(name="questinfo")
    async def questinfo(self, ctx: commands.Context, quest_id: str):
        """
        Show current quest status.
        """
        quest = await get_quest(quest_id)
        if not quest:
            await ctx.send(f"No quest found for `{quest_id}`.")
            return

        await ctx.send(
            f"🧭 **Quest Info**\n"
            f"**Quest:** `{quest['quest_id']}`\n"
            f"**Title:** {quest['title']}\n"
            f"**Customer:** {quest['customer_name']}\n"
            f"**Status:** {quest['status']}\n"
            f"**Accepted by:** {quest.get('accepted_by_name') or 'Nobody yet'}\n"
            f"**Location:** {quest.get('location') or 'Not recorded'}\n"
            f"**Budget:** {quest.get('customer_budget') or 'Not recorded'}\n"
            f"**Willingness:** {quest.get('customer_willingness') or 'Not recorded'}"
        )

    @commands.command(name="setquest")
    async def setquest(self, ctx: commands.Context, quest_id: str, field: str, *, value: str):
        """
        Update simple quest fields.

        Examples:
        !setquest kellie-mays location 123 Main St
        !setquest kellie-mays customer_budget around 1200 if safety issue
        !setquest kellie-mays customer_willingness willing to phase work
        """
        allowed = {"location", "customer_budget", "customer_willingness", "job_summary", "status"}
        field = field.strip().lower()
        if field not in allowed:
            await ctx.send(f"I can update: {', '.join(sorted(allowed))}")
            return

        quest = await update_quest(quest_id, **{field: value})
        if not quest:
            await ctx.send(f"No quest found for `{quest_id}`.")
            return

        await ctx.send(f"🧭 Updated `{field}` for `{slugify(quest_id)}`.")

    @commands.command(name="sitevisit")
    async def sitevisit(self, ctx: commands.Context, quest_id: str):
        """
        Start the site visit interview prompt.
        This does not collect answers automatically yet; use !note after each observation.
        """
        quest = await get_quest(quest_id)
        if not quest:
            await ctx.send(f"No quest found for `{quest_id}`.")
            return

        SITE_VISITS[slugify(quest_id)] = {
            "quest_id": slugify(quest_id),
            "started_by": ctx.author.display_name,
            "started_at": now_iso(),
            "status": "in_progress",
        }

        await ctx.send(
            f"🧭 **ObiJuan Site Visit Mode: `{slugify(quest_id)}`**\n\n"
            "Answer these as you walk the job. Use `!note kellie-mays <type> <note>` when useful.\n\n"
            "**Customer / Budget**\n"
            "1. What did the customer say they want done?\n"
            "2. What did they say about budget or willingness to pay?\n"
            "3. Is this safety-critical, cosmetic, comfort, resale, or chaos-control?\n\n"
            "**Site Conditions**\n"
            "4. What is visibly broken?\n"
            "5. What might be causing it?\n"
            "6. What hidden risks could change the price?\n"
            "7. What measurements/photos are needed?\n\n"
            "**Materials / Labor**\n"
            "8. What materials are definitely needed?\n"
            "9. What materials might be needed?\n"
            "10. Who is doing the labor and what is the likely payout?\n\n"
            "**Gut Check**\n"
            "11. What does your contractor intuition say?\n"
            "12. What similar job does this remind you of?"
        )

    @commands.command(name="note")
    async def note(self, ctx: commands.Context, quest_id: str, note_type: str, *, body: str):
        """
        Add a structured note to a quest.

        Examples:
        !note kellie-mays budget customer said she can phase work if needed
        !note kellie-mays materials likely needs joist hangers and exterior screws
        !note kellie-mays risk hidden rot under deck boards
        """
        quest = await get_quest(quest_id)
        if not quest:
            await ctx.send(f"No quest found for `{quest_id}`.")
            return

        await add_note(quest_id, ctx.author, note_type, body)
        await ctx.send(f"📝 Note added to `{slugify(quest_id)}` as `{note_type}`.")

    @commands.command(name="update")
    async def update(self, ctx: commands.Context, quest_id: str, *, body: str):
        """
        Add a progress update from the field or subcontractor.

        Example:
        !update kellie-mays demo complete, found rot on two joists, sending photos
        """
        quest = await get_quest(quest_id)
        if not quest:
            await ctx.send(f"No quest found for `{quest_id}`.")
            return

        await add_update(quest_id, ctx.author, body)
        await ctx.send(f"📍 Progress update logged for `{slugify(quest_id)}`.")

    @commands.command(name="materials")
    async def materials(self, ctx: commands.Context, quest_id: str, *, body: Optional[str] = None):
        """
        Add material notes and ask Guardabot for material memory placeholder.

        Example:
        !materials kellie-mays 2x6 joists, joist hangers, deck screws, exterior stain
        """
        quest = await get_quest(quest_id)
        if not quest:
            await ctx.send(f"No quest found for `{quest_id}`.")
            return

        if body:
            await add_note(quest_id, ctx.author, "materials", body)

        material_memory = await guardabot_material_memory(quest_id)
        await ctx.send(f"🛡️ **Guardabot check for `{slugify(quest_id)}`**\n{material_memory}")

    @commands.command(name="estimate")
    async def estimate(self, ctx: commands.Context, quest_id: str):
        """
        Generate estimate prep notes for Housecall Pro.
        """
        quest = await get_quest(quest_id)
        if not quest:
            await ctx.send(f"No quest found for `{quest_id}`.")
            return

        notes = await get_notes_for_quest(quest_id)
        estimate_prep = await crudobot_estimate_brain(quest, notes)
        await ctx.send(f"🧾 **Crudobot estimate prep**\n```text\n{estimate_prep}\n```")

    @commands.command(name="questdone")
    async def questdone(self, ctx: commands.Context, quest_id: str):
        """
        Mark quest work complete, pending invoice/payment.
        """
        quest = await update_quest(quest_id, status="work_complete_pending_invoice")
        if not quest:
            await ctx.send(f"No quest found for `{quest_id}`.")
            return

        await ctx.send(
            f"✅ **Quest marked work complete:** `{slugify(quest_id)}`\n"
            "Next steps: invoice customer, confirm customer payment, then confirm sub payout."
        )

    @commands.command(name="paid")
    async def paid(self, ctx: commands.Context, quest_id: str):
        """
        Mark quest paid/closed.
        """
        quest = await update_quest(quest_id, status="paid_closed")
        if not quest:
            await ctx.send(f"No quest found for `{quest_id}`.")
            return

        await ctx.send(f"💸 **Quest paid and closed:** `{slugify(quest_id)}`")


async def setup_obijuan(bot: commands.Bot):
    await bot.add_cog(ObiJuan(bot))
