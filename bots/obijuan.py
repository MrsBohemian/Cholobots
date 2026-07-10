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

async def find_wtp_category(text: str):
    if not require_supabase():
        return None

    response = (
        supabase.table("wtp_category_keyword_rules")
        .select("*")
        .execute()
    )

    text = text.lower()
    scores = {}

    for row in response.data:
        keyword = row["keyword"].lower()

        if keyword in text:
            category = row["category"]
            priority = row.get("priority", 1)

            scores[category] = scores.get(category, 0) + priority

    if not scores:
        return None

    best_category = max(scores, key=scores.get)

    return {
        "category": best_category,
        "score": scores[best_category]
    }

async def get_wtp_pricing(category: str):
    if not require_supabase():
        return None

    response = (
        supabase.table("wtp_category_pricing_rules")
        .select("*")
        .eq("category", category)
        .limit(1)
        .execute()
    )

    rows = response.data or []

    return rows[0] if rows else None

async def build_wtp_summary(text: str):
    match = await find_wtp_category(text)

    if not match:
        return {
            "summary": "No historical pricing category found.",
            "category": None
        }

    pricing = await get_wtp_pricing(match["category"])

    if not pricing:
        return {
            "summary": "Pricing category found but no pricing data available.",
            "category": match["category"]
        }

    summary = (
        f"CRUDOBOT PRICING INTELLIGENCE\n\n"
        f"Category: {pricing['category']}\n"
        f"Sample Size: {pricing['sample_size']} jobs\n"
        f"Confidence Score: {match['score']}\n\n"
        f"Median Price: ${pricing['median_amount']:.2f}\n"
        f"Recommended Starting Quote: ${pricing['recommended_starting_price']:.2f}\n"
        f"Premium Range: ${pricing['recommended_premium_anchor']:.2f}"
    )

    return {
        "summary": summary,
        "category": pricing["category"]
    }
    
async def get_risk_factors(category: Optional[str]):
    """
    Placeholder for future Crudobot risk intelligence.

    Later this should query a Supabase table populated from completed job outcomes,
    Metichebot time/activity data, and Guardabot material data.
    """
    if not category:
        return []

    return []


# -----------------------------------------------------------------------------
# Persistent quest storage
# Supabase is the source of truth. In-memory collections remain as a fallback
# if Supabase is temporarily unavailable.
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

    if require_supabase():
        rows = (
            supabase.table("obijuan_quests")
            .upsert(data, on_conflict="quest_id")
            .execute()
        ).data or []
        if rows:
            return rows[0]

    return data


async def get_quest(quest_id: str) -> Optional[Dict[str, Any]]:
    quest_id = slugify(quest_id)

    if require_supabase():
        rows = (
            supabase.table("obijuan_quests")
            .select("*")
            .eq("quest_id", quest_id)
            .limit(1)
            .execute()
        ).data or []
        if rows:
            QUESTS[quest_id] = rows[0]
            return rows[0]

    return QUESTS.get(quest_id)


async def update_quest(quest_id: str, **fields: Any) -> Optional[Dict[str, Any]]:
    quest_id = slugify(quest_id)
    fields["updated_at"] = now_iso()

    if require_supabase():
        rows = (
            supabase.table("obijuan_quests")
            .update(fields)
            .eq("quest_id", quest_id)
            .execute()
        ).data or []
        if rows:
            QUESTS[quest_id] = rows[0]
            return rows[0]

    quest = QUESTS.get(quest_id)
    if not quest:
        return None
    quest.update(fields)
    QUESTS[quest_id] = quest
    return quest


async def add_note(quest_id: str, author: discord.User | discord.Member, note_type: str, body: str) -> Dict[str, Any]:
    note = {
        "quest_id": slugify(quest_id),
        "author_id": str(author.id),
        "author_name": author.display_name,
        "note_type": note_type,
        "body": body,
        "created_at": now_iso(),
    }

    if require_supabase():
        rows = supabase.table("obijuan_quest_notes").insert(note).execute().data or []
        if rows:
            return rows[0]

    QUEST_NOTES.append(note)
    return note


async def add_update(quest_id: str, author: discord.User | discord.Member, body: str) -> Dict[str, Any]:
    update = {
        "quest_id": slugify(quest_id),
        "author_id": str(author.id),
        "author_name": author.display_name,
        "body": body,
        "created_at": now_iso(),
    }

    if require_supabase():
        rows = supabase.table("obijuan_quest_updates").insert(update).execute().data or []
        if rows:
            return rows[0]

    QUEST_UPDATES.append(update)
    return update


async def get_notes_for_quest(quest_id: str) -> List[Dict[str, Any]]:
    quest_id = slugify(quest_id)

    if require_supabase():
        return (
            supabase.table("obijuan_quest_notes")
            .select("*")
            .eq("quest_id", quest_id)
            .order("created_at")
            .execute()
        ).data or []

    return [note for note in QUEST_NOTES if note["quest_id"] == quest_id]


async def get_updates_for_quest(quest_id: str) -> List[Dict[str, Any]]:
    quest_id = slugify(quest_id)

    if require_supabase():
        return (
            supabase.table("obijuan_quest_updates")
            .select("*")
            .eq("quest_id", quest_id)
            .order("created_at")
            .execute()
        ).data or []

    return [update for update in QUEST_UPDATES if update["quest_id"] == quest_id]


async def save_quest_assignment(
    quest_id: str,
    worker_id: int,
    worker_name: str,
    accepted_price: float,
    notes: str = "",
) -> Dict[str, Any]:
    payload = {
        "quest_id": slugify(quest_id),
        "worker_id": str(worker_id),
        "worker_name": worker_name,
        "accepted_price": float(accepted_price or 0),
        "status": "accepted",
        "notes": notes,
        "accepted_at": now_iso(),
        "updated_at": now_iso(),
    }

    if not require_supabase():
        return {"ok": False, "reason": "Supabase not configured"}

    rows = (
        supabase.table("obijuan_quest_assignments")
        .upsert(payload, on_conflict="quest_id,worker_id")
        .execute()
    ).data or []

    return {"ok": True, "data": rows[0] if rows else payload}


async def update_quest_payout(
    quest_id: str,
    worker_name: str,
    paid_amount: float,
    notes: str = "",
) -> Dict[str, Any]:
    if not require_supabase():
        return {"ok": False, "reason": "Supabase not configured"}

    matches = (
        supabase.table("obijuan_quest_assignments")
        .select("*")
        .eq("quest_id", slugify(quest_id))
        .ilike("worker_name", f"%{worker_name}%")
        .limit(5)
        .execute()
    ).data or []

    if not matches:
        return {"ok": False, "reason": "No matching accepted quest assignment"}

    assignment = matches[0]
    rows = (
        supabase.table("obijuan_quest_assignments")
        .update({
            "paid_amount": float(paid_amount or 0),
            "status": "paid",
            "payout_notes": notes,
            "paid_at": now_iso(),
            "updated_at": now_iso(),
        })
        .eq("id", assignment["id"])
        .execute()
    ).data or []

    return {"ok": True, "data": rows[0] if rows else assignment}


async def save_owner_time(
    quest_id: str,
    owner_name: str,
    quantity: float,
    unit: str,
    notes: str,
    created_by: str,
) -> Dict[str, Any]:
    if not require_supabase():
        return {"ok": False, "reason": "Supabase not configured"}

    payload = {
        "quest_id": slugify(quest_id),
        "owner_name": owner_name,
        "quantity": float(quantity or 0),
        "unit": unit,
        "notes": notes,
        "created_by": created_by,
        "created_at": now_iso(),
    }

    rows = supabase.table("obijuan_owner_time").insert(payload).execute().data or []
    return {"ok": True, "data": rows[0] if rows else payload}


async def get_quest_labor_summary(quest_id: str) -> Dict[str, Any]:
    quest_id = slugify(quest_id)

    if not require_supabase():
        return {"assignments": [], "owner_time": []}

    assignments = (
        supabase.table("obijuan_quest_assignments")
        .select("*")
        .eq("quest_id", quest_id)
        .order("accepted_at")
        .execute()
    ).data or []

    owner_time = (
        supabase.table("obijuan_owner_time")
        .select("*")
        .eq("quest_id", quest_id)
        .order("created_at")
        .execute()
    ).data or []

    return {"assignments": assignments, "owner_time": owner_time}


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

    note_lines = "\n".join(
        f"- [{n['note_type']}] {n['body']}"
        for n in notes[-12:]
    ) or "- No notes yet."

    analysis_text = " ".join([
        str(quest.get("title") or ""),
        str(quest.get("job_summary") or ""),
        str(note_lines or "")
    ])

    wtp_summary = await build_wtp_summary(analysis_text)

    return (
        f"{wtp_summary}\n\n"
        f"Estimate prep for {quest['title']}\n\n"
        f"Customer: {quest['customer_name']}\n"
        f"Location: {quest.get('location') or 'Not recorded yet'}\n"
        f"Customer budget: {quest.get('customer_budget') or 'Not recorded yet'}\n"
        f"Willingness to pay: {quest.get('customer_willingness') or 'Not recorded yet'}\n\n"
        f"Recent notes:\n"
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
        
    @commands.command(name="obijuan")
    async def obijuan(self, ctx: commands.Context):
        """
        Show ObiJuan Ryobi command menu.
        """
        await ctx.send(
            "🧭 **ObiJuan Ryobi Commands**\n\n"
            "**Quest Flow**\n"
            "`!oquestcreate <quest_id> \"Customer Name\" <title>` — create a quest\n"
            "`!oaccept <quest_id> <price>` — accept a quest at your own bid price\n"
            "`!opayout <quest_id> <worker> <amount> [notes]` — record final subcontract payout\n"
            "`!oownertime <quest_id> <days> [notes]` — save owner time without costing it\n"
            "`!olabor <quest_id>` — show accepted quest prices, payouts, and owner time\n"
            "`!oquestinfo <quest_id>` — show quest status\n"
            "`!osetquest <quest_id> <field> <value>` — update location, budget, willingness, summary, or status\n\n"
            "**Site Visit / Notes**\n"
            "`!ositevisit <quest_id>` — start site visit prompts\n"
            "`!onote <quest_id> <type> <note>` — add structured quest note\n"
            "`!omaterials <quest_id> <materials note>` — log material notes / Guardabot placeholder\n"
            "`!oupdate <quest_id> <update>` — add field progress update\n\n"
            "**Estimates**\n"
            "`!oestimate <quest_id>` — generate estimate prep using Crudobot WTP pricing intelligence\n\n"
            "**Timecards**\n"
            "`!oclockin <customer> <project>` — clock in\n"
            "`!oclockout` — clock out\n\n"
            "**Closeout**\n"
            "`!questdone <quest_id>` — mark work complete pending invoice\n"
            "`!opaid<quest_id>` — mark quest paid and closed"
        )

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
        !oquestcreate kellie-mays "Kellie Mays" Deck stabilization and estimate visit
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
            f"Use `!oaccept {quest_id}` to claim it."
        )

    @commands.command(name="accept", aliases=["oaccept"])
    async def accept(self, ctx: commands.Context, quest_id: str, accepted_price: float):
        """
        Accept a quest at a fixed bid price.

        Example:
        !oaccept michael-lawrence 4900
        """
        quest = await get_quest(quest_id)
        if not quest:
            await ctx.send(f"No encuentro that quest, homie: `{quest_id}`")
            return

        result = await save_quest_assignment(
            quest_id=quest_id,
            worker_id=ctx.author.id,
            worker_name=ctx.author.display_name,
            accepted_price=accepted_price,
        )

        if not result.get("ok"):
            await ctx.send(f"Could not save quest price: {result.get('reason')}")
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
            f"**Accepted price:** ${accepted_price:,.2f}\n"
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
        !osetquest kellie-mays location 123 Main St
        !osetquest kellie-mays customer_budget around 1200 if safety issue
        !osetquest kellie-mays customer_willingness willing to phase work
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
        This does not collect answers automatically yet; use !onote after each observation.
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
            "Answer these as you walk the job. Use `!onote kellie-mays <type> <note>` when useful.\n\n"
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
        !onote kellie-mays budget customer said she can phase work if needed
        !onote kellie-mays materials likely needs joist hangers and exterior screws
        !onote kellie-mays risk hidden rot under deck boards
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
        !oupdate kellie-mays demo complete, found rot on two joists, sending photos
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
        !omaterials kellie-mays 2x6 joists, joist hangers, deck screws, exterior stain
        """
        quest = await get_quest(quest_id)
        if not quest:
            await ctx.send(f"No quest found for `{quest_id}`.")
            return

        if body:
            await add_note(quest_id, ctx.author, "materials", body)

        material_memory = await guardabot_material_memory(quest_id)
        await ctx.send(f"🛡️ **Guardabot check for `{slugify(quest_id)}`**\n{material_memory}")

    @commands.command(name="oestimate")
    async def oestimate(self, ctx: commands.Context, *, job_description: str):
        """
        Generate estimate prep from a plain job description.
        Example:
        !oestimate customer supplied pergola kit installation in backyard
        """
        wtp_result = await build_wtp_summary(job_description)
        category = wtp_result.get("category")
        risk_factors = await get_risk_factors(category)
    
        if risk_factors:
            risk_text = "\n".join(f"- {risk}" for risk in risk_factors)
        else:
            risk_text = "Insufficient risk data collected."
    
        await ctx.send(
            f"🧾 **ObiJuan Estimate Prep**\n"
            f"```text\n"
            f"{wtp_result['summary']}\n\n"
            f"Risk Factors:\n"
            f"{risk_text}\n\n"
            f"Job Description:\n"
            f"{job_description}"
            f"```"
        )

    @commands.command(name="payout", aliases=["opayout"])
    async def payout(
        self,
        ctx: commands.Context,
        quest_id: str,
        worker_name: str,
        amount: float,
        *,
        notes: str = "",
    ):
        """Record the final paid amount for an accepted quest."""
        quest = await get_quest(quest_id)
        if not quest:
            await ctx.send(f"No quest found for `{quest_id}`.")
            return

        result = await update_quest_payout(
            quest_id=quest_id,
            worker_name=worker_name,
            paid_amount=amount,
            notes=notes,
        )

        if not result.get("ok"):
            await ctx.send(f"Could not save payout: {result.get('reason')}")
            return

        await ctx.send(
            f"💸 Payout saved for `{slugify(quest_id)}`\n"
            f"Worker: {worker_name}\n"
            f"Paid: ${amount:,.2f}"
        )

    @commands.command(name="ownertime", aliases=["oownertime"])
    async def ownertime(
        self,
        ctx: commands.Context,
        quest_id: str,
        days: float,
        *,
        notes: str = "",
    ):
        """Save owner time as history only; Crudobot should not cost it yet."""
        quest = await get_quest(quest_id)
        if not quest:
            await ctx.send(f"No quest found for `{quest_id}`.")
            return

        result = await save_owner_time(
            quest_id=quest_id,
            owner_name=ctx.author.display_name,
            quantity=days,
            unit="days",
            notes=notes,
            created_by=str(ctx.author),
        )

        if not result.get("ok"):
            await ctx.send(f"Could not save owner time: {result.get('reason')}")
            return

        await ctx.send(
            f"🕒 Owner time saved for `{slugify(quest_id)}`: {days:g} day(s).\n"
            "Stored for future estimating; excluded from current job-cost calculations."
        )

    @commands.command(name="labor", aliases=["olabor"])
    async def labor(self, ctx: commands.Context, quest_id: str):
        """Show labor records in a Crudobot-readable format."""
        quest = await get_quest(quest_id)
        if not quest:
            await ctx.send(f"No quest found for `{quest_id}`.")
            return

        summary = await get_quest_labor_summary(quest_id)
        assignments = summary["assignments"]
        owner_time = summary["owner_time"]

        lines = [f"🧭 **Labor — `{slugify(quest_id)}`**", ""]

        if assignments:
            lines.append("**Accepted quests / subcontract labor**")
            for row in assignments:
                accepted = float(row.get("accepted_price") or 0)
                paid = row.get("paid_amount")
                paid_label = f"${float(paid):,.2f}" if paid is not None else "not recorded"
                lines.append(
                    f"- {row.get('worker_name')}: accepted ${accepted:,.2f}; paid {paid_label}; "
                    f"status {row.get('status') or 'accepted'}"
                )
        else:
            lines.append("No accepted quest prices saved.")

        lines.append("")
        if owner_time:
            lines.append("**Owner time — stored, not costed**")
            for row in owner_time:
                lines.append(
                    f"- {row.get('owner_name')}: {float(row.get('quantity') or 0):g} "
                    f"{row.get('unit') or 'units'}"
                    + (f" — {row.get('notes')}" if row.get("notes") else "")
                )
        else:
            lines.append("No owner time saved.")

        await ctx.send("\n".join(lines))

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
