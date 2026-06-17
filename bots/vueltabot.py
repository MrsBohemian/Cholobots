import os
import discord
from discord.ext import commands
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


class VueltaBot(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.command(name="tengo")
    async def tengo(self, ctx, *, description: str = None):
        """
        Example:
        !tengo sheet metal from Monterrey Iron for bus stop project
        """
        if not description:
            await ctx.send(
                "¿Qué tienes? Example:\n"
                "`!tengo 4x8 sheet metal from Monterrey Iron for bus stop project`"
            )
            return

        record = {
            "discord_user_id": str(ctx.author.id),
            "discord_user_name": str(ctx.author),
            "description": description,
            "status": "identified",
            "entry_type": "have",
            "current_location": None,
            "next_destination": None,
            "project": None,
        }

        result = supabase.table("vuelta_inventory").insert(record).execute()

        await ctx.send(
            f"✅ Vuelta captured.\n\n"
            f"**Tengo:** {description}\n"
            f"**Status:** identified"
        )

    @commands.command(name="necesito")
    async def necesito(self, ctx, *, description: str = None):
        """
        Example:
        !necesito clothing racks for clothing swap
        """
        if not description:
            await ctx.send(
                "¿Qué necesitas? Example:\n"
                "`!necesito clothing racks for clothing swap`"
            )
            return
    
        query = description.lower()
    
        # keyword buckets for prototype matching
        if "clothing" in query or "clothes" in query or "donation" in query:
            keywords = ["clothing", "clothes", "textile", "resale", "thrift", "donation"]
        elif "rack" in query or "hanger" in query:
            keywords = ["rack", "clothing rack", "hanger", "MIC"]
        elif "food" in query or "hospitality" in query or "sponsor" in query:
            keywords = ["food", "hospitality", "sponsor", "diversion", "catering"]
        else:
            keywords = query.split()
    
        matches = []
    
        for keyword in keywords:
            result = (
                supabase.table("vuelta_inventory")
                .select("*")
                .eq("entry_type", "have")
                .in_("status", ["identified", "available", "open"])
                .ilike("match_keywords", f"%{keyword}%")
                .execute()
            )
    
            if result.data:
                matches.extend(result.data)
    
        # de-dupe by id
        unique_matches = {item["id"]: item for item in matches}.values()
    
        if unique_matches:
            response = f"🔎 **Matches for:** {description}\n\n"
    
            for item in list(unique_matches)[:8]:
                response += (
                    f"**{item.get('organization_name') or 'Unknown organization'}**\n"
                    f"Item: {item.get('item_name') or item.get('description')}\n"
                    f"Category: {item.get('category') or 'uncategorized'}\n"
                    f"Delivery: {'Yes' if item.get('delivery_available') else 'Pickup / ask directly'}\n"
                )
    
                if item.get("delivery_notes"):
                    response += f"Delivery notes: {item.get('delivery_notes')}\n"
    
                if item.get("directory_url"):
                    response += f"Directory: {item.get('directory_url')}\n"
    
                response += "\n"
    
            await ctx.send(response)
            return
    
        # If no match, save the need
        record = {
            "discord_user_id": str(ctx.author.id),
            "discord_user_name": str(ctx.author),
            "description": description,
            "status": "open",
            "entry_type": "need",
            "current_location": None,
            "next_destination": None,
            "project": "clothing swap" if "clothing swap" in query else None,
            "match_keywords": query,
        }
    
        supabase.table("vuelta_inventory").insert(record).execute()
    
        await ctx.send(
            f"📌 No matches found yet. Need saved.\n\n"
            f"**Necesito:** {description}\n"
            f"**Status:** open"
        )

    @commands.command(name="route")
    async def route(self, ctx, item_id: int = None, *, destination: str = None):
        """
        Example:
        !route 12 fabricator
        """
        if not item_id or not destination:
            await ctx.send("Use it like this: `!route 12 fabricator`")
            return

        supabase.table("vuelta_inventory").update({
            "next_destination": destination,
            "status": "routed"
        }).eq("id", item_id).execute()

        await ctx.send(
            f"🛻 Item #{item_id} routed to **{destination}**."
        )

    @commands.command(name="received")
    async def received(self, ctx, item_id: int = None):
        """
        Example:
        !received 12
        """
        if not item_id:
            await ctx.send("Use it like this: `!received 12`")
            return

        supabase.table("vuelta_inventory").update({
            "status": "received"
        }).eq("id", item_id).execute()

        await ctx.send(f"✅ Item #{item_id} marked as received.")

    @commands.command(name="vuelta")
    async def vuelta(self, ctx, item_id: int = None):
        """
        Example:
        !vuelta 12
        """
        if not item_id:
            await ctx.send("Use it like this: `!vuelta 12`")
            return

        result = supabase.table("vuelta_inventory").select("*").eq("id", item_id).execute()

        if not result.data:
            await ctx.send(f"No item found with ID #{item_id}.")
            return

        item = result.data[0]

        await ctx.send(
            f"🔁 **Vuelta #{item['id']}**\n"
            f"**Type:** {item.get('entry_type')}\n"
            f"**Description:** {item.get('description')}\n"
            f"**Status:** {item.get('status')}\n"
            f"**Current location:** {item.get('current_location') or 'unknown'}\n"
            f"**Next destination:** {item.get('next_destination') or 'not assigned'}"
        )


async def setup(bot):
    await bot.add_cog(VueltaBot(bot))
