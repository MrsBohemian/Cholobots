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

    @commands.command(name="necessito")
    async def necessito(self, ctx, *, description: str = None):
        """
        Example:
        !necessito corrugated metal roofing for bus stop project
        """
        if not description:
            await ctx.send(
                "¿Qué necesitas? Example:\n"
                "`!necessito corrugated metal roofing for bus stop project`"
            )
            return

        record = {
            "discord_user_id": str(ctx.author.id),
            "discord_user_name": str(ctx.author),
            "description": description,
            "status": "open",
            "entry_type": "need",
            "current_location": None,
            "next_destination": None,
            "project": None,
        }

        supabase.table("vuelta_inventory").insert(record).execute()

        await ctx.send(
            f"📌 Need captured.\n\n"
            f"**Necessito:** {description}\n"
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
