import os
import re

import discord
from discord.ext import commands
from supabase import create_client, Client


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = (
    os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    or os.getenv("SUPABASE_KEY")
)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# =========================================================
# MATCHING HELPERS
# =========================================================

STOP_WORDS = {
    "a", "an", "the", "and", "or", "for", "from", "with",
    "to", "of", "in", "on", "my", "i", "im", "i'm",
    "looking", "need", "want", "some", "something",
    "used", "new"
}


def normalize_words(text):
    if not text:
        return set()

    words = re.findall(r"[a-z0-9]+", text.lower())

    return {
        word
        for word in words
        if len(word) > 2 and word not in STOP_WORDS
    }


def get_item_words(item):
    combined = " ".join([
        item.get("item_name") or "",
        item.get("description") or "",
        item.get("category") or "",
        item.get("subcategory") or "",
        item.get("brand") or "",
    ])

    return normalize_words(combined)


def get_interest_words(interest):
    combined = " ".join([
        interest.get("description") or "",
        interest.get("category") or "",
        interest.get("subcategory") or "",
        interest.get("match_keywords") or "",
    ])

    return normalize_words(combined)


def is_match(item, interest):
    item_words = get_item_words(item)
    interest_words = get_interest_words(interest)

    if not item_words or not interest_words:
        return False

    return bool(item_words.intersection(interest_words))


def extract_price(description):
    if not description:
        return None

    match = re.search(
        r"\$\s*(\d+(?:\.\d{1,2})?)",
        description
    )

    if not match:
        return None

    try:
        return float(match.group(1))
    except ValueError:
        return None


def clean_item_name(description):
    if not description:
        return description

    cleaned = re.sub(
        r"\$\s*\d+(?:\.\d{1,2})?",
        "",
        description
    )

    return " ".join(cleaned.split()).strip()


def get_first_image_url(ctx):
    """
    Grab the first attached Discord image, if one exists.
    """
    if not ctx.message.attachments:
        return None

    for attachment in ctx.message.attachments:
        content_type = attachment.content_type or ""

        if content_type.startswith("image/"):
            return attachment.url

    return None


# =========================================================
# VUELTABOT
# =========================================================

class VueltaBot(commands.Cog):

    def __init__(self, bot):
        self.bot = bot


    # =====================================================
    # !TENGO
    # =====================================================

    @commands.command(name="tengo")
    async def tengo(self, ctx, *, description: str = None):
        """
        Examples:

        !tengo stainless steel pots and pans $25
        + attach photo
        """

        if not description:
            await ctx.send(
                "♻️ **¿Qué tienes?**\n\n"
                "Try:\n"
                "`!tengo stainless steel pots and pans $25`\n\n"
                "Attach a photo if you have one."
            )
            return

        seller_id = str(ctx.author.id)
        seller_name = str(ctx.author)

        asking_price = extract_price(description)
        item_name = clean_item_name(description)
        photo_url = get_first_image_url(ctx)

        item_record = {
            "seller_discord_id": seller_id,
            "seller_discord_name": seller_name,
            "owner_name": seller_name,

            "item_name": item_name,
            "description": description,

            "asking_price": asking_price,
            "photo_url": photo_url,

            "desired_outcome": "local_transfer",
            "status": "available",

            "source_system": "circular_sa_discord"
        }

        item_result = (
            supabase
            .table("vuelta_items")
            .insert(item_record)
            .execute()
        )

        if not item_result.data:
            await ctx.send(
                "⚠️ I couldn't create the item."
            )
            return

        item = item_result.data[0]
        item_id = item["id"]

        # Create Circular SA local marketplace route
        route_record = {
            "item_id": item_id,
            "route_type": "local_marketplace",
            "platform_name": "Circular SA Discord",
            "status": "listed",
            "listed_price": asking_price,
            "source_system": "circular_sa_discord"
        }

        supabase.table(
            "vuelta_routes"
        ).insert(
            route_record
        ).execute()

        # Search persistent interests
        interest_result = (
            supabase
            .table("vuelta_interests")
            .select("*")
            .eq("status", "active")
            .eq("alert_enabled", True)
            .execute()
        )

        matched_interests = []

        for interest in interest_result.data or []:

            if interest.get("discord_user_id") == seller_id:
                continue

            if is_match(item, interest):
                matched_interests.append(interest)

        # Build listing embed
        embed = discord.Embed(
            title=f"📦 Item #{item_id}",
            description=item_name
        )

        if asking_price is not None:
            embed.add_field(
                name="Price",
                value=f"${asking_price:.2f}",
                inline=True
            )
        else:
            embed.add_field(
                name="Price",
                value="Not specified",
                inline=True
            )

        embed.add_field(
            name="Status",
            value="Available",
            inline=True
        )

        embed.add_field(
            name="Marketplace",
            value="Circular SA Discord",
            inline=False
        )

        embed.set_footer(
            text=f"Seller: {seller_name}"
        )

        if photo_url:
            embed.set_image(url=photo_url)

        await ctx.send(embed=embed)

        # Alert matched buyers
        if matched_interests:

            mentions = []
            shown_users = set()

            for interest in matched_interests[:5]:

                user_id = interest.get("discord_user_id")

                if not user_id or user_id in shown_users:
                    continue

                shown_users.add(user_id)
                mentions.append(
                    f"<@{user_id}> — you were looking for "
                    f"**{interest.get('description')}**"
                )

            if mentions:
                await ctx.send(
                    "🎯 **Possible matches:**\n"
                    + "\n".join(mentions),
                    allowed_mentions=discord.AllowedMentions(
                        users=True,
                        everyone=False,
                        roles=False
                    )
                )


    # =====================================================
    # !BUSCO
    # =====================================================

    @commands.command(name="busco")
    async def busco(self, ctx, *, description: str = None):
        """
        Examples:

        !busco pots and pans
        !busco dining table
        """

        if not description:
            await ctx.send(
                "🔎 **¿Qué buscas?**\n\n"
                "Try:\n"
                "`!busco pots and pans`"
            )
            return

        user_id = str(ctx.author.id)
        user_name = str(ctx.author)

        keywords = sorted(
            normalize_words(description)
        )

        interest_record = {
            "discord_user_id": user_id,
            "discord_user_name": user_name,

            "description": description,
            "match_keywords": " ".join(keywords),

            "alert_enabled": True,
            "status": "active"
        }

        interest_result = (
            supabase
            .table("vuelta_interests")
            .insert(interest_record)
            .execute()
        )

        interest = interest_result.data[0]

        item_result = (
            supabase
            .table("vuelta_items")
            .select("*")
            .eq("status", "available")
            .execute()
        )

        matches = []

        for item in item_result.data or []:

            if item.get("seller_discord_id") == user_id:
                continue

            if is_match(item, interest):
                matches.append(item)

        await ctx.send(
            f"🔎 Saved your search for **{description}**.\n"
            "I'll keep watching future Circular SA listings too."
        )

        if not matches:
            await ctx.send(
                "📭 Nothing currently available matched."
            )
            return

        await ctx.send(
            f"🎯 I found **{len(matches)} possible match"
            f"{'es' if len(matches) != 1 else ''}**:"
        )

        # Send one visual listing card per match
        for item in matches[:8]:

            item_id = item["id"]

            name = (
                item.get("item_name")
                or item.get("description")
            )

            embed = discord.Embed(
                title=f"📦 Item #{item_id}",
                description=name
            )

            if item.get("asking_price") is not None:
                embed.add_field(
                    name="Price",
                    value=f"${float(item['asking_price']):.2f}",
                    inline=True
                )
            else:
                embed.add_field(
                    name="Price",
                    value="Not specified",
                    inline=True
                )

            if item.get("condition"):
                embed.add_field(
                    name="Condition",
                    value=item.get("condition"),
                    inline=True
                )

            if item.get("pickup_area"):
                embed.add_field(
                    name="Pickup",
                    value=item.get("pickup_area"),
                    inline=False
                )

            seller = (
                item.get("seller_discord_name")
                or item.get("owner_name")
                or "Community member"
            )

            embed.set_footer(
                text=f"Seller: {seller} • Claim with !claim {item_id}"
            )

            if item.get("photo_url"):
                embed.set_image(
                    url=item.get("photo_url")
                )

            await ctx.send(embed=embed)


    # =====================================================
    # !VUELTA
    # =====================================================

    @commands.command(name="vuelta")
    async def vuelta(self, ctx, item_id: int = None):
        """
        Inspect an item's full journey.

        Example:
        !vuelta 57
        """

        if not item_id:
            await ctx.send(
                "Use it like this:\n"
                "`!vuelta 57`"
            )
            return

        item_result = (
            supabase
            .table("vuelta_items")
            .select("*")
            .eq("id", item_id)
            .execute()
        )

        if not item_result.data:
            await ctx.send(
                f"❌ No item found with ID #{item_id}."
            )
            return

        item = item_result.data[0]

        route_result = (
            supabase
            .table("vuelta_routes")
            .select("*")
            .eq("item_id", item_id)
            .order("created_at")
            .execute()
        )

        name = (
            item.get("item_name")
            or item.get("description")
        )

        embed = discord.Embed(
            title=f"📦 Item #{item_id}",
            description=name
        )

        embed.add_field(
            name="Status",
            value=item.get("status") or "unknown",
            inline=True
        )

        if item.get("asking_price") is not None:
            embed.add_field(
                name="Price",
                value=f"${float(item['asking_price']):.2f}",
                inline=True
            )

        if item.get("condition"):
            embed.add_field(
                name="Condition",
                value=item.get("condition"),
                inline=True
            )

        if item.get("category"):
            embed.add_field(
                name="Category",
                value=item.get("category"),
                inline=True
            )

        if item.get("pickup_area"):
            embed.add_field(
                name="Pickup",
                value=item.get("pickup_area"),
                inline=False
            )

        if item.get("owner_name"):
            embed.add_field(
                name="Owner",
                value=item.get("owner_name"),
                inline=False
            )

        if item.get("photo_url"):
            embed.set_image(
                url=item.get("photo_url")
            )

        await ctx.send(embed=embed)

        # Route history
        routes = route_result.data or []

        if not routes:
            await ctx.send(
                "🛣️ No route history recorded yet."
            )
            return

        route_text = "🛣️ **Route history**\n\n"

        for route in routes:

            platform = (
                route.get("platform_name")
                or route.get("route_type")
                or "Unknown route"
            )

            route_status = (
                route.get("status")
                or "unknown"
            )

            route_text += (
                f"• **{platform}** — {route_status}"
            )

            if route.get("listed_price") is not None:
                route_text += (
                    f" — listed "
                    f"${float(route['listed_price']):.2f}"
                )

            if route.get("sold_price") is not None:
                route_text += (
                    f" — sold "
                    f"${float(route['sold_price']):.2f}"
                )

            route_text += "\n"

        await ctx.send(route_text)


async def setup(bot):
    await bot.add_cog(VueltaBot(bot))
