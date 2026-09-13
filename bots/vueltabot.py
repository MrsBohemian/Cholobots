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
    """
    Convert text into a basic set of useful matching words.

    Example:
    "Looking for pots and pans"
    becomes:
    {"pots", "pans"}
    """
    if not text:
        return set()

    words = re.findall(r"[a-z0-9]+", text.lower())

    return {
        word
        for word in words
        if len(word) > 2 and word not in STOP_WORDS
    }


def get_item_words(item):
    """
    Build searchable words from an item record.
    """
    combined = " ".join([
        item.get("item_name") or "",
        item.get("description") or "",
        item.get("category") or "",
        item.get("subcategory") or "",
        item.get("brand") or "",
    ])

    return normalize_words(combined)


def get_interest_words(interest):
    """
    Build searchable words from a !busco record.
    """
    combined = " ".join([
        interest.get("description") or "",
        interest.get("category") or "",
        interest.get("subcategory") or "",
        interest.get("match_keywords") or "",
    ])

    return normalize_words(combined)


def is_match(item, interest):
    """
    Prototype matching engine.

    For now, if the item and the interest share at least
    one meaningful word, consider them a possible match.

    Later this becomes semantic/AI matching.
    """
    item_words = get_item_words(item)
    interest_words = get_interest_words(interest)

    if not item_words or not interest_words:
        return False

    return bool(item_words.intersection(interest_words))


def extract_price(description):
    """
    Pull a simple dollar amount from descriptions like:

    !tengo dining table $30

    Returns:
    30.00

    If no price is included, returns None.
    """
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
    """
    Remove a simple $ price from the display name.
    """
    if not description:
        return description

    cleaned = re.sub(
        r"\$\s*\d+(?:\.\d{1,2})?",
        "",
        description
    )

    return " ".join(cleaned.split()).strip()


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
        Put something into the Circular SA marketplace.

        Examples:

        !tengo bundle of pots and pans
        !tengo dining table $30
        !tengo leftover ceramic tile
        """

        if not description:
            await ctx.send(
                "♻️ **¿Qué tienes?**\n\n"
                "Try:\n"
                "`!tengo bundle of pots and pans`\n"
                "`!tengo dining table $30`"
            )
            return

        seller_id = str(ctx.author.id)
        seller_name = str(ctx.author)

        asking_price = extract_price(description)
        item_name = clean_item_name(description)

        # -------------------------------------------------
        # Create the item
        # -------------------------------------------------

        item_record = {
            "seller_discord_id": seller_id,
            "seller_discord_name": seller_name,
            "owner_name": seller_name,

            "item_name": item_name,
            "description": description,

            "asking_price": asking_price,

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
                "⚠️ I couldn't create the Vuelta."
            )
            return

        item = item_result.data[0]
        item_id = item["id"]

        # -------------------------------------------------
        # Create the LOCAL-FIRST Circular SA route
        # -------------------------------------------------

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

        # -------------------------------------------------
        # Search persistent !busco interests
        # -------------------------------------------------

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

        # -------------------------------------------------
        # Build response
        # -------------------------------------------------

        response = (
            f"♻️ **VUELTA #{item_id}**\n\n"
            f"📦 **{item_name}**\n"
        )

        if asking_price is not None:
            response += f"💰 Asking: **${asking_price:.2f}**\n"
        else:
            response += "💰 Price: **not specified**\n"

        response += (
            "📍 Route: **Circular SA local marketplace**\n"
            "🟢 Status: **available**\n"
        )

        if matched_interests:

            response += (
                "\n🎯 **Possible community matches:** "
                f"{len(matched_interests)}\n"
            )

            shown_users = set()

            for interest in matched_interests[:5]:

                user_id = interest.get("discord_user_id")

                if not user_id or user_id in shown_users:
                    continue

                shown_users.add(user_id)

                response += (
                    f"<@{user_id}> — "
                    f"you said you were looking for "
                    f"**{interest.get('description')}**\n"
                )

        else:

            response += (
                "\n🔎 No existing `!busco` matches yet.\n"
                "The item is still available to the community."
            )

        response += (
            f"\n\nUse `!vuelta {item_id}` "
            "to check this item's journey."
        )

        await ctx.send(
            response,
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
        Save something the user is looking for and
        immediately search available Circular SA items.

        Examples:

        !busco cookware
        !busco dining table
        !busco reclaimed lumber
        """

        if not description:
            await ctx.send(
                "🔎 **¿Qué buscas?**\n\n"
                "Try:\n"
                "`!busco cookware`\n"
                "`!busco dining table`\n"
                "`!busco reclaimed lumber`"
            )
            return

        user_id = str(ctx.author.id)
        user_name = str(ctx.author)

        keywords = sorted(
            normalize_words(description)
        )

        # -------------------------------------------------
        # Save persistent demand
        # -------------------------------------------------

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

        # -------------------------------------------------
        # Search current inventory
        # -------------------------------------------------

        item_result = (
            supabase
            .table("vuelta_items")
            .select("*")
            .eq("status", "available")
            .execute()
        )

        matches = []

        for item in item_result.data or []:

            # Don't recommend someone's own item back to them
            if item.get("seller_discord_id") == user_id:
                continue

            if is_match(item, interest):
                matches.append(item)

        # -------------------------------------------------
        # Build response
        # -------------------------------------------------

        response = (
            f"🔎 **BUSCO SAVED**\n\n"
            f"You're looking for:\n"
            f"**{description}**\n\n"
            "🔔 I'll keep this interest active for future "
            "Circular SA listings.\n"
        )

        if matches:

            response += (
                f"\n🎯 **I found {len(matches)} "
                "possible local match"
            )

            if len(matches) != 1:
                response += "es"

            response += ":**\n\n"

            for item in matches[:8]:

                item_id = item["id"]
                name = (
                    item.get("item_name")
                    or item.get("description")
                )

                response += (
                    f"♻️ **Vuelta #{item_id}**\n"
                    f"📦 {name}\n"
                )

                if item.get("asking_price") is not None:
                    response += (
                        f"💰 ${float(item['asking_price']):.2f}\n"
                    )

                seller = (
                    item.get("seller_discord_name")
                    or item.get("owner_name")
                    or "Community member"
                )

                response += (
                    f"👤 {seller}\n"
                    f"`!vuelta {item_id}`\n\n"
                )

        else:

            response += (
                "\n📭 Nothing currently available matched "
                "that search.\n\n"
                "When somebody uses `!tengo` for something "
                "that looks like a match, you'll be alerted."
            )

        await ctx.send(response)


    # =====================================================
    # !VUELTA
    # =====================================================

    @commands.command(name="vuelta")
    async def vuelta(self, ctx, item_id: int = None):
        """
        Show an item's current state and route history.

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
                f"❌ No Vuelta found with ID #{item_id}."
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

        response = (
            f"♻️ **VUELTA #{item_id}**\n\n"
            f"📦 **{name}**\n"
            f"🟢 Status: **{item.get('status')}**\n"
        )

        if item.get("owner_name"):
            response += (
                f"👤 Owner: **{item.get('owner_name')}**\n"
            )

        if item.get("category"):
            response += (
                f"🏷️ Category: **{item.get('category')}**\n"
            )

        if item.get("condition"):
            response += (
                f"✨ Condition: **{item.get('condition')}**\n"
            )

        if item.get("asking_price") is not None:
            response += (
                f"💰 Asking: "
                f"**${float(item['asking_price']):.2f}**\n"
            )

        if item.get("pickup_area"):
            response += (
                f"📍 Pickup: **{item.get('pickup_area')}**\n"
            )

        # -------------------------------------------------
        # Route history
        # -------------------------------------------------

        routes = route_result.data or []

        response += "\n🛣️ **Route history**\n"

        if not routes:

            response += "No routes recorded yet.\n"

        else:

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

                response += (
                    f"• **{platform}** — {route_status}"
                )

                if route.get("listed_price") is not None:
                    response += (
                        f" — listed "
                        f"${float(route['listed_price']):.2f}"
                    )

                if route.get("sold_price") is not None:
                    response += (
                        f" — sold "
                        f"${float(route['sold_price']):.2f}"
                    )

                response += "\n"

        await ctx.send(response)


async def setup(bot):
    await bot.add_cog(VueltaBot(bot))
