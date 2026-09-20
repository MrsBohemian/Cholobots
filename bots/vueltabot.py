import os
import re
from datetime import datetime, timezone

import discord
from discord.ext import commands
from supabase import create_client, Client


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = (
    os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    or os.getenv("SUPABASE_KEY")
)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

HOOD_UBER_CHANNEL_ID = int(os.getenv("HOOD_UBER_CHANNEL_ID", "0"))


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

    async def ask_user(self, ctx, question, timeout=120):
        await ctx.send(question)

        def check(message):
            return (
                message.author.id == ctx.author.id
                and message.channel.id == ctx.channel.id
            )

        try:
            message = await self.bot.wait_for(
                "message", timeout=timeout, check=check
            )
            return message.content.strip()
        except TimeoutError:
            await ctx.send(
                "⌛ Transportation request timed out. "
                "The item is still claimed."
            )
            return None

    # =====================================================
    # !VUELTABOT
    # =====================================================

    @commands.command(name="vueltabot")
    async def vueltabot(self, ctx, *, topic: str = None):
        embed = discord.Embed(
            title="♻️ Qué onda, I’m Vueltabot.",
            description=(
                "I help stuff keep moving instead of sitting around unused.\n\n"
                "List what you have, tell me what you need, claim available "
                "items, and request a local delivery gig when you need help "
                "moving something."
            )
        )
        embed.add_field(
            name="📦 !tengo",
            value="List something you have available.\nExample: `!tengo cast iron skillet $10`",
            inline=False
        )
        embed.add_field(
            name="🔎 !busco",
            value="Tell me what you're looking for and save the search.\nExample: `!busco pots and pans`",
            inline=False
        )
        embed.add_field(
            name="🤝 !claim",
            value=(
                "Claim an available item. After claiming, I can help you "
                "request paid transportation.\nExample: `!claim 57`"
            ),
            inline=False
        )
        embed.add_field(
            name="🚗 !tripclaim",
            value="Claim an open transportation gig.\nExample: `!tripclaim 7`",
            inline=False
        )
        embed.add_field(
            name="🛣️ !vuelta",
            value="See an item's current status and route history.\nExample: `!vuelta 57`",
            inline=False
        )
        embed.set_footer(text="Have it → find it → claim it → keep it moving. 🔄")
        await ctx.send(embed=embed)


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

    # =====================================================
    # !CLAIM
    # =====================================================

    @commands.command(name="claim")
    async def claim(self, ctx, item_id: int = None):
        """
        Claim an available item.

        Example:
        !claim 57
        """

        if not item_id:
            await ctx.send(
                "Use it like this:\n"
                "`!claim 57`"
            )
            return

        buyer_id = str(ctx.author.id)
        buyer_name = str(ctx.author)

        # Find the item
        result = (
            supabase
            .table("vuelta_items")
            .select("*")
            .eq("id", item_id)
            .execute()
        )

        if not result.data:
            await ctx.send(
                f"❌ I can't find Item #{item_id}."
            )
            return

        item = result.data[0]

        # Make sure it's available
        if item.get("status") != "available":
            await ctx.send(
                f"⚠️ Item #{item_id} is already "
                f"**{item.get('status')}**."
            )
            return

        # Don't let someone claim their own item
        if item.get("seller_discord_id") == buyer_id:
            await ctx.send(
                "😂 Homie, that's your own item."
            )
            return

        # Create the transaction
        transaction_record = {
            "item_id": item_id,

            "seller_discord_id": item.get(
                "seller_discord_id"
            ),
            "seller_discord_name": item.get(
                "seller_discord_name"
            ),

            "buyer_discord_id": buyer_id,
            "buyer_discord_name": buyer_name,

            "agreed_price": item.get("asking_price"),

            "transaction_type": (
                "giveaway"
                if item.get("asking_price") is None
                else "sale"
            ),

            "transport_required": False,
            "status": "claimed"
        }

        transaction_result = (
            supabase
            .table("vuelta_transactions")
            .insert(transaction_record)
            .execute()
        )

        if not transaction_result.data:
            await ctx.send(
                "⚠️ Something went wrong while claiming the item."
            )
            return

        transaction = transaction_result.data[0]
        transaction_id = transaction["id"]

        # Mark item claimed
        (
            supabase
            .table("vuelta_items")
            .update({
                "status": "claimed"
            })
            .eq("id", item_id)
            .execute()
        )

        # Mark Circular SA marketplace route claimed
        (
            supabase
            .table("vuelta_routes")
            .update({
                "status": "claimed"
            })
            .eq("item_id", item_id)
            .eq("route_type", "local_marketplace")
            .eq("status", "listed")
            .execute()
        )

        # Build confirmation card
        item_name = (
            item.get("item_name")
            or item.get("description")
        )

        embed = discord.Embed(
            title=f"🤝 Item #{item_id} claimed!",
            description=item_name
        )

        embed.add_field(
            name="Buyer",
            value=f"<@{buyer_id}>",
            inline=True
        )

        seller_id = item.get("seller_discord_id")

        if seller_id:
            embed.add_field(
                name="Seller",
                value=f"<@{seller_id}>",
                inline=True
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
                value="Free / giveaway",
                inline=True
            )

        if item.get("photo_url"):
            embed.set_image(
                url=item.get("photo_url")
            )

        embed.set_footer(
            text="This item is no longer available to other buyers."
        )

        await ctx.send(
            embed=embed,
            allowed_mentions=discord.AllowedMentions(
                users=True,
                everyone=False,
                roles=False
            )
        )
        

        # =====================================================
        # TRANSPORTATION OPTION
        # =====================================================

        transport_answer = await self.ask_user(
            ctx,
            "🚗 **Need a homie to move it?**\n\n"
            "Reply `yes` if you're willing to pay for delivery.\n"
            "Reply `no` if you'll handle pickup yourself."
        )

        if not transport_answer:
            return

        if transport_answer.lower() not in {"yes", "y", "yeah", "yep", "sure"}:
            await ctx.send("👍 Got it. You'll handle pickup.")
            return

        offer_text = await self.ask_user(
            ctx,
            "💵 **What are you willing to pay for delivery?**\n\n"
            "Enter a dollar amount, for example: `15`"
        )
        if not offer_text:
            return

        offer_match = re.search(r"\d+(?:\.\d{1,2})?", offer_text)
        if not offer_match:
            await ctx.send("⚠️ I couldn't understand that delivery amount.")
            return

        delivery_offer = float(offer_match.group())
        if delivery_offer <= 0:
            await ctx.send("⚠️ The delivery offer needs to be more than $0.")
            return

        pickup_area = await self.ask_user(
            ctx,
            "📍 **What general area is the pickup in?**\n\n"
            "Don't post the full address here.\n"
            "Example: `Walzem / 78218`"
        )
        if not pickup_area:
            return

        dropoff_area = await self.ask_user(
            ctx,
            "📍 **What general area should it be delivered to?**\n\n"
            "Don't post the full address here.\n"
            "Example: `Downtown / 78205`"
        )
        if not dropoff_area:
            return

        (
            supabase.table("vuelta_transactions")
            .update({"transport_required": True})
            .eq("id", transaction_id)
            .execute()
        )

        transport_record = {
            "item_id": str(item_id),
            "claim_id": str(transaction_id),
            "buyer_discord_id": buyer_id,
            "seller_discord_id": seller_id,
            "pickup_area": pickup_area,
            "dropoff_area": dropoff_area,
            "delivery_offer": delivery_offer,
            "status": "open"
        }
        transport_result = (
            supabase.table("transport_gigs")
            .insert(transport_record)
            .execute()
        )
        if not transport_result.data:
            await ctx.send(
                "⚠️ The item is claimed, but I couldn't create the transportation gig."
            )
            return

        gig = transport_result.data[0]
        gig_id = gig["id"]
        await ctx.send(
            f"🚗 **Transportation requested!**\n"
            f"Trip Gig #{gig_id} has been opened for **${delivery_offer:.2f}**."
        )

        if not HOOD_UBER_CHANNEL_ID:
            await ctx.send(
                "⚠️ The trip is saved, but `HOOD_UBER_CHANNEL_ID` hasn't been configured yet."
            )
            return

        hood_channel = self.bot.get_channel(HOOD_UBER_CHANNEL_ID)
        if not hood_channel:
            await ctx.send(
                "⚠️ I created the trip, but I couldn't find the Hood Uber channel."
            )
            return

        gig_embed = discord.Embed(
            title=f"🚗 Trip Gig #{gig_id}",
            description=item_name
        )
        gig_embed.add_field(name="Pickup", value=pickup_area, inline=True)
        gig_embed.add_field(name="Dropoff", value=dropoff_area, inline=True)
        gig_embed.add_field(
            name="Delivery Offer", value=f"${delivery_offer:.2f}", inline=True
        )
        gig_embed.add_field(name="Item", value=f"#{item_id}", inline=True)
        gig_embed.add_field(name="Status", value="OPEN", inline=True)
        gig_embed.set_footer(text=f"Claim this trip with !tripclaim {gig_id}")

        gig_message = await hood_channel.send(embed=gig_embed)
        (
            supabase.table("transport_gigs")
            .update({
                "gig_channel_id": str(hood_channel.id),
                "gig_message_id": str(gig_message.id)
            })
            .eq("id", gig_id)
            .execute()
        )

    # =====================================================
    # !TRIPCLAIM
    # =====================================================

    @commands.command(name="tripclaim")
    async def tripclaim(self, ctx, gig_id: int = None):
        if not gig_id:
            await ctx.send("Use it like this:\n`!tripclaim 7`")
            return

        driver_id = str(ctx.author.id)
        result = (
            supabase.table("transport_gigs")
            .select("*")
            .eq("id", gig_id)
            .execute()
        )
        if not result.data:
            await ctx.send(f"❌ I can't find Trip Gig #{gig_id}.")
            return

        gig = result.data[0]
        if gig.get("status") != "open":
            await ctx.send(
                f"⚠️ Trip Gig #{gig_id} is already **{gig.get('status')}**."
            )
            return

        if gig.get("buyer_discord_id") == driver_id:
            await ctx.send("😂 Homie, you can't claim your own delivery gig.")
            return

        claim_result = (
            supabase.table("transport_gigs")
            .update({
                "driver_discord_id": driver_id,
                "status": "claimed",
                "claimed_at": datetime.now(timezone.utc).isoformat()
            })
            .eq("id", gig_id)
            .eq("status", "open")
            .execute()
        )
        if not claim_result.data:
            await ctx.send(
                f"⚠️ Trip Gig #{gig_id} was just claimed by someone else."
            )
            return

        claimed_gig = claim_result.data[0]
        await ctx.send(
            f"🚗 **Trip Gig #{gig_id} claimed!**\n\n"
            f"<@{driver_id}> is taking this trip.\n"
            f"Pickup: **{claimed_gig.get('pickup_area')}**\n"
            f"Dropoff: **{claimed_gig.get('dropoff_area')}**\n"
            f"Pay: **${float(claimed_gig['delivery_offer']):.2f}**",
            allowed_mentions=discord.AllowedMentions(
                users=True, everyone=False, roles=False
            )
        )

        channel_id = claimed_gig.get("gig_channel_id")
        message_id = claimed_gig.get("gig_message_id")
        if channel_id and message_id:
            try:
                gig_channel = self.bot.get_channel(int(channel_id))
                if gig_channel:
                    gig_message = await gig_channel.fetch_message(int(message_id))
                    if gig_message.embeds:
                        gig_embed = gig_message.embeds[0]
                        gig_embed.set_field_at(
                            4,
                            name="Status",
                            value=f"CLAIMED by {ctx.author}",
                            inline=True
                        )
                        gig_embed.set_footer(text="This trip has been claimed.")
                        await gig_message.edit(embed=gig_embed)
            except (
                discord.NotFound, discord.Forbidden, discord.HTTPException,
                ValueError, IndexError
            ):
                pass

async def setup(bot):
    await bot.add_cog(VueltaBot(bot))
