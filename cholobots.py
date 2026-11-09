import re
import json
import sqlite3
import traceback
import asyncio
import random
from datetime import time, timedelta
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple

import discord
from discord.ext import commands
from config import DISCORD_TOKEN, GUARDABOT_DB

from bots.chismebot import register_chisme
from bots.metichebot import register_metiche, get_metiche
from bots.guardabot import register_guard
from bots.crudobot import register_crudo
from bots.obijuan import setup_obijuan

from db.database import (
    init_guardabot_db,
    ensure_guardabot_schema,
    init_metiche_db,
    init_crudobot_db,
)

# ---------- DISCORD BOT ----------

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)
register_chisme(bot)
register_metiche(bot)
register_guard(bot)
register_crudo(bot)

metiche = None

@bot.event
async def on_ready():
    global metiche
    init_guardabot_db()
    ensure_guardabot_schema()
    init_metiche_db()
    
    init_crudobot_db()
    if not hasattr(bot, "obijuan_loaded"):
        await setup_obijuan(bot)
        bot.obijuan_loaded = True
        print("✅ ObiJuan loaded")
        
    if not hasattr(bot, "vuelta_loaded"):
        await bot.load_extension("bots.vueltabot")
        bot.vuelta_loaded = True
        print("✅ Vueltabot loaded")
            
    print(f"✅ Logged in as {bot.user} | Guardabot DB: {GUARDABOT_DB}")
    
    metiche = get_metiche()
    if metiche is not None:
        if getattr(metiche, "loop_task", None) is None or metiche.loop_task.done():
            metiche.loop_task = bot.loop.create_task(metiche.start_loop())
        
@bot.event
async def on_message(message: discord.Message):

    # Ignore the bot's own messages
    if message.author == bot.user:
        return

    # Allow attachments for approved Cholobot workflows.
    # Crudobot uses attachments for purchase/return receipts.
    # Vueltabot uses attachments for marketplace item photos.
    if message.attachments:
        content = (message.content or "").strip().lower()

        attachment_commands = {"!crudoreceipt", "!tengo"}
        command_name = content.split(maxsplit=1)[0] if content else ""

        if command_name in attachment_commands:
            try:
                await bot.process_commands(message)

            except Exception as e:
                print(f"[ATTACHMENT COMMAND ERROR] {e}")
                traceback.print_exc()

                await message.channel.send(
                    "Simón... the attachment workflow broke, "
                    "but the Cholobots stayed alive."
                )

            return

        print(
            f"[ATTACHMENT BLOCKED] "
            f"{message.author} sent attachment(s) "
            f"in #{message.channel}"
        )

        await message.channel.send(
            "Órale homie, I saw the attachment. "
            "Right now photos/files are enabled for "
            "`!crudoreceipt` and `!tengo`."
        )

        return

    # Keep the system alive if a bot crashes
    try:
        await bot.process_commands(message)

    except Exception as e:
        print(f"[MESSAGE ERROR] {e}")
        traceback.print_exc()

        await message.channel.send(
            "Simón... something broke but the Cholobots stayed alive."
        )

        
@bot.command(name="cholobots")
async def cholobots(ctx):
    msg = """
    🤖 WHO ARE THE CHOLOBOTS?
    The Cholobots are hustlers who can be your best friends for life. If you need to grind as an entrepreneur, or to set a goal, these guys can help!
        
    💬 CHISMEBOT
    Chismebot wants to talk about everybody.
    She keeps the dirt on anyone and does more than CRM.
    This chola grew up watching Oprah Winfrey and Grant Cardone at the same time.
    If there’s a person, a vibe, a lead, a weird interaction, or a follow-up opportunity, she wants the tea.
        
    🧠 METICHEBOT
    Metichebot is always in your business.
    That would be annoying if it wasn’t for the fact that this girl knows how to make money, honey.
    Sure, being bugged by her might be irritating, but if you trust her and open up, my girl knows how to spin leftovers into pure gold.
    She handles planning, scheduling, check-ins, and task accounting.
        
    📦 GUARDABOT
    Guardabot is a flaco strong silent type.
    Think of him as a chill version of Gollum, except instead of “my precious,” all your construction materials are precious.
    Homeboy is happiest when he can disappear into his autistic element with your tools, supplies, and organization systems.
        
    📊 CRUDOBOT
    Crudobot is kind of like your drunk uncle meets Deepak Chopra.
    His job is job costing, reports, and estimates, but his real jam is listening while you open up about the work:
    what went right, what went terribly wrong, and what the numbers are trying to tell you.

    ...And if you're lucky, you get to meet Obi Juan Que Homie
        
    Type `!metichebot`, `!chismebot`, `!guardabot` or `!crudobot` to see what each one can do right now.
    """
    await ctx.send(msg)

if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
