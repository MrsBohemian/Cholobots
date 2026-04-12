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
    print(f"✅ Logged in as {bot.user} | Guardabot DB: {GUARDABOT_DB}")
    
    metiche = get_metiche()
    if metiche is not None:
        if not metiche.schedule:
            metiche.generate_daily_schedule()
    
        if getattr(metiche, "loop_task", None) is None or metiche.loop_task.done():
            metiche.loop_task = bot.loop.create_task(metiche.start_loop())
        
@bot.listen()
async def on_message(message: discord.Message):
    
    # ignore the bot's own messages
    if message.author == bot.user:
        return

    # If Metiche is waiting for a yes/no in her active channel, handle it
    if metiche and metiche.channel_id == message.channel.id and getattr(metiche, "awaiting_accountant_choice", False):
        text = message.content.lower().strip()

        if text in ("yes", "y"):
            metiche.awaiting_accountant_choice = False
            metiche.turn_on(message.channel.id)
            await message.channel.send("✅ Accountant mode ON. I’ll check in every 2 hours (until 5pm).")
            return

        if text in ("no", "n"):
            metiche.awaiting_accountant_choice = False
            await message.channel.send("👌 Okay. Accountant mode skipped. You can still use `!metiche_on` later.")
            return
        await bot.process_commands(message)
        
    @bot.command(name="cholobots")
    async def cholobots(ctx):
        msg = """
    🤖 WHO ARE THE CHOLOBOTS?
    
    If your business is going sideways, consider these new hires.
    
    The Cholobots are your four office staff: part thug, part guru, fully committed to dragging your business toward functionality.
    
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
    
    Type `!queso` to see what the Cholobots do.
    """
    await ctx.send(msg)
    
    @bot.command(name="queso")
    async def queso(ctx):
        msg = """
    🧀 HOW THE CHOLOBOTS ACTUALLY WORK
    
    You don’t use all four at once.
    You bring them in when you need them.
    
    –––––––––––––––––
    
    🧠 METICHEBOT — RUN YOUR WEEK
    Start here.
    
    She will:
    • get your weekly goal
    • track your jobs, estimates, invoices
    • build your schedule
    • check in on you so you don’t drift into the void
    
    Use her when:
    you need direction, structure, or accountability
    
    –––––––––––––––––
    
    💬 CHISMEBOT — TRACK PEOPLE
    Use her right after conversations.
    
    She will:
    • clean up messy notes
    • turn interactions into usable contact info
    • help you remember who matters and why
    
    Use her when:
    you meet someone, get a lead, or feel like “I should remember this”
    
    –––––––––––––––––
    
    📦 GUARDABOT — TRACK STUFF
    Use him during jobs.
    
    He will:
    • track materials and spending
    • help you find things
    • keep your physical world from turning into chaos
    
    Use him when:
    you’re buying, moving, or losing materials
    
    –––––––––––––––––
    
    📊 CRUDOBOT — MAKE SENSE OF IT
    Use him after the work is done.
    
    He will:
    • close out jobs
    • show you what actually made money
    • help you process what worked and what didn’t
    
    Use him when:
    you need clarity, numbers, or post-mortem energy
    
    –––––––––––––––––
    
    Type:
    !metichebot
    !chismebot
    !guardabot
    !crudobot
    
    to see what each one can do right now.
    """
    await ctx.send(msg)

bot.run(DISCORD_TOKEN)
