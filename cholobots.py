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
        metiche.generate_daily_schedule()
        bot.loop.create_task(metiche.start_loop())
        
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

@bot.command(name="queso")
async def queso(ctx):
    msg = """
    🧀 CHOLOBOTS QUICKSTART
    
    START YOUR WEEK
    !metiche_weekly
    
    Metiche will ask you:
    • weekly revenue goal
    • jobs scheduled
    • pending estimates
    • invoices to send
    • today's plan
    • whether to enable task accounting
    
    Just answer the questions in chat.
    
    –––––––––––––––––
    
    TURN ON 2-HOUR CHECK-INS
    !metiche_accountant
    
    Metiche will ask:
    Que onda? Last 2 hours.
    Revenue / Infrastructure / Outreach / Admin / Drift?
    
    Reply format:
    <Category> - <task> - <energy 1-5>
    
    Example:
    Revenue - sent estimate to Dancing Bear - 4
    
    –––––––––––––––––
    
    LOG JOB LABOR
    !metiche_log <job> <hours> <cost> <note>
    
    Example:
    !metiche_log dancingbear 2 0 installed outlet
    
    –––––––––––––––––
    
    TRACK MATERIAL SPENDING
    !guard spend material <job> <amount> <note>
    
    Example:
    !guard spend material dancingbear 45 romex
    
    –––––––––––––––––
    
    FIND MATERIALS
    !guard where <item>
    
    START ORGANIZATION
    !guard organization
    
    FINISH ORGANIZATION
    !guard done
    
    –––––––––––––––––
    
    CLOSE A JOB
    !crudo close <job>
    
    VIEW JOB REPORT
    !crudo report <job>
    """
    await ctx.send(msg)


bot.run(DISCORD_TOKEN)
