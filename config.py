import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv(override=True)

OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY") or "").strip()
DISCORD_TOKEN = (os.getenv("DISCORD_TOKEN") or "").strip()
GUARDABOT_DB = (os.getenv("GUARDABOT_DB") or "guardabot.db").strip()

if not DISCORD_TOKEN:
    raise RuntimeError("DISCORD_TOKEN is missing. Check your .env file.")
if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY is missing. Check your .env file.")

client = OpenAI(api_key=OPENAI_API_KEY)