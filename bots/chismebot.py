import traceback
from config import client

def safe_text_from_openai_response(resp) -> str:
    """
    Tries the most common places text appears in the OpenAI SDK response.
    Falls back to stringifying the response if needed.
    """
    text = getattr(resp, "output_text", None)
    if isinstance(text, str) and text.strip():
        return text

    try:
        if hasattr(resp, "output") and resp.output:
            first = resp.output[0]
            if hasattr(first, "content") and first.content:
                c0 = first.content[0]
                if hasattr(c0, "text") and isinstance(c0.text, str):
                    return c0.text
                if isinstance(c0, dict) and "text" in c0 and isinstance(c0["text"], str):
                    return c0["text"]
    except Exception:
        pass

    return "(No visible text returned — model may have been truncated.)"


async def send_long(ctx, text: str, limit: int = 1900):
    """
    Discord hard limit is 2000 characters. Use 1900 to be safe.
    Splits long text across multiple messages.
    """
    if not isinstance(text, str):
        text = str(text)

    text = text.strip() or "(empty response)"
    for i in range(0, len(text), limit):
        await ctx.send(text[i : i + limit])


# ---------- CHISMEBOT COMMAND ----------

def register_chisme(bot):

    @bot.command()
    async def chisme(ctx, *, note: str = ""):
        
        if not note.strip():
            await ctx.send('Tell me chisme like: `!chisme met Lucy at the coffeeshop...`')
            return
    
        await ctx.send("…thinking…")
    
        try:
            resp = client.responses.create(
                model="gpt-5-mini",
                reasoning={"effort": "low"},
                input=[
                    {
                        "role": "system",
                        "content": (
                            "You are Chismebot. Turn messy social notes into a clean contact card with fields:\n"
                            "Name\nOrg\nWhere Met\nIntro\nBuilding\nWants to Meet\nTags\nFollow up\nNext action\n\n"
                            "Ask up to three follow-up questions ONLY if needed."
                        ),
                    },
                    {"role": "user", "content": note},
                ],
                max_output_tokens=800,
            )
    
            text = safe_text_from_openai_response(resp)
            await send_long(ctx, text)
    
        except Exception:
            print("=== FULL ERROR TRACEBACK ===")
            traceback.print_exc()
            print("=== END TRACEBACK ===")
            await ctx.send("⚠️ Error. Check the terminal traceback.")