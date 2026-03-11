import random
import discord
from discord.ext import commands

from db.database import (
    insert_crudo_report,
    fetch_latest_crudo_report
)


def register_crudo(bot: commands.Bot):

    @bot.group(name="crudo", invoke_without_command=True)
    async def crudo_group(ctx: commands.Context):
        await ctx.send(
            "Crudobot commands:\n"
            "- `!crudo report`\n"
            "- `!crudo close`\n"
        )


    @crudo_group.command(name="report")
    async def crudo_report(ctx: commands.Context):
        report = fetch_latest_crudo_report()

        if not report:
            await ctx.send("No crudo reports yet.")
            return

        await ctx.send(report)


    @crudo_group.command(name="close")
    async def crudo_close(ctx: commands.Context):

        await ctx.send(
            "Closing job. Tell me how it went."
        )

        # placeholder for now
        insert_crudo_report({
            "ts": "now",
            "notes": "manual close"
        })

        await ctx.send("Report saved.")