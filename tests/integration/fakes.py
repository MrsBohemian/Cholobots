# ABOUTME: Minimal Discord stand-ins (user, channel, message, context) for driving bot callbacks without a token.
# ABOUTME: Also scripts bot.wait_for replies (failing loudly when they run out) and finds @bot.listen listeners.
import asyncio

import discord
from discord.ext import commands


class FakeUser:
    def __init__(self, id, name="tester", bot=False):
        self.id = id
        self.name = name
        self.display_name = name
        self.bot = bot
        self.mention = f"<@{id}>"

    def __str__(self):
        return self.name


class FakeGuild:
    def __init__(self, id=900000000000000099):
        self.id = id


class FakeChannel:
    def __init__(self, id):
        self.id = id
        self.sent = []

    async def send(self, content=None, **kwargs):
        self.sent.append(content if content is not None else kwargs)


class FakeAttachment:
    def __init__(self, filename="receipt.jpg", content_type="image/jpeg", data=b"", size=None):
        self.filename = filename
        self.content_type = content_type
        self.data = data
        self.size = len(data) if size is None else size
        self.url = f"https://example.invalid/{filename}"

    async def read(self):
        return self.data


class FakeMessage:
    def __init__(self, content, author, channel, guild=None, attachments=()):
        self.content = content
        self.author = author
        self.channel = channel
        self.guild = guild
        self.attachments = list(attachments)
        self.id = 0

    async def reply(self, content=None, **kwargs):
        await self.channel.send(content, **kwargs)


class FakeContext:
    def __init__(self, author, channel, guild, message, bot=None):
        self.author = author
        self.channel = channel
        self.guild = guild
        self.message = message
        self.bot = bot
        self.sent = []

    async def send(self, content=None, **kwargs):
        value = content if content is not None else kwargs
        self.sent.append(value)
        await self.channel.send(content, **kwargs)

    async def reply(self, content=None, **kwargs):
        await self.send(content, **kwargs)

    @property
    def sent_text(self):
        return "\n".join(str(item) for item in self.sent)


def make_bot():
    return commands.Bot(command_prefix="!", intents=discord.Intents.default())


TIMEOUT = object()
"""Script this in place of a reply to make that wait_for time out, as a real timeout would."""


class UnscriptedPrompt(RuntimeError):
    """The bot prompted for a reply the test did not script.

    Deliberately not TimeoutError (bot code catches that and carries on quietly) and not
    AssertionError (a strict xfail with raises=AssertionError would swallow it).
    """


def script_replies(bot, ctx, replies):
    """Replace bot.wait_for with a queue of scripted messages and return the queue.

    A str reply is sent by ctx's author in ctx's channel. Messages that fail the caller's
    check are skipped. TIMEOUT raises asyncio.TimeoutError, but only when the caller passed
    a timeout: without one the real bot would wait forever. An empty queue raises
    UnscriptedPrompt. Assert the returned queue is empty to prove the flow used every reply.
    """
    queue = [
        r if isinstance(r, FakeMessage) or r is TIMEOUT
        else FakeMessage(r, ctx.author, ctx.channel, ctx.guild)
        for r in replies
    ]

    async def wait_for(event, *, check=None, timeout=None):
        while queue:
            message = queue.pop(0)
            if message is TIMEOUT:
                if timeout is None:
                    raise UnscriptedPrompt(
                        f"TIMEOUT scripted for wait_for({event!r}) with no timeout; "
                        "the real bot would wait forever"
                    )
                raise asyncio.TimeoutError()
            if check is None or check(message):
                return message
        raise UnscriptedPrompt(
            f"bot.wait_for({event!r}) ran out of scripted replies; "
            f"last bot message: {ctx.sent[-1] if ctx.sent else None!r}"
        )

    bot.wait_for = wait_for
    return queue


def get_listener(bot, name):
    return next(f for f in bot.extra_events["on_message"] if f.__name__ == name)
