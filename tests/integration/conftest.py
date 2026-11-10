# ABOUTME: Fixtures for integration tests that drive bot handlers against the local Supabase stack.
# ABOUTME: Refuses non-local URLs, truncates and re-seeds the database before each test, and swaps every module's client.
import importlib
import os
import sys
import urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio

from . import fakes
from .local_guard import integration_settings, is_local_url

REPO_ROOT = Path(__file__).resolve().parents[2]
SEED_SQL = REPO_ROOT / "supabase" / "seed.sql"
HERE = Path(__file__).resolve().parent
BOT_MODULES = [
    "bots.metichebot",
    "bots.crudobot",
    "bots.chismebot",
    "bots.guardabot",
    "bots.vueltabot",
    "bots.obijuan",
    "db.database",
]
DEFAULT_NOW = datetime(2031, 1, 15, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

_state = {"settings": None, "problem": None}


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: needs the local Supabase stack (make test-integration)"
    )


def _probe(settings):
    request = urllib.request.Request(
        settings.url.rstrip("/") + "/rest/v1/",
        headers={"apikey": settings.service_key, "Authorization": f"Bearer {settings.service_key}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=5):
            return ""
    except Exception as exc:  # noqa: BLE001 - any failure means the stack is unusable
        return f"local stack not running (make db-up): {exc}"


def pytest_sessionstart(session):
    settings, reason = integration_settings(os.environ)
    if settings is not None:
        reason = _probe(settings)
        if reason:
            settings = None
    _state["settings"] = settings
    _state["problem"] = reason or None


def pytest_collection_modifyitems(config, items):
    ours = [i for i in items if HERE in Path(str(i.path)).parents]
    if not ours or _state["settings"] is not None:
        return
    reason = _state["problem"]
    if os.environ.get("CHOLOBOTS_INTEGRATION_REQUIRED") == "1":
        pytest.exit(f"integration harness cannot run: {reason}", returncode=2)
    skip = pytest.mark.skip(reason=reason)
    for item in ours:
        item.add_marker(skip)


@pytest.fixture(scope="session")
def settings():
    assert _state["settings"] is not None
    return _state["settings"]


@pytest.fixture(scope="session", autouse=True)
def integration_env(settings, tmp_path_factory):
    import dotenv

    mp = pytest.MonkeyPatch()
    mp.setenv("DISCORD_TOKEN", "integration-dummy")
    mp.setenv("OPENAI_API_KEY", "integration-dummy")
    mp.setenv("SUPABASE_URL", settings.url)
    mp.setenv("SUPABASE_SERVICE_ROLE_KEY", settings.service_key)
    mp.setenv("TRANSPORTATION_CHANNEL_ID", "0")
    mp.setenv("GUARDABOT_DB", str(tmp_path_factory.mktemp("guardabot") / "guardabot.db"))
    mp.delenv("DATA_SERVICE_URL", raising=False)
    mp.setattr(dotenv, "load_dotenv", lambda *a, **k: False)
    yield
    mp.undo()


@pytest.fixture(scope="session")
def db(settings, integration_env):
    from supabase import create_client

    assert is_local_url(settings.url)
    client = create_client(settings.url, settings.service_key)
    yield client
    # Close the pooled HTTP connection so the run ends without an unclosed-socket warning.
    client.postgrest.session.close()


@pytest.fixture(scope="session")
def bot_modules(integration_env):
    return {name.split(".")[-1]: importlib.import_module(name) for name in BOT_MODULES}


@pytest.fixture(scope="session")
def pg_conn(settings):
    import psycopg

    assert is_local_url(settings.db_url)
    conn = psycopg.connect(settings.db_url, autocommit=True)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def reset_database(pg_conn):
    def reset():
        rows = pg_conn.execute(
            "select tablename from pg_tables where schemaname = 'public'"
        ).fetchall()
        if rows:
            names = ", ".join('"public"."' + r[0].replace('"', '""') + '"' for r in rows)
            pg_conn.execute(f"TRUNCATE {names} RESTART IDENTITY CASCADE")
        pg_conn.execute(SEED_SQL.read_text())

    return reset


@pytest.fixture(autouse=True)
def reset_db(request, reset_database):
    if "integration" in request.keywords or HERE in Path(str(request.node.path)).parents:
        reset_database()


@pytest.fixture(autouse=True)
def local_clients(db, bot_modules, monkeypatch):
    for module in bot_modules.values():
        monkeypatch.setattr(module, "supabase", db)


@pytest.fixture(autouse=True)
def clear_sessions(bot_modules):
    def clear():
        bot_modules["metichebot"].active_time_sessions.clear()
        bot_modules["metichebot"].channels_waiting_for_command.clear()
        bot_modules["crudobot"].receipt_parse_sessions.clear()
        chismebot = bot_modules["chismebot"]
        for name in dir(chismebot):
            if name.endswith("_sessions") and isinstance(getattr(chismebot, name), dict):
                getattr(chismebot, name).clear()

    clear()
    yield
    clear()


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def refuse(*args, **kwargs):
        raise OSError("network disabled in integration tests")

    monkeypatch.setattr(urllib.request, "urlopen", refuse)


@pytest.fixture(autouse=True)
def chicago_clock(bot_modules, monkeypatch):
    def set_now(value=DEFAULT_NOW):
        monkeypatch.setattr(bot_modules["metichebot"], "local_now", lambda: value)

    set_now()
    return set_now


@pytest.fixture
def make_ctx():
    def make(author_id, channel_id, content="", attachments=(), bot=None):
        author = fakes.FakeUser(author_id)
        channel = fakes.FakeChannel(channel_id)
        guild = fakes.FakeGuild()
        message = fakes.FakeMessage(content, author, channel, guild, attachments)
        return fakes.FakeContext(author, channel, guild, message, bot)

    return make


@pytest.fixture
def script_replies():
    return fakes.script_replies


class Factories:
    def __init__(self, db):
        self.db = db

    def _insert(self, table, row):
        return self.db.table(table).insert(row).execute().data[0]

    def chisme_contact(self, name):
        return self._insert("chisme_contacts", {"name": name})

    def crudo_project(self, name, status="active", closeout_status=None, contact_id=None,
                      tasks_completed=0, tasks_pending=0, actual_minutes=None):
        if contact_id is None:
            contact_id = self.chisme_contact(f"{name} Contact")["id"]
        row = {"contact_id": contact_id, "project_name": name, "status": status}
        if closeout_status is not None:
            row["closeout_status"] = closeout_status
        project = self._insert("crudo_projects", row)
        order = 0
        for _ in range(tasks_completed):
            order += 1
            self._insert("crudo_project_tasks", {
                "project_id": project["id"], "task_name": f"Done task {order}",
                "task_order": order, "status": "completed",
                "completed_at": "2031-01-14T12:00:00+00:00", "actual_minutes": actual_minutes,
            })
        for _ in range(tasks_pending):
            order += 1
            self._insert("crudo_project_tasks", {
                "project_id": project["id"], "task_name": f"Pending task {order}",
                "task_order": order, "status": "pending",
            })
        return project

    def daily_tasks(self, calendar_key, date, tasks, completed=False):
        rows = [{"user_id": calendar_key, "calendar_key": calendar_key, "date": date,
                 "task": t, "completed": completed} for t in tasks]
        return self.db.table("daily_tasks").insert(rows).execute().data

    def vuelta_item(self, title, status="available", seller_discord_id="100000000000000009",
                    asking_price=None):
        return self._insert("vuelta_items", {
            "item_name": title, "description": title, "status": status,
            "seller_discord_id": seller_discord_id, "asking_price": asking_price,
        })


@pytest.fixture
def factories(db):
    return Factories(db)


@pytest_asyncio.fixture
async def metiche_bot(bot_modules):
    bot = fakes.make_bot()
    bot_modules["metichebot"].register_metiche(bot)
    return bot


@pytest_asyncio.fixture
async def crudo_bot(bot_modules):
    bot = fakes.make_bot()
    bot_modules["crudobot"].register_crudo(bot)
    return bot


@pytest_asyncio.fixture
async def vuelta_bot(bot_modules):
    bot = fakes.make_bot()
    cog = bot_modules["vueltabot"].VueltaBot(bot)
    await bot.add_cog(cog)
    return bot, cog
