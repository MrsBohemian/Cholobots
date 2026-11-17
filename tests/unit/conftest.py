# ABOUTME: Unit fixtures for importing a bot module with no network: dummy env, no .env, a stub Supabase client.
# ABOUTME: Bot modules imported here are dropped from sys.modules afterwards so the stub never leaks into later tests.
import importlib
import sys
import urllib.request
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
PROJECT_PACKAGES = ("config", "bots", "db")

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class _NoSupabase:
    """Stands in for the client crudobot builds at import; any query is a bug in a unit test."""

    def table(self, name):
        raise AssertionError(f"unit test touched Supabase table {name!r}; use tests/integration")


def _no_network(*args, **kwargs):
    raise AssertionError("unit tests must not open network connections")


def _is_project_module(name):
    return any(name == pkg or name.startswith(pkg + ".") for pkg in PROJECT_PACKAGES)


@pytest.fixture
def crudobot(monkeypatch):
    import dotenv
    import supabase

    monkeypatch.setenv("DISCORD_TOKEN", "unit-dummy")
    monkeypatch.setenv("OPENAI_API_KEY", "unit-dummy")
    monkeypatch.setenv("SUPABASE_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "unit-dummy")
    monkeypatch.setattr(dotenv, "load_dotenv", lambda *a, **k: False)
    monkeypatch.setattr(supabase, "create_client", lambda *a, **k: _NoSupabase())
    monkeypatch.setattr(urllib.request, "urlopen", _no_network)

    before = set(sys.modules)
    module = importlib.import_module("bots.crudobot")
    monkeypatch.setattr(module, "supabase", _NoSupabase())
    yield module
    for name in set(sys.modules) - before:
        if _is_project_module(name):
            del sys.modules[name]
