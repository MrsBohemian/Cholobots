# ABOUTME: Keeps supabase/config.toml running the local services the bot and dashboards rely on.
# ABOUTME: Reads the TOML with tomllib; imports no bot module and starts no containers.
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def config():
    return tomllib.loads((ROOT / "supabase" / "config.toml").read_text())


def test_config_names_the_project(config):
    assert config["project_id"] == "cholobots"


@pytest.mark.parametrize("service", ["api", "auth", "storage", "studio"])
def test_local_stack_enables_the_services_the_bot_and_dashboards_use(config, service):
    assert config[service]["enabled"] is True


def test_local_stack_disables_edge_functions(config):
    # With no functions in the repo the edge runtime health check 404s and `supabase start` aborts.
    assert config["edge_runtime"]["enabled"] is False


def test_api_serves_the_public_schema_on_the_default_port(config):
    assert "public" in config["api"]["schemas"]
    assert config["api"]["port"] == 54321


def test_supabase_gitignore_keeps_link_state_and_secrets_out():
    lines = (ROOT / "supabase" / ".gitignore").read_text().splitlines()
    assert any(line.rstrip("/") == ".temp" for line in lines)
    assert any(line.startswith(".env") for line in lines)
