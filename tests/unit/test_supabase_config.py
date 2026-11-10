# ABOUTME: Keeps supabase/config.toml running the local services the bot and dashboards rely on.
# ABOUTME: Reads the TOML with tomllib; imports no bot module and starts no containers.
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONFIG = tomllib.loads((ROOT / "supabase" / "config.toml").read_text())


def test_config_names_the_project():
    assert CONFIG["project_id"] == "cholobots"


def test_local_stack_enables_the_services_the_bot_and_dashboards_use():
    for service in ("api", "auth", "storage", "studio"):
        assert CONFIG[service]["enabled"] is True, service


def test_api_serves_the_public_schema_on_the_default_port():
    assert "public" in CONFIG["api"]["schemas"]
    assert CONFIG["api"]["port"] == 54321


def test_supabase_gitignore_keeps_link_state_and_secrets_out():
    lines = (ROOT / "supabase" / ".gitignore").read_text().splitlines()
    assert any(line.rstrip("/") == ".temp" for line in lines)
    assert any(line.startswith(".env") for line in lines)
