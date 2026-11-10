# ABOUTME: Keeps supabase/seed.sql loadable, complete and free of real people's data.
# ABOUTME: Reads the SQL and config as text; imports no bot module and starts no containers.
import ast
import re
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

REQUIRED_TABLES = [
    "chisme_contacts",
    "chisme_active",
    "chisme_journals",
    "chisme_notes",
    "crudo_projects",
    "crudo_project_tasks",
    "crudo_labor_entries",
    "crudo_material_transactions",
    "crudo_material_transaction_items",
    "garage_zones",
    "inventory_items",
    "metiche_weekly",
    "daily_tasks",
    "obijuan_quests",
    "obijuan_quest_assignments",
    "vuelta_items",
    "vuelta_routes",
    "vuelta_interests",
]


@pytest.fixture(scope="module")
def seed():
    return (ROOT / "supabase" / "seed.sql").read_text()


def seed_code(seed):
    """The seed without `--` comment lines, so comments cannot trip the checks."""
    return "\n".join(line for line in seed.splitlines() if not line.lstrip().startswith("--"))


def production_discord_ids():
    tree = ast.parse((ROOT / "bots" / "metichebot.py").read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "DISCORD_USER_TO_PERSON" for t in node.targets
        ):
            return [str(k.value) for k in node.value.keys]
    raise AssertionError("DISCORD_USER_TO_PERSON not found in bots/metichebot.py")


def contact_names(seed):
    block = re.search(
        r"insert into public\.chisme_contacts\b.*?values(.*?);", seed_code(seed), re.S | re.I
    )
    assert block, "no insert into public.chisme_contacts"
    return re.findall(r"^\s*\(\s*'([^']*)'", block.group(1), re.M)


def test_config_loads_seed_sql():
    config = tomllib.loads((ROOT / "supabase" / "config.toml").read_text())
    assert config["db"]["seed"]["enabled"] is True
    assert "./seed.sql" in config["db"]["seed"]["sql_paths"]


@pytest.mark.parametrize("table", REQUIRED_TABLES)
def test_seed_inserts_into_every_required_table(seed, table):
    assert re.search(rf"insert\s+into\s+public\.{table}\b", seed_code(seed), re.I)


def test_seed_contact_names_are_marked_test(seed):
    names = contact_names(seed)
    assert len(names) >= 3
    for name in names:
        assert name.endswith(" (Test)"), name


def test_seed_phone_numbers_are_fictional(seed):
    for run in re.findall(r"(?<!\d)\d{10}(?!\d)", seed_code(seed)):
        assert re.fullmatch(r"\d{3}55501\d{2}", run), run


def test_seed_emails_use_reserved_domains(seed):
    emails = re.findall(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", seed_code(seed))
    assert emails
    for email in emails:
        assert email.endswith(("@example.com", "@example.org", "@example.net")), email


def test_seed_contains_no_production_discord_ids(seed):
    for discord_id in production_discord_ids():
        assert discord_id not in seed


@pytest.mark.parametrize(
    "forbidden", [r"\btruncate\b", r"\bdrop\b", r"\bdelete\s+from\b", r"supabase\.co", r"eyJ"]
)
def test_seed_has_no_destructive_or_hosted_statements(seed, forbidden):
    assert not re.search(forbidden, seed_code(seed), re.I)
