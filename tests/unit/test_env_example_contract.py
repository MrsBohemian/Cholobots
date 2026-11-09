# ABOUTME: Keeps .env.example in step with the environment variables the bot code reads.
# ABOUTME: Parses source with ast; imports no bot module, so no env or Supabase setup is needed.
import ast
import re
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
ENV_EXAMPLE = ROOT / ".env.example"
KEY_LINE = re.compile(r"^\s*(#\s*)?([A-Z][A-Z0-9_]*)=(.*)$")


def _source_files():
    files = [ROOT / "cholobots.py", ROOT / "config.py"]
    files += sorted((ROOT / "db").glob("*.py"))
    files += sorted((ROOT / "bots").glob("*.py"))
    return files


def _is_os_environ(node):
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "environ"
        and isinstance(node.value, ast.Name)
        and node.value.id == "os"
    )


def _first_string_arg(call):
    if call.args and isinstance(call.args[0], ast.Constant) and isinstance(call.args[0].value, str):
        return call.args[0].value
    return None


def code_env_names():
    names = set()
    for path in _source_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                func = node.func
                is_getenv = (
                    func.attr == "getenv"
                    and isinstance(func.value, ast.Name)
                    and func.value.id == "os"
                )
                is_environ_get = func.attr == "get" and _is_os_environ(func.value)
                if is_getenv or is_environ_get:
                    name = _first_string_arg(node)
                    if name:
                        names.add(name)
            elif isinstance(node, ast.Subscript) and _is_os_environ(node.value):
                key = node.slice
                if isinstance(key, ast.Constant) and isinstance(key.value, str):
                    names.add(key.value)
    return names


def example_entries():
    """Map key -> (value, commented, preceding_line) for every KEY= line."""
    entries = {}
    lines = ENV_EXAMPLE.read_text(encoding="utf-8").splitlines()
    for index, line in enumerate(lines):
        match = KEY_LINE.match(line)
        if not match:
            continue
        commented = bool(match.group(1))
        preceding = lines[index - 1] if index > 0 else ""
        entries[match.group(2)] = (match.group(3).strip(), commented, preceding)
    return entries


def example_keys():
    return set(example_entries())


def local_api_port():
    config = ROOT / "supabase" / "config.toml"
    if not config.exists():
        return 54321
    data = tomllib.loads(config.read_text(encoding="utf-8"))
    return data.get("api", {}).get("port", 54321)


def test_env_example_lists_every_variable_the_code_reads():
    assert code_env_names() <= example_keys()


def test_env_example_lists_only_variables_the_code_reads():
    assert example_keys() <= code_env_names()


def test_env_example_explains_every_variable():
    for key, (_value, _commented, preceding) in example_entries().items():
        assert preceding.lstrip().startswith("#"), f"{key} has no comment line above it"
        assert not KEY_LINE.match(preceding), f"{key} is preceded by another key line"


@pytest.mark.parametrize("key", ["DISCORD_TOKEN", "OPENAI_API_KEY", "SUPABASE_SERVICE_ROLE_KEY"])
def test_env_example_leaves_secrets_empty(key):
    value, commented, _ = example_entries()[key]
    assert not commented
    assert value == ""


def test_env_example_transportation_channel_id_is_an_integer():
    value, commented, _ = example_entries()["TRANSPORTATION_CHANNEL_ID"]
    assert not commented
    int(value)


@pytest.mark.parametrize(
    "key", ["DANIEL_MORNING_AUDIO_URL", "CRUDOBOT_RECEIPT_MODEL", "SUPABASE_KEY"]
)
def test_env_example_keeps_code_defaults_commented_out(key):
    _value, commented, _ = example_entries()[key]
    assert commented


def test_env_example_points_supabase_at_the_local_stack():
    value, commented, _ = example_entries()["SUPABASE_URL"]
    assert not commented
    assert value == f"http://127.0.0.1:{local_api_port()}"
