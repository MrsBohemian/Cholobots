# ABOUTME: Pins the local ty hook in .pre-commit-config.yaml (text-based; PyYAML is not a dependency).
# ABOUTME: The hook must use the locked env and run on Python and pyproject changes only.
import re
from pathlib import Path

CONFIG = (Path(__file__).resolve().parents[2] / ".pre-commit-config.yaml").read_text()


def ty_hook_block():
    assert "- repo: local" in CONFIG
    local = CONFIG.split("- repo: local", 1)[1]
    assert "id: ty" in local
    return local


def test_ty_hook_is_local_and_uses_the_locked_env():
    block = ty_hook_block()
    assert "entry: uv run --frozen ty check" in block
    assert "language: system" in block
    assert "pass_filenames: false" in block


def test_ty_hook_runs_on_python_and_config_changes():
    match = re.search(r"^\s*files: '(.+)'\s*$", ty_hook_block(), re.MULTILINE)
    assert match, "ty hook has no files: regex"
    pattern = match.group(1)
    for path in ("bots/crudobot.py", "pyproject.toml"):
        assert re.search(pattern, path), path
    for path in ("README.md", "supabase/seed.sql"):
        assert not re.search(pattern, path), path


def test_ty_hook_has_no_baseline():
    assert "baseline" not in ty_hook_block()


def test_no_pyrefly_hook():
    assert "pyrefly" not in CONFIG, "pyrefly is informational; a hook would block commits"
