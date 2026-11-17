# ABOUTME: Keeps pyproject.toml, uv.lock, requirements.txt and the pre-commit ruff rev consistent.
# ABOUTME: Reads repo files with tomllib and pathlib only; imports no bot module.
import re
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
DEV_TOOLS = ("pytest", "pytest-asyncio", "psycopg", "ruff", "pre-commit", "ty")


def _pyproject():
    return tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))


def _normalise(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def _requirement_name(spec):
    return _normalise(re.split(r"[\s=<>!~;\[]", spec.strip(), maxsplit=1)[0])


def test_requires_python_matches_python_version():
    version = (ROOT / ".python-version").read_text(encoding="utf-8").strip()
    major, minor = (int(part) for part in version.split(".")[:2])
    assert _pyproject()["project"]["requires-python"] == f">={major}.{minor},<{major}.{minor + 1}"


def test_pre_commit_ruff_rev_matches_locked_ruff():
    config = (ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8")
    block = config.split("astral-sh/ruff-pre-commit", 1)[1]
    match = re.search(r"rev:\s*(v\S+)", block)
    assert match, "no rev found for astral-sh/ruff-pre-commit in .pre-commit-config.yaml"
    rev = match.group(1)
    lock = tomllib.loads((ROOT / "uv.lock").read_text(encoding="utf-8"))
    locked = [p["version"] for p in lock["package"] if p["name"] == "ruff"]
    assert locked == [rev.removeprefix("v")]


@pytest.mark.parametrize("tool", DEV_TOOLS)
def test_dev_tool_is_not_a_runtime_dependency(tool):
    runtime = {_requirement_name(spec) for spec in _pyproject()["project"]["dependencies"]}
    assert tool not in runtime


@pytest.mark.parametrize("tool", DEV_TOOLS)
def test_dev_tool_is_not_exported_to_requirements_txt(tool):
    exported = {
        _requirement_name(line)
        for line in (ROOT / "requirements.txt").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith(("#", " "))
    }
    assert tool not in exported


def test_project_is_not_packaged():
    assert _pyproject()["tool"]["uv"]["package"] is False
