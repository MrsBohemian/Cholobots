# ABOUTME: Pins pyrefly's informational configuration: aligned with [tool.ty], no baseline, no interpreter path (#268).
# ABOUTME: Reads pyproject.toml with tomllib; nothing from scripts/ is imported.
import tomllib
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
CONFIG = tomllib.loads((REPO / "pyproject.toml").read_text())["tool"]
PYREFLY, TY_ENV = CONFIG["pyrefly"], CONFIG["ty"]["environment"]


def test_python_version_and_platform_match_ty():
    assert PYREFLY["python-version"] == TY_ENV["python-version"]
    assert PYREFLY["python-platform"] == TY_ENV["python-platform"]


def test_search_path_covers_ty_extra_paths_and_test_directories():
    search_path = PYREFLY["search-path"]
    assert search_path[0] == "."
    assert set(TY_ENV["extra-paths"]) | {"tests", "tests/scripts"} <= set(search_path)


def test_project_includes_every_python_file():
    assert PYREFLY["project-includes"] == ["**/*.py"]


def test_pyrefly_finds_the_venv_itself():
    assert "python-interpreter-path" not in PYREFLY


def test_pyrefly_has_no_baseline():
    assert "baseline" not in PYREFLY, "a baseline would hide errors from the informational count"
