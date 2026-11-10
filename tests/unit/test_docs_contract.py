# ABOUTME: Keeps the contributor docs honest: AGENTS.md mirrors CLAUDE.md, required sections exist, links resolve.
# ABOUTME: Reads Markdown files as text; imports no bot module.

import os
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _text(name):
    return (ROOT / name).read_text(encoding="utf-8")


def _headings(name, level=2):
    prefix = "#" * level + " "
    return [line[len(prefix):].strip() for line in _text(name).splitlines() if line.startswith(prefix)]


def _relative_links(name):
    """Markdown link targets that point into the repository, without their #fragment."""
    targets = re.findall(r"\]\(([^)\s]+)\)", _text(name))
    return [
        t.split("#", 1)[0]
        for t in targets
        if not t.startswith(("http://", "https://", "mailto:", "#"))
    ]


def _intro(name):
    """Text before the first level-2 heading."""
    return _text(name).split("\n## ", 1)[0]


def test_agents_md_is_symlink_to_claude_md():
    agents = ROOT / "AGENTS.md"
    assert agents.is_symlink()
    assert os.readlink(agents) == "CLAUDE.md"
    assert agents.read_text(encoding="utf-8") == _text("CLAUDE.md")


def test_claude_md_intro_names_both_tools():
    intro = _intro("CLAUDE.md")
    assert "Codex" in intro
    assert "AGENTS.md" in intro


def test_claude_md_does_not_mention_queso():
    assert "queso" not in _text("CLAUDE.md")


# "## Ownership and license" is added by story #58 once Heaven settles it (task #122), not here.
CONTRIBUTING_SECTIONS = [
    "Access",
    "How a change gets to main",
    "Database changes",
    "Adding or renaming a command",
    "Commit attribution",
    "Keeping docs current",
    "Testing",
]


@pytest.mark.parametrize("heading", CONTRIBUTING_SECTIONS)
def test_contributing_has_required_sections(heading):
    assert heading in _headings("CONTRIBUTING.md")


def test_contributing_links_migration_doc():
    assert "docs/database-migrations.md" in _relative_links("CONTRIBUTING.md")


def test_contributing_links_merge_side_effects():
    assert ".claude/skills/shared/merge-side-effects.md" in _relative_links("CONTRIBUTING.md")


README_SECTIONS = [
    "Running the bot",
    "Resetting data",
    "Running tests",
    "Work tracking",
    "Contributing",
]


def test_readme_has_setup_section():
    headings = _headings("README.md")
    assert "Quickstart" in headings or "Local development" in headings


@pytest.mark.parametrize("heading", README_SECTIONS)
def test_readme_has_required_sections(heading):
    assert heading in _headings("README.md")


def test_readme_links_contributing():
    assert "CONTRIBUTING.md" in _relative_links("README.md")


@pytest.mark.parametrize("name", ["README.md", "CONTRIBUTING.md", "CLAUDE.md"])
def test_relative_links_resolve(name):
    missing = [t for t in _relative_links(name) if t and not (ROOT / name).parent.joinpath(t).exists()]
    assert missing == []
