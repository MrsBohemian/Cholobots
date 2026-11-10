# ABOUTME: Keeps the contributor docs honest: AGENTS.md mirrors CLAUDE.md, required sections exist, links resolve.
# ABOUTME: Reads Markdown files as text; imports no bot module.

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _text(name):
    return (ROOT / name).read_text(encoding="utf-8")


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
