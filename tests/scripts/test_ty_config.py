# ABOUTME: Pins ty's strict configuration: no baseline file or key, and warnings fail the check (#212).
# ABOUTME: Reads pyproject.toml with tomllib; nothing from scripts/ is imported.
import tomllib
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
TY = tomllib.loads((REPO / "pyproject.toml").read_text())["tool"]["ty"]


def test_ty_has_no_baseline():
    assert "baseline" not in TY, "fix the diagnostic instead of baselining it"
    assert not (REPO / "ty-baseline.json").exists(), "fix the diagnostic instead of baselining it"


def test_ty_fails_on_warnings():
    assert TY["terminal"]["error-on-warning"] is True


def test_ty_tightened_rules_are_errors():
    for name in (
        "blanket-ignore-comment",
        "division-by-zero",
        "possibly-missing-attribute",
        "possibly-missing-import",
        "possibly-unresolved-reference",
    ):
        assert TY["rules"][name] == "error", name
