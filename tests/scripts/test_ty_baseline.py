# ABOUTME: Keeps scripts/ and tests/scripts/ out of the ty baseline (#205).
# ABOUTME: #212 deletes this test together with ty-baseline.json.
import json
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def test_scripts_have_no_ty_baseline_entries():
    files = json.loads((REPO / "ty-baseline.json").read_text())["files"]
    leaked = sorted(f for f in files if f.startswith(("scripts/", "tests/scripts/")))
    assert leaked == [], f"fix these ty diagnostics instead of baselining them: {leaked}"
