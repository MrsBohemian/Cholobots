# ABOUTME: Pytest tests for scripts/epic_dependency_diagram.py, which merges, renders and validates an epic's dependency diagram.
# ABOUTME: Round-trips every rendered diagram through the shared parser and drives the CLI via subprocess.
import json
import os
import subprocess
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
SCRIPT = os.path.join(REPO_ROOT, "scripts", "epic_dependency_diagram.py")
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts"))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts", "lib"))

from dependency_parser import parse_dependency_diagram  # noqa: E402
from epic_dependency_diagram import build_description, description_fallback  # noqa: E402


def story(ref, subject=None, in_epic=True):
    return {"ref": ref, "subject": subject or f"Story {ref}", "inEpic": in_epic}


def dep(ref, depends_on, reason="because"):
    return {"ref": ref, "dependsOn": depends_on, "reason": reason}


def build(description, stories, deps=(), soft=()):
    return build_description(description, list(stories), list(deps), list(soft))


def run_cli(args):
    return subprocess.run([sys.executable, SCRIPT, *args], capture_output=True, text=True)


def write_json(path, data):
    path.write_text(json.dumps(data))
    return str(path)


class TestRendering:
    def test_render_round_trips_through_parser(self):
        stories = [story(1), story(2), story(3), story(4), story(5), story(9)]
        deps = [dep(2, 1), dep(3, 2), dep(4, 1), dep(5, 4)]
        result = build("Epic text", stories, deps)
        assert result["status"] == "written"
        assert parse_dependency_diagram(result["description"]) == {
            1: [], 2: [1], 3: [2], 4: [1], 5: [4], 9: [],
        }

    def test_multi_parent_node_appears_under_each_parent(self):
        stories = [story(1), story(2), story(3), story(4)]
        deps = [dep(3, 1), dep(3, 2), dep(4, 3)]
        result = build("", stories, deps)
        parsed = parse_dependency_diagram(result["description"])
        assert parsed[3] == [1, 2]
        assert parsed[4] == [3]
        # The subtree of #3 is expanded once; the second appearance is a pointer.
        assert result["diagram"].count("#4") == 1
        assert "(see above)" in result["diagram"]

    def test_deeper_levels_indent_six_columns(self):
        result = build("", [story(1), story(2), story(3)], [dep(2, 1), dep(3, 2)])
        lines = result["diagram"].splitlines()
        assert lines[1].startswith("  └─► #2")
        assert lines[2].startswith("        └─► #3")

    def test_out_of_epic_dep_is_a_root_marked_outside_epic(self):
        stories = [story(48), story(31, "Test Discord server", in_epic=False)]
        result = build("", stories, [dep(48, 31)])
        assert "#31 (Test Discord server) [outside epic]" in result["diagram"]
        assert result["outOfEpic"] == [31]
        assert parse_dependency_diagram(result["description"]) == {31: [], 48: [31]}

    def test_every_in_epic_story_appears(self):
        result = build("", [story(1), story(2), story(7)], [dep(2, 1)])
        assert 7 in parse_dependency_diagram(result["description"])

    def test_backticks_and_newlines_are_stripped_from_labels(self):
        result = build("", [story(1, "Fix `x`\nnow"), story(2)], [dep(2, 1, "needs\n`y`")])
        assert "`" not in result["diagram"]
        assert parse_dependency_diagram(result["description"]) == {1: [], 2: [1]}

    def test_reason_is_rendered_on_child_line(self):
        result = build("", [story(1), story(2)], [dep(2, 1, "seeds load into it")])
        assert "└─► #2 (Story 2): seeds load into it" in result["diagram"]


class TestMerge:
    EXISTING = (
        "Intro prose.\n\n## Dependency Diagram\n\n```\n#10 (Base)\n  └─► #11 (Next)\n```\n\n"
        "## Notes\n\nKeep this.\n"
    )

    def test_merges_with_existing_diagram_without_dropping_edges(self):
        stories = [story(10, "Base"), story(11, "Next"), story(12, "New")]
        result = build(self.EXISTING, stories, [dep(12, 11)])
        assert result["status"] == "written"
        assert parse_dependency_diagram(result["description"]) == {10: [], 11: [10], 12: [11]}
        assert result["added"] == [[12, 11]]
        assert result["description"].startswith("Intro prose.\n\n## Dependency Diagram\n")
        assert result["description"].endswith("## Notes\n\nKeep this.\n")

    def test_existing_subjects_are_reused_as_labels(self):
        stories = [{"ref": 10, "inEpic": True}, {"ref": 11, "inEpic": True}, story(12, "New")]
        result = build(self.EXISTING, stories, [dep(12, 11)])
        assert "#10 (Base)" in result["diagram"]

    def test_unchanged_when_graph_already_matches(self):
        stories = [story(10, "Base"), story(11, "Next")]
        result = build(self.EXISTING, stories, [dep(11, 10)])
        assert result["status"] == "unchanged"
        assert result["description"] == self.EXISTING

    def test_appends_section_when_missing(self):
        result = build("Just prose.", [story(1), story(2)], [dep(2, 1)])
        assert result["description"].startswith("Just prose.\n\n## Dependency Diagram\n\n```\n#1")
        assert result["description"].endswith("```\n")

    def test_inserts_code_block_when_section_has_none(self):
        existing = "## Dependency Diagram\n\nTBD\n\n## Other\n\nx\n"
        result = build(existing, [story(1), story(2)], [dep(2, 1)])
        assert parse_dependency_diagram(result["description"]) == {1: [], 2: [1]}
        assert "TBD" in result["description"]
        assert result["description"].endswith("## Other\n\nx\n")


class TestSoftDeps:
    def test_soft_deps_listed_but_not_edges(self):
        soft = [{"ref": 3, "after": 2, "reason": "reuses fixtures"}]
        result = build("", [story(2), story(3)], [], soft)
        assert "Soft orderings (not enforced):" in result["description"]
        assert "- #3 after #2: reuses fixtures" in result["description"]
        assert parse_dependency_diagram(result["description"]) == {2: [], 3: []}
        assert result["softDeps"] == [[3, 2]]

    def test_soft_deps_union_with_existing_bullets(self):
        stories = [story(2), story(3), story(4)]
        first = build("", stories, [], [{"ref": 3, "after": 2, "reason": "a"}])
        second = build(first["description"], stories, [], [{"ref": 4, "after": 3, "reason": "b"}])
        assert second["status"] == "written"
        assert "- #3 after #2: a" in second["description"]
        assert "- #4 after #3: b" in second["description"]
        assert second["description"].count("Soft orderings (not enforced):") == 1

    def test_same_soft_deps_are_unchanged(self):
        soft = [{"ref": 3, "after": 2, "reason": "a"}]
        first = build("", [story(2), story(3)], [], soft)
        again = build(first["description"], [story(2), story(3)], [], soft)
        assert again["status"] == "unchanged"
        assert again["description"] == first["description"]

    def test_changed_soft_reason_is_written(self):
        """A soft ordering whose reason alone changed is a change, not `unchanged` (PR #6 review)."""
        stories = [story(2), story(3)]
        first = build("", stories, [], [{"ref": 3, "after": 2, "reason": "old reason"}])
        again = build(first["description"], stories, [], [{"ref": 3, "after": 2, "reason": "new reason"}])
        assert again["status"] == "written"
        assert "- #3 after #2: new reason" in again["description"]
        assert "old reason" not in again["description"]


class TestValidation:
    def test_cycle_is_invalid_and_named(self):
        result = build("", [story(1), story(2)], [dep(1, 2), dep(2, 1)])
        assert result["status"] == "invalid"
        assert any("#1 -> #2 -> #1" in e for e in result["errors"])

    def test_cycle_through_existing_edge_is_invalid(self):
        existing = "## Dependency Diagram\n\n```\n#1\n  └─► #2\n```\n"
        result = build(existing, [story(1), story(2)], [dep(1, 2)])
        assert result["status"] == "invalid"

    def test_self_dependency_is_invalid(self):
        result = build("", [story(1)], [dep(1, 1)])
        assert result["status"] == "invalid"
        assert any("#1" in e and "itself" in e for e in result["errors"])

    def test_soft_self_ordering_is_invalid(self):
        result = build("", [story(1)], [], [{"ref": 1, "after": 1, "reason": "x"}])
        assert result["status"] == "invalid"
        assert any("#1" in e and "after itself" in e for e in result["errors"])


class TestCli:
    def files(self, tmp_path, deps, description="Prose."):
        epic = write_json(tmp_path / "epic.json", {"version": 4, "description": description})
        depf = write_json(tmp_path / "deps.json", {
            "epicRef": 46,
            "stories": [story(1), story(2)],
            "deps": deps,
            "softDeps": [],
        })
        return epic, depf

    def test_cli_writes_out_file_and_prints_json(self, tmp_path):
        epic, depf = self.files(tmp_path, [dep(2, 1, "needs it")])
        out = tmp_path / "out.md"
        result = run_cli(["--epic-json", epic, "--deps", depf, "--out", str(out)])
        assert result.returncode == 0
        payload = json.loads(result.stdout)
        assert payload["status"] == "written"
        assert payload["graph"] == {"1": [], "2": [1]}
        assert parse_dependency_diagram(out.read_text()) == {1: [], 2: [1]}

    def test_cycle_exits_2_and_writes_nothing(self, tmp_path):
        epic, depf = self.files(tmp_path, [dep(1, 2), dep(2, 1)])
        out = tmp_path / "out.md"
        result = run_cli(["--epic-json", epic, "--deps", depf, "--out", str(out)])
        assert result.returncode == 2
        assert not out.exists()
        payload = json.loads(result.stdout)
        assert payload["status"] == "invalid"
        assert any("#1 -> #2 -> #1" in e for e in payload["errors"])

    def test_cli_bad_json_exits_1(self, tmp_path):
        bad = tmp_path / "bad.json"
        bad.write_text("{not json")
        _, depf = self.files(tmp_path, [])
        out = tmp_path / "o.md"
        result = run_cli(["--epic-json", str(bad), "--deps", depf, "--out", str(out)])
        assert result.returncode == 1
        assert f"ERROR: cannot read {bad}" in result.stderr
        assert not out.exists()

    def test_cli_missing_file_exits_1(self, tmp_path):
        _, depf = self.files(tmp_path, [])
        missing = tmp_path / "nope.json"
        out = tmp_path / "o.md"
        result = run_cli(["--epic-json", str(missing), "--deps", depf, "--out", str(out)])
        assert result.returncode == 1
        assert f"ERROR: cannot read {missing}" in result.stderr
        assert not out.exists()

    def test_cli_malformed_dep_exits_1(self, tmp_path):
        epic, depf = self.files(tmp_path, [{"ref": 2}])
        out = tmp_path / "o.md"
        result = run_cli(["--epic-json", epic, "--deps", depf, "--out", str(out)])
        assert result.returncode == 1
        assert "ERROR: malformed input" in result.stderr
        assert "dependsOn" in result.stderr
        assert not out.exists()

    def test_unchanged_out_is_original_description(self, tmp_path):
        description = "## Dependency Diagram\n\n```\n#1 (Story 1)\n  └─► #2 (Story 2)\n```\n"
        epic, depf = self.files(tmp_path, [dep(2, 1)], description)
        out = tmp_path / "out.md"
        result = run_cli(["--epic-json", epic, "--deps", depf, "--out", str(out)])
        assert result.returncode == 0
        assert json.loads(result.stdout)["status"] == "unchanged"
        assert out.read_text() == description


class TestDescriptionFallback:
    """Which dependency categories must be read from a story's description (PR #6 review)."""

    def test_mixed_none_and_unknown_falls_back_for_the_unknown_side_only(self):
        state = [
            {"ref": 47, "upstreamDeps": [], "softDeps": None},
            {"ref": 48, "upstreamDeps": None, "softDeps": []},
        ]
        assert description_fallback(state, [47, 48]) == {47: ["soft"], 48: ["upstream"]}

    def test_fully_reported_story_needs_no_fallback(self):
        state = [{"ref": 47, "upstreamDeps": [{"ref": 31, "reason": "x"}], "softDeps": []}]
        assert description_fallback(state, [47]) == {}

    def test_in_epic_story_missing_from_state_falls_back_for_both(self):
        assert description_fallback([], [50]) == {50: ["upstream", "soft"]}

    def test_out_of_epic_state_story_is_ignored(self):
        state = [{"ref": 99, "upstreamDeps": None, "softDeps": None}]
        assert description_fallback(state, [50]) == {50: ["upstream", "soft"]}

    def test_cli_prints_fallback_map(self, tmp_path):
        state = write_json(tmp_path / "state.json", {"stories": [
            {"ref": 47, "upstreamDeps": [], "softDeps": None},
        ]})
        result = run_cli(["--fallback-state", state, "--epic-refs", "47,48"])
        assert result.returncode == 0
        assert json.loads(result.stdout) == {"47": ["soft"], "48": ["upstream", "soft"]}

    def test_cli_rejects_mixed_modes(self, tmp_path):
        state = write_json(tmp_path / "state.json", {"stories": []})
        result = run_cli(["--fallback-state", state, "--epic-refs", "1", "--out", str(tmp_path / "o.md")])
        assert result.returncode == 1
