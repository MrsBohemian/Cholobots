# ABOUTME: Pytest tests for the merge-train.py script.
# ABOUTME: Verifies argument parsing, state management, PR queue building, dependency ordering, exit codes, and summary.
import json
import os
import sys
import tempfile

import pytest

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts"))

from merge_train import (
    parse_args,
    gh_cmd,
    has_unit_tests,
    run_build_checks,
    StateManager,
    extract_taiga_ref,
    match_pr_to_story_ref,
    order_prs_by_dependencies,
    discover_epic_prs,
    generate_summary,
    write_action_request,
    action_path_for,
    write_build_log,
    BUILD_LOG_TAIL_LINES,
    check_taiga_status,
    process_pr,
    decide_retarget,
    find_open_children,
    OPEN_CHILDREN_LIMIT,
    restore_children_base,
    branch_merged_on_remote,
    branch_contained_in_target,
    auto_merge_state,
    fail_pr_and_restore_children,
    EXIT_OK,
    EXIT_CONFLICTS,
    EXIT_REMEDIATION,
    EXIT_ERROR,
)


class TestParseArgsExplicitPrs:
    """Parses space-separated PR numbers."""

    def test_single_pr(self):
        args = parse_args(["123"])
        assert args.mode == "explicit"
        assert args.pr_numbers == [123]

    def test_multiple_prs(self):
        args = parse_args(["123", "456", "789"])
        assert args.mode == "explicit"
        assert args.pr_numbers == [123, 456, 789]


class TestParseArgsEpicMode:
    """Parses epic:<ref> with optional flags."""

    def test_epic_ref(self):
        args = parse_args(["epic:710"])
        assert args.mode == "epic"
        assert args.epic_ref == 710

    def test_epic_ref_with_flags(self):
        args = parse_args(["epic:710", "--dry-run", "--target", "develop"])
        assert args.mode == "epic"
        assert args.epic_ref == 710
        assert args.dry_run is True
        assert args.target == "develop"


class TestParseArgsFlags:
    """--dry-run, --target, --resume parsed correctly."""

    def test_dry_run(self):
        args = parse_args(["123", "--dry-run"])
        assert args.dry_run is True

    def test_target(self):
        args = parse_args(["123", "--target", "develop"])
        assert args.target == "develop"

    def test_target_default(self):
        args = parse_args(["123"])
        assert args.target == "main"

    def test_resume(self):
        args = parse_args(["--resume", "/tmp/merge-train-123.json"])
        assert args.mode == "resume"
        assert args.resume_path == "/tmp/merge-train-123.json"

    def test_resume_ignores_pr_numbers(self):
        """When --resume is present, PR numbers are ignored."""
        args = parse_args(["--resume", "/tmp/state.json", "123", "456"])
        assert args.mode == "resume"
        assert args.resume_path == "/tmp/state.json"


class TestStateFileInit:
    """State file created with correct structure."""

    def test_init_creates_valid_state(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            state_path = f.name
        try:
            sm = StateManager(state_path)
            prs = [
                {"number": 123, "title": "PR one", "branch": "feature/one"},
                {"number": 456, "title": "PR two", "branch": "feature/two"},
            ]
            sm.init(prs, target="main")
            state = sm.state

            assert state["target"] == "main"
            assert len(state["prs"]) == 2
            assert state["prs"][0]["number"] == 123
            assert state["prs"][0]["status"] == "queued"
            assert state["prs"][0]["error"] is None
            assert state["currentIndex"] == 0
            assert state["summary"]["merged"] == 0
            assert state["summary"]["skipped"] == 0
            assert state["summary"]["failed"] == 0
            assert "startedAt" in state
        finally:
            os.unlink(state_path)

    def test_init_seeds_base_from_pr_dict(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            state_path = f.name
        try:
            sm = StateManager(state_path)
            prs = [
                {"number": 123, "title": "PR one", "branch": "feature/one", "base": "feature/parent"},
            ]
            sm.init(prs, target="main")
            assert sm.state["prs"][0]["base"] == "feature/parent"
            assert sm.state["prs"][0]["stacked"] is True
        finally:
            os.unlink(state_path)

    def test_init_falls_back_to_target_when_base_absent(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            state_path = f.name
        try:
            sm = StateManager(state_path)
            prs = [{"number": 123, "title": "PR one", "branch": "feature/one"}]
            sm.init(prs, target="main")
            assert sm.state["prs"][0]["base"] == "main"
            assert sm.state["prs"][0]["stacked"] is False
        finally:
            os.unlink(state_path)

    def test_init_stacked_false_when_base_equals_target(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            state_path = f.name
        try:
            sm = StateManager(state_path)
            prs = [{"number": 123, "title": "PR one", "branch": "feature/one", "base": "main"}]
            sm.init(prs, target="main")
            assert sm.state["prs"][0]["base"] == "main"
            assert sm.state["prs"][0]["stacked"] is False
        finally:
            os.unlink(state_path)

    def test_init_creates_empty_merged_branches(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            state_path = f.name
        try:
            sm = StateManager(state_path)
            sm.init([{"number": 123, "title": "T", "branch": "b"}], target="main")
            assert sm.state["mergedBranches"] == []
        finally:
            os.unlink(state_path)


class TestStateFileResume:
    """State loaded and processing continues from currentIndex."""

    def test_resume_loads_existing_state(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            state = {
                "startedAt": "2026-03-30T00:00:00Z",
                "target": "main",
                "prs": [
                    {"number": 100, "title": "Done", "branch": "b1", "status": "merged", "error": None},
                    {"number": 200, "title": "Next", "branch": "b2", "status": "queued", "error": None},
                ],
                "currentIndex": 1,
                "summary": {"merged": 1, "skipped": 0, "failed": 0},
            }
            json.dump(state, f)
            state_path = f.name
        try:
            sm = StateManager(state_path)
            sm.load()
            assert sm.state["currentIndex"] == 1
            assert sm.state["prs"][0]["status"] == "merged"
            assert sm.state["prs"][1]["status"] == "queued"
        finally:
            os.unlink(state_path)

    def test_resume_tolerates_missing_merged_branches(self):
        """A state file that omits mergedBranches loads, and the key defaults to []."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            state = {
                "startedAt": "2026-03-30T00:00:00Z",
                "target": "main",
                "prs": [
                    {"number": 100, "title": "Done", "branch": "b1", "status": "merged", "error": None},
                ],
                "currentIndex": 0,
                "summary": {"merged": 0, "skipped": 0, "failed": 0},
            }
            json.dump(state, f)
            state_path = f.name
        try:
            sm = StateManager(state_path)
            sm.load()
            assert sm.state["mergedBranches"] == []
            assert sm.state["prs"][0].get("stacked") is None
        finally:
            os.unlink(state_path)


class TestStateFileUpdate:
    """PR status transitions recorded correctly."""

    def test_update_pr_status(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            state_path = f.name
        try:
            sm = StateManager(state_path)
            sm.init(
                [{"number": 123, "title": "T", "branch": "b"}],
                target="main",
            )
            sm.update_pr(0, status="in_progress")
            assert sm.state["prs"][0]["status"] == "in_progress"

            sm.update_pr(0, status="merged")
            sm.advance()
            assert sm.state["prs"][0]["status"] == "merged"
            assert sm.state["currentIndex"] == 1
            assert sm.state["summary"]["merged"] == 1

            # Verify persisted to disk
            with open(state_path) as f:
                persisted = json.load(f)
            assert persisted["prs"][0]["status"] == "merged"
        finally:
            os.unlink(state_path)

    def test_update_pr_with_error(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            state_path = f.name
        try:
            sm = StateManager(state_path)
            sm.init(
                [{"number": 123, "title": "T", "branch": "b"}],
                target="main",
            )
            sm.update_pr(0, status="failed", error="CI failing: build")
            sm.advance()
            assert sm.state["prs"][0]["status"] == "failed"
            assert sm.state["prs"][0]["error"] == "CI failing: build"
            assert sm.state["summary"]["failed"] == 1
        finally:
            os.unlink(state_path)

    def test_update_pr_with_base(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            state_path = f.name
        try:
            sm = StateManager(state_path)
            sm.init([{"number": 123, "title": "T", "branch": "b"}], target="main")
            sm.update_pr(0, base="main")
            assert sm.state["prs"][0]["base"] == "main"

            with open(state_path) as f:
                persisted = json.load(f)
            assert persisted["prs"][0]["base"] == "main"
        finally:
            os.unlink(state_path)

    def test_update_pr_with_stacked(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            state_path = f.name
        try:
            sm = StateManager(state_path)
            sm.init([{"number": 123, "title": "T", "branch": "b"}], target="main")
            sm.update_pr(0, stacked=True)
            assert sm.state["prs"][0]["stacked"] is True

            with open(state_path) as f:
                persisted = json.load(f)
            assert persisted["prs"][0]["stacked"] is True
        finally:
            os.unlink(state_path)


class TestRecordMergedBranch:
    """record_merged_branch appends, is idempotent, and ignores empty branches."""

    def test_appends_branch(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            state_path = f.name
        try:
            sm = StateManager(state_path)
            sm.init([{"number": 123, "title": "T", "branch": "b"}], target="main")
            sm.record_merged_branch("feature/one")
            assert sm.state["mergedBranches"] == ["feature/one"]

            with open(state_path) as f:
                persisted = json.load(f)
            assert persisted["mergedBranches"] == ["feature/one"]
        finally:
            os.unlink(state_path)

    def test_idempotent_for_repeat(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            state_path = f.name
        try:
            sm = StateManager(state_path)
            sm.init([{"number": 123, "title": "T", "branch": "b"}], target="main")
            sm.record_merged_branch("feature/one")
            sm.record_merged_branch("feature/one")
            assert sm.state["mergedBranches"] == ["feature/one"]
        finally:
            os.unlink(state_path)

    def test_ignores_empty_branch(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            state_path = f.name
        try:
            sm = StateManager(state_path)
            sm.init([{"number": 123, "title": "T", "branch": "b"}], target="main")
            sm.record_merged_branch("")
            sm.record_merged_branch(None)
            assert sm.state["mergedBranches"] == []
        finally:
            os.unlink(state_path)


class TestPrRefMatching:
    """Story ref #12 matches distinct tokens, not #312."""

    def test_matches_ref_in_branch(self):
        assert match_pr_to_story_ref(
            {"headRefName": "feature/foo-12", "title": "Some PR", "body": ""},
            12,
        )

    def test_no_match_embedded_in_number(self):
        assert not match_pr_to_story_ref(
            {"headRefName": "feature/foo-312", "title": "Some PR", "body": ""},
            12,
        )

    def test_matches_ref_in_title(self):
        assert match_pr_to_story_ref(
            {"headRefName": "some-branch", "title": "Fix #12 something", "body": ""},
            12,
        )

    def test_matches_ref_in_body(self):
        assert match_pr_to_story_ref(
            {"headRefName": "some-branch", "title": "Some PR", "body": "Closes #12"},
            12,
        )

    def test_matches_ref_at_start_of_branch(self):
        assert match_pr_to_story_ref(
            {"headRefName": "12-feature", "title": "", "body": ""},
            12,
        )

    def test_matches_ref_at_end_of_branch(self):
        assert match_pr_to_story_ref(
            {"headRefName": "feature-12", "title": "", "body": ""},
            12,
        )

    def test_no_match_different_ref(self):
        assert not match_pr_to_story_ref(
            {"headRefName": "feature/foo-99", "title": "PR #99", "body": ""},
            12,
        )


class TestDependencyOrdering:
    """PRs reordered based on dependency diagram."""

    def test_reorders_by_deps(self):
        prs = [
            {"number": 3, "title": "Child", "branch": "b", "_story_ref": 200},
            {"number": 1, "title": "Parent", "branch": "a", "_story_ref": 100},
        ]
        deps = {100: [], 200: [100]}
        ordered = order_prs_by_dependencies(prs, deps)
        assert ordered[0]["_story_ref"] == 100
        assert ordered[1]["_story_ref"] == 200

    def test_no_deps_preserves_order(self):
        prs = [
            {"number": 2, "title": "B", "branch": "b", "_story_ref": 200},
            {"number": 1, "title": "A", "branch": "a", "_story_ref": 100},
        ]
        ordered = order_prs_by_dependencies(prs, {})
        # Without deps, fall back to ref-number order
        assert ordered[0]["_story_ref"] == 100
        assert ordered[1]["_story_ref"] == 200

    def test_circular_deps_logs_warning(self, capsys):
        prs = [
            {"number": 1, "title": "A", "branch": "a", "_story_ref": 100},
            {"number": 2, "title": "B", "branch": "b", "_story_ref": 200},
        ]
        deps = {100: [200], 200: [100]}
        ordered = order_prs_by_dependencies(prs, deps)
        assert {pr["_story_ref"] for pr in ordered} == {100, 200}
        captured = capsys.readouterr()
        assert "circular" in captured.err.lower()
        assert "100" in captured.err and "200" in captured.err

    def test_integration_pr_orders_after_parents_and_before_child(self):
        prs = [
            {"number": 4, "title": "Child", "headRefName": "feature/x-2748", "_story_ref": 2748},
            {
                "number": 3,
                "title": "Integration",
                "headRefName": "epic/2004-integration-2748",
                "_story_ref": 2748,
            },
            {"number": 2, "title": "Parent B", "headRefName": "b", "_story_ref": 2750},
            {"number": 1, "title": "Parent A", "headRefName": "a", "_story_ref": 2747},
        ]
        deps = {2748: [2747, 2750]}
        ordered = order_prs_by_dependencies(prs, deps)
        ordered_branches = [pr["headRefName"] for pr in ordered]
        assert ordered_branches.index("a") < ordered_branches.index(
            "epic/2004-integration-2748"
        )
        assert ordered_branches.index("b") < ordered_branches.index(
            "epic/2004-integration-2748"
        )
        assert ordered_branches.index("epic/2004-integration-2748") < ordered_branches.index(
            "feature/x-2748"
        )

    def test_integration_pr_orders_before_child_without_deps(self):
        prs = [
            {"number": 2, "title": "Child", "headRefName": "feature/x-2748", "_story_ref": 2748},
            {
                "number": 1,
                "title": "Integration",
                "headRefName": "epic/2004-integration-2748",
                "_story_ref": 2748,
            },
        ]
        ordered = order_prs_by_dependencies(prs, {})
        ordered_branches = [pr["headRefName"] for pr in ordered]
        assert ordered_branches.index("epic/2004-integration-2748") < ordered_branches.index(
            "feature/x-2748"
        )

    def test_single_parent_story_has_no_integration_node(self):
        prs = [
            {"number": 2, "title": "Child", "headRefName": "b", "_story_ref": 200},
            {"number": 1, "title": "Parent", "headRefName": "a", "_story_ref": 100},
        ]
        deps = {200: [100]}
        ordered = order_prs_by_dependencies(prs, deps)
        assert [pr["_story_ref"] for pr in ordered] == [100, 200]


EPIC_DIAGRAM_DESCRIPTION = """## Overview

Some prose about the epic.

## Dependency Diagram

```
#100 (Root)
  └─► #200 (Child of 100)
        └─► #300 (Child of 200)
```

## Notes

More prose.
"""


def _epic_json(description):
    return json.dumps(
        {
            "id": 846,
            "ref": 2752,
            "subject": "Stacked story branches",
            "description": description,
            "status": 15,
            "is_closed": False,
        }
    )


def _open_prs_payload(entries):
    """Build a `gh pr list --json number,title,headRefName,body` payload."""
    return json.dumps(
        [
            {
                "number": number,
                "title": title,
                "headRefName": branch,
                "body": body,
            }
            for number, title, branch, body in entries
        ]
    )


class TestDiscoverEpicPrs:
    """Epic mode builds an ordered PR queue from the epic’s dependency diagram."""

    def test_orders_prs_by_epic_diagram(self, monkeypatch):
        """The queue follows the diagram’s topological order, not the order gh listed."""
        fake_cmd, _ = _fake_run_cmd([
            ("scripts/taiga-story.sh", (0, _epic_json(EPIC_DIAGRAM_DESCRIPTION), "")),
            ("gh pr list", (0, _open_prs_payload([
                (33, "feat: grandchild (#300)", "feature/grandchild-300", "Story #300"),
                (22, "feat: child (#200)", "feature/child-200", "Story #200"),
                (11, "feat: root (#100)", "feature/root-100", "Story #100"),
            ]), "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        queue = discover_epic_prs(2752)

        assert [pr["number"] for pr in queue] == [11, 22, 33]
        assert all(set(pr) == {"number", "title", "branch", "body"} for pr in queue)
        assert queue[0]["branch"] == "feature/root-100"

    def test_uses_epic_flag_for_taiga_lookup(self, monkeypatch):
        """The lookup must ask for an epic — a user-story lookup 404s on an epic ref."""
        fake_cmd, calls = _fake_run_cmd([
            ("scripts/taiga-story.sh", (0, _epic_json(EPIC_DIAGRAM_DESCRIPTION), "")),
            ("gh pr list", (0, _open_prs_payload([
                (11, "feat: root (#100)", "feature/root-100", ""),
            ]), "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        discover_epic_prs(2752)

        taiga_calls = [c for c in calls if "taiga-story.sh" in c]
        assert taiga_calls, "expected a taiga-story.sh lookup"
        assert "--epic" in taiga_calls[0]
        assert "2752" in taiga_calls[0]

    def test_epic_lookup_failure_raises(self, monkeypatch):
        """A 404 on the epic refuses the train rather than degrading to ref order."""
        fake_cmd, _ = _fake_run_cmd([
            ("scripts/taiga-story.sh", (44, "", "No epic found with ref #9999 in project 39")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        with pytest.raises(RuntimeError) as exc:
            discover_epic_prs(9999)
        assert "9999" in str(exc.value)

    def test_missing_dependency_diagram_raises(self, monkeypatch):
        """No diagram means no order — refuse rather than queue every open PR.

        There is deliberately no fallback to queueing every open PR carrying a
        Taiga ref.
        """
        fake_cmd, calls = _fake_run_cmd([
            ("scripts/taiga-story.sh", (0, _epic_json("## Overview\n\nNo diagram here.\n"), "")),
            ("gh pr list", (0, _open_prs_payload([
                (11, "feat: unrelated (#100)", "feature/root-100", ""),
            ]), "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        with pytest.raises(RuntimeError) as exc:
            discover_epic_prs(2752)
        assert "diagram" in str(exc.value).lower()

    def test_diagram_ref_without_open_pr_is_skipped(self, monkeypatch):
        """A diagram ref with no open PR is normal (merged, or not started)."""
        fake_cmd, _ = _fake_run_cmd([
            ("scripts/taiga-story.sh", (0, _epic_json(EPIC_DIAGRAM_DESCRIPTION), "")),
            ("gh pr list", (0, _open_prs_payload([
                (11, "feat: root (#100)", "feature/root-100", "Story #100"),
            ]), "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        queue = discover_epic_prs(2752)

        assert [pr["number"] for pr in queue] == [11]

    def test_integration_pr_and_story_pr_both_queued(self, monkeypatch):
        """A story’s integration PR and its own PR share a ref; both belong in the queue."""
        fake_cmd, _ = _fake_run_cmd([
            ("scripts/taiga-story.sh", (0, _epic_json(EPIC_DIAGRAM_DESCRIPTION), "")),
            ("gh pr list", (0, _open_prs_payload([
                (22, "feat: child (#200)", "feature/child-200", "Story #200"),
                (21, "Integration for #200", "epic/2752-integration-200", ""),
                (11, "feat: root (#100)", "feature/root-100", "Story #100"),
            ]), "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        queue = discover_epic_prs(2752)

        assert [pr["number"] for pr in queue] == [11, 21, 22]

    def test_integration_pr_from_another_epic_is_not_queued(self, monkeypatch):
        """An integration branch names its own epic; a foreign epic's must not be queued.

        `epic/999-integration-200` carries `-200` delimiter-bounded, so plain ref
        matching accepts it for story #200 even though it merges epic #999's
        parents. Queueing it would merge another epic's commits on this train.
        """
        fake_cmd, _ = _fake_run_cmd([
            ("scripts/taiga-story.sh", (0, _epic_json(EPIC_DIAGRAM_DESCRIPTION), "")),
            ("gh pr list", (0, _open_prs_payload([
                (99, "Integration for another epic", "epic/999-integration-200", ""),
                (21, "Integration for #200", "epic/2752-integration-200", ""),
                (11, "feat: root (#100)", "feature/root-100", "Story #100"),
            ]), "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        queue = discover_epic_prs(2752)

        numbers = [pr["number"] for pr in queue]
        assert 99 not in numbers, "another epic's integration PR must not be queued"
        assert numbers == [11, 21]

    def test_body_dependency_mention_does_not_steal_a_pr(self, monkeypatch):
        """A PR is claimed by the ref its branch/title names, not one its body cites.

        A child PR whose body reads "depends on #100" would otherwise be claimed
        by the lower ref #100 (sorted first), which both strands the real #100 PR
        and gives the child the wrong dependency node.
        """
        fake_cmd, _ = _fake_run_cmd([
            ("scripts/taiga-story.sh", (0, _epic_json(EPIC_DIAGRAM_DESCRIPTION), "")),
            ("gh pr list", (0, _open_prs_payload([
                (22, "feat: child (#200)", "feature/child-200", "Depends on #100"),
                (11, "feat: root (#100)", "feature/root-100", ""),
            ]), "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        queue = discover_epic_prs(2752)

        # Both PRs queued, each under its own ref, so #100 sorts before #200.
        assert [pr["number"] for pr in queue] == [11, 22]

    def test_body_only_ref_mention_is_not_queued(self, monkeypatch):
        """A PR that cites a diagram ref only in prose is not one of the epic's stories.

        Bug-fix PRs routinely name the stories they sequence against. Claiming
        such a PR for that ref would merge unrelated work on this epic's train.
        """
        fake_cmd, _ = _fake_run_cmd([
            ("scripts/taiga-story.sh", (0, _epic_json(EPIC_DIAGRAM_DESCRIPTION), "")),
            ("gh pr list", (0, _open_prs_payload([
                (77, "fix: unrelated bug (bug #900)", "fix/unrelated-900",
                 "Sequences against story #100 and #200; no overlap in code."),
                (11, "feat: root (#100)", "feature/root-100", ""),
            ]), "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        queue = discover_epic_prs(2752)

        assert [pr["number"] for pr in queue] == [11]

    def test_branch_ref_wins_over_a_title_mention(self, monkeypatch):
        """A branch names the story a PR implements; a title may also cite its parent.

        Scanning branch and title together claims the PR for the lower ref it
        mentions anywhere, which both applies the wrong Taiga gate and collides
        with the PR that ref really belongs to.
        """
        fake_cmd, _ = _fake_run_cmd([
            ("scripts/taiga-story.sh", (0, _epic_json(EPIC_DIAGRAM_DESCRIPTION), "")),
            ("gh pr list", (0, _open_prs_payload([
                (22, "feat: child (#200) — builds on #100", "feature/child-200", ""),
                (11, "feat: root (#100)", "feature/root-100", ""),
            ]), "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        queue = discover_epic_prs(2752)

        assert [pr["number"] for pr in queue] == [11, 22]

    def test_two_prs_for_one_story_raises(self, monkeypatch):
        """Two PRs sharing an ordering node would collapse, silently dropping one."""
        fake_cmd, _ = _fake_run_cmd([
            ("scripts/taiga-story.sh", (0, _epic_json(EPIC_DIAGRAM_DESCRIPTION), "")),
            ("gh pr list", (0, _open_prs_payload([
                (23, "feat: child, second attempt", "feature/child-redo-200", ""),
                (22, "feat: child (#200)", "feature/child-200", ""),
                (11, "feat: root (#100)", "feature/root-100", ""),
            ]), "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        with pytest.raises(RuntimeError) as exc:
            discover_epic_prs(2752)
        message = str(exc.value)
        assert "#200" in message
        assert "22" in message and "23" in message

    def test_ambiguous_title_naming_two_diagram_refs_is_not_claimed(self, monkeypatch):
        """A title naming two diagram refs cannot say which story the PR implements."""
        fake_cmd, _ = _fake_run_cmd([
            ("scripts/taiga-story.sh", (0, _epic_json(EPIC_DIAGRAM_DESCRIPTION), "")),
            ("gh pr list", (0, _open_prs_payload([
                (44, "child (#200) builds on #100", "fix/no-ref-in-branch", ""),
                (11, "feat: root (#100)", "feature/root-100", ""),
            ]), "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        queue = discover_epic_prs(2752)

        # Picking the lower ref would both mis-gate #44 and displace #11.
        assert [pr["number"] for pr in queue] == [11]

    def test_invalid_json_raises(self, monkeypatch):
        fake_cmd, _ = _fake_run_cmd([
            ("scripts/taiga-story.sh", (0, "not json", "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        with pytest.raises(RuntimeError):
            discover_epic_prs(2752)

    def test_pr_list_failure_raises(self, monkeypatch):
        fake_cmd, _ = _fake_run_cmd([
            ("scripts/taiga-story.sh", (0, _epic_json(EPIC_DIAGRAM_DESCRIPTION), "")),
            ("gh pr list", (1, "", "gh: could not reach GitHub")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        with pytest.raises(RuntimeError):
            discover_epic_prs(2752)

class TestSummaryGeneration:
    """Correct merged/skipped/failed counts."""

    def test_summary_with_mixed_results(self):
        state = {
            "target": "main",
            "prs": [
                {"number": 1, "title": "A", "branch": "a", "status": "merged", "error": None},
                {"number": 2, "title": "B", "branch": "b", "status": "skipped", "error": "already merged"},
                {"number": 3, "title": "C", "branch": "c", "status": "failed", "error": "CI failing: build"},
                {"number": 4, "title": "D", "branch": "d", "status": "merged", "error": None},
            ],
            "summary": {"merged": 2, "skipped": 1, "failed": 1},
        }
        summary = generate_summary(state)
        assert "Merged (2)" in summary
        assert "Skipped (1)" in summary
        assert "Failed (1)" in summary
        assert "#1" in summary
        assert "#4" in summary
        assert "already merged" in summary
        assert "CI failing: build" in summary

    def test_summary_all_merged(self):
        state = {
            "target": "main",
            "prs": [
                {"number": 1, "title": "A", "branch": "a", "status": "merged", "error": None},
            ],
            "summary": {"merged": 1, "skipped": 0, "failed": 0},
        }
        summary = generate_summary(state)
        assert "Merged (1)" in summary
        assert "Skipped (0)" in summary
        assert "Failed (0)" in summary


class TestExitCodes:
    """Exit code constants are correct."""

    def test_exit_codes(self):
        assert EXIT_OK == 0
        assert EXIT_CONFLICTS == 10
        assert EXIT_REMEDIATION == 11
        assert EXIT_ERROR == 1


class TestWriteActionRequest:
    """Action-request JSON written correctly for conflicts and remediation."""

    def test_conflict_action_request(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            action_path = f.name
        try:
            write_action_request(
                action_path,
                action="resolve_conflicts",
                pr={"number": 123, "branch": "feature/foo-123", "title": "Fix foo"},
                state_file="/tmp/merge-train-123.json",
                details={
                    "files": ["bots/chismebot.py", "bots/crudobot.py"],
                    "pre_rebase_ref": "abc123",
                },
            )
            with open(action_path) as f:
                data = json.load(f)
            assert data["action"] == "resolve_conflicts"
            assert data["pr"]["number"] == 123
            assert data["pr"]["branch"] == "feature/foo-123"
            assert data["state_file"] == "/tmp/merge-train-123.json"
            assert "bots/chismebot.py" in data["details"]["files"]
            assert data["details"]["pre_rebase_ref"] == "abc123"
        finally:
            os.unlink(action_path)

    def test_remediation_action_request(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            action_path = f.name
        try:
            write_action_request(
                action_path,
                action="remediate_build",
                pr={"number": 456, "branch": "feature/bar-456", "title": "Fix bar"},
                state_file="/tmp/merge-train-456.json",
                details={
                    "failure_type": "ci_pre_rebase",
                    "failing_checks": ["build"],
                },
            )
            with open(action_path) as f:
                data = json.load(f)
            assert data["action"] == "remediate_build"
            assert data["details"]["failure_type"] == "ci_pre_rebase"
            assert "build" in data["details"]["failing_checks"]
        finally:
            os.unlink(action_path)


class TestExtractTaigaRef:
    """Extract Taiga ref from PR metadata."""

    def test_extracts_from_title(self):
        assert extract_taiga_ref("some-branch", "Fix #42 something", "") == 42

    def test_extracts_from_body(self):
        assert extract_taiga_ref("some-branch", "Title", "Closes #99") == 99

    def test_extracts_from_branch(self):
        assert extract_taiga_ref("feature/foo-123", "", "") == 123

    def test_returns_none_when_no_ref(self):
        assert extract_taiga_ref("no-ref-here", "Plain title", "Plain body") is None

    def test_branch_ref_at_start(self):
        assert extract_taiga_ref("42-feature", "", "") == 42

    def test_branch_ref_at_end(self):
        assert extract_taiga_ref("feature-42", "", "") == 42

    def test_branch_ref_delimiter_bounded(self):
        """Delimiter-bounded ref at end of branch should match."""
        assert extract_taiga_ref("feature-4123", "", "") == 4123

    def test_integration_branch_resolves_to_child_story_ref(self):
        """An integration branch names the child story, not the epic."""
        assert extract_taiga_ref("epic/2004-integration-2748", "", "") == 2748

    def test_integration_branch_beats_title_ref(self):
        """The integration-branch check wins even when a title #ref is present."""
        assert (
            extract_taiga_ref("epic/2004-integration-2748", "#2004: integration", "")
            == 2748
        )
        # Note: extract_taiga_ref matches delimiter-bounded digit sequences, not embedded digits

    def test_conventional_branch_beats_title_and_body_ref(self):
        """A child story's title may mention its parent; the branch names the story."""
        assert extract_taiga_ref(
            "feature/71-child-story", "Follow-up to #70", "Builds on #70"
        ) == 71
        assert extract_taiga_ref("fix/12-ping-timezone", "Fixes #3 regression", "") == 12

    def test_title_still_beats_a_loose_branch_number(self):
        """Only the feature/fix convention is authoritative; a stray number in another branch is not."""
        assert extract_taiga_ref("release-2026-11", "Ship #42", "") == 42

    def test_story_pr_landed_attributes_by_branch_not_parent_in_title(self, monkeypatch):
        """A merged child PR whose title cites the parent must not mark the parent landed."""
        import merge_train
        payload = _all_prs_payload([
            (31, "MERGED", "child work, depends on #70", "feature/71-child"),
        ])
        fake_cmd, _ = _fake_run_cmd([("pr list --state all", (0, payload, ""))])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        assert merge_train.story_pr_landed(70) is False
        assert merge_train.story_pr_landed(71) is True


class TestPreRebaseStabilityCheck:
    """Pre-rebase stability check verifies CI before attempting rebase."""

    def test_stable_result_allows_processing(self):
        """The script should recognize STABLE as a passing CI status."""
        # This is a contract test — the actual CI check calls monitor-pr-stability.sh
        # We test that the parse logic for the result string works correctly
        from merge_train import parse_stability_result

        result = parse_stability_result("STABLE")
        assert result["status"] == "stable"
        assert result["failing_checks"] == []

    def test_checks_failing_result(self):
        from merge_train import parse_stability_result

        result = parse_stability_result("CHECKS_FAILING:build,test-unit")
        assert result["status"] == "checks_failing"
        assert result["failing_checks"] == ["build", "test-unit"]

    def test_comments_found_result(self):
        from merge_train import parse_stability_result

        result = parse_stability_result("COMMENTS_FOUND:3")
        assert result["status"] == "comments_found"
        assert result["comment_count"] == 3

    @staticmethod
    def _run_process_pr(monkeypatch, tmp_path, is_ancestor_rc):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        fake_cmd, calls = _fake_run_cmd([
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            ("gh pr view 42 --json state -q .state", (0, "MERGED", "")),
            ("--json baseRefName", (0, "main", "")),
            ("monitor-pr-stability.sh", (0, "STABLE", "")),
            ("git checkout feature/foo-42", (0, "", "")),
            ("git reset --hard upstream/", (0, "", "")),
            ("git merge-base --is-ancestor", (is_ancestor_rc, "", "")),
            ("git rev-parse HEAD", (0, "abc123", "")),
            ("git rebase upstream/main", (0, "", "")),
            ("git diff --check", (0, "", "")),
            ("python -m py_compile", (0, "", "")),
            ("git push --force-with-lease", (0, "", "")),
            _taiga_done_response(),
            ("gh pr merge", (0, "", "")),
            ("git checkout main", (0, "", "")),
            ("git pull upstream main", (0, "", "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        process_pr(sm, 0, "main", state_path)
        return [c for c in calls if "monitor-pr-stability.sh" in c]

    def test_pre_rebase_monitor_passes_zero_quiet_minutes(self, monkeypatch, tmp_path):
        monitor_calls = self._run_process_pr(monkeypatch, tmp_path, is_ancestor_rc=0)
        assert monitor_calls[0].endswith(" 0 --poll-interval 30")

    def test_post_rebase_monitor_passes_zero_quiet_minutes(self, monkeypatch, tmp_path):
        monitor_calls = self._run_process_pr(monkeypatch, tmp_path, is_ancestor_rc=1)
        assert len(monitor_calls) == 2
        assert monitor_calls[1].endswith(" 0")
        assert not any(c.endswith(" 8") for c in monitor_calls)


def _make_sm_for_process_pr(state_path, pr_num=42, branch="feature/foo-42", title="Fix #42 foo"):
    """Helper to create a StateManager with a single PR for process_pr tests."""
    sm = StateManager(state_path)
    sm.init(
        [{"number": pr_num, "title": title, "branch": branch, "body": "Story #42"}],
        target="main",
    )
    return sm


def _pr_view_response(state, base="main", pr_num=42):
    """The canned reply to process_pr's opening `gh pr view --json state,baseRefName`.

    State and base arrive in one call, so the open check can tell a PR merged
    into the target from one merged into its own parent branch, and the
    retarget below has the live base without a second round-trip.
    """
    return (
        f"gh pr view {pr_num} --json state,baseRefName",
        (0, json.dumps({"state": state, "baseRefName": base}), ""),
    )


INTEGRATION_EPIC_DESCRIPTION = """## Dependency Diagram

```
#100 (A)
  └─► #2748 (Child)
#101 (B)
  └─► #2748 (Child)
```
"""


def _all_prs_payload(entries):
    """Build a `gh pr list --state all --json number,state,title,headRefName,body` payload."""
    return json.dumps([
        {"number": n, "state": state, "title": title, "headRefName": branch, "body": ""}
        for n, state, title, branch in entries
    ])


def _integration_parents_landed_responses():
    """Canned replies for the integration PR's parent check: both parents MERGED."""
    return [
        ("--epic", (0, _epic_json(INTEGRATION_EPIC_DESCRIPTION), "")),
        ("pr list --state all", (0, _all_prs_payload([
            (11, "MERGED", "feat: a (#100)", "feature/a-100"),
            (12, "MERGED", "feat: b (#101)", "feature/b-101"),
        ]), "")),
    ]


def _taiga_done_response():
    """Canned `scripts/taiga-story.sh` reply for an item in a closed (Done) status."""
    return ("scripts/taiga-story.sh", (0, json.dumps({
        "is_closed": True,
        "status_extra_info": {"name": "Done", "color": "#44AD54"},
        "status": 4,
    }), ""))


def _fake_run_cmd(responses):
    """Create a fake run_cmd that returns canned responses based on command substrings.

    responses is a list of (substring, result) tuples, checked in order.
    The first match wins. If no match, returns (0, "", "").
    A result is either a single (rc, stdout, stderr) tuple, or a list of such
    tuples consumed one per matching call (the last entry repeats once exhausted)
    — used when the same command is issued at different pipeline stages, e.g.
    `gh pr view --json state` for the open check and then the auto-merge poll.
    """
    calls = []
    sequences = {}

    def fake(cmd, check=True, capture=True):
        calls.append(cmd)
        for substr, result in responses:
            if substr in (cmd if isinstance(cmd, str) else " ".join(cmd)):
                if isinstance(result, list):
                    idx = sequences.get(substr, 0)
                    sequences[substr] = idx + 1
                    return result[min(idx, len(result) - 1)]
                return result
        return (0, "", "")

    return fake, calls


class TestAncestryCheckSkipsRebase:
    """When the PR branch is already up-to-date with the target, rebase is skipped."""

    def test_skips_rebase_when_up_to_date(self, monkeypatch, tmp_path):
        """When merge-base --is-ancestor returns 0, rebase/push/post-push CI are skipped."""
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)

        fake_cmd, calls = _fake_run_cmd([
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            ("gh pr view 42 --json state -q .state", (0, "MERGED", "")),
            ("--json baseRefName", (0, "main", "")),
            ("monitor-pr-stability.sh", (0, "STABLE", "")),
            ("git checkout feature/foo-42", (0, "", "")),
            ("git reset --hard upstream/", (0, "", "")),
            ("git merge-base --is-ancestor", (0, "", "")),
            # Taiga check
            ("scripts/taiga-story.sh", (0, json.dumps({
                "is_closed": True,
                "status_extra_info": {"name": "Done", "color": "#44AD54"},
                "status": 4,
            }), "")),
            ("gh pr merge", (0, "", "")),
            ("git checkout main", (0, "", "")),
            ("git pull upstream main", (0, "", "")),
        ])

        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "merged"

        # Verify rebase, force-push, and post-push monitoring were NOT called
        cmd_str = "\n".join(calls)
        assert "git rebase" not in cmd_str
        assert "git push --force-with-lease" not in cmd_str

        # The pre-rebase stability check SHOULD have run (first monitor call)
        pre_rebase_monitor_calls = [c for c in calls if "monitor-pr-stability.sh" in c]
        assert len(pre_rebase_monitor_calls) == 1  # Only pre-rebase, no post-push

    def test_logs_skip_message(self, monkeypatch, tmp_path, capsys):
        """Log message emitted when skipping rebase."""
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)

        fake_cmd, calls = _fake_run_cmd([
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            ("gh pr view 42 --json state -q .state", (0, "MERGED", "")),
            ("--json baseRefName", (0, "main", "")),
            ("monitor-pr-stability.sh", (0, "STABLE", "")),
            ("git checkout feature/foo-42", (0, "", "")),
            ("git reset --hard upstream/", (0, "", "")),
            ("git merge-base --is-ancestor", (0, "", "")),
            ("scripts/taiga-story.sh", (0, json.dumps({
                "is_closed": True,
                "status_extra_info": {"name": "Done", "color": "#44AD54"},
                "status": 4,
            }), "")),
            ("gh pr merge", (0, "", "")),
            ("git checkout main", (0, "", "")),
            ("git pull upstream main", (0, "", "")),
        ])

        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        process_pr(sm, 0, "main", state_path)

        captured = capsys.readouterr()
        assert "already up-to-date" in captured.err
        assert "skipping rebase" in captured.err

    def test_fails_pr_when_is_ancestor_errors(self, monkeypatch, tmp_path):
        """When merge-base --is-ancestor returns rc=128 (git error), PR is marked failed."""
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)

        fake_cmd, calls = _fake_run_cmd([
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            _taiga_done_response(),
            ("gh pr view 42 --json state -q .state", (0, "MERGED", "")),
            ("--json baseRefName", (0, "main", "")),
            ("monitor-pr-stability.sh", (0, "STABLE", "")),
            ("git checkout feature/foo-42", (0, "", "")),
            ("git reset --hard upstream/", (0, "", "")),
            ("git merge-base --is-ancestor", (128, "", "fatal: Not a valid object name upstream/main")),
        ])

        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "failed"
        assert "git merge-base --is-ancestor failed" in sm.state["prs"][0]["error"]
        assert "128" in sm.state["prs"][0]["error"]

        # Rebase must NOT have been attempted after the error
        cmd_str = "\n".join(calls)
        assert "git rebase" not in cmd_str

    def test_rebases_when_behind_target(self, monkeypatch, tmp_path):
        """When merge-base --is-ancestor returns non-zero, normal rebase flow runs."""
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)

        fake_cmd, calls = _fake_run_cmd([
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            ("gh pr view 42 --json state -q .state", (0, "MERGED", "")),
            ("--json baseRefName", (0, "main", "")),
            ("monitor-pr-stability.sh", (0, "STABLE", "")),
            ("git checkout feature/foo-42", (0, "", "")),
            ("git reset --hard upstream/", (0, "", "")),
            ("git merge-base --is-ancestor", (1, "", "")),  # Not ancestor — needs rebase
            ("git rev-parse HEAD", (0, "abc123", "")),
            ("git rebase upstream/main", (0, "", "")),
            ("git diff --check", (0, "", "")),
            ("python -m py_compile", (0, "", "")),
            ("git push --force-with-lease", (0, "", "")),
            # Taiga check
            ("scripts/taiga-story.sh", (0, json.dumps({
                "is_closed": True,
                "status_extra_info": {"name": "Done", "color": "#44AD54"},
                "status": 4,
            }), "")),
            ("gh pr merge", (0, "", "")),
            ("git checkout main", (0, "", "")),
            ("git pull upstream main", (0, "", "")),
        ])

        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "merged"

        # Verify rebase and force-push WERE called
        cmd_str = "\n".join(calls)
        assert "git rebase" in cmd_str
        assert "git push --force-with-lease" in cmd_str


class TestMonitorInfrastructureError:
    """monitor-pr-stability.sh exiting nonzero is a monitor failure, not a build verdict."""

    @staticmethod
    def _responses(monitor_results, diff_check=(0, "", ""), is_ancestor_rc=1):
        return [
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            ("gh pr view 42 --json state -q .state", (0, "MERGED", "")),
            ("--json baseRefName", (0, "main", "")),
            ("monitor-pr-stability.sh", monitor_results),
            ("git checkout feature/foo-42", (0, "", "")),
            ("git reset --hard upstream/", (0, "", "")),
            ("git merge-base --is-ancestor", (is_ancestor_rc, "", "")),
            ("git rev-parse HEAD", (0, "abc123", "")),
            ("git rebase upstream/main", (0, "", "")),
            ("git diff --check", diff_check),
            ("python -m py_compile", (0, "", "")),
            ("git push --force-with-lease", (0, "", "")),
            _taiga_done_response(),
            ("gh pr merge", (0, "", "")),
            ("git checkout main", (0, "", "")),
            ("git pull upstream main", (0, "", "")),
        ]

    def test_pre_rebase_monitor_error_is_not_a_build_failure(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        fake_cmd, calls = _fake_run_cmd(self._responses([(1, "ERROR:gh run list failed: HTTP 502", "")]))
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_ERROR
        pr = sm.state["prs"][0]
        assert pr["status"] == "in_progress"
        assert "HTTP 502" in pr["error"]
        assert sm.state["currentIndex"] == 0
        assert not os.path.exists(action_path_for(state_path))
        assert not any("git rebase" in c for c in calls)

    def test_post_rebase_monitor_error_does_not_fail_or_advance(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        fake_cmd, calls = _fake_run_cmd(self._responses([
            (0, "STABLE", ""),
            (1, "ERROR:GraphQL query failed: HTTP 401", ""),
        ]))
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_ERROR
        pr = sm.state["prs"][0]
        assert pr["status"] == "in_progress"
        assert "post-rebase" in pr["error"] and "HTTP 401" in pr["error"]
        assert sm.state["currentIndex"] == 0
        assert not any("gh pr merge" in c for c in calls)


class TestPostRebaseWhitespaceCheck:
    """git diff --check reports whitespace errors too; only conflict markers fail a rebase."""

    @staticmethod
    def _run(monkeypatch, tmp_path, diff_check):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        fake_cmd, calls = _fake_run_cmd(TestMonitorInfrastructureError._responses(
            [(0, "STABLE", "")], diff_check=diff_check))
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        process_pr(sm, 0, "main", state_path)
        return sm, calls

    def test_whitespace_warning_does_not_fail_the_rebase(self, monkeypatch, tmp_path):
        sm, calls = self._run(monkeypatch, tmp_path, (2, "bots/x.py:3: trailing whitespace.\n+x = 1 ", ""))
        assert sm.state["prs"][0]["status"] != "failed"
        assert any("git push --force-with-lease" in c for c in calls)

    def test_leftover_conflict_marker_fails_the_rebase(self, monkeypatch, tmp_path):
        sm, calls = self._run(monkeypatch, tmp_path, (2, "bots/x.py:3: leftover conflict marker\n+<<<<<<< HEAD", ""))
        pr = sm.state["prs"][0]
        assert pr["status"] == "failed"
        assert pr["error"] == "conflict markers remain after rebase"
        assert not any("git push --force-with-lease" in c for c in calls)
        assert any(c.startswith("git reset --hard abc123") for c in calls)

    def test_diff_check_runs_with_the_c_locale(self, monkeypatch, tmp_path):
        _, calls = self._run(monkeypatch, tmp_path, (0, "", ""))
        assert any(c.startswith("env LC_ALL=C git diff --check") for c in calls)


class TestContainedIntegrationBranchClosed:
    """An integration branch whose tree is already identical to the target
    carries no remaining diff once every parent it merged has landed -- the
    train closes it rather than treating it as a normal live PR."""

    INTEGRATION_BRANCH = "epic/2004-integration-2748"

    def test_integration_pr_contained_in_target_is_closed_and_counted_merged(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path, branch=self.INTEGRATION_BRANCH)

        fake_cmd, calls = _fake_run_cmd([
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            ("git diff --quiet", (0, "", "")),  # tree-equal: contained
            ("gh pr close 42", (0, "", "")),
        ])

        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "merged"
        # The child PR retargets onto target only if the branch is recorded landed.
        assert self.INTEGRATION_BRANCH in sm.state.get("mergedBranches", [])

        cmd_str = "\n".join(calls)
        assert "gh pr close 42" in cmd_str
        assert "monitor-pr-stability.sh" not in cmd_str
        # The branch outlives the close: a child PR still based on it would lose
        # its base if the close deleted it.
        close_calls = [c for c in calls if "gh pr close" in c]
        assert all("--delete-branch" not in c for c in close_calls)

    def test_contained_non_integration_pr_is_not_closed(self, monkeypatch, tmp_path):
        """A non-integration branch that happens to be tree-equal to target is
        left alone -- it still goes through the normal stability/merge path."""
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path, branch="feature/x-2748")

        fake_cmd, calls = _fake_run_cmd([
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            ("git diff --quiet", (0, "", "")),  # tree-equal, but not an integration branch
            ("monitor-pr-stability.sh", (0, "STABLE", "")),
            ("git checkout feature/x-2748", (0, "", "")),
            ("git reset --hard upstream/", (0, "", "")),
            ("git merge-base --is-ancestor", (0, "", "")),
            ("scripts/taiga-story.sh", (0, json.dumps({
                "is_closed": True,
                "status_extra_info": {"name": "Done", "color": "#44AD54"},
                "status": 4,
            }), "")),
            ("gh pr merge", (0, "", "")),
            ("git checkout main", (0, "", "")),
            ("git pull upstream main", (0, "", "")),
        ])

        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        cmd_str = "\n".join(calls)
        assert "gh pr close" not in cmd_str


class TestIntegrationBranchMergedForward:
    """An integration branch is a merge commit by construction, so the train
    merges target into it instead of rebasing, which would flatten it."""

    INTEGRATION_BRANCH = "epic/2004-integration-2748"

    def test_non_empty_integration_pr_is_merged_forward_not_rebased(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path, branch=self.INTEGRATION_BRANCH)

        fake_cmd, calls = _fake_run_cmd([
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            ("git diff --quiet", (1, "", "")),  # real diff remains: not contained
            *_integration_parents_landed_responses(),
            ("gh pr view 42 --json state -q .state", (0, "MERGED", "")),
            ("--json baseRefName", (0, "main", "")),
            ("monitor-pr-stability.sh", (0, "STABLE", "")),
            ("git checkout " + self.INTEGRATION_BRANCH, (0, "", "")),
            ("git reset --hard upstream/", (0, "", "")),
            ("git merge-base --is-ancestor", (1, "", "")),  # behind target
            ("git rev-parse HEAD", (0, "abc123", "")),
            ("git merge upstream/main --no-edit", (0, "", "")),
            ("git diff --check", (0, "", "")),
            ("python -m py_compile", (0, "", "")),
            ("git push --force-with-lease", (0, "", "")),
            ("scripts/taiga-story.sh", (0, json.dumps({
                "is_closed": True,
                "status_extra_info": {"name": "Done", "color": "#44AD54"},
                "status": 4,
            }), "")),
            ("gh pr merge", (0, "", "")),
            ("git checkout main", (0, "", "")),
            ("git pull upstream main", (0, "", "")),
        ])

        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "merged"

        cmd_str = "\n".join(calls)
        assert "git merge upstream/main --no-edit" in cmd_str
        assert "git rebase" not in cmd_str

    def test_non_integration_pr_still_rebases(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path, branch="feature/foo-42")

        fake_cmd, calls = _fake_run_cmd([
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            ("gh pr view 42 --json state -q .state", (0, "MERGED", "")),
            ("--json baseRefName", (0, "main", "")),
            ("monitor-pr-stability.sh", (0, "STABLE", "")),
            ("git checkout feature/foo-42", (0, "", "")),
            ("git reset --hard upstream/", (0, "", "")),
            ("git merge-base --is-ancestor", (1, "", "")),  # behind target
            ("git rev-parse HEAD", (0, "abc123", "")),
            ("git rebase upstream/main", (0, "", "")),
            ("git diff --check", (0, "", "")),
            ("python -m py_compile", (0, "", "")),
            ("git push --force-with-lease", (0, "", "")),
            ("scripts/taiga-story.sh", (0, json.dumps({
                "is_closed": True,
                "status_extra_info": {"name": "Done", "color": "#44AD54"},
                "status": 4,
            }), "")),
            ("gh pr merge", (0, "", "")),
            ("git checkout main", (0, "", "")),
            ("git pull upstream main", (0, "", "")),
        ])

        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "merged"

        cmd_str = "\n".join(calls)
        assert "git rebase upstream/main" in cmd_str
        assert "git merge upstream/main --no-edit" not in cmd_str


class TestDivergedLocalBranchResetsToOrigin:
    """A local branch left rebased-but-unpushed by a prior resumed run is synced
    to origin by reset rather than by ff-only pull (bug #2166)."""

    def test_diverged_local_branch_is_reset_to_origin_and_pr_still_merges(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)

        fake_cmd, calls = _fake_run_cmd([
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            ("gh pr view 42 --json state -q .state", (0, "MERGED", "")),
            ("--json baseRefName", (0, "main", "")),
            ("monitor-pr-stability.sh", (0, "STABLE", "")),
            ("git checkout feature/foo-42", (0, "", "")),
            ("git rev-parse upstream/feature/foo-42", (0, "origin111", "")),
            ("git rev-parse HEAD", (0, "local222", "")),
            ("git reset --hard upstream/feature/foo-42", (0, "", "")),
            ("git merge-base --is-ancestor", (0, "", "")),
            ("scripts/taiga-story.sh", (0, json.dumps({
                "is_closed": True,
                "status_extra_info": {"name": "Done", "color": "#44AD54"},
                "status": 4,
            }), "")),
            ("gh pr merge", (0, "", "")),
            ("git checkout main", (0, "", "")),
            ("git pull upstream main", (0, "", "")),
        ])

        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        result = process_pr(sm, 0, "main", state_path)

        assert "git reset --hard upstream/feature/foo-42" in calls
        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "merged"

    def test_no_ff_only_pull_is_issued(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)

        fake_cmd, calls = _fake_run_cmd([
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            ("gh pr view 42 --json state -q .state", (0, "MERGED", "")),
            ("--json baseRefName", (0, "main", "")),
            ("monitor-pr-stability.sh", (0, "STABLE", "")),
            ("git checkout feature/foo-42", (0, "", "")),
            ("git rev-parse upstream/feature/foo-42", (0, "origin111", "")),
            ("git rev-parse HEAD", (0, "origin111", "")),
            ("git reset --hard upstream/feature/foo-42", (0, "", "")),
            ("git merge-base --is-ancestor", (0, "", "")),
            ("scripts/taiga-story.sh", (0, json.dumps({
                "is_closed": True,
                "status_extra_info": {"name": "Done", "color": "#44AD54"},
                "status": 4,
            }), "")),
            ("gh pr merge", (0, "", "")),
            ("git checkout main", (0, "", "")),
            ("git pull upstream main", (0, "", "")),
        ])

        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        process_pr(sm, 0, "main", state_path)

        assert "git pull --ff-only" not in "\n".join(calls)

    def test_reset_failure_marks_pr_failed_and_advances(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)

        fake_cmd, calls = _fake_run_cmd([
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            _taiga_done_response(),
            ("gh pr view 42 --json state -q .state", (0, "MERGED", "")),
            ("--json baseRefName", (0, "main", "")),
            ("monitor-pr-stability.sh", (0, "STABLE", "")),
            ("git checkout feature/foo-42", (0, "", "")),
            ("git rev-parse upstream/feature/foo-42", (0, "origin111", "")),
            ("git rev-parse HEAD", (0, "local222", "")),
            ("git reset --hard upstream/", (1, "", "fatal: ambiguous argument")),
        ])

        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        result = process_pr(sm, 0, "main", state_path)

        assert sm.state["prs"][0]["status"] == "failed"
        assert "git reset --hard" in sm.state["prs"][0]["error"]
        assert sm.state["currentIndex"] == 1
        assert result == EXIT_OK

    def test_missing_origin_branch_marks_pr_failed(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)

        fake_cmd, calls = _fake_run_cmd([
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            _taiga_done_response(),
            ("gh pr view 42 --json state -q .state", (0, "MERGED", "")),
            ("--json baseRefName", (0, "main", "")),
            ("monitor-pr-stability.sh", (0, "STABLE", "")),
            ("git checkout feature/foo-42", (0, "", "")),
            ("git rev-parse upstream/feature/foo-42", (128, "", "fatal: ambiguous argument 'upstream/feature/foo-42'")),
        ])

        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        result = process_pr(sm, 0, "main", state_path)

        assert sm.state["prs"][0]["status"] == "failed"
        assert "upstream/feature/foo-42" in sm.state["prs"][0]["error"]
        assert "git reset --hard" not in "\n".join(calls)
        assert result == EXIT_OK

    def test_divergence_logs_discarded_local_head(self, monkeypatch, tmp_path, capsys):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)

        fake_cmd, calls = _fake_run_cmd([
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            ("gh pr view 42 --json state -q .state", (0, "MERGED", "")),
            ("--json baseRefName", (0, "main", "")),
            ("monitor-pr-stability.sh", (0, "STABLE", "")),
            ("git checkout feature/foo-42", (0, "", "")),
            ("git rev-parse upstream/feature/foo-42", (0, "origin111", "")),
            ("git rev-parse HEAD", (0, "local222", "")),
            ("git reset --hard upstream/feature/foo-42", (0, "", "")),
            ("git merge-base --is-ancestor", (0, "", "")),
            ("scripts/taiga-story.sh", (0, json.dumps({
                "is_closed": True,
                "status_extra_info": {"name": "Done", "color": "#44AD54"},
                "status": 4,
            }), "")),
            ("gh pr merge", (0, "", "")),
            ("git checkout main", (0, "", "")),
            ("git pull upstream main", (0, "", "")),
        ])

        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        process_pr(sm, 0, "main", state_path)

        captured = capsys.readouterr()
        assert "diverged" in captured.err
        assert "local222" in captured.err


def _merge_path_responses(pr_view_states, auto_merge_armed=True):
    """Canned responses for the happy up-to-date path through process_pr.

    pr_view_states[0] is the state the open check sees; the rest feed the
    post-merge auto-merge poll, which is a separate `gh pr view --json state`
    call. A single-element sequence serves both, so the poll keeps returning it.

    auto_merge_armed controls what the post-arming `auto_merge_state()` check
    (via `--json autoMergeRequest`) reports after the zero-exit `gh pr merge`
    call. Defaults to armed so the rest of the suite keeps describing the
    pending path unless a test explicitly needs the unarmed branch.
    """
    return [
        ("git fetch upstream", (0, "", "")),
        _pr_view_response(pr_view_states[0]),
        ("gh pr view 42 --json state -q .state", [(0, s, "") for s in (pr_view_states[1:] or pr_view_states)]),
        ("--json baseRefName", (0, "main", "")),
        ("monitor-pr-stability.sh", (0, "STABLE", "")),
        ("git checkout feature/foo-42", (0, "", "")),
        ("git reset --hard upstream/", (0, "", "")),
        ("git merge-base --is-ancestor", (0, "", "")),
        ("scripts/taiga-story.sh", (0, json.dumps({
            "is_closed": True,
            "status_extra_info": {"name": "Done", "color": "#44AD54"},
            "status": 4,
        }), "")),
        ("gh pr merge", (0, "", "")),
        ("--json autoMergeRequest",
         (0, '{"enabledAt":"2026-09-22T00:00:00Z"}' if auto_merge_armed else "null", "")),
        ("git checkout main", (0, "", "")),
        ("git pull upstream main", (0, "", "")),
    ]


class TestMergeWithAuto:
    """Merge uses --auto and waits for MERGED before advancing (bug #2103)."""

    def test_merge_uses_auto_flag(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        fake_cmd, calls = _fake_run_cmd(_merge_path_responses(["OPEN", "MERGED"]))
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        merge_calls = [c for c in calls if "gh pr merge" in c]
        assert merge_calls == [gh_cmd("pr merge 42 --squash --auto")]

    def test_waits_for_merged_state_before_advancing(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        fake_cmd, calls = _fake_run_cmd(_merge_path_responses(["OPEN", "OPEN", "MERGED"]))
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "merged"
        assert sm.state["currentIndex"] == 1
        # The local target update must happen only after MERGED was observed
        last_poll = max(i for i, c in enumerate(calls) if "gh pr view" in c)
        pull_target = calls.index("git pull upstream main")
        assert last_poll < pull_target
        # Open check, baseRefName refresh, the pre-merge live-base guard, then
        # two poll calls (OPEN then MERGED)
        # One `--json state,baseRefName` open check, one `--json baseRefName`
        # live-base guard, one `--json autoMergeRequest` post-arming
        # verification, and two `--json state` auto-merge polls.
        assert sum(1 for c in calls if "gh pr view" in c) == 5

    def test_merge_command_failure_marks_failed(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        responses = _merge_path_responses(["OPEN"], auto_merge_armed=False)
        responses = [(s, r) if s != "gh pr merge" else (s, (1, "", "auto-merge is not allowed")) for s, r in responses]
        fake_cmd, calls = _fake_run_cmd(responses)
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "failed"
        assert "merge failed" in sm.state["prs"][0]["error"]
        assert sm.state["currentIndex"] == 1

    def test_auto_merge_timeout_does_not_advance(self, monkeypatch, tmp_path):
        """A PR that has not merged must never be silently skipped (bug #2103)."""
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        fake_cmd, calls = _fake_run_cmd(_merge_path_responses(["OPEN"]))
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)
        monkeypatch.setattr("merge_train.MERGE_POLL_TIMEOUT_SECONDS", 0)
        action_requests = []
        monkeypatch.setattr(
            "merge_train.write_action_request",
            lambda path, action, pr, state_file, details: action_requests.append((action, details)),
        )

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_REMEDIATION
        assert sm.state["currentIndex"] == 0
        assert sm.state["prs"][0]["status"] == "in_progress"
        assert "auto-merge still pending" in sm.state["prs"][0]["error"]
        assert action_requests == [("remediate_build", {"failure_type": "auto_merge_timeout", "timeout_seconds": 0})]

    def test_pr_closed_during_auto_merge_marks_failed(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        fake_cmd, calls = _fake_run_cmd(_merge_path_responses(["OPEN", "OPEN", "CLOSED"]))
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "failed"
        assert "closed" in sm.state["prs"][0]["error"].lower()
        assert sm.state["currentIndex"] == 1


def _make_sm_with_child(
    state_path,
    parent_num=42,
    parent_branch="feature/foo-42",
    child_num=99,
    child_branch="feature/child-99",
    child_base="feature/foo-42",
):
    """A parent PR (index 0) with one child PR (index 1) stacked on it."""
    sm = StateManager(state_path)
    sm.init(
        [
            {"number": parent_num, "title": "Fix #42 foo", "branch": parent_branch, "body": "Story #42"},
            {"number": child_num, "title": "Child of #42", "branch": child_branch, "base": child_base},
        ],
        target="main",
    )
    return sm


class TestChildSweepAndMergeMethod:
    """process_pr sweeps open children onto the target before arming, and
    arms --merge for any PR that was ever stacked (task #2769)."""

    def test_stacked_pr_arms_merge(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        sm.update_pr(0, stacked=True)
        fake_cmd, calls = _fake_run_cmd(_merge_path_responses(["OPEN", "MERGED"]))
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        merge_calls = [c for c in calls if "gh pr merge" in c]
        # Exact equality: this also pins the absence of --delete-branch, which
        # GitHub's own delete_branch_on_merge must perform so a stacked child
        # is retargeted rather than closed (spike #2801).
        assert merge_calls == [gh_cmd("pr merge 42 --merge --auto")]

    def test_standalone_pr_still_arms_squash(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        fake_cmd, calls = _fake_run_cmd(_merge_path_responses(["OPEN", "MERGED"]))
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        merge_calls = [c for c in calls if "gh pr merge" in c]
        # Exact equality: this also pins the absence of --delete-branch (#2801).
        assert merge_calls == [gh_cmd("pr merge 42 --squash --auto")]

    def test_pr_retargeted_this_run_still_arms_merge(self, monkeypatch, tmp_path):
        """The regression the sticky `stacked` flag exists for: a PR whose base
        is retargeted onto the target during this same run -- so it reads
        base == target at arming time -- still arms --merge, not --squash."""
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path, branch="feature/child-42")
        sm.update_pr(0, base="feature/parent-42")
        sm.record_merged_branch("feature/parent-42")
        responses = [
            # The open check reports the pre-retarget base; the pre-merge
            # live-base guard runs after `gh pr edit` has moved it to `main`.
            _pr_view_response("OPEN", "feature/parent-42"),
            ("--json baseRefName", (0, "main", "")),
            ("gh pr edit 42 --base main", (0, "", "")),
        ] + _merge_path_responses(["OPEN", "MERGED"])
        fake_cmd, calls = _fake_run_cmd(responses)
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["stacked"] is True
        assert sm.state["prs"][0]["base"] == "main"
        merge_calls = [c for c in calls if "gh pr merge" in c]
        assert merge_calls == [gh_cmd("pr merge 42 --merge --auto")]

    def test_no_stacked_key_arms_squash(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        del sm.state["prs"][0]["stacked"]
        fake_cmd, calls = _fake_run_cmd(_merge_path_responses(["OPEN", "MERGED"]))
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        merge_calls = [c for c in calls if "gh pr merge" in c]
        assert merge_calls == [gh_cmd("pr merge 42 --squash --auto")]

    def test_open_child_retargeted_before_merge(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_with_child(state_path)
        responses = [
            ("gh pr list --base feature/foo-42 --state open", (0, "99", "")),
            ("gh pr edit 99 --base main", (0, "", "")),
        ] + _merge_path_responses(["OPEN", "MERGED"])
        fake_cmd, calls = _fake_run_cmd(responses)
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        edit_index = next(i for i, c in enumerate(calls) if "gh pr edit 99 --base main" in c)
        merge_index = next(i for i, c in enumerate(calls) if "gh pr merge 42" in c)
        assert edit_index < merge_index
        assert sm.state["prs"][1]["base"] == "main"
        assert sm.state["prs"][1]["stacked"] is True
        # The parent's own base was the target throughout, so its sticky
        # `stacked` flag is False -- but it swept an open child onto the
        # target, so the descendants term arms --merge for it too: squashing
        # would replace the parent's commits with one new commit the child
        # doesn't carry, putting the parent's changes back into the child's
        # diff after retarget.
        assert sm.state["prs"][0]["stacked"] is False
        assert calls[merge_index] == gh_cmd("pr merge 42 --merge --auto")

    def test_sweep_leaves_child_stacked_flag_alone(self, monkeypatch, tmp_path):
        """The sweep only writes `base`; it must never touch `stacked` -- that
        flag is owned by enqueue (spike #2801). Seeding it False here and
        expecting it to survive the sweep is the regression test for #2808."""
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_with_child(state_path)
        sm.update_pr(1, stacked=False)
        responses = [
            ("gh pr list --base feature/foo-42 --state open", (0, "99", "")),
            ("gh pr edit 99 --base main", (0, "", "")),
        ] + _merge_path_responses(["OPEN", "MERGED"])
        fake_cmd, calls = _fake_run_cmd(responses)
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][1]["stacked"] is False
        assert sm.state["prs"][1]["base"] == "main"

    def test_child_matching_own_pr_number_is_skipped(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        responses = [
            ("gh pr list --base feature/foo-42 --state open", (0, "42", "")),
        ] + _merge_path_responses(["OPEN", "MERGED"])
        fake_cmd, calls = _fake_run_cmd(responses)
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert not any("gh pr edit" in c for c in calls)
        # A self-entry is not a descendant, so it must not arm --merge either.
        merge_calls = [c for c in calls if "gh pr merge" in c]
        assert merge_calls == [gh_cmd("pr merge 42 --squash --auto")]

    def test_root_with_swept_child_from_an_earlier_run_arms_merge(self, monkeypatch, tmp_path):
        """On a --resume, find_open_children() sees an empty listing because
        the earlier pass already retargeted the child onto the target --
        sweptChildren is the durable record that this PR still has a
        descendant, and it must still arm --merge."""
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        sm.add_swept_child(0, 99)
        fake_cmd, calls = _fake_run_cmd(_merge_path_responses(["OPEN", "MERGED"]))
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        merge_calls = [c for c in calls if "gh pr merge" in c]
        assert merge_calls == [gh_cmd("pr merge 42 --merge --auto")]

    def test_failed_child_lookup_fails_pr_without_arming(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        responses = [
            ("gh pr list --base feature/foo-42 --state open", (1, "", "rate limited")),
        ] + _merge_path_responses(["OPEN"])
        fake_cmd, calls = _fake_run_cmd(responses)
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "failed"
        assert "could not list open PRs" in sm.state["prs"][0]["error"]
        assert not any("gh pr merge" in c for c in calls)

    def test_failed_child_edit_restores_only_already_retargeted(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_with_child(state_path)
        sm.state["prs"].append({
            "number": 100,
            "title": "Second child of #42",
            "branch": "feature/child-100",
            "status": "queued",
            "error": None,
            "base": "feature/foo-42",
            "stacked": True,
        })
        sm._save()
        responses = [
            ("gh pr list --base feature/foo-42 --state open", (0, "99\n100", "")),
            ("gh pr edit 99 --base main", (0, "", "")),
            ("gh pr edit 100 --base main", (1, "", "boom")),
            ("gh pr edit 99 --base feature/foo-42", (0, "", "")),
        ] + _merge_path_responses(["OPEN"])
        fake_cmd, calls = _fake_run_cmd(responses)
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "failed"
        # First child was retargeted then restored; the second was never edited.
        assert sm.state["prs"][1]["base"] == "feature/foo-42"
        assert sm.state["prs"][2]["base"] == "feature/foo-42"
        assert not any("gh pr edit 100 --base feature/foo-42" in c for c in calls)
        assert not any("gh pr merge" in c for c in calls)

    def test_arming_failure_restores_child_and_names_base_and_target(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_with_child(state_path)
        responses = [
            ("gh pr list --base feature/foo-42 --state open", (0, "99", "")),
            ("gh pr edit 99 --base main", (0, "", "")),
            ("gh pr merge", (1, "", "auto-merge is not allowed")),
            ("gh pr edit 99 --base feature/foo-42", (0, "", "")),
        ] + _merge_path_responses(["OPEN"], auto_merge_armed=False)
        fake_cmd, calls = _fake_run_cmd(responses)
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "failed"
        assert "base=main" in sm.state["prs"][0]["error"]
        assert "target=main" in sm.state["prs"][0]["error"]
        assert sm.state["prs"][1]["base"] == "feature/foo-42"
        assert any("gh pr edit 99 --base feature/foo-42" in c for c in calls)

    def test_closed_result_restores_child_and_skips_merged_branches(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_with_child(state_path)
        responses = [
            ("gh pr list --base feature/foo-42 --state open", (0, "99", "")),
            ("gh pr edit 99 --base main", (0, "", "")),
            ("gh pr edit 99 --base feature/foo-42", (0, "", "")),
        ] + _merge_path_responses(["OPEN", "OPEN", "CLOSED"])
        fake_cmd, calls = _fake_run_cmd(responses)
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "failed"
        assert sm.state["prs"][1]["base"] == "feature/foo-42"
        assert sm.state["mergedBranches"] == []

    def test_unresolvable_child_during_the_sweep_halts_the_train(self, monkeypatch, tmp_path):
        """The sweep's own failure path: one child moved, the next refuses, and
        the first cannot be put back. It was never enqueued, so nothing in
        state can carry the danger and the train stops."""
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        responses = [
            ("gh pr list --base feature/foo-42 --state open", (0, "999\n1000", "")),
            ("gh pr edit 999 --base main", (0, "", "")),
            ("gh pr edit 1000 --base main", (1, "", "boom")),
            ("gh pr edit 999 --base feature/foo-42", (1, "", "boom")),
            ("gh pr comment 999", (0, "", "")),
        ] + _merge_path_responses(["OPEN"])
        fake_cmd, calls = _fake_run_cmd(responses)
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        with pytest.raises(SystemExit) as exc_info:
            process_pr(sm, 0, "main", state_path)

        assert exc_info.value.code == EXIT_ERROR
        assert sm.state["prs"][0]["status"] == "failed"
        assert "999" in sm.state["halted"]["reason"]
        assert any("gh pr comment 999" in c for c in calls)
        assert not any("gh pr merge" in c for c in calls)

    def test_live_base_guard_restores_children_before_failing(self, monkeypatch, tmp_path):
        """The guard refuses to arm when the live base is not the target, and
        owes the same restore as every other give-up after the sweep."""
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_with_child(state_path)
        responses = [
            ("gh pr list --base feature/foo-42 --state open", (0, "99", "")),
            ("gh pr edit 99 --base main", (0, "", "")),
            ("gh pr edit 99 --base feature/foo-42", (0, "", "")),
            ("--json baseRefName", (0, "feature/other", "")),
        ] + _merge_path_responses(["OPEN"])
        fake_cmd, calls = _fake_run_cmd(responses)
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "failed"
        assert "feature/other" in sm.state["prs"][0]["error"]
        assert sm.state["prs"][1]["base"] == "feature/foo-42"
        assert sm.state["prs"][0]["sweptChildren"] == []
        assert not any("gh pr merge" in c for c in calls)

    def test_closed_while_waiting_halts_on_an_unresolvable_child(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        responses = [
            ("gh pr list --base feature/foo-42 --state open", (0, "999", "")),
            ("gh pr edit 999 --base main", (0, "", "")),
            ("gh pr edit 999 --base feature/foo-42", (1, "", "boom")),
            ("gh pr comment 999", (0, "", "")),
        ] + _merge_path_responses(["OPEN", "CLOSED"])
        fake_cmd, calls = _fake_run_cmd(responses)
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        with pytest.raises(SystemExit) as exc_info:
            process_pr(sm, 0, "main", state_path)

        assert exc_info.value.code == EXIT_ERROR
        assert sm.state["prs"][0]["status"] == "failed"
        assert "999" in sm.state["halted"]["reason"]
        assert sm.state["mergedBranches"] == []

    def test_timeout_does_not_restore_child(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_with_child(state_path)
        responses = [
            ("gh pr list --base feature/foo-42 --state open", (0, "99", "")),
            ("gh pr edit 99 --base main", (0, "", "")),
        ] + _merge_path_responses(["OPEN"])
        fake_cmd, calls = _fake_run_cmd(responses)
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)
        monkeypatch.setattr("merge_train.MERGE_POLL_TIMEOUT_SECONDS", 0)
        monkeypatch.setattr("merge_train.write_action_request", lambda *a, **kw: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_REMEDIATION
        # Child stays retargeted onto the target: auto-merge is still pending.
        assert sm.state["prs"][1]["base"] == "main"
        assert not any("gh pr edit 99 --base feature/foo-42" in c for c in calls)

    def test_happy_path_records_branch_and_issues_no_restoring_edit(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_with_child(state_path)
        responses = [
            ("gh pr list --base feature/foo-42 --state open", (0, "99", "")),
            ("gh pr edit 99 --base main", (0, "", "")),
        ] + _merge_path_responses(["OPEN", "MERGED"])
        fake_cmd, calls = _fake_run_cmd(responses)
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["mergedBranches"] == ["feature/foo-42"]
        assert not any("gh pr edit 99 --base feature/foo-42" in c for c in calls)


class TestFailPrAndRestoreChildren:
    """The one sequence every give-up-after-the-sweep path owes: restore the
    persisted children, record the terminal status, clear the record, and halt
    rather than advance when a child could not be put back and has no queue
    entry to carry the failure."""

    def test_restores_from_the_persisted_record_not_a_passed_list(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_with_child(state_path)
        sm.add_swept_child(0, 99)
        sm.update_pr(1, base="main")
        fake_cmd, calls = _fake_run_cmd([("gh pr edit 99 --base feature/foo-42", (0, "", ""))])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        result = fail_pr_and_restore_children(sm, 0, "feature/foo-42", "main", "arming failed")

        assert result == EXIT_OK
        assert any("gh pr edit 99 --base feature/foo-42" in c for c in calls)
        assert sm.state["prs"][1]["base"] == "feature/foo-42"
        assert sm.state["prs"][0]["status"] == "failed"
        assert sm.state["prs"][0]["error"] == "arming failed"
        assert sm.state["prs"][0]["sweptChildren"] == []
        assert sm.state["currentIndex"] == 1
        assert "halted" not in sm.state

    def test_status_is_persisted_before_the_sweep_record_is_cleared(self, monkeypatch, tmp_path):
        """A crash between the two writes must leave a PR that reads as done
        for, not a cleared record with no terminal status -- the latter resumes
        as an ordinary skip and loses the only trace of the problem."""
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_with_child(state_path)
        sm.add_swept_child(0, 99)
        fake_cmd, _ = _fake_run_cmd([("gh pr edit 99 --base feature/foo-42", (0, "", ""))])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        snapshots = []
        original_save = StateManager._save

        def recording_save(self):
            original_save(self)
            snapshots.append((
                self.state["prs"][0].get("status"),
                list(self.state["prs"][0].get("sweptChildren", [])),
            ))

        monkeypatch.setattr(StateManager, "_save", recording_save)
        fail_pr_and_restore_children(sm, 0, "feature/foo-42", "main", "arming failed")

        cleared = next(i for i, (_, swept) in enumerate(snapshots) if swept == [])
        assert snapshots[cleared][0] == "failed"

    def test_unqueued_child_that_cannot_be_restored_halts_the_train(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        sm.add_swept_child(0, 999)  # never enqueued by this train
        fake_cmd, calls = _fake_run_cmd([
            ("gh pr edit 999 --base feature/foo-42", (1, "", "boom")),
            ("gh pr comment 999", (0, "", "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        with pytest.raises(SystemExit) as exc_info:
            fail_pr_and_restore_children(sm, 0, "feature/foo-42", "main", "arming failed")

        assert exc_info.value.code == EXIT_ERROR
        assert any("gh pr comment 999" in c for c in calls)
        assert sm.state["prs"][0]["status"] == "failed"
        assert "999" in sm.state["prs"][0]["error"]
        assert "999" in sm.state["halted"]["reason"]
        # A halt does not advance: the same PR is where a resume comes back to.
        assert sm.state["currentIndex"] == 0

    def test_caller_supplied_status_survives_a_clean_restore(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        fake_cmd, _ = _fake_run_cmd([])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        fail_pr_and_restore_children(
            sm, 0, "feature/foo-42", "main", "already merged/closed", status="skipped"
        )

        assert sm.state["prs"][0]["status"] == "skipped"


class TestHaltRefusesResume:
    """A halted state file is a stop the script may not drive past on its own.
    The fatal paths exit without advancing, so --resume re-enters process_pr on
    the same PR; without this it would read the persisted "failed" status, skip
    and advance -- silently passing exactly the condition the halt records."""

    def test_process_pr_exits_on_a_halted_state_file(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        sm.halt("child PR 999 is still on main under an unlanded parent")
        fake_cmd, calls = _fake_run_cmd([])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        with pytest.raises(SystemExit) as exc_info:
            process_pr(sm, 0, "main", state_path)

        assert exc_info.value.code == EXIT_ERROR
        assert calls == []
        assert sm.state["currentIndex"] == 0

    def test_halt_survives_a_round_trip_through_the_state_file(self, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        sm.halt("unresolvable child")

        reloaded = StateManager(state_path)
        reloaded.load()
        assert reloaded.state["halted"]["reason"] == "unresolvable child"

    def test_an_already_failed_pr_is_skipped_with_its_recorded_error(self, monkeypatch, tmp_path, capsys):
        """Without a halt, a PR marked failed in place by a parent's restore is
        skipped rather than armed, and the log quotes why instead of asserting
        one cause."""
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        sm.update_pr(0, status="failed", error="could not restore base to feature/parent-42")
        fake_cmd, calls = _fake_run_cmd([])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["currentIndex"] == 1
        assert not any("gh pr merge" in c for c in calls)
        assert "could not restore base to feature/parent-42" in capsys.readouterr().err


class TestAutoMergeStateGuardsTheRestore:
    """A --resume after an auto-merge timeout re-issues `gh pr merge --auto` on
    a PR that is still armed. A non-zero rc there does not mean "never armed",
    and restoring children onto a branch GitHub is about to delete is the
    closure the sweep exists to prevent."""

    def test_reports_enabled_disabled_and_unknown(self, monkeypatch):
        for stdout, expected in [
            ('{"enabledAt":"2026-09-22T00:00:00Z"}', True),
            ("null", False),
            ("", False),
        ]:
            fake_cmd, _ = _fake_run_cmd([("--json autoMergeRequest", (0, stdout, ""))])
            monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
            assert auto_merge_state(42) is expected

        fake_cmd, _ = _fake_run_cmd([("--json autoMergeRequest", (1, "", "rate limited"))])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        assert auto_merge_state(42) is None

    def test_already_armed_arming_failure_leaves_children_on_target(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_with_child(state_path)
        responses = [
            ("gh pr list --base feature/foo-42 --state open", (0, "99", "")),
            ("gh pr edit 99 --base main", (0, "", "")),
            ("gh pr merge", (1, "", "auto-merge is already enabled")),
            ("--json autoMergeRequest", (0, '{"enabledAt":"2026-09-22T00:00:00Z"}', "")),
        ] + _merge_path_responses(["OPEN", "MERGED"])
        fake_cmd, calls = _fake_run_cmd(responses)
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "merged"
        assert not any("gh pr edit 99 --base feature/foo-42" in c for c in calls)
        assert sm.state["prs"][1]["base"] == "main"

    def test_unknown_auto_merge_state_halts_rather_than_guessing(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_with_child(state_path)
        responses = [
            ("gh pr list --base feature/foo-42 --state open", (0, "99", "")),
            ("gh pr edit 99 --base main", (0, "", "")),
            ("gh pr merge", (1, "", "transient")),
            ("--json autoMergeRequest", (1, "", "rate limited")),
        ] + _merge_path_responses(["OPEN"])
        fake_cmd, calls = _fake_run_cmd(responses)
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        with pytest.raises(SystemExit) as exc_info:
            process_pr(sm, 0, "main", state_path)

        assert exc_info.value.code == EXIT_ERROR
        assert sm.state["prs"][0]["status"] == "failed"
        assert "halted" in sm.state
        assert not any("gh pr edit 99 --base feature/foo-42" in c for c in calls)


class TestArmingVerifiedAfterZeroExit:
    """A zero exit from `gh pr merge --auto` is not proof auto-merge was armed
    (#2801): on a PR already CLEAN it merges immediately instead. The train
    must ask GitHub what happened rather than assume, and record the
    immediate-merge outcome without burning a poll."""

    def test_immediate_merge_is_recorded_without_waiting(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        fake_cmd, calls = _fake_run_cmd(
            _merge_path_responses(["OPEN", "MERGED"], auto_merge_armed=False)
        )
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)
        waits = []
        monkeypatch.setattr(
            "merge_train.wait_for_merge",
            lambda pr_num, *a, **kw: waits.append(pr_num) or "merged",
        )

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "merged"
        assert sm.state["mergedBranches"] == ["feature/foo-42"]
        assert sm.state["currentIndex"] == 1
        # The zero exit must be verified rather than trusted -- GitHub is
        # consulted for the arming outcome instead of assuming it took.
        assert any("--json autoMergeRequest" in c for c in calls)
        # The already-merged outcome is taken from that verification, so no
        # poll is entered at all.
        assert waits == []

    def test_immediate_merge_leaves_swept_children_retargeted(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_with_child(state_path)
        responses = [
            ("gh pr list --base feature/foo-42 --state open", (0, "99", "")),
            ("gh pr edit 99 --base main", (0, "", "")),
        ] + _merge_path_responses(["OPEN", "MERGED"], auto_merge_armed=False)
        fake_cmd, calls = _fake_run_cmd(responses)
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "merged"
        assert not any("gh pr edit 99 --base feature/foo-42" in c for c in calls)
        assert sm.state["prs"][1]["base"] == "main"
        assert any("--json autoMergeRequest" in c for c in calls)

    def test_unknown_auto_merge_state_falls_through_to_the_poll(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        responses = [
            ("--json autoMergeRequest", (1, "", "rate limited")),
        ] + _merge_path_responses(["OPEN", "OPEN", "MERGED"])
        fake_cmd, calls = _fake_run_cmd(responses)
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "merged"
        assert "halted" not in sm.state
        assert any("--json autoMergeRequest" in c for c in calls)

    def test_armed_pr_still_waits_for_merged(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        fake_cmd, calls = _fake_run_cmd(
            _merge_path_responses(["OPEN", "MERGED"], auto_merge_armed=True)
        )
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert any("--json autoMergeRequest" in c for c in calls)
        merge_call_index = next(i for i, c in enumerate(calls) if "gh pr merge" in c)
        assert any(
            "gh pr view 42 --json state -q .state" in c
            for c in calls[merge_call_index + 1 :]
        )

    def test_unarmed_open_pr_fails_instead_of_waiting(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        fake_cmd, calls = _fake_run_cmd(
            _merge_path_responses(["OPEN", "OPEN"], auto_merge_armed=False)
        )
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)
        monkeypatch.setattr("merge_train.MERGE_POLL_TIMEOUT_SECONDS", 0)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "failed"
        assert "auto-merge was not armed" in sm.state["prs"][0]["error"]
        assert sm.state["currentIndex"] == 1

    def test_unarmed_open_pr_restores_swept_children(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_with_child(state_path)
        responses = [
            ("gh pr list --base feature/foo-42 --state open", (0, "99", "")),
            ("gh pr edit 99 --base main", (0, "", "")),
            ("gh pr edit 99 --base feature/foo-42", (0, "", "")),
        ] + _merge_path_responses(["OPEN", "OPEN"], auto_merge_armed=False)
        fake_cmd, calls = _fake_run_cmd(responses)
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)
        monkeypatch.setattr("merge_train.MERGE_POLL_TIMEOUT_SECONDS", 0)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert any("gh pr edit 99 --base feature/foo-42" in c for c in calls)
        assert sm.state["prs"][1]["base"] == "feature/foo-42"

    def test_closed_after_unarmed_exit_is_still_handled_by_the_poll(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        fake_cmd, calls = _fake_run_cmd(
            _merge_path_responses(["OPEN", "CLOSED"], auto_merge_armed=False)
        )
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)
        monkeypatch.setattr("merge_train.MERGE_POLL_TIMEOUT_SECONDS", 0)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "failed"
        assert "closed" in sm.state["prs"][0]["error"].lower()
        assert sm.state["currentIndex"] == 1


class TestCheckTaigaStatus:
    """check_taiga_status reads status-level is_closed, not item-level."""

    def test_uses_status_extra_info_is_closed(self, monkeypatch):
        """When item-level is_closed is False but status is Done (closed), return True."""
        fake_cmd, _ = _fake_run_cmd([
            ("scripts/taiga-story.sh", (0, json.dumps({
                "is_closed": False,
                "status_extra_info": {"name": "Done", "is_closed": True},
                "status": 17,
            }), "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        is_closed, status_name, error = check_taiga_status(1200)
        assert is_closed is True
        assert status_name == "Done"
        assert error is None

    def test_falls_back_to_item_is_closed_when_no_status_extra_info(self, monkeypatch):
        """When status_extra_info is missing, fall back to item-level is_closed."""
        fake_cmd, _ = _fake_run_cmd([
            ("scripts/taiga-story.sh", (0, json.dumps({
                "is_closed": True,
                "status": 17,
            }), "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        is_closed, status_name, error = check_taiga_status(1200)
        assert is_closed is True
        assert error is None

    def test_falls_back_to_issue_when_story_lookup_404s(self, monkeypatch):
        """A 404 (exit 44) on the user-story lookup must retry as an issue."""
        fake_cmd, calls = _fake_run_cmd([
            ("scripts/taiga-story.sh 1200 39 --issue", (0, json.dumps({
                "is_closed": True,
                "status_extra_info": {"name": "Done", "is_closed": True},
                "status": 17,
            }), "")),
            ("scripts/taiga-story.sh", (44, "", "No user story found with ref #1200 in project 39")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        is_closed, status_name, error = check_taiga_status(1200)
        assert is_closed is True
        assert status_name == "Done"
        assert error is None
        assert any("--issue" in c for c in calls)

    def test_returns_not_found_when_both_lookups_404(self, monkeypatch):
        """A 404 on both the user-story and issue lookups reports not_found, not an error."""
        fake_cmd, _ = _fake_run_cmd([
            ("scripts/taiga-story.sh", (44, "", "No user story found with ref #1200 in project 39")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        is_closed, status_name, error = check_taiga_status(1200)
        assert (is_closed, status_name, error) == (False, "not_found", None)

    def test_real_error_is_not_treated_as_404(self, monkeypatch):
        """A non-404 failure must fail fast, not be retried as an issue lookup."""
        fake_cmd, calls = _fake_run_cmd([
            ("scripts/taiga-story.sh", (1, "", "Taiga API error: HTTP 500")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        is_closed, status_name, error = check_taiga_status(1200)
        assert error is not None
        assert "Taiga lookup failed" in error
        assert not any("--issue" in c for c in calls)


class TestTaigaGateRunsBeforeCiAndRebase:
    """The Taiga Done/Closed gate runs before any CI polling, checkout, rebase,
    build, force-push, or merge, so a PR the train will skip never costs a
    remediation agent or a branch rewrite (bug #2900)."""

    def test_not_done_item_is_skipped_before_any_ci_or_rebase(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)

        fake_cmd, calls = _fake_run_cmd([
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            ("scripts/taiga-story.sh", (0, json.dumps({
                "is_closed": False,
                "status_extra_info": {"name": "In progress", "is_closed": False},
                "status": 2,
            }), "")),
            # Would force a remediation dispatch if the gate did not run first.
            ("monitor-pr-stability.sh", (0, "CHECKS_FAILING:build", "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        action_requests = []
        monkeypatch.setattr(
            "merge_train.write_action_request",
            lambda *a, **kw: action_requests.append((a, kw)),
        )

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "skipped"
        assert "not in Done/Closed" in sm.state["prs"][0]["error"]
        assert "In progress" in sm.state["prs"][0]["error"]
        assert sm.state["currentIndex"] == 1
        assert action_requests == []
        for forbidden in ("monitor-pr-stability.sh", "git checkout", "git rebase",
                          "git push", "python -m py_compile", "gh pr merge"):
            assert not any(forbidden in c for c in calls), forbidden
        assert any("gh pr comment 42" in c for c in calls)

    def test_missing_taiga_ref_is_skipped_before_any_ci_or_rebase(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = StateManager(state_path)
        sm.init(
            [{"number": 42, "title": "No ref here", "branch": "feature/no-ref", "body": ""}],
            target="main",
        )

        fake_cmd, calls = _fake_run_cmd([
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            ("monitor-pr-stability.sh", (0, "CHECKS_FAILING:build", "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.write_action_request", lambda *a, **kw: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "skipped"
        assert "no Taiga item reference" in sm.state["prs"][0]["error"]
        for forbidden in ("monitor-pr-stability.sh", "git checkout", "git push"):
            assert not any(forbidden in c for c in calls), forbidden

    def test_taiga_lookup_error_fails_before_any_ci_or_rebase(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)

        fake_cmd, calls = _fake_run_cmd([
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            ("scripts/taiga-story.sh", (1, "", "Taiga API error: HTTP 500")),
            ("monitor-pr-stability.sh", (0, "CHECKS_FAILING:build", "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.write_action_request", lambda *a, **kw: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "failed"
        assert "Taiga lookup failed" in sm.state["prs"][0]["error"]
        for forbidden in ("monitor-pr-stability.sh", "git checkout"):
            assert not any(forbidden in c for c in calls), forbidden

    def test_taiga_gate_precedes_pre_rebase_ci_check(self, monkeypatch, tmp_path):
        """On the happy path the gate is still queried before the CI check."""
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        fake_cmd, calls = _fake_run_cmd(_merge_path_responses(["OPEN", "MERGED"]))
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "merged"
        taiga_at = next(i for i, c in enumerate(calls) if "scripts/taiga-story.sh" in c)
        ci_at = next(i for i, c in enumerate(calls) if "monitor-pr-stability.sh" in c)
        assert taiga_at < ci_at

    def test_integration_pr_still_bypasses_taiga_gate(self, monkeypatch, tmp_path):
        """A synthetic integration PR has no Taiga item of its own, so the gate
        is bypassed at its new location just as it was at the old one."""
        state_path = str(tmp_path / "state.json")
        branch = "epic/2004-integration-2748"
        sm = _make_sm_for_process_pr(state_path, branch=branch)

        fake_cmd, calls = _fake_run_cmd([
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            ("git diff --quiet", (1, "", "")),  # real diff remains: not contained
            *_integration_parents_landed_responses(),
            ("gh pr view 42 --json state -q .state", (0, "MERGED", "")),
            ("--json baseRefName", (0, "main", "")),
            ("monitor-pr-stability.sh", (0, "STABLE", "")),
            ("git checkout " + branch, (0, "", "")),
            ("git reset --hard upstream/", (0, "", "")),
            ("git merge-base --is-ancestor", (0, "", "")),  # up to date
            ("gh pr merge", (0, "", "")),
            ("--json autoMergeRequest", (0, '{"enabledAt":"2026-09-22T00:00:00Z"}', "")),
            ("git checkout main", (0, "", "")),
            ("git pull upstream main", (0, "", "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "merged"
        # No Taiga status gate for the synthetic PR; the only taiga-story.sh call
        # is the epic read that names its parents.
        assert [c for c in calls if "scripts/taiga-story.sh" in c] == [
            "scripts/taiga-story.sh 2004 39 --epic"
        ]

    @pytest.mark.parametrize("parent_prs, expected", [
        ([(11, "MERGED", "feat: a (#100)", "feature/a-100")], "#101"),
        ([(11, "MERGED", "feat: a (#100)", "feature/a-100"),
          (12, "OPEN", "feat: b (#101)", "feature/b-101")], "#101"),
        ([(11, "MERGED", "feat: a (#100)", "feature/a-100"),
          (12, "CLOSED", "feat: b (#101)", "feature/b-101")], "#101"),
        ([], "#100, #101"),
    ])
    def test_integration_pr_with_unlanded_parent_is_skipped(self, monkeypatch, tmp_path, parent_prs, expected):
        """Merging the integration PR would land a parent's unreviewed commits."""
        state_path = str(tmp_path / "state.json")
        branch = "epic/2004-integration-2748"
        sm = _make_sm_for_process_pr(state_path, branch=branch)
        fake_cmd, calls = _fake_run_cmd([
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            ("git diff --quiet", (1, "", "")),
            ("--epic", (0, _epic_json(INTEGRATION_EPIC_DESCRIPTION), "")),
            ("pr list --state all", (0, _all_prs_payload(parent_prs), "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "skipped"
        assert expected in sm.state["prs"][0]["error"]
        for forbidden in ("monitor-pr-stability.sh", "git checkout", "git push", "gh pr merge"):
            assert not any(forbidden in c for c in calls), forbidden

    @pytest.mark.parametrize("responses", [
        [("--epic", (1, "", "Taiga down"))],
        [("--epic", (0, _epic_json(INTEGRATION_EPIC_DESCRIPTION), "")),
         ("pr list --state all", (1, "", "gh down"))],
        [("--epic", (0, _epic_json(INTEGRATION_EPIC_DESCRIPTION), "")),
         ("pr list --state all", (0, "not json", ""))],
    ])
    def test_integration_pr_fails_closed_when_parents_cannot_be_verified(self, monkeypatch, tmp_path, responses):
        state_path = str(tmp_path / "state.json")
        branch = "epic/2004-integration-2748"
        sm = _make_sm_for_process_pr(state_path, branch=branch)
        fake_cmd, calls = _fake_run_cmd([
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            ("git diff --quiet", (1, "", "")),
        ] + responses)
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "skipped"
        assert "parent" in sm.state["prs"][0]["error"]
        assert not any("gh pr merge" in c for c in calls)


class TestCheckoutFailure:
    """Checkout failure error messages include stderr and detect worktree conflicts."""

    def test_checkout_failure_includes_stderr(self, monkeypatch, tmp_path):
        """Generic checkout failure includes the stderr message."""
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)

        fake_cmd, _ = _fake_run_cmd([
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            _taiga_done_response(),
            ("gh pr view 42 --json state -q .state", (0, "MERGED", "")),
            ("--json baseRefName", (0, "main", "")),
            ("monitor-pr-stability.sh", (0, "STABLE", "")),
            ("git checkout feature/foo-42", (128, "", "error: pathspec 'feature/foo-42' did not match any file(s) known to git")),
        ])

        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "failed"
        assert "pathspec" in sm.state["prs"][0]["error"]

    def test_checkout_worktree_conflict_provides_guidance(self, monkeypatch, tmp_path):
        """Worktree conflict gives actionable error with worktree path."""
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)

        fake_cmd, _ = _fake_run_cmd([
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            _taiga_done_response(),
            ("gh pr view 42 --json state -q .state", (0, "MERGED", "")),
            ("--json baseRefName", (0, "main", "")),
            ("monitor-pr-stability.sh", (0, "STABLE", "")),
            ("git checkout feature/foo-42", (128, "", "fatal: 'feature/foo-42' is already checked out at '/home/user/other-worktree'")),
        ])

        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "failed"
        assert "worktree" in sm.state["prs"][0]["error"].lower()
        assert "Remove the worktree" in sm.state["prs"][0]["error"]

    def test_process_pr_merges_with_status_extra_info_divergence(self, monkeypatch, tmp_path):
        """PR merges when item is_closed=False but status is Done (is_closed=True)."""
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)

        fake_cmd, _ = _fake_run_cmd([
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            ("gh pr view 42 --json state -q .state", (0, "MERGED", "")),
            ("--json baseRefName", (0, "main", "")),
            ("monitor-pr-stability.sh", (0, "STABLE", "")),
            ("git checkout feature/foo-42", (0, "", "")),
            ("git reset --hard upstream/", (0, "", "")),
            ("git merge-base --is-ancestor", (0, "", "")),
            ("scripts/taiga-story.sh", (0, json.dumps({
                "is_closed": False,
                "status_extra_info": {"name": "Done", "is_closed": True, "color": "#A8E440"},
                "status": 17,
            }), "")),
            ("gh pr merge", (0, "", "")),
            ("git checkout main", (0, "", "")),
            ("git pull upstream main", (0, "", "")),
        ])

        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "merged"


class TestBuildFailureCapturesOutput:
    """A failed post-rebase build must surface its output to the wrapper."""

    BUILD_STDOUT = "Compiling...\nerror E0001: SyntaxError: invalid syntax (bots/chismebot.py, line 12)"
    BUILD_STDERR = "warning: something"

    def _run_failing_build(self, monkeypatch, tmp_path, ts=1700000000):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        monkeypatch.setattr("merge_train.time.time", lambda: ts)

        fake_cmd, calls = _fake_run_cmd([
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            _taiga_done_response(),
            ("gh pr view 42 --json state -q .state", (0, "MERGED", "")),
            ("--json baseRefName", (0, "main", "")),
            ("monitor-pr-stability.sh", (0, "STABLE", "")),
            ("git checkout feature/foo-42", (0, "", "")),
            ("git reset --hard upstream/", (0, "", "")),
            ("git merge-base --is-ancestor", (1, "", "")),
            ("git rev-parse HEAD", (0, "abc123", "")),
            ("git rebase upstream/main", (0, "", "")),
            ("git diff --check", (0, "", "")),
            ("python -m py_compile", (1, self.BUILD_STDOUT, self.BUILD_STDERR)),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        action_path = action_path_for(state_path)
        build_log = f"/tmp/merge-train-{ts}-build-42.log"
        try:
            result = process_pr(sm, 0, "main", state_path)
            with open(action_path) as f:
                action = json.load(f)
            log_text = open(build_log).read() if os.path.exists(build_log) else None
        finally:
            for path in (action_path, build_log):
                if os.path.exists(path):
                    os.remove(path)
        return result, action, build_log, log_text

    def test_build_failure_writes_log_and_references_it_in_action_request(self, monkeypatch, tmp_path):
        result, action, build_log, log_text = self._run_failing_build(monkeypatch, tmp_path)

        assert result == EXIT_REMEDIATION
        details = action["details"]
        assert details["failure_type"] == "build_post_rebase"
        assert details["build_log"] == build_log
        assert log_text is not None
        assert self.BUILD_STDOUT in log_text
        assert self.BUILD_STDERR in log_text
        assert "error E0001" in details["build_output_tail"]

    def test_build_failure_logs_output_tail(self, monkeypatch, tmp_path, capsys):
        _, _, build_log, _ = self._run_failing_build(monkeypatch, tmp_path)

        captured = capsys.readouterr()
        assert "error E0001" in captured.err
        assert build_log in captured.err

    def test_write_build_log_truncates_to_tail(self, tmp_path):
        log_path = str(tmp_path / "build.log")
        stdout = "\n".join(f"line {i}" for i in range(1, 251))

        tail = write_build_log(log_path, stdout, "")

        tail_lines = tail.splitlines()
        assert len(tail_lines) == BUILD_LOG_TAIL_LINES
        assert tail_lines[-1] == "line 250"
        assert open(log_path).read().splitlines() == stdout.splitlines()



class TestDecideRetarget:
    """decide_retarget is a pure function -- no run_cmd calls."""

    def test_retargets_when_base_in_merged_branches(self):
        pr = {"base": "feature/parent-1"}
        assert decide_retarget(pr, ["feature/parent-1"], target="main") == "main"

    def test_none_when_base_not_in_merged_branches(self):
        pr = {"base": "feature/parent-1"}
        assert decide_retarget(pr, ["feature/other"], target="main") is None

    def test_none_when_base_equals_target(self):
        pr = {"base": "main"}
        assert decide_retarget(pr, ["main"], target="main") is None

    def test_none_when_base_missing(self):
        pr = {}
        assert decide_retarget(pr, ["feature/parent-1"], target="main") is None

    def test_none_when_base_empty(self):
        pr = {"base": ""}
        assert decide_retarget(pr, ["feature/parent-1"], target="main") is None

    def test_falls_back_to_base_ref_name(self):
        pr = {"baseRefName": "feature/parent-1"}
        assert decide_retarget(pr, ["feature/parent-1"], target="main") == "main"

    def test_is_pure_no_run_cmd(self, monkeypatch):
        calls = []
        monkeypatch.setattr("merge_train.run_cmd", lambda *a, **k: calls.append(a) or (0, "", ""))
        decide_retarget({"base": "feature/parent-1"}, ["feature/parent-1"], target="main")
        assert calls == []


class TestFindOpenChildren:
    def test_returns_false_empty_on_failure(self, monkeypatch):
        fake_cmd, _ = _fake_run_cmd([
            ("gh pr list --base", (1, "", "error")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        ok, children = find_open_children("feature/parent-1")
        assert (ok, children) == (False, [])

    def test_empty_branch_short_circuits_without_shelling_out(self, monkeypatch):
        calls = []
        monkeypatch.setattr("merge_train.run_cmd", lambda *a, **k: calls.append(a) or (0, "", ""))
        ok, children = find_open_children("")
        assert (ok, children) == (True, [])
        assert calls == []

    def test_parses_numbers(self, monkeypatch):
        fake_cmd, calls = _fake_run_cmd([
            ("gh pr list --base", (0, "11\n12\n", "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        ok, children = find_open_children("feature/parent-1")
        assert (ok, children) == (True, [11, 12])
        # An explicit --limit is load-bearing: `gh pr list` defaults to 30, and a
        # silently truncated page is a child this sweep would leave unswept when
        # the parent lands and its base branch is deleted.
        assert f"--limit {OPEN_CHILDREN_LIMIT}" in calls[0]

    def test_ignores_non_numeric_lines(self, monkeypatch):
        fake_cmd, _ = _fake_run_cmd([
            ("gh pr list --base", (0, "11\nnot-a-number\n12\n", "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        ok, children = find_open_children("feature/parent-1")
        assert (ok, children) == (True, [11, 12])

    def test_truncated_page_fails_closed(self, monkeypatch):
        page = "\n".join(str(n) for n in range(1, OPEN_CHILDREN_LIMIT + 1))
        fake_cmd, _ = _fake_run_cmd([
            ("gh pr list --base", (0, page, "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        ok, children = find_open_children("feature/parent-1")
        assert (ok, children) == (False, [])

    def test_full_page_minus_one_is_not_truncated(self, monkeypatch):
        page = "\n".join(str(n) for n in range(1, OPEN_CHILDREN_LIMIT))
        fake_cmd, _ = _fake_run_cmd([
            ("gh pr list --base", (0, page, "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        ok, children = find_open_children("feature/parent-1")
        assert ok is True
        assert len(children) == OPEN_CHILDREN_LIMIT - 1


class TestBranchMergedOnRemote:
    def test_false_for_closed(self, monkeypatch):
        fake_cmd, _ = _fake_run_cmd([("gh pr list --head", (0, "CLOSED\tmain", ""))])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        assert branch_merged_on_remote("feature/parent-1", "main") is False

    def test_false_for_multiple_prs(self, monkeypatch):
        fake_cmd, _ = _fake_run_cmd([("gh pr list --head", (0, "MERGED\tmain\nMERGED\tmain", ""))])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        assert branch_merged_on_remote("feature/parent-1", "main") is False

    def test_false_for_empty_output(self, monkeypatch):
        fake_cmd, _ = _fake_run_cmd([("gh pr list --head", (0, "", ""))])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        assert branch_merged_on_remote("feature/parent-1", "main") is False

    def test_false_on_command_failure(self, monkeypatch):
        fake_cmd, _ = _fake_run_cmd([("gh pr list --head", (1, "", "error"))])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        assert branch_merged_on_remote("feature/parent-1", "main") is False

    def test_true_for_single_merged(self, monkeypatch):
        fake_cmd, _ = _fake_run_cmd([("gh pr list --head", (0, "MERGED\tmain", ""))])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        assert branch_merged_on_remote("feature/parent-1", "main") is True

    def test_false_when_merged_into_a_different_branch(self, monkeypatch):
        # A stacked PR merged by hand into its own parent branch, not the
        # train's target -- its commits are not necessarily on target yet.
        fake_cmd, _ = _fake_run_cmd([("gh pr list --head", (0, "MERGED\tfeature/parent-0", ""))])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        assert branch_merged_on_remote("feature/parent-1", "main") is False


class TestBranchContainedInTarget:
    """branch_contained_in_target compares trees two-dot, so a parent that
    landed via squash-merge still counts as contained."""

    def test_true_when_trees_identical(self, monkeypatch):
        fake_cmd, calls = _fake_run_cmd([("git diff --quiet", (0, "", ""))])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        assert branch_contained_in_target("epic/2004-integration-2748", "main") is True
        # Two-dot, not three-dot: a three-dot diff is taken against the merge
        # base, which still reports the files of a squash-merged parent.
        assert calls == ["git diff --quiet upstream/main upstream/epic/2004-integration-2748"]

    def test_false_when_diff_remains(self, monkeypatch):
        fake_cmd, _ = _fake_run_cmd([("git diff --quiet", (1, "", ""))])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        assert branch_contained_in_target("epic/2004-integration-2748", "main") is False


class TestRestoreChildrenBase:
    def test_edits_each_child_and_updates_queue_record(self, tmp_path, monkeypatch):
        state_path = str(tmp_path / "state.json")
        sm = StateManager(state_path)
        sm.init(
            [
                {"number": 11, "title": "Child 1", "branch": "feature/child-1", "base": "feature/parent-1"},
                {"number": 12, "title": "Child 2", "branch": "feature/child-2", "base": "feature/parent-1"},
            ],
            target="main",
        )
        fake_cmd, calls = _fake_run_cmd([
            ("gh pr edit", (0, "", "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        restore_children_base(sm, [11, 12], "feature/parent-1")

        assert any("gh pr edit 11 --base feature/parent-1" in c for c in calls)
        assert any("gh pr edit 12 --base feature/parent-1" in c for c in calls)
        assert sm.state["prs"][0]["base"] == "feature/parent-1"
        assert sm.state["prs"][1]["base"] == "feature/parent-1"

    def test_failed_edit_is_logged_and_loop_continues(self, tmp_path, monkeypatch):
        state_path = str(tmp_path / "state.json")
        sm = StateManager(state_path)
        sm.init(
            [
                {"number": 11, "title": "Child 1", "branch": "feature/child-1", "base": "main"},
                {"number": 12, "title": "Child 2", "branch": "feature/child-2", "base": "main"},
            ],
            target="main",
        )
        fake_cmd, calls = _fake_run_cmd([
            ("gh pr edit 11", (1, "", "boom")),
            ("gh pr edit 12", (0, "", "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        restore_children_base(sm, [11, 12], "feature/parent-1")

        # The failed child's queue record is left untouched.
        assert sm.state["prs"][0]["base"] == "main"
        assert sm.state["prs"][1]["base"] == "feature/parent-1"
        # A child that could not be restored is marked failed -- its actual
        # GitHub base is still the target, and reprocessing it as if nothing
        # happened would let it arm and land the unlanded parent's commits.
        assert sm.state["prs"][0]["status"] == "failed"
        assert "could not restore" in sm.state["prs"][0]["error"]
        assert sm.state["prs"][1]["status"] != "failed"

    def test_unqueued_child_restore_failure_warns_and_is_returned(self, tmp_path, monkeypatch):
        """find_open_children() deliberately catches PRs stacked on `branch`
        that were never enqueued. A restore failure for one of those has no
        queue record to mark failed, so it must be surfaced another way: a
        warning comment on the PR itself, and its number handed back to the
        caller so the train can stop instead of silently continuing."""
        state_path = str(tmp_path / "state.json")
        sm = StateManager(state_path)
        sm.init(
            [{"number": 11, "title": "Child 1", "branch": "feature/child-1", "base": "main"}],
            target="main",
        )
        fake_cmd, calls = _fake_run_cmd([
            ("gh pr edit 11", (0, "", "")),
            ("gh pr edit 999", (1, "", "boom")),
            ("gh pr comment 999", (0, "", "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        unresolvable = restore_children_base(sm, [11, 999], "feature/parent-1")

        assert unresolvable == [999]
        # The queued child is unaffected by the unqueued child's failure.
        assert sm.state["prs"][0]["base"] == "feature/parent-1"
        assert sm.state["prs"][0]["status"] != "failed"
        assert any("gh pr comment 999" in c for c in calls)

    def test_queued_child_restore_failure_returns_no_unresolvable(self, tmp_path, monkeypatch):
        state_path = str(tmp_path / "state.json")
        sm = StateManager(state_path)
        sm.init(
            [{"number": 11, "title": "Child 1", "branch": "feature/child-1", "base": "main"}],
            target="main",
        )
        fake_cmd, calls = _fake_run_cmd([
            ("gh pr edit 11", (1, "", "boom")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        unresolvable = restore_children_base(sm, [11], "feature/parent-1")

        # A restore failure for a *queued* child is already tracked via its
        # own failed status -- it is not also reported as unresolvable.
        assert unresolvable == []
        assert sm.state["prs"][0]["status"] == "failed"


class TestSweptChildrenPersistence:
    """sweptChildren survives in state so a crashed or timed-out invocation's
    sweep is not forgotten on resume (find_open_children() no longer finds a
    child once its base is target, so only the persisted record remembers it)."""

    def test_sweep_persists_each_child_immediately(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_with_child(state_path)
        responses = [
            ("gh pr list --base feature/foo-42 --state open", (0, "99", "")),
            ("gh pr edit 99 --base main", (0, "", "")),
        ] + _merge_path_responses(["OPEN", "MERGED"])
        fake_cmd, calls = _fake_run_cmd(responses)
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        # Merged successfully, so the children are correctly at target and the
        # bookkeeping is no longer needed.
        assert sm.state["prs"][0]["sweptChildren"] == []

    def test_resume_restores_children_swept_by_a_prior_invocation(self, monkeypatch, tmp_path):
        """Simulates: a prior run swept child #99 onto target, then crashed
        before this PR could merge or be marked failed. On resume, the PR is
        found CLOSED (e.g. a human closed it, or it was closed while the
        train was down) -- the resumed process_pr call must restore #99 using
        the persisted record, since its own in-memory sweep never happened."""
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_with_child(state_path)
        sm.add_swept_child(0, 99)
        sm.update_pr(1, base="main")  # what the prior sweep actually did on GitHub

        responses = [
            _pr_view_response("CLOSED"),
            ("gh pr edit 99 --base feature/foo-42", (0, "", "")),
        ]
        fake_cmd, calls = _fake_run_cmd(responses)
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert any("gh pr edit 99 --base feature/foo-42" in c for c in calls)
        assert sm.state["prs"][1]["base"] == "feature/foo-42"
        assert sm.state["prs"][0]["sweptChildren"] == []
        assert sm.state["prs"][0]["status"] == "skipped"

    def test_resume_restore_failure_for_unqueued_child_stops_the_train(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        sm.add_swept_child(0, 999)  # 999 was never enqueued by this train

        responses = [
            _pr_view_response("CLOSED"),
            ("gh pr edit 999 --base feature/foo-42", (1, "", "boom")),
            ("gh pr comment 999", (0, "", "")),
        ]
        fake_cmd, calls = _fake_run_cmd(responses)
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        with pytest.raises(SystemExit) as exc_info:
            process_pr(sm, 0, "main", state_path)

        assert exc_info.value.code == EXIT_ERROR
        assert any("gh pr comment 999" in c for c in calls)


class TestPreMergeLiveBaseGuard:
    """Immediately before arming, process_pr re-reads the PR's live base on
    GitHub and refuses to merge if it is not `target` -- gh pr merge lands
    into whatever base is actually set on GitHub, not into `target` by name,
    so recording the branch as landed without this check could be wrong."""

    def test_refuses_to_merge_when_live_base_is_not_target(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        responses = [
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            ("--json baseRefName", (0, "feature/parent-42", "")),
            ("monitor-pr-stability.sh", (0, "STABLE", "")),
            ("git checkout feature/foo-42", (0, "", "")),
            ("git reset --hard upstream/", (0, "", "")),
            ("git merge-base --is-ancestor", (0, "", "")),
            ("scripts/taiga-story.sh", (0, json.dumps({
                "is_closed": True,
                "status_extra_info": {"name": "Done", "color": "#44AD54"},
                "status": 4,
            }), "")),
            ("gh pr list --base feature/foo-42 --state open", (0, "", "")),
        ]
        fake_cmd, calls = _fake_run_cmd(responses)
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "failed"
        assert "refusing to merge" in sm.state["prs"][0]["error"]
        assert not any("gh pr merge" in c for c in calls)


class TestProcessPrSkipsAlreadyFailed:
    """A PR marked failed before its own turn -- e.g. by restore_children_base()
    giving up on it after its parent didn't land -- must not be reprocessed as
    if it were still a normal open PR."""

    def test_already_failed_pr_is_skipped_without_reprocessing(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        sm.update_pr(0, status="failed", error="could not restore base to feature/parent-1: boom")
        fake_cmd, calls = _fake_run_cmd([])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert calls == []
        assert sm.state["prs"][0]["status"] == "failed"
        assert sm.state["currentIndex"] == 1


class TestProcessPrRetarget:
    """process_pr retargets a PR whose parent branch has landed, before the
    pre-rebase stability check reads CI for the (possibly stale) old base."""

    def _sm(self, state_path, base="feature/parent-42"):
        sm = StateManager(state_path)
        sm.init(
            [{"number": 42, "title": "Fix #42 foo", "branch": "feature/foo-42", "base": base}],
            target="main",
        )
        return sm

    def test_retargets_when_base_in_merged_branches(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = self._sm(state_path)
        sm.state["mergedBranches"].append("feature/parent-42")

        fake_cmd, calls = _fake_run_cmd([
            _pr_view_response("OPEN", "feature/parent-42"),
            _taiga_done_response(),
            ("gh pr edit", (0, "", "")),
            ("monitor-pr-stability.sh", (0, "CHECKS_FAILING:build", "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.write_action_request", lambda *a, **kw: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_REMEDIATION
        assert gh_cmd("pr edit 42 --base main") in calls
        assert sm.state["prs"][0]["base"] == "main"
        assert sm.state["prs"][0]["stacked"] is True
        # A base already known landed skips the gone-and-merged remote probe.
        assert not any("gh pr list --head" in c for c in calls)

    def test_no_retarget_when_base_still_open(self, monkeypatch, tmp_path):
        """A base whose PR has not landed stops the PR where it stands: no
        retarget, and -- the point of stopping here -- no stability check,
        rebase, force-push or CI run on a branch that would have had the
        unlanded parent's commits replayed into it."""
        state_path = str(tmp_path / "state.json")
        sm = self._sm(state_path)

        fake_cmd, calls = _fake_run_cmd([
            _pr_view_response("OPEN", "feature/parent-42"),
            ("monitor-pr-stability.sh", (0, "CHECKS_FAILING:build", "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.write_action_request", lambda *a, **kw: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "skipped"
        assert "has not landed" in sm.state["prs"][0]["error"]
        assert sm.state["currentIndex"] == 1
        assert not any("gh pr edit" in c for c in calls)
        assert not any("monitor-pr-stability.sh" in c for c in calls)
        assert not any("git push" in c for c in calls)
        assert not any("gh pr merge" in c for c in calls)
        assert sm.state["prs"][0]["base"] == "feature/parent-42"

    def test_retargets_when_base_gone_and_merged(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = self._sm(state_path)

        fake_cmd, calls = _fake_run_cmd([
            _pr_view_response("OPEN", "feature/parent-42"),
            _taiga_done_response(),
            ("gh pr list --head", (0, "MERGED\tmain", "")),
            ("gh pr edit", (0, "", "")),
            ("monitor-pr-stability.sh", (0, "CHECKS_FAILING:build", "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.write_action_request", lambda *a, **kw: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_REMEDIATION
        assert gh_cmd("pr edit 42 --base main") in calls
        assert sm.state["prs"][0]["base"] == "main"
        # The gone-and-merged probe result is a local decision, never persisted.
        assert "feature/parent-42" not in sm.state["mergedBranches"]

    def test_no_retarget_when_base_gone_but_closed(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = self._sm(state_path)

        fake_cmd, calls = _fake_run_cmd([
            _pr_view_response("OPEN", "feature/parent-42"),
            ("gh pr list --head", (0, "CLOSED", "")),
            ("monitor-pr-stability.sh", (0, "CHECKS_FAILING:build", "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.write_action_request", lambda *a, **kw: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "skipped"
        assert not any("gh pr edit" in c for c in calls)
        assert not any("monitor-pr-stability.sh" in c for c in calls)
        assert sm.state["prs"][0]["base"] == "feature/parent-42"

    def test_no_retarget_when_already_on_target(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = self._sm(state_path, base="main")

        fake_cmd, calls = _fake_run_cmd([
            _pr_view_response("OPEN"),
            _taiga_done_response(),
            ("monitor-pr-stability.sh", (0, "CHECKS_FAILING:build", "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.write_action_request", lambda *a, **kw: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_REMEDIATION
        assert not any("gh pr edit" in c for c in calls)
        assert sm.state["prs"][0]["base"] == "main"
        assert sm.state["prs"][0]["stacked"] is False

    def test_failed_edit_marks_pr_failed_and_stops_before_merge(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = self._sm(state_path)
        sm.state["mergedBranches"].append("feature/parent-42")

        fake_cmd, calls = _fake_run_cmd([
            _pr_view_response("OPEN", "feature/parent-42"),
            ("gh pr edit", (1, "", "branch protection blocks retarget")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "failed"
        assert "retarget to main failed" in sm.state["prs"][0]["error"]
        assert sm.state["currentIndex"] == 1
        assert not any("gh pr merge" in c for c in calls)
        assert not any("monitor-pr-stability.sh" in c for c in calls)

    def test_merged_pr_records_branch(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = self._sm(state_path)

        fake_cmd, calls = _fake_run_cmd([
            _pr_view_response("MERGED"),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "skipped"
        assert sm.state["mergedBranches"] == ["feature/foo-42"]

    def test_merged_into_another_branch_does_not_record_branch(self, monkeypatch, tmp_path):
        """MERGED says the PR merged, not where. A stacked PR merged by hand
        into its own parent branch has not reached the target, and recording it
        would let a descendant retarget straight onto the target carrying that
        unlanded ancestry."""
        state_path = str(tmp_path / "state.json")
        sm = self._sm(state_path)

        fake_cmd, _ = _fake_run_cmd([
            _pr_view_response("MERGED", "feature/parent-42"),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "skipped"
        assert sm.state["mergedBranches"] == []

    def test_closed_pr_does_not_record_branch(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = self._sm(state_path)

        fake_cmd, calls = _fake_run_cmd([
            _pr_view_response("CLOSED"),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "skipped"
        assert sm.state["mergedBranches"] == []

    def test_stacked_flag_set_when_live_base_not_target(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = self._sm(state_path, base="main")

        fake_cmd, calls = _fake_run_cmd([
            _pr_view_response("OPEN", "feature/parent-42"),
            ("monitor-pr-stability.sh", (0, "CHECKS_FAILING:build", "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.write_action_request", lambda *a, **kw: None)

        result = process_pr(sm, 0, "main", state_path)

        assert result == EXIT_OK
        assert sm.state["prs"][0]["stacked"] is True
        assert sm.state["prs"][0]["base"] == "feature/parent-42"


class TestSweepDocstringsMatchBehaviour:
    """The sweep's prose must describe the code as it is.

    The sweep writes only `base`; the sticky `stacked` flag is owned by
    enqueue. And who deletes the branch decides the outcome: GitHub's own
    delete_branch_on_merge retargets open children and leaves them open,
    while a client-side deletion closes them (#2801).
    """

    def test_find_open_children_does_not_claim_the_sweep_records_stacked(self):
        doc = find_open_children.__doc__
        assert "records the child as stacked" not in doc
        assert "--squash when the queue reaches it" not in doc

    def test_restore_children_base_does_not_claim_deletion_closes_children(self):
        doc = restore_children_base.__doc__
        assert "cannot close it" not in doc
        assert "the closure this sweep exists to prevent" not in doc


class TestCholobotsRepoWiring:
    """Every gh call drops GH_TOKEN and names MrsBohemian/Cholobots; git uses the upstream remote."""

    def test_gh_cmd_prefix_and_repo(self):
        assert gh_cmd("pr view 7 --json state") == (
            "env -u GH_TOKEN gh pr view 7 --json state --repo MrsBohemian/Cholobots"
        )

    def test_every_gh_call_in_a_merge_run_is_wrapped(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        fake_cmd, calls = _fake_run_cmd(_merge_path_responses(["OPEN", "MERGED"]))
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.sleep", lambda s: None)

        assert process_pr(sm, 0, "main", state_path) == EXIT_OK

        gh_calls = [c for c in calls if " gh " in f" {c}"]
        assert gh_calls
        for call in gh_calls:
            assert call.startswith("env -u GH_TOKEN gh "), call
            assert call.endswith("--repo MrsBohemian/Cholobots"), call
        assert "git fetch upstream" in calls
        assert "git pull upstream main" in calls
        assert "git push origin main" in calls
        assert not any("Alterna" in c for c in calls)


class TestBuildChecks:
    """The post-rebase check is py_compile, plus pytest once a unit-test suite exists."""

    def test_has_unit_tests_ignores_script_tests(self, tmp_path):
        (tmp_path / "tests" / "scripts").mkdir(parents=True)
        (tmp_path / "tests" / "scripts" / "test_x.py").write_text("")
        assert has_unit_tests(str(tmp_path)) is False
        (tmp_path / "tests" / "test_metiche.py").write_text("")
        assert has_unit_tests(str(tmp_path)) is True

    def test_no_tests_dir_means_no_unit_tests(self, tmp_path):
        assert has_unit_tests(str(tmp_path)) is False

    def test_compile_failure_skips_pytest(self, monkeypatch):
        calls = []
        monkeypatch.setattr("merge_train.run_cmd", lambda cmd, check=True: calls.append(cmd) or (1, "", "boom"))
        monkeypatch.setattr("merge_train.has_unit_tests", lambda root=".": True)
        assert run_build_checks() == (1, "", "boom")
        assert len(calls) == 1
        # No shell expands the wildcard, so run_build_checks does it.
        assert calls[0].startswith("python -m py_compile cholobots.py config.py db/database.py ")
        assert "bots/*.py" not in calls[0]

    def test_pytest_runs_only_when_unit_tests_exist(self, monkeypatch):
        calls = []
        monkeypatch.setattr("merge_train.run_cmd", lambda cmd, check=True: calls.append(cmd) or (0, "", ""))
        monkeypatch.setattr("merge_train.has_unit_tests", lambda root=".": False)
        run_build_checks()
        assert len(calls) == 1
        monkeypatch.setattr("merge_train.has_unit_tests", lambda root=".": True)
        run_build_checks()
        assert calls[-1] == "python -m pytest -q tests --ignore=tests/scripts"


class TestRunCmdHasNoShell:
    """A branch name is untrusted text: `;`, `$(...)` and quotes are all legal in git refs."""

    def test_metacharacters_in_a_quoted_argument_stay_one_argument(self, tmp_path):
        from merge_train import run_cmd, _q
        marker = tmp_path / "pwned"
        hostile = f"x; touch {marker}; $(touch {marker}) `touch {marker}` 'q\"d"
        rc, out, _ = run_cmd(f"printf %s {_q(hostile)}")
        assert rc == 0
        assert out == hostile
        assert not marker.exists()

    def test_unquoted_metacharacters_are_not_interpreted(self, tmp_path):
        from merge_train import run_cmd
        marker = tmp_path / "pwned"
        rc, out, _ = run_cmd(f"echo hi; touch {marker}", check=False)
        assert not marker.exists()
        assert out == f"hi; touch {marker}"

    def test_branch_with_semicolon_reaches_git_as_a_single_argument(self, monkeypatch):
        """End to end through process_pr's checkout: the hostile ref is quoted."""
        import shlex
        from merge_train import _q
        branch = "feature/x;touch${IFS}pwned-42"
        assert shlex.split(f"git checkout {_q(branch)}") == ["git", "checkout", branch]


class TestExplicitTrainLookupFailure:
    """A failed `gh pr view` must not make an explicit train look like a success."""

    def _run_main(self, monkeypatch, responses, argv):
        import merge_train
        fake_cmd, calls = _fake_run_cmd(responses)
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr(sys, "argv", ["merge_train.py"] + argv)
        with pytest.raises(SystemExit) as exc:
            merge_train.main()
        return exc.value.code, calls

    def test_all_lookups_failing_exits_with_error(self, monkeypatch):
        code, _ = self._run_main(monkeypatch, [("gh pr view", (1, "", "HTTP 502"))], ["41", "42"])
        assert code == EXIT_ERROR

    def test_one_lookup_failing_refuses_a_partial_train(self, monkeypatch):
        view = json.dumps({"number": 41, "title": "t", "headRefName": "feature/a-41",
                           "baseRefName": "main", "state": "OPEN", "url": "u", "body": ""})
        code, calls = self._run_main(monkeypatch, [
            ("pr view 41", (0, view, "")),
            ("pr view 42", (1, "", "HTTP 502")),
        ], ["41", "42"])
        assert code == EXIT_ERROR
        assert not any("git " in c for c in calls)

    def test_unparseable_lookup_exits_with_error(self, monkeypatch):
        code, _ = self._run_main(monkeypatch, [("gh pr view", (0, "<html>", ""))], ["41"])
        assert code == EXIT_ERROR


class TestEpicDiscoveryTruncation:
    def test_full_page_of_open_prs_fails_closed(self, monkeypatch):
        import merge_train
        entries = [(n, f"feat (#{n})", f"feature/x-{n}", "") for n in range(1, merge_train.EPIC_PR_LIST_LIMIT + 1)]
        fake_cmd, calls = _fake_run_cmd([
            ("scripts/taiga-story.sh", (0, _epic_json(EPIC_DIAGRAM_DESCRIPTION), "")),
            ("gh pr list", (0, _open_prs_payload(entries), "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        with pytest.raises(RuntimeError, match="limit"):
            discover_epic_prs(2752)
        assert f"--limit {merge_train.EPIC_PR_LIST_LIMIT}" in calls[1]


class TestStatePathIsUnique:
    def test_two_trains_in_the_same_second_get_distinct_state_files(self, monkeypatch):
        import merge_train
        paths = []
        view = json.dumps({"number": 41, "title": "t #41", "headRefName": "feature/a-41",
                           "baseRefName": "main", "state": "OPEN", "url": "u", "body": ""})
        fake_cmd, _ = _fake_run_cmd([("pr view 41", (0, view, ""))])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        monkeypatch.setattr("merge_train.time.time", lambda: 1700000000.0)
        monkeypatch.setattr("merge_train.process_pr", lambda sm, i, t, sp: (paths.append(sp), sm.advance(), EXIT_OK)[2])
        for _ in range(2):
            monkeypatch.setattr(sys, "argv", ["merge_train.py", "41"])
            with pytest.raises(SystemExit):
                merge_train.main()
        try:
            assert len(set(paths)) == 2
        finally:
            for p in paths:
                if os.path.exists(p):
                    os.remove(p)


class TestLivePrLookupFailure:
    """A failed or incomplete live `gh pr view` must stop before anything mutates the PR."""

    def _run(self, monkeypatch, tmp_path, view_result):
        state_path = str(tmp_path / "state.json")
        sm = _make_sm_for_process_pr(state_path)
        fake_cmd, calls = _fake_run_cmd([
            ("git fetch upstream", (0, "", "")),
            ("gh pr view 42 --json state,baseRefName", view_result),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        result = process_pr(sm, 0, "main", state_path)
        return sm, result, calls

    def _assert_untouched(self, sm, result, calls):
        assert result == EXIT_OK
        assert sm.state["prs"][0]["status"] == "failed"
        assert "live PR state unknown" in sm.state["prs"][0]["error"]
        assert sm.state["currentIndex"] == 1
        joined = "\n".join(calls)
        for mutating in ("git rebase", "git push", "gh pr merge", "monitor-pr-stability.sh"):
            assert mutating not in joined

    def test_nonzero_exit_fails_the_pr(self, monkeypatch, tmp_path):
        self._assert_untouched(*self._run(monkeypatch, tmp_path, (1, "", "HTTP 502")))

    def test_malformed_json_fails_the_pr(self, monkeypatch, tmp_path):
        self._assert_untouched(*self._run(monkeypatch, tmp_path, (0, "<html>", "")))

    def test_missing_state_fails_the_pr(self, monkeypatch, tmp_path):
        self._assert_untouched(*self._run(monkeypatch, tmp_path, (0, "{}", "")))

    def test_missing_base_fails_the_pr(self, monkeypatch, tmp_path):
        self._assert_untouched(*self._run(monkeypatch, tmp_path, (0, '{"state": "OPEN"}', "")))


class TestStoryPrLandedTruncation:
    def test_full_page_listing_raises_instead_of_reporting_unlanded(self, monkeypatch):
        import merge_train
        entries = [(n, "OPEN", f"feat (#{n})", f"feature/x-{n}") for n in range(1, merge_train.PARENT_PR_LIST_LIMIT + 1)]
        fake_cmd, _ = _fake_run_cmd([("pr list --state all", (0, _all_prs_payload(entries), ""))])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        with pytest.raises(RuntimeError, match="limit"):
            merge_train.story_pr_landed(7)

    def test_unlanded_parents_is_none_when_the_listing_is_truncated(self, monkeypatch):
        """None is the caller's 'could not verify' answer, not a misleading 'parent not merged'."""
        import merge_train
        entries = [(n, "OPEN", f"feat (#{n})", f"feature/x-{n}") for n in range(1, merge_train.PARENT_PR_LIST_LIMIT + 1)]
        fake_cmd, _ = _fake_run_cmd([
            ("--epic", (0, _epic_json(INTEGRATION_EPIC_DESCRIPTION), "")),
            ("pr list --state all", (0, _all_prs_payload(entries), "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        assert merge_train.unlanded_integration_parents("epic/2004-integration-2748") is None


class TestActionPathIsPerTrain:
    def test_action_path_derives_from_the_unique_state_file(self):
        from merge_train import action_path_for
        assert action_path_for("/tmp/merge-train-1700000000-ab12cd.json") == "/tmp/merge-train-1700000000-ab12cd-action.json"
        assert action_path_for("/tmp/merge-train-1700000000-ab12cd.json") != action_path_for("/tmp/merge-train-1700000000-zz99yy.json")

    def test_process_pr_writes_to_the_derived_path(self, monkeypatch, tmp_path):
        """Two trains in the same second no longer share /tmp/merge-train-<ts>-action.json."""
        from merge_train import action_path_for
        monkeypatch.setattr("merge_train.time.time", lambda: 1700000000)
        written = []
        monkeypatch.setattr("merge_train.write_action_request", lambda path, **kw: written.append(path))
        state_path = str(tmp_path / "train-a.json")
        sm = _make_sm_for_process_pr(state_path)
        fake_cmd, _ = _fake_run_cmd([
            ("git fetch upstream", (0, "", "")),
            _pr_view_response("OPEN"),
            _taiga_done_response(),
            ("monitor-pr-stability.sh", (0, "CHECKS_FAILING:build", "")),
        ])
        monkeypatch.setattr("merge_train.run_cmd", fake_cmd)
        process_pr(sm, 0, "main", state_path)
        assert written == [action_path_for(state_path)]


class TestStateFilesStayPrivate:
    def test_state_manager_save_keeps_0600_under_a_permissive_umask(self, tmp_path):
        state_path = str(tmp_path / "state.json")
        old = os.umask(0o000)
        try:
            sm = StateManager(state_path)
            sm.init([{"number": 1, "title": "t", "branch": "feature/a-1", "body": ""}], target="main")
            sm.update_pr(0, status="in_progress")
        finally:
            os.umask(old)
        assert os.stat(state_path).st_mode & 0o777 == 0o600

    def test_action_request_is_0600_under_a_permissive_umask(self, tmp_path):
        path = str(tmp_path / "action.json")
        old = os.umask(0o000)
        try:
            write_action_request(path, "resolve_comments",
                                 {"number": 1, "branch": "b", "title": "t"}, "/tmp/s.json", {})
        finally:
            os.umask(old)
        assert os.stat(path).st_mode & 0o777 == 0o600
        assert json.load(open(path))["action"] == "resolve_comments"
        assert [f for f in os.listdir(tmp_path) if ".tmp." in f] == []

