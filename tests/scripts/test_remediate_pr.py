# ABOUTME: Pytest tests for the remediate_pr.py script.
# ABOUTME: Verifies argument parsing, state management, escalation logic, action-request writing, and exit codes.
import json
import os
import sys
import tempfile

import pytest

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts"))

import remediate_pr
from remediate_pr import (
    parse_args,
    StateManager,
    write_action_request,
    main,
    EXIT_STABLE,
    EXIT_ERROR,
    EXIT_BUILD_FIX,
    EXIT_COMMENTS,
    EXIT_ESCALATED,
    EXIT_NEEDS_REBASE,
    EXIT_PENDING,
)


class TestParseArgsBasic:
    """Parses PR number and default options."""

    def test_pr_number_only(self):
        args = parse_args(["42"])
        assert args.pr_number == 42
        assert args.max_failures == 5
        assert args.taiga_id is None
        assert args.taiga_type is None
        assert args.resume_path is None


class TestParseArgsAllFlags:
    """Parses --max-failures, --taiga-id, --taiga-type."""

    def test_all_flags(self):
        args = parse_args(["99", "--max-failures", "3", "--taiga-id", "1234", "--taiga-type", "story"])
        assert args.pr_number == 99
        assert args.max_failures == 3
        assert args.taiga_id == "1234"
        assert args.taiga_type == "story"

    def test_flags_before_pr(self):
        args = parse_args(["--max-failures", "7", "--taiga-id", "99", "--taiga-type", "issue", "55"])
        assert args.pr_number == 55
        assert args.max_failures == 7
        assert args.taiga_type == "issue"


class TestParseArgsResume:
    """Parses --resume with state file path."""

    def test_resume_flag(self):
        args = parse_args(["--resume", "/tmp/remediate-pr-42.json"])
        assert args.resume_path == "/tmp/remediate-pr-42.json"
        assert args.pr_number is None  # Not required with --resume

    def test_resume_with_pr_is_ok(self):
        """PR number is accepted alongside --resume but resume_path takes precedence in main()."""
        args = parse_args(["--resume", "/tmp/state.json", "42"])
        assert args.resume_path == "/tmp/state.json"
        assert args.pr_number == 42

    def test_missing_pr_and_resume_exits(self):
        with pytest.raises(SystemExit):
            parse_args([])


class TestStateInit:
    """Initializes state file with correct structure."""

    def test_init_creates_valid_state(self, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = StateManager(state_path)
        sm.init(pr_number=42, max_failures=5, taiga_id="100", taiga_type="story")

        state = sm.state
        assert state["prNumber"] == 42
        assert state["maxFailures"] == 5
        assert state["buildFailureCount"] == 0
        assert state["phase"] == "monitoring"
        assert state["taigaId"] == "100"
        assert state["taigaType"] == "story"
        assert state["lastResult"] is None

        # Verify persisted to disk
        with open(state_path) as f:
            persisted = json.load(f)
        assert persisted["prNumber"] == 42

    def test_init_defaults(self, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = StateManager(state_path)
        sm.init(pr_number=10, max_failures=3)

        assert sm.state["taigaId"] is None
        assert sm.state["taigaType"] is None


class TestStateLoadAndResume:
    """Loads existing state and resumes."""

    def test_load_existing_state(self, tmp_path):
        state_path = str(tmp_path / "state.json")
        existing = {
            "prNumber": 42,
            "maxFailures": 5,
            "buildFailureCount": 2,
            "phase": "awaiting_fix",
            "taigaId": "100",
            "taigaType": "story",
            "lastResult": {"status": "checks_failing", "failing_checks": ["build"]},
        }
        with open(state_path, "w") as f:
            json.dump(existing, f)

        sm = StateManager(state_path)
        state = sm.load()

        assert state["prNumber"] == 42
        assert state["buildFailureCount"] == 2
        assert state["phase"] == "awaiting_fix"
        assert sm.state is state


class TestStateIncrementFailure:
    """Increments buildFailureCount."""

    def test_increment_failure(self, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = StateManager(state_path)
        sm.init(pr_number=42, max_failures=5)

        assert sm.state["buildFailureCount"] == 0
        sm.update(buildFailureCount=sm.state["buildFailureCount"] + 1)
        assert sm.state["buildFailureCount"] == 1
        sm.update(buildFailureCount=sm.state["buildFailureCount"] + 1)
        assert sm.state["buildFailureCount"] == 2

        # Verify persisted
        with open(state_path) as f:
            persisted = json.load(f)
        assert persisted["buildFailureCount"] == 2


class TestEscalationAtThreshold:
    """Exits with code 12 when failures >= max."""

    def test_escalation_on_checks_failing(self, monkeypatch, tmp_path):
        """When parsed result is checks_failing and count reaches threshold, escalate."""
        state_path = str(tmp_path / "state.json")
        existing = {
            "prNumber": 42,
            "maxFailures": 1,
            "buildFailureCount": 0,
            "phase": "monitoring",
            "taigaId": None,
            "taigaType": None,
            "lastResult": None,
        }
        with open(state_path, "w") as f:
            json.dump(existing, f)

        monkeypatch.setattr(
            "remediate_pr.run_cmd",
            lambda cmd, check=True, capture=True: (0, "CHECKS_FAILING:build", ""),
        )
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        result = main()
        assert result == EXIT_ESCALATED

        with open(state_path) as f:
            state = json.load(f)
        assert state["buildFailureCount"] == 1
        assert state["phase"] == "escalated"


class TestWriteActionRequestBuild:
    """Writes correct JSON for build fix action."""

    def test_build_fix_action(self, tmp_path):
        action_path = str(tmp_path / "action.json")
        write_action_request(
            action_path,
            action="fix_build",
            pr_number=42,
            state_file="/tmp/remediate-pr-42.json",
            details={"failing_checks": ["build", "test"], "stderr": "error output"},
        )

        with open(action_path) as f:
            data = json.load(f)

        assert data["action"] == "fix_build"
        assert data["pr"]["number"] == 42
        assert data["state_file"] == "/tmp/remediate-pr-42.json"
        assert data["details"]["failing_checks"] == ["build", "test"]
        assert data["details"]["stderr"] == "error output"


class TestWriteActionRequestComments:
    """Writes correct JSON for comment resolution action."""

    def test_comment_resolution_action(self, tmp_path):
        action_path = str(tmp_path / "action.json")
        write_action_request(
            action_path,
            action="resolve_comments",
            pr_number=99,
            state_file="/tmp/remediate-pr-99.json",
            details={"comment_count": 3},
        )

        with open(action_path) as f:
            data = json.load(f)

        assert data["action"] == "resolve_comments"
        assert data["pr"]["number"] == 99
        assert data["details"]["comment_count"] == 3


class TestStableAfterMonitoring:
    """Returns exit 0 when monitor reports STABLE."""

    def test_stable_returns_zero(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")

        monkeypatch.setattr(
            "remediate_pr.run_cmd",
            lambda cmd, check=True, capture=True: (0, "STABLE", ""),
        )
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "42"])

        # Override state file path so it uses tmp_path
        original_main = main.__wrapped__ if hasattr(main, "__wrapped__") else None

        # We need to patch the state file path. The script hardcodes /tmp/remediate-pr-{n}.json
        # so we monkeypatch StateManager to capture it.
        inits = []
        original_sm_init = StateManager.__init__

        def patched_sm_init(self, state_path_arg):
            # Redirect to tmp_path
            redirected = str(tmp_path / os.path.basename(state_path_arg))
            original_sm_init(self, redirected)
            inits.append(redirected)

        monkeypatch.setattr(StateManager, "__init__", patched_sm_init)

        result = main()
        assert result == EXIT_STABLE

    def test_stable_updates_phase(self, monkeypatch, tmp_path):
        """After STABLE result, state phase is set to 'stable'."""
        state_path = str(tmp_path / "state.json")
        existing = {
            "prNumber": 42,
            "maxFailures": 5,
            "buildFailureCount": 0,
            "phase": "monitoring",
            "taigaId": None,
            "taigaType": None,
            "lastResult": None,
        }
        with open(state_path, "w") as f:
            json.dump(existing, f)

        monkeypatch.setattr(
            "remediate_pr.run_cmd",
            lambda cmd, check=True, capture=True: (0, "STABLE", ""),
        )
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        result = main()
        assert result == EXIT_STABLE

        with open(state_path) as f:
            state = json.load(f)
        assert state["phase"] == "stable"
        assert state["lastResult"]["status"] == "stable"


class TestNoCiConfigured:
    """With no CI configured the monitor says STABLE plus a NOTE on stderr: a pass with a note."""

    def test_no_ci_note_is_stable_and_recorded(self, monkeypatch, tmp_path, capsys):
        state_path = str(tmp_path / "state.json")
        existing = {
            "prNumber": 42,
            "maxFailures": 5,
            "buildFailureCount": 0,
            "phase": "monitoring",
            "taigaId": None,
            "taigaType": None,
            "lastResult": None,
        }
        with open(state_path, "w") as f:
            json.dump(existing, f)

        note = "NOTE: no CI checks configured for MrsBohemian/Cholobots (0 workflows, no required contexts); treating checks as passed"
        monkeypatch.setattr(
            "remediate_pr.run_cmd",
            lambda cmd, check=True, capture=True: (0, "STABLE", note),
        )
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        assert main() == EXIT_STABLE

        with open(state_path) as f:
            state = json.load(f)
        assert state["phase"] == "stable"
        assert state["buildFailureCount"] == 0
        assert state["lastResult"]["note"] == "no CI checks configured"
        assert "NOTE: no CI checks configured" in capsys.readouterr().err

    def test_stable_without_note_has_no_note(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        with open(state_path, "w") as f:
            json.dump({"prNumber": 42, "maxFailures": 5, "buildFailureCount": 0,
                       "phase": "monitoring", "taigaId": None, "taigaType": None,
                       "lastResult": None}, f)
        monkeypatch.setattr(
            "remediate_pr.run_cmd",
            lambda cmd, check=True, capture=True: (0, "STABLE", ""),
        )
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        assert main() == EXIT_STABLE
        with open(state_path) as f:
            assert "note" not in json.load(f)["lastResult"]


class TestBuildFixFlow:
    """When checks are failing (below threshold), returns EXIT_BUILD_FIX and writes action."""

    def test_build_fix_exit_code(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        existing = {
            "prNumber": 42,
            "maxFailures": 5,
            "buildFailureCount": 0,
            "phase": "monitoring",
            "taigaId": None,
            "taigaType": None,
            "lastResult": None,
        }
        with open(state_path, "w") as f:
            json.dump(existing, f)

        monkeypatch.setattr(
            "remediate_pr.run_cmd",
            lambda cmd, check=True, capture=True: (0, "CHECKS_FAILING:build,test-unit", ""),
        )
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        result = main()
        assert result == EXIT_BUILD_FIX

        with open(state_path) as f:
            state = json.load(f)
        assert state["buildFailureCount"] == 1
        assert state["phase"] == "awaiting_fix"

        # Verify action request was written
        action_path = str(tmp_path / "remediate-pr-42-action.json")
        # The action path is hardcoded to /tmp, so we check state instead
        assert state["lastResult"]["failing_checks"] == ["build", "test-unit"]


class TestCommentsFlow:
    """When comments are found, returns EXIT_COMMENTS and writes action."""

    def test_comments_exit_code(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        existing = {
            "prNumber": 42,
            "maxFailures": 5,
            "buildFailureCount": 0,
            "phase": "monitoring",
            "taigaId": None,
            "taigaType": None,
            "lastResult": None,
        }
        with open(state_path, "w") as f:
            json.dump(existing, f)

        monkeypatch.setattr(
            "remediate_pr.run_cmd",
            lambda cmd, check=True, capture=True: (0, "COMMENTS_FOUND:5", ""),
        )
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        result = main()
        assert result == EXIT_COMMENTS

        with open(state_path) as f:
            state = json.load(f)
        assert state["phase"] == "awaiting_comment_resolution"
        assert state["lastResult"]["comment_count"] == 5


class TestMonitorScriptFailure:
    """A non-zero monitor exit is an API/network error, not a failing check.

    monitor-pr-stability.sh reports CHECKS_FAILING with exit 0, so a non-zero
    exit must neither count toward escalation nor dispatch a build-fix agent.
    """

    def _seed(self, tmp_path, failures=0, max_failures=5):
        state_path = str(tmp_path / "state.json")
        with open(state_path, "w") as f:
            json.dump({
                "prNumber": 42,
                "maxFailures": max_failures,
                "buildFailureCount": failures,
                "phase": "monitoring",
                "taigaId": None,
                "taigaType": None,
                "lastResult": None,
            }, f)
        return state_path

    def test_monitor_failure_returns_error_without_counting(self, monkeypatch, tmp_path):
        state_path = self._seed(tmp_path)
        monkeypatch.setattr(
            "remediate_pr.run_cmd",
            lambda cmd, check=True, capture=True: (1, "", "Something went wrong"),
        )
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        result = main()
        assert result == EXIT_ERROR

        with open(state_path) as f:
            state = json.load(f)
        assert state["buildFailureCount"] == 0
        assert state["phase"] == "error"

    def test_monitor_failure_at_threshold_does_not_escalate(self, monkeypatch, tmp_path):
        state_path = self._seed(tmp_path, failures=1, max_failures=2)
        monkeypatch.setattr("remediate_pr.run_cmd", lambda cmd, check=True, capture=True: (1, "", "HTTP 502"))
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        assert main() == EXIT_ERROR

        with open(state_path) as f:
            assert json.load(f)["buildFailureCount"] == 1

    def test_monitor_failure_writes_no_action_request(self, monkeypatch, tmp_path):
        state_path = self._seed(tmp_path)
        written = []
        monkeypatch.setattr("remediate_pr.run_cmd", lambda cmd, check=True, capture=True: (1, "", "HTTP 502"))
        monkeypatch.setattr("remediate_pr.write_action_request", lambda *a, **k: written.append((a, k)))
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        main()

        assert written == []


class TestParseArgsMissingFlagValues:
    """Exits with error when a flag is missing its required value."""

    def test_max_failures_missing_value(self):
        with pytest.raises(SystemExit):
            parse_args(["42", "--max-failures"])

    def test_taiga_id_missing_value(self):
        with pytest.raises(SystemExit):
            parse_args(["42", "--taiga-id"])

    def test_taiga_type_missing_value(self):
        with pytest.raises(SystemExit):
            parse_args(["42", "--taiga-type"])

    def test_resume_missing_value(self):
        with pytest.raises(SystemExit):
            parse_args(["--resume"])

    def test_max_failures_non_integer(self):
        with pytest.raises(SystemExit):
            parse_args(["42", "--max-failures", "abc"])

    def test_max_failures_zero(self):
        with pytest.raises(SystemExit):
            parse_args(["42", "--max-failures", "0"])

    def test_max_failures_negative(self):
        with pytest.raises(SystemExit):
            parse_args(["42", "--max-failures", "-3"])


class TestParseArgsTaigaPairing:
    """Validates --taiga-id and --taiga-type must be provided together."""

    def test_taiga_id_without_type_exits(self):
        with pytest.raises(SystemExit):
            parse_args(["42", "--taiga-id", "100"])

    def test_taiga_type_without_id_exits(self):
        with pytest.raises(SystemExit):
            parse_args(["42", "--taiga-type", "story"])

    def test_taiga_type_invalid_value_exits(self):
        with pytest.raises(SystemExit):
            parse_args(["42", "--taiga-id", "100", "--taiga-type", "epic"])

    def test_taiga_type_story_accepted(self):
        args = parse_args(["42", "--taiga-id", "100", "--taiga-type", "story"])
        assert args.taiga_id == "100"
        assert args.taiga_type == "story"

    def test_taiga_type_issue_accepted(self):
        args = parse_args(["42", "--taiga-id", "200", "--taiga-type", "issue"])
        assert args.taiga_id == "200"
        assert args.taiga_type == "issue"


class TestStateLoadValidation:
    """Validates prNumber when loading state from file."""

    def test_load_rejects_non_int_pr_number(self, tmp_path):
        state_path = str(tmp_path / "state.json")
        with open(state_path, "w") as f:
            json.dump({"prNumber": "42; rm -rf /", "maxFailures": 5}, f)

        sm = StateManager(state_path)
        with pytest.raises(ValueError, match="Invalid prNumber"):
            sm.load()

    def test_load_rejects_missing_pr_number(self, tmp_path):
        state_path = str(tmp_path / "state.json")
        with open(state_path, "w") as f:
            json.dump({"maxFailures": 5}, f)

        sm = StateManager(state_path)
        with pytest.raises(ValueError, match="Invalid prNumber"):
            sm.load()

    def test_load_accepts_int_pr_number(self, tmp_path):
        state_path = str(tmp_path / "state.json")
        existing = {
            "prNumber": 42,
            "maxFailures": 5,
            "buildFailureCount": 0,
            "phase": "monitoring",
            "taigaId": None,
            "taigaType": None,
            "lastResult": None,
        }
        with open(state_path, "w") as f:
            json.dump(existing, f)

        sm = StateManager(state_path)
        state = sm.load()
        assert state["prNumber"] == 42


class TestMonitorOutputCapture:
    """When monitor script fails, both stdout and stderr are captured."""

    def test_monitor_failure_captures_stdout_and_stderr(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        existing = {
            "prNumber": 42,
            "maxFailures": 5,
            "buildFailureCount": 0,
            "phase": "monitoring",
            "taigaId": None,
            "taigaType": None,
            "lastResult": None,
        }
        with open(state_path, "w") as f:
            json.dump(existing, f)

        monkeypatch.setattr(
            "remediate_pr.run_cmd",
            lambda cmd, check=True, capture=True: (1, "stdout error info", "stderr error info"),
        )
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        result = main()
        assert result == EXIT_ERROR

        with open(state_path) as f:
            state = json.load(f)
        assert state["lastResult"]["stdout"] == "stdout error info"
        assert state["lastResult"]["stderr"] == "stderr error info"


class TestUnknownResultUpdatesState:
    """On unknown stability result, state phase is set to 'error' and result is persisted."""

    def test_unknown_result_sets_error_phase(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        existing = {
            "prNumber": 42,
            "maxFailures": 5,
            "buildFailureCount": 0,
            "phase": "monitoring",
            "taigaId": None,
            "taigaType": None,
            "lastResult": None,
        }
        with open(state_path, "w") as f:
            json.dump(existing, f)

        # Return something the parser will produce with an unknown status
        monkeypatch.setattr(
            "remediate_pr.parse_stability_result",
            lambda stdout: {"status": "something_unexpected", "raw": "weird output"},
        )
        monkeypatch.setattr(
            "remediate_pr.run_cmd",
            lambda cmd, check=True, capture=True: (0, "weird output", ""),
        )
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        result = main()
        assert result == EXIT_ERROR

        with open(state_path) as f:
            state = json.load(f)
        assert state["phase"] == "error"
        assert state["lastResult"]["status"] == "something_unexpected"


class TestNeedsRebaseFlow:
    """When monitor reports NEEDS_REBASE, returns EXIT_NEEDS_REBASE and writes action."""

    def test_needs_rebase_exit_code(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        existing = {
            "prNumber": 42,
            "maxFailures": 5,
            "buildFailureCount": 0,
            "phase": "monitoring",
            "taigaId": None,
            "taigaType": None,
            "lastResult": None,
        }
        with open(state_path, "w") as f:
            json.dump(existing, f)

        monkeypatch.setattr(
            "remediate_pr.run_cmd",
            lambda cmd, check=True, capture=True: (0, "NEEDS_REBASE", ""),
        )
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        result = main()
        assert result == EXIT_NEEDS_REBASE

    def test_needs_rebase_updates_phase(self, monkeypatch, tmp_path):
        state_path = str(tmp_path / "state.json")
        existing = {
            "prNumber": 42,
            "maxFailures": 5,
            "buildFailureCount": 0,
            "phase": "monitoring",
            "taigaId": None,
            "taigaType": None,
            "lastResult": None,
        }
        with open(state_path, "w") as f:
            json.dump(existing, f)

        monkeypatch.setattr(
            "remediate_pr.run_cmd",
            lambda cmd, check=True, capture=True: (0, "NEEDS_REBASE", ""),
        )
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        result = main()
        assert result == EXIT_NEEDS_REBASE

        with open(state_path) as f:
            state = json.load(f)
        assert state["phase"] == "needs_rebase"
        assert state["lastResult"]["status"] == "needs_rebase"


class TestPendingFlow:
    """When monitor reports PENDING (bounded wait expired), returns EXIT_PENDING without side effects."""

    @staticmethod
    def _seed_state(tmp_path, build_failure_count=2):
        state_path = str(tmp_path / "state.json")
        existing = {
            "prNumber": 42,
            "maxFailures": 5,
            "buildFailureCount": build_failure_count,
            "phase": "monitoring",
            "taigaId": None,
            "taigaType": None,
            "lastResult": None,
        }
        with open(state_path, "w") as f:
            json.dump(existing, f)
        return state_path

    def test_pending_returns_exit_14(self, monkeypatch, tmp_path):
        state_path = self._seed_state(tmp_path)
        monkeypatch.setattr(
            "remediate_pr.run_cmd",
            lambda cmd, check=True, capture=True: (0, "PENDING", ""),
        )
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        result = main()
        assert result == EXIT_PENDING

    def test_pending_does_not_increment_failure_count(self, monkeypatch, tmp_path):
        state_path = self._seed_state(tmp_path, build_failure_count=2)
        monkeypatch.setattr(
            "remediate_pr.run_cmd",
            lambda cmd, check=True, capture=True: (0, "PENDING", ""),
        )
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        main()

        with open(state_path) as f:
            state = json.load(f)
        assert state["buildFailureCount"] == 2

    def test_pending_sets_phase_monitoring(self, monkeypatch, tmp_path):
        state_path = self._seed_state(tmp_path)
        monkeypatch.setattr(
            "remediate_pr.run_cmd",
            lambda cmd, check=True, capture=True: (0, "PENDING", ""),
        )
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        main()

        with open(state_path) as f:
            state = json.load(f)
        assert state["phase"] == "monitoring"
        assert state["lastResult"]["status"] == "pending"

    def test_monitor_invocation_includes_max_seconds(self, monkeypatch, tmp_path):
        state_path = self._seed_state(tmp_path)
        commands = []

        def fake_run_cmd(cmd, check=True, capture=True):
            commands.append(cmd)
            return (0, "PENDING", "")

        monkeypatch.setattr("remediate_pr.run_cmd", fake_run_cmd)
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        main()

        assert any("--max-seconds" in str(cmd) for cmd in commands)

    def test_monitor_invocation_passes_zero_quiet_minutes(self, monkeypatch, tmp_path):
        state_path = self._seed_state(tmp_path)
        commands = []

        def fake_run_cmd(cmd, check=True, capture=True):
            commands.append(cmd)
            return (0, "PENDING", "")

        monkeypatch.setattr("remediate_pr.run_cmd", fake_run_cmd)
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        main()

        monitor_cmds = [str(c) for c in commands if "monitor-pr-stability.sh" in str(c)]
        assert len(monitor_cmds) == 1
        assert " 0 --poll-interval 30 --max-seconds 480" in monitor_cmds[0]
        assert " 3 --poll-interval" not in monitor_cmds[0]


class TestExitCodes:
    """Exit code constants are correct."""

    def test_exit_codes(self):
        assert EXIT_STABLE == 0
        assert EXIT_ERROR == 1
        assert EXIT_BUILD_FIX == 10
        assert EXIT_COMMENTS == 11
        assert EXIT_ESCALATED == 12
        assert EXIT_NEEDS_REBASE == 13
        assert EXIT_PENDING == 14


class TestCodexReviewHook:
    """On --resume from a review-trigger phase, requests a Codex review pass before the stability check."""

    @staticmethod
    def _seed_state(tmp_path, phase):
        state_path = str(tmp_path / "state.json")
        with open(state_path, "w") as f:
            json.dump(
                {
                    "prNumber": 42,
                    "maxFailures": 5,
                    "buildFailureCount": 0,
                    "phase": phase,
                    "taigaId": None,
                    "taigaType": None,
                    "lastResult": None,
                },
                f,
            )
        return state_path

    def test_resume_from_awaiting_fix_invokes_review_before_monitor(self, monkeypatch, tmp_path):
        state_path = self._seed_state(tmp_path, "awaiting_fix")
        commands = []

        def fake_run_cmd(cmd, check=True, capture=True):
            commands.append(cmd)
            return (0, "STABLE", "")

        monkeypatch.setattr("remediate_pr.run_cmd", fake_run_cmd)
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        main()

        assert "codex-review-pr.sh" in str(commands[0])
        assert "42" in str(commands[0])
        assert "monitor-pr-stability.sh" in str(commands[1])

    def test_resume_from_awaiting_comment_resolution_invokes_review(self, monkeypatch, tmp_path):
        state_path = self._seed_state(tmp_path, "awaiting_comment_resolution")
        commands = []

        def fake_run_cmd(cmd, check=True, capture=True):
            commands.append(cmd)
            return (0, "STABLE", "")

        monkeypatch.setattr("remediate_pr.run_cmd", fake_run_cmd)
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        main()

        assert "codex-review-pr.sh" in str(commands[0])
        assert "42" in str(commands[0])
        assert "monitor-pr-stability.sh" in str(commands[1])

    def test_resume_from_monitoring_does_not_invoke_review(self, monkeypatch, tmp_path):
        state_path = self._seed_state(tmp_path, "monitoring")
        commands = []

        def fake_run_cmd(cmd, check=True, capture=True):
            commands.append(cmd)
            return (0, "STABLE", "")

        monkeypatch.setattr("remediate_pr.run_cmd", fake_run_cmd)
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        main()

        assert not any("codex-review-pr.sh" in str(c) for c in commands)

    def test_fresh_start_does_not_invoke_review(self, monkeypatch, tmp_path):
        commands = []

        def fake_run_cmd(cmd, check=True, capture=True):
            commands.append(cmd)
            return (0, "STABLE", "")

        monkeypatch.setattr("remediate_pr.run_cmd", fake_run_cmd)
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "42"])

        original_sm_init = StateManager.__init__

        def patched_sm_init(self, state_path_arg):
            redirected = str(tmp_path / os.path.basename(state_path_arg))
            original_sm_init(self, redirected)

        monkeypatch.setattr(StateManager, "__init__", patched_sm_init)

        main()

        assert not any("codex-review-pr.sh" in str(c) for c in commands)

    def test_review_script_failure_leaves_exit_code_unchanged(self, monkeypatch, tmp_path):
        state_path = self._seed_state(tmp_path, "awaiting_fix")

        def fake_run_cmd(cmd, check=True, capture=True):
            if "codex-review-pr.sh" in str(cmd):
                return (1, "", "codex failed")
            return (0, "STABLE", "")

        monkeypatch.setattr("remediate_pr.run_cmd", fake_run_cmd)
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        result = main()

        assert result == EXIT_STABLE

    def test_missing_review_script_leaves_exit_code_unchanged(self, monkeypatch, tmp_path):
        """subprocess raises FileNotFoundError before any exit status exists; the hook must absorb it."""
        state_path = self._seed_state(tmp_path, "awaiting_fix")
        commands = []

        def fake_run_cmd(cmd, check=True, capture=True):
            commands.append(cmd)
            if "codex-review-pr.sh" in str(cmd):
                raise FileNotFoundError(
                    2, "No such file or directory", "scripts/codex-review-pr.sh"
                )
            return (0, "STABLE", "")

        monkeypatch.setattr("remediate_pr.run_cmd", fake_run_cmd)
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        result = main()

        assert result == EXIT_STABLE
        assert any("monitor-pr-stability.sh" in str(c) for c in commands)

    def test_non_executable_review_script_leaves_exit_code_unchanged(self, monkeypatch, tmp_path):
        """A lost exec bit surfaces as PermissionError; the hook must absorb it too."""
        state_path = self._seed_state(tmp_path, "awaiting_fix")
        commands = []

        def fake_run_cmd(cmd, check=True, capture=True):
            commands.append(cmd)
            if "codex-review-pr.sh" in str(cmd):
                raise PermissionError(
                    13, "Permission denied", "scripts/codex-review-pr.sh"
                )
            return (0, "STABLE", "")

        monkeypatch.setattr("remediate_pr.run_cmd", fake_run_cmd)
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        result = main()

        assert result == EXIT_STABLE
        assert any("monitor-pr-stability.sh" in str(c) for c in commands)

    def test_review_pending_returns_exit_pending_without_monitor(self, monkeypatch, tmp_path):
        state_path = self._seed_state(tmp_path, "awaiting_fix")
        commands = []

        def fake_run_cmd(cmd, check=True, capture=True):
            commands.append(cmd)
            if "codex-review-pr.sh" in str(cmd):
                return (0, "REVIEW_PENDING:1/3", "")
            return (0, "STABLE", "")

        monkeypatch.setattr("remediate_pr.run_cmd", fake_run_cmd)
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        result = main()

        assert result == EXIT_PENDING
        assert not any("monitor-pr-stability.sh" in str(c) for c in commands)

        with open(state_path) as f:
            state = json.load(f)
        assert state["phase"] == "awaiting_fix"

    def test_review_posted_continues_to_monitor(self, monkeypatch, tmp_path):
        state_path = self._seed_state(tmp_path, "awaiting_fix")
        commands = []

        def fake_run_cmd(cmd, check=True, capture=True):
            commands.append(cmd)
            if "codex-review-pr.sh" in str(cmd):
                return (0, "REVIEW_POSTED:1/3:2:0", "")
            return (0, "STABLE", "")

        monkeypatch.setattr("remediate_pr.run_cmd", fake_run_cmd)
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        result = main()

        assert result == EXIT_STABLE
        assert any("monitor-pr-stability.sh" in str(c) for c in commands)

    def test_absent_review_script_on_disk_leaves_exit_code_unchanged(self, monkeypatch, tmp_path):
        """End-to-end through the real run_cmd: an absent review script must not abort the run."""
        state_path = self._seed_state(tmp_path, "awaiting_fix")
        real_run_cmd = remediate_pr.run_cmd
        commands = []

        def fake_run_cmd(cmd, check=True, capture=True):
            commands.append(cmd)
            if "codex-review-pr.sh" in str(cmd):
                return real_run_cmd(
                    str(cmd).replace(
                        "scripts/codex-review-pr.sh",
                        str(tmp_path / "definitely-absent-codex-review-pr.sh"),
                    ),
                    check=check,
                    capture=capture,
                )
            return (0, "STABLE", "")

        monkeypatch.setattr("remediate_pr.run_cmd", fake_run_cmd)
        monkeypatch.setattr("sys.argv", ["remediate_pr.py", "--resume", state_path])

        result = main()

        assert result == EXIT_STABLE
        assert any("monitor-pr-stability.sh" in str(c) for c in commands)


class TestRemediateFilesStayPrivate:
    def test_state_and_action_files_are_0600_under_a_permissive_umask(self, tmp_path):
        import os
        from remediate_pr import write_action_request
        state_path = str(tmp_path / "state.json")
        action_path = str(tmp_path / "action.json")
        old = os.umask(0o000)
        try:
            sm = StateManager(state_path)
            sm.init(pr_number=42, max_failures=5, taiga_id=None, taiga_type=None)
            sm.update(phase="monitoring")
            write_action_request(action_path, "fix_build", 42, state_path, {})
        finally:
            os.umask(old)
        assert os.stat(state_path).st_mode & 0o777 == 0o600
        assert os.stat(action_path).st_mode & 0o777 == 0o600

