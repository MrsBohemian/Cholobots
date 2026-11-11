# ABOUTME: Pytest tests for the orchestrate_bugs.py script.
# ABOUTME: Verifies state management, slot filling, outcome detection, resume, and launch retries.
import json
import os
import sys
import pytest
from pathlib import Path

from conftest import make_host_repo

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts", "devcontainer"))

from orchestrate_bugs import (
    StateManager,
    Orchestrator,
    determine_outcome,
    MAX_LAUNCH_ATTEMPTS,
)


def make_bugs(refs):
    """Create a list of bug dicts for testing."""
    return [
        {"ref": r, "id": r + 1000, "subject": f"Bug {r}"}
        for r in refs
    ]


# ── StateManager tests ──────────────────────────────────────────────


class TestInitCreatesValidState:
    """init_from_bugs creates correct structure."""

    def test_init_creates_valid_state(self, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = StateManager(state_path)
        bugs = make_bugs([101, 102, 103])
        sm.init_from_bugs(concurrency=2, bugs=bugs, base_dir=tmp_path / "bugs")

        state = sm.state
        assert state["concurrency"] == 2
        assert state["phase"] == "running"
        assert len(state["bugs"]) == 3
        assert state["bugs"][0]["ref"] == 101
        assert state["bugs"][0]["status"] == "queued"
        assert state["bugs"][0]["pid"] is None
        assert "bug-101" in state["bugs"][0]["clonePath"]

        # Verify persisted to disk
        with open(state_path) as f:
            persisted = json.load(f)
        assert len(persisted["bugs"]) == 3


class TestUpdateBugStatus:
    """update_bug modifies correct bug and persists."""

    def test_update_bug_status(self, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = StateManager(state_path)
        sm.init_from_bugs(2, make_bugs([101, 102]), tmp_path / "bugs")

        sm.update_bug(102, status="active", pid=12345)

        bug = sm.get_bug(102)
        assert bug["status"] == "active"
        assert bug["pid"] == 12345

        # Bug 101 unchanged
        assert sm.get_bug(101)["status"] == "queued"

        # Verify persistence
        with open(state_path) as f:
            persisted = json.load(f)
        bug_102 = [b for b in persisted["bugs"] if b["ref"] == 102][0]
        assert bug_102["status"] == "active"


class TestNextQueuedRefFollowsInputOrder:
    """next_queued_ref returns queued bugs in input order, not by ref."""

    def test_returns_first_in_input_order(self, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = StateManager(state_path)
        sm.init_from_bugs(2, make_bugs([103, 101, 102]), tmp_path / "bugs")

        # All queued — input position wins, not lowest ref
        assert sm.next_queued_ref() == 103

        sm.update_bug(103, status="active")
        assert sm.next_queued_ref() == 101


class TestNextQueuedRefPrefersFewerAttempts:
    """next_queued_ref prefers bugs with fewer launch attempts."""

    def test_deprioritises_high_attempt_bug(self, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = StateManager(state_path)
        sm.init_from_bugs(2, make_bugs([101, 102, 103]), tmp_path / "bugs")

        # Simulate bug 101 having already failed one launch attempt
        sm.update_bug(101, launchAttempts=1)

        # 102 and 103 have 0 attempts — 102 should be preferred (earlier input position wins tie)
        assert sm.next_queued_ref() == 102

    def test_tie_breaks_on_input_order_not_ref(self, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = StateManager(state_path)
        sm.init_from_bugs(2, make_bugs([103, 102, 101]), tmp_path / "bugs")

        sm.update_bug(103, launchAttempts=1)

        # 102 is next in input order; 101 is the lowest ref but comes later
        assert sm.next_queued_ref() == 102


class TestNextQueuedRefInputOrderSurvivesResume:
    """Input order still drives next_queued_ref after the state file is reloaded."""

    def test_resumed_state_keeps_input_order(self, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm1 = StateManager(state_path)
        sm1.init_from_bugs(2, make_bugs([103, 101, 102]), tmp_path / "bugs")

        sm2 = StateManager(state_path)
        sm2.load()

        assert sm2.next_queued_ref() == 103


class TestNextQueuedRefNoneWhenEmpty:
    """next_queued_ref returns None when no queued bugs."""

    def test_returns_none(self, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = StateManager(state_path)
        sm.init_from_bugs(2, make_bugs([101]), tmp_path / "bugs")
        sm.update_bug(101, status="active")

        assert sm.next_queued_ref() is None


class TestCountStatus:
    """count_status correctly counts by status."""

    def test_counts(self, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm = StateManager(state_path)
        sm.init_from_bugs(3, make_bugs([101, 102, 103, 104]), tmp_path / "bugs")
        sm.update_bug(101, status="active")
        sm.update_bug(102, status="active")
        sm.update_bug(103, status="complete")

        assert sm.count_status("active") == 2
        assert sm.count_status("complete") == 1
        assert sm.count_status("queued") == 1
        assert sm.count_status("error") == 0


class TestResumeLoadsExistingState:
    """load() restores state from disk."""

    def test_resume(self, tmp_path):
        state_path = str(tmp_path / "state.json")
        sm1 = StateManager(state_path)
        sm1.init_from_bugs(2, make_bugs([101, 102]), tmp_path / "bugs")
        sm1.update_bug(101, status="complete", outcome="complete")

        # Create a new StateManager and load from disk
        sm2 = StateManager(state_path)
        sm2.load()

        assert sm2.state["concurrency"] == 2
        assert sm2.get_bug(101)["status"] == "complete"
        assert sm2.get_bug(102)["status"] == "queued"


# ── determine_outcome tests ─────────────────────────────────────────


class TestCompleteOutcomeFromStateFile:
    """Reads state file with phase: complete."""

    def test_complete(self, tmp_path):
        state_file = tmp_path / "fix-and-remediate-101.json"
        state_file.write_text(json.dumps({"phase": "complete", "prNumber": 42}))
        outcome, pr, reason = determine_outcome(101, None, container_id=None, state_dir=str(tmp_path))
        assert outcome == "complete"
        assert pr == "42"
        assert reason is None


class TestUnreviewablePrNumberIsNone:
    """Only a positive integer is a reviewable PR; anything else reads as no PR."""

    @pytest.mark.parametrize("pr", [0, "0", "", " ", "abc", -3, "-3", 1.5, True])
    def test_complete_with_invalid_pr_reports_none(self, tmp_path, pr):
        (tmp_path / "fix-and-remediate-102.json").write_text(
            json.dumps({"phase": "complete", "prNumber": pr}))
        outcome, got, _reason = determine_outcome(
            102, None, container_id=None, state_dir=str(tmp_path))
        assert outcome == "complete"
        assert got == "none"

    def test_numeric_string_pr_is_kept(self, tmp_path):
        (tmp_path / "fix-and-remediate-102.json").write_text(
            json.dumps({"phase": "complete", "prNumber": "42"}))
        _outcome, got, _reason = determine_outcome(
            102, None, container_id=None, state_dir=str(tmp_path))
        assert got == "42"

    def test_zero_pr_complete_is_an_error_not_a_success(self, tmp_path, monkeypatch):
        orch = make_orch(tmp_path, [101], concurrency=1)
        orch.sm.update_bug(101, status="active", pid=None)
        (tmp_path / "fix-and-remediate-101.json").write_text(
            json.dumps({"phase": "complete", "prNumber": 0}))
        monkeypatch.setattr(orch, "_teardown", lambda path: True)
        monkeypatch.setattr("orchestrate_bugs.is_agent_process", lambda pid: False)

        orch.check_completions()

        bug = orch.sm.get_bug(101)
        assert bug["status"] == "error"
        assert bug["outcome"] == "no_pr_created"


class TestEscalatedOutcomeFromStateFile:
    """Reads state file with phase: escalated."""

    def test_escalated(self, tmp_path):
        state_file = tmp_path / "fix-and-remediate-201.json"
        state_file.write_text(json.dumps({"phase": "escalated", "prNumber": 55}))
        outcome, pr, reason = determine_outcome(201, None, container_id=None, state_dir=str(tmp_path))
        assert outcome == "escalated"
        assert pr == "55"
        assert reason is None


class TestCompleteWithoutPrDowngradesToError:
    """complete + no PR = error (guard in check_completions, not in determine_outcome itself)."""

    def test_complete_without_pr(self, tmp_path):
        state_file = tmp_path / "fix-and-remediate-301.json"
        state_file.write_text(json.dumps({"phase": "complete", "prNumber": None}))
        # determine_outcome itself reports complete; the downgrade happens in the caller
        outcome, pr, reason = determine_outcome(301, None, container_id=None, state_dir=str(tmp_path))
        assert outcome == "complete"
        assert pr == "none"
        assert reason is None


class TestFallbackToLogScanning:
    """Falls back to log tail when no state file exists."""

    def test_log_scan_complete(self, tmp_path):
        log_file = tmp_path / "bug-401.log"
        log_file.write_text("lots of output\nReady for review\nmore output\n")

        outcome, pr, reason = determine_outcome(
            401, str(log_file), container_id=None, state_dir=str(tmp_path),
        )
        assert outcome == "complete"
        assert pr == "none"
        assert reason is None

    def test_log_scan_escalated(self, tmp_path):
        log_file = tmp_path / "bug-402.log"
        log_file.write_text("lots of output\nescalation-needed tag applied\nmore output\n")

        outcome, pr, reason = determine_outcome(
            402, str(log_file), container_id=None, state_dir=str(tmp_path),
        )
        assert outcome == "escalated"
        assert pr == "none"
        assert reason is None

    def test_log_scan_error(self, tmp_path):
        log_file = tmp_path / "bug-403.log"
        log_file.write_text("some output that doesn't match any pattern\n")

        outcome, pr, reason = determine_outcome(
            403, str(log_file), container_id=None, state_dir=str(tmp_path),
        )
        assert outcome == "error"
        assert pr == "none"
        assert reason is None


# ── Orchestrator tests ───────────────────────────────────────────────


class TestFillSlotsRespectsConcurrency:
    """fill_slots launches at most N bugs."""

    def test_respects_concurrency(self, tmp_path, monkeypatch):
        bugs_file = tmp_path / "bugs.json"
        bugs_file.write_text(json.dumps(make_bugs([101, 102, 103, 104])))

        orch = Orchestrator(
            concurrency=2,
            bugs_file=str(bugs_file),
            script_dir=str(tmp_path),
            state_dir=str(tmp_path),
            base_branch="main",
        )
        orch.sm.init_from_bugs(2, make_bugs([101, 102, 103, 104]), orch.base_dir)

        # Track which bugs got launched
        launched = []

        def mock_launch(ref):
            launched.append(ref)
            orch.sm.update_bug(ref, status="active")
            return True

        monkeypatch.setattr(orch, "launch_bug", mock_launch)

        orch.fill_slots()
        # Launches run on worker threads; wait for the stubs to flip to active.
        join_launch_threads(orch)

        # Should have launched exactly 2 (concurrency limit)
        assert len(launched) == 2
        assert orch.sm.count_status("active") == 2
        assert orch.sm.count_status("queued") == 2


class TestIsDoneWhenAllTerminal:
    """is_done returns True when no active/queued/launching bugs."""

    def test_is_done(self, tmp_path):
        orch = Orchestrator(
            concurrency=2,
            bugs_file="unused",
            script_dir=str(tmp_path),
            state_dir=str(tmp_path),
            base_branch="main",
        )
        orch.sm.init_from_bugs(2, make_bugs([101, 102]), orch.base_dir)
        orch.sm.update_bug(101, status="complete")
        orch.sm.update_bug(102, status="escalated")

        assert orch.is_done() is True

    def test_not_done_with_queued(self, tmp_path):
        orch = Orchestrator(
            concurrency=2,
            bugs_file="unused",
            script_dir=str(tmp_path),
            state_dir=str(tmp_path),
            base_branch="main",
        )
        orch.sm.init_from_bugs(2, make_bugs([101, 102]), orch.base_dir)
        orch.sm.update_bug(101, status="complete")

        assert orch.is_done() is False


class TestLaunchFailureRetry:
    """Retries launch then marks error after MAX_LAUNCH_ATTEMPTS."""

    def test_retry_then_error(self, tmp_path, monkeypatch):
        orch = Orchestrator(
            concurrency=2,
            bugs_file="unused",
            script_dir=str(tmp_path),
            state_dir=str(tmp_path),
            base_branch="main",
        )
        orch.sm.init_from_bugs(2, make_bugs([101]), orch.base_dir)

        # Mock _teardown to be a no-op
        monkeypatch.setattr(orch, "_teardown", lambda path: True)

        clone_path = orch.sm.get_bug(101)["clonePath"]

        # First two failures should retry (queued)
        for i in range(MAX_LAUNCH_ATTEMPTS - 1):
            orch._handle_launch_failure(101, clone_path, "test failure")
            assert orch.sm.get_bug(101)["status"] == "queued"
            assert orch.sm.get_bug(101)["launchAttempts"] == i + 1

        # Third failure should mark as error
        orch._handle_launch_failure(101, clone_path, "test failure")
        assert orch.sm.get_bug(101)["status"] == "error"
        assert orch.sm.get_bug(101)["outcome"] == "launch_failed"


class TestResumeResetDeadProcesses:
    """Dead PIDs are reset to queued on resume."""

    def test_dead_pid_resets(self, tmp_path, monkeypatch):
        orch = Orchestrator(
            concurrency=2,
            bugs_file="unused",
            script_dir=str(tmp_path),
            state_dir=str(tmp_path),
            base_branch="main",
        )
        orch.sm.init_from_bugs(2, make_bugs([101, 102]), orch.base_dir)
        orch.sm.update_bug(101, status="active", pid=99999)
        orch.sm.update_bug(102, status="active", pid=99998)

        # Mock is_agent_process to return False (dead)
        monkeypatch.setattr(
            "orchestrate_bugs.is_agent_process", lambda pid: False,
        )
        # Mock _teardown
        monkeypatch.setattr(orch, "_teardown", lambda path: True)
        # Mock determine_outcome to return error
        monkeypatch.setattr(
            "orchestrate_bugs.determine_outcome",
            lambda ref, log_file, container_id=None, state_dir="/tmp": ("error", "none", None),
        )

        orch._resume()

        # Both should be reset to queued
        assert orch.sm.get_bug(101)["status"] == "queued"
        assert orch.sm.get_bug(102)["status"] == "queued"


class TestAlreadyClosedOutcome:
    """phase: already-closed → outcome: already_closed (distinct from complete)."""

    def test_already_closed_from_state_file(self, tmp_path):
        state_file = tmp_path / "fix-and-remediate-501.json"
        state_file.write_text(json.dumps({"phase": "already-closed"}))
        outcome, pr, reason = determine_outcome(
            501, None, container_id=None, state_dir=str(tmp_path),
        )
        assert outcome == "already_closed"
        assert pr == "none"
        assert reason is None


class TestNonTerminalPhaseWithPrBecomesEscalated:
    """Non-terminal phase + PR → escalated (bug #1825 defense-in-depth)."""

    def test_non_terminal_with_pr(self, tmp_path):
        state_file = tmp_path / "fix-and-remediate-701.json"
        state_file.write_text(json.dumps({"phase": "awaiting_fix", "prNumber": 999}))
        outcome, pr, reason = determine_outcome(
            701, None, container_id=None, state_dir=str(tmp_path),
        )
        assert outcome == "escalated"
        assert pr == "999"
        assert reason is not None
        assert "non-terminal" in reason


class TestNonTerminalPhaseWithoutPrStaysError:
    """Non-terminal phase + no PR → error (no defense-in-depth rescue)."""

    def test_non_terminal_without_pr(self, tmp_path):
        state_file = tmp_path / "fix-and-remediate-702.json"
        state_file.write_text(json.dumps({"phase": "awaiting_fix", "prNumber": None}))
        outcome, pr, reason = determine_outcome(
            702, None, container_id=None, state_dir=str(tmp_path),
        )
        assert outcome == "error"
        assert pr == "none"
        assert reason is None


class TestEmptyPhaseWithPrBecomesEscalated:
    """Missing/empty phase + PR → escalated."""

    def test_empty_phase_with_pr(self, tmp_path):
        state_file = tmp_path / "fix-and-remediate-703.json"
        state_file.write_text(json.dumps({"prNumber": 888}))
        outcome, pr, reason = determine_outcome(
            703, None, container_id=None, state_dir=str(tmp_path),
        )
        assert outcome == "escalated"
        assert pr == "888"
        assert reason is not None


class TestKnownTerminalPhasesUnaffected:
    """Terminal phases (complete, escalated, already-closed) are unchanged."""

    def test_complete_unchanged(self, tmp_path):
        state_file = tmp_path / "fix-and-remediate-704.json"
        state_file.write_text(json.dumps({"phase": "complete", "prNumber": 42}))
        outcome, pr, reason = determine_outcome(
            704, None, container_id=None, state_dir=str(tmp_path),
        )
        assert outcome == "complete"
        assert pr == "42"
        assert reason is None

    def test_escalated_unchanged(self, tmp_path):
        state_file = tmp_path / "fix-and-remediate-705.json"
        state_file.write_text(json.dumps({"phase": "escalated", "prNumber": 55}))
        outcome, pr, reason = determine_outcome(
            705, None, container_id=None, state_dir=str(tmp_path),
        )
        assert outcome == "escalated"
        assert pr == "55"
        assert reason is None

    def test_already_closed_unchanged(self, tmp_path):
        state_file = tmp_path / "fix-and-remediate-706.json"
        state_file.write_text(json.dumps({"phase": "already-closed"}))
        outcome, pr, reason = determine_outcome(
            706, None, container_id=None, state_dir=str(tmp_path),
        )
        assert outcome == "already_closed"
        assert pr == "none"
        assert reason is None


class TestLaunchTimeoutConfiguration:
    """Launch timeout defaults to 1800s and is overridable via ORCHESTRATE_LAUNCH_TIMEOUT."""

    def _make_orch(self, tmp_path):
        return Orchestrator(
            concurrency=2,
            bugs_file="unused",
            script_dir=str(tmp_path),
            state_dir=str(tmp_path),
            base_branch="main",
            # launch_bug fetches the base from `upstream` first; a sandbox
            # host repo keeps that offline instead of hitting the developer's
            # real clone (or failing where no `upstream` remote exists, as in CI).
            repo_root=make_host_repo(tmp_path / "host-repo", remote="upstream"),
        )

    def test_default_is_1800(self, tmp_path, monkeypatch):
        monkeypatch.delenv("ORCHESTRATE_LAUNCH_TIMEOUT", raising=False)
        orch = self._make_orch(tmp_path)
        assert orch.launch_timeout == 1800

    def test_env_override(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ORCHESTRATE_LAUNCH_TIMEOUT", "2400")
        orch = self._make_orch(tmp_path)
        assert orch.launch_timeout == 2400

    def test_launch_bug_passes_timeout_and_reports_timeout_reason(self, tmp_path, monkeypatch):
        import subprocess as sp

        orch = self._make_orch(tmp_path)
        orch.sm.init_from_bugs(2, make_bugs([801]), orch.base_dir)
        monkeypatch.setattr(orch, "_teardown", lambda path: True)

        seen = {}

        def mock_run_script(script_name, *args, timeout=60, ref=None):
            seen["timeout"] = timeout
            raise sp.TimeoutExpired(cmd=script_name, timeout=timeout)

        monkeypatch.setattr(orch, "_run_script", mock_run_script)

        failures = []

        def mock_failure(ref, clone_path, reason, container_id=None):
            failures.append(reason)
            return False

        monkeypatch.setattr(orch, "_handle_launch_failure", mock_failure)

        orch.launch_bug(801)

        assert seen["timeout"] == orch.launch_timeout
        assert failures == [f"devcontainer launch timed out after {orch.launch_timeout}s"]


class TestRunScriptKillsProcessGroupOnTimeout:
    """_run_script must kill the script's whole process group on timeout so
    grandchildren (e.g. devcontainer up) don't outlive the launch attempt."""

    def test_grandchild_killed_on_timeout(self, tmp_path):
        import subprocess as sp
        import time as time_mod

        orch = Orchestrator(
            concurrency=1,
            bugs_file="unused",
            script_dir=str(tmp_path),
            state_dir=str(tmp_path),
            base_branch="main",
        )

        pid_file = tmp_path / "grandchild.pid"
        script = tmp_path / "slow.sh"
        script.write_text(
            "#!/usr/bin/env bash\n"
            "sleep 300 &\n"
            f"echo $! > {pid_file}\n"
            "wait\n"
        )
        script.chmod(0o755)

        with pytest.raises(sp.TimeoutExpired):
            orch._run_script("slow.sh", timeout=2)

        grandchild_pid = int(pid_file.read_text().strip())

        def process_gone():
            try:
                os.kill(grandchild_pid, 0)
                return False
            except (OSError, ProcessLookupError):
                return True

        deadline = time_mod.monotonic() + 5
        while not process_gone() and time_mod.monotonic() < deadline:
            time_mod.sleep(0.1)
        assert process_gone(), f"grandchild {grandchild_pid} still alive after timeout"


class TestHandleLaunchFailurePersistsContainerIdOnRetry:
    """On retry path, containerId is persisted so _final_cleanup can reap leaks."""

    def test_retry_persists_container_id(self, tmp_path, monkeypatch):
        orch = Orchestrator(
            concurrency=2,
            bugs_file="unused",
            script_dir=str(tmp_path),
            state_dir=str(tmp_path),
            base_branch="main",
        )
        orch.sm.init_from_bugs(2, make_bugs([601]), orch.base_dir)
        monkeypatch.setattr(orch, "_teardown", lambda path: True)

        clone_path = orch.sm.get_bug(601)["clonePath"]
        orch._handle_launch_failure(
            601, clone_path, "prepare-clone failed", container_id="abc123",
        )

        bug = orch.sm.get_bug(601)
        assert bug["status"] == "queued"
        assert bug["launchAttempts"] == 1
        assert bug["containerId"] == "abc123"

    def test_terminal_error_still_persists_container_id(self, tmp_path, monkeypatch):
        orch = Orchestrator(
            concurrency=2,
            bugs_file="unused",
            script_dir=str(tmp_path),
            state_dir=str(tmp_path),
            base_branch="main",
        )
        orch.sm.init_from_bugs(2, make_bugs([602]), orch.base_dir)
        monkeypatch.setattr(orch, "_teardown", lambda path: True)

        clone_path = orch.sm.get_bug(602)["clonePath"]
        # Simulate attempts already at MAX - 1, then fail once more
        orch.sm.update_bug(602, launchAttempts=MAX_LAUNCH_ATTEMPTS - 1)
        orch._handle_launch_failure(
            602, clone_path, "prepare-clone failed", container_id="xyz789",
        )

        bug = orch.sm.get_bug(602)
        assert bug["status"] == "error"
        assert bug["outcome"] == "launch_failed"
        assert bug["containerId"] == "xyz789"


# ── Asynchronous launch tests (bug #2630) ───────────────────────────


def join_launch_threads(orch, timeout=10):
    """Wait for every in-flight launch thread to finish."""
    for t in list(orch.launch_threads.values()):
        t.join(timeout=timeout)


def make_orch(tmp_path, refs, concurrency):
    """Build an Orchestrator with initialized state over the given refs."""
    orch = Orchestrator(
        concurrency=concurrency,
        bugs_file="unused",
        script_dir=str(tmp_path),
        state_dir=str(tmp_path),
        base_branch="main",
        # A sandbox host repo whose `main` is current keeps the launch's
        # base-branch fetch offline and off the developer's own clone.
        repo_root=make_host_repo(tmp_path / "host-repo", remote="upstream"),
    )
    orch.sm.init_from_bugs(concurrency, make_bugs(refs), orch.base_dir)
    return orch


class TestFillSlotsDoesNotBlockOnLaunch:
    """A launch takes ~10 minutes; fill_slots must not wait for it."""

    def test_returns_while_launches_are_in_flight(self, tmp_path, monkeypatch):
        import threading
        import time as time_mod

        orch = make_orch(tmp_path, [101, 102, 103], concurrency=3)

        release = threading.Event()

        def blocking_launch(ref):
            release.wait(timeout=30)
            return True

        monkeypatch.setattr(orch, "launch_bug", blocking_launch)

        start = time_mod.monotonic()
        orch.fill_slots()
        elapsed = time_mod.monotonic() - start

        try:
            assert elapsed < 2, f"fill_slots blocked for {elapsed:.1f}s"
            assert orch.sm.count_status("launching") == 3
            assert orch.sm.count_status("queued") == 0
        finally:
            release.set()
            join_launch_threads(orch)


class TestCompletionsReapedDuringInFlightLaunch:
    """An agent that finishes mid-launch must release its slot immediately."""

    def test_completion_processed_while_launching(self, tmp_path, monkeypatch):
        import threading

        import orchestrate_bugs

        orch = make_orch(tmp_path, [101, 102], concurrency=2)
        orch.base_dir.mkdir(parents=True, exist_ok=True)
        orch.sm.update_bug(101, status="active", pid=4242)

        class FinishedProc:
            pid = 4242

            def poll(self):
                return 0

        orch.child_procs[101] = FinishedProc()

        monkeypatch.setattr(orch, "_teardown", lambda path: True)
        monkeypatch.setattr(
            orchestrate_bugs, "determine_outcome",
            lambda ref, log_file, container_id=None, state_dir="/tmp": ("complete", "55", None),
        )

        release = threading.Event()

        def blocking_launch(ref):
            release.wait(timeout=30)
            return True

        monkeypatch.setattr(orch, "launch_bug", blocking_launch)

        orch.fill_slots()
        try:
            assert orch.sm.get_bug(102)["status"] == "launching"

            orch.check_completions()

            assert orch.sm.get_bug(101)["status"] == "complete"
            assert orch.sm.get_bug(101)["prNumber"] == 55
            # The launch is still outstanding — the completion did not wait on it.
            assert orch.sm.get_bug(102)["status"] == "launching"
        finally:
            release.set()
            join_launch_threads(orch)


class TestLaunchingCountsAgainstConcurrency:
    """An in-flight launch occupies its slot, so concurrency is never exceeded."""

    def test_only_one_more_started(self, tmp_path, monkeypatch):
        import threading

        orch = make_orch(tmp_path, [101, 102, 103], concurrency=2)
        orch.sm.update_bug(101, status="launching")

        release = threading.Event()
        started = []

        def blocking_launch(ref):
            started.append(ref)
            release.wait(timeout=30)
            return True

        monkeypatch.setattr(orch, "launch_bug", blocking_launch)

        orch.fill_slots()
        try:
            assert orch.sm.count_status("launching") == 2
            assert orch.sm.count_status("queued") == 1
            assert len(orch.launch_threads) == 1
        finally:
            release.set()
            join_launch_threads(orch)


class TestLaunchWorkerClearsThreadRegistryOnException:
    """A crashed launch thread must not leave its slot wedged."""

    def test_registry_cleared_and_bug_requeued(self, tmp_path, monkeypatch):
        import threading

        orch = make_orch(tmp_path, [101], concurrency=1)
        orch.sm.update_bug(101, status="launching")
        monkeypatch.setattr(orch, "_teardown", lambda path: True)

        def exploding_launch(ref):
            raise RuntimeError("boom")

        monkeypatch.setattr(orch, "launch_bug", exploding_launch)

        # _launch_worker deregisters the thread it is running on, so register
        # this one the way fill_slots would.
        orch.launch_threads[101] = threading.current_thread()
        orch._launch_worker(101)

        assert 101 not in orch.launch_threads
        assert orch.sm.get_bug(101)["status"] in ("queued", "error")


class TestCleanupLaunchingKillsInFlightLaunchProcess:
    """Shutdown must kill helper scripts still running inside a launch."""

    def test_launch_process_killed_and_bug_requeued(self, tmp_path, monkeypatch):
        import subprocess as sp
        import time as time_mod

        orch = make_orch(tmp_path, [101], concurrency=1)
        orch.sm.update_bug(101, status="launching")
        monkeypatch.setattr(orch, "_teardown", lambda path: True)

        proc = sp.Popen(["sleep", "60"], start_new_session=True)
        orch.launch_procs[101] = proc

        orch._cleanup_launching()

        deadline = time_mod.monotonic() + 5
        while proc.poll() is None and time_mod.monotonic() < deadline:
            time_mod.sleep(0.1)

        assert proc.poll() is not None, "in-flight launch process survived cleanup"
        assert orch.sm.get_bug(101)["status"] == "queued"


class TestStateManagerConcurrentUpdates:
    """Concurrent saves must not corrupt the state file."""

    def test_concurrent_updates_all_persist(self, tmp_path):
        import threading

        refs = list(range(101, 109))
        state_path = str(tmp_path / "state.json")
        sm = StateManager(state_path)
        sm.init_from_bugs(3, make_bugs(refs), tmp_path / "bugs")

        errors = []

        def worker(ref):
            try:
                for _ in range(20):
                    sm.update_bug(ref, status="active", pid=ref)
            except Exception as e:  # concurrent saves clobbering each other
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(r,)) for r in refs]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert errors == []

        with open(state_path) as f:
            persisted = json.load(f)

        assert len(persisted["bugs"]) == len(refs)
        for bug in persisted["bugs"]:
            assert bug["status"] == "active"
            assert bug["pid"] == bug["ref"]


class TestShutdownDoesNotConsumeLaunchAttempt:
    """A launch killed by shutdown is not the bug's fault."""

    def test_no_launch_failure_recorded(self, tmp_path, monkeypatch):
        orch = make_orch(tmp_path, [101], concurrency=1)
        orch.sm.update_bug(101, status="launching")

        def killed_launch_script(script_name, *args, timeout=60, ref=None):
            orch.shutting_down = True
            return "", -9

        monkeypatch.setattr(orch, "_run_script", killed_launch_script)

        failures = []
        monkeypatch.setattr(
            orch, "_handle_launch_failure",
            lambda *a, **kw: failures.append(a) or False,
        )

        assert orch.launch_bug(101) is False
        assert failures == []
        assert orch.sm.get_bug(101)["launchAttempts"] == 0
        assert orch.sm.get_bug(101)["status"] == "launching"


class TestFastFailingLaunchDoesNotBurnEveryAttempt:
    """One flaky bug must not consume all its launch attempts in one cycle."""

    def test_ref_retried_on_a_later_cycle(self, tmp_path, monkeypatch):
        orch = make_orch(tmp_path, [101, 102], concurrency=2)

        attempts = []

        def fast_failing_launch(ref):
            attempts.append(ref)
            # Requeue from the worker thread, as _handle_launch_failure does,
            # while fill_slots is still looping.
            bug = orch.sm.get_bug(ref)
            orch.sm.update_bug(
                ref, status="queued", launchAttempts=bug["launchAttempts"] + 1,
            )
            return False

        monkeypatch.setattr(orch, "launch_bug", fast_failing_launch)

        orch.fill_slots()
        join_launch_threads(orch)

        assert attempts.count(101) == 1
        assert orch.sm.get_bug(101)["launchAttempts"] == 1


class TestWorkerDoesNotDeregisterARetryThread:
    """A slow worker must not clear the registry entry a retry just claimed."""

    def test_retry_thread_survives_old_workers_finally(self, tmp_path, monkeypatch):
        orch = make_orch(tmp_path, [101], concurrency=1)
        orch.sm.update_bug(101, status="launching")
        monkeypatch.setattr(orch, "_teardown", lambda path: True)

        retry_thread = object()  # stand-in for the retry's Thread

        def failing_launch(ref):
            # The poll loop registers a retry's thread before this worker's
            # `finally` runs.
            orch.launch_threads[ref] = retry_thread
            return False

        monkeypatch.setattr(orch, "launch_bug", failing_launch)

        orch._launch_worker(101)

        assert orch.launch_threads.get(101) is retry_thread


class TestShutdownDuringPrepareCloneDoesNotConsumeAttempt:
    """prepare-clone.sh killed by shutdown is not the bug's fault."""

    def test_no_launch_failure_recorded(self, tmp_path, monkeypatch):
        orch = make_orch(tmp_path, [101], concurrency=1)
        orch.sm.update_bug(101, status="launching")

        def scripts(script_name, *args, timeout=60, ref=None):
            if script_name == "launch.sh":
                return "CONTAINER_ID=stub\n", 0
            orch.shutting_down = True  # prepare-clone.sh killed by the shutdown
            return "", -9

        monkeypatch.setattr(orch, "_run_script", scripts)

        failures = []
        monkeypatch.setattr(
            orch, "_handle_launch_failure",
            lambda *a, **kw: failures.append(a) or False,
        )

        assert orch.launch_bug(101) is False
        assert failures == []
        assert orch.sm.get_bug(101)["launchAttempts"] == 0


class TestShutdownDoesNotDeadlockOnProcLock:
    """The signal handler re-enters proc_lock on the main thread."""

    def test_cleanup_runs_while_lock_is_held(self, tmp_path, monkeypatch):
        orch = make_orch(tmp_path, [101], concurrency=1)
        orch.sm.update_bug(101, status="launching")
        monkeypatch.setattr(orch, "_teardown", lambda path: True)

        # Holding proc_lock is exactly the state the main thread is in when a
        # signal arrives mid-registration; cleanup must still acquire it.
        with orch.proc_lock:
            # Checked before the call so a regression to a plain Lock fails
            # here instead of deadlocking the suite.
            assert orch.proc_lock.acquire(blocking=False), "proc_lock is not reentrant"
            orch.proc_lock.release()
            orch._cleanup_launching()

        assert orch.sm.get_bug(101)["status"] == "queued"


class TestCleanupJoinsSkipUnstartedThreads:
    """A thread registered but never started cannot be joined."""

    def test_unstarted_thread_does_not_abort_cleanup(self, tmp_path, monkeypatch):
        import threading

        orch = make_orch(tmp_path, [101], concurrency=1)
        orch.sm.update_bug(101, status="launching")
        monkeypatch.setattr(orch, "_teardown", lambda path: True)

        # Registered by fill_slots, signal arrived before t.start().
        orch.launch_threads[101] = threading.Thread(target=lambda: None)

        orch._cleanup_launching()

        assert orch.sm.get_bug(101)["status"] == "queued"


class TestCleanupSweepsWorkersThatTurnActiveDuringShutdown:
    """A worker can spawn its agent after _cleanup_active has already run."""

    def test_late_active_bug_is_terminated_and_requeued(self, tmp_path, monkeypatch):
        import threading

        orch = make_orch(tmp_path, [101], concurrency=1)
        orch.sm.update_bug(101, status="launching")
        monkeypatch.setattr(orch, "_teardown", lambda path: True)

        killed = []

        class LateProc:
            pid = None

            def terminate(self):
                killed.append("terminate")

            def kill(self):
                killed.append("kill")

        def flip_to_active():
            orch.sm.update_bug(101, status="active")
            orch.child_procs[101] = LateProc()

        t = threading.Thread(target=flip_to_active)
        orch.launch_threads[101] = t
        t.start()

        orch._cleanup_launching()

        assert "terminate" in killed
        assert orch.sm.get_bug(101)["status"] == "queued"


class TestRunScriptRegistersBeforeShutdownCanMissIt:
    """A helper spawned after cleanup swept must still be killed."""

    def test_process_killed_when_already_shutting_down(self, tmp_path):
        import time as time_mod

        orch = make_orch(tmp_path, [101], concurrency=1)
        script = tmp_path / "slow.sh"
        script.write_text("#!/usr/bin/env bash\nsleep 60\n")
        script.chmod(0o755)

        # Cleanup already ran and found launch_procs empty; this launch is the
        # straggler that registered afterwards.
        orch.shutting_down = True
        start = time_mod.monotonic()
        _, rc = orch._run_script("slow.sh", timeout=30, ref=101)
        elapsed = time_mod.monotonic() - start

        # Killed on the spot rather than left to run out its 60s sleep.
        assert elapsed < 5, f"_run_script waited {elapsed:.1f}s for a doomed helper"
        assert rc != 0
        assert 101 not in orch.launch_procs, "launch process was never deregistered"


class TestOrchestratorReadsHostStateFromStateDir:
    """The orchestrator reads host-side per-bug completion files from its own state dir."""

    def _orchestrator(self, tmp_path, monkeypatch):
        orch = Orchestrator(
            concurrency=1,
            bugs_file="unused",
            script_dir=str(tmp_path),
            state_dir=str(tmp_path),
            base_branch="main",
        )
        orch.sm.init_from_bugs(1, make_bugs([101]), orch.base_dir)
        orch.sm.update_bug(101, status="active", pid=99999)
        monkeypatch.setattr(
            "orchestrate_bugs.is_agent_process", lambda pid: False,
        )
        monkeypatch.setattr(orch, "_teardown", lambda path: True)
        return orch

    def test_check_completions_reads_completion_file_from_state_dir(
        self, tmp_path, monkeypatch,
    ):
        orch = self._orchestrator(tmp_path, monkeypatch)

        # In agent-override mode the orchestrator reads the host-side bug state
        # from its OWN state dir, never the shared global /tmp: two runs on one
        # host would otherwise read and delete each other's completion files.
        host_state = tmp_path / "fix-and-remediate-101.json"
        host_state.write_text(json.dumps({"phase": "complete", "prNumber": 42}))

        orch.check_completions()

        bug = orch.sm.get_bug(101)
        assert bug["status"] == "complete"
        assert bug["outcome"] == "complete"
        assert bug["prNumber"] == 42
        assert not host_state.exists()

    def test_resume_reads_completion_file_from_state_dir(
        self, tmp_path, monkeypatch,
    ):
        orch = self._orchestrator(tmp_path, monkeypatch)

        host_state = tmp_path / "fix-and-remediate-101.json"
        host_state.write_text(json.dumps({"phase": "complete", "prNumber": 42}))

        orch._resume()

        bug = orch.sm.get_bug(101)
        assert bug["status"] == "complete"
        assert bug["outcome"] == "complete"
        assert bug["prNumber"] == 42
        assert not host_state.exists()


# ── Project adaptations: upstream remote, container mode, exec-skill.sh ──


class TestRemoteAndRepoConstants:
    def test_remote_and_repo_default_to_upstream_and_cholobots(self):
        import orchestrate_bugs

        assert orchestrate_bugs.REMOTE == "upstream"
        assert orchestrate_bugs.GH_REPO == "MrsBohemian/Cholobots"

    def test_gh_cmd_drops_gh_token_and_names_the_repo(self):
        import orchestrate_bugs

        assert orchestrate_bugs.gh_cmd("pr", "view", "7") == [
            "env", "-u", "GH_TOKEN", "gh", "pr", "view", "7",
            "--repo", "MrsBohemian/Cholobots",
        ]

    def test_remote_and_repo_honour_env_overrides(self, monkeypatch):
        import importlib
        import orchestrate_bugs

        monkeypatch.setenv("ORCHESTRATE_REMOTE", "fork")
        monkeypatch.setenv("GH_REPO", "someone/else")
        try:
            reloaded = importlib.reload(orchestrate_bugs)
            assert reloaded.REMOTE == "fork"
            assert reloaded.GH_REPO == "someone/else"
        finally:
            monkeypatch.undo()
            importlib.reload(orchestrate_bugs)


class TestStateRecordsMode:
    def test_state_file_records_container_mode(self, tmp_path):
        orch = make_orch(tmp_path, [101], concurrency=1)

        assert orch.sm.state["mode"] == "container"
        assert json.loads(open(orch.sm.state_file).read())["mode"] == "container"

    def test_resume_refuses_a_worktree_mode_state_file(self, tmp_path, capsys):
        orch = make_orch(tmp_path, [101], concurrency=1)
        orch.sm.state["mode"] = "worktree"
        orch.sm.save()
        before = open(orch.sm.state_file).read()

        again = Orchestrator(1, "unused", str(tmp_path), str(tmp_path), "main")
        with pytest.raises(SystemExit) as exc:
            again.init_or_resume()

        assert exc.value.code == 2
        assert "worktree" in capsys.readouterr().err
        assert open(orch.sm.state_file).read() == before

    def test_resume_treats_a_file_without_mode_as_container(self, tmp_path):
        orch = make_orch(tmp_path, [101], concurrency=1)
        del orch.sm.state["mode"]
        orch.sm.save()

        again = Orchestrator(1, "unused", str(tmp_path), str(tmp_path), "main")
        again.init_or_resume()

        assert again.sm.get_bug(101) is not None


class TestBaseBranchPersistence:
    """The run's base branch is stored at init and wins over BASE_BRANCH on resume."""

    def _orch(self, tmp_path, base_branch):
        return Orchestrator(
            1, str(tmp_path / "bugs.json"), str(tmp_path), str(tmp_path), base_branch)

    def test_init_stores_run_base_branch(self, tmp_path):
        (tmp_path / "bugs.json").write_text(json.dumps(make_bugs([101])))
        orch = self._orch(tmp_path, "feature/x")

        orch.init_or_resume()

        assert orch.sm.state["baseBranch"] == "feature/x"
        assert json.loads(Path(orch.sm.state_file).read_text())["baseBranch"] == "feature/x"

    def test_stored_base_branch_beats_env_on_resume(self, tmp_path, capsys):
        (tmp_path / "bugs.json").write_text(json.dumps(make_bugs([101])))
        self._orch(tmp_path, "feature/x").init_or_resume()

        again = self._orch(tmp_path, "main")
        again.init_or_resume()

        assert again.base_branch == "feature/x"
        assert "using stored baseBranch=feature/x" in capsys.readouterr().err

    def test_state_file_without_base_branch_is_backfilled_from_env(self, tmp_path):
        (tmp_path / "bugs.json").write_text(json.dumps(make_bugs([101])))
        first = self._orch(tmp_path, "feature/x")
        first.init_or_resume()
        del first.sm.state["baseBranch"]
        first.sm.save()

        again = self._orch(tmp_path, "develop")
        again.init_or_resume()

        assert again.base_branch == "develop"
        assert json.loads(Path(again.sm.state_file).read_text())["baseBranch"] == "develop"


class TestBugAgentRunsThroughExecSkill:
    def test_bug_agent_runs_through_exec_skill(self, tmp_path, monkeypatch):
        orch = make_orch(tmp_path, [101], concurrency=1)
        orch.sm.update_bug(101, status="launching")
        bug = orch.sm.get_bug(101)
        skill = (
            Path(bug["clonePath"]) / ".claude/skills/fix-and-remediate-bug/SKILL.md"
        )
        skill.parent.mkdir(parents=True)
        skill.write_text("skill")

        def scripts(script_name, *args, timeout=60, ref=None):
            return ("CONTAINER_ID=abc123\n", 0) if script_name == "launch.sh" else ("", 0)

        monkeypatch.setattr(orch, "_run_script", scripts)
        # subprocess.Popen is stubbed globally below, which would also catch the
        # base-branch git calls; they have their own tests.
        monkeypatch.setattr(orch, "_ensure_base_reachable", lambda base: True)
        popen_calls = []

        class FakeProc:
            pid = 4242

        def fake_popen(cmd, **kwargs):
            popen_calls.append(cmd)
            return FakeProc()

        monkeypatch.setattr("orchestrate_bugs.subprocess.Popen", fake_popen)

        assert orch.launch_bug(101) is True

        assert popen_calls == [[
            str(tmp_path / "exec-skill.sh"), bug["clonePath"],
            "fix-and-remediate-bug", "101 main", "--log", bug["logFile"],
        ]]
        assert orch.sm.get_bug(101)["containerId"] == "abc123"


# ── PR #29 review: base branch, teardown failures, resume mapping ──


def _git(cwd, *args):
    import subprocess

    result = subprocess.run(
        ["git", "-c", "user.name=T", "-c", "user.email=t@example.invalid", *args],
        cwd=str(cwd), capture_output=True, text=True,
    )
    assert result.returncode == 0, f"git {' '.join(args)}: {result.stderr}"
    return result.stdout.strip()


class TestBaseBranchIsFetchedAndPropagated:
    """launch.sh clones the host's local branch, so the orchestrator has to make
    that branch current (or exist at all) first, then tell the worker its base."""

    def _launch_stubs(self, orch, monkeypatch):
        ran = []

        def scripts(script_name, *args, timeout=60, ref=None):
            ran.append(script_name)
            return ("CONTAINER_ID=abc123\n", 0) if script_name == "launch.sh" else ("", 0)

        monkeypatch.setattr(orch, "_run_script", scripts)
        return ran

    def test_unreachable_base_fails_the_launch_before_launch_sh(self, tmp_path, monkeypatch):
        orch = make_orch(tmp_path, [101], concurrency=1)
        orch.sm.update_bug(101, status="launching")
        ran = self._launch_stubs(orch, monkeypatch)
        monkeypatch.setattr(orch, "_ensure_base_reachable", lambda base: False)
        monkeypatch.setattr(orch, "_teardown", lambda path: True)

        assert orch.launch_bug(101) is False

        assert ran == []
        bug = orch.sm.get_bug(101)
        assert bug["status"] == "queued"
        assert bug["launchAttempts"] == 1

    def test_the_configured_base_is_what_gets_checked_and_cloned(self, tmp_path, monkeypatch):
        orch = make_orch(tmp_path, [101], concurrency=1)
        orch.base_branch = "epic/46-x"
        orch.sm.update_bug(101, status="launching")
        checked, launch_args = [], []

        def scripts(script_name, *args, timeout=60, ref=None):
            if script_name == "launch.sh":
                launch_args.append(args)
                return "CONTAINER_ID=abc123\n", 0
            return "", 0

        monkeypatch.setattr(orch, "_run_script", scripts)
        monkeypatch.setattr(orch, "_ensure_base_reachable", lambda base: checked.append(base) or True)
        bug = orch.sm.get_bug(101)
        skill = Path(bug["clonePath"]) / ".claude/skills/fix-and-remediate-bug/SKILL.md"
        skill.parent.mkdir(parents=True)
        skill.write_text("skill")
        popen_calls = []

        class FakeProc:
            pid = 4242

        monkeypatch.setattr(
            "orchestrate_bugs.subprocess.Popen",
            lambda cmd, **kw: popen_calls.append(cmd) or FakeProc(),
        )

        assert orch.launch_bug(101) is True

        assert checked == ["epic/46-x"]
        assert launch_args == [(bug["clonePath"], "epic/46-x")]
        # The worker's second argument is the base, so it branches and opens
        # its PR against it instead of falling back to upstream/main.
        assert popen_calls[0][3] == "101 epic/46-x"

    def test_a_stale_local_base_is_refreshed_from_the_remote(self, tmp_path):
        orch = make_orch(tmp_path, [101], concurrency=1)
        host = orch.repo_root
        remote_url = _git(host, "remote", "get-url", "upstream")
        # The branch exists only on the remote; the host has never seen it.
        seed = tmp_path / "seed2"
        _git(tmp_path, "clone", "--quiet", remote_url, str(seed))
        _git(seed, "switch", "-c", "epic/46-x")
        _git(seed, "commit", "--allow-empty", "-m", "epic work")
        _git(seed, "push", "--quiet", "origin", "epic/46-x")

        assert orch._ensure_base_reachable("epic/46-x") is True
        assert _git(host, "rev-parse", "refs/heads/epic/46-x") == _git(seed, "rev-parse", "HEAD")

        # The remote moves on; the local head must follow, not stay stale.
        _git(seed, "commit", "--allow-empty", "-m", "more epic work")
        _git(seed, "push", "--quiet", "origin", "epic/46-x")
        assert orch._ensure_base_reachable("epic/46-x") is True
        assert _git(host, "rev-parse", "refs/heads/epic/46-x") == _git(seed, "rev-parse", "HEAD")

    def test_a_base_missing_from_the_remote_is_not_reachable(self, tmp_path):
        orch = make_orch(tmp_path, [101], concurrency=1)

        assert orch._ensure_base_reachable("no/such-branch") is False

    def test_a_stale_checked_out_base_is_refused(self, tmp_path):
        orch = make_orch(tmp_path, [101], concurrency=1)
        orch.repo_root = make_host_repo(tmp_path / "stale", stale_main=True, remote="upstream")

        assert orch._ensure_base_reachable("main") is False


class TestTeardownReportsSuccess:
    def test_teardown_returns_false_when_the_script_fails(self, tmp_path):
        orch = make_orch(tmp_path, [101], concurrency=1)
        script = tmp_path / "teardown.sh"
        script.write_text("#!/usr/bin/env bash\nexit 1\n")
        script.chmod(0o755)

        assert orch._teardown(str(tmp_path / "clone")) is False

    def test_teardown_returns_true_when_the_script_succeeds(self, tmp_path):
        orch = make_orch(tmp_path, [101], concurrency=1)
        script = tmp_path / "teardown.sh"
        script.write_text("#!/usr/bin/env bash\nexit 0\n")
        script.chmod(0o755)

        assert orch._teardown(str(tmp_path / "clone")) is True


class TestFailedTeardownBlocksARetry:
    """A retry over an uncleaned clone binds a second container to the same
    folder, so a launch failure whose cleanup failed is terminal."""

    def test_launch_failure_with_failed_cleanup_is_not_requeued(self, tmp_path, monkeypatch):
        orch = make_orch(tmp_path, [101], concurrency=1)
        orch.sm.update_bug(101, status="launching")
        clone = orch.sm.get_bug(101)["clonePath"]
        monkeypatch.setattr(orch, "_teardown", lambda path: False)

        orch._handle_launch_failure(101, clone, "boom", container_id="abc123")

        bug = orch.sm.get_bug(101)
        assert bug["status"] == "error"
        assert bug["outcome"] == "launch_failed"
        # Kept so the final cleanup can still force-remove the leak.
        assert bug["containerId"] == "abc123"
        assert bug["clonePath"] == clone
        assert orch.sm.next_queued_ref() is None

    def test_resume_of_a_dead_agent_with_failed_cleanup_is_not_requeued(self, tmp_path, monkeypatch):
        orch = make_orch(tmp_path, [101], concurrency=1)
        orch.sm.update_bug(101, status="active", pid=99999, containerId="abc123")
        monkeypatch.setattr("orchestrate_bugs.is_agent_process", lambda pid: False)
        monkeypatch.setattr(
            "orchestrate_bugs.determine_outcome",
            lambda ref, log_file, container_id=None, state_dir="/tmp": ("error", "none", None),
        )
        monkeypatch.setattr(orch, "_teardown", lambda path: False)

        orch._resume()

        assert orch.sm.get_bug(101)["status"] == "error"
        assert orch.sm.next_queued_ref() is None

    def test_resume_of_a_launching_bug_with_failed_cleanup_is_not_requeued(self, tmp_path, monkeypatch):
        orch = make_orch(tmp_path, [101], concurrency=1)
        orch.sm.update_bug(101, status="launching")
        monkeypatch.setattr(orch, "_teardown", lambda path: False)

        orch._resume()

        assert orch.sm.get_bug(101)["status"] == "error"
        assert orch.sm.next_queued_ref() is None


class TestResumeMapsAlreadyClosedToComplete:
    def test_already_closed_on_resume_counts_as_complete(self, tmp_path, monkeypatch):
        orch = make_orch(tmp_path, [101], concurrency=1)
        orch.sm.update_bug(101, status="active", pid=99999)
        monkeypatch.setattr("orchestrate_bugs.is_agent_process", lambda pid: False)
        monkeypatch.setattr(
            "orchestrate_bugs.determine_outcome",
            lambda ref, log_file, container_id=None, state_dir="/tmp":
                ("already_closed", "none", None),
        )
        monkeypatch.setattr(orch, "_teardown", lambda path: True)

        orch._resume()

        bug = orch.sm.get_bug(101)
        assert bug["status"] == "complete"
        assert bug["outcome"] == "already_closed"
        assert orch.sm.count_status("complete") == 1
