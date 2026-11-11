# ABOUTME: Pytest tests for orchestrate_epic.py: determine_outcome, reconcile, and launch/teardown containment.
# ABOUTME: Verifies phase-to-outcome mapping, launch timeout teardown, rate-limit requeue and the upstream/gh_cmd contract.
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts", "devcontainer"))

from orchestrate_epic import (
    DEFAULT_TRANSIENT_BACKOFF_SECONDS,
    ISO_UTC_FORMAT,
    MAX_TRANSIENT_HOLD_SECONDS,
    MAX_TRANSIENT_RETRIES,
    Orchestrator,
    SMOKE_CMD,
    detect_transient_exit,
    determine_outcome,
    read_log_tail,
)
from conftest import make_host_repo


# ── determine_outcome tests ─────────────────────────────────────────


class TestCompleteOutcome:
    """phase: complete + PR → (complete, pr, None)."""

    def test_complete_with_pr(self, tmp_path):
        state_file = tmp_path / "fully-implement-101.json"
        state_file.write_text(json.dumps({"phase": "complete", "prNumber": 42}))
        outcome, pr, reason, _branch = determine_outcome(
            101, None, container_id=None, state_dir=str(tmp_path),
        )
        assert outcome == "complete"
        assert pr == "42"
        assert reason is None


class TestUnreviewablePrNumberIsNone:
    """Only a positive integer is a reviewable PR; anything else reads as no PR,
    so a "complete" with it is downgraded instead of unblocking dependents."""

    @pytest.mark.parametrize("pr", [0, "0", "", " ", "abc", -3, "-3", 1.5, True])
    def test_complete_with_invalid_pr_reports_none(self, tmp_path, pr):
        (tmp_path / "fully-implement-102.json").write_text(
            json.dumps({"phase": "complete", "prNumber": pr}))
        outcome, got, _reason, _branch = determine_outcome(
            102, None, container_id=None, state_dir=str(tmp_path),
        )
        assert outcome == "complete"
        assert got == "none"

    def test_numeric_string_pr_is_kept(self, tmp_path):
        (tmp_path / "fully-implement-102.json").write_text(
            json.dumps({"phase": "complete", "prNumber": "42"}))
        _outcome, got, _reason, _branch = determine_outcome(
            102, None, container_id=None, state_dir=str(tmp_path),
        )
        assert got == "42"


class TestFeatureBranchRecording:
    """The 4th determine_outcome element carries the story's feature branch."""

    def test_completed_story_records_its_feature_branch(self, tmp_path):
        state_file = tmp_path / "fully-implement-103.json"
        state_file.write_text(json.dumps({
            "phase": "complete", "prNumber": 43, "featureBranch": "feature/foo-103",
        }))
        outcome, pr, reason, branch = determine_outcome(
            103, None, container_id=None, state_dir=str(tmp_path),
        )
        assert outcome == "complete"
        assert branch == "feature/foo-103"

    def test_outcome_without_feature_branch_returns_none(self, tmp_path):
        state_file = tmp_path / "fully-implement-104.json"
        state_file.write_text(json.dumps({"phase": "complete", "prNumber": 44}))
        outcome, pr, reason, branch = determine_outcome(
            104, None, container_id=None, state_dir=str(tmp_path),
        )
        assert outcome == "complete"
        assert branch is None


class TestEscalatedOutcome:
    """phase: escalated + PR → (escalated, pr, None)."""

    def test_escalated_with_pr(self, tmp_path):
        state_file = tmp_path / "fully-implement-201.json"
        state_file.write_text(json.dumps({"phase": "escalated", "prNumber": 55}))
        outcome, pr, reason, _branch = determine_outcome(
            201, None, container_id=None, state_dir=str(tmp_path),
        )
        assert outcome == "escalated"
        assert pr == "55"
        assert reason is None


class TestHandoffPhaseBecomesHandedOff:
    """Handoff phase (monitoring) + PR → handed_off (clean handoff, not escalated).

    The story workflow writes phase="monitoring" after creating the PR and
    delegating CI monitoring to background /remediate-pr, then exits. Exiting
    here WITH a PR is a success handoff, counted as complete.
    """

    def test_monitoring_with_pr(self, tmp_path):
        state_file = tmp_path / "fully-implement-301.json"
        state_file.write_text(json.dumps({"phase": "monitoring", "prNumber": 77}))
        outcome, pr, reason, _branch = determine_outcome(
            301, None, container_id=None, state_dir=str(tmp_path),
        )
        assert outcome == "handed_off"
        assert pr == "77"
        assert reason is None


class TestNonTerminalPhaseWithPrBecomesEscalated:
    """Non-handoff non-terminal phase + PR → escalated."""

    def test_testing_with_pr(self, tmp_path):
        state_file = tmp_path / "fully-implement-302.json"
        state_file.write_text(json.dumps({"phase": "testing", "prNumber": 88}))
        outcome, pr, reason, _branch = determine_outcome(
            302, None, container_id=None, state_dir=str(tmp_path),
        )
        assert outcome == "escalated"
        assert pr == "88"
        assert reason is not None


class TestNonTerminalPhaseWithoutPrStaysError:
    """Non-terminal phase + no PR → error."""

    def test_testing_without_pr(self, tmp_path):
        state_file = tmp_path / "fully-implement-401.json"
        state_file.write_text(json.dumps({"phase": "testing", "prNumber": None}))
        outcome, pr, reason, _branch = determine_outcome(
            401, None, container_id=None, state_dir=str(tmp_path),
        )
        assert outcome == "error"
        assert pr == "none"
        assert reason is None

    def test_monitoring_without_pr(self, tmp_path):
        """A handoff phase with no PR is not a clean handoff — stays error."""
        state_file = tmp_path / "fully-implement-402.json"
        state_file.write_text(json.dumps({"phase": "monitoring", "prNumber": None}))
        outcome, pr, reason, _branch = determine_outcome(
            402, None, container_id=None, state_dir=str(tmp_path),
        )
        assert outcome == "error"
        assert pr == "none"
        assert reason is None


class TestFallbackToLogScanning:
    """Falls back to log tail when no state file exists."""

    def test_log_scan_complete(self, tmp_path):
        log_file = tmp_path / "story-501.log"
        log_file.write_text("lots of output\nReady for review\nmore output\n")
        outcome, pr, reason, _branch = determine_outcome(
            501, str(log_file), container_id=None, state_dir=str(tmp_path),
        )
        assert outcome == "complete"
        assert pr == "none"
        assert reason is None

    def test_log_scan_error(self, tmp_path):
        log_file = tmp_path / "story-502.log"
        log_file.write_text("some output that doesn't match any pattern\n")
        outcome, pr, reason, _branch = determine_outcome(
            502, str(log_file), container_id=None, state_dir=str(tmp_path),
        )
        assert outcome == "error"
        assert pr == "none"
        assert reason is None


class TestMissingStateFileNoLogs:
    """No state file and no log → error."""

    def test_missing_everything(self, tmp_path):
        outcome, pr, reason, _branch = determine_outcome(
            999, None, container_id=None, state_dir=str(tmp_path),
        )
        assert outcome == "error"
        assert pr == "none"
        assert reason is None


# ── transient (rate-limit) exit detection ───────────────


RATE_LIMIT_TEXT = "You've hit your session limit \u00b7 resets 8:40am (UTC)"
NOW = datetime(2026, 9, 8, 8, 0, tzinfo=timezone.utc)
# The epoch the CLI reports in rate_limit_event.rate_limit_info.resetsAt:
# 2026-09-08T08:40:00Z, forty minutes after NOW.
RESETS_AT = int(NOW.timestamp()) + 40 * 60


def rate_limit_event(resets_at=RESETS_AT, status="rejected", rate_limit_type="five_hour"):
    return {"type": "rate_limit_event", "rate_limit_info": {
        "status": status, "resetsAt": resets_at, "rateLimitType": rate_limit_type,
    }}


def write_stream_json_log(path, result_record, preamble_lines=3, events=()):
    """Write a stream-json agent log whose final record is `result_record`.

    `events` are extra records (e.g. rate_limit_event) written just before it.
    """
    lines = [json.dumps({"type": "assistant", "message": {"content": [{"type": "text", "text": f"narration {i}"}]}})
             for i in range(preamble_lines)]
    lines.extend(json.dumps(e) for e in events)
    lines.append(json.dumps(result_record))
    path.write_text("\n".join(lines) + "\n")


class TestDetectTransientExit:
    """A stream-json result record identifying a provider-side exit is transient."""

    def test_detect_rate_limit_result_record(self, tmp_path):
        log_file = tmp_path / "story-601.log"
        write_stream_json_log(log_file, {
            "type": "result", "subtype": "success", "is_error": True,
            "terminal_reason": "api_error", "result": RATE_LIMIT_TEXT,
        }, events=[rate_limit_event()])
        transient, not_before = detect_transient_exit(str(log_file), now=NOW)
        assert transient is True
        assert not_before == "2026-09-08T08:40:00Z"

    def test_reset_comes_from_last_rate_limit_event(self, tmp_path):
        """The last rate_limit_event wins over earlier ones."""
        log_file = tmp_path / "story-602.log"
        write_stream_json_log(log_file, {
            "type": "result", "is_error": True, "terminal_reason": "rate_limit",
            "result": "rate limited",
        }, events=[rate_limit_event(RESETS_AT - 3600, status="allowed"), rate_limit_event()])
        assert detect_transient_exit(str(log_file), now=NOW) == (True, "2026-09-08T08:40:00Z")

    def test_reset_already_past_relaunches_now(self, tmp_path):
        """A reset instant behind the clock (agent died just before it, or the
        orchestrator resumed the next morning) must not stall the story."""
        log_file = tmp_path / "story-603.log"
        write_stream_json_log(log_file, {
            "type": "result", "is_error": True, "terminal_reason": "rate_limit",
            "result": "rate limited",
        }, events=[rate_limit_event(int(NOW.timestamp()) - 25)])
        assert detect_transient_exit(str(log_file), now=NOW) == (True, "2026-09-08T08:00:00Z")

    def test_marker_without_terminal_reason_is_not_transient(self, tmp_path):
        """The text markers only disambiguate a bare api_error: a story whose own
        error text mentions a rate limit (e.g. GitHub's) is a real failure."""
        log_file = tmp_path / "story-604.log"
        write_stream_json_log(log_file, {
            "type": "result", "subtype": "success", "is_error": True,
            "result": "Rate limit exceeded, please retry later",
        })
        assert detect_transient_exit(str(log_file), now=NOW) == (False, None)

    def test_api_error_without_rate_limit_marker_is_not_transient(self, tmp_path):
        """api_error also covers auth/invalid-request failures — those must not retry."""
        log_file = tmp_path / "story-608.log"
        write_stream_json_log(log_file, {
            "type": "result", "subtype": "success", "is_error": True,
            "terminal_reason": "api_error",
            "result": "authentication_error: invalid x-api-key",
        })
        assert detect_transient_exit(str(log_file), now=NOW) == (False, None)

    def test_clean_result_record_is_not_transient(self, tmp_path):
        log_file = tmp_path / "story-605.log"
        write_stream_json_log(log_file, {
            "type": "result", "subtype": "success", "is_error": False,
            "result": "PR is ready for review",
        }, events=[rate_limit_event()])
        assert detect_transient_exit(str(log_file), now=NOW) == (False, None)

    def test_error_without_transient_signal_is_not_transient(self, tmp_path):
        """is_error alone is not enough — a genuine agent failure must still error."""
        log_file = tmp_path / "story-606.log"
        write_stream_json_log(log_file, {
            "type": "result", "subtype": "error_during_execution", "is_error": True,
            "terminal_reason": "error_during_execution",
            "result": "Tool execution failed: permission denied",
        })
        assert detect_transient_exit(str(log_file), now=NOW) == (False, None)

    def test_non_json_log_is_not_transient(self, tmp_path):
        log_file = tmp_path / "story-607.log"
        log_file.write_text("lots of output\nReady for review\nmore output\n")
        assert detect_transient_exit(str(log_file), now=NOW) == (False, None)

    def test_missing_log_is_not_transient(self, tmp_path):
        assert detect_transient_exit(str(tmp_path / "absent.log"), now=NOW) == (False, None)
        assert detect_transient_exit(None, now=NOW) == (False, None)

    def test_result_record_is_found_when_followed_by_trailing_output(self, tmp_path):
        """Only the final `type: result` record counts, even with trailing noise."""
        log_file = tmp_path / "story-609.log"
        write_stream_json_log(log_file, {
            "type": "result", "is_error": True, "terminal_reason": "rate_limit",
            "result": "rate limited",
        })
        with log_file.open("a") as f:
            f.write("docker exec exited\n")
        assert detect_transient_exit(str(log_file), now=NOW)[0] is True

    def test_missing_reset_uses_default_backoff(self, tmp_path, monkeypatch):
        """No rate_limit_event in the tail: wait ORCHESTRATE_TRANSIENT_BACKOFF, read at use."""
        monkeypatch.setenv("ORCHESTRATE_TRANSIENT_BACKOFF", "600")
        log_file = tmp_path / "story-610.log"
        write_stream_json_log(log_file, {
            "type": "result", "is_error": True, "terminal_reason": "api_error",
            "result": "You've hit your session limit",
        })
        assert detect_transient_exit(str(log_file), now=NOW) == (True, "2026-09-08T08:10:00Z")

    def test_default_backoff_without_env(self, tmp_path, monkeypatch):
        monkeypatch.delenv("ORCHESTRATE_TRANSIENT_BACKOFF", raising=False)
        log_file = tmp_path / "story-611.log"
        write_stream_json_log(log_file, {
            "type": "result", "is_error": True, "terminal_reason": "rate_limit",
            "result": "rate limited",
        })
        expected = (NOW + timedelta(seconds=DEFAULT_TRANSIENT_BACKOFF_SECONDS)).strftime(ISO_UTC_FORMAT)
        assert detect_transient_exit(str(log_file), now=NOW) == (True, expected)

    def test_allowed_warning_event_does_not_supply_the_hold(self, tmp_path, monkeypatch):
        """A routine window-status record reports the end of the current usage
        window, not a reset the agent is waiting on."""
        monkeypatch.setenv("ORCHESTRATE_TRANSIENT_BACKOFF", "120")
        log_file = tmp_path / "story-613.log"
        write_stream_json_log(log_file, {
            "type": "result", "subtype": "success", "is_error": True,
            "terminal_reason": "api_error", "result": "Claude usage limit reached",
        }, events=[rate_limit_event(int(NOW.timestamp()) + 12 * 3600,
                                    status="allowed_warning", rate_limit_type="seven_day")])
        assert detect_transient_exit(str(log_file), now=NOW) == (True, "2026-09-08T08:02:00Z")

    def test_allowed_event_does_not_supply_the_hold(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ORCHESTRATE_TRANSIENT_BACKOFF", "120")
        log_file = tmp_path / "story-614.log"
        write_stream_json_log(log_file, {
            "type": "result", "is_error": True, "terminal_reason": "rate_limit",
            "result": "rate limited",
        }, events=[rate_limit_event(status="allowed")])
        assert detect_transient_exit(str(log_file), now=NOW) == (True, "2026-09-08T08:02:00Z")

    def test_rejected_event_behind_an_allowed_event_still_supplies_the_hold(self, tmp_path):
        """The scan skips benign records rather than stopping at the first event."""
        log_file = tmp_path / "story-615.log"
        write_stream_json_log(log_file, {
            "type": "result", "is_error": True, "terminal_reason": "rate_limit",
            "result": "rate limited",
        }, events=[rate_limit_event(), rate_limit_event(RESETS_AT + 7200, status="allowed_warning")])
        assert detect_transient_exit(str(log_file), now=NOW) == (True, "2026-09-08T08:40:00Z")

    def test_rejected_seven_day_reset_is_honoured_in_full(self, tmp_path):
        """A weekly rejection really does reset days out — capping it at the
        five-hour bound would relaunch into the still-active limit."""
        log_file = tmp_path / "story-618.log"
        resets_at = int(NOW.timestamp()) + 3 * 24 * 3600
        write_stream_json_log(log_file, {
            "type": "result", "is_error": True, "terminal_reason": "rate_limit",
            "result": "rate limited",
        }, events=[rate_limit_event(resets_at, rate_limit_type="seven_day")])
        expected = (NOW + timedelta(days=3)).strftime(ISO_UTC_FORMAT)
        assert detect_transient_exit(str(log_file), now=NOW) == (True, expected)

    def test_seven_day_hold_is_capped_at_its_own_window(self, tmp_path):
        """Even the weekly bound is a bound: a reset past it is still a misread."""
        log_file = tmp_path / "story-619.log"
        resets_at = int(NOW.timestamp()) + 30 * 24 * 3600
        write_stream_json_log(log_file, {
            "type": "result", "is_error": True, "terminal_reason": "rate_limit",
            "result": "rate limited",
        }, events=[rate_limit_event(resets_at, rate_limit_type="seven_day")])
        expected = (NOW + timedelta(days=7)).strftime(ISO_UTC_FORMAT)
        assert detect_transient_exit(str(log_file), now=NOW) == (True, expected)

    def test_hold_is_capped_at_the_maximum(self, tmp_path, capsys):
        """A five-hour reset beyond that window must not park the epic."""
        log_file = tmp_path / "story-616.log"
        write_stream_json_log(log_file, {
            "type": "result", "is_error": True, "terminal_reason": "rate_limit",
            "result": "rate limited",
        }, events=[rate_limit_event(int(NOW.timestamp()) + 12 * 3600)])
        expected = (NOW + timedelta(seconds=MAX_TRANSIENT_HOLD_SECONDS)).strftime(ISO_UTC_FORMAT)
        assert detect_transient_exit(str(log_file), now=NOW) == (True, expected)
        assert "WARNING" in capsys.readouterr().err

    def test_event_without_status_does_not_supply_the_hold(self, tmp_path, monkeypatch):
        """An unlabelled reset is not trusted — fall back to the backoff."""
        monkeypatch.setenv("ORCHESTRATE_TRANSIENT_BACKOFF", "120")
        log_file = tmp_path / "story-617.log"
        write_stream_json_log(log_file, {
            "type": "result", "is_error": True, "terminal_reason": "rate_limit",
            "result": "rate limited",
        }, events=[{"type": "rate_limit_event", "rate_limit_info": {"resetsAt": RESETS_AT}}])
        assert detect_transient_exit(str(log_file), now=NOW) == (True, "2026-09-08T08:02:00Z")

    def test_tail_is_read_from_the_end_of_a_large_log(self, tmp_path):
        """The result record must be found without reading the whole log."""
        log_file = tmp_path / "story-612.log"
        filler = json.dumps({"type": "assistant", "message": {"content": [{"type": "text", "text": "x" * 2000}]}})
        with log_file.open("w") as f:
            for _ in range(5000):
                f.write(filler + "\n")
            f.write(json.dumps(rate_limit_event()) + "\n")
            f.write(json.dumps({"type": "result", "is_error": True, "terminal_reason": "rate_limit",
                                "result": "rate limited"}) + "\n")
        assert detect_transient_exit(str(log_file), now=NOW) == (True, "2026-09-08T08:40:00Z")


class TestReadLogTail:
    def test_returns_last_lines_only(self, tmp_path):
        log_file = tmp_path / "tail.log"
        log_file.write_text("".join(f"line {i}\n" for i in range(200)))
        assert read_log_tail(str(log_file), 3) == ["line 197\n", "line 198\n", "line 199\n"]

    def test_short_file_returns_everything(self, tmp_path):
        log_file = tmp_path / "tail.log"
        log_file.write_text("a\nb\n")
        assert read_log_tail(str(log_file), 50) == ["a\n", "b\n"]

    def test_missing_file_returns_empty(self, tmp_path):
        assert read_log_tail(str(tmp_path / "absent.log"), 50) == []


# ── reconcile_with_input tests ──────────────────────────



from orchestrate_epic import StateManager


def make_state_manager(tmp_path, stories, phase="complete"):
    """Write a state file with the given story records and return a loaded StateManager."""
    state_file = tmp_path / "implement-epic-99999.json"
    state_file.write_text(json.dumps({
        "epicRef": "99999",
        "concurrency": 2,
        "phase": phase,
        "baseDir": str(tmp_path / "epic-99999"),
        "stories": stories,
    }))
    sm = StateManager(str(state_file))
    sm.load()
    return sm


def story_record(ref, status, base_dir, outcome=None, pr_number=None,
                 completed_at=None, dependencies=None, launch_attempts=0,
                 not_before=None, transient_retries=0, branch=None,
                 holds_preserved_work=False):
    """Build a story record in the shape the orchestrator persists."""
    return {
        "ref": ref, "id": 1000 + ref, "subject": f"Story {ref}",
        "status": status,
        "clonePath": str(base_dir / f"story-{ref}"),
        "pid": None,
        "logFile": str(base_dir / f"story-{ref}.log"),
        "startedAt": None if completed_at is None else "2026-01-01T00:00:00Z",
        "completedAt": completed_at,
        "outcome": outcome,
        "prNumber": pr_number,
        "containerId": None,
        "launchAttempts": launch_attempts,
        "transientRetries": transient_retries,
        "holdsPreservedWork": holds_preserved_work,
        "notBefore": not_before,
        "dependencies": dependencies or [],
        "branch": branch,
    }


def input_story(ref, dependencies=None):
    s = {"ref": ref, "id": 1000 + ref, "subject": f"Story {ref}"}
    if dependencies is not None:
        s["dependencies"] = dependencies
    return s


class TestReconcileWithInput:
    """Resume must retry failures without re-launching PR-bearing stories."""

    def test_completed_run_with_pr_is_not_requeued(self, tmp_path):
        base_dir = tmp_path / "epic-99999"
        sm = make_state_manager(tmp_path, [
            story_record(101, "escalated", base_dir, outcome="escalated",
                         pr_number=759, completed_at="2026-07-08T00:00:00Z"),
        ])
        requeued, added = sm.reconcile_with_input([input_story(101)], base_dir)
        story = sm.get_story(101)
        assert story["status"] == "escalated"
        assert story["prNumber"] == 759
        assert requeued == []
        assert added == []

    def test_error_story_is_requeued_and_cleared(self, tmp_path):
        base_dir = tmp_path / "epic-99999"
        sm = make_state_manager(tmp_path, [
            story_record(102, "error", base_dir, outcome="launch_failed",
                         completed_at="2026-07-08T00:00:00Z", launch_attempts=3),
        ])
        requeued, added = sm.reconcile_with_input([input_story(102)], base_dir)
        story = sm.get_story(102)
        assert story["status"] == "queued"
        assert story["outcome"] is None
        assert story["completedAt"] is None
        assert story["pid"] is None
        assert story["launchAttempts"] == 0
        assert requeued == [102]

    def test_skipped_story_is_requeued(self, tmp_path):
        base_dir = tmp_path / "epic-99999"
        sm = make_state_manager(tmp_path, [
            story_record(101, "escalated", base_dir, outcome="escalated", pr_number=759),
            story_record(103, "skipped", base_dir, outcome="dependency_failed",
                         completed_at="2026-07-08T00:00:00Z", dependencies=[101]),
        ])
        requeued, added = sm.reconcile_with_input(
            [input_story(101, []), input_story(103, [101])], base_dir)
        story = sm.get_story(103)
        assert story["status"] == "queued"
        assert story["dependencies"] == [101]
        assert 103 in requeued

    def test_requeued_story_dependencies_refreshed_from_input(self, tmp_path):
        base_dir = tmp_path / "epic-99999"
        sm = make_state_manager(tmp_path, [
            story_record(101, "complete", base_dir, outcome="complete", pr_number=42),
            story_record(103, "error", base_dir, outcome="launch_failed",
                         dependencies=[101]),
        ])
        # Dependency diagram changed between runs: 103 no longer depends on 101.
        sm.reconcile_with_input(
            [input_story(101, []), input_story(103, [])], base_dir)
        assert sm.get_story(103)["dependencies"] == []

    def test_escalated_without_pr_is_requeued(self, tmp_path):
        base_dir = tmp_path / "epic-99999"
        sm = make_state_manager(tmp_path, [
            story_record(104, "escalated", base_dir, outcome="escalated",
                         completed_at="2026-07-08T00:00:00Z"),
        ])
        requeued, added = sm.reconcile_with_input([input_story(104)], base_dir)
        assert sm.get_story(104)["status"] == "queued"
        assert requeued == [104]

    @pytest.mark.parametrize("status, outcome", [
        ("skipped", "integration_error"),
        ("error", "launch_failed"),
        ("escalated", "rate_limit_exhausted"),
    ])
    def test_reconcile_keeps_preserved_work_marker_while_resetting_budget(
            self, tmp_path, status, outcome):
        """The retry budget is per run; the clone on disk is not.
        Requeuing must reset the former without erasing the record that the
        clone still holds a rate-limited run's unpushed commits."""
        base_dir = tmp_path / "epic-99999"
        sm = make_state_manager(tmp_path, [
            story_record(101, status, base_dir, outcome=outcome,
                         completed_at="2026-07-08T00:00:00Z",
                         transient_retries=1, holds_preserved_work=True),
        ])
        sm.reconcile_with_input([input_story(101)], base_dir)
        story = sm.get_story(101)
        assert story["status"] == "queued"
        assert story["transientRetries"] == 0
        assert story["holdsPreservedWork"] is True

    @pytest.mark.parametrize("status, outcome, transient_retries, expected", [
        ("queued", None, 1, True),
        ("launching", None, 1, True),
        ("skipped", "integration_error", 1, True),
        ("escalated", "rate_limit_exhausted", 3, True),
        ("error", "launch_failed", 1, True),
        ("skipped", "dependency_failed", 0, False),
        ("error", "launch_failed", 0, False),
        ("complete", "complete", 1, False),
    ])
    def test_load_backfills_preserved_work_marker_for_legacy_records(
            self, tmp_path, status, outcome, transient_retries, expected):
        """A state file written before `holdsPreservedWork` existed carries
        only the per-run signals; load must derive the marker from them so a
        legacy parked clone survives the resume's final cleanup."""
        base_dir = tmp_path / "epic-99999"
        record = story_record(101, status, base_dir, outcome=outcome,
                              transient_retries=transient_retries)
        del record["holdsPreservedWork"]
        sm = make_state_manager(tmp_path, [record])
        assert sm.get_story(101)["holdsPreservedWork"] is expected

    def test_load_leaves_existing_preserved_work_marker_alone(self, tmp_path):
        """A persisted marker is authoritative; load must not re-infer it."""
        base_dir = tmp_path / "epic-99999"
        sm = make_state_manager(tmp_path, [
            story_record(101, "error", base_dir, outcome="launch_failed",
                         transient_retries=0, holds_preserved_work=True),
            story_record(102, "queued", base_dir, transient_retries=1,
                         holds_preserved_work=False),
        ])
        assert sm.get_story(101)["holdsPreservedWork"] is True
        assert sm.get_story(102)["holdsPreservedWork"] is False

    def test_reconcile_keeps_backfilled_marker_for_legacy_skipped_story(self, tmp_path):
        """The two-run sequence, starting from a legacy state file."""
        base_dir = tmp_path / "epic-99999"
        record = story_record(101, "skipped", base_dir, outcome="integration_error",
                              completed_at="2026-07-08T00:00:00Z", transient_retries=1)
        del record["holdsPreservedWork"]
        sm = make_state_manager(tmp_path, [record])
        sm.reconcile_with_input([input_story(101)], base_dir)
        story = sm.get_story(101)
        assert story["status"] == "queued"
        assert story["transientRetries"] == 0
        assert story["holdsPreservedWork"] is True

    def test_new_story_in_input_is_added_as_queued(self, tmp_path):
        base_dir = tmp_path / "epic-99999"
        sm = make_state_manager(tmp_path, [
            story_record(101, "complete", base_dir, outcome="complete", pr_number=42),
        ])
        requeued, added = sm.reconcile_with_input(
            [input_story(101), input_story(105, [101])], base_dir)
        story = sm.get_story(105)
        assert story is not None
        assert story["status"] == "queued"
        assert story["clonePath"] == str(base_dir / "story-105")
        assert story["logFile"] == str(base_dir / "story-105.log")
        assert story["dependencies"] == [101]
        assert added == [105]

    def test_story_missing_from_input_is_left_untouched(self, tmp_path):
        base_dir = tmp_path / "epic-99999"
        original = story_record(106, "complete", base_dir, outcome="complete", pr_number=99)
        sm = make_state_manager(tmp_path, [dict(original)])
        sm.reconcile_with_input([], base_dir)
        assert sm.get_story(106) == original

    def test_reconcile_sets_phase_running(self, tmp_path):
        base_dir = tmp_path / "epic-99999"
        sm = make_state_manager(tmp_path, [
            story_record(101, "complete", base_dir, outcome="complete", pr_number=42),
        ], phase="complete")
        sm.reconcile_with_input([input_story(101)], base_dir)
        assert sm.state["phase"] == "running"
        # And it is persisted, not just in-memory
        assert json.loads(Path(sm.state_file).read_text())["phase"] == "running"

    def test_requeued_skipped_story_becomes_launchable_when_parent_complete(self, tmp_path):
        base_dir = tmp_path / "epic-99999"
        sm = make_state_manager(tmp_path, [
            story_record(101, "complete", base_dir, outcome="complete", pr_number=42),
            story_record(103, "skipped", base_dir, outcome="dependency_failed",
                         dependencies=[101]),
        ])
        sm.reconcile_with_input(
            [input_story(101, []), input_story(103, [101])], base_dir)
        assert sm.next_queued_ref() == 103

    def test_requeued_skipped_story_not_launchable_while_parent_escalated(self, tmp_path):
        base_dir = tmp_path / "epic-99999"
        sm = make_state_manager(tmp_path, [
            story_record(101, "escalated", base_dir, outcome="escalated", pr_number=759),
            story_record(103, "skipped", base_dir, outcome="dependency_failed",
                         dependencies=[101]),
        ])
        sm.reconcile_with_input(
            [input_story(101, []), input_story(103, [101])], base_dir)
        assert sm.get_story(103)["status"] == "queued"
        assert sm.next_queued_ref() is None


# ── notBefore gating of the queue ───────────────────────


class TestNotBeforeGating:
    def test_next_queued_ref_skips_future_not_before(self, tmp_path):
        base_dir = tmp_path / "epic-99999"
        sm = make_state_manager(tmp_path, [
            story_record(101, "queued", base_dir, not_before="2026-09-08T08:40:00Z"),
        ])
        assert sm.next_queued_ref(now="2026-09-08T08:10:00Z") is None

    def test_next_queued_ref_returns_story_once_not_before_passes(self, tmp_path):
        base_dir = tmp_path / "epic-99999"
        sm = make_state_manager(tmp_path, [
            story_record(101, "queued", base_dir, not_before="2026-09-08T08:40:00Z"),
        ])
        assert sm.next_queued_ref(now="2026-09-08T08:40:00Z") == 101

    def test_next_queued_ref_prefers_launchable_story_over_waiting_one(self, tmp_path):
        base_dir = tmp_path / "epic-99999"
        sm = make_state_manager(tmp_path, [
            story_record(101, "queued", base_dir, not_before="2026-09-08T08:40:00Z"),
            story_record(102, "queued", base_dir),
        ])
        assert sm.next_queued_ref(now="2026-09-08T08:10:00Z") == 102

    def test_new_story_record_has_transient_fields(self, tmp_path):
        base_dir = tmp_path / "epic-99999"
        sm = make_state_manager(tmp_path, [])
        sm.reconcile_with_input([input_story(105)], base_dir)
        story = sm.get_story(105)
        assert story["notBefore"] is None
        assert story["transientRetries"] == 0
        assert story["holdsPreservedWork"] is False

    def test_reconcile_clears_not_before(self, tmp_path):
        base_dir = tmp_path / "epic-99999"
        sm = make_state_manager(tmp_path, [
            story_record(102, "error", base_dir, outcome="launch_failed",
                         completed_at="2026-07-08T00:00:00Z",
                         not_before="2999-01-01T00:00:00Z", transient_retries=3),
        ])
        sm.reconcile_with_input([input_story(102)], base_dir)
        story = sm.get_story(102)
        assert story["status"] == "queued"
        assert story["notBefore"] is None
        assert story["transientRetries"] == 0


# ── launch timeout / teardown containment ───────────────


def _make_orchestrator(tmp_path, refs=(101,), base_branch="main", dry_run=False):
    """Orchestrator whose script_dir is tmp_path, with queued stories for refs,
    running its host-repo git against a sandbox repo whose `main` is current."""
    mock_bin = tmp_path / "mock-bin"
    mock_bin.mkdir(exist_ok=True)
    stories = [{"ref": r, "id": 1000 + r, "subject": f"Story {r}"} for r in refs]
    stories_file = tmp_path / "stories.json"
    stories_file.write_text(json.dumps(stories))
    orch = Orchestrator(
        "99999", 1, str(stories_file), str(mock_bin), str(tmp_path), base_branch,
        repo_root=make_host_repo(tmp_path / "host-repo", remote="upstream"),
        dry_run=dry_run,
    )
    orch.base_dir.mkdir(parents=True, exist_ok=True)
    orch.sm.init_from_stories("99999", 1, stories, orch.base_dir, base_branch)
    return orch, mock_bin


def _write_script(path, body):
    path.write_text("#!/usr/bin/env bash\n" + body)
    path.chmod(0o755)


def _resume_orchestrator(tmp_path, base_branch, refs=(101,), dry_run=False):
    """Orchestrator pointed at an already-initialized state file, ready for init_or_resume()."""
    mock_bin = tmp_path / "mock-bin"
    mock_bin.mkdir(exist_ok=True)
    stories = [{"ref": r, "id": 1000 + r, "subject": f"Story {r}"} for r in refs]
    stories_file = tmp_path / "stories.json"
    stories_file.write_text(json.dumps(stories))
    return Orchestrator(
        "99999", 1, str(stories_file), str(mock_bin), str(tmp_path), base_branch,
        repo_root=make_host_repo(tmp_path / "host-repo", remote="upstream"),
        dry_run=dry_run,
    )


class TestBaseBranchPersistence:
    """The run's root base branch is stored at init and wins over BASE_BRANCH on resume."""

    def test_init_stores_run_base_branch(self, tmp_path):
        orch, _ = _make_orchestrator(tmp_path, base_branch="develop")
        assert orch.sm.state["baseBranch"] == "develop"
        on_disk = json.loads(Path(orch.sm.state_file).read_text())
        assert on_disk["baseBranch"] == "develop"

    def test_stored_root_base_branch_beats_env_on_resume(self, tmp_path, capsys):
        _make_orchestrator(tmp_path, base_branch="develop")

        resumed = _resume_orchestrator(tmp_path, base_branch="main")
        resumed.init_or_resume()

        assert resumed.base_branch == "develop"
        err = capsys.readouterr().err
        assert "Ignoring BASE_BRANCH=main; using stored baseBranch=develop" in err

    def test_state_file_without_base_branch_falls_back_to_env(self, tmp_path, capsys):
        orch, _ = _make_orchestrator(tmp_path, base_branch="main")
        state = json.loads(Path(orch.sm.state_file).read_text())
        del state["baseBranch"]
        Path(orch.sm.state_file).write_text(json.dumps(state))

        resumed = _resume_orchestrator(tmp_path, base_branch="main")
        resumed.init_or_resume()

        assert resumed.base_branch == "main"
        assert "Ignoring BASE_BRANCH" not in capsys.readouterr().err

    def test_resume_backfills_missing_root_base_branch(self, tmp_path, capsys):
        orch, _ = _make_orchestrator(tmp_path, base_branch="main")
        state = json.loads(Path(orch.sm.state_file).read_text())
        del state["baseBranch"]
        Path(orch.sm.state_file).write_text(json.dumps(state))

        first = _resume_orchestrator(tmp_path, base_branch="epic/root")
        first.init_or_resume()
        assert first.base_branch == "epic/root"
        assert json.loads(Path(first.sm.state_file).read_text())["baseBranch"] == "epic/root"

        capsys.readouterr()
        second = _resume_orchestrator(tmp_path, base_branch="main")
        second.init_or_resume()
        assert second.base_branch == "epic/root"
        assert "Ignoring BASE_BRANCH=main; using stored baseBranch=epic/root" in capsys.readouterr().err


class TestRunScriptKillsProcessGroupOnTimeout:
    """_run_script must kill the script's whole process group on timeout so
    grandchildren (e.g. devcontainer up) don't outlive the launch attempt."""

    def test_grandchild_killed_on_timeout(self, tmp_path):
        import subprocess as sp
        import time as time_mod

        orch, mock_bin = _make_orchestrator(tmp_path)
        pid_file = tmp_path / "grandchild.pid"
        _write_script(
            mock_bin / "slow.sh",
            "sleep 300 &\n"
            f"echo $! > {pid_file}\n"
            "wait\n",
        )

        started = time_mod.monotonic()
        with pytest.raises(sp.TimeoutExpired):
            orch._run_script("slow.sh", timeout=2)
        elapsed = time_mod.monotonic() - started
        assert elapsed < 10, f"timeout was not enforced promptly ({elapsed:.1f}s)"

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
        if not process_gone():
            os.kill(grandchild_pid, 9)
        assert process_gone(), "grandchild survived _run_script timeout"


class TestLaunchTimeoutConfiguration:
    """Launch timeout defaults to 1800s and is overridable via ORCHESTRATE_LAUNCH_TIMEOUT."""

    def test_launch_timeout_default_is_1800(self, tmp_path, monkeypatch):
        monkeypatch.delenv("ORCHESTRATE_LAUNCH_TIMEOUT", raising=False)
        orch, _ = _make_orchestrator(tmp_path)
        assert orch.launch_timeout == 1800

    def test_launch_timeout_env_override(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ORCHESTRATE_LAUNCH_TIMEOUT", "2400")
        orch, _ = _make_orchestrator(tmp_path)
        assert orch.launch_timeout == 2400

    def test_launch_story_passes_timeout_and_reports_timeout_reason(self, tmp_path, monkeypatch):
        import subprocess as sp

        orch, _ = _make_orchestrator(tmp_path)
        seen = {}

        def mock_run_script(script_name, *args, timeout=60, ref=None):
            seen["timeout"] = timeout
            raise sp.TimeoutExpired(cmd=script_name, timeout=timeout)

        monkeypatch.setattr(orch, "_run_script", mock_run_script)

        failures = []

        def mock_failure(ref, clone_path, reason, container_id=None, keep_clone=False):
            failures.append(reason)
            return False

        monkeypatch.setattr(orch, "_handle_launch_failure", mock_failure)

        orch.launch_story(101)

        assert seen["timeout"] == orch.launch_timeout
        assert failures == [f"devcontainer launch timed out after {orch.launch_timeout}s"]


class TestTeardownReportsFailure:
    """_teardown must never swallow a failure silently: it logs, falls back to
    docker rm -f, and returns False so callers can react."""

    def test_teardown_returns_true_on_success(self, tmp_path):
        orch, mock_bin = _make_orchestrator(tmp_path)
        _write_script(mock_bin / "teardown.sh", "echo 'Teardown complete.'\n")
        assert orch._teardown(str(tmp_path / "clone")) is True

    def test_teardown_returns_false_and_logs_on_script_failure(self, tmp_path, capsys, monkeypatch):
        orch, mock_bin = _make_orchestrator(tmp_path)
        _write_script(mock_bin / "teardown.sh", "echo 'container survived' >&2\nexit 1\n")
        monkeypatch.setattr(orch, "_force_remove_containers", lambda path, cid=None: False)

        clone = str(tmp_path / "clone")
        assert orch._teardown(clone) is False
        err = capsys.readouterr().err
        assert "WARNING: teardown.sh failed" in err
        assert clone in err
        assert "container survived" in err

    def test_teardown_falls_back_to_docker_rm_on_failure(self, tmp_path, monkeypatch):
        orch, mock_bin = _make_orchestrator(tmp_path)
        _write_script(mock_bin / "teardown.sh", "exit 1\n")

        argv_log = tmp_path / "docker-argv.log"
        _write_script(
            mock_bin / "docker",
            f'echo "$*" >> "{argv_log}"\n'
            # `docker ps` lists nothing, so the post-removal re-check passes
            'exit 0\n',
        )
        monkeypatch.setenv("PATH", f"{mock_bin}{os.pathsep}{os.environ['PATH']}")

        assert orch._teardown(str(tmp_path / "clone"), "abc123") is True
        calls = argv_log.read_text().splitlines()
        assert "rm -f abc123" in calls, f"docker rm -f not attempted: {calls}"

    def test_force_remove_returns_false_when_docker_query_fails(self, tmp_path, monkeypatch, capsys):
        """A failing `docker ps` must not be mistaken for "no containers"."""
        orch, mock_bin = _make_orchestrator(tmp_path)
        _write_script(
            mock_bin / "docker",
            'echo "permission denied while trying to connect to the Docker daemon" >&2\n'
            'exit 1\n',
        )
        monkeypatch.setenv("PATH", f"{mock_bin}{os.pathsep}{os.environ['PATH']}")

        assert orch._force_remove_containers(str(tmp_path / "clone"), "abc123") is False
        err = capsys.readouterr().err
        assert "WARNING" in err and "permission denied" in err


class TestEnsureCloneAbsentRefusesWorktrees:
    """_ensure_clone_absent is the only removal path in this file that does
    not go through teardown.sh — it must not defeat teardown.sh's own
    worktree refusal by deleting a worktree directly."""

    def test_ensure_clone_absent_refuses_a_worktree(self, tmp_path, capsys):
        orch, _ = _make_orchestrator(tmp_path)
        wt = tmp_path / "wt"
        wt.mkdir()
        (wt / ".git").write_text("gitdir: /some/main/repo/.git/worktrees/wt\n")

        orch._ensure_clone_absent(str(wt))

        assert wt.exists(), "a worktree must not be removed"
        stale = [p for p in os.listdir(tmp_path)
                 if p.startswith("wt.stale-")]
        assert not stale, "a refused worktree must not be renamed aside either"
        assert "WARNING" in capsys.readouterr().err

    def test_ensure_clone_absent_removes_a_plain_clone(self, tmp_path):
        orch, _ = _make_orchestrator(tmp_path)
        clone = tmp_path / "clone"
        os.makedirs(clone / ".git")

        orch._ensure_clone_absent(str(clone))

        assert not clone.exists(), "a plain clone must still be removed as before"


class TestLaunchFailureStartsFromCleanClone:
    """A launch retry must re-clone: reusing the failed attempt's clone binds a
    second container to the same devcontainer.local_folder."""

    def _populated_clone(self, orch):
        clone = orch.sm.get_story(101)["clonePath"]
        os.makedirs(os.path.join(clone, ".git"))
        with open(os.path.join(clone, "dirty.txt"), "w") as f:
            f.write("left over from attempt 1")
        return clone

    def test_launch_failure_removes_clone_before_requeue(self, tmp_path):
        orch, mock_bin = _make_orchestrator(tmp_path)
        _write_script(mock_bin / "teardown.sh", "exit 0\n")  # leaves the clone in place
        clone = self._populated_clone(orch)

        orch._handle_launch_failure(101, clone, "devcontainer launch failed")

        assert orch.sm.get_story(101)["status"] == "queued"
        assert not os.path.exists(clone), "retry would reuse the dirty clone"

    def test_launch_failure_moves_clone_aside_when_removal_fails(self, tmp_path, monkeypatch, capsys):
        orch, mock_bin = _make_orchestrator(tmp_path)
        _write_script(mock_bin / "teardown.sh", "exit 0\n")
        clone = self._populated_clone(orch)

        def refuse(path, *args, **kwargs):
            raise OSError("Device or resource busy")

        monkeypatch.setattr(shutil, "rmtree", refuse)

        orch._handle_launch_failure(101, clone, "devcontainer launch failed")

        assert not os.path.exists(clone)
        stale = [p for p in os.listdir(os.path.dirname(clone))
                 if p.startswith(os.path.basename(clone) + ".stale-")]
        assert stale, f"expected a .stale-* sibling next to {clone}"
        assert "moved aside" in capsys.readouterr().err

    def test_launch_failure_after_failed_cleanup_is_terminal_and_keeps_clone(
            self, tmp_path, monkeypatch, capsys):
        """If a container may still be bound to the clone, retrying would bind a
        second container to the same folder — the original bug. Do not requeue."""
        orch, mock_bin = _make_orchestrator(tmp_path)
        _write_script(mock_bin / "teardown.sh", "exit 1\n")
        monkeypatch.setattr(orch, "_force_remove_containers", lambda path, cid=None: False)
        clone = self._populated_clone(orch)

        orch._handle_launch_failure(101, clone, "devcontainer launch failed", "abc123")

        story = orch.sm.get_story(101)
        assert story["status"] == "error"
        assert story["outcome"] == "launch_failed"
        assert story["containerId"] == "abc123"
        assert os.path.exists(clone), "clone must be kept for diagnosis when cleanup failed"
        out = capsys.readouterr().out
        assert "LAUNCH_FAIL:101:devcontainer launch failed; cleanup failed" in out

    def test_launch_failure_over_a_worktree_clone_is_terminal_not_requeued(
            self, tmp_path, capsys):
        """_ensure_clone_absent refuses to remove a git worktree. If
        that refusal were treated as a successful cleanup, the story would be
        requeued, launch.sh would find the directory still present and skip
        checkout, and the agent would run against whatever branch the
        worktree happened to hold instead of the resolved base."""
        orch, mock_bin = _make_orchestrator(tmp_path)
        _write_script(mock_bin / "teardown.sh", "exit 0\n")
        clone = orch.sm.get_story(101)["clonePath"]
        os.makedirs(clone)
        with open(os.path.join(clone, ".git"), "w") as f:
            f.write("gitdir: /some/main/repo/.git/worktrees/story-101\n")

        orch._handle_launch_failure(101, clone, "devcontainer launch failed")

        story = orch.sm.get_story(101)
        assert story["status"] == "error", "a refused worktree removal must not be retried"
        assert story["outcome"] == "launch_failed"
        assert os.path.exists(clone), "the worktree must be left in place, not removed"
        out = capsys.readouterr().out
        assert "LAUNCH_FAIL:101:devcontainer launch failed; clone is a git worktree" in out


# ── transient exit handling in check_completions ────────


class TestFailedStoryKeepsClone:
    """An agent that exits in error or escalates leaves unpushed work in its
    clone; the next run re-queues the story and resumes over that directory."""

    @pytest.mark.parametrize("outcome", ["error", "escalated"])
    def test_finish_story_keeps_clone_and_marker(self, tmp_path, outcome):
        orch, mock_bin = _make_orchestrator(tmp_path)
        teardown_log = tmp_path / "teardown-calls.log"
        _write_script(mock_bin / "teardown.sh", f'echo "$*" >> "{teardown_log}"\n')
        story = orch.sm.get_story(101)

        orch._finish_story(story, outcome, "none")

        after = orch.sm.get_story(101)
        assert after["holdsPreservedWork"] is True
        assert teardown_log.read_text().strip() == f"{story['clonePath']} --keep-clone"

    def test_complete_story_still_removes_clone(self, tmp_path):
        orch, mock_bin = _make_orchestrator(tmp_path)
        teardown_log = tmp_path / "teardown-calls.log"
        _write_script(mock_bin / "teardown.sh", f'echo "$*" >> "{teardown_log}"\n')
        story = orch.sm.get_story(101)

        orch._finish_story(story, "complete", "55")

        assert orch.sm.get_story(101)["holdsPreservedWork"] is False
        assert "--keep-clone" not in teardown_log.read_text()

    def test_requeued_errored_story_resumes_over_preserved_clone(self, tmp_path):
        orch, mock_bin = _make_orchestrator(tmp_path)
        _write_script(mock_bin / "teardown.sh", "exit 0\n")
        orch._finish_story(orch.sm.get_story(101), "error", "none")

        stories = json.loads(Path(orch.stories_file).read_text())
        requeued, _ = orch.sm.reconcile_with_input(stories, orch.base_dir)

        assert requeued == [101]
        story = orch.sm.get_story(101)
        assert story["status"] == "queued"
        assert story["holdsPreservedWork"] is True, "the retry must keep the marker"
        assert orch._holds_preserved_work(story)


class TestCheckCompletionsTransientExit:
    """A rate-limited agent exit requeues the story and keeps its clone/container."""

    def _active_story_with_log(self, tmp_path, result_record, transient_retries=0):
        orch, mock_bin = _make_orchestrator(tmp_path, refs=(101, 102))
        teardown_log = tmp_path / "teardown-calls.log"
        _write_script(mock_bin / "teardown.sh", f'echo "$*" >> "{teardown_log}"\n')
        log_file = tmp_path / "story-101.log"
        log_file.write_text(json.dumps(result_record) + "\n")
        # pid=None with no Popen handle → check_completions treats the agent as exited
        orch.sm.update_story(
            101, status="active", pid=None, containerId="abc123",
            logFile=str(log_file), transientRetries=transient_retries,
        )
        orch.sm.update_story(102, dependencies=[101])
        return orch, teardown_log

    RATE_LIMITED = {
        "type": "result", "subtype": "success", "is_error": True,
        "terminal_reason": "api_error", "result": "You've hit your session limit",
    }

    def test_rate_limited_exit_requeues_and_keeps_clone(self, tmp_path, capsys):
        orch, teardown_log = self._active_story_with_log(tmp_path, self.RATE_LIMITED)

        orch.check_completions()

        story = orch.sm.get_story(101)
        assert story["status"] == "queued"
        assert story["pid"] is None
        assert story["transientRetries"] == 1
        assert story["holdsPreservedWork"] is True
        assert story["notBefore"] is not None
        assert story["containerId"] == "abc123", "container must be kept for the relaunch"
        assert not teardown_log.exists(), "clone must not be torn down on a transient exit"
        assert orch.sm.get_story(102)["status"] == "queued", "dependents must not be cascade-skipped"
        out = capsys.readouterr().out
        assert f"RATE_LIMITED:101:{story['notBefore']}" in out
        assert "COMPLETED:101" not in out
        assert "SKIPPED:" not in out

    def test_rate_limit_retry_budget_exhausted_escalates_and_keeps_clone(self, tmp_path, capsys):
        """Giving up must not destroy the unpushed work: the container goes, the clone stays,
        and a human is asked to recover it."""
        orch, teardown_log = self._active_story_with_log(
            tmp_path, self.RATE_LIMITED, transient_retries=MAX_TRANSIENT_RETRIES)

        orch.check_completions()

        story = orch.sm.get_story(101)
        assert story["status"] == "escalated"
        assert story["outcome"] == "rate_limit_exhausted"
        assert story["holdsPreservedWork"] is True
        assert teardown_log.read_text().strip() == f"{story['clonePath']} --keep-clone"
        assert orch.sm.get_story(102)["status"] == "skipped"
        out = capsys.readouterr().out
        assert "RATE_LIMITED:" not in out
        assert "COMPLETED:101:escalated:none" in out
        assert "SKIPPED:102:dependency_failed:101" in out

    def test_normal_completion_clears_preserved_work_marker(self, tmp_path):
        """Once the resumed run completes, the clone goes and the marker with it."""
        completed = {"type": "result", "subtype": "success", "is_error": False}
        orch, teardown_log = self._active_story_with_log(tmp_path, completed, transient_retries=1)
        orch.sm.update_story(101, holdsPreservedWork=True)
        (tmp_path / "fully-implement-101.json").write_text(
            json.dumps({"phase": "complete", "prNumber": 42}))

        orch.check_completions()

        story = orch.sm.get_story(101)
        assert story["status"] == "complete"
        assert story["holdsPreservedWork"] is False
        assert teardown_log.read_text().strip() == story["clonePath"]

    def test_exhausted_retries_with_existing_pr_escalates_and_keeps_pr(self, tmp_path, capsys):
        """A PR created before the limit hit must be recorded so a resume never re-launches into a duplicate."""
        orch, teardown_log = self._active_story_with_log(
            tmp_path, self.RATE_LIMITED, transient_retries=MAX_TRANSIENT_RETRIES)
        # In agent-override mode the orchestrator reads the host-side story state
        # from its OWN state dir, never the shared global /tmp: two runs on one
        # host would otherwise read and delete each other's completion files.
        host_state = tmp_path / "fully-implement-101.json"
        host_state.write_text(json.dumps({"phase": "monitoring", "prNumber": 77}))

        orch.check_completions()

        story = orch.sm.get_story(101)
        assert story["status"] == "escalated"
        assert story["outcome"] == "rate_limit_exhausted"
        assert story["prNumber"] == 77
        assert teardown_log.read_text().strip() == f"{story['clonePath']} --keep-clone"
        assert orch.sm.get_story(102)["status"] == "skipped"
        out = capsys.readouterr().out
        assert "COMPLETED:101:escalated:77" in out
        assert "SKIPPED:102:dependency_failed:101" in out

    def test_exhausted_retries_with_existing_branch_escalates_and_keeps_branch(self, tmp_path):
        """A branch recorded before the limit hit must survive into the terminal
        state: a dependent resolving its base, or a human recovering the work,
        both need it."""
        orch, _ = self._active_story_with_log(
            tmp_path, self.RATE_LIMITED, transient_retries=MAX_TRANSIENT_RETRIES)
        (orch.state_dir / "fully-implement-101.json").write_text(json.dumps({
            "phase": "monitoring", "prNumber": 77, "featureBranch": "feature/foo-101",
        }))

        orch.check_completions()

        story = orch.sm.get_story(101)
        assert story["status"] == "escalated"
        assert story["branch"] == "feature/foo-101"

    def test_resume_enforces_retry_budget(self, tmp_path, capsys):
        """On restart, a story past its retry budget is terminal, not requeued."""
        orch, teardown_log = self._active_story_with_log(
            tmp_path, self.RATE_LIMITED, transient_retries=MAX_TRANSIENT_RETRIES)

        orch._resume()

        story = orch.sm.get_story(101)
        assert story["status"] == "escalated"
        assert story["outcome"] == "rate_limit_exhausted"
        assert teardown_log.read_text().strip() == f"{story['clonePath']} --keep-clone"
        assert orch.sm.get_story(102)["status"] == "skipped"
        out = capsys.readouterr().out
        assert "COMPLETED:101:escalated:none" in out
        assert "SKIPPED:102:dependency_failed:101" in out
        assert "RESUMED:99999:0:0:0:1:0" in out

    def test_genuine_error_exit_still_errors(self, tmp_path, capsys):
        orch, teardown_log = self._active_story_with_log(tmp_path, {
            "type": "result", "subtype": "error_during_execution", "is_error": True,
            "result": "Tool execution failed",
        })

        orch.check_completions()

        assert orch.sm.get_story(101)["status"] == "error"
        assert teardown_log.exists()
        assert "COMPLETED:101:error:none" in capsys.readouterr().out

    def test_resume_requeues_rate_limited_story_and_emits_event(self, tmp_path, capsys):
        """A rate-limited agent found dead on orchestrator restart is requeued the same way."""
        orch, teardown_log = self._active_story_with_log(tmp_path, self.RATE_LIMITED)

        orch._resume()

        story = orch.sm.get_story(101)
        assert story["status"] == "queued"
        assert story["transientRetries"] == 1
        assert story["notBefore"] is not None
        assert not teardown_log.exists()
        assert orch.sm.get_story(102)["status"] == "queued"
        out = capsys.readouterr().out
        assert f"RATE_LIMITED:101:{story['notBefore']}" in out
        assert "RESUMED:99999:0:2:0:0:0" in out


# ── relaunch of a preserved clone ───────────────────────


class TestLaunchStoryResume:
    """A clone directory that exists at launch holds preserved work: the
    recorded container is reused when it is still running, prepare-clone.sh
    (whose clean-tree step discards uncommitted edits) is skipped, and the
    agent is told it is resuming."""

    def _orchestrator(self, tmp_path, monkeypatch, inspect_running, base_branch="main"):
        orch, mock_bin = _make_orchestrator(tmp_path, base_branch=base_branch)
        calls = tmp_path / "calls.log"
        _write_script(mock_bin / "launch.sh",
                      f'echo "launch $1" >> "{calls}"\n'
                      'mkdir -p "$1/.claude/skills/fully-implement-story"\n'
                      'echo "CONTAINER_ID=fresh-container"\n')
        _write_script(mock_bin / "prepare-clone.sh",
                      f'echo "prepare $1" >> "{calls}"\n'
                      'mkdir -p "$1/.claude/skills/fully-implement-story"\n'
                      "echo 'Implement story $ARGUMENTS' > \"$1/.claude/skills/fully-implement-story/SKILL.md\"\n")
        _write_script(mock_bin / "teardown.sh", f'echo "teardown $*" >> "{calls}"\n')
        # exec-skill.sh is the agent launcher; the fake records its argv one
        # argument per line.
        prompt_file = tmp_path / "exec-skill-args.txt"
        _write_script(mock_bin / "exec-skill.sh", f'printf \'%s\\n\' "$@" > "{prompt_file}"\n')
        # `docker inspect` answers for the recorded container id only.
        _write_script(mock_bin / "docker",
                      'if [[ "$1" == "inspect" && "${@: -1}" == "abc123" ]]; then\n'
                      f'    echo "{"true" if inspect_running else "false"}"; exit 0\n'
                      'fi\n'
                      'exit 1\n')
        monkeypatch.setenv("PATH", f"{mock_bin}{os.pathsep}{os.environ['PATH']}")
        return orch, calls, prompt_file

    def _preserved_clone(self, orch):
        clone = orch.sm.get_story(101)["clonePath"]
        skill_dir = os.path.join(clone, ".claude/skills/fully-implement-story")
        os.makedirs(skill_dir)
        with open(os.path.join(skill_dir, "SKILL.md"), "w") as f:
            f.write("Implement story $ARGUMENTS")
        orch.sm.update_story(101, containerId="abc123", transientRetries=1)
        return clone

    @staticmethod
    def _argv(orch, prompt_file):
        orch.child_procs[101].wait(timeout=10)
        return prompt_file.read_text().splitlines()

    @classmethod
    def _prompt(cls, orch, prompt_file):
        """The skill arguments exec-skill.sh was given (its third argument)."""
        return cls._argv(orch, prompt_file)[2]

    def test_fresh_launch_runs_launch_and_prepare_with_plain_ref(self, tmp_path, monkeypatch):
        orch, calls, prompt_file = self._orchestrator(tmp_path, monkeypatch, inspect_running=True)

        assert orch.launch_story(101) is True

        clone = orch.sm.get_story(101)["clonePath"]
        assert calls.read_text().splitlines() == [f"launch {clone}", f"prepare {clone}"]
        assert self._prompt(orch, prompt_file) == "101 main"
        assert orch.sm.get_story(101)["containerId"] == "fresh-container"

    def test_story_agent_runs_through_exec_skill(self, tmp_path, monkeypatch):
        orch, _, args_file = self._orchestrator(tmp_path, monkeypatch, inspect_running=True)

        assert orch.launch_story(101) is True

        story = orch.sm.get_story(101)
        assert self._argv(orch, args_file) == [
            story["clonePath"], "fully-implement-story", "101 main",
            "--log", story["logFile"],
        ]

    def test_relaunch_after_a_rate_limit_passes_resume_to_exec_skill(self, tmp_path, monkeypatch):
        orch, _, args_file = self._orchestrator(tmp_path, monkeypatch, inspect_running=True)
        self._preserved_clone(orch)

        assert orch.launch_story(101) is True

        assert self._argv(orch, args_file)[1:3] == ["fully-implement-story", "101 main --resume"]

    def test_failed_sweep_of_a_stopped_container_aborts_the_relaunch(
            self, tmp_path, monkeypatch, capsys):
        """The recorded container is stopped, so launch.sh would start a second
        one. If the sweep that removes the stopped one fails, nothing may be
        launched on the same folder (a leftover container would sit next to the
        new one)."""
        orch, calls, args_file = self._orchestrator(tmp_path, monkeypatch, inspect_running=False)
        self._preserved_clone(orch)
        _write_script(orch.script_dir / "teardown.sh",
                      f'echo "teardown $*" >> "{calls}"\nexit 1\n')

        assert orch.launch_story(101) is False

        assert "launch " not in calls.read_text()
        assert not args_file.exists()
        story = orch.sm.get_story(101)
        assert story["status"] != "active"
        assert os.path.isdir(story["clonePath"])

    def test_missing_skill_file_fails_the_launch_without_starting_the_agent(
            self, tmp_path, monkeypatch, capsys):
        orch, _, args_file = self._orchestrator(tmp_path, monkeypatch, inspect_running=True)
        _write_script(orch.script_dir / "prepare-clone.sh", "exit 0\n")
        _write_script(orch.script_dir / "launch.sh", 'echo "CONTAINER_ID=fresh-container"\n')

        assert orch.launch_story(101) is False

        assert not args_file.exists()
        assert "LAUNCH_FAIL:101:skill file not found" in capsys.readouterr().out

    def test_relaunch_reuses_running_container(self, tmp_path, monkeypatch):
        orch, calls, prompt_file = self._orchestrator(tmp_path, monkeypatch, inspect_running=True)
        self._preserved_clone(orch)

        assert orch.launch_story(101) is True

        assert not calls.exists(), f"launch.sh/prepare-clone.sh must not run: {calls.read_text() if calls.exists() else ''}"
        assert self._prompt(orch, prompt_file) == "101 main --resume"
        story = orch.sm.get_story(101)
        assert story["status"] == "active"
        assert story["containerId"] == "abc123"

    def test_relaunch_with_dead_container_launches_over_the_preserved_clone(self, tmp_path, monkeypatch):
        """The container is gone (host restart): sweep whatever is bound to the
        folder, keep the clone, launch a new container onto it, still resume."""
        orch, calls, prompt_file = self._orchestrator(tmp_path, monkeypatch, inspect_running=False)
        clone = self._preserved_clone(orch)

        assert orch.launch_story(101) is True

        assert calls.read_text().splitlines() == [
            f"teardown {clone} --keep-clone", f"launch {clone}",
        ], "prepare-clone.sh must not run on a preserved clone"
        assert os.path.isdir(clone)
        assert self._prompt(orch, prompt_file) == "101 main --resume"
        assert orch.sm.get_story(101)["containerId"] == "fresh-container"

    def test_expanded_prompt_uses_run_base_for_dependency_free_story(self, tmp_path, monkeypatch):
        """The base in the expanded prompt is the run's root base branch, not a
        hardcoded "main" — a story with no dependencies on a `develop` run is
        told to cut its feature branch from `develop`."""
        orch, _, prompt_file = self._orchestrator(
            tmp_path, monkeypatch, inspect_running=True, base_branch="develop")
        monkeypatch.setattr(orch, "_ensure_base_reachable", lambda base: True)

        assert orch.launch_story(101) is True

        assert self._prompt(orch, prompt_file) == "101 develop"

    def test_relaunch_failure_keeps_the_preserved_clone(self, tmp_path, monkeypatch, capsys):
        """A failed relaunch must not rm -rf the clone with its unpushed commits."""
        orch, calls, prompt_file = self._orchestrator(tmp_path, monkeypatch, inspect_running=False)
        clone = self._preserved_clone(orch)
        _write_script(orch.script_dir / "launch.sh", "exit 1\n")

        assert orch.launch_story(101) is False

        story = orch.sm.get_story(101)
        assert story["status"] == "queued"
        assert story["launchAttempts"] == 1
        assert os.path.isdir(clone), "preserved clone was removed by the launch-failure path"
        assert "--keep-clone" in calls.read_text().splitlines()[-1]
        assert "LAUNCH_FAIL:101:" in capsys.readouterr().out


# ── per-story base branch resolution ───────


class TestBaseBranchResolution:
    """launch_story resolves each story's base branch from the dependency
    graph before invoking launch.sh: a story with no dependencies still
    clones from the run's root base branch, one with a completed dependency
    stacks on that dependency's branch instead."""

    @staticmethod
    def _short_circuit(orch, monkeypatch, seen):
        """Stop launch_story right after it resolves `base`, before any real
        subprocess work, by making _run_script fail immediately — the same
        trick test_launch_story_passes_timeout_and_reports_timeout_reason
        uses to inspect the args launch_story passes downstream."""
        import subprocess as sp

        def mock_run_script(script_name, *args, timeout=60, ref=None):
            seen["call"] = (script_name, args)
            raise sp.TimeoutExpired(cmd=script_name, timeout=timeout)

        monkeypatch.setattr(orch, "_run_script", mock_run_script)
        monkeypatch.setattr(orch, "_ensure_base_reachable", lambda base: True)
        monkeypatch.setattr(
            orch, "_handle_launch_failure",
            lambda ref, clone_path, reason, container_id=None, keep_clone=False: False,
        )

    def test_story_without_dependencies_uses_run_base_branch(self, tmp_path, monkeypatch):
        orch, _ = _make_orchestrator(tmp_path, refs=(101,), base_branch="main")
        seen = {}
        self._short_circuit(orch, monkeypatch, seen)

        orch.launch_story(101)

        assert orch.sm.get_story(101)["baseBranch"] == "main"
        assert seen["call"] == ("launch.sh", (orch.sm.get_story(101)["clonePath"], "main"))

    def test_handed_off_dependency_is_a_valid_base(self, tmp_path, monkeypatch):
        orch, _ = _make_orchestrator(tmp_path, refs=(101, 102), base_branch="main")
        orch.sm.update_story(
            101, status="complete", outcome="handed_off", branch="feature/foo-101",
        )
        orch.sm.update_story(102, dependencies=[101])
        seen = {}
        self._short_circuit(orch, monkeypatch, seen)

        orch.launch_story(102)

        assert orch.sm.get_story(102)["baseBranch"] == "feature/foo-101"

    def test_story_with_one_dependency_uses_that_storys_branch(self, tmp_path, monkeypatch):
        orch, _ = _make_orchestrator(tmp_path, refs=(101, 102), base_branch="main")
        orch.sm.update_story(101, status="complete", branch="feature/foo-101")
        orch.sm.update_story(102, dependencies=[101])
        seen = {}
        self._short_circuit(orch, monkeypatch, seen)

        orch.launch_story(102)

        assert orch.sm.get_story(102)["baseBranch"] == "feature/foo-101"
        assert seen["call"] == ("launch.sh", (orch.sm.get_story(102)["clonePath"], "feature/foo-101"))

    def test_multi_parent_story_uses_its_integration_branch(self, tmp_path, monkeypatch):
        """A story with two dependencies bases on its integration branch and
        never consults base_selector, which would raise here: neither parent
        has a recorded branch (the integration builder resolves them from
        their PRs instead)."""
        orch, _ = _make_orchestrator(tmp_path, refs=(101, 102, 103), base_branch="main")
        orch.sm.update_story(101, status="complete")
        orch.sm.update_story(102, status="complete")
        orch.sm.update_story(103, dependencies=[101, 102])
        monkeypatch.setattr(
            orch, "_ensure_integration_branch", lambda ref, deps: "epic/99999-integration-103",
        )
        seen = {}
        self._short_circuit(orch, monkeypatch, seen)

        orch.launch_story(103)

        story = orch.sm.get_story(103)
        assert story["status"] != "skipped"
        assert story["baseBranch"] == "epic/99999-integration-103"
        assert seen["call"] == ("launch.sh", (story["clonePath"], "epic/99999-integration-103"))

    def test_unresolvable_dependency_skips_the_story(self, tmp_path, monkeypatch, capsys):
        orch, _ = _make_orchestrator(tmp_path, refs=(101, 102), base_branch="main")
        # 101 is still queued with no branch: not a usable base for 102.
        orch.sm.update_story(102, dependencies=[101])

        result = orch.launch_story(102)

        assert result is False
        story = orch.sm.get_story(102)
        assert story["status"] == "skipped"
        assert story["outcome"] == "dependency_failed"
        out = capsys.readouterr().out
        assert "SKIPPED:102:dependency_failed:101" in out

    def test_unresolvable_dependency_preserves_a_parked_clone(self, tmp_path, capsys):
        """A story parked after a rate-limited exit (preserved clone, unpushed
        commits) that turns out to depend on an unresolvable branch must keep
        its clone: skipping it must not throw away that work."""
        orch, mock_bin = _make_orchestrator(tmp_path, refs=(101, 102), base_branch="main")
        teardown_log = tmp_path / "teardown-calls.log"
        _write_script(mock_bin / "teardown.sh", f'echo "$*" >> "{teardown_log}"\n')
        clone = orch.sm.get_story(102)["clonePath"]
        os.makedirs(clone)
        orch.sm.update_story(102, dependencies=[101], transientRetries=1)

        result = orch.launch_story(102)

        assert result is False
        assert orch.sm.get_story(102)["status"] == "skipped"
        assert os.path.isdir(clone), "preserved clone was removed by the dependency-skip path"
        assert teardown_log.read_text().strip() == f"{clone} --keep-clone"

    def test_unresolvable_dependency_removes_a_fresh_clone(self, tmp_path):
        """A story with no preserved work (no clone directory yet) has nothing
        to keep — teardown must not fabricate a --keep-clone for it."""
        orch, mock_bin = _make_orchestrator(tmp_path, refs=(101, 102), base_branch="main")
        teardown_log = tmp_path / "teardown-calls.log"
        _write_script(mock_bin / "teardown.sh", f'echo "$*" >> "{teardown_log}"\n')
        clone = orch.sm.get_story(102)["clonePath"]
        orch.sm.update_story(102, dependencies=[101])

        orch.launch_story(102)

        assert teardown_log.read_text().strip() == clone

    def test_unresolvable_dependency_cascade_skips_transitive_dependents(
            self, tmp_path, capsys):
        """A dependent of the unresolvable story must also be skipped, not left
        queued forever: `next_queued_ref` only ever selects a story whose deps
        are 'complete', and a story stuck at 'queued' behind a 'skipped'
        dependency can never satisfy that, which would hang the run."""
        orch, _ = _make_orchestrator(tmp_path, refs=(101, 102, 103), base_branch="main")
        # 101 is still queued with no branch: not a usable base for 102, and
        # 103 depends transitively on 102.
        orch.sm.update_story(102, dependencies=[101])
        orch.sm.update_story(103, dependencies=[102])

        result = orch.launch_story(102)

        assert result is False
        assert orch.sm.get_story(102)["status"] == "skipped"
        assert orch.sm.get_story(103)["status"] == "skipped"
        assert orch.sm.get_story(103)["outcome"] == "dependency_failed"
        out = capsys.readouterr().out
        assert "SKIPPED:102:dependency_failed:101" in out
        assert "SKIPPED:103:dependency_failed:102" in out

    def test_legacy_completed_story_without_branch_key_does_not_crash(self, tmp_path):
        """A state file written before this story shipped has completed story
        records with no "branch" key at all (not even null). Resuming a run
        against one must skip the dependent, not raise KeyError and abort
        the whole orchestrator."""
        orch, _ = _make_orchestrator(tmp_path, refs=(101, 102), base_branch="main")
        orch.sm.update_story(101, status="complete")
        state = json.loads(Path(orch.sm.state_file).read_text())
        for s in state["stories"]:
            if s["ref"] == 101:
                del s["branch"]
        Path(orch.sm.state_file).write_text(json.dumps(state))
        orch.sm.update_story(102, dependencies=[101])

        resumed = _resume_orchestrator(tmp_path, base_branch="main", refs=(101, 102))
        resumed.sm.load()

        result = resumed.launch_story(102)

        assert result is False
        assert resumed.sm.get_story(102)["status"] == "skipped"
        assert resumed.sm.get_story(102)["outcome"] == "dependency_failed"

    def test_requeued_story_reresolves_its_base(self, tmp_path):
        base_dir = tmp_path / "epic-99999"
        sm = make_state_manager(tmp_path, [
            story_record(102, "error", base_dir, outcome="launch_failed",
                         completed_at="2026-07-08T00:00:00Z", dependencies=[101]),
        ])
        sm.get_story(102)["baseBranch"] = "feature/foo-101"

        sm.reconcile_with_input([input_story(102, dependencies=[101])], base_dir)

        assert sm.get_story(102)["baseBranch"] is None

    def test_root_base_branch_survives_a_stacked_launch(self, tmp_path, monkeypatch):
        orch, _ = _make_orchestrator(tmp_path, refs=(101, 102), base_branch="main")
        orch.sm.update_story(101, status="complete", branch="feature/foo-101")
        orch.sm.update_story(102, dependencies=[101])
        seen = {}
        self._short_circuit(orch, monkeypatch, seen)

        orch.launch_story(102)

        assert orch.base_branch == "main"


class TestBaseReachability:
    """A dependency's branch is created and pushed to GitHub from inside that
    story's own container, so the host repo — where launch.sh's `git clone
    --branch` runs — has never seen it. launch_story must fetch it into a
    local head before invoking launch.sh."""

    @staticmethod
    def _stub_git(orch, monkeypatch, checked_out=("main",), results=None):
        """Stub _run_git, recording every call. `checked_out` is the set of
        branches `git worktree list` reports as checked out (the main worktree
        plus any linked ones); `results` maps a command's first arg to a
        CompletedProcess-alike for the commands the test wants to fail."""
        calls = []
        results = results or {}
        porcelain = "".join(
            f"worktree /repo/{i}\nHEAD 0123456789abcdef\nbranch refs/heads/{b}\n\n"
            for i, b in enumerate(checked_out)
        )

        def fake_run_git(*args, timeout=15):
            calls.append(args)
            if args[0] == "worktree":
                return SimpleNamespace(returncode=0, stdout=porcelain, stderr="")
            if args[0] in results:
                return results[args[0]]
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        monkeypatch.setattr(orch, "_run_git", fake_run_git)
        return calls

    def test_base_is_fetched_into_host_repo_when_absent(self, tmp_path, monkeypatch):
        orch, _ = _make_orchestrator(tmp_path, refs=(101,), base_branch="main")
        calls = self._stub_git(orch, monkeypatch)

        assert orch._ensure_base_reachable("feature/foo-101") is True
        assert ("fetch", "upstream", "+feature/foo-101:feature/foo-101") in calls

    def test_existing_local_head_is_refreshed_from_origin(self, tmp_path, monkeypatch):
        """a local head left behind by an earlier launch (or by
        `gh pr checkout` in the operator's host repo) must not be used
        verbatim — launch.sh clones from it, so it has to be re-fetched."""
        orch, _ = _make_orchestrator(tmp_path, refs=(101,), base_branch="main")
        calls = self._stub_git(orch, monkeypatch)

        assert orch._ensure_base_reachable("feature/foo-101") is True
        assert ("fetch", "upstream", "+feature/foo-101:feature/foo-101") in calls

    def test_force_pushed_base_resets_the_local_head(self, tmp_path, monkeypatch):
        """merge-train rebases PR branches, so upstream's tip may not be a
        descendant of the stale local head. The forced refspec makes the local
        head match upstream rather than failing the fast-forward."""
        orch, _ = _make_orchestrator(tmp_path, refs=(101,), base_branch="main")
        calls = self._stub_git(orch, monkeypatch)

        assert orch._ensure_base_reachable("feature/foo-101") is True
        fetches = [call for call in calls if call[0] == "fetch"]
        assert fetches == [("fetch", "upstream", "+feature/foo-101:feature/foo-101")]

    def test_checked_out_base_is_verified_against_origin_not_fetched_into(
        self, tmp_path, monkeypatch
    ):
        """git refuses to fetch into the checked-out branch, so when the base
        is the host repo's current branch it is verified instead: upstream's tip
        must already be contained in the local head."""
        orch, _ = _make_orchestrator(tmp_path, refs=(101,), base_branch="main")
        calls = self._stub_git(orch, monkeypatch, checked_out=("main",))

        assert orch._ensure_base_reachable("main") is True
        assert ("fetch", "upstream", "main") in calls
        assert not any(call[:3] == ("fetch", "upstream", "+main:main") for call in calls)
        assert ("rev-parse", "FETCH_HEAD") in calls
        assert ("rev-parse", "refs/heads/main") in calls

    def test_base_checked_out_in_a_linked_worktree_is_verified_not_fetched_into(
        self, tmp_path, monkeypatch
    ):
        """Worktree mode leaves base branches checked out in linked worktrees.
        git refuses to fetch into those too, so they take the verify path even
        though the main worktree is on a different branch."""
        orch, _ = _make_orchestrator(tmp_path, refs=(101,), base_branch="main")
        calls = self._stub_git(
            orch, monkeypatch, checked_out=("main", "feature/foo-101"),
        )

        assert orch._ensure_base_reachable("feature/foo-101") is True
        assert not any(
            call[:3] == ("fetch", "upstream", "+feature/foo-101:feature/foo-101")
            for call in calls
        )
        assert ("rev-parse", "FETCH_HEAD") in calls
        assert ("rev-parse", "refs/heads/feature/foo-101") in calls

    def test_stale_checked_out_base_fails_the_launch(self, tmp_path, monkeypatch, capsys):
        """when the base is checked out and behind upstream we cannot
        refresh it, so the launch fails loudly instead of cloning a stale base."""
        orch, _ = _make_orchestrator(tmp_path, refs=(101,), base_branch="main")
        self._stub_git(orch, monkeypatch, checked_out=("main",))
        stub = orch._run_git

        def differing_tips(*args, timeout=15):
            if args[0] == "rev-parse":
                tip = "a" * 40 if args[1] == "FETCH_HEAD" else "b" * 40
                return SimpleNamespace(returncode=0, stdout=tip + "\n", stderr="")
            return stub(*args, timeout=timeout)

        monkeypatch.setattr(orch, "_run_git", differing_tips)

        assert orch._ensure_base_reachable("main") is False
        assert "refusing to launch from a base that differs" in capsys.readouterr().err

    def test_unfetchable_base_fails_the_launch(self, tmp_path, monkeypatch, capsys):
        orch, mock_bin = _make_orchestrator(tmp_path, refs=(101, 102), base_branch="main")
        _write_script(mock_bin / "teardown.sh", "exit 0\n")
        orch.sm.update_story(101, status="complete", branch="feature/foo-101")
        orch.sm.update_story(102, dependencies=[101])

        monkeypatch.setattr(
            orch, "_run_git",
            lambda *args, timeout=15: SimpleNamespace(returncode=1, stdout="", stderr="fail"),
        )
        run_script_calls = []
        monkeypatch.setattr(
            orch, "_run_script", lambda *a, **k: run_script_calls.append((a, k)),
        )

        result = orch.launch_story(102)

        assert result is False
        assert run_script_calls == [], "launch.sh must never run when the base is unreachable"
        story = orch.sm.get_story(102)
        assert story["status"] == "queued"
        out = capsys.readouterr().out
        assert "LAUNCH_FAIL:102:base branch feature/foo-101 not reachable" in out


class TestHostRepoRoot:
    """the host repo _run_git targets is configurable, so tests run
    the real base-reachability git calls against a sandbox rather than the
    developer's own clone (whose checked-out main may lag upstream/main)."""

    def test_default_repo_root_is_the_clone_holding_the_script(self, tmp_path):
        import orchestrate_epic

        stories_file = tmp_path / "stories.json"
        stories_file.write_text("[]")
        orch = Orchestrator("99999", 1, str(stories_file), str(tmp_path), str(tmp_path), "main")

        assert orch.repo_root == Path(orchestrate_epic.__file__).resolve().parents[2]

    def test_current_checked_out_base_in_repo_root_is_reachable(self, tmp_path):
        orch, _ = _make_orchestrator(tmp_path, refs=(101,), base_branch="main")

        assert orch._ensure_base_reachable("main") is True

    def test_checked_out_base_ahead_of_upstream_is_refused(self, tmp_path):
        """launch.sh clones the local head, so a host branch carrying commits
        upstream does not have would leak them into every generated branch."""
        from conftest import _git

        orch, _ = _make_orchestrator(tmp_path, refs=(101,), base_branch="main")
        _git("commit", "--quiet", "--allow-empty", "-m", "unpushed host work", cwd=orch.repo_root)

        assert orch._ensure_base_reachable("main") is False

    def test_stale_checked_out_base_in_repo_root_is_refused(self, tmp_path):
        orch, _ = _make_orchestrator(tmp_path, refs=(101,), base_branch="main")
        orch.repo_root = make_host_repo(tmp_path / "stale", stale_main=True, remote="upstream")

        assert orch._ensure_base_reachable("main") is False


# ── account-wide rate-limit pause and the keep-clone contract ───


class TestRateLimitPause:
    """A provider rate limit is account-wide: while any story is parked, no
    other story is launched into the same window, and once it passes the
    parked stories (whose containers are still up) resume before fresh ones."""

    def _orchestrator(self, tmp_path, refs, concurrency=2):
        orch, mock_bin = _make_orchestrator(tmp_path, refs=refs)
        orch.concurrency = concurrency
        launched = []
        orch.launch_story = lambda ref: launched.append(ref) or orch.sm.update_story(ref, status="active")
        return orch, launched

    def test_rate_limited_until_is_the_latest_future_not_before(self, tmp_path):
        base_dir = tmp_path / "epic-99999"
        sm = make_state_manager(tmp_path, [
            story_record(101, "queued", base_dir, not_before="2026-09-08T08:40:00Z", transient_retries=1),
            story_record(102, "queued", base_dir, not_before="2026-09-08T09:00:00Z", transient_retries=1),
            story_record(103, "queued", base_dir),
        ])
        assert sm.rate_limited_until(now="2026-09-08T08:10:00Z") == "2026-09-08T09:00:00Z"
        assert sm.rate_limited_until(now="2026-09-08T09:00:00Z") is None

    def test_fill_slots_launches_nothing_while_a_story_is_parked(self, tmp_path):
        orch, launched = self._orchestrator(tmp_path, refs=(101, 102))
        orch.sm.update_story(101, notBefore="2026-09-08T08:40:00Z", transientRetries=1)

        orch.fill_slots(now="2026-09-08T08:10:00Z")

        assert launched == [], "a launchable story was started into the rate-limit window"

    def test_fill_slots_resumes_parked_stories_before_fresh_ones(self, tmp_path):
        """Parked stories already hold a container; launching a fresh story first
        would add another one on top of the idle ones."""
        orch, launched = self._orchestrator(tmp_path, refs=(100, 101, 102), concurrency=2)
        orch.sm.update_story(101, notBefore="2026-09-08T08:40:00Z", transientRetries=1)

        orch.fill_slots(now="2026-09-08T08:40:00Z")

        assert launched == [101, 100]

    def test_parked_story_stays_queued_and_keeps_the_epic_alive(self, tmp_path):
        orch, launched = self._orchestrator(tmp_path, refs=(101,), concurrency=1)
        orch.sm.update_story(101, notBefore="2026-09-08T08:40:00Z", transientRetries=1)

        orch.fill_slots(now="2026-09-08T08:10:00Z")

        assert not orch.is_done()


class TestKeepCloneContract:
    """Every teardown path that can run against a story holding preserved work
    (parked after a rate limit, relaunching, or escalated with its budget spent)
    removes the container but leaves the clone."""

    def _story_with_clone(self, tmp_path, **fields):
        orch, mock_bin = _make_orchestrator(tmp_path)
        teardown_log = tmp_path / "teardown-calls.log"
        _write_script(mock_bin / "teardown.sh", f'echo "$*" >> "{teardown_log}"\n')
        clone = orch.sm.get_story(101)["clonePath"]
        os.makedirs(clone)
        orch.sm.update_story(101, containerId=None, **fields)
        return orch, teardown_log, clone

    def test_cleanup_active_keeps_preserved_clone_and_marker(self, tmp_path, monkeypatch):
        """SIGTERM while a resumed story is active: the container goes, the
        clone and its marker stay, so the next run can still recover the work."""
        monkeypatch.setattr("orchestrate_epic.time.sleep", lambda s: None)
        orch, teardown_log, clone = self._story_with_clone(
            tmp_path, status="active", transientRetries=1, holdsPreservedWork=True)
        orch._cleanup_active()
        assert teardown_log.read_text().strip() == f"{clone} --keep-clone"
        story = orch.sm.get_story(101)
        assert story["status"] == "queued"
        assert story["holdsPreservedWork"] is True

    def test_cleanup_active_removes_clone_without_preserved_work(self, tmp_path, monkeypatch):
        monkeypatch.setattr("orchestrate_epic.time.sleep", lambda s: None)
        orch, teardown_log, clone = self._story_with_clone(tmp_path, status="active")
        orch._cleanup_active()
        assert teardown_log.read_text().strip() == clone
        assert orch.sm.get_story(101)["holdsPreservedWork"] is False

    def test_resume_of_a_dead_agent_keeps_preserved_clone_and_marker(self, tmp_path, monkeypatch):
        """An orchestrator restart finds the resumed agent dead with no usable
        outcome: requeueing it must not delete the preserved clone either."""
        monkeypatch.setattr("orchestrate_epic.is_agent_process", lambda pid: False)
        orch, teardown_log, clone = self._story_with_clone(
            tmp_path, status="active", transientRetries=1, holdsPreservedWork=True)
        orch._resume()
        assert teardown_log.read_text().strip() == f"{clone} --keep-clone"
        story = orch.sm.get_story(101)
        assert story["status"] == "queued"
        assert story["holdsPreservedWork"] is True

    def test_final_cleanup_keeps_parked_clone(self, tmp_path):
        orch, teardown_log, clone = self._story_with_clone(
            tmp_path, status="queued", transientRetries=1, holdsPreservedWork=True,
            notBefore="2999-01-01T00:00:00Z")
        orch._final_cleanup()
        assert teardown_log.read_text().strip() == f"{clone} --keep-clone"

    def test_final_cleanup_keeps_exhausted_clone(self, tmp_path):
        """run() sweeps every story at the end — it must not undo the keep."""
        orch, teardown_log, clone = self._story_with_clone(
            tmp_path, status="escalated", outcome="rate_limit_exhausted", transientRetries=3,
            holdsPreservedWork=True)
        orch._final_cleanup()
        assert teardown_log.read_text().strip() == f"{clone} --keep-clone"

    def test_final_cleanup_removes_other_clones(self, tmp_path):
        orch, teardown_log, clone = self._story_with_clone(
            tmp_path, status="escalated", outcome="escalated", transientRetries=1)
        orch._final_cleanup()
        assert teardown_log.read_text().strip() == clone

    def test_cleanup_launching_keeps_relaunching_clone(self, tmp_path):
        orch, teardown_log, clone = self._story_with_clone(
            tmp_path, status="launching", transientRetries=1, holdsPreservedWork=True)
        orch._cleanup_launching()
        assert teardown_log.read_text().strip() == f"{clone} --keep-clone"
        assert orch.sm.get_story(101)["status"] == "queued"

    def test_resume_keeps_relaunching_clone(self, tmp_path, capsys):
        orch, teardown_log, clone = self._story_with_clone(
            tmp_path, status="launching", transientRetries=1, holdsPreservedWork=True)
        orch._resume()
        assert teardown_log.read_text().strip() == f"{clone} --keep-clone"
        assert orch.sm.get_story(101)["status"] == "queued"

    @pytest.mark.parametrize(
        "outcome", ["dependency_failed", "integration_error", "integration_conflict"])
    def test_final_cleanup_keeps_skipped_parked_clone(self, tmp_path, outcome):
        """A parked story skipped before it relaunched never ran again, so its
        clone still holds the rate-limited run's unpushed commits."""
        orch, teardown_log, clone = self._story_with_clone(
            tmp_path, status="skipped", outcome=outcome, transientRetries=1,
            holdsPreservedWork=True)
        orch._final_cleanup()
        assert teardown_log.read_text().strip() == f"{clone} --keep-clone"

    def test_final_cleanup_removes_skipped_clone_without_preserved_work(self, tmp_path):
        """A story skipped without ever being parked holds no preserved work."""
        orch, teardown_log, clone = self._story_with_clone(
            tmp_path, status="skipped", outcome="dependency_failed", transientRetries=0,
            holdsPreservedWork=False)
        orch._final_cleanup()
        assert teardown_log.read_text().strip() == clone

    def test_final_cleanup_keeps_clone_skipped_again_after_reconcile(self, tmp_path):
        """A second run requeues the skipped story with a fresh retry budget;
        if the same block skips it again, the clone must still survive the
        final sweep."""
        orch, teardown_log, clone = self._story_with_clone(
            tmp_path, status="skipped", outcome="integration_error", transientRetries=1,
            holdsPreservedWork=True)
        orch.sm.reconcile_with_input([input_story(101)], orch.base_dir)
        orch.sm.update_story(101, status="skipped", outcome="integration_error")
        orch._final_cleanup()
        assert teardown_log.read_text().strip() == f"{clone} --keep-clone"

    def test_final_cleanup_keeps_clone_after_terminal_resume_launch_failure(self, tmp_path):
        """A resumed launch that fails for good keeps its clone; the final sweep
        must honour that even though the story is now in `error`."""
        orch, teardown_log, clone = self._story_with_clone(
            tmp_path, status="error", outcome="launch_failed", transientRetries=0,
            holdsPreservedWork=True)
        orch._final_cleanup()
        assert teardown_log.read_text().strip() == f"{clone} --keep-clone"



def _worktree_add_call(calls, name, start_point):
    """True when the calls include a worktree-add creating `name` at
    `start_point` (the worktree path itself is a throwaway temp dir)."""
    return any(
        cmd[:6] == ["git", "worktree", "add", "--force", "-B", name] and cmd[7:] == [start_point]
        for cmd in calls
    )


def _gh_args(cmd):
    """The gh arguments of a `env -u GH_TOKEN gh ... --repo <repo>` argv, or []
    when `cmd` is not a gh call."""
    if cmd[:4] != ["env", "-u", "GH_TOKEN", "gh"]:
        return []
    return cmd[4:-2]


def _pr_create_base(calls):
    """The `--base` value of the single `gh pr create` among `calls`."""
    creates = [cmd for cmd in calls if _gh_args(cmd)[:2] == ["pr", "create"]]
    assert len(creates) == 1, f"expected one gh pr create, got {creates}"
    cmd = creates[0]
    return cmd[cmd.index("--base") + 1]


class TestEnsureIntegrationBranch:
    """A story with 2+ dependencies gets an integration branch merging every
    parent; a conflict or unresolvable parent blocks only that story."""

    def test_multi_parent_story_gets_integration_base(self, tmp_path, monkeypatch):
        orch, _ = _make_orchestrator(tmp_path, refs=(101, 102, 201))
        orch.sm.update_story(101, branch="feature/a-101")
        orch.sm.update_story(102, branch="feature/b-102")

        calls = []

        def fake_run_cmd(cmd, cwd=None):
            calls.append(cmd)
            return 0, "", ""

        monkeypatch.setattr(orch, "_run_cmd", fake_run_cmd)

        base = orch._ensure_integration_branch(201, [101, 102])

        assert base == "epic/99999-integration-201"
        assert _worktree_add_call(calls, "epic/99999-integration-201", "upstream/feature/a-101")
        assert ["git", "merge", "--no-ff", "--no-edit", "upstream/feature/b-102"] in calls
        assert [
            "git", "push", "-u", "upstream", "epic/99999-integration-201",
            "--force-with-lease=epic/99999-integration-201:",
        ] in calls
        assert orch.sm.get_story(201)["integrationBranch"] == "epic/99999-integration-201"
        assert _pr_create_base(calls) == "main"

    def test_integration_pr_targets_run_base_branch(self, tmp_path, monkeypatch):
        """The integration PR targets the run's root base, not a hardcoded main."""
        orch, _ = _make_orchestrator(tmp_path, refs=(101, 102, 201), base_branch="develop")
        orch.sm.update_story(101, branch="feature/a-101")
        orch.sm.update_story(102, branch="feature/b-102")

        calls = []

        def fake_run_cmd(cmd, cwd=None):
            calls.append(cmd)
            return 0, "", ""

        monkeypatch.setattr(orch, "_run_cmd", fake_run_cmd)

        orch._ensure_integration_branch(201, [101, 102])

        assert _pr_create_base(calls) == "develop"

    def test_single_parent_story_gets_no_integration_branch(self, tmp_path, monkeypatch):
        orch, _ = _make_orchestrator(tmp_path, refs=(101, 201))
        calls = []
        monkeypatch.setattr(orch, "_run_cmd", lambda cmd, cwd=None: calls.append(cmd) or (0, "", ""))

        base = orch._ensure_integration_branch(201, [101])

        assert base is None
        assert calls == []

    def test_integration_merge_conflict_blocks_only_that_story(self, tmp_path, monkeypatch, capsys):
        orch, _ = _make_orchestrator(tmp_path, refs=(101, 102, 201, 301))
        orch.sm.update_story(101, branch="feature/a-101")
        orch.sm.update_story(102, branch="feature/b-102")

        calls = []

        def fake_run_cmd(cmd, cwd=None):
            calls.append(cmd)
            if cmd == ["git", "merge", "--no-ff", "--no-edit", "upstream/feature/b-102"]:
                return 1, "", "conflict"
            return 0, "", ""

        monkeypatch.setattr(orch, "_run_cmd", fake_run_cmd)

        base = orch._ensure_integration_branch(201, [101, 102])

        assert base is None
        story = orch.sm.get_story(201)
        assert story["status"] == "skipped"
        assert story["outcome"] == "integration_conflict"
        assert ["git", "merge", "--abort"] in calls
        assert "BLOCKED:201:integration_conflict:101,102" in capsys.readouterr().out
        # The unrelated queued story is unaffected.
        assert orch.sm.get_story(301)["status"] == "queued"

    def test_parent_branch_resolved_from_pr_when_record_has_none(self, tmp_path, monkeypatch):
        orch, _ = _make_orchestrator(tmp_path, refs=(101,))
        orch.sm.update_story(101, prNumber=555)

        def fake_run_cmd(cmd, cwd=None):
            assert cmd == ["env", "-u", "GH_TOKEN", "gh", "pr", "view", "555", "--json",
                           "headRefName", "-q", ".headRefName", "--repo", "MrsBohemian/Cholobots"]
            return 0, "feature/from-pr-101\n", ""

        monkeypatch.setattr(orch, "_run_cmd", fake_run_cmd)

        branch = orch._resolve_story_branch(101)

        assert branch == "feature/from-pr-101"
        assert orch.sm.get_story(101)["branch"] == "feature/from-pr-101"

    def test_integration_branch_reused_on_resume(self, tmp_path, monkeypatch):
        orch, _ = _make_orchestrator(tmp_path, refs=(101, 102, 201))
        orch.sm.update_story(201, integrationBranch="epic/99999-integration-201")

        def fake_run_cmd(cmd, cwd=None):
            if cmd[:2] == ["git", "ls-remote"]:
                return 0, "abc123\trefs/heads/epic/99999-integration-201\n", ""
            if _gh_args(cmd)[:1] == ["pr"]:
                return 0, "777\n", ""
            raise AssertionError(f"unexpected git/gh call: {cmd}")

        monkeypatch.setattr(orch, "_run_cmd", fake_run_cmd)

        base = orch._ensure_integration_branch(201, [101, 102])

        assert base == "epic/99999-integration-201"

    def test_integration_branch_recreated_when_pr_no_longer_open(self, tmp_path, monkeypatch):
        """A resume after the persisted integration PR was closed (or its
        branch deleted) must not reuse it blindly -- it should rebuild."""
        orch, _ = _make_orchestrator(tmp_path, refs=(101, 102, 201))
        orch.sm.update_story(101, branch="feature/a-101")
        orch.sm.update_story(102, branch="feature/b-102")
        orch.sm.update_story(201, integrationBranch="epic/99999-integration-201")

        calls = []

        def fake_run_cmd(cmd, cwd=None):
            calls.append(cmd)
            if cmd[:2] == ["git", "ls-remote"]:
                return 2, "", ""  # branch no longer exists remotely
            return 0, "", ""

        monkeypatch.setattr(orch, "_run_cmd", fake_run_cmd)

        base = orch._ensure_integration_branch(201, [101, 102])

        assert base == "epic/99999-integration-201"
        # Rebuilt from scratch: the branch-creation flow ran again.
        assert _worktree_add_call(calls, "epic/99999-integration-201", "upstream/feature/a-101")

    def test_push_lease_names_the_fetched_integration_ref(self, tmp_path, monkeypatch):
        """A single-branch clone's fetch refspec maps only the branch it was
        cloned from, so a bare `--force-with-lease` finds no tracking ref for
        the integration branch and git rejects the push as "stale info" even
        when nothing conflicts. Fetch the branch's own ref and lease against
        the value it reports."""
        orch, _ = _make_orchestrator(tmp_path, refs=(101, 102, 201))
        orch.sm.update_story(101, branch="feature/a-101")
        orch.sm.update_story(102, branch="feature/b-102")

        name = "epic/99999-integration-201"
        fetch_integration = ["git", "fetch", "upstream", f"+{name}:refs/remotes/upstream/{name}"]
        push = ["git", "push", "-u", "upstream", name, f"--force-with-lease={name}:abc123"]

        calls = []

        def fake_run_cmd(cmd, cwd=None):
            calls.append(cmd)
            if cmd[:2] == ["git", "rev-parse"]:
                return 0, "abc123\n", ""
            return 0, "", ""

        monkeypatch.setattr(orch, "_run_cmd", fake_run_cmd)

        base = orch._ensure_integration_branch(201, [101, 102])

        assert base == name
        assert push in calls
        assert calls.index(fetch_integration) < calls.index(push)

    def test_missing_integration_branch_on_origin_leases_against_absence(self, tmp_path, monkeypatch):
        """The first run for a story has no integration branch on upstream, so
        fetching its ref fails -- that is expected, and the push leases on the
        branch not already existing rather than blocking."""
        orch, _ = _make_orchestrator(tmp_path, refs=(101, 102, 201))
        orch.sm.update_story(101, branch="feature/a-101")
        orch.sm.update_story(102, branch="feature/b-102")

        name = "epic/99999-integration-201"

        calls = []

        def fake_run_cmd(cmd, cwd=None):
            calls.append(cmd)
            if cmd == ["git", "fetch", "upstream", f"+{name}:refs/remotes/upstream/{name}"]:
                return 128, "", "couldn't find remote ref"
            return 0, "", ""

        monkeypatch.setattr(orch, "_run_cmd", fake_run_cmd)

        base = orch._ensure_integration_branch(201, [101, 102])

        assert base == name
        assert orch.sm.get_story(201)["status"] != "skipped"
        assert ["git", "push", "-u", "upstream", name, f"--force-with-lease={name}:"] in calls

    def test_push_failure_reports_integration_error_not_conflict(self, tmp_path, monkeypatch, capsys):
        """Only a real merge conflict may be reported as one -- an operational
        failure gets its own outcome so the emitted event is truthful."""
        orch, _ = _make_orchestrator(tmp_path, refs=(101, 102, 201))
        orch.sm.update_story(101, branch="feature/a-101")
        orch.sm.update_story(102, branch="feature/b-102")

        def fake_run_cmd(cmd, cwd=None):
            if cmd[:2] == ["git", "push"]:
                return 1, "", "boom"
            return 0, "", ""

        monkeypatch.setattr(orch, "_run_cmd", fake_run_cmd)

        base = orch._ensure_integration_branch(201, [101, 102])

        assert base is None
        assert orch.sm.get_story(201)["outcome"] == "integration_error"
        assert "BLOCKED:201:integration_error:101,102" in capsys.readouterr().out

    def test_unresolvable_parent_reports_integration_error(self, tmp_path, monkeypatch, capsys):
        orch, _ = _make_orchestrator(tmp_path, refs=(101, 102, 201))
        monkeypatch.setattr(orch, "_run_cmd", lambda cmd, cwd=None: (0, "", ""))

        base = orch._ensure_integration_branch(201, [101, 102])

        assert base is None
        assert orch.sm.get_story(201)["outcome"] == "integration_error"
        assert "BLOCKED:201:integration_error:101,102" in capsys.readouterr().out

    def test_blocked_integration_on_resume_keeps_clone_through_final_cleanup(
            self, tmp_path, monkeypatch, capsys):
        """A parked story whose integration branch is blocked keeps its clone:
        the containers go now, and final cleanup must not delete the directory
        holding the rate-limited run's unpushed commits."""
        orch, mock_bin = _make_orchestrator(tmp_path, refs=(101, 102, 201))
        teardown_log = tmp_path / "teardown-calls.log"
        _write_script(mock_bin / "teardown.sh", f'echo "$*" >> "{teardown_log}"\n')
        orch.sm.update_story(201, dependencies=[101, 102], transientRetries=1,
                             holdsPreservedWork=True, containerId=None)
        clone = orch.sm.get_story(201)["clonePath"]
        os.makedirs(clone)
        # No parent resolves to a branch, so the integration branch is blocked.
        monkeypatch.setattr(orch, "_run_cmd", lambda cmd, cwd=None: (0, "", ""))

        assert orch.launch_story(201) is False

        story = orch.sm.get_story(201)
        assert story["status"] == "skipped"
        assert story["outcome"] == "integration_error"
        assert teardown_log.read_text().splitlines()[0] == f"{clone} --keep-clone"

        orch._final_cleanup()

        clone_lines = [ln for ln in teardown_log.read_text().splitlines() if ln.startswith(clone)]
        assert clone_lines
        assert all(ln == f"{clone} --keep-clone" for ln in clone_lines)
        assert os.path.isdir(clone)

    def test_dependency_failed_on_resume_keeps_clone_through_final_cleanup(
            self, tmp_path, monkeypatch, capsys):
        """Same contract on the single-dependency path: the teardown next to the
        status flip is not enough on its own -- final cleanup sweeps again."""
        orch, mock_bin = _make_orchestrator(tmp_path, refs=(101, 201))
        teardown_log = tmp_path / "teardown-calls.log"
        _write_script(mock_bin / "teardown.sh", f'echo "$*" >> "{teardown_log}"\n')
        orch.sm.update_story(201, dependencies=[101], transientRetries=1,
                             holdsPreservedWork=True, containerId=None)
        clone = orch.sm.get_story(201)["clonePath"]
        os.makedirs(clone)

        assert orch.launch_story(201) is False
        assert orch.sm.get_story(201)["outcome"] == "dependency_failed"

        orch._final_cleanup()

        clone_lines = [ln for ln in teardown_log.read_text().splitlines() if ln.startswith(clone)]
        assert clone_lines
        assert all(ln == f"{clone} --keep-clone" for ln in clone_lines)


def _git(cwd, *args):
    """Run a git command in cwd, asserting it succeeded."""
    result = subprocess.run(["git", *args], cwd=str(cwd), capture_output=True, text=True)
    assert result.returncode == 0, f"git {' '.join(args)} failed: {result.stderr}"
    return result.stdout


def _integration_sandbox(tmp_path, conflicting=False):
    """Build a real git upstream with main and two parent branches, plus a host
    clone to run the orchestrator against. The parents merge cleanly unless
    `conflicting` is set, in which case both edit the same line of base.txt.
    """
    upstream = tmp_path / "upstream.git"
    seed = tmp_path / "seed"
    host = tmp_path / "host"
    upstream.mkdir()
    _git(tmp_path, "init", "--bare", "--initial-branch=main", str(upstream))
    seed.mkdir()
    _git(seed, "init", "--initial-branch=main")
    _git(seed, "config", "user.email", "test@example.com")
    _git(seed, "config", "user.name", "Test")
    (seed / "base.txt").write_text("base\n")
    _git(seed, "add", "base.txt")
    _git(seed, "commit", "-m", "base")
    _git(seed, "remote", "add", "upstream", str(upstream))
    _git(seed, "push", "upstream", "main")

    _git(seed, "checkout", "-b", "feature/a-101")
    if conflicting:
        (seed / "base.txt").write_text("from parent a\n")
        _git(seed, "add", "base.txt")
    else:
        (seed / "a.txt").write_text("a\n")
        _git(seed, "add", "a.txt")
    _git(seed, "commit", "-m", "parent a")
    _git(seed, "push", "upstream", "feature/a-101")

    _git(seed, "checkout", "main")
    _git(seed, "checkout", "-b", "feature/b-102")
    (seed / "base.txt").write_text("from parent b\n")
    _git(seed, "add", "base.txt")
    _git(seed, "commit", "-m", "parent b")
    _git(seed, "push", "upstream", "feature/b-102")

    _git(tmp_path, "clone", "-o", "upstream", "--branch", "main", str(upstream), str(host))
    _git(host, "config", "user.email", "test@example.com")
    _git(host, "config", "user.name", "Test")
    return host


def _host_rooted_run_cmd(host):
    """A _run_cmd replacement that runs git for real (rooted at the host
    clone, honouring an explicit cwd) and stubs every gh call as successful."""
    def run_cmd(cmd, cwd=None):
        if _gh_args(cmd):
            return 0, "", ""
        result = subprocess.run(
            cmd, cwd=str(cwd or host), capture_output=True, text=True)
        return result.returncode, result.stdout, result.stderr
    return run_cmd


class TestIntegrationBranchLeavesHostWorktreeAlone:
    """The integration branch is built in a throwaway worktree, so the
    operator's checkout keeps its uncommitted work and its HEAD."""

    def _orchestrator(self, tmp_path, monkeypatch, conflicting=False):
        host = _integration_sandbox(tmp_path, conflicting=conflicting)
        state = tmp_path / "state"
        state.mkdir()
        orch, _ = _make_orchestrator(state, refs=(101, 102, 201))
        orch.sm.update_story(101, branch="feature/a-101")
        orch.sm.update_story(102, branch="feature/b-102")
        monkeypatch.setattr(orch, "_run_cmd", _host_rooted_run_cmd(host))
        return orch, host

    def test_uncommitted_host_change_does_not_block_the_story(self, tmp_path, monkeypatch):
        """An unrelated local edit to a file the parents also touch used to
        make git refuse the checkout/merge, which was misreported as a
        conflict between the parents."""
        orch, host = self._orchestrator(tmp_path, monkeypatch)
        (host / "base.txt").write_text("UNCOMMITTED LOCAL WORK\n")

        base = orch._ensure_integration_branch(201, [101, 102])

        assert base == "epic/99999-integration-201"
        assert orch.sm.get_story(201)["status"] != "skipped"
        # The operator's edit survives untouched.
        assert (host / "base.txt").read_text() == "UNCOMMITTED LOCAL WORK\n"
        # Both parents really are merged into the branch.
        merged = _git(host, "log", "--format=%s", "epic/99999-integration-201")
        assert "parent a" in merged and "parent b" in merged

    def test_host_head_is_not_moved_to_the_integration_branch(self, tmp_path, monkeypatch):
        orch, host = self._orchestrator(tmp_path, monkeypatch)

        orch._ensure_integration_branch(201, [101, 102])

        assert _git(host, "rev-parse", "--abbrev-ref", "HEAD").strip() == "main"
        assert _git(host, "worktree", "list").count("\n") == 1

    def test_conflict_leaves_no_half_built_branch_or_worktree(self, tmp_path, monkeypatch, capsys):
        orch, host = self._orchestrator(tmp_path, monkeypatch, conflicting=True)

        base = orch._ensure_integration_branch(201, [101, 102])

        assert base is None
        assert orch.sm.get_story(201)["outcome"] == "integration_conflict"
        assert "BLOCKED:201:integration_conflict:101,102" in capsys.readouterr().out
        assert _git(host, "rev-parse", "--abbrev-ref", "HEAD").strip() == "main"
        assert _git(host, "worktree", "list").count("\n") == 1
        branches = _git(host, "branch", "--list", "epic/99999-integration-201")
        assert branches.strip() == ""


# ── Asynchronous launch tests ───────────────────────────


def _join_launch_threads(orch, timeout=10):
    """Wait for every in-flight launch thread to finish."""
    for t in list(orch.launch_threads.values()):
        t.join(timeout=timeout)


class TestFillSlotsDoesNotBlockOnLaunch:
    """A launch takes ~10 minutes; fill_slots must not wait for it."""

    def test_returns_while_launches_are_in_flight(self, tmp_path, monkeypatch):
        import threading
        import time as time_mod

        orch, _ = _make_orchestrator(tmp_path, refs=(101, 102, 103))
        orch.concurrency = 3

        release = threading.Event()

        def blocking_launch(ref):
            release.wait(timeout=30)
            return True

        monkeypatch.setattr(orch, "launch_story", blocking_launch)

        start = time_mod.monotonic()
        orch.fill_slots()
        elapsed = time_mod.monotonic() - start

        try:
            assert elapsed < 2, f"fill_slots blocked for {elapsed:.1f}s"
            assert orch.sm.count_status("launching") == 3
            assert orch.sm.count_status("queued") == 0
        finally:
            release.set()
            _join_launch_threads(orch)


class TestLaunchingCountsAgainstConcurrency:
    """An in-flight launch occupies its slot, so concurrency is never exceeded."""

    def test_only_one_more_started(self, tmp_path, monkeypatch):
        import threading

        orch, _ = _make_orchestrator(tmp_path, refs=(101, 102, 103))
        orch.concurrency = 2
        orch.sm.update_story(101, status="launching")

        release = threading.Event()

        def blocking_launch(ref):
            release.wait(timeout=30)
            return True

        monkeypatch.setattr(orch, "launch_story", blocking_launch)

        orch.fill_slots()
        try:
            assert orch.sm.count_status("launching") == 2
            assert orch.sm.count_status("queued") == 1
            assert len(orch.launch_threads) == 1
        finally:
            release.set()
            _join_launch_threads(orch)


class TestRateLimitPauseStartsNoThreads:
    """A rate-limit hold short-circuits fill_slots before any launch starts."""

    def test_no_threads_started_while_paused(self, tmp_path, monkeypatch):
        orch, _ = _make_orchestrator(tmp_path, refs=(101, 102))
        # Pin the orchestrator's clock: the hold must be judged against a fixed
        # instant, not the wall clock (testing-strategy.md rule 3).
        now = datetime(2031, 1, 15, 16, 0, tzinfo=timezone.utc)
        monkeypatch.setattr("orchestrate_epic.utcnow_iso", lambda: now.strftime(ISO_UTC_FORMAT))
        future = (now + timedelta(hours=1)).strftime(ISO_UTC_FORMAT)
        orch.sm.update_story(101, notBefore=future)
        orch.sm.update_story(102, notBefore=future)

        def unexpected_launch(ref):
            raise AssertionError(f"launched story {ref} while rate-limited")

        monkeypatch.setattr(orch, "launch_story", unexpected_launch)

        orch.fill_slots()

        assert orch.launch_threads == {}
        assert orch.sm.count_status("launching") == 0
        assert orch.sm.count_status("queued") == 2


class TestCleanupLaunchingKillsInFlightLaunchProcess:
    """Shutdown must kill helper scripts still running inside a launch."""

    def test_launch_process_killed_and_story_requeued(self, tmp_path, monkeypatch):
        import time as time_mod

        orch, _ = _make_orchestrator(tmp_path, refs=(101,))
        orch.sm.update_story(101, status="launching")
        monkeypatch.setattr(
            orch, "_teardown",
            lambda clone_path, container_id=None, keep_clone=False: True,
        )

        proc = subprocess.Popen(["sleep", "60"], start_new_session=True)
        orch.launch_procs[101] = proc

        orch._cleanup_launching()

        deadline = time_mod.monotonic() + 5
        while proc.poll() is None and time_mod.monotonic() < deadline:
            time_mod.sleep(0.1)

        assert proc.poll() is not None, "in-flight launch process survived cleanup"
        assert orch.sm.get_story(101)["status"] == "queued"


class TestShutdownDoesNotConsumeLaunchAttempt:
    """A launch killed by shutdown is not the story's fault."""

    def test_no_launch_failure_recorded(self, tmp_path, monkeypatch):
        orch, _ = _make_orchestrator(tmp_path, refs=(101,))
        orch.sm.update_story(101, status="launching")
        monkeypatch.setattr(orch, "_ensure_base_reachable", lambda base: True)

        def killed_launch_script(script_name, *args, timeout=60, ref=None):
            orch.shutting_down = True
            return "", -9

        monkeypatch.setattr(orch, "_run_script", killed_launch_script)

        failures = []
        monkeypatch.setattr(
            orch, "_handle_launch_failure",
            lambda *a, **kw: failures.append(a) or False,
        )

        assert orch.launch_story(101) is False
        assert failures == []
        assert orch.sm.get_story(101)["launchAttempts"] == 0
        assert orch.sm.get_story(101)["status"] == "launching"


class TestFastFailingLaunchDoesNotBurnEveryAttempt:
    """One flaky story must not consume all its launch attempts in one cycle."""

    def test_ref_retried_on_a_later_cycle(self, tmp_path, monkeypatch):
        orch, _ = _make_orchestrator(tmp_path, refs=(101, 102))
        orch.concurrency = 2

        attempts = []

        def fast_failing_launch(ref):
            attempts.append(ref)
            # Requeue from the worker thread, as _handle_launch_failure does,
            # while fill_slots is still looping.
            story = orch.sm.get_story(ref)
            orch.sm.update_story(
                ref, status="queued", launchAttempts=story["launchAttempts"] + 1,
            )
            return False

        monkeypatch.setattr(orch, "launch_story", fast_failing_launch)

        orch.fill_slots()
        _join_launch_threads(orch)

        assert attempts.count(101) == 1
        assert orch.sm.get_story(101)["launchAttempts"] == 1


class TestWorkerDoesNotDeregisterARetryThread:
    """A slow worker must not clear the registry entry a retry just claimed."""

    def test_retry_thread_survives_old_workers_finally(self, tmp_path, monkeypatch):
        orch, _ = _make_orchestrator(tmp_path, refs=(101,))
        orch.sm.update_story(101, status="launching")
        monkeypatch.setattr(
            orch, "_teardown",
            lambda clone_path, container_id=None, keep_clone=False: True,
        )

        retry_thread = object()  # stand-in for the retry's Thread

        def failing_launch(ref):
            # The poll loop registers a retry's thread before this worker's
            # `finally` runs.
            orch.launch_threads[ref] = retry_thread
            return False

        monkeypatch.setattr(orch, "launch_story", failing_launch)

        orch._launch_worker(101)

        assert orch.launch_threads.get(101) is retry_thread


class TestLaunchContainerReturnsItsOwnFailureReason:
    """Concurrent launches must not report each other's failure reasons."""

    def test_reason_returned_not_shared(self, tmp_path, monkeypatch):
        orch, _ = _make_orchestrator(tmp_path, refs=(101,))

        monkeypatch.setattr(
            orch, "_run_script",
            lambda script_name, *args, timeout=60, ref=None: ("no id here\n", 0),
        )

        container_id, reason = orch._launch_container(101, "/tmp/clone-101", "main")

        assert container_id is None
        assert reason == "no container ID returned"
        assert not hasattr(orch, "_launch_failure_reason")


class TestShutdownDuringPrepareCloneDoesNotConsumeAttempt:
    """prepare-clone.sh killed by shutdown is not the story's fault."""

    def test_no_launch_failure_recorded(self, tmp_path, monkeypatch):
        orch, _ = _make_orchestrator(tmp_path, refs=(101,))
        orch.sm.update_story(101, status="launching")
        monkeypatch.setattr(orch, "_ensure_base_reachable", lambda base: True)

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

        assert orch.launch_story(101) is False
        assert failures == []
        assert orch.sm.get_story(101)["launchAttempts"] == 0


class TestShutdownDoesNotDeadlockOnProcLock:
    """The signal handler re-enters proc_lock on the main thread."""

    def test_cleanup_runs_while_lock_is_held(self, tmp_path, monkeypatch):
        orch, _ = _make_orchestrator(tmp_path, refs=(101,))
        orch.sm.update_story(101, status="launching")
        monkeypatch.setattr(
            orch, "_teardown",
            lambda clone_path, container_id=None, keep_clone=False: True,
        )

        # Holding proc_lock is exactly the state the main thread is in when a
        # signal arrives mid-registration; cleanup must still acquire it.
        with orch.proc_lock:
            # Checked before the call so a regression to a plain Lock fails
            # here instead of deadlocking the suite.
            assert orch.proc_lock.acquire(blocking=False), "proc_lock is not reentrant"
            orch.proc_lock.release()
            orch._cleanup_launching()

        assert orch.sm.get_story(101)["status"] == "queued"


class TestCleanupJoinsSkipUnstartedThreads:
    """A thread registered but never started cannot be joined."""

    def test_unstarted_thread_does_not_abort_cleanup(self, tmp_path, monkeypatch):
        import threading

        orch, _ = _make_orchestrator(tmp_path, refs=(101,))
        orch.sm.update_story(101, status="launching")
        monkeypatch.setattr(
            orch, "_teardown",
            lambda clone_path, container_id=None, keep_clone=False: True,
        )

        # Registered by fill_slots, signal arrived before t.start().
        orch.launch_threads[101] = threading.Thread(target=lambda: None)

        orch._cleanup_launching()

        assert orch.sm.get_story(101)["status"] == "queued"


class TestHostGitWorkIsSerialized:
    """Concurrent launches must not run host-repo git commands at once."""

    def test_run_cmd_calls_do_not_overlap(self, tmp_path, monkeypatch):
        import threading

        orch, _ = _make_orchestrator(tmp_path, refs=(101, 102))

        overlaps = []
        inside = []
        seen_lock = threading.Lock()

        def slow_git(cmd, cwd=None, capture_output=None, text=None, timeout=None):
            with seen_lock:
                inside.append(1)
                if len(inside) > 1:
                    overlaps.append(1)
            import time as time_mod
            time_mod.sleep(0.05)
            with seen_lock:
                inside.pop()
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        monkeypatch.setattr("orchestrate_epic.subprocess.run", slow_git)

        threads = [
            threading.Thread(target=lambda: orch._run_cmd(["git", "fetch"]))
            for _ in range(6)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert overlaps == []


class TestRunGitSharesTheHostLock:
    """_run_git touches the same host refs as _run_cmd."""

    def test_run_git_calls_do_not_overlap_with_run_cmd(self, tmp_path, monkeypatch):
        import threading
        import time as time_mod

        orch, _ = _make_orchestrator(tmp_path, refs=(101,))

        overlaps = []
        inside = []
        seen_lock = threading.Lock()

        def slow_git(cmd, cwd=None, capture_output=None, text=None, timeout=None):
            with seen_lock:
                inside.append(1)
                if len(inside) > 1:
                    overlaps.append(1)
            time_mod.sleep(0.05)
            with seen_lock:
                inside.pop()
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        monkeypatch.setattr("orchestrate_epic.subprocess.run", slow_git)

        threads = [
            threading.Thread(target=lambda: orch._run_git("fetch", "upstream")) for _ in range(3)
        ] + [
            threading.Thread(target=lambda: orch._run_cmd(["git", "fetch"])) for _ in range(3)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert overlaps == []


class TestRunCmdIsBounded:
    """A hung git/gh command must not hold the host lock forever."""

    def test_timeout_reported_as_failure(self, tmp_path, monkeypatch):
        orch, _ = _make_orchestrator(tmp_path, refs=(101,))

        def hanging(cmd, cwd=None, capture_output=None, text=None, timeout=None):
            raise subprocess.TimeoutExpired(cmd=cmd, timeout=timeout)

        monkeypatch.setattr("orchestrate_epic.subprocess.run", hanging)

        rc, stdout, stderr = orch._run_cmd(["git", "fetch"])

        assert rc != 0
        assert "timed out" in stderr
        # The lock is released, so other launches can proceed.
        assert orch.git_lock.acquire(blocking=False)
        orch.git_lock.release()


class TestShutdownDuringBaseFetchDoesNotConsumeAttempt:
    """A base fetch aborted by shutdown is not the story's fault."""

    def test_no_launch_failure_recorded(self, tmp_path, monkeypatch):
        orch, _ = _make_orchestrator(tmp_path, refs=(101,))
        orch.sm.update_story(101, status="launching")

        def failing_base_check(base):
            orch.shutting_down = True
            return False

        monkeypatch.setattr(orch, "_ensure_base_reachable", failing_base_check)

        failures = []
        monkeypatch.setattr(
            orch, "_handle_launch_failure",
            lambda *a, **kw: failures.append(a) or False,
        )

        assert orch.launch_story(101) is False
        assert failures == []
        assert orch.sm.get_story(101)["launchAttempts"] == 0


# ── upstream remote, gh_cmd, container mode and the agent launcher ──


class TestRemoteAndRepoConstants:
    """REMOTE and GH_REPO are read at import, like scripts/merge_train.py."""

    @staticmethod
    def _import_fresh(monkeypatch, **env):
        import importlib
        for key in ("ORCHESTRATE_REMOTE", "GH_REPO"):
            monkeypatch.delenv(key, raising=False)
        for key, value in env.items():
            monkeypatch.setenv(key, value)
        import orchestrate_epic
        return importlib.reload(orchestrate_epic)

    def test_remote_and_repo_default_to_upstream_and_cholobots(self, monkeypatch):
        module = self._import_fresh(monkeypatch)
        try:
            assert module.REMOTE == "upstream"
            assert module.GH_REPO == "MrsBohemian/Cholobots"
        finally:
            self._import_fresh(monkeypatch)

    def test_remote_and_repo_honour_env_overrides(self, monkeypatch):
        module = self._import_fresh(
            monkeypatch, ORCHESTRATE_REMOTE="fork", GH_REPO="someone/else")
        try:
            assert module.REMOTE == "fork"
            assert module.GH_REPO == "someone/else"
            assert module.gh_cmd("pr", "list")[-2:] == ["--repo", "someone/else"]
        finally:
            self._import_fresh(monkeypatch)


class TestGhCalls:
    def test_gh_cmd_drops_gh_token_and_names_the_repo(self):
        from orchestrate_epic import gh_cmd
        assert gh_cmd("pr", "view", "5") == [
            "env", "-u", "GH_TOKEN", "gh", "pr", "view", "5",
            "--repo", "MrsBohemian/Cholobots",
        ]

    def test_every_gh_call_drops_gh_token_and_names_the_repo(self, tmp_path, monkeypatch):
        """An integration-branch build is the path that shells out to gh most:
        resolve a parent from its PR, create the PR, and check for an open one."""
        host = _integration_sandbox(tmp_path)
        state = tmp_path / "state"
        state.mkdir()
        orch, _ = _make_orchestrator(state, refs=(101, 102, 201))
        orch.sm.update_story(101, branch="feature/a-101")
        orch.sm.update_story(102, prNumber=555)
        recorded = []
        real = _host_rooted_run_cmd(host)

        def recording_run_cmd(cmd, cwd=None):
            recorded.append(cmd)
            if cmd[:2] == ["env", "-u"] and _gh_args(cmd)[:2] == ["pr", "view"]:
                return 0, "feature/b-102\n", ""
            if _gh_args(cmd)[:2] == ["pr", "create"]:
                return 1, "", "already exists"
            if _gh_args(cmd)[:2] == ["pr", "list"]:
                return 0, "777\n", ""
            return real(cmd, cwd=cwd)

        monkeypatch.setattr(orch, "_run_cmd", recording_run_cmd)

        assert orch._ensure_integration_branch(201, [101, 102]) == "epic/99999-integration-201"

        gh_calls = [cmd for cmd in recorded if "gh" in cmd[:4]]
        assert {tuple(_gh_args(c)[:2]) for c in gh_calls} == {
            ("pr", "view"), ("pr", "create"), ("pr", "list")}
        assert not [cmd for cmd in recorded if cmd[0] == "gh"], "a bare gh argv leaked"
        for cmd in gh_calls:
            assert cmd[:4] == ["env", "-u", "GH_TOKEN", "gh"]
            assert cmd[-2:] == ["--repo", "MrsBohemian/Cholobots"]


class TestIntegrationBranchPush:
    def test_integration_branch_is_pushed_to_upstream_with_lease(self, tmp_path, monkeypatch):
        host = _integration_sandbox(tmp_path)
        state = tmp_path / "state"
        state.mkdir()
        orch, _ = _make_orchestrator(state, refs=(101, 102, 201))
        orch.sm.update_story(101, branch="feature/a-101")
        orch.sm.update_story(102, branch="feature/b-102")
        recorded = []
        real = _host_rooted_run_cmd(host)

        def recording_run_cmd(cmd, cwd=None):
            recorded.append(cmd)
            return real(cmd, cwd=cwd)

        monkeypatch.setattr(orch, "_run_cmd", recording_run_cmd)

        name = orch._ensure_integration_branch(201, [101, 102])

        assert name == "epic/99999-integration-201"
        pushes = [cmd for cmd in recorded if cmd[:2] == ["git", "push"]]
        # The branch does not exist on the remote yet, so the lease expects absence.
        assert pushes == [["git", "push", "-u", "upstream", name, f"--force-with-lease={name}:"]]
        assert _git(host, "ls-remote", "--heads", "upstream", name).strip() != ""
        # Never the mirror.
        assert not any("origin" in cmd for cmd in recorded if cmd[0] == "git")


class TestStateFileMode:
    def test_state_file_records_container_mode(self, tmp_path):
        orch, _ = _make_orchestrator(tmp_path)
        assert orch.sm.state["mode"] == "container"
        assert json.loads(Path(orch.sm.state_file).read_text())["mode"] == "container"

    def test_resume_accepts_a_state_file_with_no_mode(self, tmp_path):
        orch, _ = _make_orchestrator(tmp_path)
        state = json.loads(Path(orch.sm.state_file).read_text())
        del state["mode"]
        Path(orch.sm.state_file).write_text(json.dumps(state))

        resumed = _resume_orchestrator(tmp_path, base_branch="main")
        resumed.init_or_resume()

        assert resumed.sm.state["phase"] == "running"

    def test_resume_refuses_a_worktree_mode_state_file(self, tmp_path, capsys):
        orch, _ = _make_orchestrator(tmp_path)
        state = json.loads(Path(orch.sm.state_file).read_text())
        state["mode"] = "worktree"
        written = json.dumps(state)
        Path(orch.sm.state_file).write_text(written)

        resumed = _resume_orchestrator(tmp_path, base_branch="main")
        with pytest.raises(SystemExit) as excinfo:
            resumed.init_or_resume()

        assert excinfo.value.code == 2
        err = capsys.readouterr().err
        assert "written by worktree mode" in err
        assert "--worktrees" in err
        assert Path(orch.sm.state_file).read_text() == written


class TestIsAgentProcess:
    """A relaunched orchestrator finds a still-running agent by its PID, which
    is the exec-skill.sh launcher."""

    # Each child prints a line once it has exec'd, and the test reads it before
    # checking: right after Popen, /proc/<pid>/cmdline can still be the forked
    # pytest's argv, which made the positive case fail under load.

    def test_exec_skill_process_is_an_agent(self):
        from orchestrate_epic import is_agent_process
        proc = subprocess.Popen(
            ["bash", "-c", "echo ready; sleep 30; true", "/some/dir/exec-skill.sh", "/clone", "skill"],
            stdout=subprocess.PIPE, text=True)
        try:
            assert proc.stdout.readline() == "ready\n"
            assert is_agent_process(proc.pid) is True
        finally:
            proc.kill()
            proc.wait()
            proc.stdout.close()

    def test_unrelated_process_is_not_an_agent(self):
        from orchestrate_epic import is_agent_process
        proc = subprocess.Popen(["bash", "-c", "echo ready; exec sleep 30"],
                                stdout=subprocess.PIPE, text=True)
        try:
            assert proc.stdout.readline() == "ready\n"
            assert is_agent_process(proc.pid) is False
        finally:
            proc.kill()
            proc.wait()
            proc.stdout.close()

    def test_non_linux_trusts_the_process_command_not_just_the_pid(self, tmp_path, monkeypatch):
        """Without /proc a live PID is not enough: a recycled PID belonging to an
        unrelated process must not keep a dead agent's story active forever."""
        import orchestrate_epic
        monkeypatch.setattr(orchestrate_epic, "PROC_ROOT", str(tmp_path / "no-proc"))
        monkeypatch.setattr(orchestrate_epic.os, "kill", lambda pid, sig: None)

        def ps(command):
            return subprocess.CompletedProcess([], 0, stdout=command + "\n", stderr="")

        monkeypatch.setattr(orchestrate_epic.subprocess, "run",
                            lambda *a, **kw: ps("/usr/sbin/cron -f"))
        assert orchestrate_epic.is_agent_process(4242) is False

        monkeypatch.setattr(orchestrate_epic.subprocess, "run",
                            lambda *a, **kw: ps("bash /repo/scripts/devcontainer/exec-skill.sh /c skill"))
        assert orchestrate_epic.is_agent_process(4242) is True

    def test_non_linux_unverifiable_pid_is_treated_as_dead(self, tmp_path, monkeypatch):
        import orchestrate_epic
        monkeypatch.setattr(orchestrate_epic, "PROC_ROOT", str(tmp_path / "no-proc"))
        monkeypatch.setattr(orchestrate_epic.os, "kill", lambda pid, sig: None)

        def no_ps(*a, **kw):
            raise FileNotFoundError("ps")

        monkeypatch.setattr(orchestrate_epic.subprocess, "run", no_ps)
        assert orchestrate_epic.is_agent_process(4242) is False

    def test_dead_pid_is_not_an_agent(self):
        from orchestrate_epic import is_agent_process
        proc = subprocess.Popen(["true"])
        proc.wait()
        assert is_agent_process(proc.pid) is False


# ── --dry-run ───────────────────────────────────────────


class TestDryRun:
    """A dry run launches and prepares real containers, smokes the stack with
    exec.sh instead of starting the story agent, and keeps everything."""

    def _orchestrator(self, tmp_path, monkeypatch, smoke_exit=0, refs=(101,)):
        orch, mock_bin = _make_orchestrator(tmp_path, refs=refs, dry_run=True)
        calls = tmp_path / "calls.log"
        _write_script(mock_bin / "launch.sh",
                      f'echo "launch $1" >> "{calls}"\n'
                      'echo "CONTAINER_ID=cid-$(basename "$1")"\n')
        _write_script(mock_bin / "prepare-clone.sh", f'echo "prepare $1" >> "{calls}"\n')
        _write_script(mock_bin / "exec.sh",
                      f'echo "exec $1 $2" >> "{calls}"\n'
                      'echo "smoke output"\n'
                      f'exit {smoke_exit}\n')
        _write_script(mock_bin / "exec-skill.sh", f'echo "exec-skill $*" >> "{calls}"\n')
        _write_script(mock_bin / "teardown.sh", f'echo "teardown $*" >> "{calls}"\n')
        # Any gh call a dry run makes lands in the call log.
        _write_script(mock_bin / "gh", f'echo "gh $*" >> "{calls}"\n')
        _write_script(mock_bin / "docker", f'echo "docker $*" >> "{calls}"\n')
        monkeypatch.setenv("PATH", f"{mock_bin}{os.pathsep}{os.environ['PATH']}")
        return orch, calls

    def _run_story_to_completion(self, orch, ref=101):
        assert orch.launch_story(ref) is True
        orch.child_procs[ref].wait(timeout=10)
        orch.check_completions()

    def test_dry_run_runs_smoke_instead_of_agent(self, tmp_path, monkeypatch):
        orch, calls = self._orchestrator(tmp_path, monkeypatch)

        self._run_story_to_completion(orch)

        clone = orch.sm.get_story(101)["clonePath"]
        lines = calls.read_text().splitlines()
        assert lines == [
            f"launch {clone}", f"prepare {clone}", f"exec {clone} {SMOKE_CMD}",
        ]
        assert "smoke output" in Path(orch.sm.get_story(101)["logFile"]).read_text()

    def test_dry_run_never_tears_down_or_pushes(self, tmp_path, monkeypatch, capsys):
        orch, calls = self._orchestrator(tmp_path, monkeypatch)

        self._run_story_to_completion(orch)
        orch._final_cleanup()

        recorded = calls.read_text()
        for forbidden in ("teardown", "exec-skill", "gh ", "docker "):
            assert forbidden not in recorded
        clone = orch.sm.get_story(101)["clonePath"]
        assert f"Dry run: kept {clone} (container cid-story-101)" in capsys.readouterr().err

    @pytest.mark.parametrize("smoke_exit,status,outcome", [
        (0, "complete", "dry_run_ok"),
        (3, "error", "dry_run_failed"),
    ])
    def test_dry_run_outcome_follows_smoke_exit_code(
            self, tmp_path, monkeypatch, capsys, smoke_exit, status, outcome):
        orch, _ = self._orchestrator(tmp_path, monkeypatch, smoke_exit=smoke_exit)

        self._run_story_to_completion(orch)

        story = orch.sm.get_story(101)
        assert (story["status"], story["outcome"]) == (status, outcome)
        assert f"COMPLETED:101:{outcome}:none" in capsys.readouterr().out

    def test_dry_run_skips_dependency_bearing_story(self, tmp_path, monkeypatch, capsys):
        orch, calls = self._orchestrator(tmp_path, monkeypatch, refs=(101, 102))
        orch.stories_file = str(tmp_path / "stories.json")
        Path(orch.stories_file).write_text(json.dumps([
            {"ref": 101, "id": 1101, "subject": "Root"},
            {"ref": 102, "id": 1102, "subject": "Child", "dependencies": [101]},
        ]))
        Path(orch.sm.state_file).unlink()

        orch.init_or_resume()

        assert orch.sm.get_story(101)["status"] == "queued"
        child = orch.sm.get_story(102)
        assert (child["status"], child["outcome"]) == ("skipped", "dry_run_dependency")
        assert "SKIPPED:102:dry_run_dependency" in capsys.readouterr().out

    def test_dry_run_state_records_the_flag(self, tmp_path, monkeypatch):
        orch, _ = self._orchestrator(tmp_path, monkeypatch)
        Path(orch.sm.state_file).unlink()

        orch.init_or_resume()

        assert json.loads(Path(orch.sm.state_file).read_text())["dryRun"] is True

    @pytest.mark.parametrize("state_dry_run,run_dry_run", [(True, False), (False, True)])
    def test_resume_rejects_dry_run_mismatch(
            self, tmp_path, state_dry_run, run_dry_run, capsys):
        first = _resume_orchestrator(tmp_path, "main", dry_run=state_dry_run)
        first.init_or_resume()
        before = Path(first.sm.state_file).read_text()

        second = _resume_orchestrator(tmp_path, "main", dry_run=run_dry_run)
        with pytest.raises(SystemExit) as exc:
            second.init_or_resume()

        assert exc.value.code == 2
        assert "dry" in capsys.readouterr().err.lower()
        assert Path(first.sm.state_file).read_text() == before


class TestUsage:
    def _run(self, tmp_path, *extra):
        stories = tmp_path / "stories.json"
        stories.write_text("[]")
        return subprocess.run(
            [sys.executable, os.path.join(REPO_ROOT, "scripts", "devcontainer", "orchestrate_epic.py"),
             "99999", "1", str(stories), *extra],
            capture_output=True, text=True, timeout=30,
            env={**os.environ, "ORCHESTRATE_STATE_DIR": str(tmp_path)},
        )

    def test_unknown_extra_argument_is_usage_error(self, tmp_path):
        result = self._run(tmp_path, "--bogus")
        assert result.returncode == 1
        assert "--dry-run" in result.stderr
