# ABOUTME: Deterministic state machine for parallel story implementation across devcontainers.
# ABOUTME: Manages launch/monitor/teardown lifecycle; emits machine-parseable events on stdout.
"""
Usage: python3 orchestrate_epic.py <epic-ref> <concurrency> <stories-json-file> [--dry-run]

--dry-run launches and prepares each story's container like a real run, then
smokes the container's Supabase stack with exec.sh instead of starting the story
agent. It never pushes, never calls gh and never tears anything down: containers
and clones are kept so the operator can check isolation between them.

Each story runs in its own devcontainer (launch.sh), with its own Docker-in-Docker
daemon and its own local Supabase stack, and the story agent is started through
exec-skill.sh. Nothing here touches a hosted Supabase project.

Environment variables:
  ORCHESTRATE_REMOTE      Git remote that holds the base and feature branches
                          (default: upstream)
  GH_REPO                 GitHub repository every gh call targets
                          (default: MrsBohemian/Cholobots)
  ORCHESTRATE_SCRIPT_DIR  Override directory containing launch.sh, prepare-clone.sh,
                          exec-skill.sh and teardown.sh
  ORCHESTRATE_STATE_DIR   Override directory for the epic state file and the host-side
                          per-story fully-implement-<ref>.json files (default: /tmp)
  POLL_INTERVAL           Seconds between poll cycles (default: 30)
  BASE_BRANCH             Branch to clone for each story devcontainer (default: main)
  ORCHESTRATE_LAUNCH_TIMEOUT  Seconds allowed for launch.sh before giving up
                          (default: 1800 — a cold Docker-in-Docker launch pulls
                          images and provisions services, which exceeds 600s)
  ORCHESTRATE_TEARDOWN_TIMEOUT  Seconds allowed for teardown.sh (default: 180)
  ORCHESTRATE_TRANSIENT_BACKOFF  Seconds to wait before relaunching a story whose
                          agent was killed by a provider rate limit when the log
                          carries no reset time (default: 900)
"""

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Add shared lib to path for dependency_parser import
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from dependency_parser import find_dependency_cycle  # noqa: E402
from integration_branch import integration_branch_name  # noqa: E402
from base_selector import UnresolvedDependencyError, select_base_branch  # noqa: E402


MAX_LAUNCH_ATTEMPTS = 3

# Git remote and GitHub repository the orchestrator works against. Read once at
# import, like scripts/merge_train.py: the PR branches and every PR live in
# MrsBohemian/Cholobots, whose remote is `upstream` (`origin` is only a mirror).
REMOTE = os.environ.get("ORCHESTRATE_REMOTE", "upstream")
GH_REPO = os.environ.get("GH_REPO", "MrsBohemian/Cholobots")

# The state file records which mode wrote it so a resume never mixes the
# container orchestrator with the worktree slot loop.
STATE_MODE = "container"

# What a dry run executes in each container in place of the story agent:
# the stack must be healthy, reset cleanly and pass the integration tests.
SMOKE_CMD = (
    "supabase status -o env && supabase db reset "
    "&& { [ ! -d tests/integration ] || python -m pytest tests/integration -q; }"
)


def gh_cmd(*args):
    """Build a gh argv. The shell's GH_TOKEN cannot see the repo, so it is
    dropped, and every call names the repo explicitly."""
    return ["env", "-u", "GH_TOKEN", "gh", *args, "--repo", GH_REPO]

# Cap on a single host-repo git/gh command. Launch workers queue behind the
# same lock for these, so a hung networked command would stall every launch.
RUN_CMD_TIMEOUT_SECONDS = 180

# Agent exits that are transient (provider-side), not story failures. A bare
# `api_error` terminal reason also covers authentication and invalid-request
# failures, so it only counts when the result text carries a rate-limit marker;
# the markers are never consulted for any other terminal reason.
TRANSIENT_TERMINAL_REASONS = {"rate_limit", "rate_limited"}
TRANSIENT_RESULT_MARKERS = ("session limit", "rate limit", "rate_limit_error",
                            "overloaded_error", "usage limit")
MAX_TRANSIENT_RETRIES = 3
# How long to wait when the log carries no rate_limit_event with a reset time.
DEFAULT_TRANSIENT_BACKOFF_SECONDS = 900
# Only a rejecting rate_limit_event reports a reset the agent is waiting for.
# `allowed` / `allowed_warning` records are routine window-status telemetry whose
# `resetsAt` is the end of the current usage window.
REJECTING_RATE_LIMIT_STATUSES = {"rejected"}
# Upper bound on a hold parsed from a rate_limit_event, sized from the window
# the event itself names: a reset further out than its own window is a misread
# and must not park the whole epic. A rejection on the weekly window really does
# reset days out, so capping it at the five-hour bound would relaunch into the
# still-active limit and burn the retry budget. An unnamed window gets the
# shortest bound.
MAX_TRANSIENT_HOLD_SECONDS_BY_TYPE = {
    "five_hour": 5 * 60 * 60,
    "seven_day": 7 * 24 * 60 * 60,
}
MAX_TRANSIENT_HOLD_SECONDS = 5 * 60 * 60
# Number of log lines the exit classifiers inspect. The stream-json result
# record and the rate_limit_event that precedes it are always within this tail.
LOG_TAIL_LINES = 100

ISO_UTC_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class CircularDependencyError(Exception):
    """Raised when the story dependency graph contains a cycle."""

# Statuses that reconcile_with_input unconditionally re-queues on resume.
# 'escalated' is also retryable, but only when no PR exists (see reconcile rules).
RETRYABLE_STATUSES = {"error", "skipped"}

# Phases the story workflow writes when it has created the PR and handed CI
# monitoring off to background /remediate-pr. Exiting here WITH a PR is a clean
# handoff (success), not an escalation. Non-handoff non-terminal phases (e.g.
# "testing", "fixing") with a PR still escalate (defense in depth).
HANDOFF_PHASES = {"monitoring"}
SUCCESS_OUTCOMES = {"complete", "handed_off"}


def format_utc(dt):
    """Format an aware datetime as the ISO 8601 UTC string used throughout state.

    Every timestamp the orchestrator persists or compares uses this one format,
    so `notBefore` gating can compare strings without a datetime round-trip.
    """
    return dt.astimezone(timezone.utc).strftime(ISO_UTC_FORMAT)


def utcnow_iso():
    """Return current UTC time in ISO 8601 format."""
    return format_utc(datetime.now(timezone.utc))


def transient_backoff_seconds():
    """Wait before relaunching a rate-limited story whose reset time is unknown.

    Read at use, like the other ORCHESTRATE_* tunables, so it can be changed
    for a running test or process without re-importing the module.
    """
    return int(os.environ.get("ORCHESTRATE_TRANSIENT_BACKOFF", DEFAULT_TRANSIENT_BACKOFF_SECONDS))


def read_log_tail(log_file, max_lines):
    """Return the last `max_lines` lines of a log without reading the whole file.

    Agent logs run to tens of MB; only the final records matter to the exit
    classifiers, so this seeks to the end and reads a bounded window. Returns
    [] for a missing or unreadable file.
    """
    window = 256 * 1024
    try:
        with open(log_file, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - window))
            data = f.read()
    except OSError:
        return []
    lines = data.decode("utf-8", errors="replace").splitlines(keepends=True)
    if size > window and len(lines) > max_lines:
        # Drop the first line: the window may have started mid-line.
        lines = lines[1:]
    return lines[-max_lines:]


def emit(event):
    """Write a machine-parseable event to stdout."""
    print(event, flush=True)


def log(msg):
    """Write a log message to stderr."""
    print(f"[orchestrate] {msg}", file=sys.stderr, flush=True)


class StateManager:
    """Manages the orchestrator state file with atomic writes."""

    def __init__(self, state_file):
        self.state_file = state_file
        self._state = None
        # Launches run on worker threads, so the poll loop and several launches
        # read and write this state concurrently. RLock, not Lock: count_status
        # calls stories_with_status and update_story calls save.
        self._lock = threading.RLock()

    @property
    def state(self):
        return self._state

    def load(self):
        """Load state from disk, backfilling fields older state files lack."""
        with self._lock:
            with open(self.state_file) as f:
                self._state = json.load(f)
            for story in self._state["stories"]:
                story.setdefault(
                    "holdsPreservedWork", self._infer_legacy_preserved_work(story))
            return self._state

    @staticmethod
    def _infer_legacy_preserved_work(story):
        """Derive `holdsPreservedWork` for a record written before the marker existed.

        Older state files carry only the per-run signals: a story parked or
        relaunching after a rate-limited exit (or skipped before it relaunched,
        or left in `error` because the relaunch over its clone failed for good)
        has `transientRetries > 0`, and one whose retry budget ran out has
        `outcome == "rate_limit_exhausted"`. Either means the clone holds
        unpushed commits. reconcile_with_input resets the retry budget, so this
        must run before it does.
        """
        if story.get("outcome") == "rate_limit_exhausted":
            return True
        return (story.get("transientRetries", 0) > 0
                and story["status"] in ("queued", "launching", "skipped", "error"))

    def save(self):
        """Save state to disk atomically (write temp + rename)."""
        with self._lock:
            tmp = f"{self.state_file}.tmp.{os.getpid()}"
            with open(tmp, "w") as f:
                json.dump(self._state, f, indent=2)
            os.rename(tmp, self.state_file)

    def exists(self):
        return os.path.exists(self.state_file)

    def _normalize_deps(self, stories):
        """Build a ref -> dependencies map from a list of story dicts.

        Each story may include an optional 'dependencies' list of ref numbers.
        If any story has dependencies, circular dependency detection runs and
        raises CircularDependencyError if a cycle is found.
        """
        has_deps = any("dependencies" in s for s in stories)
        deps_map = {}
        if has_deps:
            known_refs = {s["ref"] for s in stories}
            for s in stories:
                raw_deps = s.get("dependencies")
                if not isinstance(raw_deps, list):
                    normalized_deps = []
                else:
                    # Only keep integer deps that refer to known refs in this run;
                    # unknown refs would never become satisfied and deadlock the run.
                    normalized_deps = [d for d in raw_deps if isinstance(d, int) and d in known_refs]
                    unknown = [d for d in raw_deps if isinstance(d, int) and d not in known_refs]
                    if unknown:
                        log(f"WARNING: Story {s['ref']} has deps on unknown refs {unknown}, ignoring them")
                deps_map[s["ref"]] = normalized_deps
            cycle = find_dependency_cycle(deps_map)
            if cycle:
                raise CircularDependencyError(
                    "Circular dependency: " + " -> ".join(f"#{r}" for r in cycle)
                )
            log(f"Dependency graph: {json.dumps({str(k): v for k, v in sorted(deps_map.items())})}")
        return deps_map

    def _new_story_record(self, s, base_dir, deps):
        """Build a fresh queued story record (shared by init and reconcile)."""
        return {
            "ref": s["ref"],
            "id": s["id"],
            "subject": s["subject"],
            "status": "queued",
            "clonePath": str(base_dir / f"story-{s['ref']}"),
            "pid": None,
            "logFile": str(base_dir / f"story-{s['ref']}.log"),
            "startedAt": None,
            "completedAt": None,
            "outcome": None,
            "prNumber": None,
            "containerId": None,
            "launchAttempts": 0,
            "transientRetries": 0,
            "holdsPreservedWork": False,
            "notBefore": None,
            "dependencies": deps,
            "branch": None,
            "baseBranch": None,
        }

    def init_from_stories(self, epic_ref, concurrency, stories, base_dir, base_branch="main",
                          dry_run=False):
        """Initialize state from a list of story dicts."""
        deps_map = self._normalize_deps(stories)
        with self._lock:
            self._state = {
                "mode": STATE_MODE,
                "dryRun": dry_run,
                "epicRef": str(epic_ref),
                "concurrency": concurrency,
                "baseBranch": base_branch,
                "phase": "running",
                "baseDir": str(base_dir),
                "stories": [
                    self._new_story_record(s, base_dir, deps_map.get(s["ref"], []))
                    for s in stories
                ],
            }
            self.save()

    def reconcile_with_input(self, stories, base_dir):
        """Merge stories-input into existing state by ref, re-queuing failures.

        Rules: 'complete' and PR-bearing 'escalated' stories are left alone
        (never re-launched, so no duplicate PRs); 'error', 'skipped', and
        PR-less 'escalated' stories are re-queued (with dependencies
        refreshed from the input); refs present in the input
        but absent from state are appended as fresh queued stories; refs in
        state but absent from the input are left untouched.

        Returns (requeued_refs, added_refs).
        """
        deps_map = self._normalize_deps(stories)
        with self._lock:
            return self._reconcile_locked(stories, base_dir, deps_map)

    def _reconcile_locked(self, stories, base_dir, deps_map):
        existing = {s["ref"]: s for s in self._state["stories"]}
        requeued, added = [], []

        for s in stories:
            ref = s["ref"]
            story = existing.get(ref)
            if story is None:
                self._state["stories"].append(
                    self._new_story_record(s, base_dir, deps_map.get(ref, []))
                )
                added.append(ref)
                continue

            status = story["status"]
            if status == "complete":
                continue
            if status == "escalated" and story.get("prNumber") is not None:
                continue
            # The dependency diagram may have changed between runs; refresh
            # deps from the input so next_queued_ref() schedules correctly.
            story["dependencies"] = deps_map.get(ref, [])
            if status in RETRYABLE_STATUSES or status == "escalated":
                story.update(
                    status="queued", outcome=None, completedAt=None,
                    startedAt=None, pid=None, containerId=None, launchAttempts=0,
                    transientRetries=0, notBefore=None, baseBranch=None,
                )
                requeued.append(ref)

        # Reset phase so a state loaded at "complete" doesn't stay stale on
        # disk if the process dies before run() finishes.
        self._state["phase"] = "running"
        self.save()
        return requeued, added

    def update_story(self, ref, **kwargs):
        """Update fields on a story by ref number."""
        with self._lock:
            for story in self._state["stories"]:
                if story["ref"] == ref:
                    story.update(kwargs)
                    break
            self.save()

    def get_story(self, ref):
        """Get a story dict by ref number."""
        with self._lock:
            for story in self._state["stories"]:
                if story["ref"] == ref:
                    return story
            return None

    def stories_with_status(self, status):
        """Return list of story dicts with the given status."""
        with self._lock:
            return [s for s in self._state["stories"] if s["status"] == status]

    def count_status(self, status):
        """Count stories with the given status."""
        with self._lock:
            return len(self.stories_with_status(status))

    def rate_limited_until(self, now=None):
        """Return the latest future `notBefore` among queued stories, or None.

        A non-None result means a provider rate limit is in force. The limit is
        account-wide, so callers hold every launch until it passes rather than
        starting more stories into the same window.
        """
        with self._lock:
            now = now or utcnow_iso()
            pending = [s["notBefore"] for s in self.stories_with_status("queued")
                       if s.get("notBefore") and s["notBefore"] > now]
            return max(pending) if pending else None

    def next_queued_ref(self, now=None):
        """Return the ref of the first launchable queued story, or None.

        A story is launchable if all its dependencies have reached 'complete' status
        and its `notBefore` (set after a rate-limited exit) has passed. `now` is an
        ISO 8601 UTC string in utcnow_iso() format; that format compares correctly
        as a plain string, so no datetime round-trip is needed.
        Stories parked by a rate limit come first — they already hold a container,
        so resuming them before starting fresh ones keeps the container count at
        the concurrency limit — then by ref number.
        """
        with self._lock:
            completed_refs = {s["ref"] for s in self._state["stories"] if s["status"] == "complete"}
            now = now or utcnow_iso()
            queued = sorted(self.stories_with_status("queued"),
                            key=lambda s: (s.get("transientRetries", 0) == 0, s["ref"]))
            for story in queued:
                not_before = story.get("notBefore")
                if not_before and not_before > now:
                    continue
                deps = story.get("dependencies", [])
                if all(d in completed_refs for d in deps):
                    return story["ref"]
            return None

    def cascade_skip(self, failed_ref):
        """Skip all stories that transitively depend on a failed/escalated story.

        Returns a list of refs that were skipped.
        """
        with self._lock:
            skipped = []
            # Build a set of refs that are failed/skipped (starts with the failed one)
            blocked_refs = {failed_ref}
            # Iterate until no more stories can be skipped
            changed = True
            while changed:
                changed = False
                for story in self._state["stories"]:
                    if story["status"] != "queued":
                        continue
                    deps = story.get("dependencies", [])
                    if any(d in blocked_refs for d in deps):
                        # Update in-memory only; persist once at the end to reduce disk churn.
                        story["status"] = "skipped"
                        story["outcome"] = "dependency_failed"
                        story["completedAt"] = utcnow_iso()
                        blocked_refs.add(story["ref"])
                        skipped.append(story["ref"])
                        changed = True
            if skipped:
                self.save()
            return skipped

    def set_phase(self, phase):
        """Set the overall phase."""
        with self._lock:
            self._state["phase"] = phase
            self.save()


# Where Linux exposes process command lines; overridable so tests can exercise
# the non-Linux path.
PROC_ROOT = "/proc"


def valid_pr_number(pr):
    """Return `pr` as a string when it is a positive integer, else None.

    A state file's prNumber can be null, 0, "", a stale string or a bool; none
    of those is a reviewable PR, and treating one as success would unblock
    dependents (or count a bug as fixed) with nothing to review.
    """
    if isinstance(pr, bool):
        return None
    if isinstance(pr, int):
        return str(pr) if pr > 0 else None
    if isinstance(pr, str):
        text = pr.strip()
        if text.isascii() and text.isdigit() and int(text) > 0:
            return str(int(text))
    return None


def is_agent_process(pid):
    """Check if a PID is alive and is an exec-skill.sh process (the agent launcher)."""
    try:
        os.kill(pid, 0)
    except (OSError, ProcessLookupError):
        return False

    # Linux-specific: check /proc/<pid>/cmdline
    cmdline_path = f"{PROC_ROOT}/{pid}/cmdline"
    if os.path.exists(cmdline_path):
        try:
            with open(cmdline_path, "rb") as f:
                # cmdline is NUL-separated argv; split to get individual tokens
                tokens = f.read().split(b"\x00")
            # The host process is `bash .../exec-skill.sh <clone> <skill> ...`.
            # Match the script's basename in an argv token so unrelated bash
            # or docker processes that reuse a recycled PID do not count.
            token_strs = [t.decode("utf-8", errors="replace") for t in tokens]
            return any(os.path.basename(t) == "exec-skill.sh" for t in token_strs)
        except (OSError, PermissionError):
            return False

    # No /proc (macOS, BSD): a live PID alone proves nothing, since a dead
    # agent's PID can be reused by an unrelated process and the story would
    # then stay active forever. Ask ps for the command line, and treat a PID
    # whose command cannot be verified as dead (the resume then requeues it).
    try:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "command="],
            capture_output=True, text=True, timeout=5,
        )
    except (subprocess.TimeoutExpired, OSError):
        return False
    if result.returncode != 0:
        return False
    return any(os.path.basename(t) == "exec-skill.sh" for t in (result.stdout or "").split())


def _classify_phase(phase, pr):
    """Classify a state file phase into an outcome.

    Returns (outcome, escalation_reason). Terminal phases return reason=None.
    A handoff phase with a PR is a clean handoff (handed_off, success). Other
    non-terminal phases with a PR are downgraded to escalated.
    Only treat prNumber as valid if it is a positive integer to avoid masking
    errors caused by empty-string, "0", or stale null-ish values.
    """
    has_valid_pr = pr is not None and str(pr).strip().isdigit() and int(str(pr)) > 0
    if phase == "complete":
        return "complete", None
    elif phase == "escalated":
        return "escalated", None
    elif phase in HANDOFF_PHASES and has_valid_pr:
        # Clean handoff: PR created, CI monitoring delegated to background
        # /remediate-pr, agent exited. Success, not escalation.
        return "handed_off", None
    elif has_valid_pr:
        # Inner agent exited without writing a terminal phase but did create a PR
        # (e.g., paused asking user questions, then was timed out by parent).
        # Surface as escalation rather than silent error.
        return "escalated", f"non-terminal phase: {phase}"
    return None, None


def determine_outcome(ref, log_file, container_id=None, state_dir=None):
    """Determine the outcome of a completed story process.

    Returns (outcome, pr_number, escalation_reason, branch). escalation_reason
    is non-None only when a non-terminal phase was downgraded to escalated.
    branch is the story's feature branch (from the state file's
    "featureBranch" key) when available, else None — the log-scan fallback
    never recovers it.
    """
    outcome = "error"
    pr_number = "none"
    escalation_reason = None
    branch = None
    if state_dir is None:
        state_dir = "/tmp"

    # Primary: read the per-story state file from inside the container.
    # The file is written by fully-implement-story inside the container's /tmp.
    if container_id:
        try:
            result = subprocess.run(
                ["docker", "exec", container_id,
                 "cat", f"/tmp/fully-implement-{ref}.json"],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                story_state = json.loads(result.stdout)
                phase = story_state.get("phase", "")
                pr = story_state.get("prNumber")
                branch = story_state.get("featureBranch")

                pr_number = valid_pr_number(pr) or "none"

                classified, reason = _classify_phase(phase, pr)
                if classified is not None:
                    outcome = classified
                    escalation_reason = reason

                # Clean up inside container
                subprocess.run(
                    ["docker", "exec", container_id,
                     "rm", "-f", f"/tmp/fully-implement-{ref}.json"],
                    capture_output=True, timeout=10,
                )
        except (subprocess.TimeoutExpired, json.JSONDecodeError, OSError):
            pass

    # Secondary: check a host-side state file in the state dir (what a fake
    # exec-skill.sh writes in tests, where no container exists).
    if outcome == "error":
        host_state_path = os.path.join(state_dir, f"fully-implement-{ref}.json")
        if os.path.exists(host_state_path):
            try:
                with open(host_state_path) as f:
                    story_state = json.loads(f.read())
                phase = story_state.get("phase", "")
                pr = story_state.get("prNumber")
                branch = story_state.get("featureBranch")

                pr_number = valid_pr_number(pr) or "none"

                classified, reason = _classify_phase(phase, pr)
                if classified is not None:
                    outcome = classified
                    escalation_reason = reason

                os.unlink(host_state_path)
            except (json.JSONDecodeError, OSError):
                pass

    # Fallback: scan log tail (exec-skill.sh writes the agent's stream to the host log)
    if outcome == "error" and log_file:
        lower_tail = "".join(read_log_tail(log_file, LOG_TAIL_LINES)).lower()
        if "ready for test" in lower_tail or "ready for review" in lower_tail:
            outcome = "complete"
        elif "escalated" in lower_tail or "escalation-needed" in lower_tail:
            outcome = "escalated"

    return outcome, pr_number, escalation_reason, branch


def _is_transient_result(record):
    """True when a stream-json result record is a provider-side (rate-limit) exit."""
    if not record.get("is_error"):
        return False
    reason = record.get("terminal_reason")
    if reason in TRANSIENT_TERMINAL_REASONS:
        return True
    if reason == "api_error":
        result_text = str(record.get("result", "")).lower()
        return any(marker in result_text for marker in TRANSIENT_RESULT_MARKERS)
    return False


def detect_transient_exit(log_file, now=None):
    """Inspect the tail of a stream-json agent log for a transient termination.

    Returns (True, not_before_iso) when the agent's final result record is a
    provider-side failure (rate limit), else (False, None). The relaunch time
    is the `resetsAt` epoch of the last *rejecting* `rate_limit_event` record
    the CLI wrote (taken from the provider's reset header, so no wall-clock or
    zone parsing is involved). Benign `allowed` / `allowed_warning` window-status
    records are skipped: their `resetsAt` is the end of the current usage window,
    not a reset the agent is waiting on. Any parsed reset is capped at the
    length of the window the event names (MAX_TRANSIENT_HOLD_SECONDS_BY_TYPE),
    so a weekly rejection is honoured in full; a reset that has already passed relaunches now,
    and a log with no rejecting event waits the default backoff. Never raises: a
    malformed or missing log is simply not transient. `now` is injectable for tests.
    """
    if not log_file:
        return False, None
    now = now or datetime.now(timezone.utc)
    result = None
    resets_at = None
    limit_type = ""
    for line in reversed(read_log_tail(log_file, LOG_TAIL_LINES)):
        try:
            obj = json.loads(line)
        except ValueError:
            continue
        if not isinstance(obj, dict):
            continue
        if result is None:
            if obj.get("type") == "result":
                result = obj
            continue
        if obj.get("type") == "rate_limit_event":
            info = obj.get("rate_limit_info") or {}
            if str(info.get("status", "")).lower() in REJECTING_RATE_LIMIT_STATUSES:
                resets_at = info.get("resetsAt")
                limit_type = str(info.get("rateLimitType", "")).lower()
                break
    if result is None or not _is_transient_result(result):
        return False, None
    if isinstance(resets_at, (int, float)) and not isinstance(resets_at, bool):
        not_before = max(datetime.fromtimestamp(resets_at, tz=timezone.utc), now)
        cap_seconds = MAX_TRANSIENT_HOLD_SECONDS_BY_TYPE.get(limit_type, MAX_TRANSIENT_HOLD_SECONDS)
        cap = now + timedelta(seconds=cap_seconds)
        if not_before > cap:
            log(f"WARNING: rate_limit_event reset {format_utc(not_before)} exceeds the "
                f"{cap_seconds}s cap — holding until {format_utc(cap)} instead")
            not_before = cap
    else:
        not_before = now + timedelta(seconds=transient_backoff_seconds())
    return True, format_utc(not_before)


class HostRepoBaseMixin:
    """Resolve a base branch in the host repo before launch.sh clones from it.

    Shared by both orchestrators. The host class provides `repo_root` (a Path)
    and `git_lock` (a threading.Lock serialising git runs against that repo).
    """

    def _run_git(self, *args, timeout=15):
        """Run a git command against the host repo (not a story's clone) and
        return the CompletedProcess. Broken out from _ensure_base_reachable so
        tests can stub the git runner without a real subprocess/network call.

        The repo is self.repo_root (see __init__).
        """
        # Same host repo, same refs and FETCH_HEAD as _run_cmd — share its lock
        # so concurrent launches cannot race on git's own index/ref locks.
        with self.git_lock:
            return subprocess.run(
                ["git", "-C", str(self.repo_root)] + list(args),
                capture_output=True, text=True, timeout=timeout,
            )

    def _ensure_base_reachable(self, base):
        """Guarantee `base` resolves as a local head in the host repo that
        matches the remote, before launch.sh clones from it.

        A dependency's `feature/<slug>-<ref>` branch is created and pushed to
        GitHub from inside that story's own container, so the host repo —
        where launch.sh's `git clone --branch` runs — has never seen it.
        `git clone --branch` resolves against refs/heads/ and refs/tags/
        only, so a bare `git fetch <remote> <base>` (which only writes
        refs/remotes/<remote>/<base>) would not be enough; the fetch below
        creates a local head instead.

        The local head this leaves behind in the operator's host repo is not
        evidence that the base is current: a relaunch, a re-run of the epic,
        or a `gh pr checkout` can all leave a head at an older commit, and
        merge-train force-pushes mean the stale head may not even be an
        ancestor of the remote tip. So the head is always re-fetched with a
        forced refspec, which resets it to the remote tip rather than failing
        the fast-forward. That is safe because the orchestrator, not a human,
        owns these heads.

        The heads the orchestrator must not rewrite are the ones checked out
        in the host repo or in one of its linked worktrees (worktree mode
        leaves a base branch checked out in one) — git refuses to fetch into
        them anyway. Those are verified instead: the local head must equal the
        remote tip, otherwise the launch fails loudly rather than cloning a
        stale base or unpushed host commits.

        Returns True if `base` is (or was made) reachable and current, False
        otherwise.
        """
        checked_out = self._checked_out_branches()
        if checked_out is None:
            return False

        if base in checked_out:
            return self._verify_checked_out_base(base)

        try:
            fetch = self._run_git("fetch", REMOTE, f"+{base}:{base}", timeout=60)
        except (subprocess.TimeoutExpired, OSError) as e:
            log(f"Could not fetch base branch {base}: {e}")
            return False
        if fetch.returncode != 0:
            log(f"Could not fetch base branch {base}: {(fetch.stderr or '').strip()}")
            return False
        return True

    def _checked_out_branches(self):
        """Return the set of branches checked out in the host repo or any of
        its linked worktrees, or None if git could not be queried.

        `symbolic-ref HEAD` would only report the main worktree's branch, but
        git refuses to fetch into a branch checked out in *any* worktree, and
        worktree mode routinely leaves a base branch checked out in a linked
        one.
        """
        try:
            listing = self._run_git("worktree", "list", "--porcelain")
        except (subprocess.TimeoutExpired, OSError) as e:
            log(f"Could not list the host repo's worktrees: {e}")
            return None
        if listing.returncode != 0:
            log(f"Could not list the host repo's worktrees: {(listing.stderr or '').strip()}")
            return None

        branches = set()
        for line in (listing.stdout or "").splitlines():
            if line.startswith("branch refs/heads/"):
                branches.add(line[len("branch refs/heads/"):].strip())
        return branches

    def _verify_checked_out_base(self, base):
        """Confirm a checked-out `base` is exactly at <remote>/<base>. Used
        instead of a fetch, which git refuses for a branch checked out in the
        host repo or any of its worktrees. Returns True if it is current,
        False otherwise.
        """
        try:
            fetch = self._run_git("fetch", REMOTE, base, timeout=60)
        except (subprocess.TimeoutExpired, OSError) as e:
            log(f"Could not fetch base branch {base}: {e}")
            return False
        if fetch.returncode != 0:
            log(f"Could not fetch base branch {base}: {(fetch.stderr or '').strip()}")
            return False

        # launch.sh clones the local head, so the head must be exactly the
        # remote tip. Containing it is not enough: a host branch with
        # local-only commits would carry that unpushed work into every
        # generated branch, while the PR targets the remote base.
        try:
            remote_tip = self._run_git("rev-parse", "FETCH_HEAD")
            local_tip = self._run_git("rev-parse", f"refs/heads/{base}")
        except (subprocess.TimeoutExpired, OSError) as e:
            log(f"Could not compare base branch {base} with {REMOTE}: {e}")
            return False
        if (remote_tip.returncode != 0 or local_tip.returncode != 0
                or (remote_tip.stdout or "").strip() != (local_tip.stdout or "").strip()):
            log(
                f"Base branch {base} is checked out in the host repo (or one of "
                f"its worktrees) and is not at {REMOTE}/{base} (behind it, or "
                f"ahead with unpushed commits); refusing to launch from a base "
                f"that differs from the remote. Check out a different branch, "
                f"or bring {base} level with {REMOTE}/{base}, and re-run."
            )
            return False
        return True


class Orchestrator(HostRepoBaseMixin):
    """Main orchestrator state machine."""

    def __init__(self, epic_ref, concurrency, stories_file, script_dir, state_dir, base_branch,
                 repo_root=None, dry_run=False):
        self.epic_ref = epic_ref
        self.dry_run = dry_run
        self.concurrency = concurrency
        self.stories_file = stories_file
        self.script_dir = Path(script_dir)
        # The host repo _run_git targets: by default the clone this script
        # lives in, derived from its own location rather than os.getcwd() —
        # the orchestrator may be invoked from anywhere.
        self.repo_root = Path(repo_root) if repo_root else Path(__file__).resolve().parents[2]
        self.state_dir = Path(state_dir)
        self.base_branch = base_branch
        self.launch_timeout = int(os.environ.get("ORCHESTRATE_LAUNCH_TIMEOUT", "1800"))
        self.shutting_down = False
        self.child_procs = {}     # ref -> Popen object (the agent)
        self.launch_threads = {}  # ref -> Thread running launch_story
        self.launch_procs = {}    # ref -> Popen of the helper script a launch is running
        # Guards child_procs / launch_threads / launch_procs, which the poll
        # loop and the launch worker threads both touch. RLock, not Lock: the
        # signal handler runs on the main thread and calls cleanup, which takes
        # this lock again — a plain Lock would deadlock a shutdown that arrived
        # while the main thread held it.
        self.proc_lock = threading.RLock()
        # Serializes host-repository git/gh work across concurrent launches
        # (see _run_cmd). RLock so a helper that already holds it can nest.
        self.git_lock = threading.RLock()
        self._paused_until_logged = None  # last rate-limit hold announced in the log

        state_file = str(self.state_dir / f"implement-epic-{epic_ref}.json")
        self.sm = StateManager(state_file)
        self.base_dir = self.state_dir / f"epic-{epic_ref}"

    @staticmethod
    def _join_threads(threads, timeout):
        """Join threads, skipping any that were registered but never started.

        fill_slots registers a thread before starting it, and the signal handler
        runs on the main thread — so it can observe that gap. join() on an
        unstarted thread raises RuntimeError, which would abort cleanup.
        """
        for t in threads:
            if t.ident is None:
                continue
            t.join(timeout=timeout)

    def setup_signals(self):
        """Install signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle SIGINT/SIGTERM by triggering cleanup."""
        self.shutting_down = True
        log("Signal received, shutting down...")

        # Guard against signals arriving before state is initialized
        if self.sm.state is None:
            emit("DONE:0:0:0:0")
            sys.exit(2)

        self._cleanup_active()
        self._cleanup_launching()
        self._final_cleanup()

        complete = self.sm.count_status("complete")
        escalated = self.sm.count_status("escalated")
        errors = self.sm.count_status("error")
        skipped = self.sm.count_status("skipped")
        emit(f"DONE:{complete}:{escalated}:{errors}:{skipped}")
        sys.exit(2)

    def _cleanup_active(self):
        """Terminate active processes and reset stories to queued."""
        for story in self.sm.stories_with_status("active"):
            ref = story["ref"]
            pid = story.get("pid")

            # Terminate the subprocess if we have a Popen handle
            with self.proc_lock:
                proc = self.child_procs.get(ref)
            if proc is not None:
                try:
                    proc.terminate()
                except OSError:
                    pass

            # Also try by PID directly
            if pid is not None:
                try:
                    os.kill(pid, signal.SIGTERM)
                except (OSError, ProcessLookupError):
                    pass

        # Brief wait for graceful shutdown
        time.sleep(2)

        # Force kill remaining
        for story in self.sm.stories_with_status("active"):
            ref = story["ref"]
            with self.proc_lock:
                proc = self.child_procs.get(ref)
            if proc is not None:
                try:
                    proc.kill()
                except OSError:
                    pass

            pid = story.get("pid")
            if pid is not None:
                try:
                    os.kill(pid, signal.SIGKILL)
                except (OSError, ProcessLookupError):
                    pass

        # Teardown containers and reset to queued
        for story in self.sm.stories_with_status("active"):
            ref = story["ref"]
            clone_path = story["clonePath"]
            # A resumed story's clone holds the rate-limited run's unpushed
            # work: the container goes, the clone and its marker stay so the
            # next run can still resume from them.
            preserved = self._holds_preserved_work(story)
            self._teardown(clone_path, keep_clone=preserved)
            self.sm.update_story(ref, status="queued", pid=None, holdsPreservedWork=preserved)

    def _cleanup_launching(self):
        """Stop in-flight launches and reset those stories to queued.

        The helper scripts must die before teardown runs: a surviving
        `devcontainer up` grandchild can create a container after its clone has
        already been torn down.

        The join covers the phases launch_procs cannot reach — base resolution
        and integration-branch git work, which run through _run_cmd (15s per
        command) rather than _run_script. launch_story checks `shutting_down`
        around each of them, so a worker caught there exits within one command.
        Should one still outlive the join, _final_cleanup force-removes any
        container recorded against the story.
        """
        with self.proc_lock:
            procs = list(self.launch_procs.values())
            threads = list(self.launch_threads.values())

        for proc in procs:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except (OSError, ProcessLookupError):
                try:
                    proc.kill()
                except OSError:
                    pass

        self._join_threads(threads, timeout=30)

        # A worker that passed its last shutdown check can have spawned its
        # agent and flipped to "active" while we waited — after _cleanup_active
        # already ran. Sweep those before requeuing the rest.
        if self.sm.count_status("active"):
            self._cleanup_active()

        for story in self.sm.stories_with_status("launching"):
            ref = story["ref"]
            clone_path = story["clonePath"]
            self._teardown(clone_path, keep_clone=self._holds_preserved_work(story))
            self.sm.update_story(ref, status="queued")

    @staticmethod
    def _holds_preserved_work(story):
        """True when the story's clone must survive teardown.

        The persisted `holdsPreservedWork` marker says the clone has a branch
        with unpushed commits from a rate-limited run; the container may go,
        the clone stays. The marker is set when a rate-limited exit parks the
        story or escalates it with its retry budget spent, and it is cleared
        wherever the clone is removed. Unlike the per-run `transientRetries`
        budget, which reconcile_with_input resets on every requeue, the marker
        lasts across orchestrator runs -- as the clone on disk does. The
        relaunch resumes from the directory (see launch_story).

        A parked story that is skipped before it relaunches -- a failed
        dependency, a cascade skip, or a blocked integration branch -- never ran
        again, so its clone still holds that work. The marker stays set, and
        the clone is kept for a human to recover, in whatever status the story
        ends up in.
        """
        return bool(story.get("holdsPreservedWork"))

    def _teardown(self, clone_path, container_id=None, keep_clone=False):
        """Stop the clone's containers and remove the clone. Returns True on success.

        `keep_clone` removes the containers but leaves the directory in place —
        for a clone that holds unpushed work a later relaunch resumes from.

        Failures are logged, never swallowed: a silent teardown failure leaks a
        multi-GB container and lets a retry reuse a dirty clone.
        """
        if self.dry_run:
            log(f"Dry run: kept {clone_path} (container {container_id or self._recorded_container(clone_path)})")
            return True
        timeout = int(os.environ.get("ORCHESTRATE_TEARDOWN_TIMEOUT", "180"))
        cmd = [str(self.script_dir / "teardown.sh"), clone_path]
        if keep_clone:
            cmd.append("--keep-clone")
        ok = True
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=timeout,
            )
            if result.returncode != 0:
                ok = False
                log(f"WARNING: teardown.sh failed for {clone_path} "
                    f"(exit {result.returncode}): {(result.stderr or '').strip()}")
        except subprocess.TimeoutExpired:
            ok = False
            log(f"WARNING: teardown.sh timed out after {timeout}s for {clone_path}")
        except OSError as e:
            ok = False
            log(f"WARNING: teardown.sh could not be run for {clone_path}: {e}")

        if not ok:
            ok = self._force_remove_containers(clone_path, container_id)
        return ok

    def _recorded_container(self, clone_path):
        """The container id recorded for a clone, for log lines."""
        for story in self.sm.state["stories"]:
            if story.get("clonePath") == clone_path:
                return story.get("containerId")
        return None

    def _force_remove_containers(self, clone_path, container_id=None):
        """Last-resort `docker rm -f` for a clone's containers. Returns True if none remain."""
        label_filter = f"label=devcontainer.local_folder={os.path.abspath(clone_path)}"

        def bound_containers():
            # check=True: a failed query (daemon down, access denied) must not
            # be mistaken for "no containers bound".
            result = subprocess.run(
                ["docker", "ps", "-aq", "--filter", label_filter],
                capture_output=True, text=True, timeout=15, check=True,
            )
            return [c for c in (result.stdout or "").split() if c]

        try:
            ids = bound_containers()
            if container_id and container_id not in ids:
                ids.append(container_id)
            for cid in ids:
                result = subprocess.run(
                    ["docker", "rm", "-f", cid],
                    capture_output=True, text=True, timeout=30,
                )
                if result.returncode == 0:
                    log(f"Force-removed container {cid} for {clone_path}")
                else:
                    log(f"WARNING: docker rm -f {cid} failed (exit {result.returncode}): "
                        f"{(result.stderr or '').strip()}")
            survivors = bound_containers()
            if survivors:
                log(f"WARNING: containers still bound to {clone_path} after force removal: "
                    f"{' '.join(survivors)}")
            return not survivors
        except subprocess.CalledProcessError as e:
            log(f"WARNING: could not query containers for {clone_path} "
                f"(exit {e.returncode}): {(e.stderr or '').strip()}")
            return False
        except (subprocess.TimeoutExpired, OSError) as e:
            log(f"WARNING: could not force-remove containers for {clone_path}: {e}")
            return False

    def _final_cleanup(self):
        """Defense-in-depth: iterate ALL stories and ensure their containers are stopped/removed.

        Per-story teardown happens in check_completions() and _handle_launch_failure(),
        but if any teardown.sh call fails silently, the container survives. This method
        sweeps every story regardless of status as a final safety net. Clones holding
        preserved work are kept (containers still go): after a signal, the next run
        resumes them from the directory.
        """
        if self.dry_run:
            return
        for story in self.sm.state["stories"]:
            clone_path = story.get("clonePath")
            container_id = story.get("containerId")

            # Attempt teardown via the standard script
            if clone_path:
                try:
                    self._teardown(clone_path, keep_clone=self._holds_preserved_work(story))
                except Exception:
                    pass

            # Also force-remove by container ID if available
            if container_id:
                try:
                    result = subprocess.run(
                        ["docker", "rm", "-f", container_id],
                        capture_output=True, text=True, timeout=15,
                    )
                    if result.returncode != 0:
                        stderr = (result.stderr or "").strip()
                        msg = f"WARNING: final cleanup failed to remove container {container_id} (exit {result.returncode})"
                        if stderr:
                            msg = f"{msg}: {stderr}"
                        print(msg, file=sys.stderr)
                except Exception:
                    pass

    def _run_script(self, script_name, *args, timeout=60, ref=None):
        """Run a helper script and return (stdout, returncode).

        stderr is forwarded to the orchestrator's stderr (visible in
        the orchestrator log) so container spin-up progress is observable.

        Runs the script in its own process group and kills the whole group on
        timeout — subprocess.run's timeout only kills the direct child, which
        would leave grandchildren (e.g. `devcontainer up`) running and able to
        create containers after teardown has already happened.

        `ref` registers the process against a story for the duration of the
        call, so a shutdown arriving mid-launch can kill the group it belongs to.
        """
        cmd = [str(self.script_dir / script_name)] + list(args)
        # Spawn and register under the lock: a shutdown landing between the two
        # would leave _cleanup_launching unable to kill this process group, and
        # the surviving `devcontainer up` could create a container after its
        # clone was torn down.
        with self.proc_lock:
            proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=None, text=True,
                start_new_session=True,
            )
            if ref is not None:
                self.launch_procs[ref] = proc
        # A shutdown that finished its sweep before we registered would have
        # missed this process entirely; kill it ourselves.
        if self.shutting_down:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except (OSError, ProcessLookupError):
                try:
                    proc.kill()
                except OSError:
                    pass
        try:
            stdout, _ = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except (OSError, ProcessLookupError):
                proc.kill()
            # Drains the stdout pipe, closes it, and reaps the process —
            # safe now that the process (group) has been killed.
            proc.communicate()
            raise
        finally:
            if ref is not None:
                with self.proc_lock:
                    self.launch_procs.pop(ref, None)
        return stdout, proc.returncode

    def _run_cmd(self, cmd, cwd=None):
        """Run a short git/gh command and return (returncode, stdout, stderr).

        `cwd` selects the working directory, which the integration-branch
        helpers use to run the checkout and merges inside a throwaway
        worktree instead of the operator's checkout. Defaults to the
        process's own directory (the host repo).

        Serialized across launch workers: these commands share the host
        repository's refs, index and FETCH_HEAD, so two concurrent launches
        doing base resolution or integration-branch work would race on git's
        own locks and turn valid launches into failures. The lock makes a hung
        networked command everyone's problem, so the call is bounded by
        RUN_CMD_TIMEOUT_SECONDS and a timeout is reported as a failed command
        rather than held forever.

        Kept separate from _run_script, whose process-group/timeout semantics
        target long-running scripts. Every git/gh call the integration-branch
        helpers make goes through this single monkeypatchable seam.
        """
        with self.git_lock:
            try:
                result = subprocess.run(
                    cmd, cwd=cwd, capture_output=True, text=True,
                    timeout=RUN_CMD_TIMEOUT_SECONDS,
                )
            except subprocess.TimeoutExpired:
                log(f"Command timed out after {RUN_CMD_TIMEOUT_SECONDS}s: {' '.join(cmd)}")
                return 124, "", f"timed out after {RUN_CMD_TIMEOUT_SECONDS}s"
        return result.returncode, result.stdout, result.stderr

    def _resolve_story_branch(self, ref):
        """Return a parent story's branch name, or None if it can't be resolved.

        Never synthesised from the ref — feature-branch slugs are
        agent-invented. Read the story record's `branch` field first; when
        absent, resolve it from the story's PR and cache it back onto the
        record so later calls don't repeat the `gh` lookup.
        """
        story = self.sm.get_story(ref)
        if story is None:
            return None
        branch = story.get("branch")
        if branch:
            return branch
        pr_number = story.get("prNumber")
        if not pr_number:
            return None
        rc, stdout, stderr = self._run_cmd(
            gh_cmd("pr", "view", str(pr_number), "--json", "headRefName", "-q", ".headRefName")
        )
        if rc != 0:
            log(f"Could not resolve branch for story {ref} from PR {pr_number}: {stderr.strip()}")
            return None
        branch = stdout.strip()
        if not branch:
            return None
        self.sm.update_story(ref, branch=branch)
        return branch

    def _block_integration(self, ref, deps, outcome):
        """Block a single story whose integration branch could not be built.

        `outcome` distinguishes a genuine merge conflict between the parents
        (`integration_conflict`) from an operational failure -- an
        unresolvable parent branch, a fetch, checkout, push or PR-create
        failure (`integration_error`). Collapsing the latter into
        `integration_conflict` makes the emitted event claim a conflict that
        never happened, which sends whoever reads it looking for a merge to
        resolve.

        Either way the block only affects this story: dependents cascade-skip,
        but every unrelated queued story stays launchable (next_queued_ref
        only gates on a story's own dependencies).
        """
        self.sm.update_story(
            ref, status="skipped", outcome=outcome, completedAt=utcnow_iso(),
        )
        emit(f"BLOCKED:{ref}:{outcome}:{','.join(str(d) for d in deps)}")
        for skipped_ref in self.sm.cascade_skip(ref):
            emit(f"SKIPPED:{skipped_ref}:dependency_failed:{ref}")

    def _integration_branch_still_valid(self, branch):
        """True when a persisted integration branch still has a live remote
        branch and an OPEN PR to reuse.

        A resume can read a record from a prior run whose branch was deleted
        or whose PR was closed after the fact -- reusing it blindly would
        launch the child story against an orphaned or unmergeable base.
        """
        rc, _, _ = self._run_cmd(["git", "ls-remote", "--exit-code", "--heads", REMOTE, branch])
        if rc != 0:
            return False
        rc, stdout, _ = self._run_cmd(gh_cmd(
            "pr", "list", "--head", branch, "--state", "open",
            "--json", "number", "--jq", ".[0].number",
        ))
        return rc == 0 and bool(stdout.strip())

    def _ensure_integration_branch(self, ref, deps):
        """Build (or reuse) the integration branch merging every parent of a
        multi-parent story. Returns the branch name to use as the story's
        base, or None when the story has fewer than two dependencies (the
        caller keeps its own base selection — single-parent stacking is a
        separate concern) or was blocked.
        """
        if len(deps) < 2:
            return None

        story = self.sm.get_story(ref)
        existing = story.get("integrationBranch") if story else None
        if existing:
            if self._integration_branch_still_valid(existing):
                return existing
            log(
                f"Story {ref}: persisted integration branch {existing} no longer has "
                f"an open PR or remote branch; recreating"
            )

        parent_branches = []
        for dep_ref in deps:
            branch = self._resolve_story_branch(dep_ref)
            if not branch:
                log(f"Story {ref}: could not resolve branch for parent {dep_ref} — blocking")
                self._block_integration(ref, deps, "integration_error")
                return None
            parent_branches.append(branch)

        name = integration_branch_name(self.epic_ref, ref)

        # A single-branch clone's remote.<name>.fetch refspec only tracks the
        # branch it was cloned from, so a plain "git fetch <remote>" silently
        # skips every parent ref that differs from it -- checkout below would
        # then either fail outright or, worse, resolve a stale local
        # <remote>/<parent> ref left over from an earlier fetch. Fetch each
        # parent explicitly and check the result, the same fix rebase-branch's
        # Step 3 and worktree-add.sh apply for the same reason.
        for parent in parent_branches:
            rc, _, stderr = self._run_cmd(
                ["git", "fetch", REMOTE, f"+{parent}:refs/remotes/{REMOTE}/{parent}"]
            )
            if rc != 0:
                log(f"Story {ref}: could not fetch parent branch {parent}: {stderr.strip()}")
                self._block_integration(ref, deps, "integration_error")
                return None

        # Build the branch in a throwaway worktree rather than the process's
        # own checkout. Doing it in place made the outcome depend on the
        # operator's uncommitted work -- git refuses a checkout/merge that
        # would overwrite a locally modified file, and that refusal was
        # reported as a conflict between the parents -- and left HEAD on the
        # integration branch afterwards. A worktree shares the repo's object
        # store and refs, so the branch is still local to the host repo (which
        # launch.sh requires: it clones the container workspace from
        # file://$REPO_ROOT), while HEAD and the working tree stay untouched.
        tmp_root = tempfile.mkdtemp(prefix="orchestrate-integration-")
        worktree = os.path.join(tmp_root, "worktree")
        # --force: an earlier in-place run may have left the branch checked
        # out in the host repo, which would otherwise refuse the -B reset.
        rc, _, stderr = self._run_cmd([
            "git", "worktree", "add", "--force", "-B", name, worktree,
            f"{REMOTE}/{parent_branches[0]}",
        ])
        if rc != 0:
            shutil.rmtree(tmp_root, ignore_errors=True)
            log(f"Story {ref}: could not create integration branch {name}: {stderr.strip()}")
            self._block_integration(ref, deps, "integration_error")
            return None

        built = False
        try:
            built = self._build_integration_branch(ref, deps, name, parent_branches, worktree)
        finally:
            # A failed build leaves nothing behind: the half-built branch goes
            # with the worktree so the next attempt starts clean.
            self._remove_integration_worktree(worktree, tmp_root, None if built else name)

        if not built:
            return None

        self.sm.update_story(ref, integrationBranch=name)
        return name

    def _remove_integration_worktree(self, worktree, tmp_root, branch=None):
        """Drop the throwaway worktree, and the branch it held when the build
        failed. The branch can only be deleted once the worktree holding it
        is gone, so the order matters."""
        self._run_cmd(["git", "worktree", "remove", "--force", worktree])
        self._run_cmd(["git", "worktree", "prune"])
        if branch:
            self._run_cmd(["git", "branch", "-D", branch])
        shutil.rmtree(tmp_root, ignore_errors=True)

    def _build_integration_branch(self, ref, deps, name, parent_branches, worktree):
        """Merge the remaining parents into the integration branch checked out
        in `worktree`, push it, and open its PR. Returns True on success;
        blocks the story and returns False otherwise."""
        for parent in parent_branches[1:]:
            rc, _, stderr = self._run_cmd(
                ["git", "merge", "--no-ff", "--no-edit", f"{REMOTE}/{parent}"], cwd=worktree
            )
            if rc != 0:
                self._run_cmd(["git", "merge", "--abort"], cwd=worktree)
                log(f"Story {ref}: integration branch {name} conflicts merging {parent}")
                self._block_integration(ref, deps, "integration_conflict")
                return False

        # --force-with-lease with no explicit expectation resolves the lease
        # through the remote's configured fetch refspec. A single-branch clone
        # only maps the branch it was cloned from, so git finds no tracking ref
        # for the integration branch and rejects the push as "stale info" even
        # when the parents merged cleanly. Fetch the branch's own ref and state
        # the expectation explicitly: the fetched SHA when the remote has the
        # branch, or the empty string ("must not already exist") when the fetch
        # fails because it does not. The lease still protects a branch someone
        # else advanced; only the ref lookup changes.
        rc, _, _ = self._run_cmd(["git", "fetch", REMOTE, f"+{name}:refs/remotes/{REMOTE}/{name}"])
        expected = ""
        if rc == 0:
            rc, stdout, _ = self._run_cmd(
                ["git", "rev-parse", "--verify", "--quiet", f"refs/remotes/{REMOTE}/{name}"]
            )
            if rc == 0:
                expected = stdout.strip()

        rc, _, stderr = self._run_cmd(
            ["git", "push", "-u", REMOTE, name, f"--force-with-lease={name}:{expected}"]
        )
        if rc != 0:
            log(f"Story {ref}: could not push integration branch {name}: {stderr.strip()}")
            self._block_integration(ref, deps, "integration_error")
            return False

        rc, _, stderr = self._run_cmd(gh_cmd(
            "pr", "create", "--base", self.base_branch, "--head", name,
            # No leading "#<ref>:" — that prefix is taiga_closing_refs.py's
            # claim marker, and this PR belongs to no Taiga item. A claim
            # here would let merging the integration PR mark the child story
            # Done before the child's own PR lands.
            "--title", f"Integration branch for story #{ref}",
            "--body", f"Integration branch merging parents {', '.join(str(d) for d in deps)} for story #{ref}.",
        ))
        if rc != 0:
            # A non-zero exit here can just mean the PR already exists (resume) —
            # confirm that before tolerating it, rather than assuming. Require
            # OPEN specifically: a closed PR on this branch is not one we can
            # reuse, and would otherwise be recorded as if it were live.
            check_rc, stdout, _ = self._run_cmd(gh_cmd(
                "pr", "list", "--head", name, "--state", "open",
                "--json", "number", "--jq", ".[0].number",
            ))
            if check_rc != 0 or not stdout.strip():
                log(f"Story {ref}: could not create integration PR for {name}: {stderr.strip()}")
                self._block_integration(ref, deps, "integration_error")
                return False

        return True

    def _load_stories_file(self):
        """Read the stories JSON file, exiting with an error if it is missing."""
        if not os.path.exists(self.stories_file):
            print(f"Error: stories file not found: {self.stories_file}", file=sys.stderr)
            sys.exit(1)
        with open(self.stories_file) as f:
            return json.load(f)

    def init_or_resume(self):
        """Initialize from stories file or resume from existing state.

        On resume, stories-input is reconciled into the existing state:
        failures are re-queued and completed/PR-bearing stories are left
        alone, so a re-invoke never produces duplicate PRs. To force a full
        from-scratch re-run, delete the state file by hand.
        """
        if self.sm.exists():
            self.sm.load()
            # A file with no `mode` predates the key and is a container run.
            written_by = self.sm.state.get("mode", STATE_MODE)
            if written_by != STATE_MODE:
                log(
                    f"ERROR: {self.sm.state_file} was written by {written_by} mode; "
                    f"re-invoke /implement-epic with n=1 or --worktrees, or delete the file"
                )
                sys.exit(2)
            if bool(self.sm.state.get("dryRun", False)) != self.dry_run:
                written = "a --dry-run" if self.sm.state.get("dryRun", False) else "a real"
                log(
                    f"ERROR: {self.sm.state_file} was written by {written} run; "
                    f"re-invoke {'without' if self.dry_run else 'with'} --dry-run, or delete the file"
                )
                sys.exit(2)
            log("Resuming from existing state file")
            # Use the stored concurrency to avoid mid-run surprises if
            # the caller passes a different -n value on resume.
            stored_concurrency = self.sm.state.get("concurrency", self.concurrency)
            if stored_concurrency != self.concurrency:
                log(
                    f"Ignoring CLI concurrency={self.concurrency}; "
                    f"using stored concurrency={stored_concurrency}"
                )
                self.concurrency = stored_concurrency

            # baseBranch is stored at init time so a resume with BASE_BRANCH
            # unset (or set differently) still targets the run's real base
            # rather than silently falling back to "main" for new/requeued
            # stories and their PRs.
            stored_base_branch = self.sm.state.get("baseBranch", self.base_branch)
            if stored_base_branch != self.base_branch:
                log(
                    f"Ignoring BASE_BRANCH={self.base_branch}; "
                    f"using stored baseBranch={stored_base_branch}"
                )
                self.base_branch = stored_base_branch
            # Backfill the key for state files written before it existed, so
            # the next resume has a stored value to prefer over the env.
            self.sm.state["baseBranch"] = stored_base_branch

            stories = self._load_stories_file()
            requeued, added = self.sm.reconcile_with_input(stories, self.base_dir)
            for ref in requeued:
                emit(f"REQUEUED:{ref}")
            for ref in added:
                emit(f"ADDED:{ref}")
            self._resume()
            self._skip_dependency_bearing_for_dry_run()
            return

        # Fresh init
        stories = self._load_stories_file()
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.sm.init_from_stories(
            self.epic_ref, self.concurrency, stories, self.base_dir, self.base_branch,
            dry_run=self.dry_run,
        )
        total = len(stories)
        emit(f"INIT:{self.epic_ref}:{total}:{self.concurrency}")
        self._skip_dependency_bearing_for_dry_run()

    def _skip_dependency_bearing_for_dry_run(self):
        """A dry run only smokes dependency-free stories: stacking on a parent
        needs a real branch (and a multi-parent story an integration branch),
        and a dry run pushes nothing."""
        if not self.dry_run:
            return
        for story in self.sm.stories_with_status("queued"):
            if story.get("dependencies"):
                self.sm.update_story(
                    story["ref"], status="skipped", outcome="dry_run_dependency",
                    completedAt=utcnow_iso(),
                )
                emit(f"SKIPPED:{story['ref']}:dry_run_dependency")

    def _resume(self):
        """Handle resumption: check active PIDs, reset dead ones."""
        # Check active stories
        for story in list(self.sm.stories_with_status("active")):
            ref = story["ref"]
            pid = story.get("pid")

            if pid is not None and is_agent_process(pid):
                log(f"Story {ref} still active (PID {pid})")
                continue

            log(f"Story {ref} PID {pid} is dead, checking outcome")
            # Checked before determine_outcome(), which deletes the in-container
            # story state file that a rate-limited relaunch resumes from.
            if self._handle_transient_exit(story):
                continue

            outcome, pr, esc_reason, branch = determine_outcome(
                ref, story.get("logFile"), story.get("containerId"),
                state_dir=str(self.state_dir),
            )

            if esc_reason:
                log(f"Story {ref} exited with non-terminal phase — marking as escalated ({esc_reason})")

            # Guard: "complete" without a PR means no meaningful work was done.
            # Treat as terminal error (not a retry) to prevent cascade-unblocking dependents.
            no_pr_downgrade = outcome == "complete" and pr == "none"
            if no_pr_downgrade:
                log(f"Story {ref} reported complete but created no PR — marking as error")
                outcome = "error"

            if outcome != "error":
                self._finish_story(story, outcome, pr, branch=branch)
            elif no_pr_downgrade:
                # Terminal error: agent exited claiming completion without creating a PR.
                # Do not retry — mark as error and cascade-skip dependents.
                self._finish_story(story, "error", pr, outcome_label="no_pr_created")
            else:
                # Transient error (e.g., process crashed): reset to queued for retry
                preserved = self._holds_preserved_work(story)
                self._teardown(story["clonePath"], keep_clone=preserved)
                self.sm.update_story(ref, status="queued", pid=None, holdsPreservedWork=preserved)
                log(f"Story {ref} reset to queued")

        # Check launching stories
        for story in list(self.sm.stories_with_status("launching")):
            ref = story["ref"]
            self._teardown(story["clonePath"], keep_clone=self._holds_preserved_work(story))
            self.sm.update_story(ref, status="queued")
            log(f"Story {ref} (launching) reset to queued")

        active = self.sm.count_status("active")
        queued = self.sm.count_status("queued")
        complete = self.sm.count_status("complete")
        escalated = self.sm.count_status("escalated")
        errors = self.sm.count_status("error")
        emit(f"RESUMED:{self.epic_ref}:{active}:{queued}:{complete}:{escalated}:{errors}")

    def _handle_launch_failure(self, ref, clone_path, reason, container_id=None, keep_clone=False):
        """Handle a launch failure: retry if under limit, otherwise mark as terminal error.

        `keep_clone` is set when the failed launch was a resume over a preserved
        clone: its unpushed work must survive for the retry (or a human).
        """
        story = self.sm.get_story(ref)
        attempts = story.get("launchAttempts", 0) + 1
        container_id = container_id or story.get("containerId")
        cleaned = self._teardown(clone_path, container_id, keep_clone=keep_clone)

        if not cleaned:
            # A container may still be bound to this clone. Retrying would bind
            # a second container to the same folder (the original bug), so the
            # story is terminal and the clone is kept for diagnosis.
            log(f"Cleanup failed for story {ref}; not retrying, clone kept at {clone_path}")
            reason = f"{reason}; cleanup failed, container may still be running"
        elif not keep_clone and not self._ensure_clone_absent(clone_path):
            # _ensure_clone_absent refused to remove a git worktree. Retrying
            # would leave launch.sh's `resuming` check finding the directory
            # still present and skipping checkout, running the agent against
            # whatever branch the worktree happened to hold instead of the
            # resolved base. Treat this the same as a failed cleanup: terminal,
            # clone kept for diagnosis.
            cleaned = False
            log(f"Could not remove worktree clone for story {ref}; not retrying, clone kept at {clone_path}")
            reason = f"{reason}; clone is a git worktree, refusing to remove it"

        if cleaned and attempts < MAX_LAUNCH_ATTEMPTS:
            log(f"Launch attempt {attempts}/{MAX_LAUNCH_ATTEMPTS} failed for story {ref}, will retry")
            self.sm.update_story(ref, status="queued", launchAttempts=attempts)
        else:
            log(f"Launch failed for story {ref} after {attempts} attempts, marking as error")
            extra = {"containerId": container_id} if container_id else {}
            self.sm.update_story(
                ref, status="error", outcome="launch_failed",
                completedAt=utcnow_iso(), launchAttempts=attempts,
                **extra,
            )

        emit(f"LAUNCH_FAIL:{ref}:{reason}")

        # Cascade skip dependents if this story reached terminal error
        story = self.sm.get_story(ref)
        if story and story["status"] == "error":
            skipped = self.sm.cascade_skip(ref)
            for skipped_ref in skipped:
                emit(f"SKIPPED:{skipped_ref}:dependency_failed:{ref}")

        return False

    def _ensure_clone_absent(self, clone_path):
        """Guarantee a launch retry re-clones: launch.sh reuses any directory it
        finds, which binds a second container to the failed attempt's clone.
        Moves the clone aside when it cannot be removed (e.g. a live bind mount).
        Refuses to touch a git worktree (its .git is a file, not a directory) —
        removing one directly would defeat teardown.sh's own worktree refusal.

        Returns True when `clone_path` is confirmed absent (or was never
        present) and False when something was left behind — the worktree
        case — so callers can tell a retry is not actually safe.
        """
        if not os.path.exists(clone_path):
            return True
        if os.path.isfile(os.path.join(clone_path, ".git")):
            log(f"WARNING: {clone_path} is a git worktree; refusing to remove it. "
                f"Remove it with 'git worktree remove' instead.")
            return False
        try:
            shutil.rmtree(clone_path)
        except OSError as e:
            stale = f"{clone_path}.stale-{int(time.time())}"
            os.rename(clone_path, stale)
            log(f"WARNING: could not remove {clone_path} ({e}); "
                f"moved aside to {stale} so the retry re-clones")
        return True

    def _pr_number_value(self, ref, pr):
        """Convert determine_outcome's "none"-or-digits pr string to an int or None."""
        try:
            return None if pr == "none" else int(pr)
        except (ValueError, TypeError):
            log(f"Story {ref}: non-integer prNumber '{pr}', treating as None")
            return None

    def _finish_story(self, story, outcome, pr, outcome_label=None, keep_clone=False, branch=None):
        """Move a story to its terminal state: persist, tear down, emit, cascade.

        `outcome` is what COMPLETED reports and what decides the status (a
        success outcome maps to status "complete" so dependents unblock; the
        distinct label is preserved in state) and whether dependents are
        cascade-skipped. `outcome_label` is the stored outcome when it differs
        from the reported one (e.g. no_pr_created, rate_limit_exhausted).
        `branch` is the story's feature branch, when known. The clone is kept for
        the error and escalated outcomes whatever `keep_clone` says.
        """
        ref = story["ref"]
        status = "complete" if outcome in SUCCESS_OUTCOMES else outcome
        # An agent that exited in error or escalated leaves unpushed commits
        # and uncommitted edits in its clone. error and PR-less escalated
        # stories are re-queued by the next run, and the relaunch resumes over
        # that directory (launch_story), so deleting it here would throw away
        # exactly the work the retry exists to recover.
        if outcome in ("error", "escalated"):
            keep_clone = True
        self.sm.update_story(
            ref,
            status=status, outcome=outcome_label or outcome,
            completedAt=utcnow_iso(), prNumber=self._pr_number_value(ref, pr), pid=None,
            branch=branch, holdsPreservedWork=keep_clone,
        )
        self._teardown(story["clonePath"], keep_clone=keep_clone)
        self.child_procs.pop(ref, None)
        emit(f"COMPLETED:{ref}:{outcome}:{pr}")
        if outcome in ("error", "escalated"):
            for skipped_ref in self.sm.cascade_skip(ref):
                emit(f"SKIPPED:{skipped_ref}:dependency_failed:{ref}")

    def _handle_transient_exit(self, story):
        """Requeue a story whose agent was killed by a provider rate limit.

        Returns True when the exit was transient and has been handled: either
        requeued with a notBefore (clone and container kept, so the relaunch
        resumes in place) or, once the retry budget is spent, escalated with
        the clone kept — its unpushed work is exactly the work a teardown would lose, so a
        human recovers it rather than the orchestrator deleting it — and
        dependents cascade-skipped. Any PR the agent had already created is
        recorded so a resume never relaunches it into a duplicate. Returns
        False for a non-transient exit so the caller runs the normal outcome
        path.
        """
        ref = story["ref"]
        transient, not_before = detect_transient_exit(story.get("logFile"))
        if not transient:
            return False
        self.child_procs.pop(ref, None)
        attempts = story.get("transientRetries", 0) + 1
        if attempts <= MAX_TRANSIENT_RETRIES:
            log(f"Story {ref} hit a provider rate limit — requeued until {not_before} "
                f"(attempt {attempts}/{MAX_TRANSIENT_RETRIES}); clone and container kept")
            self.sm.update_story(
                ref, status="queued", pid=None,
                notBefore=not_before, transientRetries=attempts, holdsPreservedWork=True,
            )
            emit(f"RATE_LIMITED:{ref}:{not_before}")
            return True
        # Read the story state before teardown destroys it: the phase
        # classification is not used here, but the branch is — it is what
        # lets a human (or a later dependent) find the escalated story's work.
        _, pr, _, branch = determine_outcome(ref, story.get("logFile"), story.get("containerId"),
                                             state_dir=str(self.state_dir))
        log(f"Story {ref} hit the rate limit {attempts} times — escalating, clone kept at "
            f"{story['clonePath']}")
        self._finish_story(story, "escalated", pr, outcome_label="rate_limit_exhausted",
                           keep_clone=True, branch=branch)
        return True

    def _running_container(self, container_id):
        """Return `container_id` if Docker reports it running, else None."""
        if not container_id:
            return None
        try:
            result = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.Running}}", container_id],
                capture_output=True, text=True, timeout=15,
            )
        except (subprocess.TimeoutExpired, OSError) as e:
            log(f"Could not inspect container {container_id}: {e}")
            return None
        if result.returncode == 0 and result.stdout.strip() == "true":
            return container_id
        log(f"Container {container_id} is not running; launching a new one")
        return None

    def _launch_container(self, ref, clone_path, base):
        """Run launch.sh and return (container_id, failure_reason).

        Exactly one of the pair is None. The reason is returned rather than
        stashed on self: launches run concurrently, so a shared attribute would
        let one story's failure be reported under another story's ref.

        `base` is the story's resolved base branch (the run's root base branch
        for a story with no dependencies, a completed dependency's branch for
        a single-parent story, or the integration branch for a multi-parent
        one) — never mutate `self.base_branch` here, it is the run's root
        base and must stay intact for later stories.

        A cold launch (no pre-built image yet) builds the image and runs
        post-start.sh, which starts the container's own Supabase stack, so it
        can take well over 10 minutes.
        """
        try:
            launch_out, launch_rc = self._run_script(
                "launch.sh", clone_path, base, timeout=self.launch_timeout, ref=ref,
            )
        except subprocess.TimeoutExpired:
            log(f"Launch timed out for story {ref} after {self.launch_timeout}s")
            return None, f"devcontainer launch timed out after {self.launch_timeout}s"
        except OSError as e:
            log(f"Launch failed for story {ref}: {e}")
            return None, "devcontainer launch failed"

        if launch_rc != 0:
            log(f"Launch failed for story {ref}: exit code {launch_rc}")
            return None, "devcontainer launch failed"

        for line in launch_out.split("\n"):
            if line.startswith("CONTAINER_ID="):
                container_id = line.split("=", 1)[1].strip()
                if container_id:
                    return container_id, None
        log(f"No CONTAINER_ID in launch output for story {ref}")
        return None, "no container ID returned"

    def launch_story(self, ref):
        """Launch a story into a devcontainer slot, resuming in place when the
        story's clone was preserved by an earlier rate-limited exit.

        Runs on a launch worker thread; fill_slots has already reserved the slot
        by setting the story to "launching". Bails between blocking steps once a
        shutdown starts, leaving the story in "launching" for _cleanup_launching
        to tear down and requeue.
        """
        story = self.sm.get_story(ref)
        clone_path = story["clonePath"]
        log_file = story["logFile"]

        if self.shutting_down:
            return False

        # A clone directory that exists here holds preserved work (a rate-limited
        # agent's branch and commits): every path that abandons a clone removes
        # it, and launch.sh only clones into an absent directory.
        resuming = os.path.isdir(clone_path)

        # Resolve the story's base branch from the dependency graph: a story
        # with no dependencies bases off the run's root base branch, one with
        # a completed dependency stacks on that dependency's branch, and one
        # with 2+ dependencies gets an integration branch merging every parent.
        # The integration branch is the whole answer for a multi-parent story —
        # base_selector's single-parent choice is never consulted for it.
        deps = story.get("dependencies", [])
        if len(deps) >= 2:
            # A conflict or unresolvable parent blocks only this story.
            base = self._ensure_integration_branch(ref, deps)
            if base is None:
                # The block already flipped the story to "skipped", so stop the
                # container bound to the clone now rather than leaving it for
                # final cleanup. A story that was not resuming has no clone
                # directory yet and the teardown is a no-op. The clone itself
                # survives the rest of the run: _holds_preserved_work counts a
                # skipped story with transient retries as preserved work.
                self._teardown(clone_path, keep_clone=resuming)
                return False
        else:
            story_branches = {
                s["ref"]: s.get("branch")
                for s in self.sm.state["stories"]
                if s.get("status") == "complete" and s.get("branch")
            }
            try:
                base, _reason = select_base_branch(
                    ref, deps, story_branches, self.base_branch,
                )
            except UnresolvedDependencyError as e:
                failed_dep = next(
                    (d for d in deps
                     if not story_branches.get(d) and not story_branches.get(str(d))),
                    None,
                )
                log(f"Story {ref}: {e}")
                # `resuming` means clone_path holds preserved work (see the comment
                # above): the teardown removes the container now, and the clone
                # survives final cleanup because _holds_preserved_work counts a
                # skipped story with transient retries as preserved work.
                self._teardown(clone_path, keep_clone=resuming)
                self.sm.update_story(
                    ref, status="skipped", outcome="dependency_failed", completedAt=utcnow_iso(),
                )
                emit(f"SKIPPED:{ref}:dependency_failed:{failed_dep}")
                for skipped_ref in self.sm.cascade_skip(ref):
                    emit(f"SKIPPED:{skipped_ref}:dependency_failed:{ref}")
                return False
        self.sm.update_story(ref, baseBranch=base)

        if self.shutting_down:
            return False

        if not self._ensure_base_reachable(base):
            # A shutdown can abort the fetch mid-call; that is not the story's
            # fault, so bail without spending a launch attempt.
            if self.shutting_down:
                return False
            return self._handle_launch_failure(
                ref, clone_path, f"base branch {base} not reachable", keep_clone=resuming)

        # Reuse the recorded container when it is still running: `devcontainer up`
        # cannot adopt it (launch.sh writes a fresh --config path each call, and
        # the CLI discards a folder match whose config_file label differs), so
        # re-running launch.sh would bind a second container to the same folder.
        container_id = self._running_container(story.get("containerId")) if resuming else None
        if container_id:
            log(f"Story {ref}: resuming in container {container_id}")
        else:
            if resuming:
                # Whatever is still bound to the folder (a stopped container after
                # a host restart) would sit next to the new one; sweep it first.
                if not self._teardown(clone_path, story.get("containerId"), keep_clone=True):
                    # A leftover container would sit next to the new one on the
                    # same folder; launch nothing until the sweep succeeds.
                    # _handle_launch_failure retries the cleanup and makes the
                    # story terminal if it still cannot be confirmed.
                    log(f"Story {ref}: could not remove the previous container; not relaunching")
                    return self._handle_launch_failure(
                        ref, clone_path, "previous container cleanup failed",
                        story.get("containerId"), keep_clone=True)
            container_id, launch_reason = self._launch_container(ref, clone_path, base)
            if not container_id:
                # A shutdown killed launch.sh's process group; that is not the
                # story's fault, so bail without spending a launch attempt.
                if self.shutting_down:
                    return False
                return self._handle_launch_failure(
                    ref, clone_path, launch_reason, keep_clone=resuming)

        if self.shutting_down:
            return False

        # Prepare clone (writes settings.local.json and .mcp.json). Skipped on a
        # resume: the preserved clone already has those files, and
        # prepare-clone.sh's clean-tree step (git checkout HEAD) would discard
        # edits the agent had not yet committed.
        if not resuming:
            try:
                _, prep_rc = self._run_script("prepare-clone.sh", clone_path, ref=ref)
            except (subprocess.TimeoutExpired, OSError) as e:
                log(f"prepare-clone.sh failed for story {ref}: {e}")
                return self._handle_launch_failure(ref, clone_path, "prepare-clone failed")

            # As above: a shutdown killed prepare-clone.sh, so don't spend an attempt.
            if self.shutting_down:
                return False

            if prep_rc != 0:
                log(f"prepare-clone.sh failed for story {ref}")
                return self._handle_launch_failure(ref, clone_path, "prepare-clone failed")

        # The skill expansion (frontmatter strip, $ARGUMENTS substitution) and
        # the in-container `claude` invocation live in exec-skill.sh, which also
        # writes the stream-json log. A resume is signalled through the
        # arguments, since the in-container state file the skill would
        # otherwise detect does not survive a container loss.
        skill_path = Path(clone_path) / ".claude/skills/fully-implement-story/SKILL.md"
        if not self.dry_run and not skill_path.is_file():
            log(f"Could not read skill for story {ref}: {skill_path} not found")
            return self._handle_launch_failure(
                ref, clone_path, "skill file not found", keep_clone=resuming)

        if self.shutting_down:
            return False

        skill_args = f"{ref} {base} --resume" if resuming else f"{ref} {base}"
        if self.dry_run:
            # Smoke the container's stack instead of running the agent; the
            # output goes to the story's log file.
            cmd = [str(self.script_dir / "exec.sh"), clone_path, SMOKE_CMD]
            launcher = "exec.sh"
        else:
            cmd = [
                str(self.script_dir / "exec-skill.sh"), clone_path,
                "fully-implement-story", skill_args, "--log", log_file,
            ]
            launcher = "exec-skill.sh"
        try:
            if self.dry_run:
                with open(log_file, "w") as smoke_log:
                    proc = subprocess.Popen(
                        cmd,
                        stdin=subprocess.DEVNULL,
                        stdout=smoke_log,
                        stderr=subprocess.STDOUT,
                        start_new_session=True,
                    )
            else:
                proc = subprocess.Popen(
                    cmd,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
        except OSError as e:
            log(f"Failed to start {launcher} for story {ref}: {e}")
            return self._handle_launch_failure(
                ref, clone_path, "agent start failed", container_id, keep_clone=resuming)

        with self.proc_lock:
            self.child_procs[ref] = proc
        self.sm.update_story(
            ref, status="active", pid=proc.pid,
            containerId=container_id, startedAt=utcnow_iso(),
        )
        emit(f"LAUNCH_OK:{ref}:{proc.pid}")
        return True

    def _launch_worker(self, ref):
        """Thread body for a launch: always deregisters itself when done."""
        try:
            self.launch_story(ref)
        except Exception as e:  # a crashed thread must not leave the slot wedged
            log(f"Unexpected error launching story {ref}: {e}")
            story = self.sm.get_story(ref)
            self._handle_launch_failure(
                ref, story["clonePath"], "unexpected launch error",
                keep_clone=self._holds_preserved_work(story),
            )
        finally:
            # A failed launch requeues the ref before we get here, so the poll
            # loop may already have started a retry and registered its thread.
            # Only clear the entry if it is still ours.
            with self.proc_lock:
                if self.launch_threads.get(ref) is threading.current_thread():
                    del self.launch_threads[ref]

    def check_completions(self):
        """Check active stories for process completion."""
        for story in list(self.sm.stories_with_status("active")):
            ref = story["ref"]
            pid = story.get("pid")

            # Check via Popen handle first, then PID
            with self.proc_lock:
                proc = self.child_procs.get(ref)
            if proc is not None:
                if proc.poll() is None:
                    continue  # Still running
            elif pid is not None and is_agent_process(pid):
                continue  # Still running

            if self.dry_run:
                self._finish_dry_run_story(story, proc)
                continue

            # Process exited. A provider rate limit is transient: requeue the
            # story to relaunch after the limit resets. This runs before
            # determine_outcome(), which deletes the in-container story state
            # file that the relaunched agent resumes from.
            if self._handle_transient_exit(story):
                continue

            outcome, pr, esc_reason, branch = determine_outcome(
                ref, story.get("logFile"), story.get("containerId"),
                state_dir=str(self.state_dir),
            )

            if esc_reason:
                log(f"Story {ref} exited with non-terminal phase — marking as escalated ({esc_reason})")

            # Guard: a "complete" outcome without a PR means the agent exited
            # without doing meaningful work (e.g., blocked by permission denials).
            # Downgrade to error so dependents aren't falsely unblocked.
            no_pr_downgrade = outcome == "complete" and pr == "none"
            if no_pr_downgrade:
                log(f"Story {ref} reported complete but created no PR — marking as error")
                outcome = "error"

            self._finish_story(
                story, outcome, pr,
                outcome_label="no_pr_created" if no_pr_downgrade else None,
                branch=branch,
            )

    def _finish_dry_run_story(self, story, proc):
        """Record a dry-run story's smoke result. Nothing is torn down."""
        ref = story["ref"]
        rc = proc.returncode if proc is not None else 1
        outcome = "dry_run_ok" if rc == 0 else "dry_run_failed"
        self.sm.update_story(
            ref,
            status="complete" if rc == 0 else "error", outcome=outcome,
            completedAt=utcnow_iso(), pid=None,
        )
        self._teardown(story["clonePath"], story.get("containerId"), keep_clone=True)
        self.child_procs.pop(ref, None)
        emit(f"COMPLETED:{ref}:{outcome}:none")

    def fill_slots(self, now=None):
        """Fill empty concurrency slots with queued stories.

        Launches nothing while a rate limit is in force (some story is parked
        with a future notBefore): the limit is account-wide, so a fresh story
        would only pay a cold launch to be parked in turn.

        A launch takes ~10 minutes, so it runs on its own thread: the poll loop
        keeps reaping completions and can start other launches meanwhile. The
        slot is reserved here (status="launching") before the thread starts, so
        concurrency is never exceeded.
        """
        now = now or utcnow_iso()
        paused_until = self.sm.rate_limited_until(now)
        if paused_until:
            if paused_until != self._paused_until_logged:
                log(f"Rate limit in force — holding all launches until {paused_until}")
                self._paused_until_logged = paused_until
            return
        started_this_cycle = set()
        while not self.shutting_down:
            occupied = self.sm.count_status("active") + self.sm.count_status("launching")
            if occupied >= self.concurrency:
                break

            next_ref = self.sm.next_queued_ref(now)
            if next_ref is None:
                break

            # A launch that fails fast requeues its ref from the worker thread
            # while this loop is still running. Retrying it here would let one
            # flaky story burn all its launch attempts in a single cycle and
            # keep other queued stories out of the slot; leave it to the next poll.
            if next_ref in started_this_cycle:
                break
            started_this_cycle.add(next_ref)

            story = self.sm.get_story(next_ref)
            self.sm.update_story(next_ref, status="launching")
            emit(f"LAUNCHING:{next_ref}:{story['subject']}")

            t = threading.Thread(
                target=self._launch_worker, args=(next_ref,), daemon=True,
            )
            with self.proc_lock:
                self.launch_threads[next_ref] = t
            t.start()

            active = self.sm.count_status("active")
            queued = self.sm.count_status("queued")
            emit(f"SLOT_FILLED:{active}:{queued}")

    def is_done(self):
        """Check if all stories have reached a terminal state."""
        active = self.sm.count_status("active")
        queued = self.sm.count_status("queued")
        launching = self.sm.count_status("launching")
        return active == 0 and queued == 0 and launching == 0

    def run(self):
        """Main orchestration loop."""
        self.setup_signals()
        self.init_or_resume()

        poll_interval = int(os.environ.get("POLL_INTERVAL", "30"))

        while not self.shutting_down:
            self.check_completions()
            self.fill_slots()

            if self.is_done():
                break

            active = self.sm.count_status("active")
            queued = self.sm.count_status("queued")
            emit(f"WAITING:{active}:{queued}")
            time.sleep(poll_interval)

        with self.proc_lock:
            threads = list(self.launch_threads.values())
        self._join_threads(threads, timeout=10)

        # Ensure all containers are stopped before declaring completion
        self._final_cleanup()

        # Finalize
        self.sm.set_phase("complete")
        complete = self.sm.count_status("complete")
        escalated = self.sm.count_status("escalated")
        errors = self.sm.count_status("error")
        skipped = self.sm.count_status("skipped")
        emit(f"DONE:{complete}:{escalated}:{errors}:{skipped}")


def main():
    usage = "Usage: orchestrate_epic.py <epic-ref> <concurrency> <stories-json-file> [--dry-run]"
    extra = sys.argv[4:]
    if len(sys.argv) < 4 or any(a != "--dry-run" for a in extra):
        print(usage, file=sys.stderr)
        sys.exit(1)
    dry_run = "--dry-run" in extra

    epic_ref = sys.argv[1]
    try:
        concurrency = int(sys.argv[2])
    except ValueError:
        print(
            f"Error: concurrency must be a positive integer, got '{sys.argv[2]}'",
            file=sys.stderr,
        )
        sys.exit(1)

    if not epic_ref.isdigit():
        print(f"Error: epic-ref must be a positive integer, got '{epic_ref}'", file=sys.stderr)
        sys.exit(1)
    if concurrency < 1 or concurrency > 5:
        print(f"Error: concurrency must be 1-5, got '{concurrency}'", file=sys.stderr)
        sys.exit(1)

    stories_file = sys.argv[3]

    # Allow overriding script and state directories and the host repo for testing
    script_dir = os.environ.get(
        "ORCHESTRATE_SCRIPT_DIR",
        os.path.dirname(os.path.abspath(__file__)),
    )
    state_dir = os.environ.get("ORCHESTRATE_STATE_DIR", "/tmp")
    base_branch = os.environ.get("BASE_BRANCH", "main")
    repo_root = os.environ.get("ORCHESTRATE_REPO_ROOT")

    orchestrator = Orchestrator(
        epic_ref, concurrency, stories_file, script_dir, state_dir, base_branch,
        repo_root=repo_root, dry_run=dry_run,
    )
    try:
        orchestrator.run()
    except CircularDependencyError as exc:
        log(f"ERROR: {exc}")
        log("Fix the epic's dependency diagram and re-invoke; no stories were launched.")
        sys.exit(1)


if __name__ == "__main__":
    main()
