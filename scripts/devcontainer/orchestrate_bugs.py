# ABOUTME: Deterministic state machine for parallel bug fixing across devcontainers.
# ABOUTME: Manages launch/monitor/teardown lifecycle; emits machine-parseable events on stdout.
"""
Usage: python3 orchestrate_bugs.py <concurrency> <bugs-json-file>

Each bug runs in its own devcontainer (launch.sh), with its own Docker-in-Docker
daemon and its own local Supabase stack, and the bug agent is started through
exec-skill.sh. Nothing here touches a hosted Supabase project.

Environment variables:
  ORCHESTRATE_REMOTE      Git remote that holds the base branch (default: upstream)
  GH_REPO                 GitHub repository every gh call targets
                          (default: MrsBohemian/Cholobots)
  ORCHESTRATE_SCRIPT_DIR  Override directory containing launch.sh, prepare-clone.sh,
                          exec-skill.sh and teardown.sh
  ORCHESTRATE_STATE_DIR   Override directory for the bug state file and the host-side
                          per-bug fix-and-remediate-<ref>.json files (default: /tmp)
  POLL_INTERVAL           Seconds between poll cycles (default: 30)
  BASE_BRANCH             Branch to clone for each bug devcontainer and open PRs against (default: main);
                          fetched from the remote before each launch and passed to the worker
  ORCHESTRATE_LAUNCH_TIMEOUT  Seconds allowed for launch.sh before giving up
                          (default: 1800 — a cold Docker-in-Docker launch pulls
                          images and provisions services, which exceeds 600s)
"""

import json
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path


MAX_LAUNCH_ATTEMPTS = 3

# Git remote and GitHub repository the orchestrator works against, as in
# orchestrate_epic.py: PRs live in MrsBohemian/Cholobots, whose remote is
# `upstream` (`origin` is only a mirror).
REMOTE = os.environ.get("ORCHESTRATE_REMOTE", "upstream")
GH_REPO = os.environ.get("GH_REPO", "MrsBohemian/Cholobots")

# The state file records which mode wrote it so a resume never mixes the
# container orchestrator with the worktree slot loop.
STATE_MODE = "container"


def gh_cmd(*args):
    """Build a gh argv. The shell's GH_TOKEN cannot see the repo, so it is
    dropped, and every call names the repo explicitly."""
    return ["env", "-u", "GH_TOKEN", "gh", *args, "--repo", GH_REPO]


def utcnow_iso():
    """Return current UTC time in ISO 8601 format."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def emit(event):
    """Write a machine-parseable event to stdout."""
    print(event, flush=True)


def log(msg):
    """Write a log message to stderr."""
    print(f"[orchestrate-bugs] {msg}", file=sys.stderr, flush=True)


class StateManager:
    """Manages the orchestrator state file with atomic writes."""

    def __init__(self, state_file):
        self.state_file = state_file
        self._state = None
        # Launches run on worker threads, so the poll loop and several launches
        # read and write this state concurrently. RLock, not Lock: count_status
        # calls bugs_with_status and update_bug calls save.
        self._lock = threading.RLock()

    @property
    def state(self):
        return self._state

    def load(self):
        """Load state from disk."""
        with self._lock:
            with open(self.state_file) as f:
                self._state = json.load(f)
            return self._state

    def save(self):
        """Save state to disk atomically (write temp + rename)."""
        with self._lock:
            tmp = f"{self.state_file}.tmp.{os.getpid()}"
            with open(tmp, "w") as f:
                json.dump(self._state, f, indent=2)
            os.rename(tmp, self.state_file)

    def exists(self):
        return os.path.exists(self.state_file)

    def init_from_bugs(self, concurrency, bugs, base_dir, base_branch="main"):
        """Initialize state from a list of bug dicts."""
        with self._lock:
            self._state = {
                "mode": STATE_MODE,
                "concurrency": concurrency,
                "baseBranch": base_branch,
                "phase": "running",
                "baseDir": str(base_dir),
                "bugs": [
                    {
                        "ref": b["ref"],
                        "id": b["id"],
                        "subject": b["subject"],
                        "status": "queued",
                        "clonePath": str(base_dir / f"bug-{b['ref']}"),
                        "pid": None,
                        "logFile": str(base_dir / f"bug-{b['ref']}.log"),
                        "startedAt": None,
                        "completedAt": None,
                        "outcome": None,
                        "prNumber": None,
                        "containerId": None,
                        "launchAttempts": 0,
                    }
                    for b in bugs
                ],
            }
            self.save()

    def update_bug(self, ref, **kwargs):
        """Update fields on a bug by ref number."""
        with self._lock:
            for bug in self._state["bugs"]:
                if bug["ref"] == ref:
                    bug.update(kwargs)
                    break
            self.save()

    def get_bug(self, ref):
        """Get a bug dict by ref number."""
        with self._lock:
            for bug in self._state["bugs"]:
                if bug["ref"] == ref:
                    return bug
            return None

    def bugs_with_status(self, status):
        """Return list of bug dicts with the given status."""
        with self._lock:
            return [b for b in self._state["bugs"] if b["status"] == status]

    def count_status(self, status):
        """Count bugs with the given status."""
        with self._lock:
            return len(self.bugs_with_status(status))

    def next_queued_ref(self):
        """Return the ref of the next queued bug, ordered by launch attempts then input position.

        Prioritising bugs with fewer launch attempts ensures a flaky bug does not
        repeatedly consume a slot before other queued bugs get a turn. Among bugs
        with equal attempts, the position in the input array wins: the input has
        no priority fields, so its order is the caller's priority/severity ranking.
        """
        with self._lock:
            queued = sorted(
                (
                    (b.get("launchAttempts", 0), position, b["ref"])
                    for position, b in enumerate(self._state["bugs"])
                    if b["status"] == "queued"
                ),
            )
            if queued:
                return queued[0][2]
            return None

    def set_phase(self, phase):
        """Set the overall phase."""
        with self._lock:
            self._state["phase"] = phase
            self.save()


# Shared with orchestrate_epic (same logic, avoids duplication): the PID check,
# PR-number validation and the host-repo base-branch resolution.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from orchestrate_epic import (  # noqa: E402
    HostRepoBaseMixin, is_agent_process, read_log_tail, valid_pr_number,
)


def _classify_phase(phase, pr):
    """Classify a state file phase into an outcome.

    Returns (outcome, escalation_reason). Terminal phases return reason=None.
    Non-terminal phases with a PR are downgraded to escalated (bug #1825).
    """
    terminal_phases = {"complete", "escalated", "already-closed"}
    if phase == "complete":
        return "complete", None
    elif phase == "escalated":
        return "escalated", None
    elif phase == "already-closed":
        return "already_closed", None
    elif phase not in terminal_phases and pr is not None and str(pr).strip().isdigit() and int(str(pr)) > 0:
        # Inner agent exited without writing a terminal phase but did create a PR
        # (e.g., paused asking user questions, then was timed out by parent).
        # Surface as escalation rather than silent error — bug #1825.
        # Only treat prNumber as valid if it is a positive integer to avoid
        # masking errors caused by empty-string, "0", or stale null-ish values.
        return "escalated", f"non-terminal phase: {phase}"
    return None, None


def determine_outcome(ref, log_file, container_id=None, state_dir="/tmp"):
    """Determine the outcome of a completed bug-fix process.

    Returns (outcome, pr_number, escalation_reason). escalation_reason is
    non-None only when a non-terminal phase was downgraded to escalated.
    """
    outcome = "error"
    pr_number = "none"
    escalation_reason = None

    # Primary: read the per-bug state file from inside the container.
    if container_id:
        try:
            result = subprocess.run(
                ["docker", "exec", container_id,
                 "cat", f"/tmp/fix-and-remediate-{ref}.json"],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                bug_state = json.loads(result.stdout)
                phase = bug_state.get("phase", "")
                pr = bug_state.get("prNumber")

                pr_number = valid_pr_number(pr) or "none"

                classified, reason = _classify_phase(phase, pr)
                if classified is not None:
                    outcome = classified
                    escalation_reason = reason

                subprocess.run(
                    ["docker", "exec", container_id,
                     "rm", "-f", f"/tmp/fix-and-remediate-{ref}.json"],
                    capture_output=True, timeout=10,
                )
        except (subprocess.TimeoutExpired, json.JSONDecodeError, OSError):
            pass

    # Secondary: check host-side state file
    if outcome == "error":
        host_state_path = os.path.join(state_dir, f"fix-and-remediate-{ref}.json")
        if os.path.exists(host_state_path):
            try:
                with open(host_state_path) as f:
                    bug_state = json.loads(f.read())
                phase = bug_state.get("phase", "")
                pr = bug_state.get("prNumber")

                pr_number = valid_pr_number(pr) or "none"

                classified, reason = _classify_phase(phase, pr)
                if classified is not None:
                    outcome = classified
                    escalation_reason = reason

                os.unlink(host_state_path)
            except (json.JSONDecodeError, OSError):
                pass

    # Fallback: scan log tail
    if outcome == "error" and log_file:
        # Bounded read from the end: agent logs run to tens of MB and several
        # bugs finish concurrently, so never load the whole file.
        lower_tail = "".join(read_log_tail(log_file, 100)).lower()
        if "ready for test" in lower_tail or "ready for review" in lower_tail:
            outcome = "complete"
        elif "escalated" in lower_tail or "escalation-needed" in lower_tail:
            outcome = "escalated"

    return outcome, pr_number, escalation_reason


class Orchestrator(HostRepoBaseMixin):
    """Main orchestrator state machine for parallel bug fixing."""

    def __init__(self, concurrency, bugs_file, script_dir, state_dir, base_branch,
                 repo_root=None):
        self.concurrency = concurrency
        self.bugs_file = bugs_file
        self.script_dir = Path(script_dir)
        self.state_dir = Path(state_dir)
        self.base_branch = base_branch
        # The host repo launch.sh clones from (and _run_git targets): by
        # default the checkout this script lives in, not the cwd.
        self.repo_root = Path(repo_root) if repo_root else Path(__file__).resolve().parents[2]
        self.git_lock = threading.RLock()
        self.launch_timeout = int(os.environ.get("ORCHESTRATE_LAUNCH_TIMEOUT", "1800"))
        self.shutting_down = False
        self.child_procs = {}     # ref -> Popen object (the agent)
        self.launch_threads = {}  # ref -> Thread running launch_bug
        self.launch_procs = {}    # ref -> Popen of the helper script a launch is running
        # Guards child_procs / launch_threads / launch_procs, which the poll
        # loop and the launch worker threads both touch. RLock, not Lock: the
        # signal handler runs on the main thread and calls cleanup, which takes
        # this lock again — a plain Lock would deadlock a shutdown that arrived
        # while the main thread held it.
        self.proc_lock = threading.RLock()

        state_file = str(self.state_dir / "parallel-fix-bugs.json")
        self.sm = StateManager(state_file)
        self.base_dir = self.state_dir / "parallel-fix-bugs"

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

        if self.sm.state is None:
            emit("DONE:0:0:0")
            sys.exit(2)

        self._cleanup_active()
        self._cleanup_launching()
        self._final_cleanup()

        complete = self.sm.count_status("complete")
        escalated = self.sm.count_status("escalated")
        errors = self.sm.count_status("error")
        emit(f"DONE:{complete}:{escalated}:{errors}")
        sys.exit(2)

    def _cleanup_active(self):
        """Terminate active processes and reset bugs to queued."""
        for bug in self.sm.bugs_with_status("active"):
            ref = bug["ref"]
            pid = bug.get("pid")

            with self.proc_lock:
                proc = self.child_procs.get(ref)
            if proc is not None:
                try:
                    proc.terminate()
                except OSError:
                    pass

            if pid is not None:
                try:
                    os.kill(pid, signal.SIGTERM)
                except (OSError, ProcessLookupError):
                    pass

        time.sleep(2)

        for bug in self.sm.bugs_with_status("active"):
            ref = bug["ref"]
            with self.proc_lock:
                proc = self.child_procs.get(ref)
            if proc is not None:
                try:
                    proc.kill()
                except OSError:
                    pass

            pid = bug.get("pid")
            if pid is not None:
                try:
                    os.kill(pid, signal.SIGKILL)
                except (OSError, ProcessLookupError):
                    pass

        for bug in self.sm.bugs_with_status("active"):
            ref = bug["ref"]
            clone_path = bug["clonePath"]
            # On a failed cleanup the bug keeps its status: the next run's
            # _resume sweeps it again instead of launching over the leftover.
            if self._teardown(clone_path):
                self.sm.update_bug(ref, status="queued", pid=None)

    def _cleanup_launching(self):
        """Stop in-flight launches and reset those bugs to queued.

        The helper scripts must die before teardown runs: a surviving
        `devcontainer up` grandchild can create a container after its clone has
        already been torn down.
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

        self._join_threads(threads, timeout=10)

        # A worker that passed its last shutdown check can have spawned its
        # agent and flipped to "active" while we waited — after _cleanup_active
        # already ran. Sweep those before requeuing the rest.
        if self.sm.count_status("active"):
            self._cleanup_active()

        for bug in self.sm.bugs_with_status("launching"):
            ref = bug["ref"]
            clone_path = bug["clonePath"]
            if self._teardown(clone_path):
                self.sm.update_bug(ref, status="queued")

    def _teardown(self, clone_path):
        """Run teardown.sh for a clone path. Returns True when it succeeded.

        A failure is logged, not raised, but callers must not requeue a bug
        whose cleanup failed: a retry would bind a second container to the
        same clone folder.
        """
        try:
            result = subprocess.run(
                [str(self.script_dir / "teardown.sh"), clone_path],
                capture_output=True, text=True, timeout=30,
            )
        except subprocess.TimeoutExpired:
            log(f"WARNING: teardown.sh timed out for {clone_path}")
            return False
        except OSError as e:
            log(f"WARNING: teardown.sh could not be run for {clone_path}: {e}")
            return False
        if result.returncode != 0:
            log(f"WARNING: teardown.sh failed for {clone_path} "
                f"(exit {result.returncode}): {(result.stderr or '').strip()}")
            return False
        return True

    def _requeue_after_teardown(self, ref, clone_path, **fields):
        """Tear the clone down and requeue the bug, only if the cleanup worked.

        On a failed cleanup the bug becomes a terminal `teardown_failed` error
        instead, keeping its clone path and container id so the final cleanup
        can still force-remove the leak. Returns True when it was requeued.
        """
        if self._teardown(clone_path):
            self.sm.update_bug(ref, status="queued", **fields)
            return True
        log(f"Bug {ref}: cleanup of {clone_path} failed; not requeueing "
            f"(a retry would bind a second container to the same folder)")
        self.sm.update_bug(
            ref, status="error", outcome="teardown_failed",
            completedAt=utcnow_iso(), pid=None,
        )
        emit(f"COMPLETED:{ref}:teardown_failed:none")
        return False

    def _final_cleanup(self):
        """Defense-in-depth: iterate ALL bugs and ensure their containers are stopped/removed."""
        for bug in self.sm.state["bugs"]:
            clone_path = bug.get("clonePath")
            container_id = bug.get("containerId")

            if clone_path:
                try:
                    self._teardown(clone_path)
                except Exception:
                    pass

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

        Runs the script in its own process group and kills the whole group on
        timeout — subprocess.run's timeout only kills the direct child, which
        would leave grandchildren (e.g. `devcontainer up`) running and able to
        create containers after teardown has already happened.

        `ref` registers the process against a bug for the duration of the call,
        so a shutdown arriving mid-launch can kill the group it belongs to.
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

    def init_or_resume(self):
        """Initialize from bugs file or resume from existing state."""
        if self.sm.exists():
            self.sm.load()
            phase = self.sm.state.get("phase", "")

            if phase == "complete":
                log("Previous run completed. Starting fresh.")
                os.remove(self.sm.state_file)
            else:
                # A file with no `mode` predates the key and is a container run.
                written_by = self.sm.state.get("mode", STATE_MODE)
                if written_by != STATE_MODE:
                    log(
                        f"ERROR: {self.sm.state_file} was written by {written_by} mode; "
                        f"re-invoke /parallel-fix-bugs with n=1 or --worktrees, "
                        f"or delete the file"
                    )
                    sys.exit(2)
                log("Resuming from existing state file")
                stored_concurrency = self.sm.state.get("concurrency", self.concurrency)
                if stored_concurrency != self.concurrency:
                    log(
                        f"Ignoring CLI concurrency={self.concurrency}; "
                        f"using stored concurrency={stored_concurrency}"
                    )
                    self.concurrency = stored_concurrency
                # baseBranch is stored at init so a resume with BASE_BRANCH
                # unset (or different) still clones and opens PRs against the
                # run's real base instead of silently falling back to "main".
                stored_base_branch = self.sm.state.get("baseBranch", self.base_branch)
                if stored_base_branch != self.base_branch:
                    log(
                        f"Ignoring BASE_BRANCH={self.base_branch}; "
                        f"using stored baseBranch={stored_base_branch}"
                    )
                    self.base_branch = stored_base_branch
                # Backfill the key for state files written before it existed.
                if self.sm.state.get("baseBranch") != stored_base_branch:
                    self.sm.state["baseBranch"] = stored_base_branch
                    self.sm.save()
                self._resume()
                return

        # Fresh init
        if not os.path.exists(self.bugs_file):
            print(f"Error: bugs file not found: {self.bugs_file}", file=sys.stderr)
            sys.exit(1)

        with open(self.bugs_file) as f:
            bugs = json.load(f)

        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.sm.init_from_bugs(self.concurrency, bugs, self.base_dir, self.base_branch)
        total = len(bugs)
        emit(f"INIT:{total}:{self.concurrency}")

    def _resume(self):
        """Handle resumption: check active PIDs, reset dead ones."""
        for bug in list(self.sm.bugs_with_status("active")):
            ref = bug["ref"]
            pid = bug.get("pid")

            if pid is not None and is_agent_process(pid):
                log(f"Bug {ref} still active (PID {pid})")
                continue

            log(f"Bug {ref} PID {pid} is dead, checking outcome")
            outcome, pr, esc_reason = determine_outcome(
                ref, bug.get("logFile"), bug.get("containerId"),
                state_dir=str(self.state_dir),
            )

            if esc_reason:
                log(f"Bug {ref} exited with non-terminal phase — marking as escalated ({esc_reason})")

            # Guard: "complete" without a PR means no meaningful work was done.
            no_pr_downgrade = outcome == "complete" and pr == "none"
            if no_pr_downgrade:
                log(f"Bug {ref} reported complete but created no PR — marking as error")
                outcome = "error"

            if outcome != "error":
                try:
                    pr_value = None if pr == "none" else int(pr)
                except (ValueError, TypeError):
                    log(f"Bug {ref}: non-integer prNumber '{pr}', treating as None")
                    pr_value = None
                # already_closed is terminal success (no PR needed): status
                # "complete" so it counts in the totals, outcome kept distinct.
                status = "complete" if outcome == "already_closed" else outcome
                self.sm.update_bug(
                    ref,
                    status=status, outcome=outcome,
                    completedAt=utcnow_iso(), prNumber=pr_value, pid=None,
                )
                self._teardown(bug["clonePath"])
                emit(f"COMPLETED:{ref}:{outcome}:{pr}")
            elif no_pr_downgrade:
                self.sm.update_bug(
                    ref,
                    status="error", outcome="no_pr_created",
                    completedAt=utcnow_iso(), pid=None,
                )
                self._teardown(bug["clonePath"])
                emit(f"COMPLETED:{ref}:error:{pr}")
            elif self._requeue_after_teardown(ref, bug["clonePath"], pid=None):
                log(f"Bug {ref} reset to queued")

        for bug in list(self.sm.bugs_with_status("launching")):
            ref = bug["ref"]
            if self._requeue_after_teardown(ref, bug["clonePath"]):
                log(f"Bug {ref} (launching) reset to queued")

        active = self.sm.count_status("active")
        queued = self.sm.count_status("queued")
        complete = self.sm.count_status("complete")
        escalated = self.sm.count_status("escalated")
        errors = self.sm.count_status("error")
        emit(f"RESUMED:{active}:{queued}:{complete}:{escalated}:{errors}")

    def _handle_launch_failure(self, ref, clone_path, reason, container_id=None):
        """Handle a launch failure: retry if under limit, otherwise mark as terminal error."""
        bug = self.sm.get_bug(ref)
        attempts = bug.get("launchAttempts", 0) + 1
        cleaned = self._teardown(clone_path)

        # Persist containerId whenever we have one so _final_cleanup can
        # force-remove the leak if teardown.sh silently failed. Applies to
        # both retry and terminal-error paths.
        extra = {"containerId": container_id} if container_id else {}

        if not cleaned:
            # A container may still be bound to this clone. A retry would bind
            # a second one to the same folder, so the bug is terminal and the
            # clone path and container id are kept for the final cleanup.
            log(f"Cleanup failed for bug {ref}; not retrying, clone kept at {clone_path}")
            reason = f"{reason}; cleanup failed, container may still be running"

        if cleaned and attempts < MAX_LAUNCH_ATTEMPTS:
            log(f"Launch attempt {attempts}/{MAX_LAUNCH_ATTEMPTS} failed for bug {ref}, will retry")
            self.sm.update_bug(ref, status="queued", launchAttempts=attempts, **extra)
        else:
            log(f"Launch failed for bug {ref} after {attempts} attempts, marking as error")
            self.sm.update_bug(
                ref, status="error", outcome="launch_failed",
                completedAt=utcnow_iso(), launchAttempts=attempts,
                **extra,
            )

        emit(f"LAUNCH_FAIL:{ref}:{reason}")
        return False

    def launch_bug(self, ref):
        """Launch a bug fix into a devcontainer slot.

        Runs on a launch worker thread; fill_slots has already reserved the slot
        by setting the bug to "launching". Bails between blocking steps once a
        shutdown starts, leaving the bug in "launching" for _cleanup_launching
        to tear down and requeue.
        """
        bug = self.sm.get_bug(ref)
        clone_path = bug["clonePath"]
        log_file = bug["logFile"]

        # launch.sh clones the host repo's local `<base>` head as-is, so make it
        # current (or create it, for a branch that only exists on the remote)
        # first; otherwise the bug runs against stale code or every launch fails.
        if not self._ensure_base_reachable(self.base_branch):
            # A shutdown can abort the fetch mid-call; that is not the bug's fault.
            if self.shutting_down:
                return False
            return self._handle_launch_failure(
                ref, clone_path, f"base branch {self.base_branch} not reachable",
            )

        # Launch devcontainer
        try:
            launch_out, launch_rc = self._run_script(
                "launch.sh", clone_path, self.base_branch,
                timeout=self.launch_timeout, ref=ref,
            )
        except subprocess.TimeoutExpired as e:
            log(f"Launch failed for bug {ref}: {e}")
            return self._handle_launch_failure(
                ref, clone_path,
                f"devcontainer launch timed out after {self.launch_timeout}s",
            )
        except OSError as e:
            log(f"Launch failed for bug {ref}: {e}")
            return self._handle_launch_failure(ref, clone_path, "devcontainer launch failed")

        # A shutdown killed launch.sh's process group; that is not the bug's
        # fault, so bail without spending one of its launch attempts.
        if self.shutting_down:
            return False

        if launch_rc != 0:
            log(f"Launch failed for bug {ref}: exit code {launch_rc}")
            return self._handle_launch_failure(ref, clone_path, "devcontainer launch failed")

        # Extract container ID
        container_id = None
        for line in launch_out.split("\n"):
            if line.startswith("CONTAINER_ID="):
                container_id = line.split("=", 1)[1].strip()
                break

        if not container_id:
            log(f"No CONTAINER_ID in launch output for bug {ref}")
            return self._handle_launch_failure(ref, clone_path, "no container ID returned")

        if self.shutting_down:
            return False

        # Prepare clone
        try:
            _, prep_rc = self._run_script("prepare-clone.sh", clone_path, ref=ref)
        except (subprocess.TimeoutExpired, OSError) as e:
            log(f"prepare-clone.sh failed for bug {ref}: {e}")
            return self._handle_launch_failure(ref, clone_path, "prepare-clone failed", container_id)

        # As above: a shutdown killed prepare-clone.sh, so don't spend an attempt.
        if self.shutting_down:
            return False

        if prep_rc != 0:
            log(f"prepare-clone.sh failed for bug {ref}")
            return self._handle_launch_failure(ref, clone_path, "prepare-clone failed", container_id)

        # The skill expansion (frontmatter strip, $ARGUMENTS substitution) and
        # the in-container `claude` invocation live in exec-skill.sh, which also
        # writes the stream-json log.
        skill_path = Path(clone_path) / ".claude/skills/fix-and-remediate-bug/SKILL.md"
        if not skill_path.is_file():
            log(f"Could not read skill for bug {ref}: {skill_path} not found")
            return self._handle_launch_failure(ref, clone_path, "skill file not found", container_id)

        if self.shutting_down:
            return False

        cmd = [
            str(Path(self.script_dir) / "exec-skill.sh"), clone_path,
            # The second word is the base the worker branches from and opens its
            # PR against; without it the skill falls back to upstream/main.
            "fix-and-remediate-bug", f"{ref} {self.base_branch}", "--log", log_file,
        ]
        try:
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except OSError as e:
            log(f"Failed to start exec-skill.sh for bug {ref}: {e}")
            return self._handle_launch_failure(ref, clone_path, "agent start failed", container_id)

        with self.proc_lock:
            self.child_procs[ref] = proc
        self.sm.update_bug(
            ref, status="active", pid=proc.pid,
            containerId=container_id, startedAt=utcnow_iso(),
        )
        emit(f"LAUNCH_OK:{ref}:{proc.pid}")
        return True

    def _launch_worker(self, ref):
        """Thread body for a launch: always deregisters itself when done."""
        try:
            self.launch_bug(ref)
        except Exception as e:  # a crashed thread must not leave the slot wedged
            log(f"Unexpected error launching bug {ref}: {e}")
            bug = self.sm.get_bug(ref)
            self._handle_launch_failure(ref, bug["clonePath"], "unexpected launch error")
        finally:
            # A failed launch requeues the ref before we get here, so the poll
            # loop may already have started a retry and registered its thread.
            # Only clear the entry if it is still ours.
            with self.proc_lock:
                if self.launch_threads.get(ref) is threading.current_thread():
                    del self.launch_threads[ref]

    def check_completions(self):
        """Check active bugs for process completion."""
        for bug in list(self.sm.bugs_with_status("active")):
            ref = bug["ref"]
            pid = bug.get("pid")

            with self.proc_lock:
                proc = self.child_procs.get(ref)
            if proc is not None:
                if proc.poll() is None:
                    continue
            elif pid is not None and is_agent_process(pid):
                continue

            outcome, pr, esc_reason = determine_outcome(
                ref, bug.get("logFile"), bug.get("containerId"),
                state_dir=str(self.state_dir),
            )

            if esc_reason:
                log(f"Bug {ref} exited with non-terminal phase — marking as escalated ({esc_reason})")

            no_pr_downgrade = outcome == "complete" and pr == "none"
            if no_pr_downgrade:
                log(f"Bug {ref} reported complete but created no PR — marking as error")
                outcome = "error"

            try:
                pr_value = None if pr == "none" else int(pr)
            except (ValueError, TypeError):
                log(f"Bug {ref}: non-integer prNumber '{pr}', treating as None")
                pr_value = None

            # already_closed is terminal success (no PR needed). Map it to
            # status="complete" so it counts toward success totals, but keep
            # the distinct outcome label for auditability.
            status = "complete" if outcome == "already_closed" else outcome
            outcome_label = "no_pr_created" if no_pr_downgrade else outcome
            self.sm.update_bug(
                ref,
                status=status, outcome=outcome_label,
                completedAt=utcnow_iso(), prNumber=pr_value, pid=None,
            )
            self._teardown(bug["clonePath"])
            with self.proc_lock:
                self.child_procs.pop(ref, None)
            emit(f"COMPLETED:{ref}:{outcome_label}:{pr}")

    def fill_slots(self):
        """Fill empty concurrency slots with queued bugs.

        A launch takes ~10 minutes, so it runs on its own thread: the poll loop
        keeps reaping completions and can start other launches meanwhile. The
        slot is reserved here (status="launching") before the thread starts, so
        concurrency is never exceeded.
        """
        started_this_cycle = set()
        while not self.shutting_down:
            occupied = self.sm.count_status("active") + self.sm.count_status("launching")
            if occupied >= self.concurrency:
                break

            next_ref = self.sm.next_queued_ref()
            if next_ref is None:
                break

            # A launch that fails fast requeues its ref from the worker thread
            # while this loop is still running. Retrying it here would let one
            # flaky bug burn all its launch attempts in a single cycle and keep
            # other queued bugs out of the slot; leave it for the next poll.
            if next_ref in started_this_cycle:
                break
            started_this_cycle.add(next_ref)

            bug = self.sm.get_bug(next_ref)
            self.sm.update_bug(next_ref, status="launching")
            emit(f"LAUNCHING:{next_ref}:{bug['subject']}")

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
        """Check if all bugs have reached a terminal state."""
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

        self._final_cleanup()

        self.sm.set_phase("complete")
        complete = self.sm.count_status("complete")
        escalated = self.sm.count_status("escalated")
        errors = self.sm.count_status("error")
        emit(f"DONE:{complete}:{escalated}:{errors}")


def main():
    if len(sys.argv) < 3:
        print(
            "Usage: orchestrate_bugs.py <concurrency> <bugs-json-file>",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        concurrency = int(sys.argv[1])
    except ValueError:
        print(
            f"Error: concurrency must be a positive integer, got '{sys.argv[1]}'",
            file=sys.stderr,
        )
        sys.exit(1)

    if concurrency < 1 or concurrency > 5:
        print(f"Error: concurrency must be 1-5, got '{concurrency}'", file=sys.stderr)
        sys.exit(1)

    bugs_file = sys.argv[2]

    script_dir = os.environ.get(
        "ORCHESTRATE_SCRIPT_DIR",
        os.path.dirname(os.path.abspath(__file__)),
    )
    state_dir = os.environ.get("ORCHESTRATE_STATE_DIR", "/tmp")
    base_branch = os.environ.get("BASE_BRANCH", "main")

    orchestrator = Orchestrator(
        concurrency, bugs_file, script_dir, state_dir, base_branch,
        repo_root=os.environ.get("ORCHESTRATE_REPO_ROOT"),
    )
    orchestrator.run()


if __name__ == "__main__":
    main()
