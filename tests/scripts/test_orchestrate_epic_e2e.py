# ABOUTME: End-to-end pytest suite driving orchestrate_epic.py and orchestrate_bugs.py as subprocesses against fake lifecycle scripts.
# ABOUTME: Fakes launch.sh, prepare-clone.sh, exec.sh, exec-skill.sh, teardown.sh and docker, so no Docker, network or hosted service is touched.
import json
import os
import shlex
import signal
import subprocess
import sys
import time

import pytest

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
ORCHESTRATE = os.path.join(REPO_ROOT, "scripts", "devcontainer", "orchestrate_epic.py")
ORCHESTRATE_BUGS = os.path.join(REPO_ROOT, "scripts", "devcontainer", "orchestrate_bugs.py")

sys.path.insert(0, os.path.join(REPO_ROOT, "scripts", "devcontainer"))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts", "lib"))
from dependency_parser import parse_dependency_diagram
from orchestrate_epic import Orchestrator
from conftest import make_host_repo


@pytest.fixture
def tmp_env(tmp_path, monkeypatch):
    """Provide a clean temp environment with stories files and mock scripts.

    ORCHESTRATE_REPO_ROOT points every orchestrator subprocess at a sandbox
    host repo whose `main` is current, so the base-reachability git calls
    never touch the developer's own clone .
    """
    monkeypatch.setenv("ORCHESTRATE_REPO_ROOT", str(make_host_repo(tmp_path / "host-repo", remote="upstream")))

    stories_3 = tmp_path / "stories-3.json"
    stories_3.write_text(json.dumps([
        {"ref": 101, "id": 1001, "subject": "First story"},
        {"ref": 102, "id": 1002, "subject": "Second story"},
        {"ref": 103, "id": 1003, "subject": "Third story"},
    ]))

    stories_1 = tmp_path / "stories-1.json"
    stories_1.write_text(json.dumps([
        {"ref": 101, "id": 1001, "subject": "First story"},
    ]))

    state_file = tmp_path / "implement-epic-99999.json"
    base_dir = tmp_path / "epic-99999"

    # Create mock scripts directory
    mock_bin = tmp_path / "mock-bin"
    mock_bin.mkdir()

    # Mock launch.sh
    (mock_bin / "launch.sh").write_text(
        '#!/usr/bin/env bash\n'
        'CLONE_PATH="$1"\n'
        'mkdir -p "$CLONE_PATH/.claude" "$CLONE_PATH/.local/bin"\n'
        'echo "CONTAINER_ID=mock-container-$(basename "$CLONE_PATH")"\n'
    )
    (mock_bin / "launch.sh").chmod(0o755)

    # Mock teardown.sh
    (mock_bin / "teardown.sh").write_text(
        '#!/usr/bin/env bash\n'
        '[[ "${2:-}" == "--keep-clone" ]] || { [[ -d "$1" ]] && rm -rf "$1"; }\n'
        'echo "Teardown complete."\n'
    )
    (mock_bin / "teardown.sh").chmod(0o755)

    # Mock prepare-clone.sh: creates the skill files the orchestrators check for.
    (mock_bin / "prepare-clone.sh").write_text(
        "#!/usr/bin/env bash\n"
        "CLONE_PATH=$1\n"
        "mkdir -p $CLONE_PATH/.claude/skills/fully-implement-story "
        "$CLONE_PATH/.claude/skills/fix-and-remediate-bug $CLONE_PATH/.local/bin\n"
        "echo '{\"permissions\":{\"allow\":[]}}' > $CLONE_PATH/.claude/settings.local.json\n"
        "echo 'Implement story $ARGUMENTS' > $CLONE_PATH/.claude/skills/fully-implement-story/SKILL.md\n"
        "echo 'Fix bug $ARGUMENTS' > $CLONE_PATH/.claude/skills/fix-and-remediate-bug/SKILL.md\n"
    )
    (mock_bin / "prepare-clone.sh").chmod(0o755)

    # Mock exec-skill.sh: the real one expands the skill and runs claude inside the
    # container. This one records the call, then pipes "<prompt prefix> <args>" into
    # the `claude` stand-in below (the agent), writing its output to --log (truncating it, as the real script does) and
    # propagating its exit code. Rewrite `claude` to change what the agent does.
    calls_log = tmp_path / "exec-skill-calls.log"
    (mock_bin / "exec-skill.sh").write_text(
        '#!/usr/bin/env bash\n'
        'CLONE_PATH="$1"; SKILL="$2"; ARGS="$3"; shift 3\n'
        'LOG=/dev/null\n'
        'while [ "$#" -gt 0 ]; do\n'
        '    case "$1" in --log) LOG="$2"; shift 2;; *) shift;; esac\n'
        'done\n'
        f'echo "$SKILL|$ARGS" >> "{calls_log}"\n'
        'case "$SKILL" in\n'
        '    fix-and-remediate-bug) PROMPT="Fix bug $ARGS";;\n'
        '    *) PROMPT="Implement story $ARGS";;\n'
        'esac\n'
        f'echo "$PROMPT" | "{mock_bin}/claude" > "$LOG"\n'
        'exit $?\n'
    )
    (mock_bin / "exec-skill.sh").chmod(0o755)

    # Mock exec.sh (used by --dry-run): records the command it was given.
    exec_log = tmp_path / "exec-calls.log"
    (mock_bin / "exec.sh").write_text(
        '#!/usr/bin/env bash\n'
        f'echo "$1|$2" >> "{exec_log}"\n'
        'echo "smoke ok"\n'
    )
    (mock_bin / "exec.sh").chmod(0o755)

    # Fake docker, always first on PATH: inspect and exec fail (the host-side state
    # file is used and no container is reused), everything else succeeds quietly.
    (mock_bin / "docker").write_text(
        '#!/usr/bin/env bash\n'
        '[[ "$1" == "inspect" || "$1" == "exec" ]] && exit 1\n'
        'exit 0\n'
    )
    (mock_bin / "docker").chmod(0o755)
    monkeypatch.setenv("PATH", f"{mock_bin}{os.pathsep}{os.environ['PATH']}")

    # Mock claude: the in-container agent stand-in. Reads the prompt exec-skill.sh pipes
    # in, writes the completion state file, and prints a log marker that
    # determine_outcome's log-scan fallback recognises.
    (mock_bin / "claude").write_text(
        '#!/usr/bin/env bash\n'
        '# Prompt arrives on stdin (from exec-skill.sh)\n'
        'PROMPT=$(cat)\n'
        'REF=$(echo "$PROMPT" | grep -oP "(?<=story )\\d+" | head -1)\n'
        # featureBranch is "main" (the head the sandbox host repo from
        # ORCHESTRATE_REPO_ROOT keeps current) so the orchestrator's
        # base-reachability check resolves without a network fetch. Base *resolution* and *reachability* are covered directly in
        # tests/scripts/test_orchestrate_epic.py; these tests own launch ordering.
        'if [[ -n "$REF" ]]; then\n'
        '    echo \'{"phase":"complete","prNumber":42,"featureBranch":"main"}\' > "${ORCHESTRATE_STATE_DIR:-/tmp}/fully-implement-${REF}.json"\n'
        'fi\n'
        '# Log marker for determine_outcome log-scan fallback\n'
        'echo "PR is ready for review"\n'
        'sleep 1\n'
        'exit 0\n'
    )
    (mock_bin / "claude").chmod(0o755)

    env = {
        "stories_3": str(stories_3),
        "stories_1": str(stories_1),
        "state_file": str(state_file),
        "base_dir": str(base_dir),
        "mock_bin": str(mock_bin),
        "calls_log": str(calls_log),
        "exec_log": str(exec_log),
        "tmp_path": str(tmp_path),
    }
    return env


def run_orchestrator(tmp_env, stories_file, concurrency=2, epic_ref="99999",
                     timeout=60, extra_env=None, extra_args=()):
    """Run the orchestrator script and return (stdout, stderr, exit_code)."""
    env = os.environ.copy()
    env["ORCHESTRATE_SCRIPT_DIR"] = tmp_env["mock_bin"]
    env["ORCHESTRATE_STATE_DIR"] = tmp_env["tmp_path"]
    env["POLL_INTERVAL"] = "1"
    if extra_env:
        env.update(extra_env)

    result = subprocess.run(
        [sys.executable, ORCHESTRATE, str(epic_ref), str(concurrency), stories_file,
         *extra_args],
        capture_output=True, text=True, timeout=timeout, env=env,
    )
    return result.stdout, result.stderr, result.returncode


def parse_events(stdout):
    """Parse stdout lines into a list of (event_type, rest) tuples."""
    events = []
    for line in stdout.strip().split("\n"):
        if not line:
            continue
        parts = line.split(":", 1)
        events.append((parts[0], parts[1] if len(parts) > 1 else ""))
    return events


class TestInitFromStoriesJson:
    """Test 1: Init from stories JSON — state file created correctly."""

    def test_state_file_created_with_correct_structure(self, tmp_env):
        stdout, stderr, code = run_orchestrator(tmp_env, tmp_env["stories_3"])
        state_file = tmp_env["state_file"]
        assert os.path.exists(state_file), f"State file not created. stderr: {stderr}"

        with open(state_file) as f:
            state = json.load(f)

        assert state["epicRef"] == "99999"
        assert state["concurrency"] == 2
        assert state["phase"] == "complete"
        assert len(state["stories"]) == 3

        # All stories should have required fields
        for story in state["stories"]:
            assert "ref" in story
            assert "id" in story
            assert "subject" in story
            assert "status" in story
            assert "clonePath" in story
            assert "logFile" in story

    def test_init_event_emitted(self, tmp_env):
        stdout, _, _ = run_orchestrator(tmp_env, tmp_env["stories_3"])
        events = parse_events(stdout)
        init_events = [e for e in events if e[0] == "INIT"]
        assert len(init_events) == 1
        parts = init_events[0][1].split(":")
        assert parts[0] == "99999"  # epic ref
        assert parts[1] == "3"     # total stories
        assert parts[2] == "2"     # concurrency


class TestResumeWithDeadPid:
    """Test 2: Resume with dead PID — reset to queued."""

    def test_dead_pid_story_resets_to_queued_and_completes(self, tmp_env):
        state_file = tmp_env["state_file"]
        base_dir = tmp_env["base_dir"]
        os.makedirs(base_dir, exist_ok=True)

        # Create state file with an active story pointing to a dead PID
        state = {
            "epicRef": "99999",
            "concurrency": 2,
            "phase": "running",
            "baseDir": base_dir,
            "stories": [
                {
                    "ref": 101, "id": 1001, "subject": "First story",
                    "status": "active", "pid": 999999999,
                    "clonePath": os.path.join(base_dir, "story-101"),
                    "logFile": os.path.join(base_dir, "story-101.log"),
                    "startedAt": "2026-01-01T00:00:00Z",
                    "completedAt": None, "outcome": None, "prNumber": None,
                },
                {
                    "ref": 102, "id": 1002, "subject": "Second story",
                    "status": "queued", "pid": None,
                    "clonePath": os.path.join(base_dir, "story-102"),
                    "logFile": os.path.join(base_dir, "story-102.log"),
                    "startedAt": None, "completedAt": None, "outcome": None,
                    "prNumber": None,
                },
            ],
        }
        with open(state_file, "w") as f:
            json.dump(state, f)

        stdout, stderr, code = run_orchestrator(
            tmp_env, tmp_env["stories_3"],
        )
        events = parse_events(stdout)
        resumed = [e for e in events if e[0] == "RESUMED"]
        assert len(resumed) == 1, f"Expected RESUMED event, got events: {events}"

        # Both stories should eventually complete
        done_events = [e for e in events if e[0] == "DONE"]
        assert len(done_events) == 1


class TestReconcileOnCompletedState:
    """Bug #2019: re-invoking a completed run retries only failures, not PR'd stories."""

    def test_completed_run_relaunches_only_failed_story(self, tmp_env):
        state_file = tmp_env["state_file"]
        base_dir = tmp_env["base_dir"]
        os.makedirs(base_dir, exist_ok=True)

        state = {
            "epicRef": "99999",
            "concurrency": 2,
            "phase": "complete",
            "baseDir": base_dir,
            "stories": [
                {
                    "ref": 101, "id": 1001, "subject": "First story",
                    "status": "complete", "pid": None,
                    "clonePath": os.path.join(base_dir, "story-101"),
                    "logFile": os.path.join(base_dir, "story-101.log"),
                    "startedAt": "2026-01-01T00:00:00Z",
                    "completedAt": "2026-01-01T01:00:00Z",
                    "outcome": "complete", "prNumber": 42,
                    "containerId": None, "launchAttempts": 0,
                    "dependencies": [],
                },
                {
                    "ref": 102, "id": 1002, "subject": "Second story",
                    "status": "error", "pid": None,
                    "clonePath": os.path.join(base_dir, "story-102"),
                    "logFile": os.path.join(base_dir, "story-102.log"),
                    "startedAt": "2026-01-01T00:00:00Z",
                    "completedAt": "2026-01-01T01:00:00Z",
                    "outcome": "launch_failed", "prNumber": None,
                    "containerId": None, "launchAttempts": 3,
                    "dependencies": [],
                },
                {
                    "ref": 103, "id": 1003, "subject": "Third story",
                    "status": "complete", "pid": None,
                    "clonePath": os.path.join(base_dir, "story-103"),
                    "logFile": os.path.join(base_dir, "story-103.log"),
                    "startedAt": "2026-01-01T00:00:00Z",
                    "completedAt": "2026-01-01T01:00:00Z",
                    "outcome": "complete", "prNumber": 43,
                    "containerId": None, "launchAttempts": 0,
                    "dependencies": [],
                },
            ],
        }
        with open(state_file, "w") as f:
            json.dump(state, f)

        stdout, stderr, code = run_orchestrator(tmp_env, tmp_env["stories_3"])
        events = parse_events(stdout)

        requeued = [e for e in events if e[0] == "REQUEUED"]
        assert requeued == [("REQUEUED", "102")], f"events: {events}"

        launching_refs = [e[1].split(":")[0] for e in events if e[0] == "LAUNCHING"]
        assert launching_refs == ["102"], f"Only the failed story should relaunch: {events}"


class TestStdoutEventFormat:
    """Test 3: Stdout event format — all events parseable."""

    def test_all_events_follow_protocol(self, tmp_env):
        stdout, _, _ = run_orchestrator(tmp_env, tmp_env["stories_1"], concurrency=1)
        valid_prefixes = {
            "INIT", "RESUMED", "LAUNCHING", "LAUNCH_OK", "LAUNCH_FAIL",
            "COMPLETED", "SKIPPED", "SLOT_FILLED", "WAITING", "DONE",
        }
        for line in stdout.strip().split("\n"):
            if not line:
                continue
            event_type = line.split(":")[0]
            assert event_type in valid_prefixes, f"Unparseable event: {line!r}"


class TestLaunchFailure:
    """Test 4: Launch failure — story marked error, continues."""

    def test_launch_failure_marks_error_and_continues(self, tmp_env):
        # Make launch.sh fail for story 101
        mock_bin = tmp_env["mock_bin"]
        launch_script = os.path.join(mock_bin, "launch.sh")
        with open(launch_script, "w") as f:
            f.write(
                '#!/usr/bin/env bash\n'
                'CLONE_PATH="$1"\n'
                'STORY_NUM=$(basename "$CLONE_PATH" | sed "s/story-//")\n'
                'if [[ "$STORY_NUM" == "101" ]]; then\n'
                '    echo "Simulated failure" >&2\n'
                '    exit 1\n'
                'fi\n'
                'mkdir -p "$CLONE_PATH/.claude" "$CLONE_PATH/.local/bin"\n'
                'echo "CONTAINER_ID=mock-container-$(basename "$CLONE_PATH")"\n'
            )
        os.chmod(launch_script, 0o755)

        stdout, _, code = run_orchestrator(tmp_env, tmp_env["stories_3"])
        events = parse_events(stdout)

        launch_fails = [e for e in events if e[0] == "LAUNCH_FAIL"]
        assert len(launch_fails) >= 1
        assert "101" in launch_fails[0][1]

        # Other stories should still complete
        completed = [e for e in events if e[0] == "COMPLETED"]
        assert len(completed) >= 2

        # Check state file
        with open(tmp_env["state_file"]) as f:
            state = json.load(f)
        error_stories = [s for s in state["stories"] if s["status"] == "error"]
        assert len(error_stories) == 1
        assert error_stories[0]["ref"] == 101


class TestHostRepoRootOverride:
    """bug #3221: ORCHESTRATE_REPO_ROOT selects the host repo the base check
    runs against, independent of the clone the script lives in."""

    def test_stale_main_in_the_configured_host_repo_fails_the_launch(self, tmp_env, monkeypatch):
        stale = make_host_repo(os.path.join(tmp_env["tmp_path"], "stale"), stale_main=True, remote="upstream")
        monkeypatch.setenv("ORCHESTRATE_REPO_ROOT", str(stale))

        stdout, _, _ = run_orchestrator(
            tmp_env, tmp_env["stories_1"], concurrency=1,
            extra_env={"ORCHESTRATE_TRANSIENT_BACKOFF": "1"},
        )
        events = parse_events(stdout)

        assert ("LAUNCH_FAIL", "101:base branch main not reachable") in events
        assert not [e for e in events if e[0] == "COMPLETED"], events


class TestSignalHandling:
    """Test 5: Signal handling — SIGTERM triggers cleanup, exit 2."""

    def test_sigterm_triggers_cleanup(self, tmp_env):
        # Use a slow claude mock that sleeps 60 seconds
        mock_bin = tmp_env["mock_bin"]
        with open(os.path.join(mock_bin, "claude"), "w") as f:
            f.write("#!/usr/bin/env bash\nsleep 60\n")
        os.chmod(os.path.join(mock_bin, "claude"), 0o755)

        env = os.environ.copy()
        env["ORCHESTRATE_SCRIPT_DIR"] = mock_bin
        env["ORCHESTRATE_STATE_DIR"] = tmp_env["tmp_path"]
        env["POLL_INTERVAL"] = "2"

        proc = subprocess.Popen(
            [sys.executable, ORCHESTRATE, "99999", "1", tmp_env["stories_1"]],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, env=env,
        )

        # Wait for the story to launch, then send signal before claude finishes
        # Poll for the state file to have an active story
        deadline = time.time() + 15
        while time.time() < deadline:
            state_file = tmp_env["state_file"]
            if os.path.exists(state_file):
                try:
                    with open(state_file) as f:
                        state = json.load(f)
                    active = [s for s in state["stories"] if s["status"] == "active"]
                    if active:
                        break
                except (json.JSONDecodeError, OSError):
                    pass
            time.sleep(0.5)

        # Send SIGTERM
        proc.send_signal(signal.SIGTERM)

        try:
            stdout, stderr = proc.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, stderr = proc.communicate()

        assert proc.returncode == 2, (
            f"Expected exit code 2, got {proc.returncode}. "
            f"stdout: {stdout}, stderr: {stderr}"
        )

        # Check state file — active stories should be reset to queued
        state_file = tmp_env["state_file"]
        if os.path.exists(state_file):
            with open(state_file) as f:
                state = json.load(f)
            active = [s for s in state["stories"] if s["status"] == "active"]
            assert len(active) == 0, "Active stories should be reset to queued after SIGTERM"


class TestConcurrencyLimit:
    """Test 6: Concurrency limit — max N active at once."""

    def test_max_n_active_at_once(self, tmp_env):
        # Use a slow claude mock (5s) so we can observe the concurrency window
        mock_bin = tmp_env["mock_bin"]
        with open(os.path.join(mock_bin, "claude"), "w") as f:
            f.write(
                '#!/usr/bin/env bash\n'
                '# Prompt arrives on stdin (from exec-skill.sh)\n'
                'PROMPT=$(cat)\n'
                'REF=$(echo "$PROMPT" | grep -oP "(?<=story )\\d+" | head -1)\n'
                'if [[ -n "$REF" ]]; then\n'
                '    echo \'{"phase":"complete","prNumber":42,"featureBranch":"main"}\''
                ' > "${ORCHESTRATE_STATE_DIR:-/tmp}/fully-implement-${REF}.json"\n'
                'fi\n'
                'echo "PR is ready for review"\n'
                'sleep 4\n'
                'exit 0\n'
            )
        os.chmod(os.path.join(mock_bin, "claude"), 0o755)

        env = os.environ.copy()
        env["ORCHESTRATE_SCRIPT_DIR"] = mock_bin
        env["ORCHESTRATE_STATE_DIR"] = tmp_env["tmp_path"]
        env["POLL_INTERVAL"] = "1"

        proc = subprocess.Popen(
            [sys.executable, ORCHESTRATE, "99999", "2", tmp_env["stories_3"]],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, env=env,
        )

        # Wait for initial launches then check state
        time.sleep(3)

        state_file = tmp_env["state_file"]
        assert os.path.exists(state_file), "State file should exist"

        with open(state_file) as f:
            state = json.load(f)

        active = [s for s in state["stories"] if s["status"] == "active"]
        assert len(active) <= 2, f"Concurrency violated: {len(active)} active (max 2)"

        # Wait for completion
        try:
            proc.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()


class TestSlotFilling:
    """Test 7: Slot filling — 3 stories with N=2, third launches after first completes."""

    def test_third_story_launches_after_first_completes(self, tmp_env):
        stdout, _, code = run_orchestrator(tmp_env, tmp_env["stories_3"], concurrency=2)
        events = parse_events(stdout)

        # All 3 stories should get LAUNCHING events
        launching = [e for e in events if e[0] == "LAUNCHING"]
        assert len(launching) == 3

        # All 3 should complete
        completed = [e for e in events if e[0] == "COMPLETED"]
        assert len(completed) == 3

        # There should be SLOT_FILLED events
        slot_filled = [e for e in events if e[0] == "SLOT_FILLED"]
        assert len(slot_filled) >= 1


class TestLaunchRetry:
    """Test 8: Launch retry — transient launch failures retry before terminal error."""

    def test_transient_launch_failure_retries_and_succeeds(self, tmp_env):
        """A story whose launch fails once should be retried and eventually succeed."""
        # Make launch.sh fail for story 101 on the first attempt, succeed on the second.
        # Uses a file-based counter to track attempts.
        mock_bin = tmp_env["mock_bin"]
        tmp_path = tmp_env["tmp_path"]
        counter_file = os.path.join(tmp_path, "launch-counter-101")

        launch_script = os.path.join(mock_bin, "launch.sh")
        with open(launch_script, "w") as f:
            f.write(
                '#!/usr/bin/env bash\n'
                'CLONE_PATH="$1"\n'
                'STORY_NUM=$(basename "$CLONE_PATH" | sed "s/story-//")\n'
                f'COUNTER_FILE="{counter_file}"\n'
                'if [[ "$STORY_NUM" == "101" ]]; then\n'
                '    COUNT=0\n'
                '    [[ -f "$COUNTER_FILE" ]] && COUNT=$(cat "$COUNTER_FILE")\n'
                '    COUNT=$((COUNT + 1))\n'
                '    echo "$COUNT" > "$COUNTER_FILE"\n'
                '    if [[ "$COUNT" -le 1 ]]; then\n'
                '        echo "Simulated transient failure" >&2\n'
                '        exit 1\n'
                '    fi\n'
                'fi\n'
                'mkdir -p "$CLONE_PATH/.claude" "$CLONE_PATH/.local/bin"\n'
                'echo "CONTAINER_ID=mock-container-$(basename "$CLONE_PATH")"\n'
            )
        os.chmod(launch_script, 0o755)

        stdout, stderr, code = run_orchestrator(tmp_env, tmp_env["stories_3"])
        events = parse_events(stdout)

        # Story 101 should have a LAUNCH_FAIL followed by another LAUNCHING
        launch_fails_101 = [e for e in events if e[0] == "LAUNCH_FAIL" and "101" in e[1]]
        assert len(launch_fails_101) >= 1, (
            f"Expected at least 1 LAUNCH_FAIL for 101, got events: {events}"
        )

        # All 3 stories should eventually complete (101 retried successfully)
        completed = [e for e in events if e[0] == "COMPLETED"]
        completed_refs = [e[1].split(":")[0] for e in completed]
        assert "101" in completed_refs, (
            f"Story 101 should complete after retry. Events: {events}"
        )
        assert len(completed) == 3, (
            f"All 3 stories should complete, got {len(completed)}. Events: {events}"
        )

        # Final state: no errors
        with open(tmp_env["state_file"]) as f:
            state = json.load(f)
        error_stories = [s for s in state["stories"] if s["status"] == "error"]
        assert len(error_stories) == 0, (
            f"No stories should be in error state, got: {error_stories}"
        )

        # Check launchAttempts is tracked in state (counts failed attempts)
        story_101 = next(s for s in state["stories"] if s["ref"] == 101)
        assert story_101.get("launchAttempts", 0) >= 1, (
            f"Story 101 should have launchAttempts >= 1 (failed attempts), got: {story_101}"
        )

    def test_exhausted_retries_marks_terminal_error(self, tmp_env):
        """A story that fails launch every time should become terminal error after retries."""
        mock_bin = tmp_env["mock_bin"]
        launch_script = os.path.join(mock_bin, "launch.sh")
        with open(launch_script, "w") as f:
            f.write(
                '#!/usr/bin/env bash\n'
                'CLONE_PATH="$1"\n'
                'STORY_NUM=$(basename "$CLONE_PATH" | sed "s/story-//")\n'
                'if [[ "$STORY_NUM" == "101" ]]; then\n'
                '    echo "Permanent failure" >&2\n'
                '    exit 1\n'
                'fi\n'
                'mkdir -p "$CLONE_PATH/.claude" "$CLONE_PATH/.local/bin"\n'
                'echo "CONTAINER_ID=mock-container-$(basename "$CLONE_PATH")"\n'
            )
        os.chmod(launch_script, 0o755)

        stdout, stderr, code = run_orchestrator(tmp_env, tmp_env["stories_3"])
        events = parse_events(stdout)

        # Story 101 should have multiple LAUNCH_FAIL events (retries)
        launch_fails_101 = [e for e in events if e[0] == "LAUNCH_FAIL" and "101" in e[1]]
        assert len(launch_fails_101) >= 2, (
            f"Expected multiple LAUNCH_FAIL for 101 (retries), got {len(launch_fails_101)}. "
            f"Events: {events}"
        )

        # Story 101 should end in error state
        with open(tmp_env["state_file"]) as f:
            state = json.load(f)
        story_101 = next(s for s in state["stories"] if s["ref"] == 101)
        assert story_101["status"] == "error", (
            f"Story 101 should be in error after exhausted retries, got: {story_101['status']}"
        )
        assert story_101.get("launchAttempts", 0) >= 3, (
            f"Story 101 should have launchAttempts >= 3, got: {story_101}"
        )

        # Other stories should still complete
        completed = [e for e in events if e[0] == "COMPLETED"]
        assert len(completed) >= 2, (
            f"Other stories should complete. Events: {events}"
        )


class TestAtomicStateWrites:
    """Test 9: Atomic state writes — no partial files."""

    def test_no_partial_state_files(self, tmp_env):
        run_orchestrator(tmp_env, tmp_env["stories_3"])

        tmp_path = tmp_env["tmp_path"]
        partials = [
            f for f in os.listdir(tmp_path)
            if f.startswith("implement-epic-99999.json.tmp")
        ]
        assert len(partials) == 0, f"Found partial state files: {partials}"


class TestDependencyStorage:
    """Test 10: Dependencies field stored in state."""

    def test_dependencies_stored_in_state(self, tmp_env):
        """stories-input.json with dependencies field is correctly stored in state."""
        stories = [
            {"ref": 101, "id": 1001, "subject": "Base", "dependencies": []},
            {"ref": 102, "id": 1002, "subject": "Depends on base", "dependencies": [101]},
        ]
        stories_file = os.path.join(tmp_env["tmp_path"], "stories-deps.json")
        with open(stories_file, "w") as f:
            json.dump(stories, f)

        stdout, stderr, code = run_orchestrator(tmp_env, stories_file, concurrency=1)
        with open(tmp_env["state_file"]) as f:
            state = json.load(f)

        story_101 = next(s for s in state["stories"] if s["ref"] == 101)
        story_102 = next(s for s in state["stories"] if s["ref"] == 102)
        assert story_101.get("dependencies") == [], (
            f"Story 101 should have empty dependencies, got: {story_101}"
        )
        assert story_102.get("dependencies") == [101], (
            f"Story 102 should depend on 101, got: {story_102}"
        )


class TestDependencyGating:
    """Test 11: Dependency-aware queuing — stories wait for prerequisites."""

    def test_dependent_story_waits_for_prerequisite(self, tmp_env):
        """Story B (depends on A) doesn't launch until A completes."""
        stories = [
            {"ref": 101, "id": 1001, "subject": "Base", "dependencies": []},
            {"ref": 102, "id": 1002, "subject": "Depends on base", "dependencies": [101]},
        ]
        stories_file = os.path.join(tmp_env["tmp_path"], "stories-deps.json")
        with open(stories_file, "w") as f:
            json.dump(stories, f)

        stdout, stderr, code = run_orchestrator(tmp_env, stories_file, concurrency=2)
        events = parse_events(stdout)

        # Find the indices of key events
        completed_101_idx = None
        launching_102_idx = None
        for i, (etype, edata) in enumerate(events):
            if etype == "COMPLETED" and edata.startswith("101"):
                completed_101_idx = i
            if etype == "LAUNCHING" and "102" in edata:
                launching_102_idx = i

        assert completed_101_idx is not None, (
            f"Story 101 should complete. Events: {events}"
        )
        assert launching_102_idx is not None, (
            f"Story 102 should launch. Events: {events}"
        )
        assert completed_101_idx < launching_102_idx, (
            f"Story 101 must complete (idx={completed_101_idx}) before "
            f"102 launches (idx={launching_102_idx}). Events: {events}"
        )

    def test_independent_stories_run_concurrently(self, tmp_env):
        """Stories with no deps launch concurrently as before."""
        stories = [
            {"ref": 101, "id": 1001, "subject": "First", "dependencies": []},
            {"ref": 102, "id": 1002, "subject": "Second", "dependencies": []},
            {"ref": 103, "id": 1003, "subject": "Third", "dependencies": []},
        ]
        stories_file = os.path.join(tmp_env["tmp_path"], "stories-nodeps.json")
        with open(stories_file, "w") as f:
            json.dump(stories, f)

        stdout, stderr, code = run_orchestrator(tmp_env, stories_file, concurrency=2)
        events = parse_events(stdout)

        # First two stories should launch before any completion
        launching_events = [(i, e) for i, e in enumerate(events) if e[0] == "LAUNCHING"]
        first_completed_idx = next(
            (i for i, e in enumerate(events) if e[0] == "COMPLETED"), len(events)
        )
        # At least 2 launches should happen before any completion
        launches_before_completion = [i for i, _ in launching_events if i < first_completed_idx]
        assert len(launches_before_completion) >= 2, (
            f"Expected at least 2 launches before first completion, "
            f"got {len(launches_before_completion)}. Events: {events}"
        )

    def test_mixed_deps_and_no_deps(self, tmp_env):
        """Some stories have deps, some don't; independent ones launch freely."""
        stories = [
            {"ref": 101, "id": 1001, "subject": "Base A", "dependencies": []},
            {"ref": 102, "id": 1002, "subject": "Base B", "dependencies": []},
            {"ref": 103, "id": 1003, "subject": "Depends on A", "dependencies": [101]},
        ]
        stories_file = os.path.join(tmp_env["tmp_path"], "stories-mixed.json")
        with open(stories_file, "w") as f:
            json.dump(stories, f)

        stdout, stderr, code = run_orchestrator(tmp_env, stories_file, concurrency=2)
        events = parse_events(stdout)

        # 101 and 102 should both launch (no deps), 103 waits for 101
        completed = [e for e in events if e[0] == "COMPLETED"]
        assert len(completed) == 3, (
            f"All 3 stories should complete, got {len(completed)}. Events: {events}"
        )

        # 103 should launch only after 101 completes
        completed_101_idx = next(
            i for i, (t, d) in enumerate(events) if t == "COMPLETED" and d.startswith("101")
        )
        launching_103_idx = next(
            i for i, (t, d) in enumerate(events) if t == "LAUNCHING" and "103" in d
        )
        assert completed_101_idx < launching_103_idx, (
            f"Story 103 should wait for 101. Events: {events}"
        )


class TestCascadeSkip:
    """Test 12: Cascade skip — failed prerequisites cause dependents to skip."""

    def _make_failing_launch(self, tmp_env, failing_ref):
        """Make launch.sh fail permanently for a specific story ref."""
        mock_bin = tmp_env["mock_bin"]
        launch_script = os.path.join(mock_bin, "launch.sh")
        with open(launch_script, "w") as f:
            f.write(
                '#!/usr/bin/env bash\n'
                'CLONE_PATH="$1"\n'
                'STORY_NUM=$(basename "$CLONE_PATH" | sed "s/story-//")\n'
                f'if [[ "$STORY_NUM" == "{failing_ref}" ]]; then\n'
                '    echo "Permanent failure" >&2\n'
                '    exit 1\n'
                'fi\n'
                'mkdir -p "$CLONE_PATH/.claude" "$CLONE_PATH/.local/bin"\n'
                'echo "CONTAINER_ID=mock-container-$(basename "$CLONE_PATH")"\n'
            )
        os.chmod(launch_script, 0o755)

    def test_cascade_skip_on_error(self, tmp_env):
        """When A errors, B (depends on A) and C (depends on B) are both skipped."""
        stories = [
            {"ref": 101, "id": 1001, "subject": "Base (will fail)", "dependencies": []},
            {"ref": 102, "id": 1002, "subject": "Depends on 101", "dependencies": [101]},
            {"ref": 103, "id": 1003, "subject": "Depends on 102", "dependencies": [102]},
        ]
        stories_file = os.path.join(tmp_env["tmp_path"], "stories-cascade.json")
        with open(stories_file, "w") as f:
            json.dump(stories, f)

        self._make_failing_launch(tmp_env, 101)

        stdout, stderr, code = run_orchestrator(tmp_env, stories_file, concurrency=2)

        # Story 101 should end as error (launch_failed after retries)
        with open(tmp_env["state_file"]) as f:
            state = json.load(f)

        story_101 = next(s for s in state["stories"] if s["ref"] == 101)
        assert story_101["status"] == "error", (
            f"Story 101 should be error, got {story_101['status']}"
        )

        # Stories 102 and 103 should be skipped (cascade)
        story_102 = next(s for s in state["stories"] if s["ref"] == 102)
        story_103 = next(s for s in state["stories"] if s["ref"] == 103)
        assert story_102["status"] == "skipped", (
            f"Story 102 should be skipped, got {story_102['status']}"
        )
        assert story_103["status"] == "skipped", (
            f"Story 103 should be skipped, got {story_103['status']}"
        )

    def test_cascade_skip_on_escalation(self, tmp_env):
        """When A is escalated, B (depends on A) is skipped."""
        stories = [
            {"ref": 101, "id": 1001, "subject": "Base (will escalate)", "dependencies": []},
            {"ref": 102, "id": 1002, "subject": "Depends on 101", "dependencies": [101]},
        ]
        stories_file = os.path.join(tmp_env["tmp_path"], "stories-escalate.json")
        with open(stories_file, "w") as f:
            json.dump(stories, f)

        # Make claude report escalation for story 101
        mock_bin = tmp_env["mock_bin"]
        with open(os.path.join(mock_bin, "claude"), "w") as f:
            f.write(
                '#!/usr/bin/env bash\n'
                'PROMPT=$(cat)\n'
                'REF=$(echo "$PROMPT" | grep -oP "(?<=story )\\d+" | head -1)\n'
                'if [[ "$REF" == "101" ]]; then\n'
                '    echo \'{"phase":"escalated","prNumber":null}\''
                ' > "${ORCHESTRATE_STATE_DIR:-/tmp}/fully-implement-${REF}.json"\n'
                '    echo "escalation-needed"\n'
                'else\n'
                '    echo \'{"phase":"complete","prNumber":42,"featureBranch":"main"}\''
                ' > "${ORCHESTRATE_STATE_DIR:-/tmp}/fully-implement-${REF}.json"\n'
                '    echo "PR is ready for review"\n'
                'fi\n'
                'sleep 1\n'
                'exit 0\n'
            )
        os.chmod(os.path.join(mock_bin, "claude"), 0o755)

        stdout, stderr, code = run_orchestrator(tmp_env, stories_file, concurrency=2)

        with open(tmp_env["state_file"]) as f:
            state = json.load(f)

        story_101 = next(s for s in state["stories"] if s["ref"] == 101)
        story_102 = next(s for s in state["stories"] if s["ref"] == 102)
        assert story_101["status"] == "escalated", (
            f"Story 101 should be escalated, got {story_101['status']}"
        )
        assert story_102["status"] == "skipped", (
            f"Story 102 should be skipped, got {story_102['status']}"
        )

    def test_cascade_skip_emits_events(self, tmp_env):
        """SKIPPED events are emitted for each cascaded story."""
        stories = [
            {"ref": 101, "id": 1001, "subject": "Base (will fail)", "dependencies": []},
            {"ref": 102, "id": 1002, "subject": "Depends on 101", "dependencies": [101]},
            {"ref": 103, "id": 1003, "subject": "Depends on 102", "dependencies": [102]},
        ]
        stories_file = os.path.join(tmp_env["tmp_path"], "stories-skip-events.json")
        with open(stories_file, "w") as f:
            json.dump(stories, f)

        self._make_failing_launch(tmp_env, 101)

        stdout, stderr, code = run_orchestrator(tmp_env, stories_file, concurrency=2)
        events = parse_events(stdout)

        skipped_events = [e for e in events if e[0] == "SKIPPED"]
        skipped_refs = [e[1].split(":")[0] for e in skipped_events]
        assert "102" in skipped_refs, (
            f"Expected SKIPPED event for 102, got: {events}"
        )
        assert "103" in skipped_refs, (
            f"Expected SKIPPED event for 103, got: {events}"
        )

    def test_skipped_counts_in_done_event(self, tmp_env):
        """DONE event includes skipped count."""
        stories = [
            {"ref": 101, "id": 1001, "subject": "Base (will fail)", "dependencies": []},
            {"ref": 102, "id": 1002, "subject": "Depends on 101", "dependencies": [101]},
        ]
        stories_file = os.path.join(tmp_env["tmp_path"], "stories-done-skip.json")
        with open(stories_file, "w") as f:
            json.dump(stories, f)

        self._make_failing_launch(tmp_env, 101)

        stdout, stderr, code = run_orchestrator(tmp_env, stories_file, concurrency=2)
        events = parse_events(stdout)

        done_events = [e for e in events if e[0] == "DONE"]
        assert len(done_events) == 1, f"Expected exactly 1 DONE event, got: {events}"
        # DONE format: complete:escalated:errors:skipped
        parts = done_events[0][1].split(":")
        assert len(parts) == 4, (
            f"DONE event should have 4 fields (complete:escalated:errors:skipped), "
            f"got {len(parts)}: {done_events[0]}"
        )
        skipped_count = int(parts[3])
        assert skipped_count >= 1, (
            f"Expected at least 1 skipped, got {skipped_count}. Events: {events}"
        )


class TestNoDependenciesFallback:
    """Test 13: No dependencies field — falls back to ref-order."""

    def test_no_dependencies_field_falls_back(self, tmp_env):
        """Stories without 'dependencies' key behave like current (ref-order)."""
        # Use the existing stories-3.json which has no dependencies field
        stdout, stderr, code = run_orchestrator(tmp_env, tmp_env["stories_3"], concurrency=2)
        events = parse_events(stdout)

        # All 3 stories should complete
        completed = [e for e in events if e[0] == "COMPLETED"]
        assert len(completed) == 3, (
            f"All stories should complete, got {len(completed)}. Events: {events}"
        )

        # No SKIPPED events
        skipped = [e for e in events if e[0] == "SKIPPED"]
        assert len(skipped) == 0, (
            f"No stories should be skipped without deps, got: {events}"
        )


class TestCircularDependency:
    """Test 14: Circular dependency — detected, named, run aborted."""

    def test_circular_dependency_aborts(self, tmp_env):
        """Circular deps abort the run with a non-zero exit and no launches."""
        stories = [
            {"ref": 101, "id": 1001, "subject": "A depends on B", "dependencies": [102]},
            {"ref": 102, "id": 1002, "subject": "B depends on A", "dependencies": [101]},
        ]
        stories_file = os.path.join(tmp_env["tmp_path"], "stories-circular.json")
        with open(stories_file, "w") as f:
            json.dump(stories, f)

        stdout, stderr, code = run_orchestrator(tmp_env, stories_file, concurrency=2)
        events = parse_events(stdout)

        assert code != 0, f"Expected non-zero exit for circular deps, got {code}"

        completed = [e for e in events if e[0] == "COMPLETED"]
        assert len(completed) == 0, (
            f"No stories should launch when the dependency graph has a cycle, "
            f"got {len(completed)}. Events: {events}"
        )

        assert "circular dependency" in stderr.lower(), (
            f"Expected circular dependency error naming the cycle in stderr, got: {stderr}"
        )
        assert "#101" in stderr and "#102" in stderr, (
            f"Expected the cycle refs named in stderr, got: {stderr}"
        )


class TestDependencyGraphLogged:
    """A well-formed dependency graph is logged for auditability."""

    def test_dependency_graph_logged(self, tmp_env):
        stories = [
            {"ref": 101, "id": 1001, "subject": "First story", "dependencies": []},
            {"ref": 102, "id": 1002, "subject": "Second story", "dependencies": [101]},
        ]
        stories_file = os.path.join(tmp_env["tmp_path"], "stories-deps.json")
        with open(stories_file, "w") as f:
            json.dump(stories, f)

        stdout, stderr, code = run_orchestrator(tmp_env, stories_file, concurrency=2)

        assert "Dependency graph:" in stderr, f"Expected dependency graph log, got: {stderr}"
        assert "101" in stderr and "102" in stderr


class TestParseDependencyDiagram:
    """Test 15: Parse dependency diagram from epic description markdown."""

    def test_parses_epic_710_format(self):
        """Parse the tree-style dependency diagram from epic #710."""
        description = """\
## Goal

Apply the design system.

## Dependency Diagram

```
#727 (Done)
  └─► #712 (Status badges — shared StatusBadge.fs)
        └─► #714, #715, #719 (consumers)
  └─► #829 (Shared foundations — pageError, loadingSkeleton)
      └─► #714, #715, #716, #717, #718, #920 (all page stories)
      └─► #915 (Make pageError icon/CTA configurable)
            └─► #919 (Consolidate not-found views)
            └─► #920 Task 4 (error state needs inline pageError)
  └─► #713 (Navigation chrome — dropdown keyboard handler)
      └─► #827 (Mobile hamburger — reuses dropdown handler)
  └─► #828 (Footer — independent)

#716 (Create/edit forms — creates FormField wrapper)
  └─► #717 (Auth flows — consumes FormField)

#919 (Not-found consolidation) + #719 (Pledge views) — can run in parallel,
  #719 excludes not-found views (handled by #919)
```

## Implementation Phases

Some more text.
"""
        deps = parse_dependency_diagram(description)

        # #712 depends on #727
        assert 712 in deps
        assert 727 in deps[712]
        # #714 depends on #712 and #829
        assert 714 in deps
        assert 712 in deps[714]
        assert 829 in deps[714]
        # #715 depends on #712 and #829
        assert 715 in deps
        assert 712 in deps[715]
        assert 829 in deps[715]
        # #829 depends on #727
        assert 829 in deps
        assert 727 in deps[829]
        # #915 depends on #829
        assert 915 in deps
        assert 829 in deps[915]
        # #919 depends on #915
        assert 919 in deps
        assert 915 in deps[919]
        # #920 depends on #829 and #915
        assert 920 in deps
        assert 829 in deps[920]
        assert 915 in deps[920]
        # #713 depends on #727
        assert 713 in deps
        assert 727 in deps[713]
        # #827 depends on #713
        assert 827 in deps
        assert 713 in deps[827]
        # #828 depends on #727
        assert 828 in deps
        assert 727 in deps[828]
        # #717 depends on #716 (from second tree) AND on #829 (from first tree)
        assert 717 in deps
        assert 716 in deps[717]
        assert 829 in deps[717]

    # Exhaustive parse_dependency_diagram coverage (no section, no code block,
    # alternative headings, comma-separated refs, root nodes, empty input) lives in
    # test_dependency_parser.py, which owns scripts/lib/dependency_parser.py.
    # orchestrate_epic consumes that function, so only the contract check above
    # belongs here (testing-strategy.md "Business Rule Ownership").


class TestNoWorkCompletion:
    """Test 16: Agent exits with 'complete' phase but no PR — should be error, not complete."""

    # determine_outcome's own branches (state file, log-scan fallback) are owned by
    # test_orchestrate_epic.py; this class covers only the orchestrator behaviour that
    # sits on top of them (testing-strategy.md "Business Rule Ownership").

    def test_check_completions_downgrades_complete_without_pr(self, tmp_env):
        """Integration test: orchestrator downgrades complete-without-PR to error."""
        # Mock claude writes a state file saying complete but with no PR number.
        # The mock also writes "ready for review" to stdout (which goes to log),
        # so determine_outcome's log-scan fallback returns "complete" with pr="none".
        mock_bin = tmp_env["mock_bin"]
        with open(os.path.join(mock_bin, "claude"), "w") as f:
            f.write(
                '#!/usr/bin/env bash\n'
                'PROMPT=$(cat)\n'
                '# Write "ready for review" so log-scan returns complete\n'
                'echo "PR is ready for review"\n'
                'sleep 1\n'
                'exit 0\n'
            )
        os.chmod(os.path.join(mock_bin, "claude"), 0o755)

        stdout, stderr, code = run_orchestrator(tmp_env, tmp_env["stories_1"], concurrency=1)
        events = parse_events(stdout)

        with open(tmp_env["state_file"]) as f:
            state = json.load(f)

        story_101 = next(s for s in state["stories"] if s["ref"] == 101)
        assert story_101["status"] == "error", (
            f"Story with 'ready for review' in log but no PR should be error, "
            f"got {story_101['status']}. Events: {events}"
        )
        assert story_101["outcome"] == "no_pr_created", (
            f"Outcome should be 'no_pr_created', got {story_101.get('outcome')}. Events: {events}"
        )

    def test_no_work_cascades_skip_dependents(self, tmp_env):
        """A no-work completion should cascade-skip dependent stories."""
        mock_bin = tmp_env["mock_bin"]
        # Story 101: "ready for review" but no PR; story 102 depends on 101
        with open(os.path.join(mock_bin, "claude"), "w") as f:
            f.write(
                '#!/usr/bin/env bash\n'
                'PROMPT=$(cat)\n'
                'REF=$(echo "$PROMPT" | grep -oP "(?<=story )\\d+" | head -1)\n'
                'if [[ "$REF" == "101" ]]; then\n'
                '    echo "PR is ready for review"\n'
                'else\n'
                '    echo \'{"phase":"complete","prNumber":42,"featureBranch":"main"}\''
                ' > "${ORCHESTRATE_STATE_DIR:-/tmp}/fully-implement-${REF}.json"\n'
                '    echo "PR is ready for review"\n'
                'fi\n'
                'sleep 1\n'
                'exit 0\n'
            )
        os.chmod(os.path.join(mock_bin, "claude"), 0o755)

        stories = [
            {"ref": 101, "id": 1001, "subject": "Base (no work)", "dependencies": []},
            {"ref": 102, "id": 1002, "subject": "Depends on 101", "dependencies": [101]},
        ]
        stories_file = os.path.join(tmp_env["tmp_path"], "stories-nowork-cascade.json")
        with open(stories_file, "w") as f:
            json.dump(stories, f)

        stdout, stderr, code = run_orchestrator(tmp_env, stories_file, concurrency=2)

        with open(tmp_env["state_file"]) as f:
            state = json.load(f)

        story_101 = next(s for s in state["stories"] if s["ref"] == 101)
        story_102 = next(s for s in state["stories"] if s["ref"] == 102)

        assert story_101["status"] == "error", (
            f"Story 101 should be error (no PR), got {story_101['status']}"
        )
        assert story_102["status"] == "skipped", (
            f"Story 102 should be skipped (depends on failed 101), got {story_102['status']}"
        )


class TestHandoffCompletion:
    """Test 18: Clean handoff — agent exits in monitoring phase with a PR (story #2095).

    A story-agent that creates a PR and exits while the state file is still at
    phase="monitoring" is a clean handoff, not an escalation. It must be scored
    status="complete", outcome="handed_off", so dependents unblock.
    """

    def test_monitoring_handoff_completes_and_unblocks_dependent(self, tmp_env):
        """Story A exits in monitoring+PR → handed_off; dependent B runs and completes."""
        stories = [
            {"ref": 101, "id": 1001, "subject": "Base (hands off)", "dependencies": []},
            {"ref": 102, "id": 1002, "subject": "Depends on 101", "dependencies": [101]},
        ]
        stories_file = os.path.join(tmp_env["tmp_path"], "stories-handoff.json")
        with open(stories_file, "w") as f:
            json.dump(stories, f)

        # Story 101 exits in the monitoring phase WITH a PR (the clean-handoff
        # state). Story 102 exits complete. Both write a distinct PR number.
        mock_bin = tmp_env["mock_bin"]
        with open(os.path.join(mock_bin, "claude"), "w") as f:
            f.write(
                '#!/usr/bin/env bash\n'
                'PROMPT=$(cat)\n'
                'REF=$(echo "$PROMPT" | grep -oP "(?<=story )\\d+" | head -1)\n'
                'if [[ "$REF" == "101" ]]; then\n'
                '    echo \'{"phase":"monitoring","prNumber":768,"featureBranch":"main"}\''
                ' > "${ORCHESTRATE_STATE_DIR:-/tmp}/fully-implement-${REF}.json"\n'
                'else\n'
                '    echo \'{"phase":"complete","prNumber":42,"featureBranch":"main"}\''
                ' > "${ORCHESTRATE_STATE_DIR:-/tmp}/fully-implement-${REF}.json"\n'
                'fi\n'
                'sleep 1\n'
                'exit 0\n'
            )
        os.chmod(os.path.join(mock_bin, "claude"), 0o755)

        stdout, stderr, code = run_orchestrator(tmp_env, stories_file, concurrency=2)
        events = parse_events(stdout)

        # Story 101 ends complete-status with handed_off outcome
        with open(tmp_env["state_file"]) as f:
            state = json.load(f)
        story_101 = next(s for s in state["stories"] if s["ref"] == 101)
        story_102 = next(s for s in state["stories"] if s["ref"] == 102)
        assert story_101["status"] == "complete", (
            f"Story 101 should be complete status, got {story_101['status']}. Events: {events}"
        )
        assert story_101["outcome"] == "handed_off", (
            f"Story 101 outcome should be handed_off, got {story_101.get('outcome')}. Events: {events}"
        )

        # COMPLETED:101:handed_off:768 event emitted
        completed_101 = [
            e for e in events if e[0] == "COMPLETED" and e[1].startswith("101:")
        ]
        assert completed_101, f"Expected COMPLETED event for 101. Events: {events}"
        assert completed_101[0][1] == "101:handed_off:768", (
            f"Expected COMPLETED:101:handed_off:768, got COMPLETED:{completed_101[0][1]}"
        )

        # Dependent 102 unblocked, launched, and completed — no SKIPPED events
        assert story_102["status"] == "complete", (
            f"Story 102 should complete (unblocked by handoff), got {story_102['status']}"
        )
        skipped = [e for e in events if e[0] == "SKIPPED"]
        assert len(skipped) == 0, (
            f"No stories should be skipped after a clean handoff, got: {skipped}"
        )

        # Final DONE:2:0:0:0 (both counted complete, none escalated/error/skipped)
        done_events = [e for e in events if e[0] == "DONE"]
        assert len(done_events) == 1, f"Expected exactly 1 DONE event, got: {events}"
        assert done_events[0][1] == "2:0:0:0", (
            f"Expected DONE:2:0:0:0, got DONE:{done_events[0][1]}. Events: {events}"
        )


class TestFinalCleanup:
    """Test 17: Final cleanup — all stories' containers torn down on completion."""

    def _make_orchestrator(self, tmp_path, stories, concurrency=2):
        """Create an Orchestrator with initialized state for unit testing."""
        epic_ref = "99999"
        mock_bin = tmp_path / "mock-bin"
        mock_bin.mkdir(exist_ok=True)

        # Create mock teardown.sh that records calls
        teardown_log = tmp_path / "teardown-calls.log"
        (mock_bin / "teardown.sh").write_text(
            "#!/usr/bin/env bash\n"
            f"echo \"$1\" >> \"{teardown_log}\"\n"
        )
        (mock_bin / "teardown.sh").chmod(0o755)

        stories_file = tmp_path / "stories.json"
        stories_file.write_text(json.dumps(stories))

        orch = Orchestrator(
            epic_ref, concurrency, str(stories_file),
            str(mock_bin), str(tmp_path), "main",
        )
        base_dir = tmp_path / f"epic-{epic_ref}"
        base_dir.mkdir(parents=True, exist_ok=True)
        orch.sm.init_from_stories(epic_ref, concurrency, stories, base_dir)
        return orch, teardown_log

    def test_final_cleanup_tears_down_all_stories(self, tmp_path):
        """Verify _final_cleanup() calls _teardown() for every story."""
        stories = [
            {"ref": 101, "id": 1001, "subject": "A"},
            {"ref": 102, "id": 1002, "subject": "B"},
            {"ref": 103, "id": 1003, "subject": "C"},
        ]
        orch, teardown_log = self._make_orchestrator(tmp_path, stories)

        # Set stories to various terminal states (simulating end-of-run)
        orch.sm.update_story(101, status="complete", outcome="complete")
        orch.sm.update_story(102, status="error", outcome="launch_failed")
        orch.sm.update_story(103, status="escalated", outcome="escalated")

        orch._final_cleanup()

        assert teardown_log.exists(), "teardown.sh should have been called"
        calls = teardown_log.read_text().strip().split("\n")
        # Should have called teardown for all 3 stories
        assert len(calls) == 3, (
            f"Expected 3 teardown calls (one per story), got {len(calls)}: {calls}"
        )

    def test_final_cleanup_removes_by_container_id(self, tmp_path):
        """Verify docker rm -f is attempted when story has containerId."""
        stories = [
            {"ref": 101, "id": 1001, "subject": "A"},
        ]
        orch, teardown_log = self._make_orchestrator(tmp_path, stories)

        # Set a containerId on the story
        orch.sm.update_story(101, status="complete", outcome="complete",
                             containerId="abc123deadbeef")

        # Mock subprocess.run to capture docker rm -f calls
        original_run = subprocess.run
        docker_rm_calls = []

        def mock_run(cmd, **kwargs):
            if isinstance(cmd, list) and len(cmd) >= 3 and cmd[:3] == ["docker", "rm", "-f"]:
                docker_rm_calls.append(cmd)
                # Return success
                return subprocess.CompletedProcess(cmd, 0)
            return original_run(cmd, **kwargs)

        subprocess.run = mock_run
        try:
            orch._final_cleanup()
        finally:
            subprocess.run = original_run

        assert len(docker_rm_calls) == 1, (
            f"Expected 1 docker rm -f call, got {len(docker_rm_calls)}: {docker_rm_calls}"
        )
        assert "abc123deadbeef" in docker_rm_calls[0], (
            f"docker rm -f should target container abc123deadbeef, got: {docker_rm_calls[0]}"
        )

    def test_run_calls_final_cleanup_on_completion(self, tmp_env):
        """Verify run() calls _final_cleanup() before setting phase to complete.

        We replace teardown.sh with a version that logs each call path.
        check_completions() calls _teardown() once per story as it completes.
        _final_cleanup() calls _teardown() once per story as a final sweep.
        So each story's clone path should appear at least twice in the log.
        """
        mock_bin = tmp_env["mock_bin"]
        teardown_log = os.path.join(tmp_env["tmp_path"], "teardown-calls.log")
        with open(os.path.join(mock_bin, "teardown.sh"), "w") as f:
            f.write(
                "#!/usr/bin/env bash\n"
                f"echo \"$1\" >> \"{teardown_log}\"\n"
            )
        os.chmod(os.path.join(mock_bin, "teardown.sh"), 0o755)

        stdout, stderr, code = run_orchestrator(tmp_env, tmp_env["stories_1"], concurrency=1)
        events = parse_events(stdout)

        done_events = [e for e in events if e[0] == "DONE"]
        assert len(done_events) == 1, f"Expected DONE event, got: {events}"

        # Teardown should have been called at least twice for story 101:
        # once by check_completions, once by _final_cleanup
        assert os.path.exists(teardown_log), (
            f"teardown log should exist. stderr: {stderr}"
        )
        with open(teardown_log) as f:
            calls = [line.strip() for line in f if line.strip()]
        story_101_calls = [c for c in calls if "story-101" in c]
        assert len(story_101_calls) >= 2, (
            f"Expected at least 2 teardown calls for story-101 "
            f"(check_completions + final_cleanup), got {len(story_101_calls)}: {calls}"
        )

    def test_signal_handler_calls_final_cleanup(self, tmp_env):
        """Verify signal handler calls _final_cleanup() after cleaning active/launching.

        We replace teardown.sh with a logging version and check that ALL stories
        get a teardown call — not just the active one. The signal handler's
        _cleanup_active() handles the active story; _final_cleanup() should sweep all.
        """
        mock_bin = tmp_env["mock_bin"]
        teardown_log = os.path.join(tmp_env["tmp_path"], "teardown-calls.log")
        with open(os.path.join(mock_bin, "teardown.sh"), "w") as f:
            f.write(
                "#!/usr/bin/env bash\n"
                f"echo \"$1\" >> \"{teardown_log}\"\n"
            )
        os.chmod(os.path.join(mock_bin, "teardown.sh"), 0o755)

        with open(os.path.join(mock_bin, "claude"), "w") as f:
            f.write("#!/usr/bin/env bash\ncat\nsleep 60\n")
        os.chmod(os.path.join(mock_bin, "claude"), 0o755)

        env = os.environ.copy()
        env["ORCHESTRATE_SCRIPT_DIR"] = mock_bin
        env["ORCHESTRATE_STATE_DIR"] = tmp_env["tmp_path"]
        env["POLL_INTERVAL"] = "2"

        # Use 3 stories with concurrency 1 so stories 102 and 103 stay queued
        proc = subprocess.Popen(
            [sys.executable, ORCHESTRATE, "99999", "1", tmp_env["stories_3"]],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, env=env,
        )

        # Wait for story to become active
        deadline = time.time() + 15
        state_file = tmp_env["state_file"]
        while time.time() < deadline:
            if os.path.exists(state_file):
                try:
                    with open(state_file) as f:
                        state = json.load(f)
                    active = [s for s in state["stories"] if s["status"] == "active"]
                    if active:
                        break
                except (json.JSONDecodeError, OSError):
                    pass
            time.sleep(0.5)

        # Send SIGTERM
        proc.send_signal(signal.SIGTERM)

        try:
            stdout, stderr = proc.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, stderr = proc.communicate()

        assert proc.returncode == 2, (
            f"Expected exit code 2, got {proc.returncode}. stderr: {stderr}"
        )

        # _final_cleanup() should have called teardown for ALL 3 stories,
        # not just the 1 active story that _cleanup_active() handled.
        assert os.path.exists(teardown_log), (
            f"teardown log should exist. stderr: {stderr}"
        )
        with open(teardown_log) as f:
            calls = [line.strip() for line in f if line.strip()]
        # Each of the 3 stories should appear at least once in the teardown log
        for ref in [101, 102, 103]:
            story_calls = [c for c in calls if f"story-{ref}" in c]
            assert len(story_calls) >= 1, (
                f"Expected at least 1 teardown call for story-{ref} "
                f"(from _final_cleanup sweep), got {len(story_calls)}. All calls: {calls}"
            )


class TestLaunchContainment:
    """Bug #2465: a launch that times out or fails must be fully contained —
    the launch script's process tree is killed, teardown failures are logged,
    and a retry never reuses the failed attempt's clone."""

    def test_launch_retry_starts_from_a_fresh_clone(self, tmp_env):
        """launch.sh only clones when the directory is missing, so the orchestrator
        must remove the previous attempt's clone before requeueing."""
        mock_bin = tmp_env["mock_bin"]
        tmp_path = tmp_env["tmp_path"]
        counter_file = os.path.join(tmp_path, "launch-counter-101")
        existed_file = os.path.join(tmp_path, "clone-existed-on-retry")

        with open(os.path.join(mock_bin, "launch.sh"), "w") as f:
            f.write(
                '#!/usr/bin/env bash\n'
                'CLONE_PATH="$1"\n'
                'STORY_NUM=$(basename "$CLONE_PATH" | sed "s/story-//")\n'
                f'COUNTER_FILE="{counter_file}"\n'
                'if [[ "$STORY_NUM" == "101" ]]; then\n'
                '    COUNT=0\n'
                '    [[ -f "$COUNTER_FILE" ]] && COUNT=$(cat "$COUNTER_FILE")\n'
                '    COUNT=$((COUNT + 1))\n'
                '    echo "$COUNT" > "$COUNTER_FILE"\n'
                '    if [[ "$COUNT" -eq 1 ]]; then\n'
                '        mkdir -p "$CLONE_PATH/.git"\n'
                '        echo dirty > "$CLONE_PATH/dirty.txt"\n'
                '        echo "Simulated failure after partial clone" >&2\n'
                '        exit 1\n'
                '    fi\n'
                f'    [[ -d "$CLONE_PATH" ]] && echo yes > "{existed_file}"\n'
                'fi\n'
                'mkdir -p "$CLONE_PATH/.claude" "$CLONE_PATH/.local/bin"\n'
                'echo "CONTAINER_ID=mock-container-$(basename "$CLONE_PATH")"\n'
            )
        os.chmod(os.path.join(mock_bin, "launch.sh"), 0o755)

        # A teardown that stops containers but leaves the clone on disk
        with open(os.path.join(mock_bin, "teardown.sh"), "w") as f:
            f.write('#!/usr/bin/env bash\necho "Teardown complete."\n')

        stdout, stderr, code = run_orchestrator(tmp_env, tmp_env["stories_1"])
        events = parse_events(stdout)

        assert int(open(counter_file).read()) >= 2, f"101 was not retried: {events}"
        assert not os.path.exists(existed_file), (
            "retry launched over the previous attempt's clone instead of re-cloning"
        )
        completed = [e for e in events if e[0] == "COMPLETED"]
        assert len(completed) == 1, f"101 should complete after retry: {events}"

    def test_launch_timeout_kills_launch_script_and_requeues(self, tmp_env):
        """ORCHESTRATE_LAUNCH_TIMEOUT is enforced even when a grandchild keeps
        launch.sh's stdout open (the devcontainer up / MSBuild hang)."""
        mock_bin = tmp_env["mock_bin"]
        tmp_path = tmp_env["tmp_path"]
        pid_file = os.path.join(tmp_path, "marker.pid")

        with open(os.path.join(mock_bin, "launch.sh"), "w") as f:
            f.write(
                '#!/usr/bin/env bash\n'
                'CLONE_PATH="$1"\n'
                'mkdir -p "$CLONE_PATH/.claude" "$CLONE_PATH/.local/bin"\n'
                f'if [[ ! -f "{pid_file}" ]]; then\n'
                '    sleep 120 &\n'
                f'    echo $! > "{pid_file}"\n'
                '    wait\n'
                'fi\n'
                'echo "CONTAINER_ID=mock-container-$(basename "$CLONE_PATH")"\n'
            )
        os.chmod(os.path.join(mock_bin, "launch.sh"), 0o755)

        started = time.monotonic()
        stdout, stderr, code = run_orchestrator(
            tmp_env, tmp_env["stories_1"], timeout=90,
            extra_env={"ORCHESTRATE_LAUNCH_TIMEOUT": "2"},
        )
        elapsed = time.monotonic() - started
        events = parse_events(stdout)

        assert ("LAUNCH_FAIL", "101:devcontainer launch timed out after 2s") in events, (
            f"expected timeout LAUNCH_FAIL, got: {events}"
        )
        assert elapsed < 60, f"launch timeout was not enforced promptly ({elapsed:.1f}s)"

        marker_pid = int(open(pid_file).read().strip())
        try:
            os.kill(marker_pid, 0)
            os.kill(marker_pid, signal.SIGKILL)
            survived = True
        except (OSError, ProcessLookupError):
            survived = False
        assert not survived, "backgrounded grandchild outlived the launch timeout"

        completed = [e for e in events if e[0] == "COMPLETED"]
        assert len(completed) == 1, f"101 should complete on retry: {events}"

    def test_teardown_failure_is_logged(self, tmp_env):
        """A failing teardown.sh must produce a WARNING naming the clone path."""
        mock_bin = tmp_env["mock_bin"]
        with open(os.path.join(mock_bin, "teardown.sh"), "w") as f:
            f.write('#!/usr/bin/env bash\necho "could not stop container" >&2\nexit 1\n')

        stdout, stderr, code = run_orchestrator(tmp_env, tmp_env["stories_1"])

        clone_path = os.path.join(tmp_env["base_dir"], "story-101")
        warnings = [line for line in stderr.splitlines()
                    if "WARNING: teardown.sh failed" in line and clone_path in line]
        assert warnings, f"no teardown failure warning in stderr:\n{stderr}"
        assert "could not stop container" in warnings[0]


class TestRateLimitedAgentExit:
    """Bug #2439: a provider rate limit that kills the agent is transient, not a
    story failure — the story is requeued, its clone kept, and no dependents skip.

    One end-to-end run through the poll loop; the per-path behaviour (budget
    exhaustion, dead container, launch failure on resume) is pinned by the unit
    tests in tests/scripts/test_orchestrate_epic.py.
    """

    # A session-limit result with no rate_limit_event in the log, so the
    # orchestrator falls back to its default backoff, shortened via env.
    RATE_LIMIT_RESULT = (
        '{"type":"result","subtype":"success","is_error":true,'
        '"terminal_reason":"api_error","result":"You\'ve hit your session limit"}'
    )

    def _dependent_pair(self, tmp_env):
        stories = [
            {"ref": 101, "id": 1001, "subject": "Base (rate limited)", "dependencies": []},
            {"ref": 102, "id": 1002, "subject": "Depends on 101", "dependencies": [101]},
        ]
        stories_file = os.path.join(tmp_env["tmp_path"], "stories-rate-limit.json")
        with open(stories_file, "w") as f:
            json.dump(stories, f)
        return stories_file

    def _make_rate_limiting_claude(self, tmp_env, ref, failures):
        """Mock claude that rate-limits story `ref` for its first `failures` runs
        and records every prompt it receives. Later runs complete normally,
        exactly like the default tmp_env mock.
        """
        counter_file = os.path.join(tmp_env["tmp_path"], f"agent-counter-{ref}")
        prompts_file = os.path.join(tmp_env["tmp_path"], "prompts.log")
        claude = os.path.join(tmp_env["mock_bin"], "claude")
        with open(claude, "w") as f:
            f.write(
                '#!/usr/bin/env bash\n'
                'PROMPT=$(cat)\n'
                f'echo "$PROMPT" >> "{prompts_file}"\n'
                'REF=$(echo "$PROMPT" | grep -oP "(?<=story )\\d+" | head -1)\n'
                f'if [[ "$REF" == "{ref}" ]]; then\n'
                '    COUNT=0\n'
                f'    [[ -f "{counter_file}" ]] && COUNT=$(cat "{counter_file}")\n'
                '    COUNT=$((COUNT + 1))\n'
                f'    echo "$COUNT" > "{counter_file}"\n'
                f'    if [[ "$COUNT" -le {failures} ]]; then\n'
                '        echo \'{"type":"system","subtype":"init"}\'\n'
                f"        echo {shlex.quote(self.RATE_LIMIT_RESULT)}\n"
                '        exit 1\n'
                '    fi\n'
                'fi\n'
                'echo \'{"phase":"complete","prNumber":42,"featureBranch":"main"}\' > "${ORCHESTRATE_STATE_DIR:-/tmp}/fully-implement-${REF}.json"\n'
                'echo "PR is ready for review"\n'
                'exit 0\n'
            )
        os.chmod(claude, 0o755)
        return prompts_file

    def _record_launch_and_teardown_calls(self, tmp_env):
        """Make launch.sh, prepare-clone.sh and teardown.sh append to one ordered
        call log, and answer `docker inspect` as "running" so the relaunch reuses
        the recorded container instead of re-running launch.sh."""
        call_log = os.path.join(tmp_env["tmp_path"], "clone-calls.log")
        with open(os.path.join(tmp_env["mock_bin"], "launch.sh"), "w") as f:
            f.write(
                '#!/usr/bin/env bash\n'
                'CLONE_PATH="$1"\n'
                'STATE=missing; [[ -d "$CLONE_PATH" ]] && STATE=existed\n'
                f'echo "launch:$(basename "$CLONE_PATH"):$STATE" >> "{call_log}"\n'
                'mkdir -p "$CLONE_PATH/.claude" "$CLONE_PATH/.local/bin"\n'
                'echo "CONTAINER_ID=mock-container-$(basename "$CLONE_PATH")"\n'
            )
        os.chmod(os.path.join(tmp_env["mock_bin"], "launch.sh"), 0o755)
        prepare = os.path.join(tmp_env["mock_bin"], "prepare-clone.sh")
        with open(prepare) as f:
            prepare_body = f.read()
        with open(prepare, "w") as f:
            f.write(prepare_body + f'echo "prepare:$(basename "$1")" >> "{call_log}"\n')
        with open(os.path.join(tmp_env["mock_bin"], "teardown.sh"), "w") as f:
            f.write(
                '#!/usr/bin/env bash\n'
                f'echo "teardown:$(basename "$1"):${{2:-}}" >> "{call_log}"\n'
                '[[ -d "$1" ]] && rm -rf "$1"\n'
                'echo "Teardown complete."\n'
            )
        os.chmod(os.path.join(tmp_env["mock_bin"], "teardown.sh"), 0o755)
        with open(os.path.join(tmp_env["mock_bin"], "docker"), "w") as f:
            f.write(
                '#!/usr/bin/env bash\n'
                '[[ "$1" == "inspect" ]] && echo true\n'
                'exit 0\n'
            )
        os.chmod(os.path.join(tmp_env["mock_bin"], "docker"), 0o755)
        return call_log

    def test_rate_limited_story_resumes_in_place_and_completes(self, tmp_env):
        stories_file = self._dependent_pair(tmp_env)
        prompts_file = self._make_rate_limiting_claude(tmp_env, 101, failures=1)
        call_log = self._record_launch_and_teardown_calls(tmp_env)

        stdout, stderr, code = run_orchestrator(
            tmp_env, stories_file, concurrency=2,
            extra_env={
                "ORCHESTRATE_TRANSIENT_BACKOFF": "1",
                "PATH": f"{tmp_env['mock_bin']}{os.pathsep}{os.environ['PATH']}",
            },
        )
        events = parse_events(stdout)

        rate_limited = [e for e in events if e[0] == "RATE_LIMITED"]
        assert len(rate_limited) == 1, f"Expected one RATE_LIMITED event, got: {events}"
        assert rate_limited[0][1].startswith("101:"), rate_limited
        assert not [e for e in events if e[0] == "SKIPPED"], (
            f"Dependent must not be skipped on a transient exit: {events}"
        )
        assert events[-1] == ("DONE", "2:0:0:0"), f"Events: {events}\nStderr: {stderr}"

        with open(tmp_env["state_file"]) as f:
            state = json.load(f)
        story_101 = next(s for s in state["stories"] if s["ref"] == 101)
        assert story_101["status"] == "complete"
        assert story_101["transientRetries"] == 1
        assert story_101["holdsPreservedWork"] is False, "marker clears once the clone is removed"

        with open(call_log) as f:
            calls_101 = [line.strip() for line in f if "story-101" in line]
        # The relaunch reuses the running container: launch.sh and prepare-clone.sh
        # (whose clean-tree step would discard the agent's uncommitted edits) run
        # once, and the clone is only torn down after the story completes (then
        # swept again by the run's final cleanup).
        assert calls_101[:3] == [
            "launch:story-101:missing", "prepare:story-101", "teardown:story-101:",
        ], f"Calls: {calls_101}"
        assert all(c.startswith("teardown:") for c in calls_101[3:]), f"Calls: {calls_101}"

        with open(prompts_file) as f:
            prompts_101 = [line.strip() for line in f if "story 101" in line]
        assert prompts_101 == ["Implement story 101 main", "Implement story 101 main --resume"], (
            f"Relaunched agent must be told it is resuming. Prompts: {prompts_101}"
        )


class TestContainerModeWiring:
    """Story #75: the orchestrators run each item through exec-skill.sh in a
    container, and record the mode that wrote the state file."""

    def test_two_stories_concurrency_two_complete(self, tmp_env):
        stories = [
            {"ref": 101, "id": 1001, "subject": "First story"},
            {"ref": 102, "id": 1002, "subject": "Second story"},
        ]
        stories_file = os.path.join(tmp_env["tmp_path"], "stories-two.json")
        with open(stories_file, "w") as f:
            json.dump(stories, f)

        stdout, stderr, code = run_orchestrator(tmp_env, stories_file, concurrency=2)
        events = parse_events(stdout)

        assert code == 0, stderr
        assert events[-1] == ("DONE", "2:0:0:0"), f"Events: {events}\nStderr: {stderr}"
        with open(tmp_env["state_file"]) as f:
            state = json.load(f)
        assert state["mode"] == "container"
        for story in state["stories"]:
            assert story["containerId"].startswith("mock-container-story-")
            assert story["clonePath"] and story["logFile"]

    def test_agent_runs_through_exec_skill_with_base_branch(self, tmp_env):
        run_orchestrator(tmp_env, tmp_env["stories_1"], concurrency=1)

        with open(tmp_env["calls_log"]) as f:
            calls = [line.strip() for line in f if line.strip()]
        assert calls == ["fully-implement-story|101 main"]

    def test_rate_limited_story_relaunches_with_resume(self, tmp_env):
        counter_file = os.path.join(tmp_env["tmp_path"], "agent-counter")
        with open(os.path.join(tmp_env["mock_bin"], "claude"), "w") as f:
            f.write(
                '#!/usr/bin/env bash\n'
                'PROMPT=$(cat)\n'
                'REF=$(echo "$PROMPT" | grep -oP "(?<=story )\\d+" | head -1)\n'
                'COUNT=0\n'
                f'[[ -f "{counter_file}" ]] && COUNT=$(cat "{counter_file}")\n'
                f'echo $((COUNT + 1)) > "{counter_file}"\n'
                'if [[ "$COUNT" -eq 0 ]]; then\n'
                '    echo \'{"type":"result","subtype":"success","is_error":true,'
                '"terminal_reason":"api_error","result":"You have hit your session limit"}\'\n'
                '    exit 1\n'
                'fi\n'
                'echo \'{"phase":"complete","prNumber":42,"featureBranch":"main"}\''
                ' > "${ORCHESTRATE_STATE_DIR:-/tmp}/fully-implement-${REF}.json"\n'
                'echo "PR is ready for review"\n'
            )
        os.chmod(os.path.join(tmp_env["mock_bin"], "claude"), 0o755)

        stdout, stderr, code = run_orchestrator(
            tmp_env, tmp_env["stories_1"], concurrency=1,
            extra_env={"ORCHESTRATE_TRANSIENT_BACKOFF": "1"},
        )
        events = parse_events(stdout)

        assert [e for e in events if e[0] == "RATE_LIMITED"], f"Events: {events}"
        assert events[-1] == ("DONE", "1:0:0:0"), f"Events: {events}\nStderr: {stderr}"
        with open(tmp_env["calls_log"]) as f:
            calls = [line.strip() for line in f if line.strip()]
        assert calls == [
            "fully-implement-story|101 main",
            "fully-implement-story|101 main --resume",
        ]


class TestDryRun:
    """Story #75: --dry-run smokes each container's stack through exec.sh and
    keeps every container and clone for the isolation check."""

    def test_dry_run_concurrency_two_keeps_both_clones(self, tmp_env):
        stories = [
            {"ref": 101, "id": 1001, "subject": "First story"},
            {"ref": 102, "id": 1002, "subject": "Second story"},
        ]
        stories_file = os.path.join(tmp_env["tmp_path"], "stories-dry.json")
        with open(stories_file, "w") as f:
            json.dump(stories, f)
        teardown_log = os.path.join(tmp_env["tmp_path"], "teardown-calls.log")
        with open(os.path.join(tmp_env["mock_bin"], "teardown.sh"), "w") as f:
            f.write(f'#!/usr/bin/env bash\necho "$1" >> "{teardown_log}"\n')
        os.chmod(os.path.join(tmp_env["mock_bin"], "teardown.sh"), 0o755)

        stdout, stderr, code = run_orchestrator(
            tmp_env, stories_file, concurrency=2,
            extra_args=["--dry-run"],
        )
        events = parse_events(stdout)

        assert code == 0, stderr
        assert events[-1] == ("DONE", "2:0:0:0"), f"Events: {events}\nStderr: {stderr}"
        for ref in (101, 102):
            assert os.path.isdir(os.path.join(tmp_env["base_dir"], f"story-{ref}"))
        assert not os.path.exists(teardown_log), "a dry run must keep every container"
        assert not os.path.exists(tmp_env["calls_log"]), "a dry run must not start the agent"
        with open(tmp_env["exec_log"]) as f:
            smoke_calls = [line for line in f if line.strip()]
        assert len(smoke_calls) == 2
        assert all("supabase db reset" in line for line in smoke_calls)
        with open(tmp_env["state_file"]) as f:
            state = json.load(f)
        assert {s["outcome"] for s in state["stories"]} == {"dry_run_ok"}


class TestBugsOrchestrator:
    """Story #75: orchestrate_bugs.py runs fix-and-remediate-bug through exec-skill.sh."""

    def test_bugs_orchestrator_two_bugs_complete(self, tmp_env):
        with open(os.path.join(tmp_env["mock_bin"], "claude"), "w") as f:
            f.write(
                '#!/usr/bin/env bash\n'
                'PROMPT=$(cat)\n'
                'REF=$(echo "$PROMPT" | grep -oP "(?<=bug )\\d+" | head -1)\n'
                'echo \'{"phase":"complete","prNumber":43,"branch":"fix/\'$REF\'-x"}\''
                ' > "${ORCHESTRATE_STATE_DIR:-/tmp}/fix-and-remediate-${REF}.json"\n'
            )
        os.chmod(os.path.join(tmp_env["mock_bin"], "claude"), 0o755)
        bugs_file = os.path.join(tmp_env["tmp_path"], "bugs.json")
        with open(bugs_file, "w") as f:
            json.dump([
                {"ref": 201, "id": 2001, "subject": "First bug"},
                {"ref": 202, "id": 2002, "subject": "Second bug"},
            ], f)

        env = os.environ.copy()
        env["ORCHESTRATE_SCRIPT_DIR"] = tmp_env["mock_bin"]
        env["ORCHESTRATE_STATE_DIR"] = tmp_env["tmp_path"]
        env["POLL_INTERVAL"] = "1"
        result = subprocess.run(
            [sys.executable, ORCHESTRATE_BUGS, "2", bugs_file],
            capture_output=True, text=True, timeout=60, env=env,
        )
        events = parse_events(result.stdout)

        assert result.returncode == 0, result.stderr
        assert events[-1] == ("DONE", "2:0:0"), f"Events: {events}\nStderr: {result.stderr}"
        with open(tmp_env["calls_log"]) as f:
            calls = sorted(line.strip() for line in f if line.strip())
        assert calls == ["fix-and-remediate-bug|201", "fix-and-remediate-bug|202"]
        with open(os.path.join(tmp_env["tmp_path"], "parallel-fix-bugs.json")) as f:
            assert json.load(f)["mode"] == "container"
