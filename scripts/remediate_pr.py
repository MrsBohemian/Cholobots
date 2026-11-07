# ABOUTME: Deterministic PR remediation script that monitors CI and review comments, requesting agent intervention as needed.
# ABOUTME: Uses exit-code/action-request pattern (like merge_train.py) so a skill wrapper can drive agent loops.
"""
Usage: python3 remediate_pr.py <pr-number> [flags]

Flags:
  --max-failures N         Build failure threshold before escalation (default: 5)
  --taiga-id ID            Taiga item ID (stored in state for skill wrapper)
  --taiga-type story|issue Taiga item type (stored in state for skill wrapper)
  --resume <path>          Resume from an existing state file

Exit codes:
  0   STABLE (PR passed CI and all comments resolved)
  10  Build fix needed (action-request JSON written)
  11  Comment resolution needed (action-request JSON written)
  12  ESCALATED (build failure count exceeded threshold)
  13  NEEDS_REBASE (PR has merge conflicts)
  14  PENDING (no CI verdict within the bounded wait window, or the Codex review is
      still running; re-invoke with --resume)
  1   Unexpected error

On --resume from an "awaiting_fix" or "awaiting_comment_resolution" phase (i.e. after
an agent pushed a fix), this script requests a Codex review pass via
scripts/codex-review-pr.sh before running the stability check. That script owns its
own 3-pass-per-PR cap and same-head-SHA guard, so this hook does not track passes or
SHAs itself. A non-zero exit from the review script -- or a failure to launch it at
all, such as the script being absent or non-executable -- is logged but never fatal;
the stability check always proceeds regardless of the review outcome.

No CI configured: the target repo (MrsBohemian/Cholobots) has no workflows yet (story #35
adds them). monitor-pr-stability.sh then reports STABLE and writes a
"NOTE: no CI checks configured ..." line to stderr; this script logs that note, records
it as lastResult.note in the state file and exits 0 as for any other STABLE result.
"""

import json
import os
import shlex
import subprocess
import sys

# Add shared lib to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "lib"))
from stability_parser import parse_stability_result
from private_files import write_json_private


# -- Exit codes --
EXIT_STABLE = 0
EXIT_ERROR = 1
EXIT_BUILD_FIX = 10
EXIT_COMMENTS = 11
EXIT_ESCALATED = 12
EXIT_NEEDS_REBASE = 13
EXIT_PENDING = 14

# Prefix of the stderr line monitor-pr-stability.sh prints when the repo has no CI at all.
NO_CI_NOTE_PREFIX = "NOTE: no CI checks configured"

# Phases from which resuming means an agent just pushed a fix, so a re-review is due.
REVIEW_TRIGGER_PHASES = ("awaiting_fix", "awaiting_comment_resolution")


class ParsedArgs:
    """Holds parsed command-line arguments."""

    def __init__(self):
        self.pr_number = None
        self.max_failures = 5
        self.taiga_id = None
        self.taiga_type = None
        self.resume_path = None


def _require_flag_value(flag, i, argv):
    """Ensure a flag at position i has a following value in argv. Exits with error if missing."""
    if i + 1 >= len(argv):
        print(f"Error: {flag} requires a value", file=sys.stderr)
        print(__doc__, file=sys.stderr)
        sys.exit(EXIT_ERROR)


def parse_args(argv):
    """Parse command-line arguments into a ParsedArgs object."""
    args = ParsedArgs()
    i = 0

    while i < len(argv):
        arg = argv[i]
        if arg == "--max-failures":
            _require_flag_value(arg, i, argv)
            i += 1
            raw = argv[i]
            if not raw.isdigit() or int(raw) <= 0:
                print(f"Error: --max-failures must be a positive integer, got '{raw}'", file=sys.stderr)
                sys.exit(EXIT_ERROR)
            args.max_failures = int(raw)
        elif arg == "--taiga-id":
            _require_flag_value(arg, i, argv)
            i += 1
            args.taiga_id = argv[i]
        elif arg == "--taiga-type":
            _require_flag_value(arg, i, argv)
            i += 1
            args.taiga_type = argv[i]
        elif arg == "--resume":
            _require_flag_value(arg, i, argv)
            i += 1
            args.resume_path = argv[i]
        elif args.pr_number is None and arg.isdigit():
            args.pr_number = int(arg)
        else:
            print(f"Unknown argument: {arg}", file=sys.stderr)
            print(__doc__, file=sys.stderr)
            sys.exit(EXIT_ERROR)
        i += 1

    if args.resume_path is None and args.pr_number is None:
        print("Error: <pr-number> is required (or use --resume)", file=sys.stderr)
        print(__doc__, file=sys.stderr)
        sys.exit(EXIT_ERROR)

    # Validate --taiga-id and --taiga-type pairing
    if args.taiga_id and not args.taiga_type:
        print("Error: --taiga-type is required when --taiga-id is set", file=sys.stderr)
        sys.exit(EXIT_ERROR)
    if args.taiga_type and not args.taiga_id:
        print("Error: --taiga-id is required when --taiga-type is set", file=sys.stderr)
        sys.exit(EXIT_ERROR)
    if args.taiga_type and args.taiga_type not in ("story", "issue"):
        print(f"Error: --taiga-type must be 'story' or 'issue', got '{args.taiga_type}'", file=sys.stderr)
        sys.exit(EXIT_ERROR)

    return args


class StateManager:
    """Manages the remediation state file with atomic writes."""

    def __init__(self, state_path):
        self.state_path = state_path
        self._state = None

    @property
    def state(self):
        return self._state

    def init(self, pr_number, max_failures, taiga_id=None, taiga_type=None):
        """Initialize a new state file."""
        self._state = {
            "prNumber": pr_number,
            "maxFailures": max_failures,
            "buildFailureCount": 0,
            "phase": "monitoring",
            "taigaId": taiga_id,
            "taigaType": taiga_type,
            "lastResult": None,
        }
        self._save()

    def load(self):
        """Load state from an existing file. Validates prNumber is an integer."""
        with open(self.state_path) as f:
            self._state = json.load(f)
        # Validate prNumber to prevent injection via tampered state files
        pr = self._state.get("prNumber")
        if not isinstance(pr, int):
            raise ValueError(f"Invalid prNumber in state file: {pr!r} (expected int)")
        return self._state

    def update(self, **kwargs):
        """Update state fields and persist."""
        for key, value in kwargs.items():
            self._state[key] = value
        self._save()

    def _save(self):
        """Save state to disk atomically (write temp + rename)."""
        write_json_private(self.state_path, self._state)


def log(msg):
    """Write a log message to stderr."""
    print(f"[remediate-pr] {msg}", file=sys.stderr, flush=True)


def run_cmd(cmd, check=True, capture=True):
    """Run a command as an argv list, returning (returncode, stdout, stderr).

    Accepts either a list of strings (preferred) or a single string (split via shlex).
    Always uses shell=False to avoid command injection.
    """
    if isinstance(cmd, str):
        cmd = shlex.split(cmd)
    result = subprocess.run(
        cmd,
        shell=False,
        capture_output=capture,
        text=True,
    )
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode, cmd, result.stdout, result.stderr
        )
    return (
        result.returncode,
        result.stdout.strip() if result.stdout else "",
        result.stderr.strip() if result.stderr else "",
    )


def write_action_request(action_path, action, pr_number, state_file, details):
    """Write an action-request JSON file for the skill wrapper."""
    data = {
        "action": action,
        "pr": {"number": pr_number},
        "state_file": state_file,
        "details": details,
    }
    write_json_private(action_path, data)


def main():
    args = parse_args(sys.argv[1:])

    # Determine state file path and initialize or resume
    if args.resume_path:
        state_path = args.resume_path
        sm = StateManager(state_path)
        sm.load()
        pr_number = sm.state["prNumber"]
        log(f"Resuming remediation for PR #{pr_number} from {state_path}")
    else:
        pr_number = args.pr_number
        state_path = f"/tmp/remediate-pr-{pr_number}.json"
        sm = StateManager(state_path)
        sm.init(
            pr_number=pr_number,
            max_failures=args.max_failures,
            taiga_id=args.taiga_id,
            taiga_type=args.taiga_type,
        )
        log(f"Starting remediation for PR #{pr_number} (max failures: {args.max_failures})")

    action_path = f"/tmp/remediate-pr-{pr_number}-action.json"

    if args.resume_path and sm.state.get("phase") in REVIEW_TRIGGER_PHASES:
        log(f"Resuming after agent intervention; requesting a Codex review pass for PR #{pr_number}")
        try:
            rc, out, err = run_cmd(f"scripts/codex-review-pr.sh {pr_number}", check=False)
            log(f"codex-review-pr.sh exit {rc}: {out[-200:]} {err[-200:]}")
            last_line = out.strip().splitlines()[-1] if out.strip() else ""
            if last_line.startswith("REVIEW_PENDING:"):
                log(f"Codex review for PR #{pr_number} is still running; re-invoke with --resume to join it.")
                return EXIT_PENDING
        except OSError as e:
            # The script may be absent, non-executable, or unreachable from the current
            # working directory. The review is advisory, so log and carry on.
            log(f"codex-review-pr.sh could not be run ({e}); continuing -- the review is non-fatal")

    # Run stability check
    log(f"Running stability check for PR #{pr_number}...")
    # Zero quiet window: the Codex review is joined above, so nothing arrives after CI goes green.
    rc, stdout, stderr = run_cmd(
        f"scripts/monitor-pr-stability.sh {pr_number} 0 --poll-interval 30 --max-seconds 480",
        check=False,
    )

    if rc != 0:
        # monitor-pr-stability.sh reports failing checks on stdout with exit 0
        # (CHECKS_FAILING:...). A non-zero exit is an API, authentication or
        # network error, which says nothing about the PR's code: dispatching a
        # build fix or counting toward escalation would act on an outage.
        log(f"monitor-pr-stability.sh failed (exit {rc}): stdout={stdout[:200]}, stderr={stderr[:200]}")
        sm.update(
            phase="error",
            lastResult={"status": "error", "stdout": stdout[:500], "stderr": stderr[:500]},
        )
        return EXIT_ERROR

    # Parse the stability result
    result = parse_stability_result(stdout)
    sm.update(lastResult=result)

    if result["status"] == "pending":
        log(f"PR #{pr_number} checks still running; no verdict within the wait window. Re-invoke with --resume.")
        sm.update(phase="monitoring")
        return EXIT_PENDING

    if result["status"] == "stable":
        no_ci_note = next(
            (line.strip() for line in (stderr or "").splitlines()
             if line.strip().startswith(NO_CI_NOTE_PREFIX)),
            None,
        )
        if no_ci_note:
            log(f"{no_ci_note} (story #35 adds CI); PR #{pr_number} counts as stable with no checks run.")
            result["note"] = "no CI checks configured"
            sm.update(lastResult=result)
        log(f"PR #{pr_number} is stable. All checks passing, no unresolved comments.")
        sm.update(phase="stable")
        return EXIT_STABLE

    if result["status"] == "needs_rebase":
        log(f"PR #{pr_number} has merge conflicts and needs a rebase.")
        write_action_request(
            action_path,
            action="needs_rebase",
            pr_number=pr_number,
            state_file=state_path,
            details={},
        )
        sm.update(phase="needs_rebase")
        return EXIT_NEEDS_REBASE

    if result["status"] == "checks_failing":
        failing = result["failing_checks"]
        log(f"PR #{pr_number} has failing checks: {failing}")
        sm.update(buildFailureCount=sm.state["buildFailureCount"] + 1)

        # Check escalation threshold
        if sm.state["buildFailureCount"] >= sm.state["maxFailures"]:
            log(f"Build failure count ({sm.state['buildFailureCount']}) reached threshold ({sm.state['maxFailures']}). Escalating.")
            sm.update(phase="escalated")
            return EXIT_ESCALATED

        write_action_request(
            action_path,
            action="fix_build",
            pr_number=pr_number,
            state_file=state_path,
            details={"failing_checks": failing},
        )
        sm.update(phase="awaiting_fix")
        log(f"Action request written to {action_path}")
        return EXIT_BUILD_FIX

    if result["status"] == "comments_found":
        count = result.get("comment_count", 0)
        log(f"PR #{pr_number} has {count} unresolved review comment thread(s).")
        write_action_request(
            action_path,
            action="resolve_comments",
            pr_number=pr_number,
            state_file=state_path,
            details={"comment_count": count},
        )
        sm.update(phase="awaiting_comment_resolution")
        log(f"Action request written to {action_path}")
        return EXIT_COMMENTS

    # Unknown status — treat as error, persist for diagnostics
    log(f"Unexpected stability result: {result}")
    sm.update(phase="error", lastResult=result)
    return EXIT_ERROR


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"[remediate-pr] Unexpected error: {e}", file=sys.stderr, flush=True)
        sys.exit(EXIT_ERROR)
