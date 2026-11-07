#!/usr/bin/env bash
# ABOUTME: Unit tests for monitor-pr-stability.sh script.
# ABOUTME: Verifies that unresolved review threads from non-advisory authors are detected (reviewers named in MONITOR_ADVISORY_REVIEWERS are excluded), including outdated ones.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
MONITOR_SCRIPT="$REPO_ROOT/scripts/monitor-pr-stability.sh"

# Wall-clock budgets. The monitor polls on real time and these tests run
# concurrently with the other script tests, so the caps are
# sized for a loaded machine and can be raised further via the environment.
MONITOR_TEST_TIMEOUT="${MONITOR_TEST_TIMEOUT:-60}"
MONITOR_TEST_POLL_INTERVAL="${MONITOR_TEST_POLL_INTERVAL:-1}"
MONITOR_TEST_MAX_SECONDS="${MONITOR_TEST_MAX_SECONDS:-2}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Under set -e a failing command aborts before the summary is printed; name
# the line and command so the abort is diagnosable from the log.
on_err() {
    local rc=$? line=$1
    echo -e "${RED}✗ ABORTED at line ${line} (exit ${rc}): ${BASH_COMMAND}${NC}" >&2
    echo -e "${RED}  (set -e abort — the summary below was never reached)${NC}" >&2
}
set -o errtrace
trap 'on_err $LINENO' ERR

pass() { echo -e "${GREEN}✓ $1${NC}"; }
fail() { echo -e "${RED}✗ $1${NC}"; FAILURES=$((FAILURES + 1)); }
info() { echo -e "${YELLOW}→ $1${NC}"; }

FAILURES=0
MOCK_DIR=""

cleanup() {
    if [ -n "$MOCK_DIR" ] && [ -d "$MOCK_DIR" ]; then
        rm -rf "$MOCK_DIR"
    fi
}
trap cleanup EXIT

# Create a mock gh command that returns canned responses.
# The mock reads scenario files from MOCK_DIR to decide what to return.
setup_mock_gh() {
    MOCK_DIR=$(mktemp -d "/tmp/test-monitor-pr-XXXXXX") || {
        echo -e "${RED}✗ Could not create mock directory under /tmp${NC}" >&2
        exit 1
    }

    cat > "$MOCK_DIR/gh" << 'MOCKSCRIPT'
#!/usr/bin/env bash
# Mock gh command that returns canned responses from MOCK_DIR files
# Record whether GH_TOKEN reached gh (the monitor must drop it via env -u).
echo "${GH_TOKEN-unset}" >> "$MOCK_DIR/gh_token_seen.log"
args="$*"; echo "${args//$'\n'/ }" >> "$MOCK_DIR/gh_calls.log"
case "$*" in
    "api repos/"*"/actions/workflows"*)
        # Workflow count probe; present file => that count, missing => unreadable
        if [[ -f "$MOCK_DIR/workflow_count.txt" ]]; then
            cat "$MOCK_DIR/workflow_count.txt"
        else
            exit 1
        fi
        ;;
    "repo view --json nameWithOwner -q .nameWithOwner")
        echo "TestOwner/TestRepo"
        ;;
    "pr view"*"--json mergeable"*)
        if [[ -f "$MOCK_DIR/mergeable.txt" ]]; then
            cat "$MOCK_DIR/mergeable.txt"
        else
            echo "MERGEABLE"
        fi
        ;;
    "pr view"*"--json headRefOid"*)
        # Default head SHA, or one line per poll from head_shas.txt (the last
        # line repeats) to simulate a force-push between polls.
        if [[ -f "$MOCK_DIR/head_shas.txt" ]]; then
            calls=$(( $(cat "$MOCK_DIR/head_sha_calls.txt" 2>/dev/null || echo 0) + 1 ))
            echo "$calls" > "$MOCK_DIR/head_sha_calls.txt"
            sed -n "$(( calls < $(wc -l < "$MOCK_DIR/head_shas.txt") ? calls : $(wc -l < "$MOCK_DIR/head_shas.txt") ))p" "$MOCK_DIR/head_shas.txt"
        else
            echo "abc123def"
        fi
        ;;
    "pr view"*"--json baseRefName"*)
        # Resolve the PR base branch; default main, override via base_ref.txt
        if [[ -f "$MOCK_DIR/base_ref.txt" ]]; then
            cat "$MOCK_DIR/base_ref.txt"
        else
            echo "main"
        fi
        ;;
    "api"*"required_status_checks/contexts"*)
        # Record the exact call so tests can assert on how base_ref was encoded.
        echo "$*" >> "$MOCK_DIR/required_contexts_calls.log"
        # Required-context lookup; present file => Mode A, missing => 404/Mode B
        if [[ -f "$MOCK_DIR/required_contexts.json" ]]; then
            cat "$MOCK_DIR/required_contexts.json"
        else
            exit 1
        fi
        ;;
    "run list"*"--json"*)
        # Optionally emit stderr noise on a successful call (tests that JSON
        # capture is not corrupted by stderr text).
        if [[ -f "$MOCK_DIR/run_list_stderr.txt" ]]; then
            cat "$MOCK_DIR/run_list_stderr.txt" >&2
        fi
        # Return all-passing runs by default, or custom if file exists
        if [[ -f "$MOCK_DIR/runs.json" ]]; then
            cat "$MOCK_DIR/runs.json"
        else
            echo '[{"name":"CI","status":"completed","conclusion":"success","createdAt":"2026-01-01T00:00:00Z"}]'
        fi
        ;;
    "api graphql"*)
        # Optionally emit stderr noise on a successful call
        if [[ -f "$MOCK_DIR/graphql_stderr.txt" ]]; then
            cat "$MOCK_DIR/graphql_stderr.txt" >&2
        fi
        # Return review threads from mock file. `gh api --paginate` prints one
        # JSON document per page, so threads_page2.json is appended as page 2.
        if [[ -f "$MOCK_DIR/threads.json" ]]; then
            cat "$MOCK_DIR/threads.json"
            if [[ -f "$MOCK_DIR/threads_page2.json" ]]; then cat "$MOCK_DIR/threads_page2.json"; fi
        else
            echo '{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[]}}}}}'
        fi
        ;;
    *"/requested_reviewers"*)
        # Return requested reviewers from mock file
        if [[ -f "$MOCK_DIR/requested_reviewers.json" ]]; then
            cat "$MOCK_DIR/requested_reviewers.json"
        else
            echo '{"users":[],"teams":[]}'
        fi
        ;;
    *)
        echo "MOCK_ERROR: unexpected gh call: $*" >&2
        exit 1
        ;;
esac
MOCKSCRIPT
    chmod +x "$MOCK_DIR/gh"
}

# Run the monitor script with mocked gh, using a 0-minute quiet period
# so it returns STABLE immediately when no issues found.
run_monitor() {
    # Prepend mock dir to PATH so our mock gh is found first
    # Use a short poll interval and quiet-minutes 0 so it exits quickly
    PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" \
        timeout "$MONITOR_TEST_TIMEOUT" bash "$MONITOR_SCRIPT" 42 0 --poll-interval "$MONITOR_TEST_POLL_INTERVAL" 2>/dev/null || true
}

# Run the monitor with a bounded wait so a "keeps waiting" scenario returns
# PENDING within MONITOR_TEST_MAX_SECONDS instead of running until the outer
# timeout kills it. PENDING proves the loop was still polling (an empty
# result would also match a silent crash).
run_monitor_bounded() {
    PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" \
        timeout "$MONITOR_TEST_TIMEOUT" bash "$MONITOR_SCRIPT" 42 0 --poll-interval "$MONITOR_TEST_POLL_INTERVAL" --max-seconds "$MONITOR_TEST_MAX_SECONDS" 2>/dev/null || true
}

# Replace the monitor's wall clock with a fake one in MOCK_DIR: `date +%s`
# reads clock.txt and `sleep N` advances it by N instead of waiting. Timing
# assertions then depend only on the poll count, never on how long each poll's
# gh/jq work takes on a loaded runner or where a real second boundary falls.
setup_fake_clock() {
    local real_date
    real_date=$(command -v date)
    echo 0 > "$MOCK_DIR/clock.txt"
    cat > "$MOCK_DIR/date" << EOF
#!/usr/bin/env bash
if [[ "\$*" == "+%s" ]]; then
    cat "\$MOCK_DIR/clock.txt"
else
    exec "$real_date" "\$@"
fi
EOF
    cat > "$MOCK_DIR/sleep" << 'EOF'
#!/usr/bin/env bash
echo $(( $(cat "$MOCK_DIR/clock.txt") + $1 )) > "$MOCK_DIR/clock.txt"
EOF
    chmod +x "$MOCK_DIR/date" "$MOCK_DIR/sleep"
}

# Helper to write a threads.json mock response
write_threads_json() {
    local nodes_json="$1"
    cat > "$MOCK_DIR/threads.json" << EOF
{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":$nodes_json}}}}}
EOF
}

# --- Test 1: Unresolved bot thread blocks by default (advisory reviewer list is empty) ---
info "Test 1: Unresolved bot thread blocks by default (advisory reviewer list is empty)"
setup_mock_gh
write_threads_json '[
    {"id":"T1","isResolved":false,"isOutdated":false,"comments":{"nodes":[{"author":{"login":"copilot-pull-request-reviewer[bot]"}}]}}
]'
output=$(run_monitor)
if [[ "$output" == "COMMENTS_FOUND:1" ]]; then
    pass "Bot thread blocks when no advisory reviewers are configured"
else
    fail "Expected COMMENTS_FOUND:1, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 2: Human reviewer unresolved thread is detected ---
info "Test 2: Human reviewer unresolved active thread detected"
setup_mock_gh
write_threads_json '[
    {"id":"T1","isResolved":false,"isOutdated":false,"comments":{"nodes":[{"author":{"login":"human-reviewer"}}]}}
]'
output=$(run_monitor)
if [[ "$output" == "COMMENTS_FOUND:1" ]]; then
    pass "Human reviewer thread detected"
else
    fail "Expected COMMENTS_FOUND:1, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 3: CodeRabbit bot unresolved thread is detected ---
info "Test 3: CodeRabbit bot unresolved thread detected"
setup_mock_gh
write_threads_json '[
    {"id":"T1","isResolved":false,"isOutdated":false,"comments":{"nodes":[{"author":{"login":"coderabbitai[bot]"}}]}}
]'
output=$(run_monitor)
if [[ "$output" == "COMMENTS_FOUND:1" ]]; then
    pass "CodeRabbit bot thread detected"
else
    fail "Expected COMMENTS_FOUND:1, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 4: Outdated thread from a reviewer named in MONITOR_ADVISORY_REVIEWERS is excluded ---
info "Test 4: Outdated unresolved thread from a reviewer named in MONITOR_ADVISORY_REVIEWERS is excluded"
setup_mock_gh
write_threads_json '[
    {"id":"T1","isResolved":false,"isOutdated":true,"comments":{"nodes":[{"author":{"login":"advisory-bot[bot]"}}]}}
]'
output=$(MONITOR_ADVISORY_REVIEWERS="advisory-bot[bot]" run_monitor)
if [[ "$output" == "STABLE" ]]; then
    pass "Outdated advisory-reviewer thread excluded, monitor reports STABLE"
else
    fail "Expected STABLE, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 5: Resolved threads are NOT counted ---
info "Test 5: Resolved threads are not counted"
setup_mock_gh
write_threads_json '[
    {"id":"T1","isResolved":true,"isOutdated":false,"comments":{"nodes":[{"author":{"login":"human-reviewer"}}]}}
]'
output=$(run_monitor)
if [[ "$output" == "STABLE" ]]; then
    pass "Resolved thread not counted"
else
    fail "Expected STABLE, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 6: Mixed resolved and unresolved from multiple authors ---
info "Test 6: Mixed resolved/unresolved from multiple authors"
setup_mock_gh
write_threads_json '[
    {"id":"T1","isResolved":true,"isOutdated":false,"comments":{"nodes":[{"author":{"login":"human-reviewer"}}]}},
    {"id":"T2","isResolved":false,"isOutdated":false,"comments":{"nodes":[{"author":{"login":"advisory-bot[bot]"}}]}},
    {"id":"T3","isResolved":false,"isOutdated":true,"comments":{"nodes":[{"author":{"login":"coderabbitai[bot]"}}]}},
    {"id":"T4","isResolved":false,"isOutdated":false,"comments":{"nodes":[{"author":{"login":"some-human"}}]}}
]'
output=$(MONITOR_ADVISORY_REVIEWERS="advisory-bot[bot]" run_monitor)
if [[ "$output" == "COMMENTS_FOUND:2" ]]; then
    pass "Counted 2 unresolved non-advisory threads (advisory-bot thread excluded)"
else
    fail "Expected COMMENTS_FOUND:2, got: $output"
fi
output=$(run_monitor)
if [[ "$output" == "COMMENTS_FOUND:3" ]]; then
    pass "Counted 3 unresolved threads when no advisory reviewers are configured"
else
    fail "Expected COMMENTS_FOUND:3, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 6b: Null thread author still blocks (deleted/ghost user) ---
info "Test 6b: Thread with null author still counts as unresolved"
setup_mock_gh
write_threads_json '[
    {"id":"T1","isResolved":false,"isOutdated":false,"comments":{"nodes":[{"author":null}]}}
]'
output=$(run_monitor)
if [[ "$output" == "COMMENTS_FOUND:1" ]]; then
    pass "Null-author thread still blocks"
else
    fail "Expected COMMENTS_FOUND:1, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 6c: MONITOR_ADVISORY_REVIEWERS override replaces the default list ---
info "Test 6c: MONITOR_ADVISORY_REVIEWERS override names only coderabbit; the other bot thread still blocks"
setup_mock_gh
write_threads_json '[
    {"id":"T1","isResolved":false,"isOutdated":false,"comments":{"nodes":[{"author":{"login":"coderabbitai[bot]"}}]}},
    {"id":"T2","isResolved":false,"isOutdated":false,"comments":{"nodes":[{"author":{"login":"copilot-pull-request-reviewer[bot]"}}]}}
]'
output=$(MONITOR_ADVISORY_REVIEWERS="coderabbitai[bot]" run_monitor)
if [[ "$output" == "COMMENTS_FOUND:1" ]]; then
    pass "Override excludes coderabbit; the other bot thread still blocks"
else
    fail "Expected COMMENTS_FOUND:1, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 6d: Advisory reviewer login match is case-insensitive ---
info "Test 6d: Advisory reviewer login matches case-insensitively"
setup_mock_gh
write_threads_json '[
    {"id":"T1","isResolved":false,"isOutdated":false,"comments":{"nodes":[{"author":{"login":"Advisory-Bot[bot]"}}]}}
]'
output=$(MONITOR_ADVISORY_REVIEWERS="advisory-bot[bot]" run_monitor)
if [[ "$output" == "STABLE" ]]; then
    pass "Case-mismatched advisory reviewer login still excluded"
else
    fail "Expected STABLE, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 7: No threads at all returns STABLE ---
info "Test 7: No review threads returns STABLE"
setup_mock_gh
write_threads_json '[]'
output=$(run_monitor)
if [[ "$output" == "STABLE" ]]; then
    pass "No threads returns STABLE"
else
    fail "Expected STABLE, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 8: Copilot pending reviewer does NOT block stability (bug #1143 regression) ---
info "Test 8: Copilot pending reviewer does not block STABLE"
setup_mock_gh
write_threads_json '[]'
echo '{"users":[{"login":"Copilot"}],"teams":[]}' > "$MOCK_DIR/requested_reviewers.json"
output=$(run_monitor)
# Copilot being a pending reviewer should NOT prevent stability.
# The script detects Copilot findings via review comment threads, not reviewer status.
if [[ "$output" == "STABLE" ]]; then
    pass "Copilot pending reviewer does not block STABLE"
else
    fail "Expected STABLE (Copilot pending should not block), got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 9: No pending reviewers allows STABLE ---
info "Test 9: No pending reviewers allows STABLE"
setup_mock_gh
write_threads_json '[]'
echo '{"users":[],"teams":[]}' > "$MOCK_DIR/requested_reviewers.json"
output=$(run_monitor)
if [[ "$output" == "STABLE" ]]; then
    pass "No pending reviewers allows STABLE"
else
    fail "Expected STABLE, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 10: PR with CONFLICTING mergeable status outputs NEEDS_REBASE ---
info "Test 10: PR with merge conflicts outputs NEEDS_REBASE"
setup_mock_gh
echo "CONFLICTING" > "$MOCK_DIR/mergeable.txt"
output=$(run_monitor)
if [[ "$output" == "NEEDS_REBASE" ]]; then
    pass "Conflicting PR outputs NEEDS_REBASE"
else
    fail "Expected NEEDS_REBASE, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 11: PR with MERGEABLE status proceeds to STABLE ---
info "Test 11: PR with MERGEABLE status proceeds to STABLE"
setup_mock_gh
echo "MERGEABLE" > "$MOCK_DIR/mergeable.txt"
write_threads_json '[]'
output=$(run_monitor)
if [[ "$output" == "STABLE" ]]; then
    pass "Mergeable PR proceeds to STABLE"
else
    fail "Expected STABLE, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 12: PR with UNKNOWN status proceeds to normal checks ---
info "Test 12: PR with UNKNOWN mergeable status proceeds to normal checks"
setup_mock_gh
echo "UNKNOWN" > "$MOCK_DIR/mergeable.txt"
write_threads_json '[]'
output=$(run_monitor)
if [[ "$output" == "STABLE" ]]; then
    pass "Unknown mergeable status falls through to STABLE"
else
    fail "Expected STABLE, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 13: Advisory failure excluded (Mode B) ---
info "Test 13: Non-CI failing run blocks in Mode B unless named in MONITOR_ADVISORY_CHECKS"
setup_mock_gh
write_threads_json '[]'
echo '[
    {"name":"CI","status":"completed","conclusion":"success","createdAt":"2026-01-01T00:00:00Z"},
    {"name":"Some Review","status":"completed","conclusion":"failure","createdAt":"2026-01-01T00:00:00Z"}
]' > "$MOCK_DIR/runs.json"
output=$(run_monitor)
if [[ "$output" == "CHECKS_FAILING:Some Review" ]]; then
    pass "Failing non-CI run blocks when no advisory checks are configured"
else
    fail "Expected CHECKS_FAILING:Some Review, got: $output"
fi
output=$(MONITOR_ADVISORY_CHECKS="Some Review" run_monitor)
if [[ "$output" == "STABLE" ]]; then
    pass "Failing run named in MONITOR_ADVISORY_CHECKS is excluded, monitor reports STABLE"
else
    fail "Expected STABLE, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 14: Required failure still blocks (Mode B) ---
info "Test 14: Non-advisory failure still blocks when contexts unreadable (Mode B)"
setup_mock_gh
write_threads_json '[]'
echo '[
    {"name":"CI","status":"completed","conclusion":"failure","createdAt":"2026-01-01T00:00:00Z"}
]' > "$MOCK_DIR/runs.json"
output=$(run_monitor)
if [[ "$output" == "CHECKS_FAILING:CI" ]]; then
    pass "Required-like failure still blocks in Mode B"
else
    fail "Expected CHECKS_FAILING:CI, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 15: Required failure blocks, advisory omitted (Mode A) ---
info "Test 15: Only required contexts block; advisory omitted (Mode A)"
setup_mock_gh
write_threads_json '[]'
echo '["CI"]' > "$MOCK_DIR/required_contexts.json"
echo '[
    {"name":"CI","status":"completed","conclusion":"failure","createdAt":"2026-01-01T00:00:00Z"},
    {"name":"Copilot Code Review","status":"completed","conclusion":"failure","createdAt":"2026-01-01T00:00:00Z"}
]' > "$MOCK_DIR/runs.json"
output=$(run_monitor)
if [[ "$output" == "CHECKS_FAILING:CI" ]]; then
    pass "Mode A reports only the required failing check"
else
    fail "Expected CHECKS_FAILING:CI, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 16: Non-required failure ignored (Mode A) ---
info "Test 16: Non-required check failure ignored when required check is green (Mode A)"
setup_mock_gh
write_threads_json '[]'
echo '["CI"]' > "$MOCK_DIR/required_contexts.json"
echo '[
    {"name":"CI","status":"completed","conclusion":"success","createdAt":"2026-01-01T00:00:00Z"},
    {"name":"Some Optional Check","status":"completed","conclusion":"failure","createdAt":"2026-01-01T00:00:00Z"}
]' > "$MOCK_DIR/runs.json"
output=$(run_monitor)
if [[ "$output" == "STABLE" ]]; then
    pass "Non-required failure ignored, monitor reports STABLE"
else
    fail "Expected STABLE, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 17: Required in_progress + advisory failed keeps waiting (Mode A) ---
info "Test 17: Required check in_progress with advisory failure does not bail (Mode A)"
setup_mock_gh
write_threads_json '[]'
echo '["CI"]' > "$MOCK_DIR/required_contexts.json"
echo '[
    {"name":"CI","status":"in_progress","conclusion":null,"createdAt":"2026-01-01T00:00:00Z"},
    {"name":"Copilot Code Review","status":"completed","conclusion":"failure","createdAt":"2026-01-01T00:00:00Z"}
]' > "$MOCK_DIR/runs.json"
output=$(run_monitor_bounded)
# Required CI still running -> no terminal verdict; advisory failure must not bail.
if [[ "$output" == "PENDING" ]]; then
    pass "Required in_progress keeps waiting (PENDING, no terminal verdict)"
else
    fail "Expected PENDING, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 18: Required not-yet-started blocks STABLE (Mode A) ---
info "Test 18: Required check with no run yet does not go STABLE (Mode A)"
setup_mock_gh
write_threads_json '[]'
echo '["CI"]' > "$MOCK_DIR/required_contexts.json"
echo '[]' > "$MOCK_DIR/runs.json"
output=$(run_monitor_bounded)
# Required CI has not produced a run yet -> keep waiting, never STABLE.
if [[ "$output" == "PENDING" ]]; then
    pass "Required not-yet-started keeps waiting (PENDING, no STABLE)"
else
    fail "Expected PENDING, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 19: Required terminal success reports STABLE (Mode A) ---
info "Test 19: Required check terminal success reports STABLE (Mode A)"
setup_mock_gh
write_threads_json '[]'
echo '["CI"]' > "$MOCK_DIR/required_contexts.json"
echo '[
    {"name":"CI","status":"completed","conclusion":"success","createdAt":"2026-01-01T00:00:00Z"}
]' > "$MOCK_DIR/runs.json"
output=$(run_monitor)
if [[ "$output" == "STABLE" ]]; then
    pass "Required terminal success reports STABLE"
else
    fail "Expected STABLE, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 20: Advisory pending does not block STABLE (Mode A) ---
info "Test 20: Advisory check still in_progress does not block STABLE (Mode A)"
setup_mock_gh
write_threads_json '[]'
echo '["CI"]' > "$MOCK_DIR/required_contexts.json"
echo '[
    {"name":"CI","status":"completed","conclusion":"success","createdAt":"2026-01-01T00:00:00Z"},
    {"name":"Optional Check","status":"in_progress","conclusion":null,"createdAt":"2026-01-01T00:00:00Z"}
]' > "$MOCK_DIR/runs.json"
output=$(run_monitor)
# Only the required CI gates stability; a pending advisory must not reset the timer.
if [[ "$output" == "STABLE" ]]; then
    pass "Advisory pending does not block STABLE"
else
    fail "Expected STABLE, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 21: Advisory list entries are trimmed (Mode B) ---
info "Test 21: Comma-separated MONITOR_ADVISORY_CHECKS entries are trimmed of whitespace (Mode B)"
setup_mock_gh
write_threads_json '[]'
echo '[
    {"name":"CI","status":"completed","conclusion":"success","createdAt":"2026-01-01T00:00:00Z"},
    {"name":"Some Check","status":"completed","conclusion":"failure","createdAt":"2026-01-01T00:00:00Z"}
]' > "$MOCK_DIR/runs.json"
# A leading space before "Some Check" must not stop it from matching the
# advisory-exclude list (jq's exact-string index() match would otherwise fail).
output=$(MONITOR_ADVISORY_CHECKS="Other Check, Some Check" run_monitor)
if [[ "$output" == "STABLE" ]]; then
    pass "Advisory entry with leading space is trimmed and excluded"
else
    fail "Expected STABLE, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 22: Default advisory check list is empty (Mode B) ---
info "Test 22: Default advisory check list is empty: a failing 'Running Copilot Code Review' run blocks in Mode B"
setup_mock_gh
write_threads_json '[]'
echo '[
    {"name":"CI","status":"completed","conclusion":"success","createdAt":"2026-01-01T00:00:00Z"},
    {"name":"Running Copilot Code Review","status":"completed","conclusion":"failure","createdAt":"2026-01-01T00:00:00Z"}
]' > "$MOCK_DIR/runs.json"
output=$(run_monitor)
if [[ "$output" == "CHECKS_FAILING:Running Copilot Code Review" ]]; then
    pass "No run is advisory by default; the failing run blocks"
else
    fail "Expected CHECKS_FAILING:Running Copilot Code Review, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 23: Job-qualified required context normalized to workflow name (Mode A) ---
info "Test 23: Required context 'CI / build' matches workflow-run name 'CI' (Mode A)"
setup_mock_gh
write_threads_json '[]'
# GitHub Actions required contexts are commonly reported as "<workflow> / <job>"
# while `gh run list --json name` returns only the workflow name. Without
# stripping the " / <job>" suffix, this required context would never match the
# run name, and the monitor would wait forever without a terminal verdict.
echo '["CI / build"]' > "$MOCK_DIR/required_contexts.json"
echo '[
    {"name":"CI","status":"completed","conclusion":"failure","createdAt":"2026-01-01T00:00:00Z"}
]' > "$MOCK_DIR/runs.json"
output=$(run_monitor)
if [[ "$output" == "CHECKS_FAILING:CI" ]]; then
    pass "Job-qualified required context normalized, failure reported"
else
    fail "Expected CHECKS_FAILING:CI, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 24: Job-qualified required context reaches STABLE once terminal (Mode A) ---
info "Test 24: Required context 'CI / build' reaches STABLE on success (Mode A)"
setup_mock_gh
write_threads_json '[]'
echo '["CI / build"]' > "$MOCK_DIR/required_contexts.json"
echo '[
    {"name":"CI","status":"completed","conclusion":"success","createdAt":"2026-01-01T00:00:00Z"}
]' > "$MOCK_DIR/runs.json"
output=$(run_monitor)
if [[ "$output" == "STABLE" ]]; then
    pass "Job-qualified required context normalized, STABLE reached"
else
    fail "Expected STABLE, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 25: Base branch containing "/" is URL-encoded in the required-contexts lookup ---
info "Test 25: Base branch name with a slash is URL-encoded before the gh api call"
setup_mock_gh
write_threads_json '[]'
echo "release/2026-07" > "$MOCK_DIR/base_ref.txt"
echo '["CI"]' > "$MOCK_DIR/required_contexts.json"
echo '[
    {"name":"CI","status":"completed","conclusion":"success","createdAt":"2026-01-01T00:00:00Z"}
]' > "$MOCK_DIR/runs.json"
run_monitor > /dev/null
if [[ -f "$MOCK_DIR/required_contexts_calls.log" ]] \
    && grep -q "branches/release%2F2026-07/protection" "$MOCK_DIR/required_contexts_calls.log" \
    && ! grep -q "branches/release/2026-07/protection" "$MOCK_DIR/required_contexts_calls.log"; then
    pass "Base branch slash is percent-encoded in the required-contexts request path"
else
    fail "Expected branches/release%2F2026-07/protection in gh api call, got: $(cat "$MOCK_DIR/required_contexts_calls.log" 2>/dev/null)"
fi
cleanup; MOCK_DIR=""

# --- Test 26: --max-seconds with checks perpetually in_progress prints PENDING ---
info "Test 26: --max-seconds bounded wait prints PENDING while checks are in_progress"
setup_mock_gh
write_threads_json '[]'
echo '["CI"]' > "$MOCK_DIR/required_contexts.json"
echo '[
    {"name":"CI","status":"in_progress","conclusion":null,"createdAt":"2026-01-01T00:00:00Z"}
]' > "$MOCK_DIR/runs.json"
rc=0
output=$(PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" \
    timeout "$MONITOR_TEST_TIMEOUT" bash "$MONITOR_SCRIPT" 42 0 --poll-interval "$MONITOR_TEST_POLL_INTERVAL" --max-seconds "$MONITOR_TEST_MAX_SECONDS" 2>/dev/null) || rc=$?
if [[ "$output" == "PENDING" && "$rc" -eq 0 ]]; then
    pass "Bounded wait returned PENDING with exit 0"
else
    fail "Expected PENDING (exit 0), got: '$output' (exit $rc)"
fi
cleanup; MOCK_DIR=""

# --- Test 27: Mode B in_progress run keeps waiting instead of jq-erroring to STABLE (bug #2163) ---
info "Test 27: Mode B non-completed run is counted as pending (no jq context-rebinding error)"
setup_mock_gh
write_threads_json '[]'
# No required_contexts.json => Mode B. A non-completed run must keep the loop
# waiting (PENDING under --max-seconds), never fall through to STABLE via a
# jq error that empties the pending count.
echo '[
    {"name":"CI","status":"in_progress","conclusion":null,"createdAt":"2026-01-01T00:00:00Z"}
]' > "$MOCK_DIR/runs.json"
rc=0
output=$(PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" \
    timeout "$MONITOR_TEST_TIMEOUT" bash "$MONITOR_SCRIPT" 42 0 --poll-interval "$MONITOR_TEST_POLL_INTERVAL" --max-seconds "$MONITOR_TEST_MAX_SECONDS" 2>/dev/null) || rc=$?
if [[ "$output" == "PENDING" && "$rc" -eq 0 ]]; then
    pass "Mode B in_progress run counted as pending, bounded wait returned PENDING"
else
    fail "Expected PENDING (exit 0), got: '$output' (exit $rc)"
fi
cleanup; MOCK_DIR=""

# --- Test 28: Required check with conclusion "skipped" blocks STABLE (Mode A) ---
info "Test 28: Required check concluded 'skipped' is reported as failing (Mode A)"
setup_mock_gh
write_threads_json '[]'
# Branch protection requires a context to report success; "skipped" does not
# satisfy it, so the monitor must not report STABLE while GitHub blocks merge.
echo '["CI"]' > "$MOCK_DIR/required_contexts.json"
echo '[
    {"name":"CI","status":"completed","conclusion":"skipped","createdAt":"2026-01-01T00:00:00Z"}
]' > "$MOCK_DIR/runs.json"
output=$(run_monitor)
if [[ "$output" == "CHECKS_FAILING:CI" ]]; then
    pass "Skipped required check reported as failing"
else
    fail "Expected CHECKS_FAILING:CI, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 29: Non-required skipped check is ignored (Mode A) ---
info "Test 29: Non-required skipped check ignored when required check is green (Mode A)"
setup_mock_gh
write_threads_json '[]'
echo '["CI"]' > "$MOCK_DIR/required_contexts.json"
echo '[
    {"name":"CI","status":"completed","conclusion":"success","createdAt":"2026-01-01T00:00:00Z"},
    {"name":"Docs","status":"completed","conclusion":"skipped","createdAt":"2026-01-01T00:00:00Z"}
]' > "$MOCK_DIR/runs.json"
output=$(run_monitor)
if [[ "$output" == "STABLE" ]]; then
    pass "Non-required skipped check ignored, monitor reports STABLE"
else
    fail "Expected STABLE, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 30: Skipped check does not block in Mode B ---
info "Test 30: Skipped check ignored when required contexts are unreadable (Mode B)"
setup_mock_gh
write_threads_json '[]'
# No required_contexts.json => 404 => Mode B; without a required set we cannot
# tell a gating skipped run from an advisory one, so lenient behavior is kept.
echo '[
    {"name":"CI","status":"completed","conclusion":"skipped","createdAt":"2026-01-01T00:00:00Z"}
]' > "$MOCK_DIR/runs.json"
output=$(run_monitor)
if [[ "$output" == "STABLE" ]]; then
    pass "Skipped check ignored in Mode B, monitor reports STABLE"
else
    fail "Expected STABLE, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 31: gh run list stderr noise does not corrupt runs_json ---
info "Test 31: gh run list stderr noise does not corrupt runs_json"
setup_mock_gh
write_threads_json '[]'
echo '[
    {"name":"CI","status":"completed","conclusion":"failure","createdAt":"2026-01-01T00:00:00Z"}
]' > "$MOCK_DIR/runs.json"
echo "Warning: gh wrote something to stderr" > "$MOCK_DIR/run_list_stderr.txt"
output=$(run_monitor)
if [[ "$output" == "CHECKS_FAILING:CI" ]]; then
    pass "stderr noise did not corrupt runs_json"
else
    fail "Expected CHECKS_FAILING:CI, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 32: gh api graphql stderr noise does not corrupt threads_json ---
info "Test 32: gh api graphql stderr noise does not corrupt threads_json"
setup_mock_gh
write_threads_json '[
    {"id":"T1","isResolved":false,"isOutdated":false,"comments":{"nodes":[{"author":{"login":"human-reviewer"}}]}}
]'
echo "Warning: gh wrote something to stderr" > "$MOCK_DIR/graphql_stderr.txt"
output=$(run_monitor)
if [[ "$output" == "COMMENTS_FOUND:1" ]]; then
    pass "stderr noise did not corrupt threads_json"
else
    fail "Expected COMMENTS_FOUND:1, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 33: non-JSON runs payload exits with ERROR instead of STABLE ---
info "Test 33: non-JSON runs payload exits with ERROR instead of STABLE"
setup_mock_gh
write_threads_json '[]'
echo 'not json at all' > "$MOCK_DIR/runs.json"
output=$(run_monitor)
if [[ "$output" == ERROR:* ]]; then
    pass "Unparseable runs payload produces a loud ERROR"
else
    fail "Expected ERROR:* output, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test 34: pending required check with stderr noise still resets the stability timer ---
info "Test 34: pending required check with stderr noise still blocks STABLE (Mode A)"
setup_mock_gh
write_threads_json '[]'
echo '["CI"]' > "$MOCK_DIR/required_contexts.json"
echo '[
    {"name":"CI","status":"in_progress","conclusion":null,"createdAt":"2026-01-01T00:00:00Z"}
]' > "$MOCK_DIR/runs.json"
echo "Warning: gh wrote something to stderr" > "$MOCK_DIR/run_list_stderr.txt"
output=$(run_monitor_bounded)
# Required CI still running -> keep waiting; stderr noise must not fake STABLE.
if [[ "$output" == "PENDING" ]]; then
    pass "Pending required check with stderr noise keeps waiting (PENDING, no STABLE)"
else
    fail "Expected PENDING, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test: Empty run list for the head SHA is pending in Mode B ---
info "Test: Mode B with no workflow runs for the head SHA yet keeps waiting (PENDING, no STABLE)"
setup_mock_gh
write_threads_json '[]'
# A freshly force-pushed SHA has no runs until GitHub registers them. With no
# required contexts (Mode B) and a zero quiet window, an empty list must not
# count as "nothing pending".
echo '[]' > "$MOCK_DIR/runs.json"
output=$(run_monitor_bounded)
if [[ "$output" == "PENDING" ]]; then
    pass "Empty Mode B run list keeps waiting"
else
    fail "Expected PENDING, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test: Empty run list past the registration grace is STABLE (bug #3727) ---
info "Test: Mode B with no workflow runs after the registration grace reports STABLE"
setup_mock_gh
write_threads_json '[]'
# A PR whose base no workflow triggers on (e.g. a feature-branch sub-PR) never
# gets runs. Once the grace for runs to register has passed, the empty list is
# final and must not keep the monitor polling forever.
echo '[]' > "$MOCK_DIR/runs.json"
echo "feature/some-story-123" > "$MOCK_DIR/base_ref.txt"
# The repo has workflows (they just do not trigger on this base); a readable
# probe is what separates this from an unreadable one, which stays pending.
echo '2' > "$MOCK_DIR/workflow_count.txt"
output=$(MONITOR_RUN_REGISTRATION_GRACE_SECONDS=1 run_monitor)
if [[ "$output" == "STABLE" ]]; then
    pass "Empty Mode B run list reaches STABLE after the registration grace"
else
    fail "Expected STABLE, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test: A head-SHA change restarts the registration grace (bug #3727) ---
info "Test: Mode B registration grace restarts when the head SHA changes"
setup_mock_gh
write_threads_json '[]'
echo '[]' > "$MOCK_DIR/runs.json"
# SHA A is seen empty for three polls, then a force-push moves the head to
# SHA B. B must get its own full grace rather than inherit A's expired one.
# On the fake clock A is polled at t=0,1,2 (inside its 3s grace) and B first
# at t=3, when A's grace has just expired; B's grace then outlasts the 5s cap.
setup_fake_clock
printf 'shaA\nshaA\nshaA\nshaB\n' > "$MOCK_DIR/head_shas.txt"
output=$(PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" MONITOR_RUN_REGISTRATION_GRACE_SECONDS=3 \
    timeout "$MONITOR_TEST_TIMEOUT" bash "$MONITOR_SCRIPT" 42 0 --poll-interval 1 --max-seconds 5 2>/dev/null || true)
if [[ "$output" == "PENDING" ]]; then
    pass "New head SHA gets a fresh registration grace"
else
    fail "Expected PENDING, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test: Default registration grace outlasts a short bounded wait (bug #3727) ---
info "Test: Mode B with no workflow runs within the default registration grace stays PENDING"
setup_mock_gh
write_threads_json '[]'
echo '[]' > "$MOCK_DIR/runs.json"
output=$(MONITOR_RUN_REGISTRATION_GRACE_SECONDS= run_monitor_bounded)
if [[ "$output" == "PENDING" ]]; then
    pass "Default registration grace keeps an empty run list pending"
else
    fail "Expected PENDING, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test: Invalid registration grace is rejected (bug #3727) ---
info "Test: Non-numeric MONITOR_RUN_REGISTRATION_GRACE_SECONDS is rejected"
setup_mock_gh
rc=0
output=$(PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" MONITOR_RUN_REGISTRATION_GRACE_SECONDS=abc \
    timeout "$MONITOR_TEST_TIMEOUT" bash "$MONITOR_SCRIPT" 42 0 --poll-interval "$MONITOR_TEST_POLL_INTERVAL" 2>&1) || rc=$?
if [[ "$rc" -ne 0 && "$output" == *"MONITOR_RUN_REGISTRATION_GRACE_SECONDS"* ]]; then
    pass "Invalid registration grace exits non-zero naming the variable"
else
    fail "Expected non-zero exit naming the variable, got rc=$rc: $output"
fi
cleanup; MOCK_DIR=""

# --- Test: No CI configured is STABLE at once, with a NOTE on stderr ---
info "Test: zero workflows and no required contexts reports STABLE without the grace wait"
setup_mock_gh
write_threads_json '[]'
echo '[]' > "$MOCK_DIR/runs.json"
echo '0' > "$MOCK_DIR/workflow_count.txt"
stderr_file="$MOCK_DIR/stderr.txt"
# The default 120s grace would outlast the bounded wait; no CI must skip it.
output=$(PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" GH_TOKEN=leaked-token \
    timeout "$MONITOR_TEST_TIMEOUT" bash "$MONITOR_SCRIPT" 42 0 --poll-interval "$MONITOR_TEST_POLL_INTERVAL" \
    --max-seconds "$MONITOR_TEST_MAX_SECONDS" 2>"$stderr_file" || true)
if [[ "$output" == "STABLE" ]]; then
    pass "No CI configured reports STABLE (stdout stays one line)"
else
    fail "Expected STABLE, got: $output"
fi
if grep -q '^NOTE: no CI checks configured' "$stderr_file"; then
    pass "No CI configured prints a NOTE line on stderr"
else
    fail "Expected a 'NOTE: no CI checks configured' line on stderr, got: $(cat "$stderr_file")"
fi
if grep -qv '^unset$' "$MOCK_DIR/gh_token_seen.log"; then
    fail "GH_TOKEN reached gh: $(sort -u "$MOCK_DIR/gh_token_seen.log" | tr '\n' ' ')"
else
    pass "Every gh call ran without GH_TOKEN"
fi
if grep -v '^api' "$MOCK_DIR/gh_calls.log" | grep -qv -- '--repo MrsBohemian/Cholobots'; then
    fail "Every non-api gh call targets --repo MrsBohemian/Cholobots"
else
    pass "Every non-api gh call targets --repo MrsBohemian/Cholobots"
fi
cleanup; MOCK_DIR=""

# --- Test: Workflows exist but none ran keeps the registration grace ---
info "Test: a repo with workflows but no runs for the head SHA keeps waiting"
setup_mock_gh
write_threads_json '[]'
echo '[]' > "$MOCK_DIR/runs.json"
echo '2' > "$MOCK_DIR/workflow_count.txt"
output=$(run_monitor_bounded)
if [[ "$output" == "PENDING" ]]; then
    pass "Configured CI with no runs yet stays PENDING"
else
    fail "Expected PENDING, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test: An unreadable workflow probe never reads as no CI ---
info "Test: a failing workflow probe with no runs stays PENDING, not STABLE"
setup_mock_gh
write_threads_json '[]'
echo '[]' > "$MOCK_DIR/runs.json"
# No workflow_count.txt: the mock's workflow probe exits non-zero (unreadable).
# A zero grace makes the registration window expire at once, so only the
# unproven-CI rule can keep this pending.
output=$(MONITOR_RUN_REGISTRATION_GRACE_SECONDS=0 run_monitor_bounded)
if [[ "$output" == "PENDING" ]]; then
    pass "Unreadable workflow probe stays PENDING after the grace expires"
else
    fail "Expected PENDING, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test: No workflows but a required context still waits for it ---
info "Test: zero workflows with a required context does not count as no CI"
setup_mock_gh
write_threads_json '[]'
echo '[]' > "$MOCK_DIR/runs.json"
echo '0' > "$MOCK_DIR/workflow_count.txt"
echo '["CI / build"]' > "$MOCK_DIR/required_contexts.json"
output=$(run_monitor_bounded)
if [[ "$output" == "PENDING" ]]; then
    pass "Required context with no runs stays PENDING"
else
    fail "Expected PENDING, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test: review threads are paginated, an unresolved thread on page 2 blocks ---
info "Test: unresolved thread beyond the first 100 blocks (paginated GraphQL)"
setup_mock_gh
write_threads_json '[
    {"id":"T1","isResolved":true,"isOutdated":false,"comments":{"nodes":[{"author":{"login":"human-reviewer"}}]}}
]'
cat > "$MOCK_DIR/threads_page2.json" << 'EOF'
{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[{"id":"T101","isResolved":false,"isOutdated":false,"comments":{"nodes":[{"author":{"login":"human-reviewer"}}]}}]}}}}}
EOF
output=$(run_monitor)
if [[ "$output" == "COMMENTS_FOUND:1" ]]; then
    pass "Unresolved thread on page 2 is counted"
else
    fail "Expected COMMENTS_FOUND:1, got: $output"
fi
if grep -q "api graphql --paginate" "$MOCK_DIR/gh_calls.log"; then
    pass "GraphQL call uses --paginate"
else
    fail "GraphQL call must use --paginate"
fi
cleanup; MOCK_DIR=""

# --- Test: a GraphQL page without a nodes array is an ERROR, never STABLE ---
info "Test: GraphQL error payload yields ERROR rather than STABLE"
setup_mock_gh
echo '{"errors":[{"message":"rate limited"}]}' > "$MOCK_DIR/threads.json"
output=$(run_monitor)
if [[ "$output" == ERROR:* ]]; then
    pass "Page without reviewThreads nodes produces a loud ERROR"
else
    fail "Expected ERROR:*, got: $output"
fi
cleanup; MOCK_DIR=""

# --- Test: gh run list asks for more than its default 20 runs ---
info "Test: gh run list passes an explicit high --limit"
setup_mock_gh
write_threads_json '[]'
output=$(run_monitor)
if grep "^run list" "$MOCK_DIR/gh_calls.log" | grep -q -- "--limit 1000"; then
    pass "gh run list is called with --limit 1000"
else
    fail "Expected --limit 1000 on gh run list. Calls: $(cat "$MOCK_DIR/gh_calls.log")"
fi
cleanup; MOCK_DIR=""

# --- Test: ERR trap names the aborting line (bug #2477) ---
info "Test: set -e abort prints an ABORTED diagnostic naming the line and command"
trap_output=$(bash -c "
    set -euo pipefail
    RED=''; NC=''
    $(declare -f on_err)
    trap 'on_err \$LINENO' ERR
    false
" 2>&1 || true)
if [[ "$trap_output" == *"ABORTED at line"* && "$trap_output" == *"false"* ]]; then
    pass "ERR trap reported the aborting line and command"
else
    fail "Expected 'ABORTED at line ...: false' on stderr, got: $trap_output"
fi

# --- Test: --poll-interval 0 is rejected (would busy-loop on `sleep 0`) ---
info "Test: --poll-interval 0 is rejected as non-positive"
setup_mock_gh
rc=0
err=$(PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" timeout "$MONITOR_TEST_TIMEOUT" bash "$MONITOR_SCRIPT" 42 0 --poll-interval 0 2>&1) || rc=$?
if [[ "$rc" -eq 1 && "$err" == *"--poll-interval requires a positive integer"* ]]; then
    pass "--poll-interval 0 exits 1 with the positive-integer error"
else
    fail "Expected exit 1 and a positive-integer error, got rc=$rc: $err"
fi
cleanup; MOCK_DIR=""

# --- Summary ---
echo ""
if [[ "$FAILURES" -eq 0 ]]; then
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}${FAILURES} test(s) failed.${NC}"
    exit 1
fi
