#!/usr/bin/env bash
# ABOUTME: Unit tests for taiga-update-on-merge.sh script.
# ABOUTME: Verifies opt-in Taiga status and task updates on PR merge using mocked curl responses.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SCRIPT_UNDER_TEST="$REPO_ROOT/scripts/taiga-update-on-merge.sh"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}✓ $1${NC}"; }
fail() { echo -e "${RED}✗ $1${NC}"; FAILURES=$((FAILURES + 1)); }
info() { echo -e "${YELLOW}→ $1${NC}"; }

FAILURES=0
MOCK_DIR=""
# setup_mock_curl runs once per case; remember every dir so the EXIT trap
# removes all of them, not just the last one.
MOCK_DIRS=()

cleanup() {
    local dir
    for dir in "${MOCK_DIRS[@]}"; do
        [ -d "$dir" ] && rm -rf "$dir"
    done
}
trap cleanup EXIT

# Create a mock curl command that returns canned responses.
# The mock uses MOCK_DIR files and a call log to verify API interactions.
setup_mock_curl() {
    MOCK_DIR=$(mktemp -d "/tmp/test-taiga-merge-XXXXXX")
    MOCK_DIRS+=("$MOCK_DIR")

    # Initialize call log
    echo -n "" > "$MOCK_DIR/curl_calls.log"

    cat > "$MOCK_DIR/curl" << 'MOCKSCRIPT'
#!/usr/bin/env bash
# Mock curl command for taiga-update-on-merge.sh tests

MOCK_DIR_LOCAL="$MOCK_DIR"

# Parse args to find URL and data
URL=""
DATA=""
METHOD="GET"
OUTPUT_FILE=""
WRITE_OUT=""
SILENT=false

args=("$@")
i=0
while [ $i -lt ${#args[@]} ]; do
    case "${args[$i]}" in
        -X) i=$((i+1)); METHOD="${args[$i]}" ;;
        # The script sends bodies on stdin (-d @-) so they stay out of argv.
        -d) i=$((i+1)); DATA="${args[$i]}"; if [ "$DATA" = "@-" ]; then DATA=$(cat); fi ;;
        -o) i=$((i+1)); OUTPUT_FILE="${args[$i]}" ;;
        -w) i=$((i+1)); WRITE_OUT="${args[$i]}" ;;
        -s|-sf|-sS) SILENT=true ;;
        -H) i=$((i+1)) ;; # skip headers
        *) if [[ ! "${args[$i]}" =~ ^- ]]; then URL="${args[$i]}"; fi ;;
    esac
    i=$((i+1))
done

# Log the call (arguments plus any stdin body) for assertions
echo "$* $DATA" >> "$MOCK_DIR_LOCAL/curl_calls.log"
echo "$*" >> "$MOCK_DIR_LOCAL/curl_argv.log"

# Route based on URL pattern
case "$URL" in
    */api/v1/auth)
        # Auth endpoint
        HTTP_CODE="${MOCK_AUTH_HTTP_CODE:-200}"
        if [[ "$HTTP_CODE" != "200" ]]; then
            RESPONSE='{"detail":"Invalid credentials"}'
        else
            RESPONSE='{"auth_token":"mock-token-123"}'
        fi
        if [ -n "$OUTPUT_FILE" ]; then
            echo "$RESPONSE" > "$OUTPUT_FILE"
        else
            echo "$RESPONSE"
        fi
        if [ "$WRITE_OUT" = "%{http_code}" ]; then echo "$HTTP_CODE"; fi
        ;;
    */api/v1/userstories/by_ref*)
        # User story by_ref lookup
        if [[ -f "$MOCK_DIR_LOCAL/userstory_response.json" ]]; then
            RESPONSE=$(cat "$MOCK_DIR_LOCAL/userstory_response.json")
        else
            RESPONSE=""
        fi
        HTTP_CODE="${MOCK_USERSTORY_HTTP_CODE:-200}"
        if [[ "$HTTP_CODE" == "404" ]]; then
            RESPONSE='{"detail":"Not found."}'
        fi
        if [ -n "$OUTPUT_FILE" ]; then
            echo "$RESPONSE" > "$OUTPUT_FILE"
        else
            echo "$RESPONSE"
        fi
        if [ "$WRITE_OUT" = "%{http_code}" ]; then echo "$HTTP_CODE"; fi
        ;;
    */api/v1/issues/by_ref*)
        # Issue by_ref lookup
        if [[ -f "$MOCK_DIR_LOCAL/issue_response.json" ]]; then
            RESPONSE=$(cat "$MOCK_DIR_LOCAL/issue_response.json")
        else
            RESPONSE='{"detail":"Not found."}'
        fi
        HTTP_CODE="${MOCK_ISSUE_HTTP_CODE:-200}"
        if [[ "$HTTP_CODE" == "404" ]]; then
            RESPONSE='{"detail":"Not found."}'
        fi
        if [ -n "$OUTPUT_FILE" ]; then
            echo "$RESPONSE" > "$OUTPUT_FILE"
        else
            echo "$RESPONSE"
        fi
        if [ "$WRITE_OUT" = "%{http_code}" ]; then echo "$HTTP_CODE"; fi
        ;;
    */api/v1/userstories/*)
        # User story PATCH
        RESPONSE='{"id":100,"version":2,"status":80,"status_extra_info":{"name":"S80","is_closed":false}}'
        if [ -n "$OUTPUT_FILE" ]; then
            echo "$RESPONSE" > "$OUTPUT_FILE"
        else
            echo "$RESPONSE"
        fi
        if [ "$WRITE_OUT" = "%{http_code}" ]; then echo "200"; fi
        ;;
    */api/v1/issues/*)
        # Issue PATCH
        RESPONSE='{"id":200,"version":2,"status":74,"status_extra_info":{"name":"S74","is_closed":false}}'
        if [ -n "$OUTPUT_FILE" ]; then
            echo "$RESPONSE" > "$OUTPUT_FILE"
        else
            echo "$RESPONSE"
        fi
        if [ "$WRITE_OUT" = "%{http_code}" ]; then echo "200"; fi
        ;;
    */api/v1/tasks\?*)
        # Task list query
        if [[ -f "$MOCK_DIR_LOCAL/tasks_response.json" ]]; then
            RESPONSE=$(cat "$MOCK_DIR_LOCAL/tasks_response.json")
        else
            RESPONSE='[]'
        fi
        if [ -n "$OUTPUT_FILE" ]; then
            echo "$RESPONSE" > "$OUTPUT_FILE"
        else
            echo "$RESPONSE"
        fi
        if [ "$WRITE_OUT" = "%{http_code}" ]; then echo "200"; fi
        ;;
    */api/v1/tasks/*)
        # Task PATCH (close task); MOCK_TASK_PATCH_HTTP_CODE forces a failure
        RESPONSE='{"id":300,"version":2,"status":62,"status_extra_info":{"name":"S62","is_closed":false}}'
        if [ -n "$OUTPUT_FILE" ]; then
            echo "$RESPONSE" > "$OUTPUT_FILE"
        else
            echo "$RESPONSE"
        fi
        if [ "$WRITE_OUT" = "%{http_code}" ]; then echo "${MOCK_TASK_PATCH_HTTP_CODE:-200}"; fi
        ;;
    *)
        echo "MOCK_ERROR: unexpected URL: $URL" >&2
        exit 1
        ;;
esac
MOCKSCRIPT
    chmod +x "$MOCK_DIR/curl"

    # Also mock python3 for JSON extraction (same as real script uses)
    # Actually, let python3 be real — it's just for JSON parsing
}

# Run the script with mocked curl
run_script() {
    PR_LABELS="${PR_LABELS:-}" \
    TAIGA_API_URL="https://taiga.example.com" \
    TAIGA_USERNAME="testuser" \
    TAIGA_PASSWORD="testpass" \
    MOCK_DIR="$MOCK_DIR" \
    PATH="$MOCK_DIR:$PATH" \
    bash "$SCRIPT_UNDER_TEST" "$@" 2>&1
}

# Count logged curl calls matching an extended regex.
count_calls() {
    grep -cE "$1" "$MOCK_DIR/curl_calls.log" || true
}

# PATCHes to a user story carrying a status change.
story_status_patches() { count_calls 'PATCH .*/api/v1/userstories/[0-9]+ .*"status"'; }
# PATCHes to an issue carrying a status change.
issue_status_patches() { count_calls 'PATCH .*/api/v1/issues/[0-9]+ .*"status"'; }
# PATCHes closing tasks.
task_patches() { count_calls 'PATCH .*/api/v1/tasks/[0-9]+'; }

# Writes a user story lookup response with the given status, tags JSON, item-level is_closed and status-level is_closed.
write_story() {
    local status="$1" tags="${2:-[]}" is_closed="${3:-false}" status_is_closed="${4:-false}"
    cat > "$MOCK_DIR/userstory_response.json" << EOF
{"id":100,"ref":689,"subject":"Test story","status":${status},"version":5,"is_closed":${is_closed},"status_extra_info":{"name":"S${status}","is_closed":${status_is_closed}},"tags":${tags}}
EOF
}

# Writes two open tasks for the story.
write_two_open_tasks() {
    cat > "$MOCK_DIR/tasks_response.json" << 'EOF'
[{"id":301,"ref":223,"subject":"Task 1","status":59,"version":2,"is_closed":false},{"id":302,"ref":224,"subject":"Task 2","status":59,"version":3,"is_closed":false}]
EOF
}

# Runs the script for the story fixture and asserts the story-status and task PATCH counts.
assert_story_effects() {
    local name="$1" expected_status="$2" expected_tasks="$3"
    local output
    if ! output=$(run_script 689 522 "https://github.com/example/repo/pull/522" 2>&1); then
        fail "$name: script exited with error: $output"
        return
    fi
    local status_count task_count
    status_count=$(story_status_patches)
    task_count=$(task_patches)
    if [ "$status_count" -eq "$expected_status" ]; then
        pass "$name: $status_count story status PATCH(es)"
    else
        fail "$name: expected $expected_status story status PATCH(es), got $status_count. Output: $output"
    fi
    if [ "$task_count" -eq "$expected_tasks" ]; then
        pass "$name: $task_count task PATCH(es)"
    else
        fail "$name: expected $expected_tasks task PATCH(es), got $task_count. Output: $output"
    fi
}

# ============================================================
# Test: Missing arguments shows usage
# ============================================================
test_missing_args() {
    info "Test: Missing arguments shows usage"
    local output
    if output=$(run_script 2>&1); then
        fail "Should exit non-zero with no args"
    else
        # Here-strings, not echo-to-grep pipes: under pipefail, grep -q's early exit can SIGPIPE the writer (bug #2791).
        if grep -q "Usage" <<< "$output"; then
            pass "Missing args shows usage"
        else
            fail "Expected usage message, got: $output"
        fi
    fi
}

# ============================================================
# Test: Bug (issue) is updated to Closed
# ============================================================
test_close_bug() {
    info "Test: Bug is updated to Closed"
    setup_mock_curl

    # No userstory found, issue found with status != Closed
    export MOCK_USERSTORY_HTTP_CODE=404
    export MOCK_ISSUE_HTTP_CODE=200
    cat > "$MOCK_DIR/issue_response.json" << 'EOF'
{"id":200,"ref":1350,"subject":"Test bug","status":72,"version":3,"is_closed":false,"status_extra_info":{"name":"New","is_closed":false}}
EOF

    local output
    if output=$(PR_LABELS="mark-done-on-merge" run_script 1350 536 "https://github.com/example/repo/pull/536" 2>&1); then
        # Verify PATCH was called for issue status update
        if [ "$(issue_status_patches)" -eq 1 ] && grep -q '"status": 74' "$MOCK_DIR/curl_calls.log"; then
            pass "Bug updated to Closed"
        else
            fail "Expected PATCH to issues endpoint. Calls: $(cat "$MOCK_DIR/curl_calls.log")"
        fi
    else
        fail "Script exited with error: $output"
    fi

    unset MOCK_USERSTORY_HTTP_CODE MOCK_ISSUE_HTTP_CODE
}

# ============================================================
# Test: Story is updated to Done and open tasks are closed
# ============================================================
test_close_story_with_tasks() {
    info "Test: Story is updated to Done and open tasks are closed"
    setup_mock_curl

    export MOCK_USERSTORY_HTTP_CODE=200
    cat > "$MOCK_DIR/userstory_response.json" << 'EOF'
{"id":100,"ref":689,"subject":"Test story","status":76,"version":5,"is_closed":false,"status_extra_info":{"name":"New","is_closed":false}}
EOF

    cat > "$MOCK_DIR/tasks_response.json" << 'EOF'
[{"id":301,"ref":223,"subject":"Task 1","status":59,"version":2,"is_closed":false},{"id":302,"ref":224,"subject":"Task 2","status":59,"version":3,"is_closed":false}]
EOF

    local output
    if output=$(PR_LABELS="close-tasks-on-merge mark-done-on-merge" run_script 689 522 "https://github.com/example/repo/pull/522" 2>&1); then
        # Verify story PATCH
        if [ "$(story_status_patches)" -eq 1 ] && grep -q '"status": 80' "$MOCK_DIR/curl_calls.log"; then
            pass "Story updated to Done"
        else
            fail "Expected PATCH to userstories endpoint"
        fi
        # Verify task PATCHes (look for PATCH method in calls containing /tasks/)
        local task_patches
        task_patches=$(grep -c "PATCH.*/api/v1/tasks/" "$MOCK_DIR/curl_calls.log" || true)
        if [ "$task_patches" -ge 2 ]; then
            pass "Open tasks closed ($task_patches task PATCH calls)"
        else
            fail "Expected at least 2 task PATCH calls, got $task_patches"
        fi
    else
        fail "Script exited with error: $output"
    fi

    unset MOCK_USERSTORY_HTTP_CODE
}

# ============================================================
# Test: Already-closed item is skipped
# ============================================================
test_skip_already_closed() {
    info "Test: Already-closed item is skipped"
    setup_mock_curl

    export MOCK_USERSTORY_HTTP_CODE=404
    export MOCK_ISSUE_HTTP_CODE=200
    cat > "$MOCK_DIR/issue_response.json" << 'EOF'
{"id":200,"ref":1350,"subject":"Test bug","status":74,"version":3,"is_closed":true,"status_extra_info":{"name":"Closed","is_closed":true}}
EOF

    local output
    if output=$(run_script 1350 536 "https://github.com/example/repo/pull/536" 2>&1); then
        if grep -qi "skip\|already" <<< "$output"; then
            pass "Already-closed item skipped"
        else
            fail "Expected skip message, got: $output"
        fi
        # Verify no PATCH was attempted (only auth + lookups)
        local patch_count
        patch_count=$(grep -c "PATCH" "$MOCK_DIR/curl_calls.log" || true)
        if [ "$patch_count" -eq 0 ]; then
            pass "No PATCH calls for already-closed item"
        else
            fail "Expected 0 PATCH calls, got $patch_count"
        fi
    else
        fail "Script exited with error: $output"
    fi

    unset MOCK_USERSTORY_HTTP_CODE MOCK_ISSUE_HTTP_CODE
}

# ============================================================
# Test: Item not found exits 0 with warning
# ============================================================
test_item_not_found() {
    info "Test: Item not found exits 0 with warning"
    setup_mock_curl

    export MOCK_USERSTORY_HTTP_CODE=404
    export MOCK_ISSUE_HTTP_CODE=404

    local output exit_code=0
    output=$(run_script 99999 1 "https://github.com/example/repo/pull/1" 2>&1) || exit_code=$?

    if [ "$exit_code" -eq 0 ]; then
        pass "Exits 0 when item not found"
    else
        fail "Expected exit 0, got $exit_code"
    fi

    if grep -qi "not found\|no.*item\|warning" <<< "$output"; then
        pass "Warning message when item not found"
    else
        fail "Expected warning message, got: $output"
    fi

    unset MOCK_USERSTORY_HTTP_CODE MOCK_ISSUE_HTTP_CODE
}

# ============================================================
# Test: Dry-run mode does not PATCH
# ============================================================
test_dry_run() {
    info "Test: Dry-run mode does not PATCH"
    setup_mock_curl

    export MOCK_USERSTORY_HTTP_CODE=404
    export MOCK_ISSUE_HTTP_CODE=200
    cat > "$MOCK_DIR/issue_response.json" << 'EOF'
{"id":200,"ref":1350,"subject":"Test bug","status":72,"version":3,"is_closed":false,"status_extra_info":{"name":"New","is_closed":false}}
EOF

    local output
    if output=$(run_script --dry-run 1350 536 "https://github.com/example/repo/pull/536" 2>&1); then
        local patch_count
        patch_count=$(grep -c "PATCH" "$MOCK_DIR/curl_calls.log" || true)
        if [ "$patch_count" -eq 0 ]; then
            pass "Dry-run: no PATCH calls"
        else
            fail "Expected 0 PATCH calls in dry-run, got $patch_count"
        fi
        if grep -qi "dry.run\|would" <<< "$output"; then
            pass "Dry-run: informational output"
        else
            fail "Expected dry-run message, got: $output"
        fi
    else
        fail "Script exited with error in dry-run: $output"
    fi

    unset MOCK_USERSTORY_HTTP_CODE MOCK_ISSUE_HTTP_CODE
}

# ============================================================
# Test: Comment is posted on the Taiga item
# ============================================================
test_comment_posted() {
    info "Test: Comment is posted on the Taiga item"
    setup_mock_curl

    export MOCK_USERSTORY_HTTP_CODE=404
    export MOCK_ISSUE_HTTP_CODE=200
    cat > "$MOCK_DIR/issue_response.json" << 'EOF'
{"id":200,"ref":1350,"subject":"Test bug","status":72,"version":3,"is_closed":false,"status_extra_info":{"name":"New","is_closed":false}}
EOF

    local output
    if output=$(PR_LABELS="mark-done-on-merge" run_script 1350 536 "https://github.com/example/repo/pull/536" 2>&1); then
        # The comment is posted via PATCH with a comment field — look for the PR URL in curl calls
        if grep -q "Closed by PR #536" "$MOCK_DIR/curl_calls.log"; then
            pass "Comment posted with PR reference"
        else
            fail "Expected PR reference in API calls"
        fi
    else
        fail "Script exited with error: $output"
    fi

    unset MOCK_USERSTORY_HTTP_CODE MOCK_ISSUE_HTTP_CODE
}

# ============================================================
# Test: Auth failure reports HTTP code
# ============================================================
test_auth_failure_reports_http_code() {
    info "Test: Auth failure reports HTTP code"
    setup_mock_curl

    export MOCK_AUTH_HTTP_CODE=401

    local output exit_code=0
    output=$(run_script 1350 536 "https://github.com/example/repo/pull/536" 2>&1) || exit_code=$?

    if [ "$exit_code" -ne 0 ]; then
        pass "Exits non-zero on auth failure"
    else
        fail "Expected non-zero exit, got 0"
    fi

    if grep -q "HTTP 401" <<< "$output"; then
        pass "Reports HTTP status code"
    else
        fail "Expected 'HTTP 401' in output, got: $output"
    fi

    unset MOCK_AUTH_HTTP_CODE
}

# ============================================================
# Test: Auth failure reports response body
# ============================================================
test_auth_failure_reports_response_body() {
    info "Test: Auth failure reports response body"
    setup_mock_curl

    export MOCK_AUTH_HTTP_CODE=401

    local output exit_code=0
    output=$(run_script 1350 536 "https://github.com/example/repo/pull/536" 2>&1) || exit_code=$?

    if grep -q "Invalid credentials" <<< "$output"; then
        pass "Reports response body on auth failure"
    else
        fail "Expected 'Invalid credentials' in output, got: $output"
    fi

    unset MOCK_AUTH_HTTP_CODE
}

# ============================================================
# Test: Story with neither label nor tag only gets a comment
# ============================================================
test_lookups_use_project_39() {
    info "Test: lookups and the task list target Taiga project 39"
    setup_mock_curl
    export MOCK_USERSTORY_HTTP_CODE=200
    write_story 78 '["mark-done-on-merge", "close-tasks-on-merge"]'
    write_two_open_tasks
    run_script 689 522 "https://github.com/example/repo/pull/522" > /dev/null 2>&1 || true
    if grep -q 'userstories/by_ref?project=39&ref=689' "$MOCK_DIR/curl_calls.log" \
        && grep -q 'tasks?project=39&user_story=100' "$MOCK_DIR/curl_calls.log"; then
        pass "project 39: story lookup and task list use project=39"
    else
        fail "project 39: expected project=39 lookups. Calls: $(cat "$MOCK_DIR/curl_calls.log")"
    fi
    if grep -q '"status": 62' "$MOCK_DIR/curl_calls.log"; then
        pass "project 39: tasks close with status 62"
    else
        fail "project 39: expected task status 62. Calls: $(cat "$MOCK_DIR/curl_calls.log")"
    fi
}

test_story_neither_signal() {
    info "Test: Story with neither label nor tag only gets a comment"
    setup_mock_curl
    export MOCK_USERSTORY_HTTP_CODE=200
    write_story 78
    write_two_open_tasks

    assert_story_effects "neither" 0 0
    if [ "$(count_calls 'PATCH .*/api/v1/userstories/[0-9]+ .*Merged in PR #522')" -eq 1 ]; then
        pass "neither: one comment-only PATCH posted"
    else
        fail "neither: expected one 'Merged in PR #522' comment PATCH. Calls: $(cat "$MOCK_DIR/curl_calls.log")"
    fi

    unset MOCK_USERSTORY_HTTP_CODE
}

# ============================================================
# Test: a failed task close fails the sync after every task was tried
# ============================================================
test_story_task_close_failure_exits_nonzero() {
    info "Test: a failed task PATCH makes the script exit nonzero"
    setup_mock_curl
    export MOCK_USERSTORY_HTTP_CODE=200
    write_story 78
    write_two_open_tasks

    local output rc=0
    export MOCK_TASK_PATCH_HTTP_CODE=500
    output=$(PR_LABELS="close-tasks-on-merge" \
        run_script 689 522 "https://github.com/example/repo/pull/522" 2>&1) || rc=$?
    unset MOCK_TASK_PATCH_HTTP_CODE
    if [ "$rc" -ne 0 ]; then
        pass "task close failure: script exited $rc"
    else
        fail "task close failure: expected a nonzero exit. Output: $output"
    fi
    if [ "$(task_patches)" -eq 2 ]; then
        pass "task close failure: every task was still attempted"
    else
        fail "task close failure: expected 2 task PATCH attempts, got $(task_patches)"
    fi

    unset MOCK_USERSTORY_HTTP_CODE
}

# ============================================================
# Test: the task status-level is_closed flag decides which tasks are open
# ============================================================
test_story_tasks_use_status_level_closed_flag() {
    info "Test: task status-level is_closed overrides the item-level flag"
    setup_mock_curl
    export MOCK_USERSTORY_HTTP_CODE=200
    write_story 78
    # 301: item flag says open, status says closed -> skipped.
    # 302: item flag says closed, status says open -> closed.
    # 303: no status_extra_info -> falls back to the item flag (open) -> closed.
    cat > "$MOCK_DIR/tasks_response.json" << 'EOF'
[{"id":301,"ref":223,"status":62,"version":2,"is_closed":false,"status_extra_info":{"is_closed":true}},{"id":302,"ref":224,"status":59,"version":3,"is_closed":true,"status_extra_info":{"is_closed":false}},{"id":303,"ref":225,"status":59,"version":1,"is_closed":false}]
EOF

    PR_LABELS="close-tasks-on-merge" assert_story_effects "status-level flag" 0 2
    if [ "$(count_calls 'PATCH .*/api/v1/tasks/301 ')" -eq 0 ] \
        && [ "$(count_calls 'PATCH .*/api/v1/tasks/302 ')" -eq 1 ] \
        && [ "$(count_calls 'PATCH .*/api/v1/tasks/303 ')" -eq 1 ]; then
        pass "status-level flag: only the genuinely open tasks were PATCHed"
    else
        fail "status-level flag: wrong tasks PATCHed. Calls: $(cat "$MOCK_DIR/curl_calls.log")"
    fi

    unset MOCK_USERSTORY_HTTP_CODE
}

# ============================================================
# Test: close-tasks-on-merge label closes tasks only
# ============================================================
test_story_close_tasks_label() {
    info "Test: close-tasks-on-merge label closes tasks without changing status"
    setup_mock_curl
    export MOCK_USERSTORY_HTTP_CODE=200
    write_story 78
    write_two_open_tasks

    PR_LABELS="close-tasks-on-merge" assert_story_effects "close-tasks label" 0 2

    unset MOCK_USERSTORY_HTTP_CODE
}

# ============================================================
# Test: mark-done-on-merge label marks Done only
# ============================================================
test_story_mark_done_label() {
    info "Test: mark-done-on-merge label marks the story Done without closing tasks"
    setup_mock_curl
    export MOCK_USERSTORY_HTTP_CODE=200
    write_story 78
    write_two_open_tasks

    PR_LABELS="mark-done-on-merge" assert_story_effects "mark-done label" 1 0
    if grep -q '"status": 80' "$MOCK_DIR/curl_calls.log"; then
        pass "mark-done label: status 80 sent"
    else
        fail "mark-done label: expected status 80 in PATCH"
    fi

    unset MOCK_USERSTORY_HTTP_CODE
}

# ============================================================
# Test: close-tasks-on-merge Taiga tag closes tasks
# ============================================================
test_story_close_tasks_tag() {
    info "Test: close-tasks-on-merge tag closes tasks"
    setup_mock_curl
    export MOCK_USERSTORY_HTTP_CODE=200
    write_story 78 '[["ci", null], ["close-tasks-on-merge", null]]'
    write_two_open_tasks

    assert_story_effects "close-tasks tag" 0 2

    unset MOCK_USERSTORY_HTTP_CODE
}

# ============================================================
# Test: mark-done-on-merge Taiga tag marks Done (plain-string tags accepted)
# ============================================================
test_story_mark_done_tag() {
    info "Test: mark-done-on-merge tag marks the story Done"
    setup_mock_curl
    export MOCK_USERSTORY_HTTP_CODE=200
    write_story 78 '["mark-done-on-merge"]'
    write_two_open_tasks

    assert_story_effects "mark-done tag" 1 0

    unset MOCK_USERSTORY_HTTP_CODE
}

# ============================================================
# Test: Same signal as both label and tag acts once
# ============================================================
test_story_label_and_tag_same_effect() {
    info "Test: label and tag for the same effect close each task once"
    setup_mock_curl
    export MOCK_USERSTORY_HTTP_CODE=200
    write_story 78 '[["close-tasks-on-merge", "#123456"]]'
    write_two_open_tasks

    PR_LABELS="close-tasks-on-merge" assert_story_effects "label and tag" 0 2
    if [ "$(count_calls 'PATCH .*/api/v1/tasks/301 ')" -eq 1 ] && [ "$(count_calls 'PATCH .*/api/v1/tasks/302 ')" -eq 1 ]; then
        pass "label and tag: each task PATCHed exactly once"
    else
        fail "label and tag: expected one PATCH per task. Calls: $(cat "$MOCK_DIR/curl_calls.log")"
    fi

    unset MOCK_USERSTORY_HTTP_CODE
}

# ============================================================
# Test: Both labels mark Done and close tasks
# ============================================================
test_story_both_labels() {
    info "Test: both labels mark Done and close tasks"
    setup_mock_curl
    export MOCK_USERSTORY_HTTP_CODE=200
    write_story 78
    write_two_open_tasks

    PR_LABELS="mark-done-on-merge close-tasks-on-merge" assert_story_effects "both labels" 1 2

    unset MOCK_USERSTORY_HTTP_CODE
}

# ============================================================
# Test: Story already Done still has its tasks closed
# ============================================================
test_story_already_done_closes_tasks() {
    info "Test: story already Done still closes tasks with close-tasks-on-merge"
    setup_mock_curl
    export MOCK_USERSTORY_HTTP_CODE=200
    write_story 80 '[]' true true
    write_two_open_tasks

    PR_LABELS="close-tasks-on-merge" assert_story_effects "already Done" 0 2

    unset MOCK_USERSTORY_HTTP_CODE
}

# ============================================================
# Test: Issue with neither signal keeps its status
# ============================================================
test_issue_neither_signal() {
    info "Test: issue with neither label nor tag keeps its status"
    setup_mock_curl
    export MOCK_USERSTORY_HTTP_CODE=404
    export MOCK_ISSUE_HTTP_CODE=200
    cat > "$MOCK_DIR/issue_response.json" << 'EOF'
{"id":200,"ref":1350,"subject":"Test bug","status":72,"version":3,"is_closed":false,"status_extra_info":{"name":"New","is_closed":false},"tags":[]}
EOF

    local output
    if output=$(PR_LABELS="close-tasks-on-merge" run_script 1350 536 "https://github.com/example/repo/pull/536" 2>&1); then
        if [ "$(issue_status_patches)" -eq 0 ]; then
            pass "issue neither: no status PATCH"
        else
            fail "issue neither: expected no status PATCH. Calls: $(cat "$MOCK_DIR/curl_calls.log")"
        fi
        if [ "$(count_calls 'PATCH .*/api/v1/issues/[0-9]+ .*Merged in PR #536')" -eq 1 ]; then
            pass "issue neither: comment-only PATCH posted"
        else
            fail "issue neither: expected a 'Merged in PR #536' comment PATCH"
        fi
        if [ "$(count_calls '/api/v1/tasks')" -eq 0 ]; then
            pass "issue neither: close-tasks-on-merge ignored for issues"
        else
            fail "issue neither: expected no task calls for an issue"
        fi
    else
        fail "Script exited with error: $output"
    fi

    unset MOCK_USERSTORY_HTTP_CODE MOCK_ISSUE_HTTP_CODE
}

# ============================================================
# Test: Issue with mark-done-on-merge tag is Closed
# ============================================================
test_issue_mark_done_tag() {
    info "Test: issue with mark-done-on-merge tag is Closed"
    setup_mock_curl
    export MOCK_USERSTORY_HTTP_CODE=404
    export MOCK_ISSUE_HTTP_CODE=200
    cat > "$MOCK_DIR/issue_response.json" << 'EOF'
{"id":200,"ref":1350,"subject":"Test bug","status":72,"version":3,"is_closed":false,"status_extra_info":{"name":"New","is_closed":false},"tags":[["mark-done-on-merge", null]]}
EOF

    local output
    if output=$(run_script 1350 536 "https://github.com/example/repo/pull/536" 2>&1); then
        if [ "$(issue_status_patches)" -eq 1 ] && grep -q '"status": 74' "$MOCK_DIR/curl_calls.log"; then
            pass "issue tag: status 74 sent"
        else
            fail "issue tag: expected status 74 PATCH. Calls: $(cat "$MOCK_DIR/curl_calls.log")"
        fi
    else
        fail "Script exited with error: $output"
    fi

    unset MOCK_USERSTORY_HTTP_CODE MOCK_ISSUE_HTTP_CODE
}

# ============================================================
# Test: Story with all tasks closed but an open status is marked Done
# ============================================================
test_story_tasks_closed_status_open_marks_done() {
    info "Test: story whose tasks are all closed but whose status is open is marked Done"
    setup_mock_curl
    export MOCK_USERSTORY_HTTP_CODE=200
    write_story 79 '[]' true false

    PR_LABELS="mark-done-on-merge" assert_story_effects "tasks-closed open status" 1 0
    if grep -q '"status": 80' "$MOCK_DIR/curl_calls.log"; then
        pass "tasks-closed open status: status 80 sent"
    else
        fail "tasks-closed open status: expected status 80 in PATCH"
    fi

    unset MOCK_USERSTORY_HTTP_CODE
}

# ============================================================
# Test: Story at a closed non-target status (Archived) is skipped
# ============================================================
test_story_archived_status_skips() {
    info "Test: story at a closed non-target status is skipped"
    setup_mock_curl
    export MOCK_USERSTORY_HTTP_CODE=200
    write_story 81 '[]' false true

    PR_LABELS="mark-done-on-merge" assert_story_effects "archived" 0 0
    local output
    output=$(PR_LABELS="mark-done-on-merge" run_script 689 522 "https://github.com/example/repo/pull/522" 2>&1)
    if grep -q "already closed/done" <<< "$output"; then
        pass "archived: skip message logged"
    else
        fail "archived: expected 'already closed/done' message, got: $output"
    fi

    unset MOCK_USERSTORY_HTTP_CODE
}

# ============================================================
# Test: Story with all tasks closed, open status and no signal gets a comment
# ============================================================
test_story_tasks_closed_no_signal_comments() {
    info "Test: story whose tasks are all closed, with no signal, only gets a comment"
    setup_mock_curl
    export MOCK_USERSTORY_HTTP_CODE=200
    write_story 79 '[]' true false

    assert_story_effects "tasks-closed no signal" 0 0
    if [ "$(count_calls 'PATCH .*/api/v1/userstories/[0-9]+ .*Merged in PR #522')" -eq 1 ]; then
        pass "tasks-closed no signal: one comment-only PATCH posted"
    else
        fail "tasks-closed no signal: expected one 'Merged in PR #522' comment PATCH. Calls: $(cat "$MOCK_DIR/curl_calls.log")"
    fi

    unset MOCK_USERSTORY_HTTP_CODE
}

# ============================================================
# Test: Issue at a closed non-target status (Rejected) is skipped
# ============================================================
test_issue_rejected_status_skips() {
    info "Test: issue at a closed non-target status keeps its status"
    setup_mock_curl
    export MOCK_USERSTORY_HTTP_CODE=404
    export MOCK_ISSUE_HTTP_CODE=200
    cat > "$MOCK_DIR/issue_response.json" << 'EOF'
{"id":200,"ref":1350,"status":76,"version":3,"is_closed":true,"status_extra_info":{"name":"Rejected","is_closed":true},"tags":[]}
EOF

    local output
    if output=$(PR_LABELS="mark-done-on-merge" run_script 1350 536 "https://github.com/example/repo/pull/536" 2>&1); then
        if [ "$(issue_status_patches)" -eq 0 ]; then
            pass "issue rejected: no status PATCH"
        else
            fail "issue rejected: expected no status PATCH. Calls: $(cat "$MOCK_DIR/curl_calls.log")"
        fi
    else
        fail "Script exited with error: $output"
    fi

    unset MOCK_USERSTORY_HTTP_CODE MOCK_ISSUE_HTTP_CODE
}

# ============================================================
# Test: Missing status_extra_info is unknown, not open: no status change, no top-level fallback
# ============================================================
test_story_missing_status_extra_info_skips() {
    info "Test: story without status_extra_info keeps its status even with a true top-level is_closed"
    setup_mock_curl
    export MOCK_USERSTORY_HTTP_CODE=200
    cat > "$MOCK_DIR/userstory_response.json" << 'EOF'
{"id":100,"ref":689,"subject":"Test story","status":79,"version":5,"is_closed":true,"tags":[]}
EOF

    PR_LABELS="mark-done-on-merge" assert_story_effects "missing status_extra_info" 0 0
    local output
    output=$(PR_LABELS="mark-done-on-merge" run_script 689 522 "https://github.com/example/repo/pull/522" 2>&1)
    if grep -q "status_extra_info.is_closed missing" <<< "$output"; then
        pass "missing status_extra_info: warning logged"
    else
        fail "missing status_extra_info: expected warning, got: $output"
    fi

    unset MOCK_USERSTORY_HTTP_CODE
}

# ============================================================
# Test: Missing status_extra_info without a mark-done signal still posts the merge comment
# ============================================================
test_story_missing_status_extra_info_no_signal_comments() {
    info "Test: story without status_extra_info and no mark-done signal still gets the merge comment"
    setup_mock_curl
    export MOCK_USERSTORY_HTTP_CODE=200
    cat > "$MOCK_DIR/userstory_response.json" << 'EOF2'
{"id":100,"ref":689,"subject":"Test story","status":79,"version":5,"is_closed":true,"tags":[]}
EOF2

    assert_story_effects "missing status_extra_info no signal" 0 0
    if [ "$(count_calls 'PATCH .*/api/v1/userstories/[0-9]+ .*Merged in PR #522')" -eq 1 ]; then
        pass "missing status_extra_info no signal: one comment-only PATCH posted"
    else
        fail "missing status_extra_info no signal: expected one 'Merged in PR #522' comment PATCH. Calls: $(cat "$MOCK_DIR/curl_calls.log")"
    fi

    unset MOCK_USERSTORY_HTTP_CODE
}

# ============================================================
# Test: Issue without status_extra_info keeps its status
# ============================================================
test_issue_missing_status_extra_info_skips() {
    info "Test: issue without status_extra_info keeps its status"
    setup_mock_curl
    export MOCK_USERSTORY_HTTP_CODE=404
    export MOCK_ISSUE_HTTP_CODE=200
    cat > "$MOCK_DIR/issue_response.json" << 'EOF'
{"id":200,"ref":1350,"status":76,"version":3,"is_closed":true,"tags":[]}
EOF

    local output
    if output=$(PR_LABELS="mark-done-on-merge" run_script 1350 536 "https://github.com/example/repo/pull/536" 2>&1); then
        if [ "$(issue_status_patches)" -eq 0 ]; then
            pass "issue missing status_extra_info: no status PATCH"
        else
            fail "issue missing status_extra_info: expected no status PATCH. Calls: $(cat "$MOCK_DIR/curl_calls.log")"
        fi
    else
        fail "Script exited with error: $output"
    fi

    unset MOCK_USERSTORY_HTTP_CODE MOCK_ISSUE_HTTP_CODE
}

# ============================================================
# Run all tests
# ============================================================
# ============================================================
# Test: a non-404 story lookup failure is an error, not "try the issue endpoint"
# (a 500 followed by an issue 404 must not end the run as a successful no-op)
# ============================================================
test_story_lookup_server_error_fails() {
    info "Test: user story lookup HTTP 500 exits non-zero without syncing"
    setup_mock_curl

    export MOCK_USERSTORY_HTTP_CODE=500
    export MOCK_ISSUE_HTTP_CODE=404

    local output exit_code=0
    output=$(PR_LABELS="mark-done-on-merge" run_script 536 1 "https://github.com/example/repo/pull/1" 2>&1) || exit_code=$?

    if [ "$exit_code" -ne 0 ]; then
        pass "Exits non-zero on story lookup HTTP 500"
    else
        fail "Expected non-zero exit on story HTTP 500, got 0. Output: $output"
    fi

    if grep -q "HTTP 500" <<< "$output"; then
        pass "Error names the failing status"
    else
        fail "Expected 'HTTP 500' in output, got: $output"
    fi

    if [ "$(count_calls '/api/v1/issues/by_ref')" -eq 0 ]; then
        pass "Issue endpoint was not tried after a non-404 story failure"
    else
        fail "Issue endpoint must not be tried after story HTTP 500"
    fi

    unset MOCK_USERSTORY_HTTP_CODE MOCK_ISSUE_HTTP_CODE
}

test_issue_lookup_server_error_fails() {
    info "Test: issue lookup HTTP 500 (after a story 404) exits non-zero"
    setup_mock_curl

    export MOCK_USERSTORY_HTTP_CODE=404
    export MOCK_ISSUE_HTTP_CODE=500

    local output exit_code=0
    output=$(run_script 536 1 "https://github.com/example/repo/pull/1" 2>&1) || exit_code=$?

    if [ "$exit_code" -ne 0 ] && grep -q "HTTP 500" <<< "$output"; then
        pass "Exits non-zero on issue lookup HTTP 500"
    else
        fail "Expected non-zero exit naming HTTP 500, got $exit_code. Output: $output"
    fi

    unset MOCK_USERSTORY_HTTP_CODE MOCK_ISSUE_HTTP_CODE
}

# ============================================================
# Test: secrets and bodies stay off curl's argv; task listing is unpaginated
# ============================================================
test_credentials_and_bodies_not_in_argv() {
    info "Test: password and bearer token never appear on a curl command line"
    setup_mock_curl

    export MOCK_USERSTORY_HTTP_CODE=200
    cat > "$MOCK_DIR/userstory_response.json" << 'EOF'
{"id":100,"ref":689,"subject":"Test story","status":76,"version":5,"is_closed":false,"status_extra_info":{"name":"New","is_closed":false}}
EOF
    PR_LABELS="mark-done-on-merge close-tasks-on-merge" run_script 536 1 "https://github.com/example/repo/pull/1" >/dev/null 2>&1 || true

    # The mock logs argv followed by the stdin body, so split on the marker the
    # mock cannot add: assert on argv alone via the recorded call prefix.
    if grep -q "mock-token-123" "$MOCK_DIR/curl_argv.log"; then
        fail "Bearer token found in curl argv"
    else
        pass "Bearer token is not in curl argv"
    fi
    if grep -q "testpass" "$MOCK_DIR/curl_argv.log"; then
        fail "Password found in curl argv"
    else
        pass "Password is not in curl argv"
    fi
    if grep -q "x-disable-pagination: True" <<< "$(grep '/api/v1/tasks?' "$MOCK_DIR/curl_argv.log")"; then
        pass "Task listing disables pagination"
    else
        fail "Task listing must send x-disable-pagination: True"
    fi

    unset MOCK_USERSTORY_HTTP_CODE
}

echo ""
echo "=== taiga-update-on-merge.sh tests ==="
echo ""

test_missing_args
test_close_bug
test_close_story_with_tasks
test_skip_already_closed
test_item_not_found
test_dry_run
test_comment_posted
test_auth_failure_reports_http_code
test_auth_failure_reports_response_body
test_lookups_use_project_39
test_story_neither_signal
test_story_task_close_failure_exits_nonzero
test_story_tasks_use_status_level_closed_flag
test_story_close_tasks_label
test_story_mark_done_label
test_story_close_tasks_tag
test_story_mark_done_tag
test_story_label_and_tag_same_effect
test_story_both_labels
test_story_already_done_closes_tasks
test_issue_neither_signal
test_issue_mark_done_tag
test_story_tasks_closed_status_open_marks_done
test_story_archived_status_skips
test_story_tasks_closed_no_signal_comments
test_issue_rejected_status_skips
test_story_missing_status_extra_info_skips
test_story_missing_status_extra_info_no_signal_comments
test_issue_missing_status_extra_info_skips
test_story_lookup_server_error_fails
test_issue_lookup_server_error_fails
test_credentials_and_bodies_not_in_argv

echo ""
if [ "$FAILURES" -gt 0 ]; then
    echo -e "${RED}${FAILURES} test(s) failed${NC}"
    exit 1
else
    echo -e "${GREEN}All tests passed${NC}"
    exit 0
fi
