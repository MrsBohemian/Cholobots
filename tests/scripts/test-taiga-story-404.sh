#!/usr/bin/env bash
# ABOUTME: Unit tests for taiga-story.sh's by_ref 404 exit-code contract.
# ABOUTME: Verifies exit 44 on not-found, exit 1 on other errors, and exit 0 on success, using mocked curl.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SCRIPT_UNDER_TEST="$REPO_ROOT/scripts/taiga-story.sh"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

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

setup_mock_curl() {
    MOCK_DIR=$(mktemp -d "/tmp/test-taiga-story-XXXXXX")

    cat > "$MOCK_DIR/curl" << 'MOCKSCRIPT'
#!/usr/bin/env bash
# Mock curl command for taiga-story.sh tests

URL=""
OUTPUT_FILE=""
WRITE_OUT=""

# Record argv so tests can assert what was (not) put on the command line.
if [ -n "${MOCK_ARGV_LOG:-}" ]; then echo "$*" >> "$MOCK_ARGV_LOG"; fi

args=("$@")
i=0
while [ $i -lt ${#args[@]} ]; do
    case "${args[$i]}" in
        -o) i=$((i+1)); OUTPUT_FILE="${args[$i]}" ;;
        -w) i=$((i+1)); WRITE_OUT="${args[$i]}" ;;
        -d) i=$((i+1)) ;;
        -H) i=$((i+1)) ;;
        *) if [[ ! "${args[$i]}" =~ ^- ]]; then URL="${args[$i]}"; fi ;;
    esac
    i=$((i+1))
done

case "$URL" in
    */api/v1/auth)
        echo '{"auth_token":"mock-token-123"}'
        ;;
    */api/v1/tasks\?*)
        # 31 tasks: more than one default Taiga page (30) would hold.
        RESPONSE=$(python3 -c "import json; print(json.dumps([{'id': n, 'ref': n, 'subject': 't', 'status': 1, 'status_extra_info': {}, 'is_closed': False} for n in range(1, 32)]))")
        if [ -n "$OUTPUT_FILE" ]; then echo "$RESPONSE" > "$OUTPUT_FILE"; else echo "$RESPONSE"; fi
        if [ "$WRITE_OUT" = "%{http_code}" ]; then echo "200"; fi
        ;;
    */api/v1/userstories/by_ref*|*/api/v1/issues/by_ref*)
        HTTP_CODE="${MOCK_HTTP_CODE:-200}"
        if [[ "$HTTP_CODE" == "200" ]]; then
            RESPONSE='{"id":100,"ref":9999,"subject":"Test item","status":15,"status_extra_info":{"name":"New","is_closed":false},"milestone":null,"milestone_name":null,"milestone_slug":null,"tags":[],"is_closed":false}'
        else
            RESPONSE='{"detail":"Not found."}'
        fi
        if [ -n "$OUTPUT_FILE" ]; then
            echo "$RESPONSE" > "$OUTPUT_FILE"
        else
            echo "$RESPONSE"
        fi
        if [ "$WRITE_OUT" = "%{http_code}" ]; then echo "$HTTP_CODE"; fi
        ;;
    *)
        echo "MOCK_ERROR: unexpected URL: $URL" >&2
        exit 1
        ;;
esac
MOCKSCRIPT
    chmod +x "$MOCK_DIR/curl"
}

run_script() {
    TAIGA_API_URL="https://taiga.example.com" \
    TAIGA_USERNAME="testuser" \
    TAIGA_PASSWORD="testpass" \
    MOCK_HTTP_CODE="${MOCK_HTTP_CODE:-200}" \
    PATH="$MOCK_DIR:$PATH" \
    bash "$SCRIPT_UNDER_TEST" "$@"
}

# ============================================================
# Test: user-story 404 exits 44 with "user story" label
# ============================================================
test_userstory_404_exits_44() {
    info "Test: user-story by_ref 404 exits 44"
    setup_mock_curl
    export MOCK_HTTP_CODE=404

    local output rc=0
    output=$(run_script 9999 39 2>&1) && rc=0 || rc=$?

    if [ "$rc" -eq 44 ]; then
        pass "Exits 44 on user-story 404"
    else
        fail "Expected exit 44, got $rc. Output: $output"
    fi

    # Here-strings, not echo-to-grep pipes: under pipefail, grep -q's early exit can SIGPIPE the writer (bug #2791).
    if grep -q "No user story found with ref #9999 in project 39" <<< "$output"; then
        pass "Message reports 'user story' not 'userstorie'"
    else
        fail "Expected 'No user story found with ref #9999 in project 39', got: $output"
    fi

    unset MOCK_HTTP_CODE
}

# ============================================================
# Test: omitting the project argument targets project 39
# ============================================================
test_default_project_is_39() {
    info "Test: the default project is 39 (cholo-bots)"
    setup_mock_curl
    export MOCK_HTTP_CODE=404

    local output rc=0
    output=$(run_script 9999 2>&1) && rc=0 || rc=$?

    if [ "$rc" -eq 44 ] && grep -q "in project 39" <<< "$output"; then
        pass "Default project 39 reported in the 404 message"
    else
        fail "Expected exit 44 naming project 39, got rc=$rc: $output"
    fi

    unset MOCK_HTTP_CODE
}

# ============================================================
# Test: issue 404 exits 44 with "issue" label
# ============================================================
test_issue_404_exits_44() {
    info "Test: issue by_ref 404 (--issue) exits 44"
    setup_mock_curl
    export MOCK_HTTP_CODE=404

    local output rc=0
    output=$(run_script 9999 39 --issue 2>&1) && rc=0 || rc=$?

    if [ "$rc" -eq 44 ]; then
        pass "Exits 44 on issue 404"
    else
        fail "Expected exit 44, got $rc. Output: $output"
    fi

    if grep -q "No issue found with ref #9999 in project 39" <<< "$output"; then
        pass "Message reports 'issue'"
    else
        fail "Expected 'No issue found with ref #9999 in project 39', got: $output"
    fi

    unset MOCK_HTTP_CODE
}

# ============================================================
# Test: non-404 API error exits 1, not 44
# ============================================================
test_non_404_error_exits_1() {
    info "Test: non-404 API error exits 1"
    setup_mock_curl
    export MOCK_HTTP_CODE=500

    local output rc=0
    output=$(run_script 9999 39 2>&1) && rc=0 || rc=$?

    if [ "$rc" -eq 1 ]; then
        pass "Exits 1 on HTTP 500 (not 44)"
    else
        fail "Expected exit 1, got $rc. Output: $output"
    fi

    if grep -q "Taiga API error: HTTP 500" <<< "$output"; then
        pass "Reports HTTP status code"
    else
        fail "Expected 'Taiga API error: HTTP 500', got: $output"
    fi

    unset MOCK_HTTP_CODE
}

# ============================================================
# Test: 200 response exits 0 with compact JSON
# ============================================================
test_success_exits_0() {
    info "Test: 200 response exits 0 with compact JSON"
    setup_mock_curl
    export MOCK_HTTP_CODE=200

    local output rc=0
    output=$(run_script 9999 39 2>&1) && rc=0 || rc=$?

    if [ "$rc" -eq 0 ]; then
        pass "Exits 0 on success"
    else
        fail "Expected exit 0, got $rc. Output: $output"
    fi

    if grep -q '"id": 100' <<< "$output" && grep -q '"ref": 9999' <<< "$output"; then
        pass "Emits compact JSON with id/ref"
    else
        fail "Expected id/ref in JSON output, got: $output"
    fi

    unset MOCK_HTTP_CODE
}

# ============================================================
# Test: credentials stay off argv, tasks are unpaginated, no fixed /tmp files
# ============================================================
test_secrets_off_argv_and_tasks_unpaginated() {
    info "Test: no credentials in argv, full task list requested, no fixed /tmp paths"
    setup_mock_curl
    export MOCK_HTTP_CODE=200
    export MOCK_ARGV_LOG="$MOCK_DIR/argv.log"
    rm -f /tmp/taiga_item_response.json /tmp/taiga_tasks_response.json /tmp/taiga_tasks_compact.json

    local output rc=0
    output=$(run_script 9999 39 --with-tasks 2>&1) && rc=0 || rc=$?

    if [ "$rc" -eq 0 ]; then
        pass "--with-tasks exits 0"
    else
        fail "Expected exit 0, got $rc. Output: $output"
    fi

    if grep -q "testpass\|mock-token-123" "$MOCK_ARGV_LOG"; then
        fail "Password or bearer token appeared on a curl command line"
    else
        pass "Password and bearer token are not in curl argv"
    fi

    if grep "api/v1/tasks" "$MOCK_ARGV_LOG" | grep -q "x-disable-pagination: True"; then
        pass "Task listing sends x-disable-pagination: True"
    else
        fail "Task listing must disable pagination"
    fi

    if [ "$(python3 -c 'import json,sys; print(len(json.load(sys.stdin)["tasks"]))' <<< "$output")" -eq 31 ]; then
        pass "All 31 tasks are in the output"
    else
        fail "Expected 31 tasks in output"
    fi

    if ls /tmp/taiga_item_response.json /tmp/taiga_tasks_response.json /tmp/taiga_tasks_compact.json >/dev/null 2>&1; then
        fail "Script wrote predictable files under /tmp"
    else
        pass "No predictable /tmp files are written"
    fi

    unset MOCK_HTTP_CODE MOCK_ARGV_LOG
}

echo ""
echo "=== taiga-story.sh 404 exit-code tests ==="
echo ""

test_userstory_404_exits_44
test_default_project_is_39
test_issue_404_exits_44
test_non_404_error_exits_1
test_success_exits_0
test_secrets_off_argv_and_tasks_unpaginated

echo ""
if [ "$FAILURES" -gt 0 ]; then
    echo -e "${RED}${FAILURES} test(s) failed${NC}"
    exit 1
else
    echo -e "${GREEN}All tests passed${NC}"
    exit 0
fi
