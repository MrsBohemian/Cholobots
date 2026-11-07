#!/usr/bin/env bash
# ABOUTME: Unit tests for taiga-story.sh's --epic lookup and its unconditional description field.
# ABOUTME: Verifies the epics/by_ref endpoint, the epic 404 label, intact description, and the date/version keys, using mocked curl.
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

# The mock records every URL it is asked for in $MOCK_DIR/urls.log so a test
# can assert which by_ref endpoint the script chose.
setup_mock_curl() {
    MOCK_DIR=$(mktemp -d "/tmp/test-taiga-story-epic-XXXXXX")

    cat > "$MOCK_DIR/curl" << 'MOCKSCRIPT'
#!/usr/bin/env bash
# Mock curl command for taiga-story.sh --epic tests

URL=""
OUTPUT_FILE=""
WRITE_OUT=""

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

echo "$URL" >> "$MOCK_URL_LOG"

# A dependency diagram with real newlines, JSON-escaped for the response body.
DIAGRAM='## Dependency Diagram\n\n```\n#100 (Root)\n  └─► #200 (Child)\n```\n'

case "$URL" in
    */api/v1/auth)
        echo '{"auth_token":"mock-token-123"}'
        ;;
    */api/v1/epics/by_ref*|*/api/v1/userstories/by_ref*|*/api/v1/issues/by_ref*)
        HTTP_CODE="${MOCK_HTTP_CODE:-200}"
        if [[ "$HTTP_CODE" == "200" ]]; then
            RESPONSE="{\"id\":100,\"ref\":9999,\"subject\":\"Test item\",\"description\":\"${DIAGRAM}\",\"description_html\":\"<p>ignored</p>\",\"status\":15,\"status_extra_info\":{\"name\":\"New\",\"is_closed\":false},\"milestone\":null,\"milestone_name\":null,\"milestone_slug\":null,\"tags\":[],\"is_closed\":false,\"created_date\":\"2026-11-06T15:00:00.000Z\",\"modified_date\":\"2026-11-07T09:30:00.000Z\",\"version\":7}"
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
    MOCK_URL_LOG="$MOCK_DIR/urls.log" \
    PATH="$MOCK_DIR:$PATH" \
    bash "$SCRIPT_UNDER_TEST" "$@"
}

# Read one key out of the script's JSON output.
json_key() {
    python3 -c "import json,sys; print(json.load(sys.stdin).get(sys.argv[1], '<<missing>>'))" "$1"
}

# ============================================================
# Test: --epic requests the epics by_ref endpoint
# ============================================================
test_epic_flag_uses_epics_endpoint() {
    info "Test: --epic requests /api/v1/epics/by_ref"
    setup_mock_curl
    export MOCK_HTTP_CODE=200

    local rc=0
    run_script 9999 39 --epic > /dev/null 2>&1 || rc=$?

    if [ "$rc" -eq 0 ]; then
        pass "Exits 0 on a successful epic lookup"
    else
        fail "Expected exit 0, got $rc"
    fi

    if grep -q "/api/v1/epics/by_ref" "$MOCK_DIR/urls.log"; then
        pass "Requests the epics by_ref endpoint"
    else
        fail "Expected an epics/by_ref request, got: $(cat "$MOCK_DIR/urls.log")"
    fi

    if grep -q "/api/v1/userstories/by_ref" "$MOCK_DIR/urls.log"; then
        fail "Must not fall back to the userstories endpoint for --epic"
    else
        pass "Does not request the userstories endpoint"
    fi

    unset MOCK_HTTP_CODE
}

# ============================================================
# Test: epic output carries the description markdown intact
# ============================================================
test_epic_output_includes_description() {
    info "Test: --epic output includes description markdown"
    setup_mock_curl
    export MOCK_HTTP_CODE=200

    local description
    description=$(run_script 9999 39 --epic 2>/dev/null | json_key description)

    if [[ "$description" == *"## Dependency Diagram"* ]]; then
        pass "description carries the Dependency Diagram section"
    else
        fail "Expected '## Dependency Diagram' in description, got: $description"
    fi

    if [[ "$description" == *"#100 (Root)"* && "$description" == *"#200 (Child)"* ]]; then
        pass "description preserves the diagram body"
    else
        fail "Expected diagram refs in description, got: $description"
    fi

    unset MOCK_HTTP_CODE
}

# ============================================================
# Test: description is emitted for a plain user-story lookup too
# ============================================================
test_userstory_output_includes_description() {
    info "Test: user-story output includes description (field is unconditional)"
    setup_mock_curl
    export MOCK_HTTP_CODE=200

    local description
    description=$(run_script 9999 39 2>/dev/null | json_key description)

    if [[ "$description" == *"## Dependency Diagram"* ]]; then
        pass "description present on a user-story lookup"
    else
        fail "Expected description on user-story lookup, got: $description"
    fi

    unset MOCK_HTTP_CODE
}

# ============================================================
# Test: created_date, modified_date and version for every item type
# ============================================================
# The MCP get_issue cannot return these; triage-bugs reads them here for the
# triage window, the 14-day Needs Info expiry and the 90-day Low review.
test_output_includes_dates_and_version() {
    info "Test: output includes created_date, modified_date and version for story, issue and epic"
    setup_mock_curl
    export MOCK_HTTP_CODE=200

    local flag label output
    for flag in "" "--issue" "--epic"; do
        label="${flag:-story}"
        # shellcheck disable=SC2086
        output=$(run_script 9999 39 $flag 2>/dev/null)

        if [[ "$(json_key created_date <<< "$output")" == "2026-11-06T15:00:00.000Z" ]]; then
            pass "$label: created_date present"
        else
            fail "$label: expected created_date, got: $output"
        fi
        if [[ "$(json_key modified_date <<< "$output")" == "2026-11-07T09:30:00.000Z" ]]; then
            pass "$label: modified_date present"
        else
            fail "$label: expected modified_date, got: $output"
        fi
        if [[ "$(json_key version <<< "$output")" == "7" ]]; then
            pass "$label: version present"
        else
            fail "$label: expected version 7, got: $output"
        fi
    done

    # Additive: the existing keys keep their order and the new ones follow.
    local keys
    keys=$(run_script 9999 39 --issue 2>/dev/null | python3 -c "import json,sys; print(','.join(json.load(sys.stdin)))")
    if [[ "$keys" == "id,ref,subject,description,status,status_extra_info,milestone,milestone_name,milestone_slug,tags,is_closed,created_date,modified_date,version" ]]; then
        pass "Existing key order kept; new keys appended"
    else
        fail "Unexpected key order: $keys"
    fi

    unset MOCK_HTTP_CODE
}

# ============================================================
# Test: epic 404 exits 44 and says "epic"
# ============================================================
test_epic_404_exits_44() {
    info "Test: epic by_ref 404 exits 44 with 'epic' label"
    setup_mock_curl
    export MOCK_HTTP_CODE=404

    local output rc=0
    output=$(run_script 9999 39 --epic 2>&1) && rc=0 || rc=$?

    if [ "$rc" -eq 44 ]; then
        pass "Exits 44 on epic 404"
    else
        fail "Expected exit 44, got $rc. Output: $output"
    fi

    # Here-strings, not echo-to-grep pipes: under pipefail, grep -q's early exit can SIGPIPE the writer (bug #2791).
    if grep -q "No epic found with ref #9999 in project 39" <<< "$output"; then
        pass "Message reports 'epic' not 'user story'"
    else
        fail "Expected 'No epic found with ref #9999 in project 39', got: $output"
    fi

    unset MOCK_HTTP_CODE
}

echo ""
echo "=== taiga-story.sh --epic and description tests ==="
echo ""

test_epic_flag_uses_epics_endpoint
test_epic_output_includes_description
test_userstory_output_includes_description
test_output_includes_dates_and_version
test_epic_404_exits_44

echo ""
if [ "$FAILURES" -gt 0 ]; then
    echo -e "${RED}${FAILURES} test(s) failed${NC}"
    exit 1
else
    echo -e "${GREEN}All tests passed${NC}"
    exit 0
fi
