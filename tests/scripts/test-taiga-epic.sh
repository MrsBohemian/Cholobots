#!/usr/bin/env bash
# ABOUTME: Unit tests for taiga-epic.sh: get/patch an epic description by ref, with mocked curl.
# ABOUTME: Verifies the epics/by_ref endpoint in project 39, the versioned PATCH body, exit 44 on 404 and exit 3 on conflict.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SCRIPT_UNDER_TEST="$REPO_ROOT/scripts/taiga-epic.sh"

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

# The mock logs "METHOD URL" per call, every argv word, and every PATCH body.
setup_mock_curl() {
    cleanup
    MOCK_DIR=$(mktemp -d "/tmp/test-taiga-epic-XXXXXX")
    export CURL_LOG="$MOCK_DIR/curl.log" CURL_ARGV="$MOCK_DIR/argv.log" CURL_STDIN="$MOCK_DIR/stdin.log"

    cat > "$MOCK_DIR/curl" << 'MOCKSCRIPT'
#!/usr/bin/env bash
out=""; url=""; prev=""; method="GET"; has_body=false
for arg in "$@"; do
    [[ "$prev" == "-o" ]] && out="$arg"
    [[ "$prev" == "-X" ]] && method="$arg"
    [[ "$arg" == "--data-binary" ]] && has_body=true
    [[ "$arg" == http* ]] && url="$arg"
    prev="$arg"
done
printf '%s\n' "$@" >> "$CURL_ARGV"
echo "$method $url" >> "$CURL_LOG"
if [[ "$has_body" == true ]]; then
    body=$(cat)
    [[ "$method" == "PATCH" ]] && printf '%s\n' "$body" >> "$CURL_STDIN"
fi
EPIC='{"id": 77, "ref": 3, "version": 5, "subject": "Multi-business isolation", "status": 1, "status_extra_info": {"name": "New", "is_closed": false}, "tags": [], "is_closed": false, "description": "## Dependency Diagram\n\n#24\n"}'
case "$method $url" in
    "POST "*/api/v1/auth)
        echo '{"auth_token": "stub-token"}' > "$out"; printf 200 ;;
    "GET "*"/api/v1/epics/by_ref?project="*"&ref=3")
        printf '%s' "$EPIC" > "$out"; printf 200 ;;
    "GET "*/api/v1/epics/by_ref*)
        echo '{"detail": "No Epic matches the given query."}' > "$out"; printf 404 ;;
    "PATCH "*/api/v1/epics/77)
        if [[ "${STUB_PATCH_CODE:-200}" == "400" ]]; then
            printf '{"version": "The version doesn'"'"'t match with the current one"}' > "$out"; printf 400
        else
            echo '{"id": 77, "ref": 3, "version": 6}' > "$out"; printf 200
        fi ;;
    *)
        echo '{}' > "$out"; printf 500 ;;
esac
MOCKSCRIPT
    chmod +x "$MOCK_DIR/curl"
    printf '## Dependency Diagram\n\n#24 -> #25\n' > "$MOCK_DIR/desc.md"
}

run_script() {
    TAIGA_API_URL="https://taiga.stub.test" \
    TAIGA_USERNAME="stub-user" \
    TAIGA_PASSWORD="stub-pass-do-not-leak" \
    PATH="$MOCK_DIR:$PATH" \
    bash "$SCRIPT_UNDER_TEST" "$@"
}

test_get_uses_epics_by_ref_in_project_39() {
    info "Test: get <ref> reads /api/v1/epics/by_ref?project=39"
    setup_mock_curl
    local output rc=0
    output=$(run_script get 3 2>&1) && rc=0 || rc=$?
    if [ "$rc" -eq 0 ] && grep -q 'GET .*/api/v1/epics/by_ref?project=39&ref=3$' "$CURL_LOG"; then
        pass "get hits epics/by_ref?project=39&ref=3"
    else
        fail "Expected epics/by_ref?project=39 (rc=$rc): $(cat "$CURL_LOG")"
    fi
    if [ "$(jq -r '.version' <<< "$output")" = "5" ] && jq -r '.description' <<< "$output" | grep -q 'Dependency Diagram'; then
        pass "get prints version and description as JSON"
    else
        fail "Expected JSON with version 5 and the description, got: $output"
    fi
}

test_get_404_exits_44() {
    info "Test: get of a missing epic exits 44"
    setup_mock_curl
    local output rc=0
    output=$(run_script get 999 2>&1) && rc=0 || rc=$?
    if [ "$rc" -eq 44 ] && grep -q "No epic found with ref #999 in project 39" <<< "$output"; then
        pass "404 exits 44 naming project 39"
    else
        fail "Expected exit 44, got $rc: $output"
    fi
}

test_patch_sends_description_and_given_version() {
    info "Test: patch sends the file bytes and --version"
    setup_mock_curl
    local output rc=0
    output=$(run_script patch 3 --description-file "$MOCK_DIR/desc.md" --version 5 2>&1) && rc=0 || rc=$?
    if [ "$rc" -eq 0 ] && grep -q 'PATCH .*/api/v1/epics/77$' "$CURL_LOG"; then
        pass "PATCHes epics/77 (id resolved from the ref)"
    else
        fail "Expected PATCH epics/77 (rc=$rc, output: $output): $(cat "$CURL_LOG")"
    fi
    if [ "$(jq -r '.version' "$CURL_STDIN")" = "5" ] \
        && [ "$(jq -r '.description' "$CURL_STDIN")" = "$(cat "$MOCK_DIR/desc.md")" ]; then
        pass "PATCH body carries version 5 and the description bytes"
    else
        fail "Unexpected PATCH body: $(cat "$CURL_STDIN")"
    fi
    if grep -q "version=6" <<< "$output"; then
        pass "Reports the new version"
    else
        fail "Expected 'version=6' in output: $output"
    fi
}

test_patch_defaults_to_fetched_version() {
    info "Test: patch without --version sends the fetched version"
    setup_mock_curl
    run_script patch 3 --description-file "$MOCK_DIR/desc.md" > /dev/null 2>&1 || true
    if [ "$(jq -r '.version' "$CURL_STDIN")" = "5" ]; then
        pass "Fetched version 5 sent"
    else
        fail "Expected version 5 in PATCH body: $(cat "$CURL_STDIN" 2>/dev/null)"
    fi
}

test_patch_conflict_exits_3() {
    info "Test: a stale --version exits 3"
    setup_mock_curl
    local output rc=0
    output=$(STUB_PATCH_CODE=400 run_script patch 3 --description-file "$MOCK_DIR/desc.md" --version 4 2>&1) && rc=0 || rc=$?
    if [ "$rc" -eq 3 ] && grep -q "version conflict" <<< "$output"; then
        pass "Version conflict exits 3"
    else
        fail "Expected exit 3, got $rc: $output"
    fi
}

test_patch_dry_run_contacts_nothing() {
    info "Test: patch --dry-run needs no credentials and makes no calls"
    setup_mock_curl
    local output rc=0
    output=$(env -u TAIGA_PASSWORD PATH="$MOCK_DIR:$PATH" bash "$SCRIPT_UNDER_TEST" patch 3 \
        --description-file "$MOCK_DIR/desc.md" --dry-run 2>&1) && rc=0 || rc=$?
    if [ "$rc" -eq 0 ] && [ ! -s "$CURL_LOG" ] && grep -q "chars" <<< "$output"; then
        pass "Dry run printed a redacted body without contacting Taiga"
    else
        fail "Expected a redacted dry run with no curl calls (rc=$rc): $output"
    fi
}

test_password_never_in_argv() {
    info "Test: the password never appears in curl argv"
    setup_mock_curl
    run_script get 3 > /dev/null 2>&1 || true
    run_script patch 3 --description-file "$MOCK_DIR/desc.md" > /dev/null 2>&1 || true
    if grep -q "stub-pass-do-not-leak" "$CURL_ARGV"; then
        fail "Password leaked into curl argv"
    else
        pass "Password kept out of curl argv"
    fi
}

test_usage_errors() {
    info "Test: usage errors"
    setup_mock_curl
    local rc=0
    run_script patch 3 > /dev/null 2>&1 && rc=0 || rc=$?
    if [ "$rc" -eq 1 ]; then pass "patch without --description-file exits 1"; else fail "Expected exit 1, got $rc"; fi
    run_script bogus 3 > /dev/null 2>&1 && rc=0 || rc=$?
    if [ "$rc" -eq 1 ]; then pass "unknown command exits 1"; else fail "Expected exit 1, got $rc"; fi
    run_script --help > /dev/null 2>&1 && rc=0 || rc=$?
    if [ "$rc" -eq 0 ]; then pass "--help exits 0"; else fail "Expected exit 0, got $rc"; fi
}

test_get_uses_epics_by_ref_in_project_39
test_get_404_exits_44
test_patch_sends_description_and_given_version
test_patch_defaults_to_fetched_version
test_patch_conflict_exits_3
test_patch_dry_run_contacts_nothing
test_password_never_in_argv
test_usage_errors

echo ""
if [ "$FAILURES" -gt 0 ]; then
    echo -e "${RED}${FAILURES} test(s) failed${NC}"
    exit 1
else
    echo -e "${GREEN}All tests passed${NC}"
    exit 0
fi
