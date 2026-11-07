#!/usr/bin/env bash
# ABOUTME: Tests for taiga-wiki.sh's REST get/patch over /api/v1/wiki, using a
# ABOUTME: stub curl on PATH so no live Taiga instance is needed.
#
# Guards the page body never reaching stdout (the point of the file-handle
# path), and a version conflict exiting 3 rather than 1 so roadmap-wiki-update
# can tell a concurrency conflict from a real failure.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SCRIPT_UNDER_TEST="$REPO_ROOT/scripts/taiga-wiki.sh"

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

# Stub curl: logs the requested URL and the full argv, captures any stdin
# body, and answers the auth POST / wiki GET / wiki PATCH with canned JSON
# written to the -o target, printing the HTTP code the way -w does.
# STUB_CURL_FAIL_METHOD (POST/GET/PATCH) plus STUB_CURL_EXIT make curl fail the
# way a transport error does: a curl-style message on stderr, no HTTP code on
# stdout, and curl's own non-zero exit status.
setup_mock_curl() {
    # Each test builds a fresh stub dir; remove the previous test's first so
    # the EXIT trap (which only sees the last MOCK_DIR) leaves nothing behind.
    cleanup
    MOCK_DIR=$(mktemp -d "/tmp/test-taiga-wiki-XXXXXX")
    export CURL_LOG="$MOCK_DIR/curl.log"
    export CURL_ARGV="$MOCK_DIR/curl.argv"
    export CURL_STDIN="$MOCK_DIR/curl.stdin"
    export JQ_ARGV="$MOCK_DIR/jq.argv"
    : > "$CURL_LOG"
    : > "$CURL_ARGV"
    : > "$CURL_STDIN"
    : > "$JQ_ARGV"

    # Shim jq: logs the exact argv it was called with (so a test can assert a
    # secret never appears there), then execs the real jq so every caller's
    # behavior is unaffected.
    local real_jq
    real_jq="$(command -v jq)"
    cat > "$MOCK_DIR/jq" << MOCKJQ
#!/usr/bin/env bash
printf '%s\n' "\$@" >> "$JQ_ARGV"
exec "$real_jq" "\$@"
MOCKJQ
    chmod +x "$MOCK_DIR/jq"

    cat > "$MOCK_DIR/curl" << 'MOCKSCRIPT'
#!/usr/bin/env bash
out=""
url=""
prev=""
method="GET"
has_body=false
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
    cat >> "$CURL_STDIN"
fi
if [[ -n "${STUB_CURL_FAIL_METHOD:-}" && "$method" == "$STUB_CURL_FAIL_METHOD" ]]; then
    echo "curl: (${STUB_CURL_EXIT}) stub transport failure" >&2
    exit "$STUB_CURL_EXIT"
fi
WIKI_PAGE='{"id": 1, "project": 1, "slug": "cross-project-execution-order", "version": 85, "content": "line one\nSENTINELBODY\n"}'
case "$method $url" in
    "POST "*/api/v1/auth)
        echo '{"auth_token": "stub-token"}' > "$out"
        printf 200 ;;
    "PATCH "*/api/v1/wiki/1)
        if [[ "$STUB_PATCH_CODE" == "400" ]]; then
            printf '{"version": "The version doesn'"'"'t match with the current one"}' > "$out"
            printf 400
        else
            echo '{"id": 1, "slug": "cross-project-execution-order", "version": 86}' > "$out"
            printf 200
        fi ;;
    "GET "*/api/v1/wiki/by_slug*)
        printf '%s' "$WIKI_PAGE" > "$out"
        printf 200 ;;
    "GET "*/api/v1/wiki/1)
        printf '%s' "$WIKI_PAGE" > "$out"
        printf 200 ;;
    *)
        echo '{}' > "$out"
        printf 500 ;;
esac
MOCKSCRIPT
    chmod +x "$MOCK_DIR/curl"
}

run_script() {
    STUB_PATCH_CODE="${STUB_PATCH_CODE:-200}" \
    TAIGA_API_URL="https://taiga.stub.test" \
    TAIGA_USERNAME="stub-user" \
    TAIGA_PASSWORD="stub-pass-do-not-leak" \
    PATH="$MOCK_DIR:$PATH" \
    bash "$SCRIPT_UNDER_TEST" "$@"
}

# ============================================================
# Test: get writes the page content to --out and reports the version on stdout
# ============================================================
test_get_writes_out_and_reports_version() {
    info "Test: get writes --out and reports version on stdout"
    setup_mock_curl

    local output rc=0
    output=$(run_script get 1 --out "$MOCK_DIR/page.md" 2>&1) && rc=0 || rc=$?

    if [ "$rc" -eq 0 ]; then
        pass "Exits 0"
    else
        fail "Expected exit 0, got $rc. Output: $output"
    fi

    if grep -q '"version": *85' <<<"$output"; then
        pass "Reports version 85 on stdout"
    else
        fail "Expected version 85 in output, got: $output"
    fi

    if grep -q 'SENTINELBODY' "$MOCK_DIR/page.md"; then
        pass "Writes page content to --out"
    else
        fail "Expected SENTINELBODY in --out file"
    fi
}

# ============================================================
# Test: get writes the content byte-exactly, adding no trailing newline
# ============================================================
test_get_writes_content_byte_exact() {
    info "Test: get writes byte-exact content, no trailing newline added"
    setup_mock_curl

    run_script get 1 --out "$MOCK_DIR/page.md" > /dev/null 2>&1

    # Stub content is "line one\nSENTINELBODY\n" — 22 bytes. A jq -r fetch
    # would append a newline and grow the page by a byte on every round-trip.
    local bytes
    bytes=$(wc -c < "$MOCK_DIR/page.md")
    if [ "$bytes" -eq 22 ]; then
        pass "--out file is exactly 22 bytes"
    else
        fail "Expected 22 bytes, got $bytes"
    fi
}

# ============================================================
# Test: get keeps the page body off stdout
# ============================================================
test_get_keeps_body_off_stdout() {
    info "Test: get keeps the page body off stdout"
    setup_mock_curl

    local output
    output=$(run_script get 1 --out "$MOCK_DIR/page.md" 2>&1)

    if grep -q 'SENTINELBODY' <<<"$output"; then
        fail "Page body leaked to stdout: $output"
    else
        pass "Page body never appears on stdout"
    fi
}

# ============================================================
# Test: get resolves a page by slug
# ============================================================
test_get_resolves_by_slug() {
    info "Test: get resolves a page by slug"
    setup_mock_curl

    run_script get --slug cross-project-execution-order --project-id 1 --out "$MOCK_DIR/page.md" > /dev/null 2>&1

    if grep -q 'by_slug?project=1&slug=cross-project-execution-order' "$CURL_LOG"; then
        pass "Hits the by_slug endpoint with project and slug"
    else
        fail "Expected by_slug request in curl log: $(cat "$CURL_LOG")"
    fi

    if grep -q 'SENTINELBODY' "$MOCK_DIR/page.md"; then
        pass "Writes resolved page content to --out"
    else
        fail "Expected SENTINELBODY in --out file"
    fi
}

# ============================================================
# Test: a positional slug resolves through by_slug in project 39 by default
# ============================================================
test_get_positional_slug_defaults_to_project_39() {
    info "Test: get <slug> uses by_slug with project 39"
    setup_mock_curl

    local output rc=0
    output=$(run_script get cross-project-execution-order --out "$MOCK_DIR/page.md" 2>&1) && rc=0 || rc=$?

    if [ "$rc" -eq 0 ] && grep -q 'by_slug?project=39&slug=cross-project-execution-order' "$CURL_LOG"; then
        pass "Positional slug hits by_slug?project=39"
    else
        fail "Expected by_slug?project=39 request (rc=$rc, output: $output): $(cat "$CURL_LOG")"
    fi
    if grep -q 'SENTINELBODY' "$MOCK_DIR/page.md"; then
        pass "Positional slug writes the page body to --out"
    else
        fail "Expected the page body in --out"
    fi
}

# ============================================================
# Test: --slug without --project-id also defaults to project 39
# ============================================================
test_get_slug_flag_defaults_to_project_39() {
    info "Test: get --slug without --project-id uses project 39"
    setup_mock_curl

    run_script get --slug home --out "$MOCK_DIR/page.md" > /dev/null 2>&1 || true
    if grep -q 'by_slug?project=39&slug=home' "$CURL_LOG"; then
        pass "--slug defaults to project 39"
    else
        fail "Expected by_slug?project=39&slug=home: $(cat "$CURL_LOG")"
    fi
}

# ============================================================
# Test: patch <slug> resolves the page id, then PATCHes it
# ============================================================
test_patch_by_slug_resolves_id() {
    info "Test: patch <slug> resolves the id via by_slug, then PATCHes wiki/<id>"
    setup_mock_curl
    printf 'new body\n' > "$MOCK_DIR/new.md"

    local output rc=0
    output=$(run_script patch cross-project-execution-order --version 85 --content-file "$MOCK_DIR/new.md" 2>&1) && rc=0 || rc=$?

    if [ "$rc" -eq 0 ] \
        && grep -q 'GET .*by_slug?project=39&slug=cross-project-execution-order' "$CURL_LOG" \
        && grep -q 'PATCH .*/api/v1/wiki/1$' "$CURL_LOG"; then
        pass "patch by slug: by_slug lookup then PATCH wiki/1"
    else
        fail "Expected by_slug GET then PATCH wiki/1 (rc=$rc, output: $output): $(cat "$CURL_LOG")"
    fi
}

# ============================================================
# Test: get rejects an --out path outside /tmp
# ============================================================
test_get_rejects_out_path_outside_tmp() {
    info "Test: get rejects --out path outside /tmp"
    setup_mock_curl

    local escape_target="$REPO_ROOT/scripts/taiga-wiki-escape-test.md"
    rm -f "$escape_target"

    local output rc=0
    output=$(run_script get 1 --out "$escape_target" 2>&1) && rc=0 || rc=$?

    if [ "$rc" -ne 0 ]; then
        pass "Exits non-zero for an --out path outside /tmp"
    else
        fail "Expected a non-zero exit, got 0. Output: $output"
    fi

    if grep -q '/tmp' <<<"$output"; then
        pass "Error message names the /tmp boundary"
    else
        fail "Expected an error naming /tmp, got: $output"
    fi

    if [ -e "$escape_target" ]; then
        fail "Escape target was written: $escape_target"
        rm -f "$escape_target"
    else
        pass "No file written outside /tmp"
    fi

    if grep -q 'POST.*api/v1/auth' "$CURL_LOG" 2>/dev/null; then
        fail "Auth request was made despite the rejected --out path"
    else
        pass "Rejected before any Taiga request was made"
    fi
}

# ============================================================
# Test: patch sends content and version, and exits 0 on a 200
# ============================================================
test_patch_sends_content_and_version() {
    info "Test: patch sends content and version, exits 0 on 200"
    setup_mock_curl
    printf 'replacement body\n' > "$MOCK_DIR/new.md"

    local rc=0
    run_script patch 1 --version 85 --content-file "$MOCK_DIR/new.md" > /dev/null 2>&1 && rc=0 || rc=$?

    if [ "$rc" -eq 0 ]; then
        pass "Exits 0"
    else
        fail "Expected exit 0, got $rc"
    fi

    if grep -q 'PATCH .*/api/v1/wiki/1' "$CURL_LOG"; then
        pass "Sends PATCH to /api/v1/wiki/1"
    else
        fail "Expected PATCH request in curl log: $(cat "$CURL_LOG")"
    fi

    if grep -q '"version": *85' "$CURL_STDIN" && grep -q 'replacement body' "$CURL_STDIN"; then
        pass "PATCH body carries content and version"
    else
        fail "Expected version 85 and content in PATCH body: $(cat "$CURL_STDIN")"
    fi
}

# ============================================================
# Test: patch exits 3 — not 1 — on a stale-version 400
# ============================================================
test_patch_exits_3_on_version_conflict() {
    info "Test: patch exits 3 on stale-version 400"
    setup_mock_curl
    export STUB_PATCH_CODE=400
    printf 'replacement body\n' > "$MOCK_DIR/new.md"

    local output rc=0
    output=$(run_script patch 1 --version 12 --content-file "$MOCK_DIR/new.md" 2>&1) && rc=0 || rc=$?

    if [ "$rc" -eq 3 ]; then
        pass "Exits 3 (version conflict), not 1"
    else
        fail "Expected exit 3, got $rc. Output: $output"
    fi

    if grep -q "version conflict" <<<"$output"; then
        pass "Reports 'version conflict' in stderr"
    else
        fail "Expected 'version conflict' in output, got: $output"
    fi

    unset STUB_PATCH_CODE
}

# ============================================================
# Test: patch --dry-run makes no Taiga request and redacts the content to a length
# ============================================================
test_patch_dry_run_sends_no_patch() {
    info "Test: patch --dry-run makes no Taiga request, redacts content"
    setup_mock_curl
    printf 'replacement body\n' > "$MOCK_DIR/new.md"

    local output rc=0
    output=$(run_script patch 1 --version 85 --content-file "$MOCK_DIR/new.md" --dry-run 2>&1) && rc=0 || rc=$?

    if [ "$rc" -eq 0 ]; then
        pass "Exits 0"
    else
        fail "Expected exit 0, got $rc. Output: $output"
    fi

    if [ ! -s "$CURL_LOG" ]; then
        pass "Makes no Taiga request at all (no auth, no PATCH)"
    else
        fail "Expected no curl requests, got: $(cat "$CURL_LOG")"
    fi

    if grep -q 'replacement body' <<<"$output"; then
        fail "Content leaked unredacted: $output"
    else
        pass "Content is redacted"
    fi

    if grep -q 'chars)' <<<"$output"; then
        pass "Redacted content shows a char count"
    else
        fail "Expected 'chars)' in redacted output, got: $output"
    fi
}

# Run the script with every TAIGA_* credential unset and the stub curl on PATH.
run_script_without_env() {
    env -u TAIGA_API_URL -u TAIGA_USERNAME -u TAIGA_PASSWORD \
        PATH="$MOCK_DIR:$PATH" \
        bash "$SCRIPT_UNDER_TEST" "$@"
}

# ============================================================
# Test: patch --dry-run needs no credentials and contacts no Taiga
# ============================================================
test_patch_dry_run_needs_no_credentials() {
    info "Test: patch --dry-run works with TAIGA_* unset and makes no request"
    setup_mock_curl
    printf 'replacement body\n' > "$MOCK_DIR/new.md"

    local output rc=0
    output=$(run_script_without_env patch 1 --version 85 --content-file "$MOCK_DIR/new.md" --dry-run 2>&1) && rc=0 || rc=$?

    if [ "$rc" -eq 0 ]; then
        pass "Exits 0 without TAIGA_* credentials"
    else
        fail "Expected exit 0, got $rc. Output: $output"
    fi

    if grep -q '"version": *85' <<<"$output" && grep -q 'chars)' <<<"$output"; then
        pass "Prints the redacted PATCH body"
    else
        fail "Expected version 85 and a redacted char count, got: $output"
    fi

    if [ ! -s "$CURL_LOG" ]; then
        pass "Makes no Taiga request"
    else
        fail "Expected no curl requests, got: $(cat "$CURL_LOG")"
    fi
}

# Assert a help invocation prints usage on stdout and exits 0 without needing
# credentials or contacting Taiga.
#
# Args:
#   $1  test label
#   $@  script arguments
assert_help_exits_0() {
    local label="$1"
    shift
    info "Test: $label"
    setup_mock_curl

    local stdout rc=0
    stdout=$(run_script_without_env "$@" 2>"$MOCK_DIR/stderr") && rc=0 || rc=$?

    if [ "$rc" -eq 0 ]; then
        pass "Exits 0"
    else
        fail "Expected exit 0, got $rc. stdout: $stdout stderr: $(cat "$MOCK_DIR/stderr")"
    fi

    if grep -q '^Usage: taiga-wiki.sh' <<<"$stdout"; then
        pass "Prints usage on stdout"
    else
        fail "Expected usage on stdout, got: $stdout"
    fi

    if grep -q 'ERROR' "$MOCK_DIR/stderr"; then
        fail "Help reported an error: $(cat "$MOCK_DIR/stderr")"
    else
        pass "Reports no error"
    fi

    if [ ! -s "$CURL_LOG" ]; then
        pass "Makes no Taiga request"
    else
        fail "Expected no curl requests, got: $(cat "$CURL_LOG")"
    fi
}

# ============================================================
# Test: --help / -h print usage and exit 0, at top level and after a command
# ============================================================
test_top_level_long_help_exits_0() {
    assert_help_exits_0 "top-level --help prints usage, exits 0" --help
}

test_top_level_short_help_exits_0() {
    assert_help_exits_0 "top-level -h prints usage, exits 0" -h
}

test_command_help_exits_0() {
    assert_help_exits_0 "get --help prints usage, exits 0" get --help
}

# ============================================================
# Test: --help documents every metadata field get prints
# ============================================================
test_help_documents_get_output_fields() {
    info "Test: --help lists every field get prints"
    setup_mock_curl

    local help get_output keys key
    help=$(run_script_without_env --help 2>/dev/null)
    get_output=$(run_script get 1 --out "$MOCK_DIR/page.md" 2>/dev/null)
    keys=$(echo "$get_output" | jq -r 'keys_unsorted[]')

    if [ -z "$keys" ]; then
        fail "Expected JSON metadata from get, got: $get_output"
        return
    fi

    for key in $keys; do
        if grep -q "\"$key\"" <<<"$help"; then
            pass "Help lists \"$key\""
        else
            fail "Help omits \"$key\", which get prints"
        fi
    done
}

# Assert a misuse prints usage on stderr (plus the expected ERROR: line, if
# any) and exits 1.
#
# Args:
#   $1  test label
#   $2  expected stderr text (grep pattern), or empty when only usage is printed
#   $@  script arguments
assert_misuse_exits_1() {
    local label="$1" expected="$2"
    shift 2
    info "Test: $label"
    setup_mock_curl

    local stdout rc=0
    stdout=$(run_script_without_env "$@" 2>"$MOCK_DIR/stderr") && rc=0 || rc=$?

    if [ "$rc" -eq 1 ]; then
        pass "Exits 1"
    else
        fail "Expected exit 1, got $rc. stderr: $(cat "$MOCK_DIR/stderr")"
    fi

    if grep -q '^Usage: taiga-wiki.sh' "$MOCK_DIR/stderr" && [ -z "$stdout" ]; then
        pass "Prints usage on stderr, nothing on stdout"
    else
        fail "Expected usage on stderr only. stdout: $stdout stderr: $(cat "$MOCK_DIR/stderr")"
    fi

    if [ -z "$expected" ] || grep -q "$expected" "$MOCK_DIR/stderr"; then
        pass "Reports the misuse"
    else
        fail "Expected '$expected' on stderr, got: $(cat "$MOCK_DIR/stderr")"
    fi
}

# ============================================================
# Test: misuse still prints usage to stderr and exits 1
# ============================================================
test_no_args_exits_1() {
    assert_misuse_exits_1 "no arguments prints usage, exits 1" ""
}

test_unknown_command_exits_1() {
    assert_misuse_exits_1 "unknown command prints usage, exits 1" \
        "ERROR: unknown command 'frobnicate'" frobnicate
}

test_unknown_flag_exits_1() {
    assert_misuse_exits_1 "unknown flag prints usage, exits 1" \
        "ERROR: unknown flag --bogus" get 1 --bogus
}

# ============================================================
# Test: no credential or body ever appears in curl's argv
# ============================================================
test_no_credentials_in_argv() {
    info "Test: no credential or body ever appears in curl's argv"
    setup_mock_curl
    printf 'replacement body\n' > "$MOCK_DIR/new.md"

    run_script get 1 --out "$MOCK_DIR/page.md" > /dev/null 2>&1
    run_script patch 1 --version 85 --content-file "$MOCK_DIR/new.md" > /dev/null 2>&1

    if grep -q 'stub-pass-do-not-leak' "$CURL_ARGV"; then
        fail "Password leaked into curl argv"
    elif grep -q 'stub-token' "$CURL_ARGV"; then
        fail "Auth token leaked into curl argv"
    elif grep -q 'replacement body' "$CURL_ARGV"; then
        fail "Content leaked into curl argv"
    else
        pass "No credential or body appears in curl argv"
    fi

    if grep -q 'stub-pass-do-not-leak' "$JQ_ARGV"; then
        fail "Password leaked into jq argv"
    else
        pass "No credential appears in jq argv"
    fi
}

# ============================================================
# Test: missing TAIGA_PASSWORD fails with a clear error
# ============================================================
test_missing_password_fails_clearly() {
    info "Test: missing TAIGA_PASSWORD exits non-zero with a clear message"
    setup_mock_curl

    local output rc=0
    output=$(env -u TAIGA_PASSWORD \
        TAIGA_API_URL="https://taiga.stub.test" \
        TAIGA_USERNAME="stub-user" \
        PATH="$MOCK_DIR:$PATH" \
        bash "$SCRIPT_UNDER_TEST" get 1 --out "$MOCK_DIR/page.md" 2>&1) && rc=0 || rc=$?

    if [ "$rc" -ne 0 ]; then
        pass "Exits non-zero when TAIGA_PASSWORD is unset"
    else
        fail "Expected a non-zero exit, got 0. Output: $output"
    fi

    if grep -q "TAIGA_PASSWORD is required" <<<"$output"; then
        pass "Reports 'TAIGA_PASSWORD is required'"
    else
        fail "Expected 'TAIGA_PASSWORD is required' in output, got: $output"
    fi
}

# Run the script with curl failing on one HTTP method and assert it exits 1
# (never curl's own code, and never the version-conflict 3) with an ERROR: line.
# The literal argument CONTENT_FILE is replaced with a content file created
# inside this test's fresh stub dir.
#
# Args:
#   $1  test label
#   $2  HTTP method whose curl call fails (POST/GET/PATCH)
#   $3  curl exit status to simulate
#   $@  script arguments
assert_transport_failure_exits_1() {
    local label="$1" method="$2" curl_exit="$3"
    shift 3
    info "Test: $label"
    setup_mock_curl
    printf 'replacement body\n' > "$MOCK_DIR/new.md"

    local args=() arg
    for arg in "$@"; do
        if [ "$arg" = "CONTENT_FILE" ]; then args+=("$MOCK_DIR/new.md"); else args+=("$arg"); fi
    done

    local output rc=0
    output=$(STUB_CURL_FAIL_METHOD="$method" STUB_CURL_EXIT="$curl_exit" \
        run_script "${args[@]}" 2>&1) && rc=0 || rc=$?

    if [ "$rc" -eq 1 ]; then
        pass "Exits 1 (not curl's exit $curl_exit)"
    else
        fail "Expected exit 1, got $rc. Output: $output"
    fi

    if grep -q "^ERROR: .*curl exit $curl_exit" <<<"$output"; then
        pass "Reports an ERROR: line naming curl exit $curl_exit"
    else
        fail "Expected an 'ERROR: ... curl exit $curl_exit' line, got: $output"
    fi

    if grep -q "version conflict" <<<"$output"; then
        fail "Transport failure reported as a version conflict: $output"
    else
        pass "Not reported as a version conflict"
    fi
}

# ============================================================
# Test: curl transport failures exit 1, never curl's own code
# ============================================================
test_login_transport_failure_exits_1() {
    assert_transport_failure_exits_1 \
        "auth request curl exit 3 (malformed URL) exits 1" POST 3 \
        get 1 --out "/tmp/test-taiga-wiki-unused.md"
}

test_get_transport_failure_exits_1() {
    assert_transport_failure_exits_1 \
        "wiki GET curl exit 3 exits 1" GET 3 \
        get 1 --out "/tmp/test-taiga-wiki-unused.md"
}

test_get_by_slug_transport_failure_exits_1() {
    assert_transport_failure_exits_1 \
        "wiki by_slug GET curl exit 7 (connection refused) exits 1" GET 7 \
        get --slug cross-project-execution-order --project-id 1 --out "/tmp/test-taiga-wiki-unused.md"
}

test_patch_transport_failure_exits_1_not_3() {
    assert_transport_failure_exits_1 \
        "wiki PATCH curl exit 3 exits 1, not the version-conflict 3" PATCH 3 \
        patch 1 --version 85 --content-file CONTENT_FILE
}

test_patch_login_transport_failure_exits_1() {
    assert_transport_failure_exits_1 \
        "patch auth request curl exit 7 (connection refused) exits 1" POST 7 \
        patch 1 --version 85 --content-file CONTENT_FILE
}

# ============================================================
# Test: assertion hygiene (bug #3324, pattern from bug #2626)
# ============================================================
# Piping a captured value into `grep -q` races SIGPIPE against pipefail: grep
# exits on its first match, echo can hit a broken pipe, and the assertion fails
# even though the text was found. Assertions in this file must use here-strings.
test_no_echo_piped_into_grep() {
    info "Test: no assertion pipes a captured value into grep"

    if grep -qE 'echo .*\| *grep' "$SCRIPT_DIR/test-taiga-wiki.sh"; then
        fail "Found echo piped into grep (SIGPIPE/pipefail race): $(grep -nE 'echo .*\| *grep' "$SCRIPT_DIR/test-taiga-wiki.sh")"
    else
        pass "No echo piped into grep"
    fi
}

echo ""
echo "=== taiga-wiki.sh mock-curl tests ==="
echo ""

test_get_writes_out_and_reports_version
test_get_writes_content_byte_exact
test_get_keeps_body_off_stdout
test_get_resolves_by_slug
test_get_positional_slug_defaults_to_project_39
test_get_slug_flag_defaults_to_project_39
test_patch_by_slug_resolves_id
test_get_rejects_out_path_outside_tmp
test_patch_sends_content_and_version
test_patch_exits_3_on_version_conflict
test_patch_dry_run_sends_no_patch
test_patch_dry_run_needs_no_credentials
test_top_level_long_help_exits_0
test_top_level_short_help_exits_0
test_command_help_exits_0
test_help_documents_get_output_fields
test_no_args_exits_1
test_unknown_command_exits_1
test_unknown_flag_exits_1
test_no_credentials_in_argv
test_missing_password_fails_clearly
test_login_transport_failure_exits_1
test_get_transport_failure_exits_1
test_get_by_slug_transport_failure_exits_1
test_patch_transport_failure_exits_1_not_3
test_patch_login_transport_failure_exits_1
test_no_echo_piped_into_grep

echo ""
if [ "$FAILURES" -gt 0 ]; then
    echo -e "${RED}${FAILURES} test(s) failed${NC}"
    exit 1
else
    echo -e "${GREEN}All tests passed${NC}"
    exit 0
fi
