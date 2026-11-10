#!/usr/bin/env bash
# ABOUTME: Unit tests for the reject-self-matching-pgrep.sh PreToolUse hook.
# ABOUTME: Verifies pkill/pgrep -f commands are blocked and safe commands pass through.
# shellcheck disable=SC2016  # single-quoted payloads are literal command text, not expansions
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
HOOK="$REPO_ROOT/scripts/hooks/reject-self-matching-pgrep.sh"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}✓ $1${NC}"; }
fail() { echo -e "${RED}✗ $1${NC}"; FAILURES=$((FAILURES + 1)); }
info() { echo -e "${YELLOW}→ $1${NC}"; }

FAILURES=0
HOOK_EXIT=0
HOOK_STDERR=""

# Runs the hook with a Bash tool payload for the given command.
# Sets HOOK_EXIT and HOOK_STDERR.
run_hook() {
    local payload
    payload=$(jq -cn --arg cmd "$1" '{tool_name: "Bash", tool_input: {command: $cmd}}')
    run_hook_raw "$payload"
}

run_hook_raw() {
    set +e
    HOOK_STDERR=$(echo "$1" | bash "$HOOK" 2>&1 >/dev/null)
    HOOK_EXIT=$?
    set -e
}

expect_blocked() {
    run_hook "$1"
    # Here-strings, not echo-to-grep pipes: under pipefail, grep -q's early exit can SIGPIPE the writer.
    if [ "$HOOK_EXIT" -eq 2 ] && grep -q "pgrep -x" <<< "$HOOK_STDERR" && ! grep -q "dotnet" <<< "$HOOK_STDERR"; then
        pass "blocked: $2"
    else
        fail "expected exit 2 with the pgrep -x hint and no dotnet for '$2', got exit $HOOK_EXIT: $HOOK_STDERR"
    fi
}

expect_allowed() {
    run_hook "$1"
    if [ "$HOOK_EXIT" -eq 0 ]; then
        pass "allowed: $2"
    else
        fail "expected exit 0 for '$2', got exit $HOOK_EXIT: $HOOK_STDERR"
    fi
}

info "Test 1: pkill -f and pkill -9 -f are blocked"
expect_blocked 'pkill -f x' 'pkill -f x'
expect_blocked 'pkill -9 -f x' 'pkill -9 -f x'
expect_blocked 'true && pkill -f x' 'true && pkill -f x'

info "Test 2: pgrep -f wait loop is blocked"
expect_blocked 'while kill -0 $(pgrep -f "python bot" | head -1); do sleep 5; done' 'kill -0 $(pgrep -f ...) loop'

info "Test 3: other flag orderings are blocked"
expect_blocked 'pkill -f -9 x' 'pkill -f -9'
expect_blocked 'pgrep -fl x' 'pgrep -fl'

info "Test 4: pgrep -f filtered by its own pid with grep -v is allowed"
expect_allowed 'pgrep -f foo | grep -v "^$$\$"' 'pgrep -f foo | grep -v "^$$\$"'
expect_allowed "pgrep -f foo | grep -v '^\$\$\$'" "pgrep -f foo | grep -v '^\$\$\$'"

info "Test 4b: grep -v does not exempt pkill or unrelated pgrep filters"
expect_blocked 'pkill -f x | grep -v foo' 'pkill -f x | grep -v foo'
expect_blocked 'pgrep -f x | grep -v foo' 'pgrep -f x | grep -v foo'

info "Test 4c: an exempt pgrep elsewhere in the command does not shield a self-matching kill"
expect_blocked 'pgrep -f foo | grep -v "^$$\$"; pkill -f x' 'exempt pgrep then pkill -f'
expect_blocked 'pgrep -f foo | grep -v "^$$\$" && pkill -9 -f y' 'exempt pgrep && pkill -9 -f'

info "Test 5: pgrep -x exact-name match and ordinary commands are allowed"
expect_allowed 'pgrep -x python' 'pgrep -x python'
expect_allowed 'ls' 'ls'

info "Test 6: non-Bash tool payload, missing command and empty object are allowed"
run_hook_raw '{"tool_name":"Read","tool_input":{"file_path":"/tmp/x"}}'
if [ "$HOOK_EXIT" -eq 0 ]; then pass "allowed: non-Bash tool payload"; else fail "non-Bash payload got exit $HOOK_EXIT: $HOOK_STDERR"; fi
run_hook_raw '{"tool_name":"Bash","tool_input":{}}'
if [ "$HOOK_EXIT" -eq 0 ]; then pass "allowed: missing command"; else fail "missing command got exit $HOOK_EXIT: $HOOK_STDERR"; fi
run_hook_raw '{}'
if [ "$HOOK_EXIT" -eq 0 ]; then pass "allowed: empty object"; else fail "empty object got exit $HOOK_EXIT: $HOOK_STDERR"; fi

echo ""
if [ "$FAILURES" -eq 0 ]; then
    echo -e "${GREEN}All tests passed${NC}"
    exit 0
else
    echo -e "${RED}$FAILURES test(s) failed${NC}"
    exit 1
fi
