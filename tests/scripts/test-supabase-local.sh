#!/usr/bin/env bash
# ABOUTME: Two-worktree isolation test for scripts/supabase-local.sh: two stacks run side by side,
# ABOUTME: each sees only its own data, and stopping one leaves the other (and the main stack) alone.
#
# Starts real stacks (Docker and npx, several minutes), so it is opt-in:
#   SUPABASE_LOCAL_LIVE=1 bash tests/scripts/test-supabase-local.sh
# Otherwise, or when Docker, npx, supabase/config.toml or tests/integration is missing, it exits 0.
# Never touches the hosted project and never starts, stops or resets the main checkout's stack.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SCRIPT_UNDER_TEST="scripts/supabase-local.sh"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}✓ $1${NC}"; }
fail() { echo -e "${RED}✗ $1${NC}"; FAILURES=$((FAILURES + 1)); }
info() { echo -e "${YELLOW}→ $1${NC}"; }

FAILURES=0
TMP=""
WT_A=""
WT_B=""
MARKER="Isolation Marker (Test)"

if [ "${SUPABASE_LOCAL_LIVE:-}" != "1" ]; then
    echo "SKIP: live two-stack test is opt-in (SUPABASE_LOCAL_LIVE=1)"
    exit 0
fi
# Inherited values would redirect the script: CHOLOBOTS_DEVCONTAINER=1 maps both scratch worktrees
# to the main "cholobots" stack (and the cleanup would stop it), CHOLOBOTS_CHECKOUT points every
# call at one checkout, SUPABASE_LOCAL_EXCLUDE changes which services start.
unset CHOLOBOTS_DEVCONTAINER CHOLOBOTS_CHECKOUT SUPABASE_LOCAL_EXCLUDE
if ! docker info >/dev/null 2>&1; then
    echo "SKIP: docker unavailable"
    exit 0
fi
if ! command -v npx >/dev/null 2>&1; then
    echo "SKIP: npx unavailable"
    exit 0
fi
if [ ! -f "$REPO_ROOT/supabase/config.toml" ] || [ ! -d "$REPO_ROOT/tests/integration" ]; then
    echo "SKIP: repo has no supabase/config.toml or tests/integration"
    exit 0
fi

# Python with requirements.txt and requirements-dev.txt installed: SUPABASE_LOCAL_TEST_PYTHON,
# else the main checkout's venv, else python3.
main_checkout="$(dirname "$(git -C "$REPO_ROOT" rev-parse --path-format=absolute --git-common-dir)")"
if [ -n "${SUPABASE_LOCAL_TEST_PYTHON:-}" ]; then
    PY="$SUPABASE_LOCAL_TEST_PYTHON"
elif [ -x "$main_checkout/.venv/bin/python" ]; then
    PY="$main_checkout/.venv/bin/python"
else
    PY="python3"
fi

# Container and volume names of the main checkout's stack (project id "cholobots").
main_footprint() {
    { docker ps -a --format 'c {{.Names}}' | grep -E '^c supabase_.*_cholobots$'
      docker volume ls --format 'v {{.Name}}' | grep -E '^v supabase_.*_cholobots$'; } | sort
}

cleanup() {
    for wt in "$WT_A" "$WT_B"; do
        [ -n "$wt" ] && [ -d "$wt" ] || continue
        (cd "$wt" && "$SCRIPT_UNDER_TEST" stop >/dev/null 2>&1) || true
        git -C "$REPO_ROOT" worktree remove --force "$wt" >/dev/null 2>&1 || true
    done
    [ -n "$TMP" ] && rm -rf "$TMP"
}
trap cleanup EXIT

TMP="$(mktemp -d "${TMPDIR:-/tmp}/test-supabase-local-XXXXXX")"
WT_A="$TMP/sl-a"
WT_B="$TMP/sl-b"
git -C "$REPO_ROOT" worktree add --detach "$WT_A" HEAD >/dev/null 2>&1 \
    && git -C "$REPO_ROOT" worktree add --detach "$WT_B" HEAD >/dev/null 2>&1 \
    || { fail "could not create the scratch worktrees"; exit 1; }

BEFORE="$(main_footprint)"

# rest <env file> <curl args...> against that stack's PostgREST with its service key.
rest() {
    local envfile="$1"; shift
    local url key
    url="$(sed -n 's/^SUPABASE_URL=//p' "$envfile")"
    key="$(sed -n 's/^SUPABASE_SERVICE_ROLE_KEY=//p' "$envfile")"
    curl -sS -H "apikey: $key" -H "Authorization: Bearer $key" "$@" "$url/rest/v1/${REST_PATH}"
}

info "Starting both stacks concurrently"
(cd "$WT_A" && "$SCRIPT_UNDER_TEST" start >"$TMP/a.env" 2>"$TMP/a.err") &
pid_a=$!
(cd "$WT_B" && "$SCRIPT_UNDER_TEST" start >"$TMP/b.env" 2>"$TMP/b.err") &
pid_b=$!
wait "$pid_a"; rc_a=$?
wait "$pid_b"; rc_b=$?
if [ "$rc_a" -eq 0 ] && [ "$rc_b" -eq 0 ]; then
    pass "both stacks started"
else
    fail "start failed (a=$rc_a b=$rc_b)"
    tail -5 "$TMP/a.err" "$TMP/b.err" >&2
    exit 1
fi

url_a="$(sed -n 's/^SUPABASE_URL=//p' "$TMP/a.env")"
url_b="$(sed -n 's/^SUPABASE_URL=//p' "$TMP/b.env")"
case "$url_a$url_b" in
    http://127.0.0.1:*http://127.0.0.1:*) pass "both stacks listen on 127.0.0.1" ;;
    *) fail "unexpected hosts: $url_a $url_b" ;;
esac
if [ "$url_a" != "$url_b" ]; then pass "API ports differ ($url_a, $url_b)"; else fail "both stacks share $url_a"; fi

names="$(docker ps --format '{{.Names}}')"
if grep -qE 'supabase_db_cholobots-sl-a-[0-9a-f]{8}$' <<<"$names" && grep -qE 'supabase_db_cholobots-sl-b-[0-9a-f]{8}$' <<<"$names"; then
    pass "each stack runs under its own project id"
else
    fail "expected supabase_db_cholobots-sl-a and -sl-b containers, got: $names"
fi

info "Data isolation"
REST_PATH='chisme_contacts?select=id&name=like.*(Test)'
seed_a="$(rest "$TMP/a.env" -G 2>/dev/null | jq length 2>/dev/null)"
REST_PATH='chisme_contacts?select=id&name=like.*(Test)'
seed_b="$(rest "$TMP/b.env" -G 2>/dev/null | jq length 2>/dev/null)"
if [ -n "$seed_a" ] && [ "$seed_a" = "$seed_b" ]; then
    pass "both stacks hold the same seeded contacts ($seed_a)"
else
    fail "seeded contact counts differ or are unreadable (a=$seed_a b=$seed_b)"
fi

REST_PATH='chisme_contacts'
code="$(rest "$TMP/a.env" -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
    -H 'Prefer: return=minimal' -d "{\"name\":\"$MARKER\",\"source\":\"isolation-test\"}")"
if [ "$code" = "201" ]; then pass "marker contact inserted into stack A"; else fail "marker insert returned $code"; fi

REST_PATH='chisme_contacts'
count_a="$(rest "$TMP/a.env" -G --data-urlencode "name=eq.$MARKER" --data-urlencode 'select=id' | jq length)"
count_b="$(rest "$TMP/b.env" -G --data-urlencode "name=eq.$MARKER" --data-urlencode 'select=id' | jq length)"
if [ "$count_a" = "1" ]; then pass "stack A sees its marker"; else fail "stack A marker count is $count_a"; fi
if [ "$count_b" = "0" ]; then pass "stack B does not see A's marker"; else fail "stack B marker count is $count_b"; fi

info "Integration suite in both worktrees at the same time"
run_suite() {
    local wt="$1" envfile="$2" log="$3"
    (
        cd "$wt" || exit 1
        set -a
        # shellcheck disable=SC1090
        . "$envfile"
        set +a
        CHOLOBOTS_INTEGRATION_REQUIRED=1 "$PY" -m pytest -q -m integration tests/integration
    ) >"$log" 2>&1
}
run_suite "$WT_A" "$TMP/a.env" "$TMP/a.pytest" &
pid_a=$!
run_suite "$WT_B" "$TMP/b.env" "$TMP/b.pytest" &
pid_b=$!
wait "$pid_a"; rc_a=$?
wait "$pid_b"; rc_b=$?
if [ "$rc_a" -eq 0 ]; then pass "integration suite passed in worktree A"; else fail "suite failed in A (exit $rc_a)"; tail -15 "$TMP/a.pytest" >&2; fi
if [ "$rc_b" -eq 0 ]; then pass "integration suite passed in worktree B"; else fail "suite failed in B (exit $rc_b)"; tail -15 "$TMP/b.pytest" >&2; fi

info "Stopping A leaves B running"
(cd "$WT_A" && "$SCRIPT_UNDER_TEST" stop >/dev/null 2>"$TMP/stop-a.err")
if [ $? -eq 0 ]; then pass "stop A exited 0"; else fail "stop A failed: $(cat "$TMP/stop-a.err")"; fi
leftover="$({ docker ps -a --format '{{.Names}}'; docker volume ls --format '{{.Name}}'; } | grep 'cholobots-sl-a')"
if [ -z "$leftover" ]; then pass "no container or volume of stack A remains"; else fail "stack A left behind: $leftover"; fi
REST_PATH=''
code="$(rest "$TMP/b.env" -o /dev/null -w '%{http_code}')"
if [ "$code" = "200" ]; then pass "stack B still answers (200)"; else fail "stack B answered $code after A stopped"; fi

(cd "$WT_B" && "$SCRIPT_UNDER_TEST" stop >/dev/null 2>"$TMP/stop-b.err")
if [ $? -eq 0 ]; then pass "stop B exited 0"; else fail "stop B failed: $(cat "$TMP/stop-b.err")"; fi

AFTER="$(main_footprint)"
if [ "$BEFORE" = "$AFTER" ]; then pass "main checkout's containers and volumes unchanged"; else fail "main footprint changed"; fi

if [ "$FAILURES" -eq 0 ]; then
    echo -e "${GREEN}All supabase-local isolation checks passed${NC}"
    exit 0
fi
echo -e "${RED}$FAILURES check(s) failed${NC}"
exit 1
