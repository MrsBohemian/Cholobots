#!/usr/bin/env bash
# ABOUTME: Tests .devcontainer/post-create.sh and post-start.sh against fake docker, supabase, gh, sudo and python3.
# ABOUTME: Guards call order, idempotency, the Docker wait failure, and that no hosted Supabase command or the bot ever runs.
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}✓ $1${NC}"; }
fail() { echo -e "${RED}✗ $1${NC}"; FAILURES=$((FAILURES + 1)); }
info() { echo -e "${YELLOW}→ $1${NC}"; }

FAILURES=0

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
POST_CREATE="$REPO_ROOT/.devcontainer/post-create.sh"
POST_START="$REPO_ROOT/.devcontainer/post-start.sh"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# The scripts read these from the environment, and README/Makefile tell developers to export them
# (inside the devcontainer DISCORD_TOKEN and OPENAI_API_KEY are real secrets). An inherited
# SUPABASE=/path/to/supabase would bypass the fake and reset a real local stack, so start clean.
unset SUPABASE SUPABASE_START_EXCLUDE SUPABASE_RETRY_ATTEMPTS SUPABASE_RETRY_DELAY \
    DOCKER_WAIT_LIMIT DOCKER_INIT DISCORD_TOKEN OPENAI_API_KEY \
    FAKE_DOCKER_DOWN FAKE_DOCKER_DOWN_ONCE FAKE_STACK_STARTING FAKE_DB_RESET_FAILS FAKE_LEGACY_IPTABLES_BROKEN

info "Testing $POST_CREATE and $POST_START"

for script in "$POST_CREATE" "$POST_START"; do
    name="$(basename "$script")"
    if [ ! -x "$script" ]; then
        fail "$name exists and is executable"
        exit 1
    fi
    pass "$name exists and is executable"
    if bash -n "$script"; then pass "$name passes bash -n"; else fail "$name passes bash -n"; fi
    if command -v shellcheck >/dev/null 2>&1; then
        if shellcheck "$script"; then pass "$name passes shellcheck"; else fail "$name passes shellcheck"; fi
    else
        info "shellcheck not installed; skipped for $name"
    fi
done

# Static: nothing hosted, no credentials, no bot. Comments are dropped so prose
# about what the scripts must never do cannot trip the check.
for script in "$POST_CREATE" "$POST_START"; do
    name="$(basename "$script")"
    body=$(sed -E 's/[[:space:]]+#.*$//; /^[[:space:]]*#/d' "$script")
    for forbidden in '--linked' '--db-url' 'supabase link' 'db push' 'login' 'SUPABASE_ACCESS_TOKEN' 'cholobots.py'; do
        if grep -qF -- "$forbidden" <<< "$body"; then
            fail "$name never uses '$forbidden'"
        else
            pass "$name never uses '$forbidden'"
        fi
    done
done

# --- Fakes -------------------------------------------------------------------
BIN="$TMP/bin"
mkdir -p "$BIN"
CALLS="$TMP/calls.log"
STATE="$TMP/state"
mkdir -p "$STATE"
export CALLS STATE

log_call='printf "%s %s\n" "$(basename "$0")" "$*" >> "$CALLS"'

cat > "$BIN/docker" <<FAKE
#!/usr/bin/env bash
$log_call
if [ "\${1:-}" = "info" ] && [ -n "\${FAKE_DOCKER_DOWN:-}" ]; then exit 1; fi
if [ "\${1:-}" = "info" ] && [ -n "\${FAKE_DOCKER_DOWN_ONCE:-}" ] && [ ! -f "\$STATE/dockerd" ]; then exit 1; fi
exit 0
FAKE

cat > "$BIN/supabase" <<FAKE
#!/usr/bin/env bash
$log_call
case "\$*" in
    "status"|start*)
        # FAKE_STACK_STARTING=N: the first N status/start calls fail like a stack whose db container is restarting.
        if [ -n "\${FAKE_STACK_STARTING:-}" ]; then
            n=\$(cat "\$STATE/starting-count" 2>/dev/null || echo 0)
            if [ "\$n" -lt "\$FAKE_STACK_STARTING" ]; then
                echo \$((n + 1)) > "\$STATE/starting-count"
                echo "supabase_db_cholobots container is not ready: starting" >&2
                exit 1
            fi
        fi
        case "\$*" in
            "status") [ -f "\$STATE/started" ] || exit 1 ;;
            start*) touch "\$STATE/started" ;;
        esac
        ;;
    "status -o env")
        echo 'API_URL="http://127.0.0.1:54321"'
        echo 'SERVICE_ROLE_KEY="local-key"'
        ;;
    "db reset")
        # FAKE_DB_RESET_FAILS=N: the first N resets fail the way a still-starting db container does.
        n=\$(cat "\$STATE/reset-count" 2>/dev/null || echo 0)
        echo \$((n + 1)) > "\$STATE/reset-count"
        if [ "\$n" -lt "\${FAKE_DB_RESET_FAILS:-0}" ]; then
            echo "supabase_db_cholobots container is not ready: starting" >&2
            exit 1
        fi
        ;;
esac
exit 0
FAKE

cat > "$BIN/gh" <<FAKE
#!/usr/bin/env bash
$log_call
exit 0
FAKE

cat > "$BIN/sudo" <<FAKE
#!/usr/bin/env bash
$log_call
if [ "\${1:-}" = "iptables" ] && [ -n "\${FAKE_LEGACY_IPTABLES_BROKEN:-}" ] && [ ! -f "\$STATE/nft" ]; then exit 3; fi
if [ "\${1:-}" = "update-alternatives" ]; then touch "\$STATE/nft"; fi
if [ "\${1:-}" = "/fake/docker-init.sh" ]; then touch "\$STATE/dockerd"; fi
exit 0
FAKE

# sleep is logged, never real: the Docker wait and the retry loops finish instantly.
cat > "$BIN/sleep" <<FAKE
#!/usr/bin/env bash
$log_call
exit 0
FAKE

chmod +x "$BIN"/*

# Fake python3 for post-create only: "-m venv .venv" creates a logging .venv/bin/python.
CREATE_BIN="$TMP/create-bin"
mkdir -p "$CREATE_BIN"
cp "$BIN"/* "$CREATE_BIN"/
cat > "$CREATE_BIN/python3" <<FAKE
#!/usr/bin/env bash
$log_call
if [ "\${1:-}" = "-m" ] && [ "\${2:-}" = "venv" ]; then
    mkdir -p "\$3/bin"
    cp "\$(dirname "\$0")/venv-python" "\$3/bin/python"
fi
exit 0
FAKE
cat > "$CREATE_BIN/venv-python" <<FAKE
#!/usr/bin/env bash
printf "venv-python %s\n" "\$*" >> "\$CALLS"
exit 0
FAKE
chmod +x "$CREATE_BIN"/*

make_workspace() {
    local ws="$1"
    mkdir -p "$ws/.devcontainer" "$ws/scripts/devcontainer"
    cp "$POST_CREATE" "$POST_START" "$ws/.devcontainer/"
    cp "$REPO_ROOT/scripts/devcontainer/write_env.py" "$ws/scripts/devcontainer/"
    cp "$REPO_ROOT/scripts/check_local_env.py" "$ws/scripts/"
}

calls_has() { grep -qxF -- "$1" "$CALLS"; }
line_of() { grep -nxF -- "$1" "$CALLS" | head -1 | cut -d: -f1; }

# --- post-start.sh -----------------------------------------------------------
WS="$TMP/ws-start"
make_workspace "$WS"
: > "$CALLS"
rm -f "$STATE/started"

if (cd "$WS" && PATH="$BIN:$PATH" DOCKER_WAIT_LIMIT=5 DISCORD_TOKEN= OPENAI_API_KEY= \
        .devcontainer/post-start.sh > "$TMP/start1.out" 2> "$TMP/start1.err"); then
    pass "post-start.sh first run exits 0"
else
    fail "post-start.sh first run exits 0"
    cat "$TMP/start1.err"
fi

START_LINE=$(grep -m1 '^supabase start' "$CALLS" || true)
if [ -n "$START_LINE" ]; then pass "first run calls supabase start"; else fail "first run calls supabase start"; fi
if grep -qE '^supabase start -x .*imgproxy' <<< "$START_LINE"; then
    pass "supabase start excludes the unused services"
else
    fail "supabase start excludes the unused services (got: $START_LINE)"
fi

d=$(line_of "docker info"); s=$(grep -nm1 '^supabase start' "$CALLS" | cut -d: -f1)
r=$(line_of "supabase db reset"); e=$(line_of "supabase status -o env")
if [ -n "$d" ] && [ -n "$s" ] && [ -n "$r" ] && [ -n "$e" ] && [ "$d" -lt "$s" ] && [ "$s" -lt "$r" ] && [ "$r" -lt "$e" ]; then
    pass "call order is docker info, supabase start, db reset, status -o env"
else
    fail "call order is docker info, supabase start, db reset, status -o env"
    cat "$CALLS"
fi

if grep -qxF 'SUPABASE_URL=http://127.0.0.1:54321' "$WS/.env" && grep -qxF 'SUPABASE_SERVICE_ROLE_KEY=local-key' "$WS/.env"; then
    pass ".env holds the local Supabase coordinates"
else
    fail ".env holds the local Supabase coordinates"
fi
if grep -qF 'Devcontainer ready: Supabase API http://127.0.0.1:54321, Studio http://127.0.0.1:54323' "$TMP/start1.out"; then
    pass "post-start.sh prints the ready line"
else
    fail "post-start.sh prints the ready line"
fi

cp "$WS/.env" "$TMP/env.first"
: > "$CALLS"
if (cd "$WS" && PATH="$BIN:$PATH" DOCKER_WAIT_LIMIT=5 DISCORD_TOKEN= OPENAI_API_KEY= \
        .devcontainer/post-start.sh > "$TMP/start2.out" 2> "$TMP/start2.err"); then
    pass "post-start.sh second run exits 0"
else
    fail "post-start.sh second run exits 0"
    cat "$TMP/start2.err"
fi
if grep -q '^supabase start' "$CALLS"; then fail "second run skips supabase start"; else pass "second run skips supabase start"; fi
if calls_has "supabase db reset"; then pass "second run repeats db reset"; else fail "second run repeats db reset"; fi
if cmp -s "$TMP/env.first" "$WS/.env"; then pass "second run leaves .env byte-identical"; else fail "second run leaves .env byte-identical"; fi

# Empty SUPABASE_START_EXCLUDE starts everything: no -x flag.
: > "$CALLS"
rm -f "$STATE/started"
(cd "$WS" && PATH="$BIN:$PATH" DOCKER_WAIT_LIMIT=5 SUPABASE_START_EXCLUDE= \
    .devcontainer/post-start.sh > /dev/null 2>&1) || true
if grep -qxF 'supabase start' "$CALLS"; then
    pass "empty SUPABASE_START_EXCLUDE starts every service"
else
    fail "empty SUPABASE_START_EXCLUDE starts every service"
    cat "$CALLS"
fi

# Docker never comes up.
: > "$CALLS"
rm -f "$STATE/started"
set +e
(cd "$WS" && PATH="$BIN:$PATH" FAKE_DOCKER_DOWN=1 DOCKER_WAIT_LIMIT=2 \
    .devcontainer/post-start.sh > "$TMP/down.out" 2> "$TMP/down.err")
DOWN_STATUS=$?
set -e
if [ "$DOWN_STATUS" -ne 0 ]; then pass "post-start.sh exits non-zero when Docker never starts"; else fail "post-start.sh exits non-zero when Docker never starts"; fi
if grep -qF 'Docker daemon' "$TMP/down.err"; then pass "failure message names the Docker daemon"; else fail "failure message names the Docker daemon"; fi
if grep -q '^supabase' "$CALLS"; then fail "no supabase call when Docker is down"; else pass "no supabase call when Docker is down"; fi
if [ "$(grep -cxF 'sleep 1' "$CALLS")" = "2" ]; then
    pass "Docker wait gives up after DOCKER_WAIT_LIMIT one-second sleeps"
else
    fail "Docker wait gives up after DOCKER_WAIT_LIMIT one-second sleeps"
    cat "$CALLS"
fi

# After a container restart dockerd brings the stack's containers back while its db container is still
# starting: status, start and db reset all fail for a while. post-start retries each of them.
: > "$CALLS"
rm -f "$STATE/started" "$STATE/reset-count" "$STATE/starting-count"
(cd "$WS" && PATH="$BIN:$PATH" DOCKER_WAIT_LIMIT=5 SUPABASE_RETRY_ATTEMPTS=5 SUPABASE_RETRY_DELAY=0 FAKE_STACK_STARTING=2 \
    .devcontainer/post-start.sh > /dev/null 2> "$TMP/starting.err") \
    || { fail "post-start.sh retries start while the stack is restarting"; cat "$TMP/starting.err"; }
if [ "$(grep -c '^supabase start' "$CALLS")" -ge 1 ] && calls_has "supabase db reset" && calls_has "supabase status -o env"; then
    pass "start is retried until the stack is ready, then db reset and .env follow"
else
    fail "start is retried until the stack is ready, then db reset and .env follow"
    cat "$CALLS"
fi

: > "$CALLS"
rm -f "$STATE/started" "$STATE/reset-count"
(cd "$WS" && PATH="$BIN:$PATH" DOCKER_WAIT_LIMIT=5 SUPABASE_RETRY_ATTEMPTS=5 SUPABASE_RETRY_DELAY=0 FAKE_DB_RESET_FAILS=2 \
    .devcontainer/post-start.sh > /dev/null 2> "$TMP/retry.err") \
    || { fail "post-start.sh retries db reset while the db container starts"; cat "$TMP/retry.err"; }
if [ "$(grep -cxF 'supabase db reset' "$CALLS")" = "3" ] && calls_has "supabase status -o env"; then
    pass "db reset is retried until the db container is ready"
else
    fail "db reset is retried until the db container is ready"
    cat "$CALLS"
fi

: > "$CALLS"
rm -f "$STATE/started" "$STATE/reset-count"
set +e
(cd "$WS" && PATH="$BIN:$PATH" DOCKER_WAIT_LIMIT=5 SUPABASE_RETRY_ATTEMPTS=3 SUPABASE_RETRY_DELAY=0 FAKE_DB_RESET_FAILS=99 \
    .devcontainer/post-start.sh > /dev/null 2> "$TMP/resetfail.err")
RESET_STATUS=$?
set -e
if [ "$RESET_STATUS" -ne 0 ] && [ "$(grep -cxF 'supabase db reset' "$CALLS")" = "3" ]; then
    pass "post-start.sh gives up after SUPABASE_RETRY_ATTEMPTS failed resets"
else
    fail "post-start.sh gives up after SUPABASE_RETRY_ATTEMPTS failed resets"
fi
if calls_has "supabase status -o env"; then fail "no .env written when db reset keeps failing"; else pass "no .env written when db reset keeps failing"; fi
rm -f "$STATE/reset-count"

# Host kernel without legacy iptables (nftables only): the DinD feature selects iptables-legacy,
# dockerd cannot create its NAT chain, so post-start switches to iptables-nft and restarts dockerd.
: > "$CALLS"
rm -f "$STATE/started" "$STATE/nft" "$STATE/dockerd"
(cd "$WS" && PATH="$BIN:$PATH" FAKE_LEGACY_IPTABLES_BROKEN=1 FAKE_DOCKER_DOWN_ONCE=1 DOCKER_WAIT_LIMIT=5 \
    DOCKER_INIT=/fake/docker-init.sh .devcontainer/post-start.sh > /dev/null 2> "$TMP/nft.err") \
    || { fail "post-start.sh succeeds on a host without legacy iptables"; cat "$TMP/nft.err"; }
if calls_has "sudo update-alternatives --set iptables /usr/sbin/iptables-nft" \
    && calls_has "sudo update-alternatives --set ip6tables /usr/sbin/ip6tables-nft"; then
    pass "legacy iptables unusable: switches iptables and ip6tables to nft"
else
    fail "legacy iptables unusable: switches iptables and ip6tables to nft"
    cat "$CALLS"
fi
if calls_has "sudo /fake/docker-init.sh true"; then
    pass "restarts dockerd through docker-init after the switch"
else
    fail "restarts dockerd through docker-init after the switch"
fi

# Working legacy iptables: nothing is switched.
: > "$CALLS"
rm -f "$STATE/started" "$STATE/nft" "$STATE/dockerd"
(cd "$WS" && PATH="$BIN:$PATH" DOCKER_WAIT_LIMIT=5 .devcontainer/post-start.sh > /dev/null 2>&1) || true
if grep -q 'update-alternatives' "$CALLS"; then fail "no iptables switch when legacy works"; else pass "no iptables switch when legacy works"; fi

# --- post-create.sh ----------------------------------------------------------
WSC="$TMP/ws-create"
make_workspace "$WSC"
: > "$CALLS"
if (cd "$WSC" && PATH="$CREATE_BIN:$PATH" env -u GH_TOKEN -u GITHUB_TOKEN \
        .devcontainer/post-create.sh > "$TMP/create1.out" 2> "$TMP/create1.err"); then
    pass "post-create.sh first run exits 0"
else
    fail "post-create.sh first run exits 0"
    cat "$TMP/create1.err"
fi
if [ "$(grep -c '^python3 -m venv' "$CALLS")" = "1" ]; then pass "first run creates the venv"; else fail "first run creates the venv"; cat "$CALLS"; fi
if calls_has "venv-python -m pip install -r requirements.txt -r requirements-dev.txt"; then
    pass "pip installs requirements.txt and requirements-dev.txt into the venv"
else
    fail "pip installs requirements.txt and requirements-dev.txt into the venv"
fi
if grep -q '^gh ' "$CALLS"; then fail "no gh call without GH_TOKEN"; else pass "no gh call without GH_TOKEN"; fi

: > "$CALLS"
(cd "$WSC" && PATH="$CREATE_BIN:$PATH" GH_TOKEN=dummy \
    .devcontainer/post-create.sh > "$TMP/create2.out" 2> "$TMP/create2.err") || fail "post-create.sh second run exits 0"
if grep -q '^python3 -m venv' "$CALLS"; then fail "second run reuses the venv"; else pass "second run reuses the venv"; fi
if grep -qF 'Reusing .venv' "$TMP/create2.out"; then pass "second run says it reuses .venv"; else fail "second run says it reuses .venv"; fi
if calls_has "gh auth setup-git"; then pass "gh auth setup-git runs when GH_TOKEN is set"; else fail "gh auth setup-git runs when GH_TOKEN is set"; fi

if [ "$FAILURES" -gt 0 ]; then
    echo -e "${RED}$FAILURES check(s) failed${NC}"
    exit 1
fi
echo -e "${GREEN}All devcontainer lifecycle checks passed${NC}"
