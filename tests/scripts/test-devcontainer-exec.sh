#!/usr/bin/env bash
# ABOUTME: Behavioural test for exec.sh: container lookup, venv-first PATH and argv handling,
# ABOUTME: using a stub docker so no real daemon is touched (story #73).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SCRIPT="$REPO_ROOT/scripts/devcontainer/exec.sh"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/bin" "$TMP/my-ws"
cat >"$TMP/bin/docker" <<'STUB'
#!/usr/bin/env bash
if [ "$1" = ps ]; then
    printf '%s\n' "$@" >"${STUB_PS_ARGS:-/dev/null}"
    [ -n "${STUB_IDS:-}" ] && printf '%s\n' $STUB_IDS
    exit 0
fi
printf '%s\n' "$@" >"$STUB_ARGS"
STUB
chmod +x "$TMP/bin/docker"
export PATH="$TMP/bin:$PATH"
if [ "$(command -v docker)" != "$TMP/bin/docker" ]; then
    echo "docker does not resolve to the stub; refusing to run" >&2
    exit 1
fi
export STUB_ARGS="$TMP/args"

PASS=0
FAIL=0
assert_true() {
    local test_name="$1" condition="$2"
    if eval "$condition"; then
        echo "  PASS: $test_name"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $test_name"
        FAIL=$((FAIL + 1))
    fi
}

echo "=== exec.sh tests (story #73) ==="

# 1. One container: argv shape, venv prefix, command preserved as one argument.
STUB_IDS="abc123" "$SCRIPT" "$TMP/my-ws" 'echo "a b"; echo c' >/dev/null 2>&1
for token in exec -u vscode -w /workspaces/my-ws abc123 bash -lc; do
    assert_true "args contain '$token'" "grep -Fxq -- '$token' '$TMP/args'"
done
assert_true "args carry the .venv/bin prefix" "grep -Fq '.venv/bin' '$TMP/args'"
assert_true "command string is a single argument line" "grep -Fxq 'echo \"a b\"; echo c' '$TMP/args'"

# 2. No container.
OUT="$TMP/none.out"
RC=0
STUB_IDS="" "$SCRIPT" "$TMP/my-ws" 'true' >"$OUT" 2>&1 || RC=$?
assert_true "no container exits 1" "[ $RC -eq 1 ]"
assert_true "no container message" "grep -q 'no running container' '$OUT'"

# 3. Two containers.
RC=0
STUB_IDS="aaa bbb" "$SCRIPT" "$TMP/my-ws" 'true' >"$OUT" 2>&1 || RC=$?
assert_true "two containers exits 1" "[ $RC -eq 1 ]"
assert_true "two containers message" "grep -q 'run teardown.sh first' '$OUT'"

# 4. No command.
RC=0
STUB_IDS="abc123" "$SCRIPT" "$TMP/my-ws" >"$OUT" 2>&1 || RC=$?
assert_true "no command exits 1" "[ $RC -eq 1 ]"
assert_true "no command prints Usage" "grep -q 'Usage' '$OUT'"

# 5. A symlinked workspace path is resolved to the physical path, as launch.sh labels it.
ln -s "$TMP/my-ws" "$TMP/link-ws"
PS_ARGS="$TMP/ps-args"
STUB_PS_ARGS="$PS_ARGS" STUB_IDS="abc123" "$SCRIPT" "$TMP/link-ws" 'true' >/dev/null 2>&1
REAL_WS="$(cd "$TMP/my-ws" && pwd -P)"
assert_true "symlinked path filters on the physical path" "grep -Fxq 'label=devcontainer.local_folder=$REAL_WS' '$PS_ARGS'"
assert_true "symlinked path still maps to /workspaces/my-ws" "grep -Fxq /workspaces/my-ws '$TMP/args'"

echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
