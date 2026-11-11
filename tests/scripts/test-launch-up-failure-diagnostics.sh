#!/usr/bin/env bash
# ABOUTME: Behavioural test for launch.sh reporting diagnostics when `devcontainer up` fails
# ABOUTME: and warning when the installed devcontainer CLI predates the pinned version.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LAUNCH="$REPO_ROOT/scripts/devcontainer/launch.sh"
WORKTREE_ADD="$REPO_ROOT/scripts/devcontainer/worktree-add.sh"

export GIT_CONFIG_GLOBAL=/dev/null
export GIT_CONFIG_SYSTEM=/dev/null

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
export XDG_CACHE_HOME="$TMP/cache"

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

echo "=== launch.sh devcontainer-up failure diagnostics ==="

# Stub CLIs on PATH: `devcontainer up` fails the way an older CLI would when it
# meets an unknown option, and `docker` succeeds silently (pull succeeds, ps is
# empty) so the container-ID label fallback finds nothing.
STUB_BIN="$TMP/bin"
mkdir -p "$STUB_BIN"

cat >"$STUB_BIN/devcontainer" <<'STUB'
#!/usr/bin/env bash
if [ "${1:-}" = "--version" ]; then
    echo "${STUB_DEVCONTAINER_VERSION:-0.84.1}"
    exit 0
fi
echo "stub-up-failure: unknown option '--mount-git-worktree-common-dir'" >&2
exit 1
STUB
chmod +x "$STUB_BIN/devcontainer"

cat >"$STUB_BIN/docker" <<'STUB'
#!/usr/bin/env bash
exit 0
STUB
chmod +x "$STUB_BIN/docker"

export PATH="$STUB_BIN:$PATH"
# Abort, not just record a failure: a run with the real CLIs would reach the Docker daemon.
[ "$(command -v docker)" = "$STUB_BIN/docker" ] || { echo "FATAL: docker is not the stub; refusing to run" >&2; exit 1; }
[ "$(command -v devcontainer)" = "$STUB_BIN/devcontainer" ] || { echo "FATAL: devcontainer is not the stub; refusing to run" >&2; exit 1; }

# A linked worktree of a sandbox repo, so launch.sh skips checkout creation,
# passes the main-checkout refusal and goes straight to `devcontainer up`.
SANDBOX="$TMP/sandbox"
git init -q -b main "$SANDBOX"
mkdir -p "$SANDBOX/.devcontainer"
echo '{"name":"t","build":{"dockerfile":"Dockerfile"}}' >"$SANDBOX/.devcontainer/devcontainer.json"
git -C "$SANDBOX" add .devcontainer
git -C "$SANDBOX" -c user.email=t@t -c user.name=t commit -q -m init
WS="$TMP/ws"
"$WORKTREE_ADD" "$SANDBOX" "$WS" feature/diag main >/dev/null

run_launch() {
    local version="$1" stderr_file="$2"
    local exit_code=0
    STUB_DEVCONTAINER_VERSION="$version" \
        "$LAUNCH" "$WS" >/dev/null 2>"$stderr_file" </dev/null || exit_code=$?
    echo "$exit_code"
}

# 1. A failing `devcontainer up` reaches the diagnostic instead of dying silently.
STDERR_1="$TMP/launch-current.err"
EXIT_1="$(run_launch 0.84.1 "$STDERR_1")"
assert_true "launch.sh exits non-zero when devcontainer up fails" "[ \"$EXIT_1\" -ne 0 ]"
assert_true "launch.sh reports it could not determine the container ID" \
    "grep -q 'could not determine container ID' \"$STDERR_1\""
assert_true "launch.sh echoes the captured devcontainer up output" \
    "grep -q 'stub-up-failure' \"$STDERR_1\""

# 2. A CLI older than the pinned version is called out explicitly.
STDERR_2="$TMP/launch-old.err"
EXIT_2="$(run_launch 0.80.0 "$STDERR_2")"
assert_true "launch.sh still exits non-zero with an old CLI" "[ \"$EXIT_2\" -ne 0 ]"
assert_true "launch.sh warns that the installed CLI is older than the pin" \
    "grep -q '0.84.1' \"$STDERR_2\" && grep -qi 'older' \"$STDERR_2\""

# 3. A CLI at or above the pin produces no version warning.
assert_true "launch.sh does not warn when the CLI meets the pin" \
    "! grep -qi 'older' \"$STDERR_1\""

# 4. Version comparison is numeric per component, not lexicographic.
STDERR_PATCH="$TMP/launch-patch.err"
run_launch 0.84.0 "$STDERR_PATCH" >/dev/null
assert_true "launch.sh warns on a lower patch version" \
    "grep -qi 'older' \"$STDERR_PATCH\""

STDERR_MINOR="$TMP/launch-minor.err"
run_launch 0.9.0 "$STDERR_MINOR" >/dev/null
assert_true "launch.sh warns on a numerically lower minor version" \
    "grep -qi 'older' \"$STDERR_MINOR\""

STDERR_NEWER="$TMP/launch-newer.err"
run_launch 1.2.0 "$STDERR_NEWER" >/dev/null
assert_true "launch.sh does not warn on a newer major version" \
    "! grep -qi 'older' \"$STDERR_NEWER\""

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ]
