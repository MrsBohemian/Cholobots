#!/usr/bin/env bash
# ABOUTME: Behavioural test for teardown.sh's refusal to rm -rf a git worktree
# ABOUTME: or a directory launch.sh did not mark as a managed clone.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEARDOWN="$REPO_ROOT/scripts/devcontainer/teardown.sh"
WORKTREE_ADD="$REPO_ROOT/scripts/devcontainer/worktree-add.sh"

export GIT_CONFIG_GLOBAL=/dev/null
export GIT_CONFIG_SYSTEM=/dev/null

TMP="$(mktemp -d)"
# Stub docker: reports no containers, so teardown never touches a real daemon.
mkdir -p "$TMP/bin"
printf '#!/usr/bin/env bash\nexit 0\n' >"$TMP/bin/docker"
chmod +x "$TMP/bin/docker"
export PATH="$TMP/bin:$PATH"
if [ "$(command -v docker)" != "$TMP/bin/docker" ]; then
    echo "docker does not resolve to the stub; refusing to run" >&2
    exit 1
fi
export XDG_CACHE_HOME="$TMP/cache"
trap 'git -C "$TMP/repo" worktree remove --force "$TMP/worktree" >/dev/null 2>&1 || true; rm -rf "$TMP"' EXIT

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

echo "=== teardown.sh worktree/managed-clone guard tests (story #73) ==="

git -c user.name=test -c user.email=test@example.com init -q -b main "$TMP/repo"
git -C "$TMP/repo" -c user.name=test -c user.email=test@example.com commit -q --allow-empty -m "initial"

# 1. Refuses a worktree.
"$WORKTREE_ADD" "$TMP/repo" "$TMP/worktree" feature/x main >"$TMP/worktree-add-out" 2>&1
WORKTREE_EXIT=0
STDERR_1="$TMP/teardown-worktree.err"
"$TEARDOWN" "$TMP/worktree" >/dev/null 2>"$STDERR_1" || WORKTREE_EXIT=$?
assert_true "teardown.sh exits non-zero on a worktree path" "[ \"$WORKTREE_EXIT\" -ne 0 ]"
assert_true "worktree directory still exists after refusal" "[ -d \"$TMP/worktree\" ]"
assert_true "worktree refusal message mentions git worktree" "grep -q 'git worktree' \"$STDERR_1\""

# 2. Refuses a directory with no cholobots-managed-clone marker.
mkdir -p "$TMP/unmarked"
git -c user.name=test -c user.email=test@example.com init -q "$TMP/unmarked"
STDERR_2="$TMP/teardown-unmarked.err"
UNMARKED_EXIT=0
"$TEARDOWN" "$TMP/unmarked" >/dev/null 2>"$STDERR_2" || UNMARKED_EXIT=$?
assert_true "teardown.sh exits non-zero on an unmarked directory" "[ \"$UNMARKED_EXIT\" -ne 0 ]"
assert_true "unmarked directory still exists after refusal" "[ -d \"$TMP/unmarked\" ]"
assert_true "unmarked refusal message mentions cholobots-managed-clone" "grep -q 'cholobots-managed-clone' \"$STDERR_2\""

# 3. Removes a marked clone.
mkdir -p "$TMP/marked"
git -c user.name=test -c user.email=test@example.com init -q "$TMP/marked"
touch "$TMP/marked/.git/cholobots-managed-clone"
MARKED_OK=0
if "$TEARDOWN" "$TMP/marked" >/dev/null 2>&1; then
    MARKED_OK=1
fi
assert_true "teardown.sh exits zero on a marked clone" "[ \"$MARKED_OK\" -eq 1 ]"
assert_true "marked clone directory is gone after teardown" "[ ! -d \"$TMP/marked\" ]"

# 4. --keep-clone still short-circuits both guards.
mkdir -p "$TMP/unmarked2"
git -c user.name=test -c user.email=test@example.com init -q "$TMP/unmarked2"
KEEP_OK=0
if "$TEARDOWN" "$TMP/unmarked2" --keep-clone >/dev/null 2>&1; then
    KEEP_OK=1
fi
assert_true "teardown.sh --keep-clone exits zero on unmarked directory" "[ \"$KEEP_OK\" -eq 1 ]"
assert_true "--keep-clone leaves directory in place" "[ -d \"$TMP/unmarked2\" ]"

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ]
