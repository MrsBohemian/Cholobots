#!/usr/bin/env bash
# ABOUTME: Regression test for smoke-test.sh on a detached-HEAD checkout: launch.sh must be given a real
# ABOUTME: branch (clone --branch HEAD fails), and any temporary branch is removed afterwards. Stubs only.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

export GIT_CONFIG_GLOBAL=/dev/null
export GIT_CONFIG_SYSTEM=/dev/null

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

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "=== Devcontainer smoke-test detached HEAD tests ==="

# A throwaway repo laid out like this one, so smoke-test.sh resolves REPO_ROOT inside it.
FAKE="$TMP/repo"
mkdir -p "$FAKE/scripts/devcontainer" "$FAKE/.devcontainer" "$TMP/bin"
cp "$REPO_ROOT/scripts/devcontainer/smoke-test.sh" "$FAKE/scripts/devcontainer/"
printf 'ARG SUPABASE_CLI_VERSION=1.0.0\n' >"$FAKE/.devcontainer/Dockerfile"
for stub in launch exec teardown prepare-clone; do
  cat >"$FAKE/scripts/devcontainer/$stub.sh" <<'STUB'
#!/usr/bin/env bash
echo "$(basename "$0") $*" >>"$STUB_LOG"
exit 1
STUB
  chmod +x "$FAKE/scripts/devcontainer/$stub.sh"
done
printf '#!/usr/bin/env bash\nexit 0\n' >"$TMP/bin/docker"
chmod +x "$TMP/bin/docker"
export PATH="$TMP/bin:$PATH"
export STUB_LOG="$TMP/log"
export TMPDIR="$TMP"

git init -q -b main "$FAKE"
git -C "$FAKE" add -A
git -C "$FAKE" -c user.name=t -c user.email=t@example.com commit -qm init
git -C "$FAKE" checkout -q --detach

run_smoke() { : >"$STUB_LOG"; "$FAKE/scripts/devcontainer/smoke-test.sh" >"$TMP/out" 2>&1 || true; }

# (a) Detached HEAD: launch.sh gets a branch name that exists, never the literal HEAD.
run_smoke
LAUNCH_BRANCH="$(sed -n 's/^launch.sh [^ ]* //p' "$STUB_LOG" | head -1)"
assert_true "(a) launch.sh was called" "[ -n \"$LAUNCH_BRANCH\" ]"
assert_true "(a) the branch is not the literal HEAD" "[ \"$LAUNCH_BRANCH\" != HEAD ]"
assert_true "(a) the branch pointed at HEAD while the smoke test ran" \
  "grep -q 'smoke-test-' <<<\"$LAUNCH_BRANCH\""
assert_true "(a) the temporary branch is removed afterwards" \
  "! git -C \"$FAKE\" rev-parse --verify -q \"refs/heads/$LAUNCH_BRANCH\" >/dev/null"
assert_true "(a) the checkout is still detached" "[ -z \"\$(git -C \"$FAKE\" symbolic-ref -q HEAD || true)\" ]"

# (b) On a branch, that branch is passed through and no temporary branch is made.
git -C "$FAKE" checkout -q -b feature/x
run_smoke
assert_true "(b) the current branch is passed to launch.sh" "grep -qE '^launch.sh [^ ]* feature/x\$' \"$STUB_LOG\""
assert_true "(b) no temporary branch left" "[ -z \"\$(git -C \"$FAKE\" branch --list 'smoke-test-*')\" ]"

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ]
