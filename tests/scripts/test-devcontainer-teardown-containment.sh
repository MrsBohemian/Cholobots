#!/usr/bin/env bash
# ABOUTME: Containment test for teardown.sh: container removal is forced and verified, volumes and the
# ABOUTME: per-workspace override dir are cleaned only after removal succeeds. Uses a stub docker only.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEARDOWN="$REPO_ROOT/scripts/devcontainer/teardown.sh"

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

echo "=== Devcontainer teardown containment tests ==="

# Static shape checks.
assert_true "teardown.sh uses docker rm -f" \
  "grep -qE 'docker rm -f' \"\$TEARDOWN\""
assert_true "teardown.sh re-queries for surviving containers" \
  "[ \"\$(grep -cE 'docker ps -aq --filter \"label=devcontainer.local_folder=' \"\$TEARDOWN\")\" -ge 2 ]"
assert_true "docker rm -f outcome is not suppressed with || true" \
  "! grep -E 'docker rm -f' \"\$TEARDOWN\" | grep -q '|| true'"
assert_true "teardown.sh tracks FAILED=1 and exits 1" \
  "grep -qE 'FAILED=1' \"\$TEARDOWN\" && grep -qE 'exit 1' \"\$TEARDOWN\""

# Stub docker. Logs argv; `ps -aq` prints cid until `rm -f` creates the state file.
mkdir -p "$TMP/bin"
cat >"$TMP/bin/docker" <<'STUB'
#!/usr/bin/env bash
echo "$*" >>"$STUB_LOG"
case "$1" in
  ps)
    if [ ! -e "$STUB_STATE" ]; then echo cid; fi
    ;;
  inspect)
    printf 'cholobots-venv-abc\ndind-var-lib-docker-abc\nother-vol\n'
    ;;
  stop) ;;
  rm)
    if [ -n "${STUB_RM_FAIL:-}" ]; then exit 1; fi
    : >"$STUB_STATE"
    ;;
  volume)
    if [ -n "${STUB_VOLUME_RM_FAIL:-}" ]; then exit 1; fi
    ;;
esac
exit 0
STUB
chmod +x "$TMP/bin/docker"
export PATH="$TMP/bin:$PATH"
if [ "$(command -v docker)" != "$TMP/bin/docker" ]; then
  echo "docker does not resolve to the stub; refusing to run" >&2
  exit 1
fi
export STUB_LOG="$TMP/log"
export STUB_STATE="$TMP/state"
export XDG_CACHE_HOME="$TMP/cache"

make_clone() {
  rm -rf "$TMP/clone"
  mkdir -p "$TMP/clone"
  git init -q "$TMP/clone"
  touch "$TMP/clone/.git/cholobots-managed-clone"
}

override_key() {
  printf %s "$1" | sha256sum | cut -c1-16
}

reset_state() {
  rm -f "$STUB_STATE" "$STUB_LOG"
  : >"$STUB_LOG"
}

# (a) Success path.
make_clone
reset_state
ABS="$(cd "$TMP/clone" && pwd)"
OVERRIDE="$XDG_CACHE_HOME/cholobots-devcontainer/$(override_key "$ABS")"
mkdir -p "$OVERRIDE"
touch "$OVERRIDE/devcontainer.json"
OK_EXIT=0
"$TEARDOWN" "$TMP/clone" >"$TMP/a.out" 2>"$TMP/a.err" || OK_EXIT=$?
assert_true "(a) exits 0" "[ \"$OK_EXIT\" -eq 0 ]"
assert_true "(a) removes cholobots-venv volume" "grep -qx 'volume rm cholobots-venv-abc' \"$TMP/log\""
assert_true "(a) removes dind volume" "grep -qx 'volume rm dind-var-lib-docker-abc' \"$TMP/log\""
assert_true "(a) leaves unrelated volumes alone" "! grep -q 'volume rm other-vol' \"$TMP/log\""
RM_LINE="$(grep -nE '^rm -f cid' "$TMP/log" | head -1 | cut -d: -f1 || true)"
VOL_LINE="$(grep -nE '^volume rm' "$TMP/log" | head -1 | cut -d: -f1 || true)"
assert_true "(a) container rm precedes the first volume rm" \
  "[ -n \"$RM_LINE\" ] && [ -n \"$VOL_LINE\" ] && [ \"$RM_LINE\" -lt \"$VOL_LINE\" ]"
assert_true "(a) override dir is gone" "[ ! -e \"$OVERRIDE\" ]"
assert_true "(a) clone is gone" "[ ! -d \"$TMP/clone\" ]"

# (b) Container removal fails.
make_clone
reset_state
ABS="$(cd "$TMP/clone" && pwd)"
OVERRIDE="$XDG_CACHE_HOME/cholobots-devcontainer/$(override_key "$ABS")"
mkdir -p "$OVERRIDE"
B_EXIT=0
STUB_RM_FAIL=1 "$TEARDOWN" "$TMP/clone" >"$TMP/b.out" 2>"$TMP/b.err" || B_EXIT=$?
assert_true "(b) exits 1" "[ \"$B_EXIT\" -eq 1 ]"
assert_true "(b) stderr says Teardown incomplete" "grep -q 'Teardown incomplete' \"$TMP/b.err\""
assert_true "(b) no volume rm" "! grep -q '^volume rm' \"$TMP/log\""
assert_true "(b) clone still exists" "[ -d \"$TMP/clone\" ]"
assert_true "(b) override dir still exists" "[ -d \"$OVERRIDE\" ]"

# (c) Volume removal fails: warning only.
make_clone
reset_state
C_EXIT=0
STUB_VOLUME_RM_FAIL=1 "$TEARDOWN" "$TMP/clone" >"$TMP/c.out" 2>"$TMP/c.err" || C_EXIT=$?
assert_true "(c) exits 0" "[ \"$C_EXIT\" -eq 0 ]"
assert_true "(c) warns could not remove volume" "grep -q 'could not remove volume' \"$TMP/c.err\""

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ]
