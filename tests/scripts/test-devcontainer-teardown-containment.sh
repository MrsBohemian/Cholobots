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

# Stub docker. Logs argv; `ps -aq` prints cid until a successful `rm -f` creates the state file.
mkdir -p "$TMP/bin"
cat >"$TMP/bin/docker" <<'STUB'
#!/usr/bin/env bash
echo "$*" >>"$STUB_LOG"
case "$1" in
  ps)
    # STUB_PS_FAIL: the daemon is unreachable for every ps. STUB_PS_FAIL_AFTER_RM: only once rm succeeded.
    if [ -n "${STUB_PS_FAIL:-}" ]; then exit 1; fi
    if [ -n "${STUB_PS_FAIL_AFTER_RM:-}" ] && [ -e "$STUB_STATE" ]; then exit 1; fi
    if [ ! -e "$STUB_STATE" ]; then echo cid; fi
    ;;
  inspect)
    printf 'cholobots-venv-abc\ndind-var-lib-docker-abc\nother-vol\n'
    ;;
  stop) ;;
  rm)
    if [ -n "${STUB_RM_FAIL:-}" ]; then exit 1; fi
    # STUB_RM_SURVIVES: rm reports success but the container is still listed afterwards.
    if [ -z "${STUB_RM_SURVIVES:-}" ]; then : >"$STUB_STATE"; fi
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
ABS="$(cd "$TMP/clone" && pwd -P)"
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
KEY_A="$(override_key "$ABS")"
assert_true "(a) removes this workspace's fallback images" \
  "grep -qx 'rmi cholobots-devcontainer:local-$KEY_A cholobots-devcontainer-base:local-$KEY_A' \"$TMP/log\""
assert_true "(a) clone is gone" "[ ! -d \"$TMP/clone\" ]"

# (b) Container removal fails.
make_clone
reset_state
ABS="$(cd "$TMP/clone" && pwd -P)"
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

# (d) Workspace given through a symlink: launch.sh hashes realpath, so teardown
# must remove the override dir keyed by the physical path.
make_clone
reset_state
PHYS="$(cd "$TMP/clone" && pwd -P)"
ln -sfn "$PHYS" "$TMP/clone-link"
OVERRIDE="$XDG_CACHE_HOME/cholobots-devcontainer/$(override_key "$PHYS")"
LOGICAL_OVERRIDE="$XDG_CACHE_HOME/cholobots-devcontainer/$(override_key "$TMP/clone-link")"
mkdir -p "$OVERRIDE"
touch "$OVERRIDE/devcontainer.json"
D_EXIT=0
"$TEARDOWN" "$TMP/clone-link" --keep-clone >"$TMP/d.out" 2>"$TMP/d.err" || D_EXIT=$?
assert_true "(d) exits 0" "[ \"$D_EXIT\" -eq 0 ]"
assert_true "(d) override dir keyed by the physical path is gone" "[ ! -e \"$OVERRIDE\" ]"
assert_true "(d) container lookup uses the physical path" \
  "grep -q 'label=devcontainer.local_folder=$PHYS\$' \"$TMP/log\" && ! grep -q 'clone-link' \"$TMP/log\""
assert_true "(d) logical and physical override keys differ" "[ \"$OVERRIDE\" != \"$LOGICAL_OVERRIDE\" ]"

# (e) rm -f reports success but the container is still listed: the re-query catches it.
make_clone
reset_state
ABS="$(cd "$TMP/clone" && pwd -P)"
OVERRIDE="$XDG_CACHE_HOME/cholobots-devcontainer/$(override_key "$ABS")"
mkdir -p "$OVERRIDE"
E_EXIT=0
STUB_RM_SURVIVES=1 "$TEARDOWN" "$TMP/clone" >"$TMP/e.out" 2>"$TMP/e.err" || E_EXIT=$?
assert_true "(e) exits 1" "[ \"$E_EXIT\" -eq 1 ]"
assert_true "(e) names the surviving container" "grep -q 'still bound' \"$TMP/e.err\" && grep -q 'cid' \"$TMP/e.err\""
assert_true "(e) no volume rm" "! grep -q '^volume rm' \"$TMP/log\""
assert_true "(e) clone still exists" "[ -d \"$TMP/clone\" ]"
assert_true "(e) override dir still exists" "[ -d \"$OVERRIDE\" ]"

# (f) docker ps fails on the initial query: nothing may be deleted, and it must not look like "no containers".
make_clone
reset_state
ABS="$(cd "$TMP/clone" && pwd -P)"
OVERRIDE="$XDG_CACHE_HOME/cholobots-devcontainer/$(override_key "$ABS")"
mkdir -p "$OVERRIDE"
F_EXIT=0
STUB_PS_FAIL=1 "$TEARDOWN" "$TMP/clone" >"$TMP/f.out" 2>"$TMP/f.err" || F_EXIT=$?
assert_true "(f) exits 1" "[ \"$F_EXIT\" -eq 1 ]"
assert_true "(f) stderr says Teardown incomplete" "grep -q 'Teardown incomplete' \"$TMP/f.err\""
assert_true "(f) no volume rm" "! grep -q '^volume rm' \"$TMP/log\""
assert_true "(f) no image rm" "! grep -q '^rmi' \"$TMP/log\""
assert_true "(f) clone still exists" "[ -d \"$TMP/clone\" ]"
assert_true "(f) override dir still exists" "[ -d \"$OVERRIDE\" ]"

# (g) rm succeeds but the survivor re-check ps fails: cannot confirm removal, so keep the clone.
make_clone
reset_state
ABS="$(cd "$TMP/clone" && pwd -P)"
OVERRIDE="$XDG_CACHE_HOME/cholobots-devcontainer/$(override_key "$ABS")"
mkdir -p "$OVERRIDE"
G_EXIT=0
STUB_PS_FAIL_AFTER_RM=1 "$TEARDOWN" "$TMP/clone" >"$TMP/g.out" 2>"$TMP/g.err" || G_EXIT=$?
assert_true "(g) exits 1" "[ \"$G_EXIT\" -eq 1 ]"
assert_true "(g) stderr says Teardown incomplete" "grep -q 'Teardown incomplete' \"$TMP/g.err\""
assert_true "(g) no volume rm" "! grep -q '^volume rm' \"$TMP/log\""
assert_true "(g) clone still exists" "[ -d \"$TMP/clone\" ]"
assert_true "(g) override dir still exists" "[ -d \"$OVERRIDE\" ]"

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ]
