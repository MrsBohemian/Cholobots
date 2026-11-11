#!/usr/bin/env bash
# ABOUTME: Tests start-registry-mirror.sh detection: a running mirror in a very large `docker ps` listing is
# ABOUTME: still found (no SIGPIPE/pipefail false negative), a stopped one is restarted, none is created. Stub docker.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
MIRROR="$REPO_ROOT/scripts/devcontainer/start-registry-mirror.sh"

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

echo "=== start-registry-mirror.sh detection tests ==="

# Stub docker. `ps` lists the mirror first, then STUB_FILLER_LINES more names, so a reader that
# stops at the first match closes the pipe while the stub is still writing. `ps -a` adds the
# mirror only when STUB_STOPPED is set.
mkdir -p "$TMP/bin"
cat >"$TMP/bin/docker" <<'STUB'
#!/usr/bin/env bash
echo "$*" >>"$STUB_LOG"
case "$1" in
  ps)
    if [ "$2" = "-a" ]; then
      [ -n "${STUB_STOPPED:-}" ] && echo registry-mirror
      exit 0
    fi
    if [ -n "${STUB_PS_FAIL:-}" ]; then exit 1; fi
    # Like the real CLI, die with the writer's status when the reader closes the pipe early.
    set -o pipefail
    { [ -z "${STUB_RUNNING:-}" ] || echo registry-mirror; seq 1 "${STUB_FILLER_LINES:-0}"; } | sed 's/^/other-container-/;s/^other-container-registry-mirror$/registry-mirror/'
    exit "${PIPESTATUS[1]}"
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

run() { : >"$STUB_LOG"; EXIT=0; "$MIRROR" >"$TMP/out" 2>"$TMP/err" || EXIT=$?; }

# (a) Running mirror in a huge listing: detected, nothing started or created.
STUB_RUNNING=1 STUB_FILLER_LINES=300000 run
assert_true "(a) exits 0" "[ \"$EXIT\" -eq 0 ]"
assert_true "(a) no docker start" "! grep -q '^start' \"$STUB_LOG\""
assert_true "(a) no docker run" "! grep -q '^run' \"$STUB_LOG\""

# (b) Running mirror, small listing: same.
STUB_RUNNING=1 run
assert_true "(b) no docker start or run" "! grep -qE '^(start|run)' \"$STUB_LOG\""

# (c) Stopped mirror: restarted, not recreated.
STUB_STOPPED=1 run
assert_true "(c) docker start registry-mirror" "grep -qx 'start registry-mirror' \"$STUB_LOG\""
assert_true "(c) no docker run" "! grep -q '^run' \"$STUB_LOG\""

# (d) No mirror at all: created.
run
assert_true "(d) docker run creates the mirror" "grep -q '^run -d --name registry-mirror' \"$STUB_LOG\""

# (e) A name that merely contains the mirror name does not count as running.
STUB_FILLER_LINES=3 run
assert_true "(e) only exact name matches (other-container-N is not the mirror)" "grep -q '^run' \"$STUB_LOG\""

# (f) A failing `docker ps` is an error, not "no mirror": nothing is started or created.
STUB_PS_FAIL=1 run
assert_true "(f) exits non-zero" "[ \"$EXIT\" -ne 0 ]"
assert_true "(f) no docker start or run" "! grep -qE '^(start|run)' \"$STUB_LOG\""

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ]
