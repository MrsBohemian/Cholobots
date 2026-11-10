#!/usr/bin/env bash
# ABOUTME: Behavioural test for launch.sh's pre-built image step: GHCR login token order, pull, notices
# ABOUTME: and the two-stage local fallback, using stubbed docker and devcontainer (no daemon is reached).

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
unset SSH_AUTH_SOCK GH_TOKEN GITHUB_TOKEN GH_TOKEN_FOR_PACKAGES CHOLOBOTS_DEVCONTAINER_IMAGE CHOLOBOTS_CONTAINER_GH_TOKEN || true

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

echo "=== launch.sh pre-built image step ==="

STUB_BIN="$TMP/bin"
mkdir -p "$STUB_BIN"
export DOCKER_LOG="$TMP/docker.log"
export LOGIN_STDIN="$TMP/login.stdin"

# docker logs its argv; `login` also records stdin; STUB_LOGIN_FAIL and STUB_PULL_FAIL force failures.
cat >"$STUB_BIN/docker" <<'STUB'
#!/usr/bin/env bash
echo "docker $*" >>"$DOCKER_LOG"
case "${1:-}" in
    login)
        cat >"$LOGIN_STDIN"
        [ -z "${STUB_LOGIN_FAIL:-}" ] || exit 1
        ;;
    pull)
        [ -z "${STUB_PULL_FAIL:-}" ] || exit 1
        ;;
    ps)
        # No registry mirror listed, no container found by label.
        ;;
esac
exit 0
STUB
chmod +x "$STUB_BIN/docker"

cat >"$STUB_BIN/devcontainer" <<'STUB'
#!/usr/bin/env bash
if [ "${1:-}" = "--version" ]; then
    echo "0.84.1"
    exit 0
fi
echo "devcontainer $*" >>"$DOCKER_LOG"
echo '{"outcome":"success","containerId":"stubcontainer123"}'
STUB
chmod +x "$STUB_BIN/devcontainer"

export PATH="$STUB_BIN:$PATH"
[ "$(command -v docker)" = "$STUB_BIN/docker" ] || { echo "FATAL: docker is not the stub; refusing to run" >&2; exit 1; }
[ "$(command -v devcontainer)" = "$STUB_BIN/devcontainer" ] || { echo "FATAL: devcontainer is not the stub; refusing to run" >&2; exit 1; }

# A linked worktree of a sandbox repo with a Dockerfile.prebuild, so launch.sh
# goes straight to the image step and the fallback runs both stages.
SANDBOX="$TMP/sandbox"
git init -q -b main "$SANDBOX"
mkdir -p "$SANDBOX/.devcontainer"
echo '{"name":"t","build":{"dockerfile":"Dockerfile","context":".."}}' >"$SANDBOX/.devcontainer/devcontainer.json"
: >"$SANDBOX/.devcontainer/Dockerfile"
: >"$SANDBOX/.devcontainer/Dockerfile.prebuild"
git -C "$SANDBOX" add .devcontainer
git -C "$SANDBOX" -c user.email=t@t -c user.name=t commit -q -m init
WS="$TMP/ws"
"$WORKTREE_ADD" "$SANDBOX" "$WS" feature/img main >/dev/null

# run_launch <name> [VAR=value ...]: runs launch.sh with a clean log; leaves
# $TMP/<name>.out, $TMP/<name>.err and the override config in $TMP/<name>.json.
run_launch() {
    local name="$1"
    shift
    : >"$DOCKER_LOG"
    rm -f "$LOGIN_STDIN"
    rm -rf "$XDG_CACHE_HOME"
    env "$@" "$LAUNCH" "$WS" >"$TMP/$name.out" 2>"$TMP/$name.err" </dev/null || true
    cp "$XDG_CACHE_HOME"/cholobots-devcontainer/*/devcontainer.json "$TMP/$name.json"
    cp "$DOCKER_LOG" "$TMP/$name.log"
}

# 1. Token precedence: GH_TOKEN_FOR_PACKAGES, then GH_TOKEN, then GITHUB_TOKEN.
run_launch t1 GH_TOKEN_FOR_PACKAGES=tok-a GH_TOKEN=tok-b GITHUB_TOKEN=tok-c
assert_true "login reads GH_TOKEN_FOR_PACKAGES from stdin first" "[ \"\$(cat \"$LOGIN_STDIN\")\" = tok-a ]"
assert_true "login targets ghcr.io over --password-stdin" \
    "grep -q '^docker login ghcr.io .*--password-stdin' \"$TMP/t1.log\""
assert_true "no token appears in any logged argv" \
    "! grep -q 'tok-' \"$TMP/t1.log\""

run_launch t2 GH_TOKEN=tok-b GITHUB_TOKEN=tok-c
assert_true "login falls back to GH_TOKEN" "[ \"\$(cat \"$LOGIN_STDIN\")\" = tok-b ]"

run_launch t3 GITHUB_TOKEN=tok-c
assert_true "login falls back to GITHUB_TOKEN" "[ \"\$(cat \"$LOGIN_STDIN\")\" = tok-c ]"

run_launch t4 STUB_UNUSED=1
assert_true "no token means no docker login" "! grep -q '^docker login' \"$TMP/t4.log\""

# 2. A failed login warns and still pulls.
run_launch t5 GH_TOKEN_FOR_PACKAGES=tok-a STUB_LOGIN_FAIL=1
assert_true "login failure warns about an anonymous pull on stderr" \
    "grep -q 'GHCR login failed; trying an anonymous pull' \"$TMP/t5.err\""
assert_true "login failure still pulls the image" "grep -q '^docker pull ' \"$TMP/t5.log\""
assert_true "login failure is not fatal" "grep -q '^CONTAINER_ID=stubcontainer123\$' \"$TMP/t5.out\""

# 3. Pull succeeds: no build, config uses the GHCR reference, stdout is the container id only.
GHCR_REF=ghcr.io/mrsbohemian/cholobots/devcontainer:latest
run_launch t6 STUB_UNUSED=1
assert_true "pull ok: pulls the default GHCR image" "grep -qx 'docker pull $GHCR_REF' \"$TMP/t6.log\""
assert_true "pull ok: no docker build" "! grep -q '^docker build' \"$TMP/t6.log\""
assert_true "pull ok: notice on stderr" "grep -q 'Using pre-built image: $GHCR_REF' \"$TMP/t6.err\""
assert_true "pull ok: override config image is the GHCR reference" \
    "[ \"\$(jq -r .image \"$TMP/t6.json\")\" = '$GHCR_REF' ]"
assert_true "pull ok: override config has no build" "[ \"\$(jq 'has(\"build\")' \"$TMP/t6.json\")\" = false ]"
assert_true "pull ok: stdout is only the container id line" \
    "[ \"\$(cat \"$TMP/t6.out\")\" = 'CONTAINER_ID=stubcontainer123' ]"

# 4. Pull fails: stderr fallback notice, two builds in order, local image in config.
run_launch t7 STUB_PULL_FAIL=1
assert_true "pull fails: stderr says the image is not available and builds locally" \
    "grep -q 'Pre-built image not available' \"$TMP/t7.err\" && grep -q 'building locally' \"$TMP/t7.err\""
BUILD_LINES="$(grep '^docker build' "$TMP/t7.log" || true)"
assert_true "pull fails: exactly two builds" "[ \"\$(printf '%s\n' \"$BUILD_LINES\" | wc -l)\" -eq 2 ]"
FIRST_BUILD="$(printf '%s\n' "$BUILD_LINES" | sed -n 1p)"
SECOND_BUILD="$(printf '%s\n' "$BUILD_LINES" | sed -n 2p)"
WS_REAL="$(realpath "$WS")"
assert_true "pull fails: first build is the base Dockerfile tagged cholobots-devcontainer-base:local" \
    "case '$FIRST_BUILD' in *'-f $WS_REAL/.devcontainer/Dockerfile '*'-t cholobots-devcontainer-base:local'*) true;; *) false;; esac"
assert_true "pull fails: second build is Dockerfile.prebuild with the local base as BASE_IMAGE" \
    "case '$SECOND_BUILD' in *'-f $WS_REAL/.devcontainer/Dockerfile.prebuild '*'--build-arg BASE_IMAGE=cholobots-devcontainer-base:local'*) true;; *) false;; esac"
assert_true "pull fails: both builds use the devcontainer.json build context (repo root)" \
    "[ \"\${FIRST_BUILD##* }\" = '$WS_REAL' ] && [ \"\${SECOND_BUILD##* }\" = '$WS_REAL' ]"
LOCAL_IMG="$(jq -r .image "$TMP/t7.json")"
assert_true "pull fails: config image is the local prebuild tag, not the GHCR name" \
    "case '$LOCAL_IMG' in cholobots-devcontainer:local*) true;; *) false;; esac"
assert_true "pull fails: config has no build" "[ \"\$(jq 'has(\"build\")' \"$TMP/t7.json\")\" = false ]"
assert_true "pull fails: stdout is only the container id line" \
    "[ \"\$(cat \"$TMP/t7.out\")\" = 'CONTAINER_ID=stubcontainer123' ]"

# 5. CHOLOBOTS_DEVCONTAINER_IMAGE overrides the pulled reference.
run_launch t8 CHOLOBOTS_DEVCONTAINER_IMAGE=foo:bar
assert_true "override: pulls foo:bar" "grep -qx 'docker pull foo:bar' \"$TMP/t8.log\""
assert_true "override: config image is foo:bar" "[ \"\$(jq -r .image \"$TMP/t8.json\")\" = foo:bar ]"

# 6. Static: the pull comes before the first non-comment `devcontainer up`.
PULL_LINE="$(grep -n 'docker pull' "$LAUNCH" | head -1 | cut -d: -f1)"
UP_LINE="$(grep -n 'devcontainer up' "$LAUNCH" | grep -v '^[0-9]*:[[:space:]]*#' | head -1 | cut -d: -f1)"
assert_true "docker pull precedes the first non-comment devcontainer up" \
    "[ -n '$PULL_LINE' ] && [ -n '$UP_LINE' ] && [ '$PULL_LINE' -lt '$UP_LINE' ]"

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ]
