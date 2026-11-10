#!/usr/bin/env bash
# ABOUTME: Behavioural test for launch.sh credentials, remotes, main-checkout refusal, image selection
# ABOUTME: and stable override config, using stubbed docker and devcontainer (no daemon is reached).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

export GIT_CONFIG_GLOBAL=/dev/null
export GIT_CONFIG_SYSTEM=/dev/null

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

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

STUB_BIN="$TMP/bin"
mkdir -p "$STUB_BIN"

cat >"$STUB_BIN/devcontainer" <<'STUB'
#!/usr/bin/env bash
if [ "${1:-}" = "--version" ]; then
    echo "0.89.0"
    exit 0
fi
echo "up $* GH_TOKEN=${GH_TOKEN-}" >>"$STUB_LOG_DIR/up.log"
echo '{"outcome":"success","containerId":"cid"}'
STUB
chmod +x "$STUB_BIN/devcontainer"

cat >"$STUB_BIN/docker" <<'STUB'
#!/usr/bin/env bash
stdin=""
case " $* " in
    *" -i "* | *--password-stdin*) stdin="$(cat)" ;;
esac
echo "docker $* <<stdin:$stdin>>" >>"$STUB_LOG_DIR/docker.log"
# Emulate the container's global git config in a file, so tests can assert resulting state.
case "$*" in
    *"git config --global "*"url."*)
        args=("$@")
        for i in "${!args[@]}"; do
            if [ "${args[$i]}" = "--global" ]; then
                unset 'args[i]'
                args=("${args[@]}")
                break
            fi
        done
        # args: exec -u vscode cid git config <rest>; rebuild as git config --file F <rest>
        for i in "${!args[@]}"; do
            if [ "${args[$i]}" = "config" ]; then
                git config --file "$STUB_LOG_DIR/containergitconfig" "${args[@]:$((i + 1))}"
                exit $?
            fi
        done
        ;;
esac
if [ "${1:-}" = "pull" ]; then
    exit "${STUB_PULL_EXIT:-0}"
fi
exit 0
STUB
chmod +x "$STUB_BIN/docker"

export PATH="$STUB_BIN:$PATH"
export STUB_LOG_DIR="$TMP/logs"
mkdir -p "$STUB_LOG_DIR"
[ "$(command -v docker)" = "$STUB_BIN/docker" ] || { echo "FATAL: docker is not the stub"; exit 1; }
[ "$(command -v devcontainer)" = "$STUB_BIN/devcontainer" ] || { echo "FATAL: devcontainer is not the stub"; exit 1; }

export XDG_CACHE_HOME="$TMP/cache"
unset GH_TOKEN GITHUB_TOKEN GH_TOKEN_FOR_PACKAGES CHOLOBOTS_CONTAINER_GH_TOKEN SSH_AUTH_SOCK \
    CHOLOBOTS_DEVCONTAINER_IMAGE STUB_PULL_EXIT || true

# Sandbox source repo holding a copy of the scripts, so launch.sh's REPO_ROOT is the sandbox.
SANDBOX="$TMP/sandbox"
git init -q -b main "$SANDBOX"
mkdir -p "$SANDBOX/scripts" "$SANDBOX/.devcontainer"
cp -r "$REPO_ROOT/scripts/devcontainer" "$SANDBOX/scripts/devcontainer"
rm -rf "$SANDBOX/scripts/devcontainer/__pycache__"
echo '{"name":"t","build":{"dockerfile":"Dockerfile","context":".."}}' >"$SANDBOX/.devcontainer/devcontainer.json"
echo '{"features":{}}' >"$SANDBOX/.devcontainer/devcontainer-lock.json"
echo 'FROM scratch' >"$SANDBOX/.devcontainer/Dockerfile"
git -C "$SANDBOX" add -A
git -C "$SANDBOX" -c user.email=t@t -c user.name=t commit -q -m init
git -C "$SANDBOX" remote add origin git@github.com:o/r.git
git -C "$SANDBOX" remote add upstream git@github.com:MrsBohemian/Cholobots.git
LAUNCH="$SANDBOX/scripts/devcontainer/launch.sh"

reset_logs() { rm -f "$STUB_LOG_DIR"/*.log; }
launch() { # path [args...]; stdout to $TMP/out, stderr to $TMP/err
    "$LAUNCH" "$@" >"$TMP/out" 2>"$TMP/err" </dev/null
}

echo "=== launch.sh git auth, remotes, override config ==="

# 1. Container token: only it reaches `devcontainer up`; the host token stays host-side.
reset_logs
GH_TOKEN=hosttoken CHOLOBOTS_CONTAINER_GH_TOKEN=ctoken launch "$TMP/wt1" feature/one --worktree main || true
assert_true "1: up sees the container token" "grep -q 'GH_TOKEN=ctoken\$' \"\$STUB_LOG_DIR/up.log\""
assert_true "1: up never sees the host token" "! grep -q hosttoken \"\$STUB_LOG_DIR/up.log\""
assert_true "1: gh auth login gets the token on stdin with GH_TOKEN unset" \
    "grep -qF 'exec -i -u vscode cid env -u GH_TOKEN gh auth login --hostname github.com --with-token <<stdin:ctoken>>' \"\$STUB_LOG_DIR/docker.log\""
assert_true "1: gh auth setup-git runs" "grep -qF 'exec -u vscode cid gh auth setup-git' \"\$STUB_LOG_DIR/docker.log\""
assert_true "1: both insteadOf rewrites are configured" \
    "grep -qF 'insteadOf git@github.com:' \"\$STUB_LOG_DIR/docker.log\" && grep -qF 'insteadOf ssh://git@github.com/' \"\$STUB_LOG_DIR/docker.log\""
assert_true "1: both insteadOf values survive in the resulting git config" \
    "[ \"\$(git config --file \"\$STUB_LOG_DIR/containergitconfig\" --get-all url.https://github.com/.insteadOf | sort | tr '\\n' ' ')\" = 'git@github.com: ssh://git@github.com/ ' ]"
GH_TOKEN=hosttoken CHOLOBOTS_CONTAINER_GH_TOKEN=ctoken launch "$TMP/wt1" feature/one --worktree main || true
assert_true "1: relaunch does not duplicate insteadOf values" \
    "[ \"\$(git config --file \"\$STUB_LOG_DIR/containergitconfig\" --get-all url.https://github.com/.insteadOf | wc -l)\" -eq 2 ]"
assert_true "1: host token appears only on the ghcr login line" \
    "[ -z \"\$(grep hosttoken \"\$STUB_LOG_DIR/docker.log\" | grep -v 'login ghcr.io')\" ]"
assert_true "1: container token appears in no argv" \
    "! grep -v 'gh auth login' \"\$STUB_LOG_DIR/docker.log\" | grep -q ctoken"
assert_true "1: stdout is exactly CONTAINER_ID=cid" "[ \"\$(cat \"$TMP/out\")\" = 'CONTAINER_ID=cid' ]"

# 2. Host token only: it is not forwarded, and the user is warned.
reset_logs
GH_TOKEN=hosttoken launch "$TMP/wt2" feature/two --worktree main || true
assert_true "2: up sees an empty GH_TOKEN" "grep -q 'GH_TOKEN=\$' \"\$STUB_LOG_DIR/up.log\""
assert_true "2: no gh auth login" "! grep -q 'gh auth login' \"\$STUB_LOG_DIR/docker.log\""
assert_true "2: warns about the missing container token" "grep -q 'no CHOLOBOTS_CONTAINER_GH_TOKEN' \"$TMP/err\""

# 3. No token but an ssh-agent socket: it is mounted and configured as IdentityAgent.
reset_logs
SOCK="$TMP/agent.sock"
python3 -c 'import socket,sys; s=socket.socket(socket.AF_UNIX); s.bind(sys.argv[1])' "$SOCK"
SSH_AUTH_SOCK="$SOCK" launch "$TMP/wt3" feature/three --worktree main || true
assert_true "3: up mounts the agent socket" \
    "grep -qF -- '--mount type=bind,source=$SOCK,target=/tmp/cholobots-ssh-agent.sock' \"\$STUB_LOG_DIR/up.log\""
assert_true "3: ssh config uses IdentityAgent" "grep -qF 'IdentityAgent /tmp/cholobots-ssh-agent.sock' \"\$STUB_LOG_DIR/docker.log\""
assert_true "3: no gh auth login" "! grep -q 'gh auth login' \"\$STUB_LOG_DIR/docker.log\""

# 4. Clone mode: both remotes, HTTPS only with a token, marker file.
reset_logs
CHOLOBOTS_CONTAINER_GH_TOKEN=ctoken launch "$TMP/clone-tok" main || true
assert_true "4: origin is https with a token" \
    "[ \"\$(git -C \"$TMP/clone-tok\" remote get-url origin)\" = https://github.com/o/r.git ]"
assert_true "4: upstream is https with a token" \
    "[ \"\$(git -C \"$TMP/clone-tok\" remote get-url upstream)\" = https://github.com/MrsBohemian/Cholobots.git ]"
assert_true "4: marker holds source and branch" \
    "grep -q '^source=' \"$TMP/clone-tok/.git/cholobots-managed-clone\" && grep -q '^branch=main\$' \"$TMP/clone-tok/.git/cholobots-managed-clone\""
reset_logs
launch "$TMP/clone-ssh" main || true
assert_true "4: origin stays ssh without a token" \
    "[ \"\$(git -C \"$TMP/clone-ssh\" remote get-url origin)\" = git@github.com:o/r.git ]"
assert_true "4: upstream stays ssh without a token" \
    "[ \"\$(git -C \"$TMP/clone-ssh\" remote get-url upstream)\" = git@github.com:MrsBohemian/Cholobots.git ]"

# 5. A main checkout is refused before `devcontainer up`.
reset_logs
EXIT_5=0
launch "$SANDBOX" || EXIT_5=$?
assert_true "5: exit 1 on a main checkout" "[ \"$EXIT_5\" -eq 1 ]"
assert_true "5: names the main checkout" "grep -q 'main checkout' \"$TMP/err\""
assert_true "5: devcontainer up was never called" "[ ! -e \"\$STUB_LOG_DIR/up.log\" ]"

# 6. Override config: stable path, no build key, image set, lockfile copied, reused on relaunch.
reset_logs
launch "$TMP/wt1" feature/one --worktree main || true
OVERRIDE="$(grep -o -- "--config [^ ]*" "$STUB_LOG_DIR/up.log" | head -1 | cut -d' ' -f2)"
assert_true "6: override path is under the cache dir with a 16-hex key" \
    "[[ \"$OVERRIDE\" =~ ^$TMP/cache/cholobots-devcontainer/[0-9a-f]{16}/devcontainer.json\$ ]]"
assert_true "6: override has no build key" "[ \"\$(jq 'has(\"build\")' \"$OVERRIDE\")\" = false ]"
assert_true "6: override has an image" "[ -n \"\$(jq -r '.image // empty' \"$OVERRIDE\")\" ]"
assert_true "6: lockfile copied beside the override" "[ -f \"$(dirname "$OVERRIDE")/devcontainer-lock.json\" ]"
reset_logs
launch "$TMP/wt1" feature/one --worktree main || true
assert_true "6: a second launch reuses the same path" "grep -qF -- \"--config $OVERRIDE\" \"\$STUB_LOG_DIR/up.log\""
assert_true "6: the override is kept after launch" "[ -f \"$OVERRIDE\" ]"

# 7. A failed pull builds locally and uses a per-workspace local tag (concurrent
# launches must not share a tag, or one build retags the other's image).
reset_logs
STUB_PULL_EXIT=1 launch "$TMP/wt7" feature/seven --worktree main || true
OVERRIDE7="$(grep -o -- "--config [^ ]*" "$STUB_LOG_DIR/up.log" | head -1 | cut -d' ' -f2)"
KEY7="$(basename "$(dirname "$OVERRIDE7")")"
BASE7="cholobots-devcontainer-base:local-$KEY7"
IMG7="cholobots-devcontainer:local-$KEY7"
assert_true "7: base image is built from the Dockerfile with the devcontainer.json context" \
    "grep -qE 'docker build -f .*/\.devcontainer/Dockerfile -t $BASE7 $TMP/wt7( |<)' \"\$STUB_LOG_DIR/docker.log\""
assert_true "7: base is tagged as the final image when there is no prebuild stage" \
    "grep -qF 'docker tag $BASE7 $IMG7' \"\$STUB_LOG_DIR/docker.log\""
assert_true "7: override image is the per-workspace local tag" "[ \"\$(jq -r .image \"$OVERRIDE7\")\" = $IMG7 ]"
assert_true "7: no unsuffixed shared local tag is used" "! grep -qE ':local( |\$)' \"\$STUB_LOG_DIR/docker.log\""
assert_true "7: no second build without Dockerfile.prebuild" "[ \"\$(grep -c 'docker build' \"\$STUB_LOG_DIR/docker.log\")\" -eq 1 ]"

reset_logs
echo 'ARG BASE_IMAGE' >"$TMP/wt7/.devcontainer/Dockerfile.prebuild"
STUB_PULL_EXIT=1 launch "$TMP/wt7" feature/seven --worktree main || true
assert_true "7: prebuild stage gets BASE_IMAGE of the local base" \
    "grep -qF -- '--build-arg BASE_IMAGE=$BASE7 -t $IMG7' \"\$STUB_LOG_DIR/docker.log\""
assert_true "7: no tag step when the prebuild stage ran" "! grep -q 'docker tag' \"\$STUB_LOG_DIR/docker.log\""

# 7b. A second workspace falling back gets different tags from the first.
reset_logs
STUB_PULL_EXIT=1 launch "$TMP/wt1" feature/one --worktree main || true
OVERRIDE7B="$(grep -o -- "--config [^ ]*" "$STUB_LOG_DIR/up.log" | head -1 | cut -d' ' -f2)"
assert_true "7b: a different workspace gets a different local image tag" \
    "[ \"\$(jq -r .image \"$OVERRIDE7B\")\" != $IMG7 ] && jq -r .image \"$OVERRIDE7B\" | grep -q '^cholobots-devcontainer:local-'"

# 8. A successful pull uses the published image name.
reset_logs
launch "$TMP/wt1" feature/one --worktree main || true
assert_true "8: published image is the override image" \
    "[ \"\$(jq -r .image \"$OVERRIDE\")\" = ghcr.io/mrsbohemian/cholobots/devcontainer:latest ]"

# 9. The host's shared git config is never written, in any mode. The identity
# lives only in a global config file here, so a write into the sandbox's
# .git/config (shared with every worktree) would be an observable mutation.
HOSTCFG="$TMP/host-identity.gitconfig"
printf '[user]\n\tname = Host Person\n\temail = host@example.com\n' >"$HOSTCFG"
sandbox_cfg_sum() { sha256sum "$SANDBOX/.git/config" | cut -d' ' -f1; }
launch_with_identity() { GIT_CONFIG_GLOBAL="$HOSTCFG" launch "$@" || true; }

reset_logs
launch_with_identity "$TMP/wt9" feature/nine --worktree main
assert_true "9: new worktree launch leaves identity out of the shared config" \
    "! git -C \"$SANDBOX\" config --local --get user.name && ! git -C \"$SANDBOX\" config --local --get user.email"
assert_true "9: new worktree launch is a worktree (.git is a file)" "[ -f \"$TMP/wt9/.git\" ]"
assert_true "9: worktree identity reaches the container's global config instead" \
    "grep -qF 'git config --global user.name Host Person' \"\$STUB_LOG_DIR/docker.log\""

AFTER_FIRST9="$(sandbox_cfg_sum)"
reset_logs
launch_with_identity "$TMP/wt9" feature/nine --worktree main
assert_true "9: relaunch with --worktree leaves the shared config byte-identical" \
    "[ \"\$(sandbox_cfg_sum)\" = \"$AFTER_FIRST9\" ]"

# The mode-mismatch case: an existing worktree launched WITHOUT --worktree must
# be detected from its .git file, not trusted to the flag.
reset_logs
launch_with_identity "$TMP/wt9"
assert_true "9: relaunch of an existing worktree without --worktree leaves the shared config byte-identical" \
    "[ \"\$(sandbox_cfg_sum)\" = \"$AFTER_FIRST9\" ]"
assert_true "9: ... and no identity landed in the shared config" \
    "! git -C \"$SANDBOX\" config --local --get user.name && ! git -C \"$SANDBOX\" config --local --get user.email"
assert_true "9: ... and the identity went to the container's global config" \
    "grep -qF 'git config --global user.name Host Person' \"\$STUB_LOG_DIR/docker.log\""

# Clone mode writes only to the clone's own config.
reset_logs
launch_with_identity "$TMP/clone9" main
assert_true "9: clone launch leaves the source repo's config free of identity" \
    "! git -C \"$SANDBOX\" config --local --get user.name"
assert_true "9: clone launch writes the identity into the clone's own config" \
    "[ \"\$(git -C \"$TMP/clone9\" config --local --get user.name)\" = 'Host Person' ]"

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ]
