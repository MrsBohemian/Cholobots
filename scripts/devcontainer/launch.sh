#!/usr/bin/env bash
# ABOUTME: Launches an isolated devcontainer, either in a shallow clone or on a git worktree.
# ABOUTME: Used by orchestrating agents to create parallel dev environments.
set -euo pipefail

usage() {
    echo "Usage: launch.sh <path> [<branch>] [--worktree [<base>]]" >&2
    exit 1
}

[ "$#" -ge 1 ] || usage
PATH_ARG="$1"
shift

# worktree-add.sh resolves a relative path under the source repo, while the
# later `cd "$PATH_ARG"` resolves it under the caller's CWD. Absolutise here
# to remove the mismatch.
case "$PATH_ARG" in
    /*) : ;;
    *) PATH_ARG="$(pwd)/$PATH_ARG" ;;
esac

BRANCH=main
BRANCH_EXPLICIT=0
WORKTREE_MODE=0
BASE=""

if [ "$#" -gt 0 ] && [ "$1" != "--worktree" ]; then
    BRANCH="$1"
    BRANCH_EXPLICIT=1
    shift
fi

if [ "$#" -gt 0 ] && [ "$1" = "--worktree" ]; then
    WORKTREE_MODE=1
    shift
    if [ "$#" -gt 0 ]; then
        BASE="$1"
        shift
    fi
fi

[ "$#" -eq 0 ] || usage

# A defaulted branch on a brand-new worktree would land the checkout straight
# on `main`, letting an agent commit there by accident. Require an explicit
# branch whenever --worktree is creating a new path.
if [ "$WORKTREE_MODE" -eq 1 ] && [ "$BRANCH_EXPLICIT" -eq 0 ] && [ ! -e "$PATH_ARG" ]; then
    echo "Error: --worktree on a new path requires an explicit <branch>" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Check prerequisites
# Worktree mode passes --mount-git-worktree-common-dir and the prebuild config
# override relies on the --config file-naming rules, both of which need this
# version. An older CLI already on PATH fails inside `devcontainer up` with an
# unknown-option error that is easy to mistake for an unrelated problem.
DEVCONTAINER_MIN_VERSION=0.84.1
if ! command -v devcontainer &>/dev/null; then
    echo "Error: devcontainer CLI not found. Install with: npm install -g @devcontainers/cli@$DEVCONTAINER_MIN_VERSION" >&2
    exit 1
fi
# Compares dot-separated numeric versions, returning 0 when $1 is strictly
# older than $2. `sort -V` would be shorter but BSD sort has no -V, and this
# script supports macOS.
version_lt() {
    local lhs rhs i left right
    IFS=. read -r -a lhs <<<"$1"
    IFS=. read -r -a rhs <<<"$2"
    for i in 0 1 2; do
        left="${lhs[i]:-0}"
        right="${rhs[i]:-0}"
        # Trim any pre-release/build suffix (e.g. "1-beta") to its leading digits.
        left="${left%%[!0-9]*}"
        right="${right%%[!0-9]*}"
        if [ "${left:-0}" -lt "${right:-0}" ]; then
            return 0
        elif [ "${left:-0}" -gt "${right:-0}" ]; then
            return 1
        fi
    done
    return 1
}
DEVCONTAINER_VERSION="$(devcontainer --version 2>/dev/null | tail -1)" || DEVCONTAINER_VERSION=""
if [ -n "$DEVCONTAINER_VERSION" ] && version_lt "$DEVCONTAINER_VERSION" "$DEVCONTAINER_MIN_VERSION"; then
    echo "Warning: devcontainer CLI $DEVCONTAINER_VERSION is older than the pinned $DEVCONTAINER_MIN_VERSION. Upgrade with: npm install -g @devcontainers/cli@$DEVCONTAINER_MIN_VERSION" >&2
fi
if ! command -v jq &>/dev/null; then
    echo "Error: jq not found. Install with: sudo apt-get install jq  (or brew install jq on macOS)" >&2
    exit 1
fi

# Token pushed into the container for GitHub access. Deliberately not the host
# GH_TOKEN: that token cannot write to MrsBohemian/Cholobots and is broader than
# a container needs. Empty means ssh-agent forwarding (or no push access).
CONTAINER_TOKEN="${CHOLOBOTS_CONTAINER_GH_TOKEN:-}"

# Maps a GitHub SSH remote URL to its HTTPS form; other URLs pass through.
to_https() {
    case "$1" in
        git@github.com:*) printf 'https://github.com/%s\n' "${1#git@github.com:}" ;;
        ssh://git@github.com/*) printf 'https://github.com/%s\n' "${1#ssh://git@github.com/}" ;;
        *) printf '%s\n' "$1" ;;
    esac
}

# Per-workspace override config directory. teardown.sh computes the identical key.
override_dir() {
    local key
    if command -v sha256sum >/dev/null 2>&1; then
        key=$(printf %s "$1" | sha256sum | cut -c1-16)
    else
        key=$(printf %s "$1" | shasum -a 256 | cut -c1-16)
    fi
    echo "${XDG_CACHE_HOME:-$HOME/.cache}/cholobots-devcontainer/$key"
}

# Create the checkout if the path doesn't exist yet: a worktree of the local
# repo's object store in worktree mode, or a shallow clone otherwise.
if [ ! -e "$PATH_ARG" ]; then
    if [ "$WORKTREE_MODE" -eq 1 ]; then
        echo "Creating worktree for $BRANCH into $PATH_ARG..." >&2
        "$SCRIPT_DIR/worktree-add.sh" "$REPO_ROOT" "$PATH_ARG" "$BRANCH" "$BASE" >&2
    else
        ORIGIN_URL="$(git -C "$REPO_ROOT" remote get-url origin 2>/dev/null || true)"
        UPSTREAM_URL="$(git -C "$REPO_ROOT" remote get-url upstream 2>/dev/null || true)"
        echo "Cloning $BRANCH into $PATH_ARG..." >&2
        mkdir -p "$(dirname "$PATH_ARG")"
        # A clone that fails partway leaves $PATH_ARG behind without the
        # cholobots-managed-clone marker below, so a retry sees the path already
        # exists and skips cloning into it. Remove the partial clone before
        # this `set -e` script exits so a retry starts clean.
        if ! git clone --depth 1 --branch "$BRANCH" "file://$REPO_ROOT" "$PATH_ARG"; then
            rm -rf "$PATH_ARG"
            exit 1
        fi
        # Point origin and upstream at GitHub so push works from inside the
        # container (pipeline branches push to upstream). SSH-style URLs become
        # HTTPS only when a container token is present: the token cannot
        # authenticate an SSH URL, while ssh-agent forwarding cannot
        # authenticate an HTTPS one.
        if [ -n "$ORIGIN_URL" ]; then
            [ -z "$CONTAINER_TOKEN" ] || ORIGIN_URL="$(to_https "$ORIGIN_URL")"
            git -C "$PATH_ARG" remote set-url origin "$ORIGIN_URL"
        fi
        if [ -n "$UPSTREAM_URL" ]; then
            [ -z "$CONTAINER_TOKEN" ] || UPSTREAM_URL="$(to_https "$UPSTREAM_URL")"
            git -C "$PATH_ARG" remote add upstream "$UPSTREAM_URL"
        fi
        # Mark this clone as one launch.sh created, so teardown.sh knows it's
        # safe to rm -rf.
        printf 'source=%s\nbranch=%s\n' "$REPO_ROOT" "$BRANCH" >"$PATH_ARG/.git/cholobots-managed-clone"
    fi
fi

# realpath (not `cd && pwd`) so a symlinked path component or a `..` left
# over from the caller's argument resolves to the same canonical path Git
# itself registers as the worktree's gitdir — see worktree-add.sh's own
# canonicalization for why a mismatch here breaks the relative .git pointer.
WS="$(realpath "$PATH_ARG")"

# Only .claude/worktrees worktrees (.git is a file) and clones this script made
# are launched. A main checkout can hold a .env with real credentials, and the
# container's post-start step rewrites .env in the workspace.
if [ -d "$WS/.git" ] && [ ! -f "$WS/.git/cholobots-managed-clone" ]; then
    echo "Error: $WS is a main checkout (not a .claude/worktrees worktree or a launch.sh clone); refusing to mount it into a container" >&2
    exit 1
fi

# Propagate host git identity into the clone for headless (non-VS Code) usage.
# VS Code's devcontainer extension forwards git config automatically, but
# headless `devcontainer up` does not. In clone mode this writes straight
# into the clone's own .git/config. Worktree mode can't do the same: .git/config
# is shared with the main checkout, and writing there would mutate the
# developer's own repo just to satisfy a container. If the identity only
# exists in the host's global ~/.gitconfig (never copied into the shared
# .git/config), the container has no way to see it, so it's propagated into
# the container's own global config after the container starts (below,
# alongside the HTTPS URL rewrite) instead.
if [ "$WORKTREE_MODE" -eq 0 ]; then
    HOST_GIT_NAME="$(git -C "$REPO_ROOT" config user.name 2>/dev/null || true)"
    HOST_GIT_EMAIL="$(git -C "$REPO_ROOT" config user.email 2>/dev/null || true)"
    if [ -n "$HOST_GIT_NAME" ]; then
        git -C "$WS" config user.name "$HOST_GIT_NAME"
    fi
    if [ -n "$HOST_GIT_EMAIL" ]; then
        git -C "$WS" config user.email "$HOST_GIT_EMAIL"
    fi
else
    HOST_GIT_NAME="$(git -C "$REPO_ROOT" config user.name 2>/dev/null || true)"
    HOST_GIT_EMAIL="$(git -C "$REPO_ROOT" config user.email 2>/dev/null || true)"
fi

# A worktree's .git is a file pointing at the shared common dir. When present
# (whether freshly created above or from a prior launch), make sure the
# pointer is relative and mount the common dir into the container.
UP_FLAGS=()
if [ -f "$WS/.git" ]; then
    "$SCRIPT_DIR/worktree-add.sh" --relativize "$WS"
    UP_FLAGS+=(--mount-git-worktree-common-dir)
fi

# Ensure registry mirror is running for DinD image caching
"$SCRIPT_DIR/start-registry-mirror.sh" >/dev/null 2>&1 || true

# Use the pre-built GHCR image if available (faster launch). Falls back to a
# local build of .devcontainer/Dockerfile. The config override is written
# outside the worktree so the checked-in devcontainer.json is never modified.
PREBUILD_IMAGE="${CHOLOBOTS_DEVCONTAINER_IMAGE:-ghcr.io/mrsbohemian/cholobots/devcontainer:latest}"
# Authenticate to GHCR (host side only) if a GitHub token is available.
# GH_TOKEN_FOR_PACKAGES is preferred (has read:packages scope);
# falls back to GH_TOKEN / GITHUB_TOKEN.
GHCR_TOKEN="${GH_TOKEN_FOR_PACKAGES:-${GH_TOKEN:-${GITHUB_TOKEN:-}}}"
if [ -n "$GHCR_TOKEN" ]; then
    if ! echo "$GHCR_TOKEN" | docker login ghcr.io -u oauth2 --password-stdin >/dev/null 2>&1; then
        echo "Warning: Failed to authenticate to ghcr.io; continuing without registry auth (pulls may be slower or rate-limited)." >&2
    fi
fi
if docker pull "$PREBUILD_IMAGE" >/dev/null 2>&1; then
    echo "Using pre-built image: $PREBUILD_IMAGE" >&2
    IMAGE="$PREBUILD_IMAGE"
else
    echo "Pre-built image not available — building locally (this will be slow)..." >&2
    # One build context for both stages: devcontainer.json's build.context,
    # resolved against .devcontainer/.
    CTX="$(jq -r '.build.context // "."' "$WS/.devcontainer/devcontainer.json")"
    case "$CTX" in
        /*) CTXDIR="$CTX" ;;
        *) CTXDIR="$(cd "$WS/.devcontainer/$CTX" && pwd)" ;;
    esac
    # Stage 1: base image from Dockerfile
    docker build -f "$WS/.devcontainer/Dockerfile" \
        -t cholobots-devcontainer-base:local "$CTXDIR" >&2
    # Stage 2: prebuild on top of base, when the checkout has one.
    if [ -f "$WS/.devcontainer/Dockerfile.prebuild" ]; then
        docker build -f "$WS/.devcontainer/Dockerfile.prebuild" \
            --build-arg BASE_IMAGE=cholobots-devcontainer-base:local \
            -t cholobots-devcontainer:local "$CTXDIR" >&2
    else
        docker tag cholobots-devcontainer-base:local cholobots-devcontainer:local
    fi
    IMAGE=cholobots-devcontainer:local
    echo "Local image built and tagged as $IMAGE" >&2
fi
# In both cases, override devcontainer.json to use the image instead of building.
# devcontainer CLI 0.84+ requires --config files to be named devcontainer.json
# or .devcontainer.json, so the override gets its own directory. The directory
# is stable per workspace and kept: the CLI derives devcontainerId (and so the
# cholobots-venv-* volume) from the config path, so a fresh temp path on every
# launch would leak a venv volume each time. teardown.sh removes it.
OVERRIDE_DIR="$(override_dir "$WS")"
mkdir -p "$OVERRIDE_DIR"
OVERRIDE_JSON="$OVERRIDE_DIR/devcontainer.json"
jq --arg img "$IMAGE" 'del(.build) | .image = $img' \
    "$WS/.devcontainer/devcontainer.json" >"$OVERRIDE_JSON"
# The lockfile sits beside the config so the docker-in-docker feature stays pinned.
if [ -f "$WS/.devcontainer/devcontainer-lock.json" ]; then
    cp "$WS/.devcontainer/devcontainer-lock.json" "$OVERRIDE_DIR/devcontainer-lock.json"
fi

# Without a token, forward the host ssh-agent socket (Linux; macOS uses the token).
SSH_AGENT_MOUNTED=0
if [ -z "$CONTAINER_TOKEN" ] && [ -S "${SSH_AUTH_SOCK:-}" ]; then
    UP_FLAGS+=(--mount "type=bind,source=$SSH_AUTH_SOCK,target=/tmp/cholobots-ssh-agent.sock")
    SSH_AGENT_MOUNTED=1
fi

# Start devcontainer (postStartCommand handles service init)
# devcontainer up may return non-zero if a lifecycle command fails, but the
# container itself may still be running and usable. GH_TOKEN is set for this
# one process only, so the container's containerEnv.GH_TOKEN receives the
# container token (or an empty value), never the host's token.
echo "Starting devcontainer..." >&2
UP_OUTPUT=$(GH_TOKEN="$CONTAINER_TOKEN" devcontainer up --workspace-folder "$WS" --config "$OVERRIDE_JSON" ${UP_FLAGS[@]+"${UP_FLAGS[@]}"} 2>&1) || true

# Extract container ID from devcontainer up JSON output.
# The output mixes build logs with a JSON result line — find the JSON and parse it.
# A failed `devcontainer up` leaves no JSON, so `grep` exits non-zero and, under
# `set -euo pipefail`, would kill the script here — before the label fallback and
# the diagnostics below that exist to explain exactly that failure.
CONTAINER_ID=$(echo "$UP_OUTPUT" | grep -o '{.*}' | tail -1 | jq -r '.containerId // empty' 2>/dev/null) || CONTAINER_ID=""
if [ -z "$CONTAINER_ID" ]; then
    # Fallback: look up container by workspace label
    CONTAINER_ID=$(docker ps -q --filter "label=devcontainer.local_folder=$WS" | head -1) || CONTAINER_ID=""
fi
if [ -z "$CONTAINER_ID" ]; then
    echo "Error: could not determine container ID" >&2
    echo "$UP_OUTPUT" >&2
    exit 1
fi

# Container credentials. The token goes over stdin and into the devcontainer
# process env only, never into an argv. gh refuses --with-token while GH_TOKEN
# is set, hence env -u. The stored login is what the skills' gh calls use.
if [ -n "$CONTAINER_TOKEN" ]; then
    printf %s "$CONTAINER_TOKEN" | docker exec -i -u vscode "$CONTAINER_ID" \
        env -u GH_TOKEN gh auth login --hostname github.com --with-token \
        >/dev/null 2>&1 || echo "Warning: could not log gh in inside the container; gh calls will fail." >&2
    docker exec -u vscode "$CONTAINER_ID" gh auth setup-git \
        >/dev/null 2>&1 || echo "Warning: could not run gh auth setup-git for vscode; git pushes over HTTPS may fail." >&2
    # A worktree keeps its scp-style origin (rewriting it would mutate the
    # shared .git/config of the main checkout), so the HTTPS rewrite happens
    # inside the container instead, for the user agents run as.
    docker exec -u vscode "$CONTAINER_ID" \
        git config --global url."https://github.com/".insteadOf "git@github.com:" \
        >/dev/null 2>&1 || echo "Warning: could not configure HTTPS git rewriting for vscode; pushes may fail." >&2
    docker exec -u vscode "$CONTAINER_ID" \
        git config --global url."https://github.com/".insteadOf "ssh://git@github.com/" \
        >/dev/null 2>&1 || echo "Warning: could not configure ssh:// git rewriting for vscode; pushes may fail." >&2
elif [ "$SSH_AGENT_MOUNTED" -eq 1 ]; then
    # IdentityAgent rather than SSH_AUTH_SOCK, because docker exec does not apply remoteEnv.
    docker exec -u vscode "$CONTAINER_ID" sh -c 'mkdir -p ~/.ssh && chmod 700 ~/.ssh && printf "Host github.com\n  IdentityAgent /tmp/cholobots-ssh-agent.sock\n  StrictHostKeyChecking accept-new\n" > ~/.ssh/config && chmod 600 ~/.ssh/config' \
        >/dev/null 2>&1 || echo "Warning: could not configure the forwarded ssh-agent for vscode; pushes may fail." >&2
else
    echo "Warning: no CHOLOBOTS_CONTAINER_GH_TOKEN and no ssh-agent; pushes from the container will fail." >&2
fi

if [ "$WORKTREE_MODE" -eq 1 ]; then
    # The shared .git/config may not carry the identity (it can live only in
    # the host's global ~/.gitconfig), so configure it for vscode's global
    # config inside the container.
    if [ -n "$HOST_GIT_NAME" ]; then
        docker exec -u vscode "$CONTAINER_ID" \
            git config --global user.name "$HOST_GIT_NAME" \
            >/dev/null 2>&1 || echo "Warning: could not configure git user.name for vscode; commits from this worktree may fail." >&2
    fi
    if [ -n "$HOST_GIT_EMAIL" ]; then
        docker exec -u vscode "$CONTAINER_ID" \
            git config --global user.email "$HOST_GIT_EMAIL" \
            >/dev/null 2>&1 || echo "Warning: could not configure git user.email for vscode; commits from this worktree may fail." >&2
    fi
fi

echo "CONTAINER_ID=$CONTAINER_ID"
