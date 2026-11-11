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

# Absolute path: worktree-add.sh would resolve a relative one under the repo
# root, everything else here under the caller's CWD.
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

# --mount-git-worktree-common-dir and the --config naming rules need this
# version; an older CLI fails inside `devcontainer up` with a misleading
# unknown-option error.
DEVCONTAINER_MIN_VERSION=0.84.1
if ! command -v devcontainer &>/dev/null; then
    echo "Error: devcontainer CLI not found. Install with: npm install -g @devcontainers/cli@$DEVCONTAINER_MIN_VERSION" >&2
    exit 1
fi
# Returns 0 when version $1 is strictly older than $2. Not `sort -V`: BSD sort
# (macOS) lacks it.
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

# Per-workspace key: names the override config directory and the fallback
# image tags. teardown.sh computes the identical key.
workspace_key() {
    if command -v sha256sum >/dev/null 2>&1; then
        printf %s "$1" | sha256sum | cut -c1-16
    else
        printf %s "$1" | shasum -a 256 | cut -c1-16
    fi
}

# Per-workspace override config directory.
override_dir() {
    echo "${XDG_CACHE_HOME:-$HOME/.cache}/cholobots-devcontainer/$(workspace_key "$1")"
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
        # Remove a partial clone, or a retry would find the path and skip cloning.
        if ! git clone --depth 1 --branch "$BRANCH" "file://$REPO_ROOT" "$PATH_ARG"; then
            rm -rf "$PATH_ARG"
            exit 1
        fi
        # Point origin and upstream at GitHub so pushes work from the container.
        # SSH URLs become HTTPS only with a container token: a token cannot
        # authenticate SSH, and the forwarded ssh-agent cannot authenticate HTTPS.
        if [ -n "$ORIGIN_URL" ]; then
            [ -z "$CONTAINER_TOKEN" ] || ORIGIN_URL="$(to_https "$ORIGIN_URL")"
            git -C "$PATH_ARG" remote set-url origin "$ORIGIN_URL"
        fi
        if [ -n "$UPSTREAM_URL" ]; then
            [ -z "$CONTAINER_TOKEN" ] || UPSTREAM_URL="$(to_https "$UPSTREAM_URL")"
            git -C "$PATH_ARG" remote add upstream "$UPSTREAM_URL"
        fi
        # teardown.sh only deletes clones carrying this marker.
        printf 'source=%s\nbranch=%s\n' "$REPO_ROOT" "$BRANCH" >"$PATH_ARG/.git/cholobots-managed-clone"
    fi
fi

# realpath resolves symlinks and `..` to the canonical path git registers for
# a worktree; the relative .git pointer breaks on a mismatch.
WS="$(realpath "$PATH_ARG")"

# Only .claude/worktrees worktrees (.git is a file) and clones this script made
# are launched. A main checkout can hold a .env with real credentials, and the
# container's post-start step rewrites .env in the workspace.
if [ -d "$WS/.git" ] && [ ! -f "$WS/.git/cholobots-managed-clone" ]; then
    echo "Error: $WS is a main checkout (not a .claude/worktrees worktree or a launch.sh clone); refusing to mount it into a container" >&2
    exit 1
fi

# Trust the .git file, not the flag: a worktree relaunched without --worktree
# still shares the main checkout's .git/config.
if [ -f "$WS/.git" ]; then
    WORKTREE_MODE=1
fi

# Headless `devcontainer up` does not forward the host git identity. A clone
# gets it in its own .git/config. A worktree shares .git/config with the main
# checkout, so its identity goes into the container's global config once the
# container is up (below).
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

# A worktree (new or existing): make its .git pointer relative and mount the
# common git dir.
UP_FLAGS=()
if [ -f "$WS/.git" ]; then
    "$SCRIPT_DIR/worktree-add.sh" --relativize "$WS"
    UP_FLAGS+=(--mount-git-worktree-common-dir)
fi

# Pull-through cache for the in-container Docker; optional.
"$SCRIPT_DIR/start-registry-mirror.sh" >/dev/null 2>&1 || true

# Prefer the pre-built GHCR image; fall back to building .devcontainer/Dockerfile.
PREBUILD_IMAGE="${CHOLOBOTS_DEVCONTAINER_IMAGE:-ghcr.io/mrsbohemian/cholobots/devcontainer:latest}"
# Host-side GHCR login only. GH_TOKEN_FOR_PACKAGES has read:packages.
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
    # Per-workspace tags, so concurrent fallback builds never retag each
    # other's image. Both stages use devcontainer.json's build.context.
    WS_KEY="$(workspace_key "$WS")"
    LOCAL_BASE="cholobots-devcontainer-base:local-$WS_KEY"
    LOCAL_IMAGE="cholobots-devcontainer:local-$WS_KEY"
    CTX="$(jq -r '.build.context // "."' "$WS/.devcontainer/devcontainer.json")"
    case "$CTX" in
        /*) CTXDIR="$CTX" ;;
        *) CTXDIR="$(cd "$WS/.devcontainer/$CTX" && pwd)" ;;
    esac
    docker build -f "$WS/.devcontainer/Dockerfile" \
        -t "$LOCAL_BASE" "$CTXDIR" >&2
    # Dockerfile.prebuild layers the agent tooling on top, when present.
    if [ -f "$WS/.devcontainer/Dockerfile.prebuild" ]; then
        docker build -f "$WS/.devcontainer/Dockerfile.prebuild" \
            --build-arg BASE_IMAGE="$LOCAL_BASE" \
            -t "$LOCAL_IMAGE" "$CTXDIR" >&2
    else
        docker tag "$LOCAL_BASE" "$LOCAL_IMAGE"
    fi
    IMAGE="$LOCAL_IMAGE"
    echo "Local image built and tagged as $IMAGE" >&2
fi
# Override config pointing at that image, outside the checkout so the tracked
# devcontainer.json is untouched. It needs its own directory (the CLI requires
# the name devcontainer.json), and that directory must be stable per workspace:
# the CLI derives the cholobots-venv-* volume name from the config path.
# teardown.sh removes it.
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

# `devcontainer up` can fail in a lifecycle command yet leave a usable
# container, so its status is ignored. GH_TOKEN is overridden for this process
# so containerEnv.GH_TOKEN gets the container token (or empty), never the host's.
echo "Starting devcontainer..." >&2
UP_OUTPUT=$(GH_TOKEN="$CONTAINER_TOKEN" devcontainer up --workspace-folder "$WS" --config "$OVERRIDE_JSON" ${UP_FLAGS[@]+"${UP_FLAGS[@]}"} 2>&1) || true

# The container id is in the JSON line among the build logs. The `||` keeps a
# failed up (no JSON) from exiting before the fallback and diagnostics below.
CONTAINER_ID=$(echo "$UP_OUTPUT" | grep -o '{.*}' | tail -1 | jq -r '.containerId // empty' 2>/dev/null) || CONTAINER_ID=""
if [ -z "$CONTAINER_ID" ]; then
    CONTAINER_ID=$(docker ps -q --filter "label=devcontainer.local_folder=$WS" | head -1) || CONTAINER_ID=""
fi
if [ -z "$CONTAINER_ID" ]; then
    echo "Error: could not determine container ID" >&2
    echo "$UP_OUTPUT" >&2
    exit 1
fi

# Container credentials. The token travels over stdin, never argv. gh refuses
# --with-token while GH_TOKEN is set, hence env -u.
if [ -n "$CONTAINER_TOKEN" ]; then
    printf %s "$CONTAINER_TOKEN" | docker exec -i -u vscode "$CONTAINER_ID" \
        env -u GH_TOKEN gh auth login --hostname github.com --with-token \
        >/dev/null 2>&1 || echo "Warning: could not log gh in inside the container; gh calls will fail." >&2
    docker exec -u vscode "$CONTAINER_ID" gh auth setup-git \
        >/dev/null 2>&1 || echo "Warning: could not run gh auth setup-git for vscode; git pushes over HTTPS may fail." >&2
    # A worktree keeps its SSH remotes (its .git/config is shared), so rewrite
    # them to HTTPS in vscode's global config. Both rules share one multivalue
    # key: clear it (exit 5 when unset is fine), then --add each.
    docker exec -u vscode "$CONTAINER_ID" \
        git config --global --unset-all url."https://github.com/".insteadOf \
        >/dev/null 2>&1 || true
    docker exec -u vscode "$CONTAINER_ID" \
        git config --global --add url."https://github.com/".insteadOf "git@github.com:" \
        >/dev/null 2>&1 || echo "Warning: could not configure HTTPS git rewriting for vscode; pushes may fail." >&2
    docker exec -u vscode "$CONTAINER_ID" \
        git config --global --add url."https://github.com/".insteadOf "ssh://git@github.com/" \
        >/dev/null 2>&1 || echo "Warning: could not configure ssh:// git rewriting for vscode; pushes may fail." >&2
elif [ "$SSH_AGENT_MOUNTED" -eq 1 ]; then
    # IdentityAgent rather than SSH_AUTH_SOCK, because docker exec does not apply remoteEnv.
    docker exec -u vscode "$CONTAINER_ID" sh -c 'mkdir -p ~/.ssh && chmod 700 ~/.ssh && printf "Host github.com\n  IdentityAgent /tmp/cholobots-ssh-agent.sock\n  StrictHostKeyChecking accept-new\n" > ~/.ssh/config && chmod 600 ~/.ssh/config' \
        >/dev/null 2>&1 || echo "Warning: could not configure the forwarded ssh-agent for vscode; pushes may fail." >&2
else
    echo "Warning: no CHOLOBOTS_CONTAINER_GH_TOKEN and no ssh-agent; pushes from the container will fail." >&2
fi

if [ "$WORKTREE_MODE" -eq 1 ]; then
    # Worktree git identity (see above).
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

echo "Devcontainer ready" >&2
echo "CONTAINER_ID=$CONTAINER_ID"
