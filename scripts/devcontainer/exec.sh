#!/usr/bin/env bash
# ABOUTME: Runs a command inside the devcontainer launched for a checkout.
# ABOUTME: The container lookup and workspace/venv setup here are mirrored in exec-skill.sh.
set -euo pipefail

WORKTREE_PATH="${1:?Usage: exec.sh <worktree-path> <command...>}"
shift

if [ "$#" -eq 0 ]; then
    echo "Usage: exec.sh <worktree-path> <command...>" >&2
    exit 1
fi

# pwd -P matches launch.sh's realpath, which is what the container label holds.
ABS_PATH=$(cd "$WORKTREE_PATH" && pwd -P)
mapfile -t CONTAINER_IDS < <(docker ps -q --filter "label=devcontainer.local_folder=$ABS_PATH")
if [ "${#CONTAINER_IDS[@]}" -eq 0 ]; then
    echo "Error: no running container found for $ABS_PATH" >&2
    exit 1
elif [ "${#CONTAINER_IDS[@]}" -gt 1 ]; then
    echo "Error: multiple containers found for $ABS_PATH (${CONTAINER_IDS[*]}); run teardown.sh first" >&2
    exit 1
fi
CONTAINER_ID="${CONTAINER_IDS[0]}"

# Workspace path in the container: a worktree is mounted relative to its main checkout, a clone
# at /workspaces/<name>, so read the mount whose source is this path.
REPO_NAME=$(basename "$ABS_PATH")
WORKSPACE=$(docker inspect --format '{{json .Mounts}}' "$CONTAINER_ID" 2>/dev/null \
    | jq -r --arg p "$ABS_PATH" '[.[] | select(.Source == $p) | .Destination][0] // empty' 2>/dev/null) || WORKSPACE=""
[ -n "$WORKSPACE" ] || WORKSPACE="/workspaces/$REPO_NAME"

# docker exec does not apply remoteEnv, so put the workspace venv first on PATH here.
# The user command travels as $1 to the inner bash so quotes and ';' in it survive.
# shellcheck disable=SC2016 # expands inside the container, not here
VENV_PREFIX='if [ -x .venv/bin/python ]; then PATH="$PWD/.venv/bin:$PATH"; export PATH; fi; '
docker exec -u vscode -w "$WORKSPACE" "$CONTAINER_ID" bash -lc "${VENV_PREFIX}"'eval "$1"' _ "$*"
