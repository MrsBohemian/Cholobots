#!/usr/bin/env bash
# ABOUTME: Stops a devcontainer and removes its cloned directory.
# ABOUTME: Used by orchestrating agents for cleanup after work completes.
set -euo pipefail

# Per-workspace override config directory. launch.sh computes the identical key.
override_dir() {
    local key
    if command -v sha256sum >/dev/null 2>&1; then
        key=$(printf %s "$1" | sha256sum | cut -c1-16)
    else
        key=$(printf %s "$1" | shasum -a 256 | cut -c1-16)
    fi
    echo "${XDG_CACHE_HOME:-$HOME/.cache}/cholobots-devcontainer/$key"
}

WORKTREE_PATH="${1:?Usage: teardown.sh <path> [--keep-clone]}"
KEEP_CLONE=false
[[ "${2:-}" == "--keep-clone" ]] && KEEP_CLONE=true

# Physical path (pwd -P), the same resolution as launch.sh's realpath, so the
# container label and the override-dir hash match what launch.sh used.
ABS_PATH=$(cd "$WORKTREE_PATH" 2>/dev/null && pwd -P || echo "$WORKTREE_PATH")

FAILED=0
VOLUMES=()

# Find and remove all containers for this workspace. Containers are removed
# BEFORE the clone is deleted on purpose: removing the clone out from under a
# live container's bind mount is what left two containers bound to one folder
# in bug #2465. A bounded stop gives the container a moment to flush, then
# rm -f guarantees removal regardless of the stop's outcome.
# A failed `docker ps` (daemon down, permission denied) must not read as "no
# containers": the ids are captured first so the exit status is not lost to a
# process substitution, and a failure marks the teardown FAILED.
CONTAINER_IDS=()
if PS_OUT=$(docker ps -aq --filter "label=devcontainer.local_folder=$ABS_PATH" 2>/dev/null); then
    mapfile -t CONTAINER_IDS <<<"$PS_OUT"
else
    echo "Could not list containers for $ABS_PATH (is the Docker daemon reachable?); nothing was removed." >&2
    FAILED=1
fi
for CONTAINER_ID in ${CONTAINER_IDS[@]+"${CONTAINER_IDS[@]}"}; do
    [ -z "$CONTAINER_ID" ] && continue
    # Record the container's venv and DinD volumes before it is removed.
    while IFS= read -r VOLUME; do
        [[ "$VOLUME" =~ ^(cholobots-venv-|dind-var-lib-docker-) ]] && VOLUMES+=("$VOLUME")
    done < <(docker inspect -f '{{range .Mounts}}{{if eq .Type "volume"}}{{.Name}}{{"\n"}}{{end}}{{end}}' "$CONTAINER_ID" 2>/dev/null || true)
    echo "Stopping container $CONTAINER_ID..."
    docker stop -t 5 "$CONTAINER_ID" >/dev/null 2>&1 || true
    if ! docker rm -f "$CONTAINER_ID" >/dev/null; then
        echo "Failed to remove container $CONTAINER_ID" >&2
        FAILED=1
    fi
done

# Re-check: anything still bound to this folder means the teardown did not work.
# A failed re-query cannot confirm removal, so it counts as failure too.
SURVIVORS=()
if SURVIVOR_OUT=$(docker ps -aq --filter "label=devcontainer.local_folder=$ABS_PATH" 2>/dev/null); then
    mapfile -t SURVIVORS <<<"$SURVIVOR_OUT"
else
    echo "Could not re-check containers bound to $ABS_PATH after teardown; cannot confirm removal." >&2
    FAILED=1
fi
if [ "${#SURVIVORS[@]}" -gt 0 ] && [ -n "${SURVIVORS[0]}" ]; then
    echo "Containers still bound to $ABS_PATH after teardown: ${SURVIVORS[*]}" >&2
    FAILED=1
fi

# Containers are gone, so their volumes and the override config can go too. A
# volume that will not delete is a warning, not a failed teardown.
if [ "$FAILED" -eq 0 ]; then
    for VOLUME in ${VOLUMES[@]+"${VOLUMES[@]}"}; do
        docker volume rm "$VOLUME" >/dev/null 2>&1 || echo "Warning: could not remove volume $VOLUME" >&2
    done
    rm -rf "$(override_dir "$ABS_PATH")"
fi

# Remove cloned directory with safety guards. Skipped when a container is still
# bound to it: deleting a live bind mount is what this script exists to prevent.
if [ "$FAILED" -eq 0 ] && [ "$KEEP_CLONE" = false ] && [ -d "$WORKTREE_PATH" ]; then
    RESOLVED_PATH="$(realpath "$WORKTREE_PATH" 2>/dev/null || echo "$WORKTREE_PATH")"

    # Reject obviously dangerous targets
    if [ "$RESOLVED_PATH" = "/" ] || [ "$RESOLVED_PATH" = "$HOME" ]; then
        echo "Refusing to remove '$RESOLVED_PATH': unsafe target." >&2
        exit 1
    fi

    # A worktree removed with rm -rf leaves a dangling entry in its main
    # checkout; that removal belongs to `git worktree remove` there.
    if [ -f "$RESOLVED_PATH/.git" ]; then
        echo "Refusing to remove '$RESOLVED_PATH': it is a git worktree; run \`git worktree remove\` from its main checkout (or pass --keep-clone)." >&2
        exit 1
    fi
    if [ ! -f "$RESOLVED_PATH/.git/cholobots-managed-clone" ]; then
        echo "Refusing to remove '$RESOLVED_PATH': it is not a launch.sh-managed clone (no .git/cholobots-managed-clone); pass --keep-clone if this is intentional." >&2
        exit 1
    fi

    echo "Removing clone at $RESOLVED_PATH..."
    rm -rf "$RESOLVED_PATH"
fi

if [ "$FAILED" -ne 0 ]; then
    echo "Teardown incomplete." >&2
    exit 1
fi

echo "Teardown complete."
