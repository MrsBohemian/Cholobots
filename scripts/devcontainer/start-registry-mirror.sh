#!/usr/bin/env bash
# ABOUTME: Starts a local Docker registry mirror for DinD image caching.
# ABOUTME: Transparent fallback — if mirror fails, DinD pulls from public registry.
set -euo pipefail

MIRROR_NAME="registry-mirror"
# Must match the registry-mirrors URL in .devcontainer/Dockerfile's daemon.json.
MIRROR_PORT=6000

# Exact-name match against a captured `docker ps` listing. Capturing first keeps
# the match out of a pipeline: `docker ps | grep -q` can close the pipe on the
# first hit, and under pipefail the SIGPIPE'd `docker ps` then reads as "not
# found" and this script would start or create a duplicate mirror. A failing
# `docker ps` aborts the script (set -e is off inside an `if` condition, hence
# the explicit exit) instead of being treated as "no mirror".
mirror_listed() {
    local names
    names="$(docker ps "$@" --format '{{.Names}}')" || {
        echo "Error: docker ps failed; not starting a registry mirror" >&2
        exit 1
    }
    case $'\n'"$names"$'\n' in
        *$'\n'"$MIRROR_NAME"$'\n'*) return 0 ;;
        *) return 1 ;;
    esac
}

# Check if already running
if mirror_listed; then
    exit 0
fi

# Check if stopped container exists, restart it
if mirror_listed -a; then
    docker start "$MIRROR_NAME"
    exit 0
fi

# Create new mirror with persistent cache volume.
# REGISTRY_PROXY_REMOTEURL configures registry as a pull-through cache for
# Docker Hub. Without this, the container is a plain registry and won't cache
# upstream pulls.
docker run -d \
    --name "$MIRROR_NAME" \
    --restart unless-stopped \
    -p "${MIRROR_PORT}:5000" \
    -v registry-mirror-cache:/var/lib/registry \
    -e REGISTRY_PROXY_REMOTEURL=https://registry-1.docker.io \
    registry:2.8.3

echo "Registry mirror started on port ${MIRROR_PORT}"
