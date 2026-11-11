#!/usr/bin/env bash
# ABOUTME: Starts a local Docker registry mirror for DinD image caching.
# ABOUTME: Optional: without the mirror, DinD pulls from the public registry.
set -euo pipefail

MIRROR_NAME="registry-mirror"
# Must match the registry-mirrors URL in .devcontainer/Dockerfile's daemon.json.
MIRROR_PORT=6000

# Exact-name match on a captured `docker ps` listing. Not `docker ps | grep -q`:
# grep closing the pipe early makes pipefail report "not found". A failing
# `docker ps` exits explicitly, since set -e is off inside an `if`.
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

if mirror_listed; then
    exit 0
fi

# Restart a stopped mirror.
if mirror_listed -a; then
    docker start "$MIRROR_NAME"
    exit 0
fi

# REGISTRY_PROXY_REMOTEURL makes it a pull-through cache of Docker Hub.
docker run -d \
    --name "$MIRROR_NAME" \
    --restart unless-stopped \
    -p "${MIRROR_PORT}:5000" \
    -v registry-mirror-cache:/var/lib/registry \
    -e REGISTRY_PROXY_REMOTEURL=https://registry-1.docker.io \
    registry:2.8.3

echo "Registry mirror started on port ${MIRROR_PORT}"
