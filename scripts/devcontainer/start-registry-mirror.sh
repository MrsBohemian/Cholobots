#!/usr/bin/env bash
# ABOUTME: Starts a local Docker registry mirror for DinD image caching.
# ABOUTME: Transparent fallback — if mirror fails, DinD pulls from public registry.
set -euo pipefail

MIRROR_NAME="registry-mirror"
# Must match the registry-mirrors URL in .devcontainer/Dockerfile's daemon.json.
MIRROR_PORT=6000

# Check if already running
if docker ps --format '{{.Names}}' | grep -q "^${MIRROR_NAME}$"; then
    exit 0
fi

# Check if stopped container exists, restart it
if docker ps -a --format '{{.Names}}' | grep -q "^${MIRROR_NAME}$"; then
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
