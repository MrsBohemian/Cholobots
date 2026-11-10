# ABOUTME: Devcontainer base image with stable system packages only.
# ABOUTME: Volatile tool installs (claude, codex, chrome-devtools-mcp, pytaiga-mcp) live in Dockerfile.prebuild (#74).
# Base image pinned by the digest of the multi-arch index behind the 2-3.14-trixie tag.
# Obtained 2026-11-10 with: curl -fsSI -H 'Accept: application/vnd.oci.image.index.v1+json' \
#   https://mcr.microsoft.com/v2/devcontainers/python/manifests/2-3.14-trixie  (Docker-Content-Digest header).
# Built 2025-10-17 (amd64 and arm64 image configs), so it is older than two weeks. The 3.14 must equal .python-version.
FROM mcr.microsoft.com/devcontainers/python:2-3.14-trixie@sha256:43311767f101e1c2a7bd0ecacf2c866f5c42124fc2efe45bb384eb2995ecce4d

# The pinned base image still lists the Yarn apt repo with a signing key that has since rotated, which makes
# every `apt-get update` fail. Nothing here installs from it, so drop the source list.
RUN rm -f /etc/apt/sources.list.d/yarn.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
       lsof procps postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# GitHub CLI (gh) -- agents use it for PR creation, monitoring, and comments.
# GH_TOKEN is passed via containerEnv in devcontainer.json.
RUN curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
        | dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
        > /etc/apt/sources.list.d/github-cli.list \
    && apt-get update && apt-get install -y --no-install-recommends gh \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js 22 (needed for the npm global tools).
# Claude Code's engines floor is Node >=22; with an older Node, npm silently
# installs the newest release that still supports it instead of failing.
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# Supabase CLI from the release .deb, pinned to the version the Makefile uses (#47, #50).
# Verified against the sha256 line for this asset in the release's checksums.txt.
# Release v2.121.0 was published 2026-10-14.
ARG SUPABASE_CLI_VERSION=2.121.0
RUN arch="$(dpkg --print-architecture)" \
    && deb="supabase_${SUPABASE_CLI_VERSION}_linux_${arch}.deb" \
    && base="https://github.com/supabase/cli/releases/download/v${SUPABASE_CLI_VERSION}" \
    && cd /tmp \
    && curl -fsSLO "$base/$deb" \
    && curl -fsSL "$base/checksums.txt" -o checksums.txt \
    && grep " ${deb}\$" checksums.txt | sha256sum -c - \
    && apt-get update && apt-get install -y ./"$deb" \
    && rm -rf /var/lib/apt/lists/* "$deb" checksums.txt \
    && supabase --version

# CHROME_PATH is the single source of truth for the MCP browser binary:
# prepare-clone.sh parses this exact line to fill --executablePath in .mcp.json.
ENV CHROME_PATH=/usr/bin/google-chrome-stable

# Google Chrome -- chrome-devtools-mcp drives it for agent UI testing of the dashboards.
# Google's deb ships a real binary and pulls its own runtime dependencies.
# amd64 only: Google publishes no arm64 Chrome deb, so the image still builds on Apple Silicon without it.
RUN if [ "$(dpkg --print-architecture)" = amd64 ]; then \
        curl -fsSL https://dl.google.com/linux/linux_signing_key.pub \
            | gpg --dearmor > /usr/share/keyrings/google-chrome-keyring.gpg \
        && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome-keyring.gpg] https://dl.google.com/linux/chrome/deb/ stable main" \
            > /etc/apt/sources.list.d/google-chrome.list \
        && apt-get update && apt-get install -y --no-install-recommends google-chrome-stable \
        && rm -rf /var/lib/apt/lists/* \
        && "$CHROME_PATH" --version; \
    else \
        echo "Skipping Google Chrome: no arm64 build"; \
    fi

# Configure DinD to use a host-side registry mirror for faster image pulls.
# The DinD feature (installed after this Dockerfile) starts dockerd, which reads
# /etc/docker/daemon.json automatically. If the mirror is unreachable, Docker
# falls back to pulling from the public registry.
RUN mkdir -p /etc/docker \
    && echo '{"registry-mirrors": ["http://host.docker.internal:6000"]}' > /etc/docker/daemon.json
