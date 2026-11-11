# ABOUTME: Devcontainer image: Python, gh, Node 22, the Supabase CLI, psql and Chrome (amd64).
# ABOUTME: Docker-in-Docker comes from the devcontainer feature in devcontainer.json.
# Pinned by the digest of the multi-arch index behind the tag; the 3.14 must equal .python-version. To re-pin, read
# the Docker-Content-Digest header of: curl -fsSI -H 'Accept: application/vnd.oci.image.index.v1+json' \
#   https://mcr.microsoft.com/v2/devcontainers/python/manifests/2-3.14-trixie
FROM mcr.microsoft.com/devcontainers/python:2-3.14-trixie@sha256:43311767f101e1c2a7bd0ecacf2c866f5c42124fc2efe45bb384eb2995ecce4d

# The base image's Yarn apt repo has a rotated signing key that breaks `apt-get update`; nothing uses it.
RUN rm -f /etc/apt/sources.list.d/yarn.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
       lsof procps postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# GitHub CLI; GH_TOKEN comes from containerEnv in devcontainer.json.
RUN curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
        | dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
        > /etc/apt/sources.list.d/github-cli.list \
    && apt-get update && apt-get install -y --no-install-recommends gh \
    && rm -rf /var/lib/apt/lists/*

# Node.js 22: Claude Code needs >=22, and with an older Node npm silently installs an older release instead of failing.
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# Supabase CLI release .deb, checksum-verified. Keep the version equal to SUPABASE_CLI_VERSION in the Makefile.
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

# Google Chrome for chrome-devtools-mcp. amd64 only: Google publishes no arm64 deb, so Apple Silicon builds skip it.
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

# Registry mirror on the host for the DinD dockerd; if it is unreachable, Docker pulls from Docker Hub.
RUN mkdir -p /etc/docker \
    && echo '{"registry-mirrors": ["http://host.docker.internal:6000"]}' > /etc/docker/daemon.json
