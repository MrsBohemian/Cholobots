#!/usr/bin/env bash
# ABOUTME: Devcontainer post-create script: builds the container's .venv and installs the pinned requirements.
# ABOUTME: Idempotent; wires git HTTPS auth through gh when GH_TOKEN is set. Never starts the bot.
set -euo pipefail

cd "$(dirname "$0")/.."

# The .venv mount is a named volume (devcontainer.json) and starts root-owned.
# Outside a devcontainer the mount point does not exist yet.
mkdir -p .venv
if [ ! -w .venv ]; then
    sudo chown "$(id -u):$(id -g)" .venv
fi

if [ ! -x .venv/bin/python ]; then
    python3 -m venv .venv
else
    echo "Reusing .venv"
fi
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt -r requirements-dev.txt

if [ -n "${GH_TOKEN:-}" ]; then
    gh auth setup-git || echo "Warning: 'gh auth setup-git' failed; git HTTPS may need manual auth." >&2
fi

echo "Post-create setup complete."
