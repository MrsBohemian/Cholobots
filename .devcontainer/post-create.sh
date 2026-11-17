#!/usr/bin/env bash
# ABOUTME: Devcontainer post-create script: builds the container's .venv by installing the locked dependencies with uv.
# ABOUTME: Idempotent; wires git HTTPS auth through gh when GH_TOKEN is set. Never starts the bot.
set -euo pipefail

cd "$(dirname "$0")/.."

# The .venv mount is a named volume (devcontainer.json) and starts root-owned.
# Outside a devcontainer the mount point does not exist yet.
mkdir -p .venv
if [ ! -w .venv ]; then
    sudo chown "$(id -u):$(id -g)" .venv
fi

# Use the image's Python (.python-version) instead of letting uv download one.
export UV_PYTHON_DOWNLOADS=never
if ! command -v uv >/dev/null 2>&1; then
    # VS Code "Reopen in Container" builds only the base image, which has no uv.
    eval "$(scripts/devcontainer/pins.sh)"
    python3 -m pip install --user "uv==${UV_VERSION}"
    PATH="$HOME/.local/bin:$PATH"
    # Later terminals and agent commands do not inherit this script's PATH.
    sudo ln -sf "$HOME/.local/bin/uv" /usr/local/bin/uv
    [ -x "$HOME/.local/bin/uvx" ] && sudo ln -sf "$HOME/.local/bin/uvx" /usr/local/bin/uvx
fi
if [ -x .venv/bin/python ]; then
    echo "Reusing .venv"
fi
# The uv cache and the .venv volume are on different filesystems, so hardlinks fail.
UV_LINK_MODE=copy uv sync --locked

if [ -n "${GH_TOKEN:-}" ]; then
    gh auth setup-git || echo "Warning: 'gh auth setup-git' failed; git HTTPS may need manual auth." >&2
fi

echo "Post-create setup complete."
