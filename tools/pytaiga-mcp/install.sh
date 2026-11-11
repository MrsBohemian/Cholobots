#!/usr/bin/env bash
# ABOUTME: Installs the pinned workflow-mode pytaiga-mcp server (Alterna-Dev-Studio fork) into PYTAIGA_MCP_HOME.
# ABOUTME: The only place the commit is pinned; a marker written last makes reruns no-ops.
set -euo pipefail

PYTAIGA_MCP_REPO=https://github.com/Alterna-Dev-Studio/pytaiga-mcp.git
PYTAIGA_MCP_COMMIT=7c91b72044316f73941b24903ddb9feee193e359

HOME_DIR="${PYTAIGA_MCP_HOME:-$HOME/work/utils/pytaiga-mcp}"
MARKER="$PYTAIGA_MCP_COMMIT uv-sync-frozen"

if [ "${1:-}" = "--print-marker" ]; then
    echo "$MARKER"
    exit 0
fi

for tool in git uv; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "install.sh: $tool is required but not on PATH" >&2
        exit 1
    fi
done

# Complete means marker, venv python and server entry point all present.
if [ "$(cat "$HOME_DIR/.installed" 2>/dev/null)" = "$MARKER" ] \
    && [ -x "$HOME_DIR/.venv/bin/python" ] && [ -f "$HOME_DIR/src/server_workflow.py" ]; then
    echo "pytaiga-mcp up to date at $HOME_DIR"
    exit 0
fi

# Never clear a directory that is not ours.
if [ -d "$HOME_DIR" ] && [ -n "$(ls -A "$HOME_DIR")" ] \
    && [ ! -e "$HOME_DIR/.installed" ] && [ ! -e "$HOME_DIR/pyproject.toml" ]; then
    echo "install.sh: $HOME_DIR is not a pytaiga-mcp checkout" >&2
    exit 1
fi

mkdir -p "$HOME_DIR"
find "$HOME_DIR" -mindepth 1 -delete

git init "$HOME_DIR"
git -C "$HOME_DIR" remote add origin "$PYTAIGA_MCP_REPO"
git -C "$HOME_DIR" fetch --depth 1 origin "$PYTAIGA_MCP_COMMIT"
git -C "$HOME_DIR" checkout FETCH_HEAD

# The system python is used as is; a missing interpreter must fail, not download one.
# One fetch at a time: uv's parallel git fetches of pyTaigaClient can fail with a TLS
# "internal error" on Docker bridge networks.
UV_PYTHON_DOWNLOADS=never UV_CONCURRENT_DOWNLOADS=1 uv sync --frozen --directory "$HOME_DIR"

for required in .venv/bin/python src/server_workflow.py; do
    if [ ! -e "$HOME_DIR/$required" ]; then
        echo "install.sh: $required missing after install in $HOME_DIR" >&2
        exit 1
    fi
done

# Written last, so a failed install never looks complete.
echo "$MARKER" > "$HOME_DIR/.installed"
