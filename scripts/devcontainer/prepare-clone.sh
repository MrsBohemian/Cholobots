#!/usr/bin/env bash
# ABOUTME: Prepares a launched checkout for in-container agent sessions.
# ABOUTME: Writes .claude/settings.local.json (permissions, pgrep hook) and .mcp.json (MCP servers).
set -euo pipefail

CLONE_PATH="${1:?Usage: prepare-clone.sh <clone-path>}"

if [ ! -d "$CLONE_PATH" ]; then
    echo "Error: clone path does not exist: $CLONE_PATH" >&2
    exit 1
fi

ABS_CLONE_PATH=$(cd "$CLONE_PATH" && pwd)

mkdir -p "$ABS_CLONE_PATH/.claude"

# The hook blocks pkill/pgrep -f, which match the agent's own Bash shell.
# The heredoc is unquoted so $ABS_CLONE_PATH interpolates; escape every other dollar sign as \$.
cat > "$ABS_CLONE_PATH/.claude/settings.local.json" <<SETTINGS
{
  "permissions": {
    "allow": [
      "Bash(python3 *)",
      "Bash(python -m pytest *)",
      "Bash(python -m py_compile *)",
      "Bash(supabase *)",
      "Bash(scripts/*)",
      "Bash(bash tests/scripts/*)",
      "Bash(git *)",
      "Bash(env -u GH_TOKEN gh *)",
      "Bash(cat *)",
      "Bash(ls *)",
      "Bash(mkdir *)",
      "Bash(pwd)",
      "Bash(echo *)",
      "Bash(jq *)",
      "mcp__taiga-py__*",
      "mcp__chrome-devtools__*",
      "Read(*)",
      "Write(*)",
      "Edit(*)",
      "Edit(.claude/skills/**)",
      "Grep(*)",
      "Glob(*)",
      "Agent(*)",
      "WebFetch(domain:github.com)",
      "WebFetch(domain:api.github.com)"
    ]
  },
  "enabledMcpjsonServers": [
    "taiga-py",
    "chrome-devtools"
  ],
  "hooks": {
    "PreToolUse": [
      { "matcher": "Bash",
        "hooks": [ { "type": "command", "command": "bash \"\${CLAUDE_PROJECT_DIR:-$ABS_CLONE_PATH}/scripts/hooks/reject-self-matching-pgrep.sh\"" } ] }
    ]
  }
}
SETTINGS

# This runs on the host, so read the browser path from the Dockerfile's ENV line
# rather than the container's environment.
DOCKERFILE="$ABS_CLONE_PATH/.devcontainer/Dockerfile"
CHROME_PATH=$(sed -n 's/^ENV CHROME_PATH=//p' "$DOCKERFILE" 2>/dev/null | tail -1)
if [ -z "$CHROME_PATH" ]; then
    echo "Error: no 'ENV CHROME_PATH=' line found in $DOCKERFILE" >&2
    exit 1
fi

# The taiga-py server lives under PYTAIGA_MCP_HOME from Dockerfile.prebuild,
# else /opt/pytaiga-mcp.
PREBUILD="$ABS_CLONE_PATH/.devcontainer/Dockerfile.prebuild"
PYTAIGA_HOME=""
if [ -f "$PREBUILD" ]; then
    PYTAIGA_HOME=$(sed -n 's/^ENV PYTAIGA_MCP_HOME=\([^[:space:]]*\).*/\1/p' "$PREBUILD" | tail -1)
fi
if [ -z "$PYTAIGA_HOME" ]; then
    PYTAIGA_HOME="/opt/pytaiga-mcp"
    if [ ! -f "$PREBUILD" ]; then
        echo "Note: .devcontainer/Dockerfile.prebuild not found; taiga-py expects pytaiga-mcp at /opt/pytaiga-mcp (#74)." >&2
    fi
fi

# taiga-py reads TAIGA_API_URL, TAIGA_USERNAME and TAIGA_PASSWORD from the container
# environment; no credentials are written to this file.
cat > "$ABS_CLONE_PATH/.mcp.json" <<MCPJSON
{
  "mcpServers": {
    "chrome-devtools": {
      "type": "stdio",
      "command": "chrome-devtools-mcp",
      "args": [
        "--headless",
        "--isolated",
        "--executablePath", "$CHROME_PATH",
        "--chrome-arg=--no-sandbox",
        "--chrome-arg=--disable-gpu",
        "--chrome-arg=--disable-dev-shm-usage",
        "--no-usage-statistics"
      ]
    },
    "taiga-py": {
      "type": "stdio",
      "command": "$PYTAIGA_HOME/.venv/bin/python",
      "args": ["$PYTAIGA_HOME/src/server_workflow.py"],
      "env": {"TAIGA_TRANSPORT": "stdio"}
    }
  }
}
MCPJSON

# Hide our .mcp.json overwrite from git when the repo tracks one, and warn about any other
# modified tracked file. Never restore them: on a relaunched worktree they are the agent's work.
cd "$ABS_CLONE_PATH"
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    if git ls-files --error-unmatch .mcp.json >/dev/null 2>&1; then
        git update-index --assume-unchanged .mcp.json
    fi
    DIRTY=$(git status --porcelain --untracked-files=no)
    if [ -n "$DIRTY" ]; then
        {
            echo "Warning: tracked files are modified in $ABS_CLONE_PATH:"
            echo "$DIRTY"
        } >&2
    fi
fi

echo "Clone prepared at $ABS_CLONE_PATH"
