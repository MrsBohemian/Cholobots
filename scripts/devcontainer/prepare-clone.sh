#!/usr/bin/env bash
# ABOUTME: Prepares a clone directory for in-container claude agent sessions.
# ABOUTME: Writes settings.local.json (permissions) and .mcp.json (native MCP servers).
set -euo pipefail

CLONE_PATH="${1:?Usage: prepare-clone.sh <clone-path>}"

if [ ! -d "$CLONE_PATH" ]; then
    echo "Error: clone path does not exist: $CLONE_PATH" >&2
    exit 1
fi

ABS_CLONE_PATH=$(cd "$CLONE_PATH" && pwd)

# Ensure .claude directory exists
mkdir -p "$ABS_CLONE_PATH/.claude"

# Write .claude/settings.local.json with permissions and one PreToolUse hook.
# Agents run inside the container so all tools operate on the container filesystem;
# the hook blocks pkill/pgrep -f, which self-match the agent's own Bash shell.
# The heredoc is unquoted so $ABS_CLONE_PATH interpolates into the hook command; every other
# dollar sign in the body must stay escaped as \$ to survive into the JSON.
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

# The browser path comes from the clone's own Dockerfile so .mcp.json cannot
# drift from the image. prepare-clone.sh runs on the host, so the container's
# CHROME_PATH env is not visible here; the ENV line is the source of truth.
DOCKERFILE="$ABS_CLONE_PATH/.devcontainer/Dockerfile"
CHROME_PATH=$(sed -n 's/^ENV CHROME_PATH=//p' "$DOCKERFILE" 2>/dev/null | tail -1)
if [ -z "$CHROME_PATH" ]; then
    echo "Error: no 'ENV CHROME_PATH=' line found in $DOCKERFILE" >&2
    exit 1
fi

# The in-image taiga-py workflow server lives under PYTAIGA_MCP_HOME, set in
# Dockerfile.prebuild (#74). Until that file exists the default location is used.
PREBUILD="$ABS_CLONE_PATH/.devcontainer/Dockerfile.prebuild"
PYTAIGA_HOME=""
if [ -f "$PREBUILD" ]; then
    PYTAIGA_HOME=$(sed -n 's/^ENV PYTAIGA_MCP_HOME=//p' "$PREBUILD" | tail -1)
fi
if [ -z "$PYTAIGA_HOME" ]; then
    PYTAIGA_HOME="/opt/pytaiga-mcp"
    if [ ! -f "$PREBUILD" ]; then
        echo "Note: .devcontainer/Dockerfile.prebuild not found; taiga-py expects pytaiga-mcp at /opt/pytaiga-mcp (#74)." >&2
    fi
fi

# Write .mcp.json for native in-container MCP servers.
# chrome-devtools runs directly (npm install -g in the image); its --executablePath
# is derived from CHROME_PATH in the Dockerfile (see above).
# taiga-py runs the in-image workflow server and inherits TAIGA_API_URL, TAIGA_USERNAME
# and TAIGA_PASSWORD from the container environment; no credentials are written here.
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
