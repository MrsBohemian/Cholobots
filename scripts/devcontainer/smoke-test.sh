#!/usr/bin/env bash
# ABOUTME: Smoke test for devcontainer setup: launches a clone, verifies the Cholobots toolset and the
# ABOUTME: in-container Supabase stack, then tears down. Needs a reachable Docker daemon.
# shellcheck disable=SC2016 # remote commands are single-quoted on purpose: the container shell expands them
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

docker info >/dev/null 2>&1 || { echo "Error: Docker daemon not reachable" >&2; exit 1; }

# Reserve a unique path; launch.sh clones into it.
WORKTREE_PATH="$(mktemp -d "${TMPDIR:-/tmp}/devcontainer-smoke-test-XXXXXX")"
rmdir "$WORKTREE_PATH"
FAILURES=0

# launch.sh clones a named branch, so a detached HEAD gets a temporary branch,
# deleted on exit.
SMOKE_BRANCH="$(git -C "$REPO_ROOT" symbolic-ref --short -q HEAD || true)"
TEMP_BRANCH=""
if [ -z "$SMOKE_BRANCH" ]; then
    TEMP_BRANCH="smoke-test-$$"
    git -C "$REPO_ROOT" branch "$TEMP_BRANCH" HEAD
    SMOKE_BRANCH="$TEMP_BRANCH"
fi

# shellcheck disable=SC2329 # invoked through the EXIT trap
cleanup() {
    echo ""
    echo "Cleaning up..."
    "$SCRIPT_DIR/teardown.sh" "$WORKTREE_PATH" 2>/dev/null || true
    if [ -n "$TEMP_BRANCH" ]; then
        git -C "$REPO_ROOT" branch -D "$TEMP_BRANCH" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

echo "=== Devcontainer Smoke Test ==="
echo ""

# Clone mode. A failed post-start can still leave a usable container.
echo "Launching devcontainer..."
"$SCRIPT_DIR/launch.sh" "$WORKTREE_PATH" "$SMOKE_BRANCH" || true
echo ""

check_tool() {
    local tool="$1"
    local extra_cmd="${2:-}"
    if "$SCRIPT_DIR/exec.sh" "$WORKTREE_PATH" "command -v $tool" >/dev/null 2>&1; then
        local version=""
        if [ -n "$extra_cmd" ]; then
            version=$("$SCRIPT_DIR/exec.sh" "$WORKTREE_PATH" "$extra_cmd" 2>/dev/null || true)
            echo "  PASS: $tool ($version)"
        else
            echo "  PASS: $tool"
        fi
    else
        echo "  FAIL: $tool not found"
        ((FAILURES++)) || true
    fi
}

check_version() {
    local label="$1" expected="$2" actual="$3"
    if [ -n "$expected" ] && [ "$expected" = "$actual" ]; then
        echo "  PASS: $label is $actual"
    else
        echo "  FAIL: $label is '$actual', expected '$expected'"
        ((FAILURES++)) || true
    fi
}

echo "Checking tools:"
check_tool "python3" "python3 --version"
check_tool "docker" "docker --version"
check_tool "gh" "gh --version"
check_tool "psql" "psql --version"
check_tool "jq" "jq --version"
check_tool "git" "git --version"

# Version assertions. The expected values come from pins.sh (which parses the Dockerfiles and the
# pytaiga-mcp installer), so the test cannot drift from the pins.
PINS_OUT=""
PINS_OK=1
PINS_OUT=$("$SCRIPT_DIR/pins.sh" "$REPO_ROOT" 2>&1) || PINS_OK=0
EXPECTED_PYTHON="/workspaces/$(basename "$WORKTREE_PATH")/.venv/bin/python"

echo ""
echo "Checking versions and paths match the pins and venv:"
if [ "$PINS_OK" -eq 1 ]; then
    eval "$PINS_OUT"
    in_container() { "$SCRIPT_DIR/exec.sh" "$WORKTREE_PATH" "$1" 2>/dev/null || true; }
    check_version "supabase" "$SUPABASE_CLI_VERSION" "$(in_container 'supabase --version' | tr -d '[:space:]')"
    check_version "node major" "$NODE_MAJOR" "$(in_container 'node --version' | sed 's/^v\([0-9]*\).*/\1/')"
    check_version "claude" "$CLAUDE_CODE_VERSION" "$(in_container 'claude --version' | awk '{print $1}')"
    check_version "codex" "$CODEX_VERSION" "$(in_container 'codex --version' | awk '{print $2}')"
    check_version "chrome-devtools-mcp" "$CHROME_DEVTOOLS_MCP_VERSION" \
        "$(in_container 'npm ls -g chrome-devtools-mcp --json | jq -r ".dependencies[\"chrome-devtools-mcp\"].version"')"
    check_version "uv" "$UV_VERSION" "$(in_container 'uv --version' | awk '{print $2}')"
    check_version "pytaiga-mcp marker" "$PYTAIGA_MCP_MARKER" "$(in_container 'cat "${PYTAIGA_MCP_HOME:-/opt/pytaiga-mcp}/.installed"')"
else
    echo "  FAIL: pins.sh could not read the pins: $PINS_OUT"
    ((FAILURES++)) || true
fi
check_version "python (workspace venv)" "$EXPECTED_PYTHON" "$("$SCRIPT_DIR/exec.sh" "$WORKTREE_PATH" "command -v python" 2>/dev/null || true)"

echo ""
echo "Checking the in-container Supabase stack:"
SUPABASE_URL_EXPECTED="http://127.0.0.1:54321"
STATUS_EXIT=0
STATUS_OUT=$("$SCRIPT_DIR/exec.sh" "$WORKTREE_PATH" "supabase status" 2>&1) || STATUS_EXIT=$?
if [ "$STATUS_EXIT" -eq 0 ] && grep -qF "$SUPABASE_URL_EXPECTED" <<<"$STATUS_OUT"; then
    echo "  PASS: supabase status reports $SUPABASE_URL_EXPECTED"
else
    echo "  FAIL: supabase status (exit $STATUS_EXIT) did not report $SUPABASE_URL_EXPECTED"
    ((FAILURES++)) || true
fi
ENV_MATCHES=$("$SCRIPT_DIR/exec.sh" "$WORKTREE_PATH" 'grep -c "^SUPABASE_URL=http://127.0.0.1:54321$" .env' 2>/dev/null || true)
check_version ".env SUPABASE_URL lines pointing at the local stack" "1" "$ENV_MATCHES"

echo ""
echo "Checking agent tooling:"
if [ -f "$REPO_ROOT/.devcontainer/Dockerfile.prebuild" ]; then
    check_tool "claude" "claude --version"
    check_tool "codex" "codex --version"
    check_tool "chrome-devtools-mcp"
    if "$SCRIPT_DIR/exec.sh" "$WORKTREE_PATH" 'test -f "${PYTAIGA_MCP_HOME:-/opt/pytaiga-mcp}/src/server_workflow.py"' >/dev/null 2>&1; then
        echo "  PASS: pytaiga-mcp server_workflow.py present"
    else
        echo "  FAIL: pytaiga-mcp server_workflow.py missing"
        ((FAILURES++)) || true
    fi
else
    echo "  SKIP: claude, codex, chrome-devtools-mcp, pytaiga-mcp (no .devcontainer/Dockerfile.prebuild yet, #74)"
fi

echo ""
echo "Checking browser path agreement (Dockerfile ENV, binary, .mcp.json):"
ARCH=$("$SCRIPT_DIR/exec.sh" "$WORKTREE_PATH" 'dpkg --print-architecture' 2>/dev/null || true)
if [ "$ARCH" = "amd64" ]; then
    EXPECTED_CHROME_PATH=$(sed -n 's/^ENV CHROME_PATH=//p' "$REPO_ROOT/.devcontainer/Dockerfile")
    ACTUAL_CHROME_PATH=$("$SCRIPT_DIR/exec.sh" "$WORKTREE_PATH" 'echo "$CHROME_PATH"' 2>/dev/null || true)
    check_version "CHROME_PATH" "$EXPECTED_CHROME_PATH" "$ACTUAL_CHROME_PATH"
    CHROME_VERSION=$("$SCRIPT_DIR/exec.sh" "$WORKTREE_PATH" '"$CHROME_PATH" --version' 2>/dev/null || true)
    if grep -q "Chrome" <<<"$CHROME_VERSION"; then
        echo "  PASS: browser runs ($CHROME_VERSION)"
    else
        echo "  FAIL: \$CHROME_PATH --version did not run: '$CHROME_VERSION'"
        ((FAILURES++)) || true
    fi
    "$SCRIPT_DIR/prepare-clone.sh" "$WORKTREE_PATH" >/dev/null
    MCP_EXEC_PATH=$(jq -r '.mcpServers["chrome-devtools"].args | .[index("--executablePath") + 1]' "$WORKTREE_PATH/.mcp.json")
    check_version ".mcp.json executablePath" "$EXPECTED_CHROME_PATH" "$MCP_EXEC_PATH"
else
    echo "  SKIP: browser (no Chrome on ${ARCH:-unknown arch}, #71)"
fi

echo ""
if [ "$FAILURES" -gt 0 ]; then
    echo "RESULT: FAIL ($FAILURES check(s) failed)"
    exit 1
else
    echo "RESULT: PASS (toolset present, Supabase stack local, venv python on PATH)"
    exit 0
fi
