#!/usr/bin/env bash
# ABOUTME: Unit tests for the prepare-clone.sh script.
# ABOUTME: Verifies settings.local.json, .mcp.json contents, idempotency and tracked-file safety.
# shellcheck disable=SC2016  # single-quoted payloads are literal command text, not expansions
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PREPARE="$REPO_ROOT/scripts/devcontainer/prepare-clone.sh"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}✓ $1${NC}"; }
fail() { echo -e "${RED}✗ $1${NC}"; FAILURES=$((FAILURES + 1)); }
info() { echo -e "${YELLOW}→ $1${NC}"; }

FAILURES=0
TEST_DIR=""

cleanup() {
    if [ -n "$TEST_DIR" ] && [ -d "$TEST_DIR" ]; then
        rm -rf "$TEST_DIR"
    fi
}
trap cleanup EXIT

# prepare-clone.sh derives the MCP browser path from the clone's own Dockerfile.
FAKE_CHROME_PATH="/opt/fake/chrome"
write_fake_dockerfile() {
    mkdir -p "$1/.devcontainer"
    echo "ENV CHROME_PATH=$FAKE_CHROME_PATH" > "$1/.devcontainer/Dockerfile"
}

setup_fake_clone() {
    cleanup
    TEST_DIR=$(mktemp -d "/tmp/test-prepare-clone-XXXXXX")
    mkdir -p "$TEST_DIR/.claude"
    write_fake_dockerfile "$TEST_DIR"
}

setup_fake_repo() {
    setup_fake_clone
    git -C "$TEST_DIR" init -q
    git -C "$TEST_DIR" config user.email "test@test.com"
    git -C "$TEST_DIR" config user.name "Test"
}

# Runs prepare-clone, capturing PREP_OUT (stdout), PREP_ERR (stderr) and PREP_EXIT.
run_prepare() {
    local errfile
    errfile=$(mktemp)
    PREP_EXIT=0
    PREP_OUT=$(bash "$PREPARE" "$1" 2>"$errfile") || PREP_EXIT=$?
    PREP_ERR=$(cat "$errfile")
    rm -f "$errfile"
}

info "Test 1: settings.local.json is valid JSON with a cwd-independent pgrep hook"
setup_fake_clone
run_prepare "$TEST_DIR"
SETTINGS_FILE="$TEST_DIR/.claude/settings.local.json"
MCP_FILE="$TEST_DIR/.mcp.json"
if [ "$PREP_EXIT" -eq 0 ] && jq '.' "$SETTINGS_FILE" >/dev/null 2>&1; then
    pass "settings.local.json created and is valid JSON"
else
    fail "settings.local.json missing or invalid (exit $PREP_EXIT): $PREP_ERR"
fi
HOOK_CMD=$(jq -r '.hooks.PreToolUse[0].hooks[0].command // empty' "$SETTINGS_FILE")
if grep -q "reject-self-matching-pgrep.sh" <<< "$HOOK_CMD"; then
    pass "registers the reject-self-matching-pgrep PreToolUse hook"
else
    fail "missing reject-self-matching-pgrep hook (got: $HOOK_CMD)"
fi
if grep -qF "$TEST_DIR" <<< "$HOOK_CMD" && grep -qF '${CLAUDE_PROJECT_DIR' <<< "$HOOK_CMD"; then
    pass "hook command resolves independently of the working directory"
else
    fail "hook command is not cwd-independent (got: $HOOK_CMD)"
fi

info "Test 2: permissions are the Cholobots allow-list"
PERMS=$(jq -r '.permissions.allow[]' "$SETTINGS_FILE")
REQUIRED=(
    "Bash(python3 *)" "Bash(python -m pytest *)" "Bash(python -m py_compile *)" "Bash(supabase *)"
    "Bash(scripts/*)" "Bash(bash tests/scripts/*)" "Bash(git *)" "Bash(env -u GH_TOKEN gh *)"
    "Bash(cat *)" "Bash(ls *)" "Bash(mkdir *)" "Bash(pwd)" "Bash(echo *)" "Bash(jq *)"
    "mcp__taiga-py__*" "mcp__chrome-devtools__*" "Read(*)" "Write(*)" "Edit(*)"
    "Edit(.claude/skills/**)" "Grep(*)" "Glob(*)" "Agent(*)"
    "WebFetch(domain:github.com)" "WebFetch(domain:api.github.com)"
)
for req in "${REQUIRED[@]}"; do
    if ! grep -qxF "$req" <<< "$PERMS"; then
        fail "Missing required permission: $req"
    fi
done
for forbidden in "Bash(dotnet" "Bash(npm" "Bash(npx" "Bash(docker"; do
    if grep -qF "$forbidden" <<< "$PERMS"; then
        fail "Unexpected permission present: $forbidden"
    fi
done
if grep -qxF "Bash(gh *)" <<< "$PERMS"; then
    fail "Bare Bash(gh *) must not be allowed; gh calls go through env -u GH_TOKEN"
fi
pass "permission allow-list checked"

info "Test 3: chrome-devtools runs natively with the Dockerfile's CHROME_PATH"
CHROME_CMD=$(jq -r '.mcpServers["chrome-devtools"].command' "$MCP_FILE")
CHROME_EXEC=$(jq -r '.mcpServers["chrome-devtools"].args | .[index("--executablePath") + 1]' "$MCP_FILE")
if [ "$CHROME_CMD" = "chrome-devtools-mcp" ] && [ "$CHROME_EXEC" = "$FAKE_CHROME_PATH" ]; then
    pass "chrome-devtools command and executablePath correct"
else
    fail "chrome-devtools command='$CHROME_CMD' executablePath='$CHROME_EXEC'"
fi

info "Test 4: taiga-py defaults to /opt/pytaiga-mcp and notes #74"
TAIGA_CMD=$(jq -r '.mcpServers["taiga-py"].command' "$MCP_FILE")
TAIGA_ARG=$(jq -r '.mcpServers["taiga-py"].args[0]' "$MCP_FILE")
TAIGA_TRANSPORT=$(jq -r '.mcpServers["taiga-py"].env.TAIGA_TRANSPORT' "$MCP_FILE")
if [ "$TAIGA_CMD" = "/opt/pytaiga-mcp/.venv/bin/python" ] \
    && [ "$TAIGA_ARG" = "/opt/pytaiga-mcp/src/server_workflow.py" ] \
    && [ "$TAIGA_TRANSPORT" = "stdio" ]; then
    pass "taiga-py command, args and TAIGA_TRANSPORT correct"
else
    fail "taiga-py cmd='$TAIGA_CMD' arg='$TAIGA_ARG' transport='$TAIGA_TRANSPORT'"
fi
if grep -qF "#74" <<< "$PREP_ERR"; then
    pass "stderr notes the missing Dockerfile.prebuild (#74)"
else
    fail "stderr does not mention #74: $PREP_ERR"
fi

info "Test 5: PYTAIGA_MCP_HOME comes from Dockerfile.prebuild when present"
setup_fake_clone
echo "ENV PYTAIGA_MCP_HOME=/srv/ptm" > "$TEST_DIR/.devcontainer/Dockerfile.prebuild"
run_prepare "$TEST_DIR"
TAIGA_CMD=$(jq -r '.mcpServers["taiga-py"].command' "$TEST_DIR/.mcp.json")
TAIGA_ARG=$(jq -r '.mcpServers["taiga-py"].args[0]' "$TEST_DIR/.mcp.json")
if [ "$TAIGA_CMD" = "/srv/ptm/.venv/bin/python" ] && [ "$TAIGA_ARG" = "/srv/ptm/src/server_workflow.py" ]; then
    pass "taiga-py paths use /srv/ptm"
else
    fail "taiga-py cmd='$TAIGA_CMD' arg='$TAIGA_ARG'"
fi
if ! grep -qF "#74" <<< "$PREP_ERR"; then
    pass "no #74 note when Dockerfile.prebuild exists"
else
    fail "unexpected #74 note: $PREP_ERR"
fi

info "Test 6: .mcp.json carries no vault, taiga-mcp or password references"
BAD=""
for needle in "taiga-mcp" "jon-dev-env" "TAIGA_PASSWORD"; do
    if grep -qF "$needle" "$TEST_DIR/.mcp.json"; then BAD="$BAD $needle"; fi
done
if [ -z "$BAD" ]; then pass ".mcp.json is free of forbidden references"; else fail ".mcp.json contains:$BAD"; fi

info "Test 7: enabledMcpjsonServers lists both servers"
MCP_SERVERS=$(jq -r '.enabledMcpjsonServers[]' "$TEST_DIR/.claude/settings.local.json")
if grep -qx "taiga-py" <<< "$MCP_SERVERS" && grep -qx "chrome-devtools" <<< "$MCP_SERVERS"; then
    pass "taiga-py and chrome-devtools enabled"
else
    fail "enabledMcpjsonServers: $MCP_SERVERS"
fi

info "Test 8: idempotent (files and output identical across runs)"
setup_fake_clone
run_prepare "$TEST_DIR"
FIRST_OUT="$PREP_OUT"
SETTINGS_BEFORE=$(cat "$TEST_DIR/.claude/settings.local.json")
MCP_BEFORE=$(cat "$TEST_DIR/.mcp.json")
run_prepare "$TEST_DIR"
if [ "$SETTINGS_BEFORE" = "$(cat "$TEST_DIR/.claude/settings.local.json")" ] \
    && [ "$MCP_BEFORE" = "$(cat "$TEST_DIR/.mcp.json")" ] \
    && [ "$FIRST_OUT" = "$PREP_OUT" ] \
    && [ "$PREP_OUT" = "Clone prepared at $TEST_DIR" ]; then
    pass "running twice produces the same files and output"
else
    fail "not idempotent; output: '$PREP_OUT'"
fi

info "Test 9: fails on a missing clone path"
run_prepare "/tmp/nonexistent-clone-path-XXXXXX"
if [ "$PREP_EXIT" -ne 0 ]; then pass "non-zero exit on missing clone path"; else fail "exited 0 on a missing path"; fi

info "Test 10: fails naming ENV CHROME_PATH when the Dockerfile lacks it"
setup_fake_clone
echo "FROM scratch" > "$TEST_DIR/.devcontainer/Dockerfile"
run_prepare "$TEST_DIR"
if [ "$PREP_EXIT" -ne 0 ] && grep -q "ENV CHROME_PATH" <<< "$PREP_ERR"; then
    pass "non-zero exit naming ENV CHROME_PATH"
else
    fail "expected failure naming ENV CHROME_PATH; exit=$PREP_EXIT err='$PREP_ERR'"
fi

info "Test 11: git repo with untracked .mcp.json exits 0"
setup_fake_repo
run_prepare "$TEST_DIR"
if [ "$PREP_EXIT" -eq 0 ]; then pass "exit 0 with untracked .mcp.json"; else fail "exit $PREP_EXIT: $PREP_ERR"; fi

info "Test 12: committed .mcp.json is marked assume-unchanged"
setup_fake_repo
echo '{}' > "$TEST_DIR/.mcp.json"
git -C "$TEST_DIR" add .mcp.json
git -C "$TEST_DIR" commit -q -m "initial"
run_prepare "$TEST_DIR"
FLAG=$(git -C "$TEST_DIR" ls-files -v .mcp.json | cut -c1)
if [ "$PREP_EXIT" -eq 0 ] && [ "$FLAG" = "h" ]; then
    pass ".mcp.json is assume-unchanged"
else
    fail "exit=$PREP_EXIT flag='$FLAG'"
fi

info "Test 13: modified tracked files are kept and reported on stderr"
setup_fake_repo
echo "original" > "$TEST_DIR/tracked.txt"
git -C "$TEST_DIR" add tracked.txt
git -C "$TEST_DIR" commit -q -m "initial"
echo "modified" > "$TEST_DIR/tracked.txt"
run_prepare "$TEST_DIR"
if [ "$PREP_EXIT" -eq 0 ] && [ "$(cat "$TEST_DIR/tracked.txt")" = "modified" ] && grep -q "tracked.txt" <<< "$PREP_ERR"; then
    pass "tracked.txt kept as modified and listed on stderr"
else
    fail "exit=$PREP_EXIT content='$(cat "$TEST_DIR/tracked.txt")' err='$PREP_ERR'"
fi

echo ""
if [ "$FAILURES" -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}$FAILURES test(s) failed.${NC}"
    exit 1
fi
