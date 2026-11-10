#!/usr/bin/env bash
# ABOUTME: Tests scripts/devcontainer/pins.sh, the single parser of the devcontainer tool pins, and that
# ABOUTME: smoke-test.sh takes its expected versions from it instead of hardcoding them. No Docker, no network.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PINS="$REPO_ROOT/scripts/devcontainer/pins.sh"
SMOKE="$REPO_ROOT/scripts/devcontainer/smoke-test.sh"
PREBUILD="$REPO_ROOT/.devcontainer/Dockerfile.prebuild"
BASE_DOCKERFILE="$REPO_ROOT/.devcontainer/Dockerfile"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}✓ $1${NC}"; }
fail() { echo -e "${RED}✗ $1${NC}"; FAILURES=$((FAILURES + 1)); }
info() { echo -e "${YELLOW}→ $1${NC}"; }

FAILURES=0
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

KEYS=(NODE_MAJOR SUPABASE_CLI_VERSION CLAUDE_CODE_VERSION CODEX_VERSION CHROME_DEVTOOLS_MCP_VERSION UV_VERSION PYTAIGA_MCP_MARKER)

info "Syntax"
if bash -n "$PINS" 2>/dev/null; then pass "pins.sh passes bash -n"; else fail "pins.sh fails bash -n (or is missing)"; fi
if bash -n "$SMOKE" 2>/dev/null; then pass "smoke-test.sh passes bash -n"; else fail "smoke-test.sh fails bash -n"; fi
if command -v shellcheck >/dev/null 2>&1; then
    if shellcheck "$PINS" >/dev/null 2>&1; then pass "pins.sh passes shellcheck"; else fail "pins.sh fails shellcheck"; fi
else
    info "shellcheck not installed, skipping"
fi

info "Real repo: every key printed, non-empty, equal to the declared value"
OUT="$(bash "$PINS" 2>"$TMP/err")"
RC=$?
if [ "$RC" -eq 0 ]; then pass "pins.sh exits 0"; else fail "pins.sh exited $RC: $(cat "$TMP/err")"; fi
for key in "${KEYS[@]}"; do
    if grep -qE "^$key='.+'\$" <<<"$OUT"; then pass "$key is printed and non-empty"; else fail "$key missing or empty in: $OUT"; fi
done

pinned() { eval "$OUT"; eval "printf '%s' \"\${$1:-}\""; }
for key in CLAUDE_CODE_VERSION CODEX_VERSION CHROME_DEVTOOLS_MCP_VERSION UV_VERSION; do
    declared="$(sed -n "s/^ARG $key=//p" "$PREBUILD")"
    if [ -n "$declared" ] && [ "$(pinned "$key")" = "$declared" ]; then
        pass "$key equals the ARG in Dockerfile.prebuild ($declared)"
    else
        fail "$key '$(pinned "$key")' differs from the Dockerfile.prebuild ARG '$declared'"
    fi
done
declared="$(sed -n 's/^ARG SUPABASE_CLI_VERSION=//p' "$BASE_DOCKERFILE")"
if [ -n "$declared" ] && [ "$(pinned SUPABASE_CLI_VERSION)" = "$declared" ]; then
    pass "SUPABASE_CLI_VERSION equals the base Dockerfile ARG ($declared)"
else
    fail "SUPABASE_CLI_VERSION differs from the base Dockerfile ARG"
fi
if [ "$(pinned PYTAIGA_MCP_MARKER)" = "$(bash "$REPO_ROOT/tools/pytaiga-mcp/install.sh" --print-marker)" ]; then
    pass "PYTAIGA_MCP_MARKER equals install.sh --print-marker (value contains a space and survives eval)"
else
    fail "PYTAIGA_MCP_MARKER differs from install.sh --print-marker"
fi

info "Temp copy missing the codex ARG"
FAKE="$TMP/repo"
mkdir -p "$FAKE/.devcontainer" "$FAKE/tools/pytaiga-mcp"
cp "$BASE_DOCKERFILE" "$FAKE/.devcontainer/Dockerfile"
grep -v '^ARG CODEX_VERSION=' "$PREBUILD" >"$FAKE/.devcontainer/Dockerfile.prebuild"
cp "$REPO_ROOT/tools/pytaiga-mcp/install.sh" "$FAKE/tools/pytaiga-mcp/install.sh"
bash "$PINS" "$FAKE" >"$TMP/fake-out" 2>"$TMP/fake-err"
RC=$?
if [ "$RC" -eq 1 ]; then pass "exits 1 when a pin is missing"; else fail "exit code $RC, expected 1"; fi
if grep -q 'CODEX_VERSION' "$TMP/fake-err"; then pass "stderr names CODEX_VERSION"; else fail "stderr does not name CODEX_VERSION: $(cat "$TMP/fake-err")"; fi
if grep -q 'CLAUDE_CODE_VERSION' "$TMP/fake-err"; then fail "stderr names a pin that is present"; else pass "stderr names only the missing pin"; fi

info "smoke-test.sh takes expected versions from pins.sh"
if grep -v '^[[:space:]]*#' "$SMOKE" | grep -q 'pins\.sh'; then pass "smoke-test.sh calls pins.sh"; else fail "smoke-test.sh does not call pins.sh"; fi
LITERAL_FOUND=""
for key in CLAUDE_CODE_VERSION CODEX_VERSION CHROME_DEVTOOLS_MCP_VERSION UV_VERSION SUPABASE_CLI_VERSION; do
    literal="$(pinned "$key")"
    if [ -n "$literal" ] && grep -v '^[[:space:]]*#' "$SMOKE" | grep -qF "$literal"; then LITERAL_FOUND="$LITERAL_FOUND $key=$literal"; fi
done
if [ -z "$LITERAL_FOUND" ]; then pass "smoke-test.sh hardcodes no pinned version"; else fail "smoke-test.sh hardcodes:$LITERAL_FOUND"; fi

echo ""
if [ "$FAILURES" -gt 0 ]; then echo -e "${RED}$FAILURES failure(s)${NC}"; exit 1; fi
echo -e "${GREEN}All devcontainer pin tests passed${NC}"
