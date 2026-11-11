#!/usr/bin/env bash
# ABOUTME: Tests scripts/devcontainer/pins.sh, the single parser of the devcontainer tool pins, against a fixture
# ABOUTME: repo with known values, and that smoke-test.sh calls it. No Docker, no network.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PINS="$REPO_ROOT/scripts/devcontainer/pins.sh"
SMOKE="$REPO_ROOT/scripts/devcontainer/smoke-test.sh"

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

# A fixture repo with known literal pins, so expected values are stated here rather than
# re-derived from the Dockerfiles with the same sed pins.sh uses (testing-strategy.md Rule 1).
make_fixture() {
    local root="$1"
    mkdir -p "$root/.devcontainer" "$root/tools/pytaiga-mcp"
    cat >"$root/.devcontainer/Dockerfile" <<'DOCKERFILE'
ARG SUPABASE_CLI_VERSION=9.8.7
RUN curl -fsSL https://deb.nodesource.com/setup_31.x | bash -
DOCKERFILE
    cat >"$root/.devcontainer/Dockerfile.prebuild" <<'DOCKERFILE'
ARG CLAUDE_CODE_VERSION=1.2.3
ARG CODEX_VERSION=4.5.6
ARG CHROME_DEVTOOLS_MCP_VERSION=7.8.9
ARG UV_VERSION=0.1.2
DOCKERFILE
    cat >"$root/tools/pytaiga-mcp/install.sh" <<'INSTALLER'
#!/usr/bin/env bash
[ "${1:-}" = "--print-marker" ] && echo "abc123 uv-sync-frozen"
INSTALLER
}

info "Fixture repo: exact KEY='value' output"
FIX="$TMP/fixture"
make_fixture "$FIX"
bash "$PINS" "$FIX" >"$TMP/fix-out" 2>"$TMP/fix-err"
RC=$?
if [ "$RC" -eq 0 ]; then pass "pins.sh exits 0 on a complete fixture"; else fail "pins.sh exited $RC: $(cat "$TMP/fix-err")"; fi
EXPECTED="NODE_MAJOR='31'
SUPABASE_CLI_VERSION='9.8.7'
CLAUDE_CODE_VERSION='1.2.3'
CODEX_VERSION='4.5.6'
CHROME_DEVTOOLS_MCP_VERSION='7.8.9'
UV_VERSION='0.1.2'
PYTAIGA_MCP_MARKER='abc123 uv-sync-frozen'"
if [ "$(cat "$TMP/fix-out")" = "$EXPECTED" ]; then pass "prints every pin with its fixture value"; else fail "unexpected output: $(cat "$TMP/fix-out")"; fi
MARKER_AFTER_EVAL="$(eval "$(cat "$TMP/fix-out")"; printf '%s' "$PYTAIGA_MCP_MARKER")"
if [ "$MARKER_AFTER_EVAL" = "abc123 uv-sync-frozen" ]; then pass "a value with a space survives eval"; else fail "eval gave '$MARKER_AFTER_EVAL'"; fi

info "Fixture repo missing the codex ARG"
FAKE="$TMP/missing"
make_fixture "$FAKE"
sed -i '/^ARG CODEX_VERSION=/d' "$FAKE/.devcontainer/Dockerfile.prebuild"
bash "$PINS" "$FAKE" >"$TMP/fake-out" 2>"$TMP/fake-err"
RC=$?
if [ "$RC" -eq 1 ]; then pass "exits 1 when a pin is missing"; else fail "exit code $RC, expected 1"; fi
if grep -q 'CODEX_VERSION' "$TMP/fake-err"; then pass "stderr names CODEX_VERSION"; else fail "stderr does not name CODEX_VERSION: $(cat "$TMP/fake-err")"; fi
if grep -q 'CLAUDE_CODE_VERSION' "$TMP/fake-err"; then fail "stderr names a pin that is present"; else pass "stderr names only the missing pin"; fi

info "Real repo: every key printed and non-empty"
OUT="$(bash "$PINS" 2>"$TMP/err")"
RC=$?
if [ "$RC" -eq 0 ]; then pass "pins.sh exits 0"; else fail "pins.sh exited $RC: $(cat "$TMP/err")"; fi
for key in "${KEYS[@]}"; do
    if grep -qE "^$key='.+'\$" <<<"$OUT"; then pass "$key is printed and non-empty"; else fail "$key missing or empty in: $OUT"; fi
done

info "smoke-test.sh takes expected versions from pins.sh"
if grep -v '^[[:space:]]*#' "$SMOKE" | grep -q 'pins\.sh'; then pass "smoke-test.sh calls pins.sh"; else fail "smoke-test.sh does not call pins.sh"; fi

echo ""
if [ "$FAILURES" -gt 0 ]; then echo -e "${RED}$FAILURES failure(s)${NC}"; exit 1; fi
echo -e "${GREEN}All devcontainer pin tests passed${NC}"
