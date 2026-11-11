#!/usr/bin/env bash
# ABOUTME: Tests tools/pytaiga-mcp/install.sh with fake git and uv on PATH (no network, no real checkout).
# ABOUTME: Covers pinned fetch, idempotency, marker mismatch, failed sync, default dir and refusal of foreign dirs.
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}✓ $1${NC}"; }
fail() { echo -e "${RED}✗ $1${NC}"; FAILURES=$((FAILURES + 1)); }
info() { echo -e "${YELLOW}→ $1${NC}"; }

FAILURES=0

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
INSTALL="$REPO_ROOT/tools/pytaiga-mcp/install.sh"

info "Testing $INSTALL"

if [ ! -f "$INSTALL" ]; then
    fail "install.sh exists"
    exit 1
fi
pass "install.sh exists"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# Fake git and uv: log argv, and create just the files the real tools would.
FAKEBIN="$TMP/bin"
mkdir -p "$FAKEBIN"
cat > "$FAKEBIN/git" <<'FAKE'
#!/usr/bin/env bash
echo "git $*" >> "$CALL_LOG"
dir="."
if [ "$1" = "-C" ]; then dir="$2"; shift 2; fi
case "$1" in
    init)
        target="${2:-$dir}"
        mkdir -p "$target"
        ;;
    checkout)
        mkdir -p "$dir/src"
        : > "$dir/pyproject.toml"
        : > "$dir/uv.lock"
        : > "$dir/src/server_workflow.py"
        ;;
esac
exit 0
FAKE
cat > "$FAKEBIN/uv" <<'FAKE'
#!/usr/bin/env bash
echo "uv $* [UV_PYTHON_DOWNLOADS=${UV_PYTHON_DOWNLOADS:-unset} UV_CONCURRENT_DOWNLOADS=${UV_CONCURRENT_DOWNLOADS:-unset}]" >> "$CALL_LOG"
[ "${FAKE_UV_FAIL:-0}" = "1" ] && exit 1
dir=""
while [ $# -gt 0 ]; do
    if [ "$1" = "--directory" ]; then dir="$2"; fi
    shift
done
mkdir -p "$dir/.venv/bin"
: > "$dir/.venv/bin/python"
chmod +x "$dir/.venv/bin/python"
exit 0
FAKE
chmod +x "$FAKEBIN/git" "$FAKEBIN/uv"

CALL_LOG="$TMP/calls.log"
export CALL_LOG

# Runs install.sh with the fakes first on PATH; OUT/RC hold the result.
run_install() {
    : > "$CALL_LOG"
    set +e
    OUT=$(PATH="$FAKEBIN:$PATH" bash "$INSTALL" "$@" 2>&1)
    RC=$?
    set -e
}

EXPECTED_COMMIT=$(grep -E '^PYTAIGA_MCP_COMMIT=' "$INSTALL" | head -1 | cut -d= -f2)
MARKER=$(bash "$INSTALL" --print-marker)

# Syntax
if bash -n "$INSTALL"; then pass "bash -n install.sh"; else fail "bash -n install.sh"; fi
if command -v shellcheck >/dev/null 2>&1; then
    if shellcheck "$INSTALL"; then pass "shellcheck install.sh"; else fail "shellcheck install.sh"; fi
else
    info "shellcheck not installed; skipping"
fi

# Static pins
if grep -qxF 'PYTAIGA_MCP_REPO=https://github.com/Alterna-Dev-Studio/pytaiga-mcp.git' "$INSTALL"; then
    pass "repo URL is the Alterna-Dev-Studio fork"
else
    fail "repo URL is the Alterna-Dev-Studio fork"
fi
if [[ "$EXPECTED_COMMIT" =~ ^[0-9a-f]{40}$ ]]; then
    pass "commit pin is a 40-hex sha"
else
    fail "commit pin is a 40-hex sha (got '$EXPECTED_COMMIT')"
fi
if [ "$MARKER" = "$EXPECTED_COMMIT uv-sync-frozen" ]; then
    pass "--print-marker prints '<commit> uv-sync-frozen'"
else
    fail "--print-marker prints '<commit> uv-sync-frozen' (got '$MARKER')"
fi

# Fresh install
HOME_A="$TMP/home-a"
export PYTAIGA_MCP_HOME="$HOME_A"
run_install
if [ "$RC" -eq 0 ]; then pass "fresh install exits 0"; else fail "fresh install exits 0 (rc=$RC: $OUT)"; fi
if grep -qF "UV_CONCURRENT_DOWNLOADS=1]" "$CALL_LOG"; then
    pass "uv sync runs with UV_CONCURRENT_DOWNLOADS=1 (parallel git fetches fail TLS on some Docker bridge networks)"
else
    fail "uv sync runs with UV_CONCURRENT_DOWNLOADS=1"
fi
if grep -qF "fetch --depth 1 origin $EXPECTED_COMMIT" "$CALL_LOG"; then
    pass "fresh install fetches the pinned commit"
else
    fail "fresh install fetches the pinned commit"
fi
if grep -qF "uv sync --frozen --directory $HOME_A" "$CALL_LOG"; then
    pass "fresh install runs uv sync --frozen --directory <home>"
else
    fail "fresh install runs uv sync --frozen --directory <home>"
fi
if grep -qF "UV_PYTHON_DOWNLOADS=never" "$CALL_LOG"; then
    pass "UV_PYTHON_DOWNLOADS=never reaches uv"
else
    fail "UV_PYTHON_DOWNLOADS=never reaches uv"
fi
if [ "$(cat "$HOME_A/.installed" 2>/dev/null)" = "$MARKER" ]; then
    pass "marker written and equals --print-marker"
else
    fail "marker written and equals --print-marker"
fi

# Second run is a no-op
run_install
if [ "$RC" -eq 0 ] && grep -qF "pytaiga-mcp up to date at $HOME_A" <<< "$OUT"; then
    pass "second run reports up to date"
else
    fail "second run reports up to date (rc=$RC: $OUT)"
fi
if [ ! -s "$CALL_LOG" ]; then pass "second run makes no git or uv calls"; else fail "second run makes no git or uv calls"; fi

# Marker mismatch reinstalls
echo "stale-marker" > "$HOME_A/.installed"
run_install
if [ "$RC" -eq 0 ] && grep -qF "fetch --depth 1 origin" "$CALL_LOG" \
    && [ "$(cat "$HOME_A/.installed")" = "$MARKER" ]; then
    pass "marker mismatch reinstalls and rewrites the marker"
else
    fail "marker mismatch reinstalls and rewrites the marker (rc=$RC: $OUT)"
fi

# uv sync failure leaves no marker
HOME_B="$TMP/home-b"
export PYTAIGA_MCP_HOME="$HOME_B"
FAKE_UV_FAIL=1 run_install
if [ "$RC" -ne 0 ] && [ ! -e "$HOME_B/.installed" ]; then
    pass "failed uv sync exits non-zero and writes no marker"
else
    fail "failed uv sync exits non-zero and writes no marker (rc=$RC)"
fi

# Default dir
FAKE_HOME="$TMP/fakehome"
mkdir -p "$FAKE_HOME"
unset PYTAIGA_MCP_HOME
HOME="$FAKE_HOME" run_install
if [ "$RC" -eq 0 ] && [ -f "$FAKE_HOME/work/utils/pytaiga-mcp/.installed" ]; then
    pass "default dir is \$HOME/work/utils/pytaiga-mcp"
else
    fail "default dir is \$HOME/work/utils/pytaiga-mcp (rc=$RC: $OUT)"
fi

# Foreign non-empty dir refused untouched
HOME_C="$TMP/home-c"
mkdir -p "$HOME_C"
echo "precious" > "$HOME_C/notes.txt"
export PYTAIGA_MCP_HOME="$HOME_C"
run_install
if [ "$RC" -ne 0 ] && grep -qF "is not a pytaiga-mcp checkout" <<< "$OUT" \
    && [ "$(cat "$HOME_C/notes.txt")" = "precious" ] && [ ! -s "$CALL_LOG" ]; then
    pass "foreign non-empty dir is refused and left untouched"
else
    fail "foreign non-empty dir is refused and left untouched (rc=$RC: $OUT)"
fi

# Missing git or uv gives a clear error. PATH holds only a shim dir with the
# basic tools plus whichever of git/uv is not under test.
for tool in git uv; do
    shimdir="$TMP/shim-$tool"
    mkdir -p "$shimdir"
    for t in bash cat rm mkdir find dirname cut ls env; do
        ln -sf "$(command -v "$t")" "$shimdir/$t"
    done
    for t in git uv; do
        [ "$t" = "$tool" ] || ln -sf "$FAKEBIN/$t" "$shimdir/$t"
    done
    set +e
    OUT=$(PYTAIGA_MCP_HOME="$TMP/home-missing-$tool" PATH="$shimdir" "$(command -v bash)" "$INSTALL" 2>&1)
    RC=$?
    set -e
    if [ "$RC" -ne 0 ] && grep -qF "$tool" <<< "$OUT"; then
        pass "missing $tool gives a clear error"
    else
        fail "missing $tool gives a clear error (rc=$RC: $OUT)"
    fi
done

echo
if [ "$FAILURES" -eq 0 ]; then
    echo -e "${GREEN}All pytaiga-mcp installer tests passed${NC}"
else
    echo -e "${RED}$FAILURES test(s) failed${NC}"
    exit 1
fi
