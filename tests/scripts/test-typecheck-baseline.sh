#!/usr/bin/env bash
# ABOUTME: Behavioural test for `make typecheck` on a throwaway copy of the tracked files.
# ABOUTME: Checks that baselined errors survive line shifts and that new or stale entries fail.
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
TY="$REPO_ROOT/.venv/bin/ty"

if [ ! -x "$TY" ]; then
    echo "SKIP: $TY is missing (run make setup)"
    exit 0
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# Tracked files at HEAD, then the working-tree versions of the files this test depends on.
git -C "$REPO_ROOT" archive HEAD | tar -x -C "$TMP"
for f in Makefile pyproject.toml ty-baseline.json; do
    cp "$REPO_ROOT/$f" "$TMP/$f"
done
ln -s "$REPO_ROOT/.venv" "$TMP/.venv"

run_typecheck() {
    make -C "$TMP" typecheck TY="$TY" 2>&1
}

info "Case 1: clean copy passes"
if out="$(run_typecheck)"; then pass "clean copy exits 0"; else fail "clean copy exits 0"; echo "$out"; fi

info "Case 2: baselined errors survive a line shift"
# Shift a file that still has baseline entries: the ty stories empty the baseline one file at a
# time, and shifting a clean file would pass without testing anything.
BASELINED="$(python3 -c 'import json, sys; files = json.load(open(sys.argv[1]))["files"]; print(next((f for f, e in sorted(files.items()) if e and f.startswith("bots/")), next((f for f, e in sorted(files.items()) if e), "")))' "$TMP/ty-baseline.json")"
if [ -z "$BASELINED" ]; then
    info "baseline is empty; nothing to shift"
else
    cp "$TMP/$BASELINED" "$TMP/baselined.orig"
    { printf '\n\n\n\n\n'; cat "$TMP/baselined.orig"; } > "$TMP/$BASELINED"
    if out="$(run_typecheck)"; then pass "five blank lines at the top of $BASELINED still exit 0"; else fail "five blank lines at the top of $BASELINED still exit 0"; echo "$out"; fi
    cp "$TMP/baselined.orig" "$TMP/$BASELINED"
fi

cp "$TMP/bots/crudobot.py" "$TMP/crudobot.orig"

info "Case 3: a new error fails and is named"
{ cat "$TMP/crudobot.orig"; printf '\n\ndef _probe() -> int:\n    return "x"\n'; } > "$TMP/bots/crudobot.py"
if out="$(run_typecheck)"; then
    fail "a new type error exits non-zero"
else
    pass "a new type error exits non-zero"
    if grep -q 'bots/crudobot.py' <<< "$out" && grep -q 'invalid-return-type\|invalid-explicit-return' <<< "$out"; then
        pass "output names bots/crudobot.py and the rule"
    else
        fail "output names bots/crudobot.py and the rule"; echo "$out"
    fi
fi
cp "$TMP/crudobot.orig" "$TMP/bots/crudobot.py"

info "Case 4: a stale baseline entry fails"
python3 - "$TMP/ty-baseline.json" <<'PY'
import json, sys
path = sys.argv[1]
data = json.load(open(path))
data["files"].setdefault("cholobots.py", []).append(
    {"rule": "invalid-assignment", "startColumn": 1, "endColumn": 2, "lineCount": 1}
)
json.dump(data, open(path, "w"), indent=2)
PY
if out="$(run_typecheck)"; then
    fail "a fabricated baseline entry exits non-zero"
else
    pass "a fabricated baseline entry exits non-zero"
    if grep -q 'make typecheck-baseline' <<< "$out"; then pass "output says to run make typecheck-baseline"; else fail "output says to run make typecheck-baseline"; echo "$out"; fi
fi

if [ "$FAILURES" -ne 0 ]; then
    echo -e "${RED}$FAILURES failure(s)${NC}"
    exit 1
fi
echo -e "${GREEN}All typecheck baseline tests passed${NC}"
