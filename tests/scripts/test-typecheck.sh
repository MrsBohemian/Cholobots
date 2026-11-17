#!/usr/bin/env bash
# ABOUTME: Behavioural test for `make typecheck` on a throwaway copy of the tracked files.
# ABOUTME: Checks that a clean tree passes and that a new type error fails and is named.
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
for f in Makefile pyproject.toml; do
    cp "$REPO_ROOT/$f" "$TMP/$f"
done
ln -s "$REPO_ROOT/.venv" "$TMP/.venv"

run_typecheck() {
    make -C "$TMP" typecheck TY="$TY" 2>&1
}

info "Case 1: clean copy passes"
if out="$(run_typecheck)"; then pass "clean copy exits 0"; else fail "clean copy exits 0"; echo "$out"; fi

cp "$TMP/bots/crudobot.py" "$TMP/crudobot.orig"

info "Case 2: a new error fails and is named"
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

info "Case 3: a possibly-unbound name fails and is named"
{ cat "$TMP/crudobot.orig"; printf '\n\ndef _probe2(flag: bool) -> int:\n    if flag:\n        value = 1\n    return value\n'; } > "$TMP/bots/crudobot.py"
if out="$(run_typecheck)"; then
    fail "a possibly-unbound name exits non-zero"
else
    pass "a possibly-unbound name exits non-zero"
    if grep -q 'possibly-unresolved-reference' <<< "$out"; then
        pass "output names possibly-unresolved-reference"
    else
        fail "output names possibly-unresolved-reference"; echo "$out"
    fi
fi
cp "$TMP/crudobot.orig" "$TMP/bots/crudobot.py"

info "Case 4: a blanket ty: ignore fails and is named"
{ cat "$TMP/crudobot.orig"; printf '\nx = 1  # ty: ignore\n'; } > "$TMP/bots/crudobot.py"
if out="$(run_typecheck)"; then
    fail "a blanket ignore exits non-zero"
else
    pass "a blanket ignore exits non-zero"
    if grep -q 'blanket-ignore-comment' <<< "$out"; then
        pass "output names blanket-ignore-comment"
    else
        fail "output names blanket-ignore-comment"; echo "$out"
    fi
fi
cp "$TMP/crudobot.orig" "$TMP/bots/crudobot.py"

if [ "$FAILURES" -ne 0 ]; then
    echo -e "${RED}$FAILURES failure(s)${NC}"
    exit 1
fi
echo -e "${GREEN}All typecheck tests passed${NC}"
