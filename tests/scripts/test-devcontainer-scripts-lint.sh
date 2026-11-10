#!/usr/bin/env bash
# ABOUTME: Lint test for the devcontainer lifecycle scripts: bash -n and shellcheck over every script,
# ABOUTME: plus a scan proving none of them can touch the hosted Supabase project or start the bot.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}✓ $1${NC}"; }
fail() { echo -e "${RED}✗ $1${NC}"; FAILURES=$((FAILURES + 1)); }
info() { echo -e "${YELLOW}→ $1${NC}"; }

FAILURES=0

# nullglob so an empty directory yields an empty list, which the count check then fails.
shopt -s nullglob
DEVCONTAINER_SCRIPTS=("$REPO_ROOT"/scripts/devcontainer/*.sh)
HOOK_SCRIPTS=("$REPO_ROOT"/scripts/hooks/*.sh)
ALL_SCRIPTS=("${DEVCONTAINER_SCRIPTS[@]}" "${HOOK_SCRIPTS[@]}")

info "Script inventory"
if [ "${#DEVCONTAINER_SCRIPTS[@]}" -ge 8 ]; then
    pass "scripts/devcontainer has ${#DEVCONTAINER_SCRIPTS[@]} scripts (at least 8)"
else
    fail "scripts/devcontainer has only ${#DEVCONTAINER_SCRIPTS[@]} scripts, expected at least 8"
fi
if [ "${#HOOK_SCRIPTS[@]}" -ge 1 ]; then
    pass "scripts/hooks has ${#HOOK_SCRIPTS[@]} script(s)"
else
    fail "scripts/hooks has no scripts"
fi

info "bash -n"
for f in "${ALL_SCRIPTS[@]}"; do
    if bash -n "$f" 2>/dev/null; then
        pass "bash -n ${f#"$REPO_ROOT"/}"
    else
        fail "bash -n ${f#"$REPO_ROOT"/}"
    fi
done

info "shellcheck"
if command -v shellcheck >/dev/null 2>&1; then
    for f in "${ALL_SCRIPTS[@]}"; do
        if shellcheck "$f" >/dev/null 2>&1; then
            pass "shellcheck ${f#"$REPO_ROOT"/}"
        else
            fail "shellcheck ${f#"$REPO_ROOT"/}"
        fi
    done
else
    echo "SKIP: shellcheck not installed"
fi

info "No hosted-Supabase commands and no bot start"
FORBIDDEN='supabase (login|link)|db push|--linked|--db-url|SUPABASE_ACCESS_TOKEN|cholobots\.py'
HITS=""
if [ "${#DEVCONTAINER_SCRIPTS[@]}" -gt 0 ]; then
    HITS=$(grep -nE "$FORBIDDEN" "${DEVCONTAINER_SCRIPTS[@]}" || true)
fi
if [ -z "$HITS" ]; then
    pass "no forbidden commands in scripts/devcontainer/*.sh"
else
    fail "forbidden commands found:"
    echo "$HITS"
fi

echo ""
if [ "$FAILURES" -gt 0 ]; then
    echo "RESULT: FAIL ($FAILURES check(s) failed)"
    exit 1
fi
echo "RESULT: PASS"
