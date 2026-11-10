#!/usr/bin/env bash
# ABOUTME: Static guard tests for the supabase-migrations-lint.yml workflow definition.
# ABOUTME: Keeps the PR-triggered lint secret-free, environment-free, read-only, and comparing against the PR base.
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
WORKFLOW="$REPO_ROOT/.github/workflows/supabase-migrations-lint.yml"

info "Testing $WORKFLOW"

if [ ! -f "$WORKFLOW" ]; then
    fail "workflow file exists"
    exit 1
fi
pass "workflow file exists"

# Comments are dropped so prose cannot satisfy or trip the structural checks.
BODY=$(sed -E 's/[[:space:]]+#.*$//; /^[[:space:]]*#/d' "$WORKFLOW")

check() {
    local what="$1"; shift
    if "$@"; then pass "$what"; else fail "$what"; fi
}
has() { grep -qE -- "$1" <<< "$BODY"; }
lacks() { ! grep -qE -- "$1" <<< "$BODY"; }

check "triggers on pull_request" has '^[[:space:]]+pull_request:'
# A path filter would leave a required check pending on PRs that skip migrations.
check "pull_request has no path filter" lacks '^[[:space:]]+paths(-ignore)?:'
# pull_request_target would run with secrets and write access on fork code.
check "no pull_request_target trigger" lacks 'pull_request_target'
check "no push or workflow_dispatch trigger" lacks '^[[:space:]]+(push|workflow_dispatch):'
check "no secrets context" lacks 'secrets\.'
check "no environment" lacks '^[[:space:]]+environment:'
check "no SUPABASE_* variables" lacks 'SUPABASE_'
check "no supabase CLI" lacks 'supabase/setup-cli|supabase (link|db)'
check "permissions are contents: read" has '^[[:space:]]+contents: read$'
check "no write permissions" lacks ': write'
check "no continue-on-error" lacks '^[[:space:]]*continue-on-error:'
check "full history checked out for the merge base" has 'fetch-depth: 0'
check "fetches the PR base ref" has 'git fetch .*\$\{BASE_REF\}'
check "base ref comes from github.base_ref" has 'BASE_REF: \$\{\{ github\.base_ref \}\}'
check "lints against the PR base" has 'supabase_migrations\.py lint --base "origin/\$\{BASE_REF\}"'

if [ "$FAILURES" -gt 0 ]; then
    echo -e "${RED}$FAILURES check(s) failed${NC}"
    exit 1
fi
echo -e "${GREEN}All checks passed${NC}"
