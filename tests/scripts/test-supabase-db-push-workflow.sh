#!/usr/bin/env bash
# ABOUTME: Static guard tests for the supabase-db-push.yml workflow definition.
# ABOUTME: Keeps the production push gated closed (variable plus protected environment), PR-trigger free, and plan-checked.
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
WORKFLOW="$REPO_ROOT/.github/workflows/supabase-db-push.yml"

info "Testing $WORKFLOW"

if [ ! -f "$WORKFLOW" ]; then
    fail "workflow file exists"
    exit 1
fi
pass "workflow file exists"

# Comments are dropped so prose about what the workflow must never do cannot
# satisfy or trip the structural checks.
BODY=$(sed -E 's/[[:space:]]+#.*$//; /^[[:space:]]*#/d' "$WORKFLOW")
# Here-strings throughout: echo | grep -q under pipefail fails on SIGPIPE.
NORMALIZED=$(sed -E 's/^[[:space:]]+//' <<< "$BODY")

has_line() { grep -qxF "$1" <<< "$NORMALIZED"; }

expect_line() {
    local line="$1" what="$2"
    if has_line "$line"; then pass "$what"; else fail "$what (missing line: $line)"; fi
}

# The repo is public and the job holds production credentials: no trigger a PR can fire.
for trigger in pull_request pull_request_target; do
    if grep -qE "(^|[^a-z_])${trigger}([^a-z_]|$)" <<< "$BODY"; then
        fail "no '$trigger' trigger"
    else
        pass "no '$trigger' trigger"
    fi
done

ON_BLOCK=$(sed -n '/^on:/,/^[a-z]/p' <<< "$BODY")
if grep -qE '^[[:space:]]+push:' <<< "$ON_BLOCK" \
    && grep -qE '^[[:space:]]+branches: \[main\]' <<< "$ON_BLOCK" \
    && grep -qF "'supabase/migrations/**'" <<< "$ON_BLOCK"; then
    pass "push to main limited to supabase/migrations/**"
else
    fail "push to main limited to supabase/migrations/**"
fi
if grep -qE '^[[:space:]]+workflow_dispatch:' <<< "$ON_BLOCK"; then
    pass "workflow_dispatch trigger present"
else
    fail "workflow_dispatch trigger present"
fi

DBPUSH=$(sed -n '/^  db-push:/,$p' <<< "$BODY")
GATE=$(sed -n '/^  gate-closed:/,/^  db-push:/p' <<< "$BODY")

if grep -qF "if: vars.SUPABASE_DB_PUSH_ENABLED == 'true'" <<< "$DBPUSH"; then
    pass "db-push is guarded by SUPABASE_DB_PUSH_ENABLED == 'true'"
else
    fail "db-push is guarded by SUPABASE_DB_PUSH_ENABLED == 'true'"
fi
if grep -qE '^[[:space:]]+environment: supabase-production$' <<< "$DBPUSH"; then
    pass "db-push uses the supabase-production environment"
else
    fail "db-push uses the supabase-production environment"
fi
if grep -qF "if: vars.SUPABASE_DB_PUSH_ENABLED != 'true'" <<< "$GATE" && grep -qF '::notice::' <<< "$GATE"; then
    pass "gate-closed runs when the variable is not 'true' and emits a notice"
else
    fail "gate-closed runs when the variable is not 'true' and emits a notice"
fi

expect_line "version: 2.121.0" "Supabase CLI pinned to 2.121.0"
expect_line "contents: read" "permissions are contents: read"
expect_line "cancel-in-progress: false" "concurrency never cancels a running push"
expect_line "group: supabase-db-push" "concurrency group supabase-db-push"

if grep -qE '^[[:space:]]*continue-on-error:' <<< "$BODY"; then
    fail "no continue-on-error (a failed step must fail the run)"
else
    pass "no continue-on-error (a failed step must fail the run)"
fi

# Order: dry-run, then plan check, then the real push.
# grep -m1 instead of | head (SIGPIPE under pipefail), and || true so a missing
# line reports as a failed check instead of aborting the script under set -e.
line_of() { { grep -m1 -nF -- "$1" <<< "$BODY" || true; } | cut -d: -f1; }
DRY=$(line_of "--dry-run")
CHECK=$(line_of "check-push-plan")
PUSH=$(line_of "name: Push migrations")
if [ -n "$DRY" ] && [ -n "$CHECK" ] && [ -n "$PUSH" ] && [ "$DRY" -lt "$PUSH" ] && [ "$CHECK" -lt "$PUSH" ]; then
    pass "dry-run and check-push-plan come before Push migrations"
else
    fail "dry-run and check-push-plan come before Push migrations"
fi

# Credentials only via the secrets context; never a literal.
BAD_SECRET=0
while IFS= read -r l; do
    if ! grep -qE '\$\{\{ secrets\.[A-Z_]+ \}\}' <<< "$l"; then BAD_SECRET=1; fi
done < <(grep -E '^[[:space:]]*SUPABASE_(ACCESS_TOKEN|DB_PASSWORD|PROJECT_REF):' <<< "$BODY")
if [ "$BAD_SECRET" -eq 0 ]; then
    pass "SUPABASE_* credentials come only from secrets.*"
else
    fail "SUPABASE_* credentials come only from secrets.*"
fi

if [ "$FAILURES" -gt 0 ]; then
    echo -e "${RED}$FAILURES check(s) failed${NC}"
    exit 1
fi
echo -e "${GREEN}All checks passed${NC}"
