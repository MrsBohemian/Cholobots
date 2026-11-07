#!/usr/bin/env bash
# ABOUTME: Unit tests for scripts/taiga_closing_refs.py, the shared Taiga closing-ref extractor.
# ABOUTME: Validates that only claim-position refs (never a bare #ref mention) are extracted.
set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}✓ $1${NC}"; }
fail() { echo -e "${RED}✗ $1${NC}"; FAILURES=$((FAILURES + 1)); }
info() { echo -e "${YELLOW}→ $1${NC}"; }

FAILURES=0

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

extract_ref() {
    PR_TITLE="${1:-}" PR_BODY="${2:-}" PR_BRANCH="${3:-}" \
        python3 "$REPO_ROOT/scripts/taiga_closing_refs.py"
}

assert_ref() {
    local description="$1"
    local expected="$2"
    local actual="$3"

    if [ "$actual" = "$expected" ]; then
        pass "$description (got $actual)"
    else
        fail "$description: expected '$expected', got '$actual'"
    fi
}

# ============================================================
echo ""
echo "=== Taiga closing ref extraction tests ==="
echo ""

info "Regressions: bare mentions must not close anything"
assert_ref "Docs PR naming two stories in title (PR #839)" "" \
    "$(extract_ref "docs: sync #2204/#2206 design docs with analysis outcomes (fee basis, resolved open questions)" "" "docs/story-2206-fee-basis-recorded-fee")"
assert_ref "Docs title + epic ref branch (PR #838)" "" \
    "$(extract_ref "docs: sync epic notes" "" "design/epic-2167-fee-share-ui")"
assert_ref "Mid-sentence body mention" "" \
    "$(extract_ref "Some title" "Same defect class as PR #854 / bug #2265 -- a per-item failure..." "")"
assert_ref "Quoted bug description with closing verb" "" \
    "$(extract_ref "Some title" "PR #839 wrongly closed story #2204 and its 8 tasks" "")"
assert_ref "Dependabot changelog refs" "" \
    "$(extract_ref "deps(deps): bump the pip-minor-patch group with 2 updates" "* fix: something (#22986)" "dependabot/pip/pip-minor-patch-90ef8d58c2")"

info "Conventions that must keep working"
assert_ref "Story PR title prefix with colon" "2256" \
    "$(extract_ref "#2256: Accrual idempotency from the payables projection" "" "")"
assert_ref "Story PR title prefix without colon" "689" \
    "$(extract_ref "#689 Replace pagination" "" "")"
assert_ref "Conventional-commit scope" "2119" \
    "$(extract_ref "fix(#2119): name the curl exit code when Taiga login never reaches HTTP" "" "")"
assert_ref "Trailing parenthetical (bug)" "1350" \
    "$(extract_ref "fix: Dashboard layout (bug #1350)" "" "")"
assert_ref "Trailing parenthetical (story)" "221" \
    "$(extract_ref "feat: Component patterns (story #221)" "" "")"
assert_ref "Trailing parenthetical (bare)" "100" \
    "$(extract_ref "fix: something (#100)" "" "")"
assert_ref "Body-only standalone closing declaration with link" "2269" \
    "$(extract_ref "No ref in title" "Fixes bug [#2269](https://backlog.growthgarage.org/project/cholo-bots/issue/2269)" "")"
assert_ref "Body-only standalone closing declaration with parenthetical" "1282" \
    "$(extract_ref "No ref in title" "Fixes #1282 (CI skip detection)" "")"
assert_ref "Branch fallback: fix/slug-ref" "2269" \
    "$(extract_ref "" "" "fix/scan-reminder-isolation-2269")"
assert_ref "Branch fallback: feature/slug-ref" "2256" \
    "$(extract_ref "" "" "feature/accrual-idempotency-2256")"
assert_ref "Branch fallback: fix/taiga-sync-on-merge-1364" "1364" \
    "$(extract_ref "" "" "fix/taiga-sync-on-merge-1364")"
assert_ref "Branch fallback: fix/ref-slug (this repo's convention)" "4" \
    "$(extract_ref "" "" "fix/4-unknown-users-overwrite-heaven")"
assert_ref "Branch fallback: feature/ref-slug" "70" \
    "$(extract_ref "" "" "feature/70-pipeline-port")"
assert_ref "Branch fallback: bare feature/ref" "70" \
    "$(extract_ref "" "" "feature/70")"
assert_ref "Branch fallback: leading ref wins over trailing digits" "21" \
    "$(extract_ref "" "" "fix/21-retry-after-500")"

info "Priority: title claim wins over branch"
assert_ref "Title takes priority over branch" "100" \
    "$(extract_ref "fix: something (#100)" "" "fix/branch-200")"

info "Multi-ref: every claimed ref is returned"
assert_ref "Two standalone body declarations" "2204 2206" \
    "$(extract_ref "No ref in title" "Fixes #2204
Closes #2206" "")"

info "No ref found"
assert_ref "No ref in any source" "" \
    "$(extract_ref "no numbers" "also no numbers" "main")"

echo ""
if [ "$FAILURES" -gt 0 ]; then
    echo -e "${RED}${FAILURES} test(s) failed${NC}"
    exit 1
else
    echo -e "${GREEN}All tests passed${NC}"
    exit 0
fi
