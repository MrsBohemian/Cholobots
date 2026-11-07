#!/usr/bin/env bash
# ABOUTME: Structural validation tests for the integration-branch lookup
# ABOUTME: documented in the merge-train SKILL.md (epic ref, not internal id).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SKILL_FILE="$REPO_ROOT/.claude/skills/merge-train/SKILL.md"

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

FAILURES=0

pass() { echo -e "  ${GREEN}PASS: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; FAILURES=$((FAILURES + 1)); }

echo "=== merge-train integration-branch lookup tests ==="

# Test 1: SKILL.md exists
if [[ -f "$SKILL_FILE" ]]; then
    pass "SKILL.md exists"
else
    # The skill is ported separately; until it lands there is nothing to check.
    echo "SKIP: $SKILL_FILE not present yet (merge-train skill not ported)"
    exit 0
fi

# Test 2: No unresolved edit artifact in the placeholder
if grep -q 'epic_internal_id\.\.\.' "$SKILL_FILE"; then
    fail "Contains the garbled 'epic_internal_id...' placeholder"
else
    pass "No garbled 'epic_internal_id...' placeholder"
fi

# Test 3: The branch-name call is never passed the internal id
if grep -qE 'integration_branch_name\(<epic_internal_id' "$SKILL_FILE"; then
    fail "Passes <epic_internal_id> to integration_branch_name()"
else
    pass "Does not pass <epic_internal_id> to integration_branch_name()"
fi

# Test 4: The branch-name call takes the epic ref
if grep -qF 'integration_branch_name(<epic ref>, <story ref>)' "$SKILL_FILE"; then
    pass "Calls integration_branch_name(<epic ref>, <story ref>)"
else
    fail "Does not call integration_branch_name(<epic ref>, <story ref>)"
fi

# Test 5: Names the integration-branch convention
if grep -qF 'epic/<epic-ref>-integration-<story-ref>' "$SKILL_FILE"; then
    pass "Names the epic/<epic-ref>-integration-<story-ref> convention"
else
    fail "Does not name the epic/<epic-ref>-integration-<story-ref> convention"
fi

# Test 6: References the shared naming helper
if grep -qF 'scripts/lib/integration_branch.py' "$SKILL_FILE"; then
    pass "References scripts/lib/integration_branch.py"
else
    fail "Does not reference scripts/lib/integration_branch.py"
fi

echo ""
if [[ "$FAILURES" -eq 0 ]]; then
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}$FAILURES test(s) failed${NC}"
    exit 1
fi
