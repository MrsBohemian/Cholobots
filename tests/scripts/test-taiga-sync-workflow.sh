#!/usr/bin/env bash
# ABOUTME: Unit tests for the taiga-sync.yml workflow definition itself.
# ABOUTME: Guards that a failed Taiga auth or item update turns the run red and that PR labels reach the update script.
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

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WORKFLOW="$REPO_ROOT/.github/workflows/taiga-sync.yml"

info "Testing $WORKFLOW"

if [ ! -f "$WORKFLOW" ]; then
    fail "workflow file exists"
    exit 1
fi
pass "workflow file exists"

# A sync that cannot sync must not look green: continue-on-error at any level
# would mask a failed login or a failed item update as a successful run.
if grep -qE '^[[:space:]]*continue-on-error:' "$WORKFLOW"; then
    fail "taiga-sync.yml has no continue-on-error (a failed step must fail the run)"
    grep -nE '^[[:space:]]*continue-on-error:' "$WORKFLOW"
else
    pass "taiga-sync.yml has no continue-on-error (a failed step must fail the run)"
fi

# Without the mask, the benign "PR carries no Taiga ref" path must still be
# green — it stays green only because both Taiga steps are skipped.
for step in "Verify Taiga login" "Update Taiga item"; do
    guard=$(grep -A1 "name: $step" "$WORKFLOW" | grep -c "if: steps.extract.outputs.found == 'true'" || true)
    if [ "$guard" -eq 1 ]; then
        pass "'$step' is guarded on found == 'true'"
    else
        fail "'$step' is guarded on found == 'true'"
    fi
done

# A pre-HTTP curl failure is ambiguous on its own — DNS, TLS, and a malformed
# URL all look identical without the exit code, which sent a real investigation
# after the wrong cause. The message must name it.
if grep -q 'curl exit \$CURL_EXIT_CODE' "$WORKFLOW"; then
    pass "connectivity error names the curl exit code"
else
    fail "connectivity error names the curl exit code"
fi

# Case labels are literal text, and one of them ('35|60') contains a regex
# metacharacter — so normalize the indentation away and match literally rather
# than building a pattern out of the label.
NORMALIZED=$(sed -E 's/^[[:space:]]+//; s/\)[[:space:]]+DIAGNOSIS=/) DIAGNOSIS=/' "$WORKFLOW")

for label in '6' '7' '28' '35|60'; do
    # Here-strings, not echo-to-grep pipes: under pipefail, grep -q's early exit can SIGPIPE the writer (bug #2791).
    if grep -qF "${label}) DIAGNOSIS=" <<< "$NORMALIZED"; then
        pass "curl exit '$label' is translated to a diagnosis"
    else
        fail "curl exit '$label' is translated to a diagnosis"
    fi
done

# The inline extractor treated any bare #ref mention as a claim (bug #2237).
# It must not creep back in — extraction has to go through the shared script.
if grep -qE "grep -oP '#\\\\K\\\\d\\+'" "$WORKFLOW"; then
    fail "taiga-sync.yml has no inline bare-#ref extractor"
    grep -nE "grep -oP '#\\\\K\\\\d\\+'" "$WORKFLOW"
else
    pass "taiga-sync.yml has no inline bare-#ref extractor"
fi

if grep -q 'scripts/taiga_closing_refs.py' "$WORKFLOW"; then
    pass "taiga-sync.yml invokes scripts/taiga_closing_refs.py"
else
    fail "taiga-sync.yml invokes scripts/taiga_closing_refs.py"
fi

# A script the workflow calls but never checks out only fails at runtime, on
# a real merge -- the sparse-checkout list must carry it too.
if grep -A3 'sparse-checkout:' "$WORKFLOW" | grep -q 'scripts/taiga_closing_refs.py'; then
    pass "sparse-checkout lists scripts/taiga_closing_refs.py"
else
    fail "sparse-checkout lists scripts/taiga_closing_refs.py"
fi

# The update script's side effects are opt-in via the merged PR's labels, so
# the step must forward them from the event payload (no extra token needed).
UPDATE_STEP=$(sed -n '/name: Update Taiga item/,/run: |/p' "$WORKFLOW")
if grep -qF "PR_LABELS: \${{ join(github.event.pull_request.labels.*.name, ' ') }}" <<< "$UPDATE_STEP"; then
    pass "'Update Taiga item' forwards PR_LABELS from github.event.pull_request.labels.*.name"
else
    fail "'Update Taiga item' forwards PR_LABELS from github.event.pull_request.labels.*.name"
fi

echo ""
if [ "$FAILURES" -eq 0 ]; then
    echo -e "${GREEN}All tests passed${NC}"
    exit 0
else
    echo -e "${RED}$FAILURES test(s) failed${NC}"
    exit 1
fi
