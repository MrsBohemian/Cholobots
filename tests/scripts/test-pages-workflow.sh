#!/usr/bin/env bash
# ABOUTME: Unit tests for the pages.yml workflow definition itself.
# ABOUTME: Guards that only mobile/ is published, the least-privilege permissions, SHA-pinned actions, and the PAGES_ENABLED skip.
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
WORKFLOW="$REPO_ROOT/.github/workflows/pages.yml"

info "Testing $WORKFLOW"

if [ ! -f "$WORKFLOW" ]; then
    fail "workflow file exists"
    echo ""
    echo -e "${RED}$FAILURES test(s) failed${NC}"
    exit 1
fi
pass "workflow file exists"

# Comments are dropped before the structural checks so prose about what the
# workflow must never do cannot satisfy or trip them.
BODY=$(sed -E 's/[[:space:]]+#.*$//; /^[[:space:]]*#/d' "$WORKFLOW")
NORMALIZED=$(sed -E 's/^[[:space:]]+//' <<< "$BODY")

expect_line() {
    local line="$1" what="$2"
    if grep -qxF "$line" <<< "$NORMALIZED"; then
        pass "$what"
    else
        fail "$what (missing line: $line)"
    fi
}

forbid_pattern() {
    local pattern="$1" what="$2"
    if grep -qE "$pattern" <<< "$BODY"; then
        fail "$what"
        grep -nE "$pattern" <<< "$BODY"
    else
        pass "$what"
    fi
}

# Only mobile/ may reach the public site: .claude/, docs/ and scripts/ must never be uploaded.
expect_line "path: mobile" "upload-pages-artifact publishes path: mobile"
forbid_pattern '^[[:space:]]*path:[[:space:]]*\.?/?[[:space:]]*$' "no artifact path of the repo root"

ON_BLOCK=$(sed -n '/^on:/,/^[a-z]/p' <<< "$BODY")
if grep -qE '^[[:space:]]+push:' <<< "$ON_BLOCK" \
    && grep -qE '^[[:space:]]+branches: \[main\]' <<< "$ON_BLOCK" \
    && grep -qF "mobile/**" <<< "$ON_BLOCK"; then
    pass "push is limited to main and mobile/**"
else
    fail "push is limited to main and mobile/**"
fi
if grep -qE '^[[:space:]]+workflow_dispatch:' <<< "$ON_BLOCK"; then
    pass "workflow_dispatch is present"
else
    fail "workflow_dispatch is present"
fi
for trigger in pull_request pull_request_target workflow_run issue_comment; do
    forbid_pattern "(^|[^a-z_])${trigger}([^a-z_]|$)" "no '$trigger' trigger"
done

expect_line "pages: write" "pages: write is granted"
expect_line "id-token: write" "id-token: write is granted"
forbid_pattern 'contents:[[:space:]]*write' "contents: write is absent"
forbid_pattern 'permissions:[[:space:]]*write-all' "write-all permissions are absent"

# The default token stays read-only; only the deploy job may publish.
TOP_PERMS=$(sed -n '/^permissions:/,/^[a-z]/p' <<< "$BODY")
if grep -qF 'contents: read' <<< "$TOP_PERMS" \
    && ! grep -qE 'pages:|id-token:' <<< "$TOP_PERMS"; then
    pass "top-level permissions are contents: read only"
else
    fail "top-level permissions are contents: read only"
fi

expect_line "name: github-pages" "environment is github-pages"
forbid_pattern '^[[:space:]]*continue-on-error:' "no continue-on-error (a failed deploy must fail the run)"
expect_line "group: pages" "concurrency group is pages"
expect_line "cancel-in-progress: false" "a running Pages deploy is never cancelled"
forbid_pattern 'enablement:' "no enablement (needs an admin token)"

# Every action is pinned to a full commit SHA; a tag can be moved.
USES=$(grep -E '^[[:space:]]*-?[[:space:]]*uses:' <<< "$BODY" || true)
if [ -z "$USES" ]; then
    fail "workflow uses actions"
else
    UNPINNED=$(grep -vE 'uses:[[:space:]]+[A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+@[0-9a-f]{40}[[:space:]]*$' <<< "$USES" || true)
    if [ -z "$UNPINNED" ]; then
        pass "every uses: is pinned to a 40-hex commit SHA"
    else
        fail "every uses: is pinned to a 40-hex commit SHA"
        echo "$UNPINNED"
    fi
fi
# The version comment does not survive the strip above, so read it from the raw file.
# A here-string, not a pipe: under pipefail a SIGPIPE in the first grep would read as a pass.
RAW_USES=$(grep -E '^[[:space:]]*-?[[:space:]]*uses:' "$WORKFLOW" || true)
if grep -qvE '@[0-9a-f]{40}[[:space:]]+#[[:space:]]*v[0-9]' <<< "$RAW_USES"; then
    fail "every pinned action carries a version comment"
else
    pass "every pinned action carries a version comment"
fi

# Pages is switched on by hand after merge; until then the run must stay green.
DEPLOY_JOB=$(sed -n '/^  deploy:/,/^  [a-z-]*:$/p' <<< "$BODY")
DISABLED_JOB=$(sed -n '/^  pages-disabled:/,$p' <<< "$BODY")
if grep -qF "if: vars.PAGES_ENABLED == 'true'" <<< "$DEPLOY_JOB"; then
    pass "deploy job runs only when vars.PAGES_ENABLED == 'true'"
else
    fail "deploy job runs only when vars.PAGES_ENABLED == 'true'"
fi
if grep -qF "if: vars.PAGES_ENABLED != 'true'" <<< "$DISABLED_JOB"; then
    pass "pages-disabled job runs when vars.PAGES_ENABLED != 'true'"
else
    fail "pages-disabled job runs when vars.PAGES_ENABLED != 'true'"
fi
if grep -qF '::notice::' <<< "$DISABLED_JOB"; then
    pass "pages-disabled job emits a ::notice::"
else
    fail "pages-disabled job emits a ::notice::"
fi
if grep -qE 'pages:|id-token:' <<< "$DISABLED_JOB"; then
    fail "pages-disabled job gets no publish permissions"
else
    pass "pages-disabled job gets no publish permissions"
fi

# Structural check when PyYAML is available; the greps above stand alone without it.
if python3 -c 'import yaml' 2>/dev/null; then
    if python3 - "$WORKFLOW" <<'PY'
import sys
import yaml

doc = yaml.safe_load(open(sys.argv[1]))
on = doc.get("on", doc.get(True))  # PyYAML reads a bare `on` key as True
assert set(on) == {"push", "workflow_dispatch"}, on
assert on["push"]["branches"] == ["main"], on
assert "mobile/**" in on["push"]["paths"], on
assert doc["permissions"] == {"contents": "read"}, doc["permissions"]
deploy = doc["jobs"]["deploy"]
assert deploy["permissions"]["pages"] == "write"
assert deploy["permissions"]["id-token"] == "write"
assert deploy["if"] == "vars.PAGES_ENABLED == 'true'", deploy["if"]
assert deploy["environment"]["name"] == "github-pages"
assert doc["jobs"]["pages-disabled"]["if"] == "vars.PAGES_ENABLED != 'true'"
upload = [s for s in deploy["steps"] if s.get("uses", "").startswith("actions/upload-pages-artifact@")]
assert len(upload) == 1 and upload[0]["with"]["path"] == "mobile", upload
PY
    then
        pass "YAML parses and has the expected triggers, permissions, jobs and artifact path"
    else
        fail "YAML parses and has the expected triggers, permissions, jobs and artifact path"
    fi
else
    info "PyYAML not installed; structural YAML check skipped"
fi

echo ""
if [ "$FAILURES" -eq 0 ]; then
    echo -e "${GREEN}All tests passed${NC}"
    exit 0
else
    echo -e "${RED}$FAILURES test(s) failed${NC}"
    exit 1
fi
