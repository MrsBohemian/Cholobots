#!/usr/bin/env bash
# ABOUTME: Static tests for the CI workflow definitions, checks.yml and integration.yml.
# ABOUTME: Guards names, triggers, pins, permissions and the commands each job must run.
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
WORKFLOWS="$REPO_ROOT/.github/workflows"

# Comments are dropped before the checks so prose about what a workflow must
# never do cannot satisfy or trip them.
strip_comments() {
    sed -E 's/[[:space:]]+#.*$//; /^[[:space:]]*#/d' "$1"
}

# Here-strings, not echo-to-grep pipes: under pipefail, grep -q's early exit can SIGPIPE the writer (bug #2791).
expect_match() {
    local body="$1" pattern="$2" what="$3"
    if grep -qE -- "$pattern" <<< "$body"; then pass "$what"; else fail "$what"; fi
}

expect_no_match() {
    local body="$1" pattern="$2" what="$3"
    if grep -qE -- "$pattern" <<< "$body"; then
        fail "$what"
        grep -nE -- "$pattern" <<< "$body" || true
    else
        pass "$what"
    fi
}

# Rules every CI workflow shares. $1 is the workflow and job name.
check_common() {
    local name="$1" file="$WORKFLOWS/$1.yml" body on_block pr_block
    info "Testing $file"
    if [ ! -f "$file" ]; then
        fail "$name.yml exists"
        return
    fi
    pass "$name.yml exists"
    body=$(strip_comments "$file")

    # scripts/monitor-pr-stability.sh matches branch-protection contexts (job
    # names) against workflow-run names, so the two must be identical.
    expect_match "$body" "^name: $name\$" "$name.yml: workflow is named '$name'"
    expect_match "$body" "^  $name:\$" "$name.yml: job id is '$name'"
    expect_match "$body" "^    name: $name\$" "$name.yml: job is named '$name'"

    on_block=$(sed -n '/^on:/,/^[a-z]/p' <<< "$body")
    pr_block=$(sed -n '/^  pull_request:/,/^  [a-z]/p' <<< "$on_block" | sed '$d')
    expect_match "$on_block" '^  pull_request:' "$name.yml: runs on pull_request"
    # A branch filter would skip stacked PRs into feature/* and epic/*; a path
    # filter leaves a required context unreported and blocks the PR forever.
    expect_no_match "$pr_block" '(branches|branches-ignore|paths|paths-ignore):' "$name.yml: pull_request has no branch or path filter"
    expect_no_match "$on_block" '(paths|paths-ignore):' "$name.yml: no path filters"
    expect_match "$on_block" '^  push:' "$name.yml: runs on push"
    expect_match "$on_block" '^    branches: \[main\]$' "$name.yml: push is limited to main"
    expect_no_match "$on_block" 'pull_request_target' "$name.yml: no pull_request_target trigger"

    expect_no_match "$body" '^[[:space:]]*continue-on-error:' "$name.yml: no continue-on-error (a failed step must fail the run)"
    expect_no_match "$body" 'secrets\.' "$name.yml: uses no secrets"
    expect_match "$body" '^permissions:$' "$name.yml: sets top-level permissions"
    expect_match "$body" '^  contents: read$' "$name.yml: permissions are contents: read"
    expect_match "$body" 'python-version-file: \.python-version$' "$name.yml: Python comes from .python-version"
    expect_no_match "$body" '^[[:space:]]*python-version:' "$name.yml: no hardcoded python-version"
    expect_match "$body" 'pip install -r requirements\.txt -r requirements-dev\.txt$' "$name.yml: installs the pinned requirements"

    # Every action is pinned to a full commit SHA (with the tag in a comment).
    local uses unpinned
    uses=$(grep -E '^[[:space:]]*-?[[:space:]]*uses:' <<< "$body" || true)
    if [ -z "$uses" ]; then
        fail "$name.yml: uses at least one action"
    else
        unpinned=$(grep -vE 'uses: [A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+@[0-9a-f]{40}$' <<< "$uses" || true)
        if [ -z "$unpinned" ]; then
            pass "$name.yml: every action is pinned to a commit SHA"
        else
            fail "$name.yml: every action is pinned to a commit SHA"
            echo "$unpinned"
        fi
    fi
    if grep -qE 'uses: [^ ]+@[0-9a-f]{40} # v[0-9]+\.[0-9]+\.[0-9]+$' "$file" \
        && ! grep -E '^[[:space:]]*-?[[:space:]]*uses:' "$file" | grep -vqE '@[0-9a-f]{40} # v[0-9]+\.[0-9]+\.[0-9]+$'; then
        pass "$name.yml: every pinned SHA names its release tag"
    else
        fail "$name.yml: every pinned SHA names its release tag"
    fi

    # Importing or running cholobots.py starts the real bot.
    expect_no_match "$body" 'python3? (-[a-zA-Z]+ )*cholobots\.py|import cholobots' "$name.yml: never runs or imports cholobots"
}

check_common checks
CHECKS=$(strip_comments "$WORKFLOWS/checks.yml" 2>/dev/null || true)
expect_match "$CHECKS" 'make check-tools$' "checks.yml: checks the shell-test tools like make test"
expect_match "$CHECKS" 'python -m py_compile cholobots\.py config\.py db/database\.py bots/\*\.py$' "checks.yml: compiles every bot module"
expect_match "$CHECKS" 'ruff check' "checks.yml: runs ruff"
expect_match "$CHECKS" 'python -m pytest -q tests/unit$' "checks.yml: runs the unit tests"
expect_match "$CHECKS" 'python -m pytest -q tests/scripts$' "checks.yml: runs the script pytest suite"
expect_match "$CHECKS" 'for t in tests/scripts/test-\*\.sh; do' "checks.yml: runs every tests/scripts/test-*.sh"
for step in Compile Lint 'Unit tests' 'Script tests'; do
    expect_match "$CHECKS" "^      - name: $step\$" "checks.yml: has a '$step' step"
done

check_common integration
INTEGRATION=$(strip_comments "$WORKFLOWS/integration.yml" 2>/dev/null || true)
MAKE_CLI_VERSION=$(sed -nE 's/^SUPABASE_CLI_VERSION := ([0-9.]+)$/\1/p' "$REPO_ROOT/Makefile")
expect_match "$INTEGRATION" 'uses: supabase/setup-cli@[0-9a-f]{40}$' "integration.yml: installs the CLI with supabase/setup-cli"
CLI_VERSION=$(sed -nE 's/^[[:space:]]+version: ([0-9][0-9.]*)$/\1/p' <<< "$INTEGRATION")
if [ -n "$MAKE_CLI_VERSION" ] && [ "$CLI_VERSION" = "$MAKE_CLI_VERSION" ]; then
    pass "integration.yml: pins the Supabase CLI to the Makefile's $MAKE_CLI_VERSION"
else
    fail "integration.yml: pins the Supabase CLI to the Makefile's version (workflow '${CLI_VERSION:-none}', Makefile '${MAKE_CLI_VERSION:-none}')"
fi
expect_match "$INTEGRATION" 'supabase start -x ' "integration.yml: starts the local Supabase stack"
for service in gotrue realtime storage-api kong postgrest; do
    expect_no_match "$(grep -E 'supabase start' <<< "$INTEGRATION")" "[ ,]$service(,|\$)" "integration.yml: keeps $service running"
done
expect_match "$INTEGRATION" 'make test-integration SUPABASE=supabase VENV_PY=python$' "integration.yml: runs make test-integration with the job's CLI and Python"
# The job env must leave SUPABASE_* unset: make test-integration only fills
# them from `supabase status` when they are empty.
expect_no_match "$INTEGRATION" '^[[:space:]]+SUPABASE_[A-Z_]+:' "integration.yml: sets no SUPABASE_* env (make reads them from the local stack)"
MAKE_RECIPE=$(sed -n '/^test-integration:/,/^[a-z]/p' "$REPO_ROOT/Makefile")
expect_match "$MAKE_RECIPE" 'CHOLOBOTS_INTEGRATION_REQUIRED=1 \$\(VENV_PY\) -m pytest -q -m integration tests/integration' "Makefile: test-integration fails instead of skipping and runs the integration marker"

# Structural check when PyYAML is available; the greps above stand alone without it.
if python3 -c 'import yaml' 2>/dev/null; then
    if python3 - "$WORKFLOWS" <<'PY'
import sys
from pathlib import Path

import yaml

for name in ("checks", "integration"):
    doc = yaml.safe_load((Path(sys.argv[1]) / f"{name}.yml").read_text())
    on = doc.get("on", doc.get(True))  # PyYAML reads a bare `on` key as True
    assert set(on) == {"pull_request", "push"}, on
    assert not on["pull_request"], on["pull_request"]
    assert on["push"] == {"branches": ["main"]}, on["push"]
    assert doc["name"] == name, doc["name"]
    assert list(doc["jobs"]) == [name], list(doc["jobs"])
    job = doc["jobs"][name]
    assert job["name"] == name, job["name"]
    assert doc["permissions"] == {"contents": "read"}, doc["permissions"]
    assert job["runs-on"] == "ubuntu-latest", job["runs-on"]
    for step in job["steps"]:
        assert "continue-on-error" not in step, step
PY
    then
        pass "YAML parses with the expected triggers, names and permissions"
    else
        fail "YAML parses with the expected triggers, names and permissions"
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
