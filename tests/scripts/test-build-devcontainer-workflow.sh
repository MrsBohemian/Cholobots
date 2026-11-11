#!/usr/bin/env bash
# ABOUTME: Structural tests for build-devcontainer.yml: triggers, least-privilege permissions, PR-never-publishes,
# ABOUTME: SHA-pinned actions, build/check/push ordering and the version guard against pins.sh.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WORKFLOW="$REPO_ROOT/.github/workflows/build-devcontainer.yml"
PY="${PYTHON:-python3}"

FAILURES=0
pass() { echo "  PASS: $1"; }
fail() { echo "  FAIL: $1"; FAILURES=$((FAILURES + 1)); }

echo "=== build-devcontainer.yml workflow tests ==="

if [ ! -f "$WORKFLOW" ]; then
    fail "workflow file exists"
    exit 1
fi
pass "workflow file exists"

# Comments are dropped so prose about what the workflow must never do cannot satisfy or trip a check.
BODY=$(sed -E 's/[[:space:]]+#.*$//; /^[[:space:]]*#/d' "$WORKFLOW")

check() {
    local what="$1" condition="$2"
    if eval "$condition"; then pass "$what"; else fail "$what"; fi
}

# Structural checks on the parsed YAML; prints one "ok|FAIL: message" per line.
STRUCT=$("$PY" - "$WORKFLOW" <<'PYEOF'
import re
import sys

import yaml

doc = yaml.safe_load(open(sys.argv[1]))
out = []


def check(ok, msg):
    out.append(("ok: " if ok else "FAIL: ") + msg)


on = doc.get(True, doc.get("on"))  # PyYAML parses a bare `on` key as boolean True
jobs = doc["jobs"]
build = jobs.get("build-devcontainer", {})
publish = jobs.get("publish", {})

check(doc["name"] == "build-devcontainer", "workflow is named build-devcontainer")
check(build.get("name") == "build-devcontainer", "first job id and name are build-devcontainer")
check(set(on) == {"push", "pull_request", "schedule", "workflow_dispatch"}, "triggers are push, pull_request, schedule, workflow_dispatch")
check(on["push"]["branches"] == ["main"], "push is limited to main")
want_paths = {".devcontainer/**", "requirements*.txt", "tools/pytaiga-mcp/**", ".dockerignore", ".github/workflows/build-devcontainer.yml"}
check(set(on["push"]["paths"]) == want_paths, "push paths list")
check(set(on["pull_request"]["paths"]) == want_paths, "pull_request paths list")
check(on["schedule"] == [{"cron": "0 4 * * 0"}], "weekly cron Sunday 04:00 UTC")
check(doc["permissions"] == {"contents": "read"}, "top-level permissions are exactly contents: read")
check("permissions" not in build, "build job inherits contents: read only")
check(publish.get("permissions") == {"contents": "read", "packages": "write"}, "publish job alone has packages: write")
check([n for n, j in jobs.items() if "write" in str(j.get("permissions", ""))] == ["publish"], "no other job writes anything")
cond = publish.get("if", "")
check(cond.strip() == "github.event_name == 'push' && github.ref == 'refs/heads/main'", "publish runs only on a push to main")
check(publish.get("needs") == "build-devcontainer", "publish needs the build job")
check(doc["concurrency"]["cancel-in-progress"] is False and doc["concurrency"]["group"].startswith("build-devcontainer"), "concurrency does not cancel in progress")
check(doc["env"]["REGISTRY"] == "ghcr.io", "REGISTRY is ghcr.io")
check(doc["env"]["IMAGE_NAME"] == "mrsbohemian/cholobots/devcontainer" and doc["env"]["IMAGE_NAME"] == doc["env"]["IMAGE_NAME"].lower(), "IMAGE_NAME is the lowercase GHCR path")
for name, job in jobs.items():
    check(job["runs-on"] == "ubuntu-24.04", f"{name} runs on ubuntu-24.04")
    check(job["timeout-minutes"] == 60, f"{name} times out at 60 minutes")


def steps(job):
    return [s.get("name") or s.get("uses") for s in job["steps"]]


for name, job in jobs.items():
    names = steps(job)
    uses = [s["uses"] for s in job["steps"] if "uses" in s]
    check(all(re.fullmatch(r"[\w.-]+/[\w.-]+@[0-9a-f]{40}", u) for u in uses), f"{name}: every action is pinned to a full commit SHA")
    check(all("docker/login-action" not in u for u in uses) or name == "publish", f"{name}: only the publish job logs in to GHCR")
    check(all("continue-on-error" not in s for s in job["steps"]) and "continue-on-error" not in job, f"{name}: no continue-on-error")

pub = steps(publish)
bld = steps(build)
check("Push to GHCR" not in bld and not any("login-action" in str(s) for s in build["steps"]), "build job neither logs in nor pushes")
need = ["Build base devcontainer image", "Build pre-built devcontainer image", "Check tool versions match the pins", "Push to GHCR"]
check([n for n in pub if n in need] == need, "publish: base build, prebuild, version check, then push, in order")
check([n for n in bld if n in need] == need[:3], "build job: base build, prebuild, version check, in order")
runs = lambda job: {s["name"]: s.get("run") for s in job["steps"] if s.get("name") in need[:3]}
check(runs(build) == runs(publish), "build job runs the same build and version-check commands as publish")
login_idx = [i for i, s in enumerate(publish["steps"]) if "docker/login-action" in s.get("uses", "")]
check(login_idx == [1] and publish["steps"][1]["with"]["password"] == "${{ secrets.GITHUB_TOKEN }}" and publish["steps"][1]["with"]["username"] == "${{ github.actor }}", "publish logs in with github.actor and GITHUB_TOKEN")
print("\n".join(out))
PYEOF
) || { fail "structural parse of $WORKFLOW"; STRUCT=""; }

while IFS= read -r line; do
    case "$line" in
        ok:*) pass "${line#ok: }" ;;
        FAIL:*) fail "${line#FAIL: }" ;;
    esac
done <<< "$STRUCT"

check "prebuild tags :latest and the commit sha" \
    'grep -qF -- "-t \${{ env.REGISTRY }}/\${{ env.IMAGE_NAME }}:latest" <<< "$BODY" && grep -qF -- "-t \${{ env.REGISTRY }}/\${{ env.IMAGE_NAME }}:\${{ github.sha }}" <<< "$BODY"'
# The :base push is owned by test-build-devcontainer-cache.sh.
check "push pushes :latest and :sha" \
    'grep -qF "docker push \${{ env.REGISTRY }}/\${{ env.IMAGE_NAME }}:latest" <<< "$BODY" && grep -qF "docker push \${{ env.REGISTRY }}/\${{ env.IMAGE_NAME }}:\${{ github.sha }}" <<< "$BODY"'
check "version check reads the pins from pins.sh" 'grep -qF "eval \"\$(scripts/devcontainer/pins.sh)\"" <<< "$BODY"'
check "version check exits non-zero on mismatch" 'grep -qF "exit \"\$status\"" <<< "$BODY"'
check "only secrets.GITHUB_TOKEN is used" '[ "$(grep -o "secrets\.[A-Za-z_]*" <<< "$BODY" | sort -u)" = "secrets.GITHUB_TOKEN" ]'
check "no pull_request_target trigger" '! grep -q "pull_request_target" <<< "$BODY"'
check "never runs cholobots.py, --linked or --db-url" '! grep -qE "cholobots\.py|--linked|--db-url" <<< "$BODY"'

if command -v actionlint >/dev/null 2>&1; then
    if actionlint "$WORKFLOW"; then pass "actionlint is clean"; else fail "actionlint is clean"; fi
else
    echo "  SKIP: actionlint not installed"
fi

echo ""
if [ "$FAILURES" -eq 0 ]; then echo "All build-devcontainer workflow tests passed"; else echo "$FAILURES failed"; exit 1; fi
