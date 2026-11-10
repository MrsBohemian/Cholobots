#!/usr/bin/env bash
# ABOUTME: Regression test for build-devcontainer.yml registry cache wiring (you-thon bug #2667).
# ABOUTME: Both build steps must export inline cache and seed from an image that carries it.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WORKFLOW="$REPO_ROOT/.github/workflows/build-devcontainer.yml"

PASS=0
FAIL=0

assert_true() {
  local test_name="$1" condition="$2"
  if eval "$condition"; then
    echo "  PASS: $test_name"
    PASS=$((PASS + 1))
  else
    echo "  FAIL: $test_name"
    FAIL=$((FAIL + 1))
  fi
}

# Extract one named step (from its "- name:" line to the next "- name:" or EOF).
extract_step() {
  local step_name="$1"
  awk -v step_name="$step_name" '
    $0 ~ "^      - name: " step_name "$" { in_step = 1; print; next }
    in_step && $0 ~ "^      - name: " { exit }
    in_step { print }
  ' "$WORKFLOW"
}

echo "=== build-devcontainer registry cache tests (you-thon bug #2667) ==="

base_step=$(extract_step "Build base devcontainer image")
prebuild_step=$(extract_step "Build pre-built devcontainer image")
push_step=$(extract_step "Push to GHCR")

assert_true "base build step exists" '[ -n "$base_step" ]'
assert_true "prebuild step exists" '[ -n "$prebuild_step" ]'

# The base Dockerfile's layers only get cache records if the base build itself
# exports inline cache; the prebuild's export does not cover its FROM image.
# Here-strings, not echo-to-grep pipes: under pipefail, grep -q's early exit can SIGPIPE the writer (you-thon bug #2791).
assert_true "base build exports inline cache" \
  'grep -q "BUILDKIT_INLINE_CACHE=1" <<< "$base_step"'
assert_true "prebuild exports inline cache" \
  'grep -q "BUILDKIT_INLINE_CACHE=1" <<< "$prebuild_step"'

# The base build must seed from the published base image, not from :latest
# (the prebuild image), whose inline cache has no keys for the base layers.
assert_true "base build seeds from the published :base image" \
  'grep -q -- "--cache-from \${{ env.REGISTRY }}/\${{ env.IMAGE_NAME }}:base" <<< "$base_step"'
assert_true "base build does not seed from :latest" \
  '! grep -q -- "--cache-from \${{ env.REGISTRY }}/\${{ env.IMAGE_NAME }}:latest" <<< "$base_step"'
assert_true "base build tags the published :base image" \
  'grep -q -- "-t \${{ env.REGISTRY }}/\${{ env.IMAGE_NAME }}:base" <<< "$base_step"'
assert_true "prebuild seeds from the published :latest image" \
  'grep -q -- "--cache-from \${{ env.REGISTRY }}/\${{ env.IMAGE_NAME }}:latest" <<< "$prebuild_step"'

# Both images must be pulled before building and pushed afterwards, or the
# next run has nothing to seed from.
assert_true "seed step pulls the :base image" \
  'grep -q "docker pull \${{ env.REGISTRY }}/\${{ env.IMAGE_NAME }}:base" "$WORKFLOW"'
assert_true "push step pushes the :base image" \
  'grep -q "docker push \${{ env.REGISTRY }}/\${{ env.IMAGE_NAME }}:base" <<< "$push_step"'

echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
