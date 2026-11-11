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

# Extract every step with this name (each from its "- name:" line to the next "- name:" or
# the next job), one per line-block, separated by a "----" line. Both the build-devcontainer
# job and the publish job carry the build steps; the publish copy is the one that gets pushed.
extract_steps() {
  local step_name="$1"
  awk -v step_name="$step_name" '
    $0 ~ "^      - name: " step_name "$" { if (n++) print "----"; in_step = 1; print; next }
    in_step && ($0 ~ "^      - (name|uses): " || $0 ~ "^  [^ ]") { in_step = 0 }
    in_step { print }
  ' "$WORKFLOW"
}

# count_in <text> <fixed string>: number of lines in text containing the string.
count_in() { grep -cF -- "$2" <<< "$1" || true; }

echo "=== build-devcontainer registry cache tests (you-thon bug #2667) ==="

base_steps=$(extract_steps "Build base devcontainer image")
prebuild_steps=$(extract_steps "Build pre-built devcontainer image")
push_step=$(extract_steps "Push to GHCR")
BASE_REF='${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}:base'
LATEST_REF='${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}:latest'

# One copy in the build-devcontainer job, one in publish; every check below must hold for both.
assert_true "base build step exists in both jobs" '[ "$(count_in "$base_steps" "- name: Build base")" -eq 2 ]'
assert_true "prebuild step exists in both jobs" '[ "$(count_in "$prebuild_steps" "- name: Build pre-built")" -eq 2 ]'

# The base Dockerfile's layers only get cache records if the base build itself
# exports inline cache; the prebuild's export does not cover its FROM image.
# Here-strings, not echo-to-grep pipes: under pipefail, grep -q's early exit can SIGPIPE the writer (you-thon bug #2791).
assert_true "both base builds export inline cache" \
  '[ "$(count_in "$base_steps" "BUILDKIT_INLINE_CACHE=1")" -eq 2 ]'
assert_true "both prebuilds export inline cache" \
  '[ "$(count_in "$prebuild_steps" "BUILDKIT_INLINE_CACHE=1")" -eq 2 ]'

# The base build must seed from the published base image, not from :latest
# (the prebuild image), whose inline cache has no keys for the base layers.
assert_true "both base builds seed from the published :base image" \
  '[ "$(count_in "$base_steps" "--cache-from $BASE_REF")" -eq 2 ]'
assert_true "no base build seeds from :latest" \
  '[ "$(count_in "$base_steps" "--cache-from $LATEST_REF")" -eq 0 ]'
assert_true "both base builds tag the published :base image" \
  '[ "$(count_in "$base_steps" "-t $BASE_REF")" -eq 2 ]'
assert_true "both prebuilds seed from the published :latest image" \
  '[ "$(count_in "$prebuild_steps" "--cache-from $LATEST_REF")" -eq 2 ]'

# Both images must be pulled before building and pushed afterwards, or the
# next run has nothing to seed from.
assert_true "seed step pulls the :base image" \
  'grep -qF "docker pull $BASE_REF" "$WORKFLOW"'
assert_true "push step pushes the :base image" \
  'grep -qF "docker push $BASE_REF" <<< "$push_step"'

echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
