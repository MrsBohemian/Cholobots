#!/usr/bin/env bash
# ABOUTME: Verifies exec-skill.sh dispatch for Claude and Codex, both reading .claude/skills,
# ABOUTME: with a stub docker so no real container is used.
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/../.." && pwd)
SCRIPT="$REPO_ROOT/scripts/devcontainer/exec-skill.sh"
FIXTURE=$(mktemp -d)
trap 'rm -rf "$FIXTURE"' EXIT

CLONE="$FIXTURE/consumer"
BIN="$FIXTURE/bin"
mkdir -p "$CLONE/.claude/skills/sample" "$BIN"

cat > "$CLONE/.claude/skills/sample/SKILL.md" <<'SKILL'
---
name: sample
---
Body: $ARGUMENTS
SKILL
cat > "$BIN/docker" <<'STUB'
#!/usr/bin/env bash
if [ "$1" = ps ]; then
    printf '%s\n' "$@" > "${FAKE_DOCKER_PS_ARGS:-/dev/null}"
    echo fake-container
    exit 0
fi
if [ "$1" = inspect ]; then
    printf '%s' "${FAKE_DOCKER_MOUNTS:-[]}"
    exit 0
fi
printf '%s\n' "$@" > "$FAKE_DOCKER_ARGS"
cat > "$FAKE_DOCKER_STDIN"
echo '{"type":"fake.event"}'
STUB
chmod +x "$BIN/docker"

export PATH="$BIN:$PATH"
if [ "$(command -v docker)" != "$BIN/docker" ]; then
    echo "docker does not resolve to the stub; refusing to run" >&2
    exit 1
fi
export FAKE_DOCKER_ARGS="$FIXTURE/docker-args"
export FAKE_DOCKER_STDIN="$FIXTURE/docker-stdin"

assert_contains() {
    if ! grep -Fq -- "$2" "$1"; then
        echo "FAIL: expected '$2' in $1" >&2
        exit 1
    fi
}
assert_absent() {
    if grep -Fq -- "$2" "$1"; then
        echo "FAIL: unexpected '$2' in $1" >&2
        exit 1
    fi
}

# Default provider is Claude.
"$SCRIPT" "$CLONE" sample 'bug 42' --log "$FIXTURE/claude.log"
assert_contains "$FAKE_DOCKER_ARGS" 'claude --dangerously-skip-permissions -p - --output-format stream-json --verbose'
assert_contains "$FAKE_DOCKER_ARGS" '.venv/bin'
assert_contains "$FAKE_DOCKER_STDIN" 'Body: bug 42'
assert_absent "$FAKE_DOCKER_STDIN" 'name: sample'
assert_contains "$FAKE_DOCKER_ARGS" "/workspaces/$(basename "$CLONE")"

# A worktree's workspace is the container mount whose source is the checkout path.
REAL_CLONE="$(cd "$CLONE" && pwd -P)"
FAKE_DOCKER_MOUNTS="[{\"Source\":\"$REAL_CLONE\",\"Destination\":\"/workspaces/.claude/worktrees/wt\"}]" \
    "$SCRIPT" "$CLONE" sample 'bug 42' --log "$FIXTURE/mount.log"
assert_contains "$FAKE_DOCKER_ARGS" '/workspaces/.claude/worktrees/wt'
assert_contains "$FIXTURE/claude.log" 'fake.event'

# Codex reads the same .claude/skills file and gets an explicit instruction prefix.
"$SCRIPT" "$CLONE" sample 'bug 43 --mark-done-on-merge' --provider codex --log "$FIXTURE/codex.log"
assert_contains "$FAKE_DOCKER_ARGS" 'codex exec --dangerously-bypass-approvals-and-sandbox --json -'
assert_contains "$FAKE_DOCKER_STDIN" 'Follow these skill instructions with these arguments: bug 43 --mark-done-on-merge'
assert_contains "$FAKE_DOCKER_STDIN" 'Body: bug 43 --mark-done-on-merge'
assert_absent "$FAKE_DOCKER_STDIN" 'name: sample'
assert_contains "$FIXTURE/codex.log" 'fake.event'
if [ "$(head -n 1 "$FAKE_DOCKER_STDIN")" != 'Follow these skill instructions with these arguments: bug 43 --mark-done-on-merge' ] \
    || [ -n "$(sed -n 2p "$FAKE_DOCKER_STDIN")" ]; then
    echo 'FAIL: codex prompt must start with the instruction line then a blank line' >&2
    exit 1
fi

# Dry run launches nothing.
rm "$FAKE_DOCKER_ARGS" "$FAKE_DOCKER_STDIN"
"$SCRIPT" "$CLONE" sample 44 --provider codex --dry-run > "$FIXTURE/dry-run"
assert_contains "$FIXTURE/dry-run" 'codex exec --dangerously-bypass-approvals-and-sandbox --json -'
if [ -e "$FAKE_DOCKER_ARGS" ] || [ -e "$FAKE_DOCKER_STDIN" ]; then
    echo 'FAIL: dry run launched an agent' >&2
    exit 1
fi

if "$SCRIPT" "$CLONE" sample 45 --provider unknown > "$FIXTURE/invalid" 2>&1; then
    echo 'FAIL: unknown provider was accepted' >&2
    exit 1
fi
assert_contains "$FIXTURE/invalid" 'requires claude or codex'

if "$SCRIPT" "$CLONE" missing 46 > "$FIXTURE/missing" 2>&1; then
    echo 'FAIL: missing skill was accepted' >&2
    exit 1
fi
assert_contains "$FIXTURE/missing" '.claude/skills/missing/SKILL.md'

echo 'PASS: Claude default, Codex dispatch, venv prefix'

# A symlinked clone path is resolved to the physical path used for the container label.
ln -s "$CLONE" "$FIXTURE/consumer-link"
export FAKE_DOCKER_PS_ARGS="$FIXTURE/ps-args"
"$SCRIPT" "$FIXTURE/consumer-link" sample 'bug 43' --log "$FIXTURE/link.log"
if ! grep -Fxq -- "label=devcontainer.local_folder=$(cd "$CLONE" && pwd -P)" "$FAKE_DOCKER_PS_ARGS"; then
    echo 'FAIL: symlinked clone path was not resolved to the physical path' >&2
    exit 1
fi
echo 'PASS: symlinked clone path resolved'
