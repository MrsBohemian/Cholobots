#!/usr/bin/env bash
# ABOUTME: Runs a .claude/skills skill with Claude or Codex inside the devcontainer for a checkout.
# ABOUTME: Pipes SKILL.md (frontmatter stripped, $ARGUMENTS filled in) to the agent; output goes to a log.
set -euo pipefail

usage() {
    echo "Usage: exec-skill.sh <clone-path> <skill-name> <arguments> [--provider claude|codex] [--log <log-file>] [--dry-run]" >&2
    exit 1
}

if [ "$#" -lt 3 ]; then
    usage
fi

CLONE_PATH="$1"
SKILL_NAME="$2"
ARGUMENTS="$3"
shift 3

LOG_FILE=""
DRY_RUN=false
PROVIDER=claude

while [ "$#" -gt 0 ]; do
    case "$1" in
        --log)
            if [ "$#" -lt 2 ]; then
                echo "Error: --log requires a path argument" >&2
                exit 1
            fi
            LOG_FILE="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --provider)
            if [ "$#" -lt 2 ] || { [ "$2" != claude ] && [ "$2" != codex ]; }; then
                echo "Error: --provider requires claude or codex" >&2
                exit 1
            fi
            PROVIDER="$2"
            shift 2
            ;;
        *)
            echo "Error: unknown option: $1" >&2
            usage
            ;;
    esac
done

if [ ! -d "$CLONE_PATH" ]; then
    echo "Error: clone path does not exist: $CLONE_PATH" >&2
    exit 1
fi
# Container lookup, workspace path and venv PATH: same as exec.sh, which explains them.
ABS_PATH=$(cd "$CLONE_PATH" && pwd -P)

mapfile -t CONTAINER_IDS < <(docker ps -q --filter "label=devcontainer.local_folder=$ABS_PATH")
if [ "${#CONTAINER_IDS[@]}" -eq 0 ]; then
    echo "Error: no running container found for $ABS_PATH" >&2
    exit 1
elif [ "${#CONTAINER_IDS[@]}" -gt 1 ]; then
    echo "Error: multiple containers found for $ABS_PATH (${CONTAINER_IDS[*]}); run teardown.sh first" >&2
    exit 1
fi
CONTAINER_ID="${CONTAINER_IDS[0]}"

SKILL_FILE="$ABS_PATH/.claude/skills/$SKILL_NAME/SKILL.md"
if [ "$PROVIDER" = codex ]; then
    AGENT_COMMAND="codex exec --dangerously-bypass-approvals-and-sandbox --json -"
else
    AGENT_COMMAND="claude --dangerously-skip-permissions -p - --output-format stream-json --verbose"
fi
if [ ! -f "$SKILL_FILE" ]; then
    echo "Error: skill file not found: $SKILL_FILE" >&2
    exit 1
fi

# Strip YAML frontmatter (a leading --- block).
DELIM_COUNT=$(grep -c '^---$' "$SKILL_FILE" || true)
if [ "$DELIM_COUNT" -ge 2 ]; then
    SKILL_TEXT=$(awk '
        BEGIN { in_front = 0; seen_first = 0 }
        NR == 1 && $0 == "---" {
            in_front = 1
            seen_first = 1
            next
        }
        in_front && $0 == "---" {
            in_front = 0
            next
        }
        {
            if (!in_front) print
        }
    ' "$SKILL_FILE")
else
    SKILL_TEXT=$(cat "$SKILL_FILE")
fi

SKILL_TEXT="${SKILL_TEXT//\$ARGUMENTS/$ARGUMENTS}"
if [ "$PROVIDER" = codex ]; then
    SKILL_TEXT="Follow these skill instructions with these arguments: $ARGUMENTS"$'\n\n'"$SKILL_TEXT"
fi

# shellcheck disable=SC2016 # expands inside the container, not here
VENV_PREFIX='if [ -x .venv/bin/python ]; then PATH="$PWD/.venv/bin:$PATH"; export PATH; fi; '

REPO_NAME=$(basename "$ABS_PATH")
WORKSPACE=$(docker inspect --format '{{json .Mounts}}' "$CONTAINER_ID" 2>/dev/null \
    | jq -r --arg p "$ABS_PATH" '[.[] | select(.Source == $p) | .Destination][0] // empty' 2>/dev/null) || WORKSPACE=""
[ -n "$WORKSPACE" ] || WORKSPACE="/workspaces/$REPO_NAME"

if [ -z "$LOG_FILE" ]; then
    TIMESTAMP=$(date +%Y%m%d-%H%M%S)
    LOG_FILE="/tmp/exec-skill-${SKILL_NAME}-${TIMESTAMP}.log"
fi

if [ "$DRY_RUN" = true ]; then
    echo "Would execute:"
    echo "  docker exec -i -u vscode -w $WORKSPACE $CONTAINER_ID bash -lc \"${VENV_PREFIX}${AGENT_COMMAND}\""
    echo ""
    echo "Log file: $LOG_FILE"
    echo ""
    echo "Prompt (first 5 lines):"
    printf '%s' "$SKILL_TEXT" | head -n 5
    exit 0
fi

printf '%s' "$SKILL_TEXT" | docker exec -i -u vscode -w "$WORKSPACE" "$CONTAINER_ID" \
    bash -lc "${VENV_PREFIX}${AGENT_COMMAND}" \
    > "$LOG_FILE" 2>&1

exit $?
