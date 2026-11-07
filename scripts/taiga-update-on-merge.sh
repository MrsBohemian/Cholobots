#!/usr/bin/env bash
# ABOUTME: Apply opt-in Taiga updates after a PR merges (Done/Closed, task closing), each gated on a PR label or item tag.
# ABOUTME: Called by the taiga-sync GitHub Actions workflow; also usable standalone for manual fixes.

set -euo pipefail

TMPDIR_SCRIPT=$(mktemp -d)
chmod 700 "$TMPDIR_SCRIPT"
trap 'rm -rf "$TMPDIR_SCRIPT"' EXIT

DRY_RUN=false

usage() {
    echo "Usage: $0 [--dry-run] <ref> <pr-number> <pr-url>"
    echo "  ref          Taiga item ref number"
    echo "  pr-number    GitHub PR number"
    echo "  pr-url       GitHub PR URL"
    echo "  --dry-run    Show what would be done without making changes"
    echo ""
    echo "Env vars required: TAIGA_API_URL, TAIGA_USERNAME, TAIGA_PASSWORD"
    echo "Env vars optional: PR_LABELS  whitespace-separated labels of the merged PR"
    echo ""
    echo "Each side effect runs only when its name is a PR label OR a tag on the Taiga item"
    echo "(tags are read live from Taiga):"
    echo "  mark-done-on-merge    story -> Done, issue -> Closed"
    echo "  close-tasks-on-merge  close the story's open tasks (ignored for issues)"
    echo "With neither, only a 'Merged in PR' comment is posted."
    echo ""
    echo "Status IDs (project 39):"
    echo "  User story Done: 80"
    echo "  Issue Closed: 74"
    echo "  Task Closed: 62"
    exit 1
}

# Parse flags
POSITIONAL=()
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=true ;;
        --help|-h) usage ;;
        *) POSITIONAL+=("$arg") ;;
    esac
done

REF="${POSITIONAL[0]:-}"
PR_NUMBER="${POSITIONAL[1]:-}"
PR_URL="${POSITIONAL[2]:-}"

[[ -z "$REF" ]] && usage
[[ -z "$PR_NUMBER" ]] && usage
[[ -z "$PR_URL" ]] && usage

: "${TAIGA_API_URL:?TAIGA_API_URL is required}"
: "${TAIGA_USERNAME:?TAIGA_USERNAME is required}"
: "${TAIGA_PASSWORD:?TAIGA_PASSWORD is required}"

# Project-specific status IDs
STORY_DONE_STATUS=80
ISSUE_CLOSED_STATUS=74
TASK_CLOSED_STATUS=62
PROJECT_ID=39

log() { echo "[taiga-sync] $*"; }
warn() { echo "[taiga-sync] WARNING: $*" >&2; }
err() { echo "[taiga-sync] ERROR: $*" >&2; }

# Credentials and request bodies never go on a command line (readable via
# /proc/<pid>/cmdline): bodies are sent on stdin and the bearer header is read
# by curl from a process substitution.
auth_header() { printf 'Authorization: Bearer %s\n' "$TOKEN"; }

# Authenticate with Taiga API
log "Authenticating with Taiga..."
AUTH_PAYLOAD=$(TAIGA_USERNAME="$TAIGA_USERNAME" TAIGA_PASSWORD="$TAIGA_PASSWORD" python3 -c 'import json, os; print(json.dumps({"type": "normal", "username": os.environ["TAIGA_USERNAME"], "password": os.environ["TAIGA_PASSWORD"]}))')
set +e
HTTP_CODE=$(curl -sS -o "$TMPDIR_SCRIPT/auth.json" -w "%{http_code}" \
    -X POST "${TAIGA_API_URL}/api/v1/auth" \
    -H "Content-Type: application/json" \
    -d @- <<<"$AUTH_PAYLOAD")
CURL_EXIT=$?
set -e

if [[ "$CURL_EXIT" -ne 0 ]]; then
    err "Authentication request failed (curl exit $CURL_EXIT). Check TAIGA_API_URL and network/TLS connectivity."
    exit 1
fi

if [[ "$HTTP_CODE" != "200" ]]; then
    err "Authentication failed: HTTP $HTTP_CODE"
    err "Response: $(cat "$TMPDIR_SCRIPT/auth.json")"
    exit 1
fi

TOKEN=$(python3 -c "import json, sys; print(json.load(open(sys.argv[1]))['auth_token'])" "$TMPDIR_SCRIPT/auth.json")
log "Authenticated successfully"

# Try to find the item: user story first, then issue
ITEM_TYPE=""
ITEM_ID=""
ITEM_STATUS=""
ITEM_VERSION=""
ITEM_STATUS_IS_CLOSED=""
ITEM_JSON=""

# Try user story
HTTP_CODE=$(curl -s -o "$TMPDIR_SCRIPT/userstory.json" -w "%{http_code}" \
    "${TAIGA_API_URL}/api/v1/userstories/by_ref?project=${PROJECT_ID}&ref=${REF}" \
    -H @<(auth_header))

if [[ "$HTTP_CODE" -eq 200 ]]; then
    ITEM_TYPE="userstories"
    ITEM_JSON="$TMPDIR_SCRIPT/userstory.json"
    ITEM_ID=$(python3 -c "import json; print(json.load(open('$TMPDIR_SCRIPT/userstory.json'))['id'])")
    ITEM_STATUS=$(python3 -c "import json; print(json.load(open('$TMPDIR_SCRIPT/userstory.json'))['status'])")
    ITEM_VERSION=$(python3 -c "import json; print(json.load(open('$TMPDIR_SCRIPT/userstory.json'))['version'])")
    # A story's top-level is_closed turns true once all its tasks close, whatever its status;
    # the status's own is_closed flag is the authoritative "already done" signal for stories and issues.
    # When it is missing the state is "unknown" and the status step is skipped; never fall back to the top-level flag.
    ITEM_STATUS_IS_CLOSED=$(python3 -c "import json; print((json.load(open('$TMPDIR_SCRIPT/userstory.json')).get('status_extra_info') or {}).get('is_closed', 'unknown'))")
    log "Found user story #${REF} (id=${ITEM_ID}, status=${ITEM_STATUS})"
else
    # Only a 404 means "not a user story, try the issue endpoint". Any other
    # status (500, 401, ...) says nothing about the ref, and falling through
    # would let a later issue-404 end the run as a successful no-op.
    if [[ "$HTTP_CODE" -ne 404 ]]; then
        err "User story lookup for #${REF} failed: HTTP $HTTP_CODE"
        cat "$TMPDIR_SCRIPT/userstory.json" >&2
        exit 1
    fi

    # Try issue
    HTTP_CODE=$(curl -s -o "$TMPDIR_SCRIPT/issue.json" -w "%{http_code}" \
        "${TAIGA_API_URL}/api/v1/issues/by_ref?project=${PROJECT_ID}&ref=${REF}" \
        -H @<(auth_header))

    if [[ "$HTTP_CODE" -eq 200 ]]; then
        ITEM_TYPE="issues"
        ITEM_JSON="$TMPDIR_SCRIPT/issue.json"
        ITEM_ID=$(python3 -c "import json; print(json.load(open('$TMPDIR_SCRIPT/issue.json'))['id'])")
        ITEM_STATUS=$(python3 -c "import json; print(json.load(open('$TMPDIR_SCRIPT/issue.json'))['status'])")
        ITEM_VERSION=$(python3 -c "import json; print(json.load(open('$TMPDIR_SCRIPT/issue.json'))['version'])")
        ITEM_STATUS_IS_CLOSED=$(python3 -c "import json; print((json.load(open('$TMPDIR_SCRIPT/issue.json')).get('status_extra_info') or {}).get('is_closed', 'unknown'))")
        log "Found issue #${REF} (id=${ITEM_ID}, status=${ITEM_STATUS})"
    elif [[ "$HTTP_CODE" -eq 404 ]]; then
        warn "No user story or issue found with ref #${REF} in project ${PROJECT_ID}"
        exit 0
    else
        err "Issue lookup for #${REF} failed: HTTP $HTTP_CODE"
        cat "$TMPDIR_SCRIPT/issue.json" >&2
        exit 1
    fi
fi

# Determine target status
TARGET_STATUS=""
if [[ "$ITEM_TYPE" == "userstories" ]]; then
    TARGET_STATUS=$STORY_DONE_STATUS
else
    TARGET_STATUS=$ISSUE_CLOSED_STATUS
fi

# Taiga returns tags as [["name", color], ...]; plain string entries are accepted too.
ITEM_TAGS=$(python3 -c "
import json, sys
for tag in json.load(open(sys.argv[1])).get('tags') or []:
    print(tag[0] if isinstance(tag, list) else tag)
" "$ITEM_JSON")

# Prints the source enabling a side effect ("label" or "tag"), or nothing when neither does.
signal_source() {
    local name="$1" label
    for label in ${PR_LABELS:-}; do
        if [[ "$label" == "$name" ]]; then
            echo "label"
            return
        fi
    done
    if grep -qxF "$name" <<< "$ITEM_TAGS"; then
        echo "tag"
    fi
}

MARK_DONE_SOURCE=$(signal_source "mark-done-on-merge")
CLOSE_TASKS_SOURCE=$(signal_source "close-tasks-on-merge")
if [[ -n "$MARK_DONE_SOURCE" ]]; then
    log "mark-done-on-merge: enabled (${MARK_DONE_SOURCE})"
else
    log "mark-done-on-merge: disabled (no label or tag)"
fi
if [[ "$ITEM_TYPE" == "userstories" ]]; then
    if [[ -n "$CLOSE_TASKS_SOURCE" ]]; then
        log "close-tasks-on-merge: enabled (${CLOSE_TASKS_SOURCE})"
    else
        log "close-tasks-on-merge: disabled (no label or tag)"
    fi
fi

# PATCHes the item with the given JSON payload, exiting on failure.
patch_item() {
    local payload="$1"
    HTTP_CODE=$(curl -s -o "$TMPDIR_SCRIPT/patch_response.json" -w "%{http_code}" \
        -X PATCH "${TAIGA_API_URL}/api/v1/${ITEM_TYPE}/${ITEM_ID}" \
        -H @<(auth_header) \
        -H "Content-Type: application/json" \
        -d @- <<<"$payload")

    if [[ "$HTTP_CODE" -ne 200 ]]; then
        err "Failed to update ${ITEM_TYPE} #${REF}: HTTP $HTTP_CODE"
        cat "$TMPDIR_SCRIPT/patch_response.json" >&2
        exit 1
    fi
}

# Status step: an item already closed or at the target status is left alone.
if [[ "$ITEM_STATUS_IS_CLOSED" == "True" ]] || [[ "$ITEM_STATUS" == "$TARGET_STATUS" ]]; then
    log "Item #${REF} is already closed/done (status=${ITEM_STATUS}). Skipping status update."
elif [[ -n "$MARK_DONE_SOURCE" && "$ITEM_STATUS_IS_CLOSED" != "False" ]]; then
    warn "Item #${REF}: status_extra_info.is_closed missing from Taiga response; closed state unknown. Skipping status update."
elif [[ -n "$MARK_DONE_SOURCE" ]]; then
    if [[ "$DRY_RUN" == true ]]; then
        log "[DRY RUN] Would update ${ITEM_TYPE} #${REF} (id=${ITEM_ID}) to status ${TARGET_STATUS}"
    else
        log "Updating ${ITEM_TYPE} #${REF} to status ${TARGET_STATUS}..."

        # Build JSON payload with comment
        COMMENT="Closed by PR #${PR_NUMBER} (${PR_URL})"
        PATCH_JSON=$(python3 -c "
import json, sys
print(json.dumps({
    'status': int(sys.argv[1]),
    'version': int(sys.argv[2]),
    'comment': sys.argv[3]
}))
" "$TARGET_STATUS" "$ITEM_VERSION" "$COMMENT")

        patch_item "$PATCH_JSON"
        log "Updated ${ITEM_TYPE} #${REF} to status ${TARGET_STATUS}"
    fi
else
    COMMENT="Merged in PR #${PR_NUMBER} (${PR_URL}). Status unchanged: no mark-done-on-merge label or tag."
    if [[ "$DRY_RUN" == true ]]; then
        log "[DRY RUN] Would comment on ${ITEM_TYPE} #${REF} (id=${ITEM_ID}) without changing status: ${COMMENT}"
    else
        PATCH_JSON=$(python3 -c "
import json, sys
print(json.dumps({'version': int(sys.argv[1]), 'comment': sys.argv[2]}))
" "$ITEM_VERSION" "$COMMENT")

        patch_item "$PATCH_JSON"
        log "Commented on ${ITEM_TYPE} #${REF}; status unchanged"
    fi
fi

# Close open tasks (user stories only)
if [[ "$ITEM_TYPE" == "userstories" ]]; then
    log "Checking for open tasks on story #${REF}..."

    # Taiga caps a list response at 30 rows unless pagination is disabled; a
    # story with more tasks would otherwise leave the later ones open.
    HTTP_CODE=$(curl -s -o "$TMPDIR_SCRIPT/tasks.json" -w "%{http_code}" \
        "${TAIGA_API_URL}/api/v1/tasks?project=${PROJECT_ID}&user_story=${ITEM_ID}" \
        -H @<(auth_header) \
        -H "x-disable-pagination: True")
    if [[ "$HTTP_CODE" -ne 200 ]]; then
        err "Failed to list tasks for story #${REF}: HTTP $HTTP_CODE"
        cat "$TMPDIR_SCRIPT/tasks.json" >&2
        exit 1
    fi

    # Filter to open tasks and close each one
    OPEN_TASKS=$(python3 -c "
import json, sys
tasks = json.load(open(sys.argv[1]))
# The task status's own is_closed flag is authoritative; the item-level flag
# can disagree with it, so it is only the fallback.
def closed(t):
    sei = t.get('status_extra_info') or {}
    return sei.get('is_closed', t.get('is_closed', False))
open_tasks = [t for t in tasks if not closed(t)]
for t in open_tasks:
    print(f\"{t['id']}:{t['version']}:{t.get('ref', '?')}\")
" "$TMPDIR_SCRIPT/tasks.json")

    if [[ -z "$OPEN_TASKS" ]]; then
        log "No open tasks to close"
    elif [[ -z "$CLOSE_TASKS_SOURCE" ]]; then
        log "Leaving $(wc -l <<< "$OPEN_TASKS" | tr -d ' ') open task(s): no close-tasks-on-merge label or tag"
    else
        TASK_COUNT=0
        TASK_FAILURES=0
        while IFS= read -r task_line; do
            TASK_ID=$(echo "$task_line" | cut -d: -f1)
            TASK_VERSION=$(echo "$task_line" | cut -d: -f2)
            TASK_REF=$(echo "$task_line" | cut -d: -f3)

            if [[ "$DRY_RUN" == true ]]; then
                log "[DRY RUN] Would close task #${TASK_REF} (id=${TASK_ID})"
            else
                TASK_JSON=$(python3 -c "
import json, sys
print(json.dumps({'status': int(sys.argv[1]), 'version': int(sys.argv[2])}))
" "$TASK_CLOSED_STATUS" "$TASK_VERSION")

                HTTP_CODE=$(curl -s -o "$TMPDIR_SCRIPT/task_patch.json" -w "%{http_code}" \
                    -X PATCH "${TAIGA_API_URL}/api/v1/tasks/${TASK_ID}" \
                    -H @<(auth_header) \
                    -H "Content-Type: application/json" \
                    -d @- <<<"$TASK_JSON")

                if [[ "$HTTP_CODE" -ne 200 ]]; then
                    warn "Failed to close task #${TASK_REF} (id=${TASK_ID}): HTTP $HTTP_CODE"
                    TASK_FAILURES=$((TASK_FAILURES + 1))
                else
                    log "Closed task #${TASK_REF}"
                fi
            fi
            TASK_COUNT=$((TASK_COUNT + 1))
        done <<< "$OPEN_TASKS"
        log "Processed ${TASK_COUNT} open task(s)"
        if [[ "$TASK_FAILURES" -gt 0 ]]; then
            # Every task was attempted; failing now turns the workflow run red
            # so the story is not left with open tasks behind a green sync.
            err "Failed to close ${TASK_FAILURES} of ${TASK_COUNT} open task(s) on story #${REF}"
            exit 1
        fi
    fi
fi

log "Done"
