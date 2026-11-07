#!/usr/bin/env bash
# ABOUTME: Fetch a Taiga item (user story, issue, or epic) by ref number using the REST API directly.
# ABOUTME: Uses /by_ref endpoint for O(1) lookup; the task list is fetched with pagination disabled.

set -euo pipefail

# Private per-run directory (mode 700): item and task bodies can carry private
# descriptions, so nothing is written to a fixed, world-readable /tmp path.
TMPDIR_SCRIPT=$(mktemp -d)
chmod 700 "$TMPDIR_SCRIPT"
trap 'rm -rf "$TMPDIR_SCRIPT"' EXIT

usage() {
    echo "Usage: $0 <ref> [project_id] [--issue] [--epic] [--with-tasks]"
    echo "  ref          Item ref number (as shown in Taiga UI)"
    echo "  project_id   Taiga project ID (default: 39)"
    echo "  --issue      Look up an issue instead of a user story"
    echo "  --epic       Look up an epic instead of a user story"
    echo "  --with-tasks Also fetch and include the task list (user stories only)"
    echo ""
    echo "Env vars required: TAIGA_API_URL, TAIGA_USERNAME, TAIGA_PASSWORD"
    echo ""
    echo "Output: compact JSON with id, ref, subject, description, status, status_extra_info, milestone, milestone_name, milestone_slug, tags, is_closed,"
    echo "        created_date, modified_date, version (every item type)"
    echo "        (plus 'tasks' array when --with-tasks is used)"
    echo ""
    echo "Exit codes: 0 success, 44 item not found (404), 1 any other error"
    exit 1
}

REF="${1:-}"
# The project id is positional but optional: `taiga-story.sh 21 --with-tasks`
# must not read `--with-tasks` as the project.
if [[ -n "${2:-}" && "${2}" != --* ]]; then
    PROJECT_ID="$2"
else
    PROJECT_ID="39"
fi
ITEM_TYPE="userstories"
WITH_TASKS=false

for arg in "$@"; do
    if [[ "$arg" == "--issue" ]]; then
        ITEM_TYPE="issues"
    elif [[ "$arg" == "--epic" ]]; then
        ITEM_TYPE="epics"
    elif [[ "$arg" == "--with-tasks" ]]; then
        WITH_TASKS=true
    fi
done

[[ -z "$REF" ]] && usage

: "${TAIGA_API_URL:?TAIGA_API_URL is required}"
: "${TAIGA_USERNAME:?TAIGA_USERNAME is required}"
: "${TAIGA_PASSWORD:?TAIGA_PASSWORD is required}"

# Credentials never go on a command line (readable via /proc/<pid>/cmdline):
# the login body is built by python3 from the environment and sent on stdin,
# and the bearer header is read by curl from a process substitution.
AUTH_PAYLOAD=$(python3 -c 'import json, os; print(json.dumps({"type": "normal", "username": os.environ["TAIGA_USERNAME"], "password": os.environ["TAIGA_PASSWORD"]}))')
auth_header() { printf 'Authorization: Bearer %s\n' "$TOKEN"; }

# Authenticate
AUTH_RESPONSE=$(curl -sf -X POST "${TAIGA_API_URL}/api/v1/auth" \
    -H "Content-Type: application/json" \
    -d @- <<<"$AUTH_PAYLOAD")

TOKEN=$(python3 -c "import json,sys; print(json.load(sys.stdin)['auth_token'])" <<<"$AUTH_RESPONSE")

# Fetch item by ref using dedicated by_ref endpoint (O(1), no pagination)
HTTP_CODE=$(curl -s -o "$TMPDIR_SCRIPT/item.json" -w "%{http_code}" \
    "${TAIGA_API_URL}/api/v1/${ITEM_TYPE}/by_ref?project=${PROJECT_ID}&ref=${REF}" \
    -H @<(auth_header))

if [[ "$HTTP_CODE" -eq 404 ]]; then
    ITEM_LABEL="user story"
    [[ "$ITEM_TYPE" == "issues" ]] && ITEM_LABEL="issue"
    [[ "$ITEM_TYPE" == "epics" ]] && ITEM_LABEL="epic"
    echo "No ${ITEM_LABEL} found with ref #${REF} in project ${PROJECT_ID}" >&2
    exit 44
fi

if [[ "$HTTP_CODE" -ne 200 ]]; then
    echo "Taiga API error: HTTP $HTTP_CODE" >&2
    cat "$TMPDIR_SCRIPT/item.json" >&2
    exit 1
fi

STORY_ID=$(python3 -c "import json, sys; print(json.load(open(sys.argv[1]))['id'])" "$TMPDIR_SCRIPT/item.json")

# Optionally fetch tasks (user stories only). Taiga caps a list response at 30
# rows unless pagination is disabled, so ask for the whole collection.
echo "null" > "$TMPDIR_SCRIPT/tasks_compact.json"
if [[ "$WITH_TASKS" == true && "$ITEM_TYPE" == "userstories" ]]; then
    HTTP_CODE=$(curl -s -o "$TMPDIR_SCRIPT/tasks_response.json" -w "%{http_code}" \
        "${TAIGA_API_URL}/api/v1/tasks?project=${PROJECT_ID}&user_story=${STORY_ID}" \
        -H @<(auth_header) \
        -H "x-disable-pagination: True")
    if [[ "$HTTP_CODE" -ne 200 ]]; then
        echo "Taiga API error fetching tasks: HTTP $HTTP_CODE" >&2
        cat "$TMPDIR_SCRIPT/tasks_response.json" >&2
        exit 1
    fi
    python3 -c "
import json, sys
tasks = json.load(open(sys.argv[1]))
compact = [{
    'id': t['id'],
    'ref': t['ref'],
    'subject': t['subject'],
    'status': t['status'],
    'status_extra_info': t['status_extra_info'],
    'is_closed': t['is_closed'],
} for t in tasks]
open(sys.argv[2], 'w').write(json.dumps(compact))
" "$TMPDIR_SCRIPT/tasks_response.json" "$TMPDIR_SCRIPT/tasks_compact.json"
fi

python3 -c "
import json, sys
d = json.load(open(sys.argv[1]))
tasks = json.load(open(sys.argv[2]))
out = {
    'id': d.get('id'),
    'ref': d.get('ref'),
    'subject': d.get('subject'),
    'description': d.get('description'),
    'status': d.get('status'),
    'status_extra_info': d.get('status_extra_info'),
    'milestone': d.get('milestone'),
    'milestone_name': d.get('milestone_name'),
    'milestone_slug': d.get('milestone_slug'),
    'tags': d.get('tags'),
    'is_closed': d.get('is_closed'),
    'created_date': d.get('created_date'),
    'modified_date': d.get('modified_date'),
    'version': d.get('version'),
}
if tasks is not None:
    out['tasks'] = tasks
print(json.dumps(out, indent=2))
" "$TMPDIR_SCRIPT/item.json" "$TMPDIR_SCRIPT/tasks_compact.json"
