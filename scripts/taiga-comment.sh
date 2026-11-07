#!/usr/bin/env bash
# ABOUTME: Post a comment on a Taiga issue or user story using the REST API directly.
# ABOUTME: PATCHes the user story/issue item endpoint with the `comment` field (and current version).

set -euo pipefail

TMPDIR_SCRIPT=$(mktemp -d)
chmod 700 "$TMPDIR_SCRIPT"
trap 'rm -rf "$TMPDIR_SCRIPT"' EXIT

usage() {
    echo "Usage: $0 <item-id> <comment> [--issue]"
    echo "  item-id    Taiga internal ID (not ref number)"
    echo "  comment    Comment text (markdown supported)"
    echo "  --issue    Comment on an issue (default: user story)"
    echo ""
    echo "Env vars required: TAIGA_API_URL, TAIGA_USERNAME, TAIGA_PASSWORD"
    echo ""
    echo "Output: Human-readable message including HTTP status code (200 = success)"
    exit 1
}

ITEM_ID="${1:-}"
COMMENT="${2:-}"
ITEM_TYPE="userstories"

for arg in "$@"; do
    if [[ "$arg" == "--issue" ]]; then
        ITEM_TYPE="issues"
    fi
done

[[ -z "$ITEM_ID" ]] && usage
[[ -z "$COMMENT" ]] && usage

: "${TAIGA_API_URL:?TAIGA_API_URL is required}"
: "${TAIGA_USERNAME:?TAIGA_USERNAME is required}"
: "${TAIGA_PASSWORD:?TAIGA_PASSWORD is required}"

# Credentials and comment text never go on a command line (readable via
# /proc/<pid>/cmdline): request bodies are sent on stdin and the bearer header
# is read by curl from a process substitution.
AUTH_PAYLOAD=$(python3 -c 'import json, os; print(json.dumps({"type": "normal", "username": os.environ["TAIGA_USERNAME"], "password": os.environ["TAIGA_PASSWORD"]}))')
auth_header() { printf 'Authorization: Bearer %s\n' "$TOKEN"; }

# Authenticate
AUTH_RESPONSE=$(curl -sf -X POST "${TAIGA_API_URL}/api/v1/auth" \
    -H "Content-Type: application/json" \
    -d @- <<<"$AUTH_PAYLOAD")

TOKEN=$(python3 -c "import json,sys; print(json.load(sys.stdin)['auth_token'])" <<<"$AUTH_RESPONSE")

# Get current version (required for PATCH)
HTTP_CODE=$(curl -s -o "$TMPDIR_SCRIPT/item.json" -w "%{http_code}" \
    "${TAIGA_API_URL}/api/v1/${ITEM_TYPE}/${ITEM_ID}" \
    -H @<(auth_header))

if [[ "$HTTP_CODE" -ne 200 ]]; then
    echo "Failed to fetch item: HTTP $HTTP_CODE" >&2
    exit 1
fi

VERSION=$(python3 -c "import json, sys; print(json.load(open(sys.argv[1]))['version'])" "$TMPDIR_SCRIPT/item.json")

# Build JSON payload via the environment (no shell interpolation, and the
# comment text stays out of python3's argv)
COMMENT_JSON=$(COMMENT="$COMMENT" VERSION="$VERSION" python3 -c "
import json, os
print(json.dumps({'comment': os.environ['COMMENT'], 'version': int(os.environ['VERSION'])}))
")

HTTP_CODE=$(curl -s -o "$TMPDIR_SCRIPT/response.json" -w "%{http_code}" \
    -X PATCH "${TAIGA_API_URL}/api/v1/${ITEM_TYPE}/${ITEM_ID}" \
    -H @<(auth_header) \
    -H "Content-Type: application/json" \
    -d @- <<<"$COMMENT_JSON")

if [[ "$HTTP_CODE" -ne 200 ]]; then
    echo "Failed to post comment: HTTP $HTTP_CODE" >&2
    cat "$TMPDIR_SCRIPT/response.json" >&2
    exit 1
fi

echo "Comment posted successfully (HTTP $HTTP_CODE)"
