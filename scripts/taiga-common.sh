# ABOUTME: Shared auth/fetch/PATCH primitives for taiga-wiki.sh (the other
# ABOUTME: taiga-*.sh scripts keep their own inline auth). Source; do not execute.
# shellcheck shell=bash
#
# Security model (see CLAUDE.md):
#  - Credentials never appear in argv. Passwords go via stdin; bearer tokens
#    via process substitution into -H @<(...).
#  - PATCH bodies travel via stdin to keep description text (which may embed
#    operator-private content) out of /proc/<pid>/cmdline.
#
# Globals consumed (env): TAIGA_API_URL, TAIGA_USERNAME, TAIGA_PASSWORD.
# Globals set: TAIGA_TOKEN, TAIGA_TMPDIR, TAIGA_ITEM_ID, TAIGA_ITEM_VERSION.

taiga_check_env() {
    : "${TAIGA_API_URL:?TAIGA_API_URL is required}"
    : "${TAIGA_USERNAME:?TAIGA_USERNAME is required}"
    : "${TAIGA_PASSWORD:?TAIGA_PASSWORD is required}"
}

taiga_setup_tmpdir() {
    TAIGA_TMPDIR=$(mktemp -d)
    chmod 700 "$TAIGA_TMPDIR"
    trap 'rm -rf "$TAIGA_TMPDIR"' EXIT
}

# Run curl -sS with the given args, passing stdin and stdout through. A curl
# process failure (no HTTP response: DNS, refused connection, timeout,
# malformed URL) prints an ERROR: line and exits 1 instead of leaking curl's
# own exit status, which would otherwise collide with a caller's documented
# codes (curl exit 3 is taiga-wiki.sh's version-conflict code). Callers run
# this inside $(...), so they must follow it with `|| exit 1` for the failure
# to leave the script.
#
# Args:
#   $1  human label for the error message ("auth", "wiki page fetch")
#   $@  remaining args are passed to curl
taiga_curl() {
    local label="$1" rc=0
    shift
    curl -sS "$@" || rc=$?
    if [[ "$rc" -ne 0 ]]; then
        echo "ERROR: Taiga $label request failed (curl exit $rc)" >&2
        exit 1
    fi
}

taiga_login() {
    local body http
    # $TAIGA_PASSWORD is read via jq's env accessor, not --arg: --arg would
    # place the password in jq's own argv, readable from /proc/<pid>/cmdline
    # for the life of that process. env.* pulls from /proc/<pid>/environ
    # instead, which is not world-readable.
    body=$(jq -n \
        --arg username "$TAIGA_USERNAME" \
        '{type: "normal", username: $username, password: env.TAIGA_PASSWORD}')
    http=$(taiga_curl "auth" -o "$TAIGA_TMPDIR/auth.json" -w '%{http_code}' \
        -X POST "$TAIGA_API_URL/api/v1/auth" \
        -H 'Content-Type: application/json' \
        --data-binary @- <<<"$body") || exit 1
    if [[ "$http" != "200" ]]; then
        echo "ERROR: Taiga auth failed (HTTP $http)" >&2
        cat "$TAIGA_TMPDIR/auth.json" >&2
        exit 1
    fi
    TAIGA_TOKEN=$(jq -r '.auth_token' "$TAIGA_TMPDIR/auth.json")
}

taiga_auth_header_fd() {
    printf 'Authorization: Bearer %s\n' "$TAIGA_TOKEN"
}

# Fetch an item by human-readable ref. Writes JSON to $TAIGA_TMPDIR/$2.json.
# Sets globals TAIGA_ITEM_ID and TAIGA_ITEM_VERSION on success.
# Exits 1 with a clear error if the ref doesn't exist or the API errors.
#
# Args:
#   $1  resource path segment ("userstories" or "epics")
#   $2  basename for the cache file (e.g., "story", "epic")
#   $3  project_id
#   $4  ref
#   $5  human label for error messages ("user story", "epic")
taiga_fetch_by_ref() {
    local resource="$1" basename="$2" project_id="$3" ref="$4" label="$5" http
    http=$(taiga_curl "$label fetch" -o "$TAIGA_TMPDIR/$basename.json" -w '%{http_code}' \
        "$TAIGA_API_URL/api/v1/$resource/by_ref?project=${project_id}&ref=${ref}" \
        -H @<(taiga_auth_header_fd)) || exit 1
    if [[ "$http" -eq 404 ]]; then
        echo "ERROR: no $label found with ref #${ref} in project ${project_id}" >&2
        exit 1
    fi
    if [[ "$http" -ne 200 ]]; then
        echo "ERROR: failed to fetch $label (HTTP $http)" >&2
        cat "$TAIGA_TMPDIR/$basename.json" >&2
        exit 1
    fi
    # shellcheck disable=SC2034  # consumed by callers (sourcing script)
    TAIGA_ITEM_ID=$(jq -r '.id' "$TAIGA_TMPDIR/$basename.json")
    # shellcheck disable=SC2034  # consumed by callers (sourcing script)
    TAIGA_ITEM_VERSION=$(jq -r '.version' "$TAIGA_TMPDIR/$basename.json")
}

# GET an arbitrary API path (path + query, no leading slash) into
# $TAIGA_TMPDIR/$2.json. Sets TAIGA_ITEM_ID and TAIGA_ITEM_VERSION on success.
# Exits 1 with a clear error if the object doesn't exist or the API errors.
#
# Args:
#   $1  API path relative to /api/v1/, query string included
#         (e.g. "wiki/1", "wiki/by_slug?project=1&slug=roadmap")
#   $2  basename for the cache file (e.g., "wiki")
#   $3  human label for error messages ("wiki page")
taiga_fetch_path() {
    local path="$1" basename="$2" label="$3" http
    http=$(taiga_curl "$label fetch" -o "$TAIGA_TMPDIR/$basename.json" -w '%{http_code}' \
        "$TAIGA_API_URL/api/v1/$path" \
        -H @<(taiga_auth_header_fd)) || exit 1
    if [[ "$http" -eq 404 ]]; then
        echo "ERROR: no $label found at /api/v1/${path}" >&2
        exit 1
    fi
    if [[ "$http" -ne 200 ]]; then
        echo "ERROR: failed to fetch $label (HTTP $http)" >&2
        cat "$TAIGA_TMPDIR/$basename.json" >&2
        exit 1
    fi
    # shellcheck disable=SC2034  # consumed by callers (sourcing script)
    TAIGA_ITEM_ID=$(jq -r '.id' "$TAIGA_TMPDIR/$basename.json")
    # shellcheck disable=SC2034  # consumed by callers (sourcing script)
    TAIGA_ITEM_VERSION=$(jq -r '.version' "$TAIGA_TMPDIR/$basename.json")
}

# Fetch an item by its numeric primary key, for resources that have no ref
# (wiki pages) and so cannot use the by_ref-shaped taiga_fetch_by_ref.
#
# Args:
#   $1  resource path segment ("wiki")
#   $2  basename for the cache file
#   $3  numeric item id
#   $4  human label for error messages
taiga_fetch_by_id() {
    taiga_fetch_path "$1/$3" "$2" "$4"
}

# Resolve a status name to its numeric id within the given project.
# Echoes the numeric id on stdout; exits 1 with the list of valid names
# if no match.
#
# Args:
#   $1  statuses endpoint segment ("userstory-statuses" or "epic-statuses")
#   $2  project_id
#   $3  status name (e.g., "Ready", "In progress")
taiga_resolve_status() {
    local endpoint="$1" project_id="$2" name="$3" http id
    http=$(taiga_curl "statuses fetch" -o "$TAIGA_TMPDIR/statuses.json" -w '%{http_code}' \
        "$TAIGA_API_URL/api/v1/$endpoint?project=${project_id}" \
        -H @<(taiga_auth_header_fd)) || exit 1
    if [[ "$http" -ne 200 ]]; then
        echo "ERROR: failed to fetch statuses (HTTP $http)" >&2
        cat "$TAIGA_TMPDIR/statuses.json" >&2
        exit 1
    fi
    id=$(jq -r --arg n "$name" '[.[] | select(.name == $n)][0].id // empty' \
        "$TAIGA_TMPDIR/statuses.json")
    if [[ -z "$id" ]]; then
        echo "ERROR: no status named '$name' on project ${project_id}." >&2
        echo "       Valid: $(jq -r '[.[].name] | join(", ")' "$TAIGA_TMPDIR/statuses.json")" >&2
        exit 1
    fi
    echo "$id"
}

# Merge new tag names with an item's existing tags, deduping.
# Echoes the resolved tags array as JSON on stdout.
# Taiga stores tags as [["name", color_or_null], ...]; new tags are
# appended as ["name", null] entries.
#
# Args:
#   $1  path to JSON file with the existing item (must have .tags)
#   $2  JSON array of new tag names (e.g., '["llm-analyzed","ready"]')
taiga_build_tag_list() {
    local item_file="$1" new_tags_json="$2"
    jq -n \
        --slurpfile item "$item_file" \
        --argjson new "$new_tags_json" \
        '
        ($item[0].tags // []) as $existing
        | ($existing | map(.[0])) as $existing_names
        | $new
          | map(select(. as $n | ($existing_names | index($n)) == null))
          | map([., null]) as $additions
        | $existing + $additions
        '
}

# Print a PATCH body to stdout for dry-run inspection, redacting the
# description field to its length to avoid leaking large markdown bodies
# into shell history or operator logs.
#
# Args: $1 = PATCH body JSON
taiga_print_redacted() {
    echo "$1" | jq 'if has("description") then .description = "(\(.description | length) chars)" else . end'
}

# Send a PATCH and echo the HTTP status code on stdout instead of exiting, so
# the caller can decide which codes are acceptable. Body is read from stdin and
# the response body lands in $TAIGA_TMPDIR/patch.json.
#
# taiga-wiki.sh maps the 400 version-mismatch response to its own exit code 3;
# taiga_patch below is the exit-on-anything-but-200 wrapper.
#
# Args: same as taiga_patch.
taiga_patch_code() {
    local resource="$1" item_id="$2"
    taiga_curl "PATCH" -o "$TAIGA_TMPDIR/patch.json" -w '%{http_code}' \
        -X PATCH "$TAIGA_API_URL/api/v1/$resource/$item_id" \
        -H @<(taiga_auth_header_fd) \
        -H 'Content-Type: application/json' \
        --data-binary @-
}

# Send a PATCH and exit non-zero on failure. Body is read from stdin.
# Writes the response body to $TAIGA_TMPDIR/patch.json for the caller to
# inspect (e.g., to print the resulting status_extra_info.name or version).
#
# Args:
#   $1  resource path segment ("userstories" or "epics")
#   $2  item id (numeric, returned by taiga_fetch_by_ref via TAIGA_ITEM_ID)
taiga_patch() {
    local http
    http=$(taiga_patch_code "$@") || exit 1
    if [[ "$http" -ne 200 ]]; then
        echo "ERROR: PATCH failed (HTTP $http)" >&2
        cat "$TAIGA_TMPDIR/patch.json" >&2
        exit 1
    fi
}
