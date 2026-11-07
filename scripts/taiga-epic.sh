#!/usr/bin/env bash
# ABOUTME: GET or PATCH a Taiga epic's description by ref over the REST API (the workflow MCP server cannot edit epics).
# ABOUTME: Writes the dependency diagram and hand-off sections; PATCH carries the version for optimistic concurrency.
#
# Auth + REST primitives live in scripts/taiga-common.sh (credentials stay out
# of argv). Exit codes: 0 success, 1 generic failure, 3 version conflict on
# patch, 44 no epic with that ref (same code as taiga-story.sh).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
. "$SCRIPT_DIR/taiga-common.sh"

print_usage() {
    cat <<'EOF'
Usage: taiga-epic.sh get <ref> [--project-id ID]
       taiga-epic.sh patch <ref> --description-file PATH [--version N] [--project-id ID] [--dry-run]

get    Prints the epic as JSON: id, ref, version, subject, status,
       status_extra_info, tags, is_closed, description.
patch  Replaces the epic description with the bytes of PATH. The PATCH sends
       --version N when given (the version your edit is based on, from a prior
       get), otherwise the version just fetched. Prints a status line.

Options:
  --project-id ID         Taiga project id (default: 39, cholo-bots)
  --description-file PATH File whose bytes become the new description
  --version N             Expected current version (optimistic concurrency)
  --dry-run               Print the PATCH body (description redacted to a length)
                            and exit without contacting Taiga

Env vars required (except for patch --dry-run): TAIGA_API_URL, TAIGA_USERNAME,
TAIGA_PASSWORD

Exit codes: 0 success, 1 failure, 3 version conflict (the epic moved under you;
re-run get and re-apply the edit), 44 no epic with that ref.
EOF
}

usage() {
    local code="${1:-1}"
    if [[ "$code" -eq 0 ]]; then print_usage; else print_usage >&2; fi
    exit "$code"
}

[[ $# -ge 1 ]] || usage
case "$1" in -h|--help) usage 0 ;; esac
COMMAND="$1"
shift

REF=""
PROJECT_ID="39"
DESCRIPTION_FILE=""
VERSION=""
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help) usage 0 ;;
        --project-id)
            [[ $# -ge 2 ]] || { echo "ERROR: --project-id needs a value" >&2; exit 1; }
            PROJECT_ID="$2"; shift 2 ;;
        --project-id=*) PROJECT_ID="${1#*=}"; shift ;;
        --description-file)
            [[ $# -ge 2 ]] || { echo "ERROR: --description-file needs a value" >&2; exit 1; }
            DESCRIPTION_FILE="$2"; shift 2 ;;
        --description-file=*) DESCRIPTION_FILE="${1#*=}"; shift ;;
        --version)
            [[ $# -ge 2 ]] || { echo "ERROR: --version needs a value" >&2; exit 1; }
            VERSION="$2"; shift 2 ;;
        --version=*) VERSION="${1#*=}"; shift ;;
        --dry-run) DRY_RUN=true; shift ;;
        -*) echo "ERROR: unknown flag $1" >&2; usage ;;
        *)
            if [[ -z "$REF" ]]; then REF="$1"; else
                echo "ERROR: unexpected positional arg $1" >&2; usage
            fi
            shift ;;
    esac
done

[[ -n "$REF" ]] || { echo "ERROR: $COMMAND requires an epic ref" >&2; usage; }
[[ "$REF" =~ ^[0-9]+$ ]] || { echo "ERROR: ref must be numeric: $REF" >&2; exit 1; }
[[ "$PROJECT_ID" =~ ^[0-9]+$ ]] || { echo "ERROR: --project-id must be numeric: $PROJECT_ID" >&2; exit 1; }

case "$COMMAND" in
    get) ;;
    patch)
        [[ -n "$DESCRIPTION_FILE" ]] || { echo "ERROR: patch requires --description-file PATH" >&2; usage; }
        [[ -r "$DESCRIPTION_FILE" ]] || { echo "ERROR: description file not readable: $DESCRIPTION_FILE" >&2; exit 1; }
        [[ -z "$VERSION" || "$VERSION" =~ ^[0-9]+$ ]] || { echo "ERROR: --version must be numeric: $VERSION" >&2; exit 1; }
        ;;
    *)
        echo "ERROR: unknown command '$COMMAND', expected 'get' or 'patch'" >&2
        usage ;;
esac

if [[ "$COMMAND" == "patch" && "$DRY_RUN" == true ]]; then
    # shellcheck disable=SC2016  # $d / $v are jq variables
    jq -n --rawfile d "$DESCRIPTION_FILE" --arg v "${VERSION:-<fetched>}" \
        '{description: ($d | "(\(length) chars)"), version: $v}' \
        | { echo "Would PATCH epic #${REF} in project ${PROJECT_ID}:"; cat; }
    exit 0
fi

taiga_check_env
taiga_setup_tmpdir
taiga_login

# Fetch by ref; a 404 is exit 44 so callers can tell "no such epic" apart.
HTTP=$(taiga_curl "epic fetch" -o "$TAIGA_TMPDIR/epic.json" -w '%{http_code}' \
    "$TAIGA_API_URL/api/v1/epics/by_ref?project=${PROJECT_ID}&ref=${REF}" \
    -H @<(taiga_auth_header_fd)) || exit 1
if [[ "$HTTP" -eq 404 ]]; then
    echo "No epic found with ref #${REF} in project ${PROJECT_ID}" >&2
    exit 44
fi
if [[ "$HTTP" -ne 200 ]]; then
    echo "ERROR: failed to fetch epic (HTTP $HTTP)" >&2
    cat "$TAIGA_TMPDIR/epic.json" >&2
    exit 1
fi

if [[ "$COMMAND" == "get" ]]; then
    jq '{id, ref, version, subject, status, status_extra_info, tags, is_closed, description}' \
        "$TAIGA_TMPDIR/epic.json"
    exit 0
fi

EPIC_ID=$(jq -r '.id' "$TAIGA_TMPDIR/epic.json")
[[ -n "$VERSION" ]] || VERSION=$(jq -r '.version' "$TAIGA_TMPDIR/epic.json")

# The body travels on stdin, keeping the description out of argv.
# shellcheck disable=SC2016  # $d / $v are jq variables
HTTP=$(jq -n --rawfile d "$DESCRIPTION_FILE" --argjson v "$VERSION" \
        '{description: $d, version: $v}' \
    | taiga_patch_code "epics" "$EPIC_ID") || exit 1

if [[ "$HTTP" -eq 400 ]] && jq -e 'has("version")' "$TAIGA_TMPDIR/patch.json" >/dev/null 2>&1; then
    echo "ERROR: version conflict, epic #${REF} has moved past version ${VERSION}." >&2
    echo "       Re-run 'get' and re-apply the edit against the current version." >&2
    exit 3
fi
if [[ "$HTTP" -ne 200 ]]; then
    echo "ERROR: PATCH failed (HTTP $HTTP)" >&2
    cat "$TAIGA_TMPDIR/patch.json" >&2
    exit 1
fi

echo "Updated epic #${REF} (id=${EPIC_ID}): version=$(jq -r '.version' "$TAIGA_TMPDIR/patch.json")"
