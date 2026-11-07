#!/usr/bin/env bash
# ABOUTME: GET/PATCH a Taiga wiki page over /api/v1/wiki, keeping the page body
# ABOUTME: in a file so it never round-trips through an agent's output.
#
# The wiki page is addressed by its numeric page id or by its slug (looked up in
# --project-id, default 39, cholo-bots). A purely numeric argument is a page id.
#
# Auth + REST primitives live in scripts/taiga-common.sh; see that file for the
# security model (credentials kept out of argv). Exit codes: 0 success,
# 1 generic failure, 3 version conflict on patch.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
. "$SCRIPT_DIR/taiga-common.sh"

# Print usage and exit with $1 (default 1). A requested help (exit 0) goes to
# stdout; a misuse (non-zero) goes to stderr alongside its ERROR: line.
usage() {
    local code="${1:-1}"
    if [[ "$code" -eq 0 ]]; then print_usage; else print_usage >&2; fi
    exit "$code"
}

print_usage() {
    cat <<'EOF'
Usage: taiga-wiki.sh get <slug|page-id> --out PATH
       taiga-wiki.sh get --slug SLUG [--project-id ID] --out PATH
       taiga-wiki.sh patch <slug|page-id> --version N --content-file PATH [--dry-run]

A purely numeric argument is a page id; anything else is a slug, resolved via
/api/v1/wiki/by_slug?project=ID&slug=SLUG.

get options:
  --out PATH                Write the page's raw content to PATH (required;
                              must resolve to a path under /tmp)
  --slug SLUG               Resolve the page by slug instead of by id

common options:
  --project-id ID           Project the slug belongs to (default: 39)

patch options:
  --version N               The version the edit is based on (from a prior get)
  --content-file PATH       File whose bytes become the new page content
  --dry-run                 Print the resolved PATCH body (content redacted to a
                              length) and exit without contacting Taiga

Env vars required (except for patch --dry-run): TAIGA_API_URL, TAIGA_USERNAME,
TAIGA_PASSWORD

Output: get prints page metadata as JSON on stdout ({"id","version","slug",
"project","bytes"}) and never prints the page body. patch prints a status line.

Exit codes: 0 success, 1 failure, 3 version conflict (the page moved under you;
re-run get and re-apply the edit).
EOF
}

[[ $# -ge 1 ]] || usage
case "$1" in -h|--help) usage 0 ;; esac
COMMAND="$1"
shift

PAGE_ID=""
SLUG=""
PROJECT_ID="39"
OUT_FILE=""
VERSION=""
CONTENT_FILE=""
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help) usage 0 ;;
        --out)
            [[ $# -ge 2 ]] || { echo "ERROR: --out needs a value" >&2; exit 1; }
            OUT_FILE="$2"; shift 2 ;;
        --out=*) OUT_FILE="${1#*=}"; shift ;;
        --slug)
            [[ $# -ge 2 ]] || { echo "ERROR: --slug needs a value" >&2; exit 1; }
            SLUG="$2"; shift 2 ;;
        --slug=*) SLUG="${1#*=}"; shift ;;
        --project-id)
            [[ $# -ge 2 ]] || { echo "ERROR: --project-id needs a value" >&2; exit 1; }
            PROJECT_ID="$2"; shift 2 ;;
        --project-id=*) PROJECT_ID="${1#*=}"; shift ;;
        --version)
            [[ $# -ge 2 ]] || { echo "ERROR: --version needs a value" >&2; exit 1; }
            VERSION="$2"; shift 2 ;;
        --version=*) VERSION="${1#*=}"; shift ;;
        --content-file)
            [[ $# -ge 2 ]] || { echo "ERROR: --content-file needs a value" >&2; exit 1; }
            CONTENT_FILE="$2"; shift 2 ;;
        --content-file=*) CONTENT_FILE="${1#*=}"; shift ;;
        --dry-run) DRY_RUN=true; shift ;;
        --) shift; break ;;
        -*) echo "ERROR: unknown flag $1" >&2; usage ;;
        *)
            if [[ -z "$PAGE_ID" ]]; then PAGE_ID="$1"; else
                echo "ERROR: unexpected positional arg $1" >&2; usage
            fi
            shift ;;
    esac
done

# A non-numeric positional is a slug.
if [[ -n "$PAGE_ID" && ! "$PAGE_ID" =~ ^[0-9]+$ ]]; then
    [[ -z "$SLUG" ]] || { echo "ERROR: pass the slug either positionally or with --slug, not both" >&2; usage; }
    SLUG="$PAGE_ID"
    PAGE_ID=""
fi
[[ "$PROJECT_ID" =~ ^[0-9]+$ ]] || { echo "ERROR: --project-id must be numeric: $PROJECT_ID" >&2; exit 1; }

case "$COMMAND" in
    get)
        [[ -n "$OUT_FILE" ]] || { echo "ERROR: get requires --out PATH" >&2; usage; }
        # Callers reach this script through a skill's Bash(scripts/taiga-wiki.sh *)
        # grant, which does not constrain arguments the way the skill's own
        # Edit/Read grants are scoped to /tmp/**. Enforce that same boundary here
        # so --out can't be pointed at a file in the repository.
        OUT_FILE_RESOLVED="$(realpath -m -- "$OUT_FILE")"
        [[ "$OUT_FILE_RESOLVED" == /tmp/* ]] || {
            echo "ERROR: --out must resolve to a path under /tmp (got: $OUT_FILE -> $OUT_FILE_RESOLVED)" >&2
            exit 1
        }
        if [[ -z "$PAGE_ID" ]]; then
            [[ -n "$SLUG" ]] || {
                echo "ERROR: get needs a page id or a slug" >&2; usage; }
        fi
        ;;
    patch)
        [[ -n "$PAGE_ID" || -n "$SLUG" ]] || { echo "ERROR: patch requires a page id or a slug" >&2; usage; }
        [[ -n "$VERSION" ]] || { echo "ERROR: patch requires --version N" >&2; usage; }
        [[ -n "$CONTENT_FILE" ]] || { echo "ERROR: patch requires --content-file PATH" >&2; usage; }
        [[ -r "$CONTENT_FILE" ]] || { echo "ERROR: content file not readable: $CONTENT_FILE" >&2; exit 1; }
        [[ "$VERSION" =~ ^[0-9]+$ ]] || { echo "ERROR: --version must be numeric: $VERSION" >&2; exit 1; }
        ;;
    *)
        echo "ERROR: unknown command '$COMMAND' — expected 'get' or 'patch'" >&2
        usage ;;
esac

if [[ "$COMMAND" == "patch" ]]; then
    # JQ_FILTER stays single-quoted: $c / $v are jq vars, not shell vars.
    # shellcheck disable=SC2016
    PATCH_BODY=$(jq -n --rawfile c "$CONTENT_FILE" --argjson v "$VERSION" \
        '{content: $c, version: $v}')

    # The body is built only from --version and --content-file, so a dry run
    # needs no credentials and never contacts Taiga.
    if [[ "$DRY_RUN" == true ]]; then
        echo "Would PATCH wiki page ${PAGE_ID:-$SLUG} (version=${VERSION}):"
        echo "$PATCH_BODY" | jq '.content = "(\(.content | length) chars)"'
        exit 0
    fi
fi

taiga_check_env
taiga_setup_tmpdir
taiga_login

if [[ "$COMMAND" == "get" ]]; then
    if [[ -n "$PAGE_ID" ]]; then
        taiga_fetch_by_id "wiki" "wiki" "$PAGE_ID" "wiki page"
    else
        taiga_fetch_path "wiki/by_slug?project=${PROJECT_ID}&slug=${SLUG}" "wiki" "wiki page"
    fi
    # The body goes straight from the response file to --out; it is never
    # echoed, so it cannot land in an agent's context or a shell log.
    # -j, not -r: -r appends a newline, which a patch would then write back as
    # real content and grow the page by a byte on every round-trip.
    jq -j '.content' "$TAIGA_TMPDIR/wiki.json" > "$OUT_FILE"
    jq --arg bytes "$(wc -c < "$OUT_FILE" | tr -d ' ')" \
        '{id: .id, version: .version, slug: .slug, project: .project, bytes: ($bytes | tonumber)}' \
        "$TAIGA_TMPDIR/wiki.json"
    exit 0
fi

# patch
if [[ -z "$PAGE_ID" ]]; then
    # Resolve the slug to the page id the PATCH endpoint needs.
    taiga_fetch_path "wiki/by_slug?project=${PROJECT_ID}&slug=${SLUG}" "wiki" "wiki page"
    PAGE_ID="$TAIGA_ITEM_ID"
fi
HTTP=$(echo "$PATCH_BODY" | taiga_patch_code "wiki" "$PAGE_ID") || exit 1

if [[ "$HTTP" -eq 400 ]] && jq -e 'has("version")' "$TAIGA_TMPDIR/patch.json" >/dev/null 2>&1; then
    echo "ERROR: version conflict — wiki page ${PAGE_ID} has moved past version ${VERSION}." >&2
    echo "       Re-run 'get' and re-apply the edit against the current version." >&2
    exit 3
fi

if [[ "$HTTP" -ne 200 ]]; then
    echo "ERROR: PATCH failed (HTTP $HTTP)" >&2
    cat "$TAIGA_TMPDIR/patch.json" >&2
    exit 1
fi

NEW_VERSION=$(jq -r '.version' "$TAIGA_TMPDIR/patch.json")
NEW_SLUG=$(jq -r '.slug' "$TAIGA_TMPDIR/patch.json")
echo "Updated wiki page ${PAGE_ID} (${NEW_SLUG}): version=${NEW_VERSION}"
