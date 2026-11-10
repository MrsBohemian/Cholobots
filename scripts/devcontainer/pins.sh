#!/usr/bin/env bash
# ABOUTME: Prints every devcontainer tool pin as eval-safe KEY='value' lines, parsed from the Dockerfiles and the
# ABOUTME: pytaiga-mcp installer. Single parser shared by smoke-test.sh and the build workflow's version guard.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${1:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
BASE_DOCKERFILE="$ROOT/.devcontainer/Dockerfile"
PREBUILD_DOCKERFILE="$ROOT/.devcontainer/Dockerfile.prebuild"
INSTALLER="$ROOT/tools/pytaiga-mcp/install.sh"

MISSING=()

emit() {
    local key="$1" value="$2"
    if [ -z "$value" ]; then
        MISSING+=("$key")
    else
        printf "%s='%s'\n" "$key" "$value"
    fi
}

arg_value() {
    local file="$1" name="$2"
    [ -f "$file" ] || return 0
    sed -n "s/^ARG $name=//p" "$file" | head -1
}

node_major=""
marker=""
[ -f "$BASE_DOCKERFILE" ] && node_major="$(sed -n 's#.*deb\.nodesource\.com/setup_\([0-9]*\)\.x.*#\1#p' "$BASE_DOCKERFILE" | head -1)"
[ -f "$INSTALLER" ] && marker="$(bash "$INSTALLER" --print-marker 2>/dev/null || true)"

emit NODE_MAJOR "$node_major"
emit SUPABASE_CLI_VERSION "$(arg_value "$BASE_DOCKERFILE" SUPABASE_CLI_VERSION)"
for name in CLAUDE_CODE_VERSION CODEX_VERSION CHROME_DEVTOOLS_MCP_VERSION UV_VERSION; do
    emit "$name" "$(arg_value "$PREBUILD_DOCKERFILE" "$name")"
done
emit PYTAIGA_MCP_MARKER "$marker"

if [ "${#MISSING[@]}" -gt 0 ]; then
    echo "pins.sh: missing pin(s): ${MISSING[*]}" >&2
    exit 1
fi
