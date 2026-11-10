#!/usr/bin/env bash
# ABOUTME: PreToolUse hook that blocks Bash commands using pkill -f / pgrep -f.
# ABOUTME: -f matches the calling shell's own command line, killing the caller or never returning empty.
set -euo pipefail

command=$(jq -r '.tool_input.command // empty' 2>/dev/null || true)
[ -n "$command" ] || exit 0

# Match pkill/pgrep followed by a flag group containing 'f' (any ordering: -f, -9 -f, -f -9, -fl).
block_re='\bp(kill|grep)\b( +-[0-9a-zA-Z]+)* +-[a-zA-Z]*f'
# The only exemption: a pgrep -f whose output goes straight into a grep -v that filters out the
# caller's own pid ($$). pkill is never exempt because it kills before any pipe runs.
filter_re='^ *grep +-v +["'"'"']?\^\\?\$\$'

block() {
    echo "Blocked: pkill/pgrep -f matches this shell's own command line. Use pgrep -x <exe> or a pid file." >&2
    exit 2
}

# Each command is judged on its own, so an exempt pgrep cannot shield a self-matching pkill/pgrep
# anywhere else in the line: first split on the separators that start a new invocation, then split
# each segment on single pipes and examine every pipeline stage.
while IFS= read -r segment; do
    stages=()
    while IFS= read -r stage; do stages+=("$stage"); done < <(echo "$segment" | sed -E 's/\|/\n/g')
    for i in "${!stages[@]}"; do
        stage="${stages[$i]}"
        grep -qE "$block_re" <<< "$stage" || continue
        # Exempt only a lone pgrep -f stage directly followed by the self-pid filter.
        if grep -qE '^ *pgrep\b' <<< "$stage" \
            && [ "$(grep -oE "$block_re" <<< "$stage" | wc -l)" -eq 1 ] \
            && [ -n "${stages[$((i + 1))]:-}" ] \
            && grep -qE "$filter_re" <<< "${stages[$((i + 1))]}"; then
            continue
        fi
        block
    done
done < <(echo "$command" | sed -E 's/(\|\||&&|;|&)/\n/g')
exit 0
