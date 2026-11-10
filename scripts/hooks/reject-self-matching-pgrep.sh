#!/usr/bin/env bash
# ABOUTME: PreToolUse hook that blocks Bash commands using pkill -f / pgrep -f.
# ABOUTME: -f matches the calling shell's own command line, killing the caller or never returning empty.
set -euo pipefail

command=$(jq -r '.tool_input.command // empty' 2>/dev/null || true)
[ -n "$command" ] || exit 0

# Match pkill/pgrep followed by a flag group containing 'f' (any ordering: -f, -9 -f, -f -9, -fl).
block_re='\bp(kill|grep)\b( +-[0-9a-zA-Z]+)* +-[a-zA-Z]*f'
# The only exemption is pgrep piped into a grep -v that filters out the caller's own pid ($$);
# pkill is never exempt because it kills before any pipe runs.
exempt_re='\bpgrep\b[^|]*\| *grep +-v +["'"'"']?\^\\?\$\$'

# Each segment is judged on its own so that an exempt pgrep elsewhere in the command
# cannot shield a self-matching pkill: split on the separators that start a new invocation.
while IFS= read -r segment; do
    if echo "$segment" | grep -qE "$block_re" && ! echo "$segment" | grep -qE "$exempt_re"; then
        echo "Blocked: pkill/pgrep -f matches this shell's own command line. Use pgrep -x <exe> or a pid file." >&2
        exit 2
    fi
done < <(echo "$command" | sed -E 's/(\|\||&&|;|&)/\n/g')
exit 0
