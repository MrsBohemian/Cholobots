#!/usr/bin/env bash
# ABOUTME: Entry point for the per-checkout local Supabase stack (start, stop, status, env, prune, CLI passthrough).
# ABOUTME: Delegates to supabase_local.py next to this file; see scripts/README.md.
set -euo pipefail
# Resolve symlinks without `readlink -f` (BSD/macOS readlink has no -f).
src="${BASH_SOURCE[0]}"
while [ -h "$src" ]; do
    dir="$(cd -P "$(dirname "$src")" && pwd)"
    src="$(readlink "$src")"
    case "$src" in /*) ;; *) src="$dir/$src" ;; esac
done
exec python3 "$(cd -P "$(dirname "$src")" && pwd)/supabase_local.py" "$@"
