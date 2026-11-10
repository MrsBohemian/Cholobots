#!/usr/bin/env bash
# ABOUTME: Entry point for the per-checkout local Supabase stack (start, stop, status, env, prune, CLI passthrough).
# ABOUTME: Delegates to supabase_local.py next to this file; see scripts/README.md.
set -euo pipefail
exec python3 "$(dirname "$(readlink -f "$0")")/supabase_local.py" "$@"
