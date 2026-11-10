#!/usr/bin/env bash
# ABOUTME: Devcontainer post-start script: waits for dockerd, starts and resets the local Supabase stack, writes .env.
# ABOUTME: Local stack only; every start reseeds the database. Never starts the bot.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "Waiting for Docker daemon..."
limit="${DOCKER_WAIT_LIMIT:-60}"
waited=0
while ! docker info >/dev/null 2>&1; do
    if [ "$waited" -ge "$limit" ]; then
        echo "Error: Docker daemon did not start within ${limit}s." >&2
        exit 1
    fi
    sleep 1
    waited=$((waited + 1))
done
echo "Docker daemon ready."

SUPABASE_BIN="${SUPABASE:-supabase}"
# Services nothing in this repo uses. Set SUPABASE_START_EXCLUDE= (empty) to start everything.
exclude="${SUPABASE_START_EXCLUDE-imgproxy,logflare,vector,mailpit}"

if "$SUPABASE_BIN" status >/dev/null 2>&1; then
    echo "Supabase already running"
elif [ -n "$exclude" ]; then
    "$SUPABASE_BIN" start -x "$exclude"
else
    "$SUPABASE_BIN" start
fi

# Container start means fresh seed data; stop the bot first if one is running.
"$SUPABASE_BIN" db reset

"$SUPABASE_BIN" status -o env | python3 scripts/devcontainer/write_env.py .env

echo "Devcontainer ready: Supabase API http://127.0.0.1:54321, Studio http://127.0.0.1:54323"
