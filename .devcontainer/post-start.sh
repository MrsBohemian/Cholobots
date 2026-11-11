#!/usr/bin/env bash
# ABOUTME: Devcontainer post-start script: waits for dockerd, starts and resets the local Supabase stack, writes .env.
# ABOUTME: Local stack only; every start reseeds the database. Never starts the bot.
set -euo pipefail

cd "$(dirname "$0")/.."

# The DinD feature selects iptables-legacy, which fails on nftables-only host kernels; switch to nft and restart dockerd.
if ! sudo iptables -t nat -L -n >/dev/null 2>&1 && sudo iptables-nft -t nat -L -n >/dev/null 2>&1; then
    echo "Legacy iptables is unusable on this host; switching to iptables-nft."
    sudo update-alternatives --set iptables /usr/sbin/iptables-nft
    sudo update-alternatives --set ip6tables /usr/sbin/ip6tables-nft
    if ! docker info >/dev/null 2>&1; then
        sudo "${DOCKER_INIT:-/usr/local/share/docker-init.sh}" true
    fi
fi

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

# After a container restart the stack's db container takes a while to become healthy; until then
# `status`, `start` and `db reset` fail, so each step retries.
attempts="${SUPABASE_RETRY_ATTEMPTS:-30}"
delay="${SUPABASE_RETRY_DELAY:-2}"

retry() {
    local label="$1"
    shift
    local attempt=1
    until "$@"; do
        if [ "$attempt" -ge "$attempts" ]; then
            echo "Error: ${label} still failing after ${attempts} attempts." >&2
            exit 1
        fi
        echo "${label} failed (attempt ${attempt}/${attempts}); retrying in ${delay}s..."
        sleep "$delay"
        attempt=$((attempt + 1))
    done
}

start_stack() {
    if "$SUPABASE_BIN" status >/dev/null 2>&1; then
        echo "Supabase already running"
    elif [ -n "$exclude" ]; then
        "$SUPABASE_BIN" start -x "$exclude"
    else
        "$SUPABASE_BIN" start
    fi
}

retry "supabase start" start_stack

# Reseeds on every start; stop the bot first if one is running.
retry "supabase db reset" "$SUPABASE_BIN" db reset

"$SUPABASE_BIN" status -o env | python3 scripts/devcontainer/write_env.py .env

echo "Devcontainer ready: Supabase API http://127.0.0.1:54321, Studio http://127.0.0.1:54323"
