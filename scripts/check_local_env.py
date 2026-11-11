#!/usr/bin/env python3
# ABOUTME: Preflight for `make run`: refuses to start the bot unless .env points at the local Supabase stack.
# ABOUTME: Also reports the empty secrets and bad channel id that would crash the bot at import or in on_ready.
import argparse
import os
import sys
from collections.abc import Mapping
from pathlib import Path
from urllib.parse import urlsplit

LOCAL_HOSTS = {"127.0.0.1", "localhost", "::1"}
REQUIRED_SECRETS = ("DISCORD_TOKEN", "OPENAI_API_KEY", "SUPABASE_SERVICE_ROLE_KEY")
DEFAULT_ENV_FILE = Path(__file__).resolve().parents[1] / ".env"


def effective_env(dotenv: Mapping[str, str | None], environ: Mapping[str, str]) -> dict[str, str]:
    """Merge the shell and .env the way config.py's load_dotenv(override=True) does.

    .env wins, except for bare `KEY` lines, which python-dotenv reports as None
    and load_dotenv skips.
    """
    env = dict(environ)
    env.update({key: value for key, value in dotenv.items() if value is not None})
    return env


def is_local_url(url: str | None) -> bool:
    if not url:
        return False
    try:
        parts = urlsplit(url)
        host = parts.hostname
    except ValueError:
        return False
    return parts.scheme in ("http", "https") and host in LOCAL_HOSTS


def problems(env: Mapping[str, str]) -> list[str]:
    found = []
    url = env.get("SUPABASE_URL")
    if not is_local_url(url):
        shown = url or "not set"
        found.append(
            f"SUPABASE_URL is {shown}: make run only starts the bot against the local stack "
            "(make db-up, then make db-env)."
        )
    for name in REQUIRED_SECRETS:
        if not (env.get(name) or "").strip():
            found.append(f"{name} is empty in .env")
    channel = env.get("TRANSPORTATION_CHANNEL_ID")
    if channel is not None:
        try:
            int(channel)
        except ValueError:
            found.append("TRANSPORTATION_CHANNEL_ID must be a whole number (use 0)")
    return found


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check .env before make run starts the bot.")
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    args = parser.parse_args(argv)

    from dotenv import dotenv_values  # same parser config.py uses

    if args.env_file.is_file():
        dotenv = dotenv_values(args.env_file)
    else:
        dotenv = {}
        print(f"{args.env_file} not found; run `make setup` to create it from .env.example.", file=sys.stderr)

    found = problems(effective_env(dotenv, os.environ))
    if not found:
        return 0
    for problem in found:
        print(problem, file=sys.stderr)
    print("See .env.example.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
