#!/usr/bin/env python3
# ABOUTME: Merges the in-container Supabase coordinates and passed-through bot secrets into the workspace .env.
# ABOUTME: Reads `supabase status -o env` on stdin, refuses non-local URLs, and never prints secret values.
import os
import sys
from collections.abc import Mapping
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from check_local_env import is_local_url  # noqa: E402

COMMENT = "# Managed by .devcontainer/post-start.sh (scripts/devcontainer/write_env.py)"
MANAGED_ORDER = ("SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY", "DISCORD_TOKEN", "OPENAI_API_KEY")
PASSED_SECRETS = ("DISCORD_TOKEN", "OPENAI_API_KEY")
HOST_VARIABLE = {"DISCORD_TOKEN": "CHOLOBOTS_TEST_DISCORD_TOKEN", "OPENAI_API_KEY": "OPENAI_API_KEY"}


def parse_status_env(text: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for line in text.splitlines():
        key, sep, value = line.strip().partition("=")
        if not sep or not key.isidentifier():
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        parsed[key] = value
    for required in ("API_URL", "SERVICE_ROLE_KEY"):
        if not parsed.get(required):
            raise ValueError(f"{required} missing from `supabase status -o env` output")
    return parsed


def managed_values(status: Mapping[str, str], environ: Mapping[str, str]) -> tuple[dict[str, str], list[str]]:
    url = status["API_URL"]
    if not is_local_url(url):
        raise ValueError(f"API_URL {url} is not a local address; refusing to write it to .env")
    managed = {"SUPABASE_URL": url, "SUPABASE_SERVICE_ROLE_KEY": status["SERVICE_ROLE_KEY"]}
    warnings = []
    for name in PASSED_SECRETS:
        value = (environ.get(name) or "").strip()
        if value:
            managed[name] = value
        else:
            warnings.append(
                f"warning: {name} is not set in the container "
                f"(export {HOST_VARIABLE[name]} on the host); keeping the .env value"
            )
    return managed, warnings


def _line_key(line: str) -> str | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None
    stripped = stripped.removeprefix("export ").lstrip()
    key, sep, _ = stripped.partition("=")
    return key.strip() if sep else None


def merge_env(existing: str, values: Mapping[str, str]) -> str:
    out: list[str] = []
    seen: set[str] = set()
    for line in existing.splitlines():
        key = _line_key(line)
        if key in values:
            if key not in seen:
                seen.add(key)
                out.append(f"{key}={values[key]}")
            continue
        out.append(line)
    missing = [key for key in MANAGED_ORDER if key in values and key not in seen]
    if missing:
        if COMMENT not in out:
            out.append(COMMENT)
        out.extend(f"{key}={values[key]}" for key in missing)
    return "\n".join(out) + "\n"


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 1:
        print("usage: supabase status -o env | write_env.py <path-to-.env>", file=sys.stderr)
        return 2
    target = Path(args[0])
    try:
        status = parse_status_env(sys.stdin.read())
        managed, warnings = managed_values(status, os.environ)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    for warning in warnings:
        print(warning, file=sys.stderr)

    existing = target.read_text() if target.exists() else ""
    merged = merge_env(existing, managed)
    tmp = target.with_name(target.name + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as handle:
        handle.write(merged)
    os.replace(tmp, target)
    os.chmod(target, 0o600)
    print(f"Wrote {len(managed)} managed keys to {target}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
