#!/usr/bin/env python3
# ABOUTME: Core of the per-checkout local Supabase stack: identity, slot registry, port block, config rendering.
# ABOUTME: Stdlib only; the main checkout keeps default ports, a worktree gets its own project id and port block.
import fcntl
import json
import os
import re
import socket
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass

SLOT_COUNT = 99
REGISTRY_NAME = "cholobots-supabase-slots.json"
MIRROR_SKIP = {"config.toml", ".temp", ".branches"}
WORKTREE_EXCLUDES = ["studio", "mailpit", "imgproxy", "logflare", "vector", "edge-runtime"]
PORT_DEFAULTS = {
    "shadow": 54320, "api": 54321, "db": 54322, "studio": 54323,
    "smtp": 54324, "analytics": 54327, "pooler": 54329, "inspector": 8083,
}
_PORT_LINE = re.compile(r"^(\s*)([a-z_]*port)(\s*=\s*)(\d+)(\s*)$")
_PROJECT_LINE = re.compile(r'^project_id\s*=\s*".*"\s*$')
_STATUS_LINE = re.compile(r"^([A-Z][A-Z0-9_]*)=(.*)$")


@dataclass(frozen=True)
class Identity:
    toplevel: str
    git_common_dir: str
    name: str
    project_id: str
    is_main: bool


def _git(path: str, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", path, *args], check=True, capture_output=True, text=True
    ).stdout.strip()


def sanitize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9-]+", "-", name.lower()).strip("-")


def checkout_identity(path: str, environ: Mapping[str, str]) -> Identity:
    toplevel = os.path.realpath(_git(path, "rev-parse", "--show-toplevel"))
    git_dir = os.path.realpath(_git(path, "rev-parse", "--absolute-git-dir"))
    common = os.path.realpath(
        _git(path, "rev-parse", "--path-format=absolute", "--git-common-dir")
    )
    is_main = git_dir == common or environ.get("CHOLOBOTS_DEVCONTAINER") == "1"
    name = sanitize_name(os.path.basename(toplevel))
    project_id = "cholobots" if is_main else f"cholobots-{name}"
    return Identity(toplevel, common, name, project_id, is_main)


def registry_path(identity: Identity) -> str:
    return os.path.join(identity.git_common_dir, REGISTRY_NAME)


def _locked_update(path: str, update):
    """Read-modify-write the slot registry under an exclusive flock on a sibling lock file."""
    with open(path + ".lock", "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            with open(path) as f:
                slots = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            slots = {}
        result = update(slots)
        with open(path, "w") as f:
            json.dump(slots, f, indent=1, sort_keys=True)
        return result


def allocate_slot(path: str, name: str) -> int:
    def update(slots: dict) -> int:
        if name in slots:
            return slots[name]
        used = set(slots.values())
        for slot in range(1, SLOT_COUNT + 1):
            if slot not in used:
                slots[name] = slot
                return slot
        raise RuntimeError("all 99 Supabase slots in use; run scripts/supabase-local.sh prune")

    return _locked_update(path, update)


def release_slot(path: str, name: str) -> None:
    _locked_update(path, lambda slots: slots.pop(name, None))


def registered_slots(path: str) -> dict[str, int]:
    return _locked_update(path, lambda slots: dict(slots))


def port_block(slot: int) -> dict[str, int]:
    return {
        key: port + (slot if key == "inspector" else 10 * slot)
        for key, port in PORT_DEFAULTS.items()
    }


def render_config(text: str, project_id: str, slot: int) -> str:
    if slot == 0:
        return text
    out = []
    for line in text.split("\n"):
        if _PROJECT_LINE.match(line):
            line = f'project_id = "{project_id}"'
        else:
            m = _PORT_LINE.match(line)
            if m:
                indent, key, eq, value, tail = m.groups()
                port = int(value)
                if key == "inspector_port" and port == PORT_DEFAULTS["inspector"]:
                    line = f"{indent}{key}{eq}{port + slot}{tail}"
                elif 54320 <= port <= 54329:
                    line = f"{indent}{key}{eq}{port + 10 * slot}{tail}"
        out.append(line)
    return "\n".join(out)


def mirror_entries(src_dir: str) -> list[str]:
    return sorted(n for n in os.listdir(src_dir) if n not in MIRROR_SKIP)


def default_excludes(identity: Identity, environ: Mapping[str, str]) -> list[str]:
    if "SUPABASE_LOCAL_EXCLUDE" in environ:
        return [s.strip() for s in environ["SUPABASE_LOCAL_EXCLUDE"].split(",") if s.strip()]
    return [] if identity.is_main else list(WORKTREE_EXCLUDES)


def parse_status_env(text: str) -> dict[str, str]:
    parsed = {}
    for line in text.splitlines():
        m = _STATUS_LINE.match(line.strip())
        if m:
            parsed[m.group(1)] = m.group(2).strip().strip('"')
    return parsed


def env_lines(status: Mapping[str, str]) -> list[str]:
    return [
        f"{bot_key}={status[cli_key]}"
        for bot_key, cli_key in (
            ("SUPABASE_URL", "API_URL"),
            ("SUPABASE_SERVICE_ROLE_KEY", "SERVICE_ROLE_KEY"),
            ("SUPABASE_DB_URL", "DB_URL"),
        )
    ]


def split_args(argv: list[str], environ: Mapping[str, str], cwd: str) -> tuple[str, str, list[str]]:
    sub, rest = argv[0], list(argv[1:])
    if rest and not rest[0].startswith("-") and os.path.isdir(rest[0]):
        return sub, rest[0], rest[1:]
    return sub, environ.get("CHOLOBOTS_CHECKOUT") or cwd, rest


def port_in_use(port: int) -> bool:
    with socket.socket() as s:
        try:
            s.bind(("127.0.0.1", port))
        except OSError:
            return True
    return False
