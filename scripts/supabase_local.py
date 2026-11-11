#!/usr/bin/env python3
# ABOUTME: Core of the per-checkout local Supabase stack: identity, slot registry, ports, config rendering.
# ABOUTME: Stdlib only. The main checkout keeps the default ports; each worktree gets its own id and ports.
import fcntl
import hashlib
import json
import os
import re
import shlex
import shutil
import socket
import subprocess
import sys
import tomllib
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


def worktree_key(toplevel: str) -> str:
    """Sanitized basename plus a hash of the full path: the registry key and the project id suffix.

    The hash keeps `/a/story-72` and `/b/story-72` (or `Story_72` and `story-72`) from sharing a stack."""
    path = os.path.realpath(toplevel)
    digest = hashlib.sha256(path.encode()).hexdigest()[:8]
    return f"{sanitize_name(os.path.basename(path))}-{digest}"


def checkout_identity(path: str, environ: Mapping[str, str]) -> Identity:
    toplevel = os.path.realpath(_git(path, "rev-parse", "--show-toplevel"))
    git_dir = os.path.realpath(_git(path, "rev-parse", "--absolute-git-dir"))
    common = os.path.realpath(
        _git(path, "rev-parse", "--path-format=absolute", "--git-common-dir")
    )
    is_main = git_dir == common or environ.get("CHOLOBOTS_DEVCONTAINER") == "1"
    name = worktree_key(toplevel)
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
    """Split `<command words> [checkout-path] [args...]` into (first word, checkout, remaining args).

    The checkout path sits right after the command words: one word, or two for the CLI's command
    groups (`db reset <path>`, `migration new <path> name`)."""
    sub = argv[0]
    words = 2 if sub in COMMAND_GROUPS and len(argv) > 1 else 1
    head, tail = list(argv[1:words]), list(argv[words:])
    if tail and not tail[0].startswith("-") and os.path.isdir(tail[0]):
        return sub, tail[0], head + tail[1:]
    return sub, environ.get("CHOLOBOTS_CHECKOUT") or cwd, head + tail


def port_in_use(port: int) -> bool:
    with socket.socket() as s:
        # Docker binds with SO_REUSEADDR, so connections a stopped stack left in TIME_WAIT must
        # not count as taken; a live listener still fails the bind.
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("127.0.0.1", port))
        except OSError:
            return True
    return False


DEFAULT_CLI = "npx --yes supabase@2.121.0"
# Option names (matched with or without `=value`) that select a hosted project or database.
HOSTED_FLAGS = {"--linked", "--db-url", "--project-ref", "--password", "-p"}
HOSTED_WORDS = {"link", "login"}
# Options that would point a command at another stack than the selected checkout's. This helper
# supplies --workdir and --project-id itself.
RETARGET_FLAGS = {"--workdir", "--project-id"}
# Commands whose target is chosen by --local/--linked/--db-url: --local is always forced so a
# hosted default can never apply. `migration new` only writes a file and has no target flag.
FORCE_LOCAL = {("db", "reset"), ("db", "diff"), ("db", "lint"), ("migration", "list"), ("migration", "up"), ("test", "db")}
# Handled here, against this checkout's stack.
NATIVE_COMMANDS = {"start", "stop", "status", "env", "prune"}
# An allowlist: other commands (`db pull`, `gen types`, `projects`, ...) can reach the hosted project
# when the checkout carries link metadata.
PASSTHROUGH_COMMANDS = {
    ("db", "reset"), ("db", "diff"), ("db", "lint"),
    ("migration", "new"), ("migration", "list"), ("migration", "up"),
    ("test", "db"),
}
WORKDIR_NAME = ".supabase-local"
COMMAND_GROUPS = {group for group, _ in PASSTHROUGH_COMMANDS}
USAGE = "usage: supabase-local.sh start|stop|status|env|prune|<supabase cli subcommand> [checkout-path] [args...]"


class LocalError(Exception):
    def __init__(self, message: str, code: int = 1):
        super().__init__(message)
        self.code = code


def _fail(message: str) -> None:
    print(f"supabase-local: {message}", file=sys.stderr)


def hosted_operation(argv: list[str]) -> str | None:
    for tok in argv:
        name, _, value = tok.partition("=")
        if tok in HOSTED_WORDS or name in HOSTED_FLAGS:
            return tok
        if name == "--local" and "=" in tok and value != "true":
            return tok
    if argv[:1] == ["db"] and "push" in argv:
        return "db push"
    return None


def retargeting_option(argv: list[str]) -> str | None:
    for tok in argv:
        if tok.partition("=")[0] in RETARGET_FLAGS:
            return tok
    return None


def allowed_command(argv: list[str]) -> bool:
    if argv[0] in NATIVE_COMMANDS:
        return True
    return tuple(argv[:2]) in PASSTHROUGH_COMMANDS


def _cli(environ: Mapping[str, str]) -> list[str]:
    return shlex.split(environ.get("SUPABASE_CLI", DEFAULT_CLI))


def _workdir(identity: Identity) -> str:
    return identity.toplevel if identity.is_main else os.path.join(identity.toplevel, WORKDIR_NAME)


def _slot_for_start(identity: Identity) -> int:
    return 0 if identity.is_main else allocate_slot(registry_path(identity), identity.name)


def _stack_running(identity: Identity, run) -> bool:
    try:
        result = run(
            ["docker", "ps", "--filter", f"name=supabase_db_{identity.project_id}", "--format", "{{.Names}}"],
            capture_output=True, text=True,
        )
    except FileNotFoundError:
        return False
    return f"supabase_db_{identity.project_id}" in (result.stdout or "").split()


def _prepare_workdir(identity: Identity, slot: int, base_config: str) -> str:
    """Render config.toml and link the rest of supabase/ into <checkout>/.supabase-local/supabase."""
    src = os.path.join(identity.toplevel, "supabase")
    dst = os.path.join(_workdir(identity), "supabase")
    os.makedirs(dst, exist_ok=True)
    wanted = mirror_entries(src)
    for entry in os.listdir(dst):
        path = os.path.join(dst, entry)
        if entry != "config.toml" and os.path.islink(path) and entry not in wanted:
            os.unlink(path)
    with open(os.path.join(dst, "config.toml"), "w") as f:
        f.write(render_config(base_config, identity.project_id, slot))
    for entry in wanted:
        link = os.path.join(dst, entry)
        if os.path.lexists(link):
            os.unlink(link)
        os.symlink(os.path.relpath(os.path.join(src, entry), dst), link)
    return os.path.dirname(dst)


def _ports_to_probe(base_config: str, excludes: list[str], slot: int) -> list[int]:
    cfg = tomllib.loads(base_config)
    block = port_block(slot)

    def enabled(*path: str) -> bool:
        node = cfg
        for key in path:
            node = node.get(key, {}) if isinstance(node, dict) else {}
        return node.get("enabled", True) if isinstance(node, dict) else True

    ports = [block["shadow"], block["api"], block["db"]]
    if cfg.get("db", {}).get("pooler", {}).get("enabled", False):
        ports.append(block["pooler"])
    if "studio" not in excludes and enabled("studio"):
        ports.append(block["studio"])
    if "mailpit" not in excludes and enabled("inbucket") and enabled("local_smtp"):
        ports.append(block["smtp"])
    if "logflare" not in excludes and enabled("analytics"):
        ports.append(block["analytics"])
    if "edge-runtime" not in excludes and enabled("edge_runtime"):
        ports.append(block["inspector"])
    return ports


def _print_env(cli: list[str], workdir: str, run) -> int:
    result = run([*cli, "status", "--workdir", workdir, "-o", "env"], capture_output=True, text=True)
    if result.returncode != 0:
        sys.stderr.write(result.stderr or "")
        return result.returncode
    status = parse_status_env(result.stdout or "")
    try:
        lines = env_lines(status)
    except KeyError as missing:
        _fail(f"supabase status did not report {missing}")
        return 1
    print("\n".join(lines))
    return 0


def _require_workdir(identity: Identity) -> str:
    workdir = _workdir(identity)
    if not os.path.isdir(workdir):
        raise LocalError(
            f"no stack for {identity.project_id}; run scripts/supabase-local.sh start {identity.toplevel}"
        )
    return workdir


def _start(identity, cli, environ, run) -> int:
    base_path = os.path.join(identity.toplevel, "supabase", "config.toml")
    if not os.path.isfile(base_path):
        _fail(f"no supabase/config.toml in {identity.toplevel}; nothing to start")
        return 0
    with open(base_path) as f:
        base_config = f.read()
    excludes = default_excludes(identity, environ)
    slot = _slot_for_start(identity)
    if not _stack_running(identity, run):
        for port in _ports_to_probe(base_config, excludes, slot):
            if port_in_use(port):
                raise LocalError(f"port {port} (slot {slot}) is in use")
    workdir = identity.toplevel if identity.is_main else _prepare_workdir(identity, slot, base_config)
    command = [*cli, "start", "--workdir", workdir]
    if excludes:
        command += ["-x", ",".join(excludes)]
    result = run(command, stdout=2)
    if result.returncode != 0:
        return result.returncode
    return _print_env(cli, workdir, run)


def _stop(identity, cli, run) -> int:
    workdir = _workdir(identity)
    command = [*cli, "stop"]
    if os.path.isdir(workdir):
        command += ["--workdir", workdir]
    result = run([*command, "--project-id", identity.project_id, "--no-backup"], stdout=2)
    if result.returncode != 0:
        return result.returncode
    if not identity.is_main:
        release_slot(registry_path(identity), identity.name)
        shutil.rmtree(workdir, ignore_errors=True)
    return 0


def _prune(identity, cli, run) -> int:
    listing = run(["git", "-C", identity.toplevel, "worktree", "list", "--porcelain"], capture_output=True, text=True, check=True)
    # A worktree whose directory was deleted stays listed (marked prunable) until git prunes its
    # metadata, so only an existing directory counts as live.
    live = {
        worktree_key(path)
        for path in (
            line.split(" ", 1)[1].strip()
            for line in listing.stdout.splitlines()
            if line.startswith("worktree ")
        )
        if os.path.isdir(path)
    }
    reg = registry_path(identity)
    code = 0
    for name in sorted(set(registered_slots(reg)) - live):
        result = run([*cli, "stop", "--project-id", f"cholobots-{name}", "--no-backup"], stdout=2)
        if result.returncode == 0:
            release_slot(reg, name)
        else:
            code = result.returncode
    return code


def main(argv: list[str], environ: Mapping[str, str] = os.environ, run=subprocess.run) -> int:
    if not argv:
        print(USAGE, file=sys.stderr)
        return 2
    hosted = hosted_operation(argv)
    if hosted:
        _fail(f"refusing hosted operation {hosted}")
        return 2
    retarget = retargeting_option(argv)
    if retarget:
        _fail(f"refusing option {retarget}: the checkout's own stack is selected by path, not by flag")
        return 2
    if not allowed_command(argv):
        _fail(f"{' '.join(argv[:2])!r} is not an allowed local-only command; allowed: "
              + ", ".join(sorted(NATIVE_COMMANDS | {" ".join(c) for c in PASSTHROUGH_COMMANDS})))
        return 2
    sub, checkout, rest = split_args(argv, environ, os.getcwd())
    cli = _cli(environ)
    try:
        identity = checkout_identity(checkout, environ)
        if sub == "start":
            return _start(identity, cli, environ, run)
        if sub == "stop":
            return _stop(identity, cli, run)
        if sub == "prune":
            return _prune(identity, cli, run)
        workdir = _require_workdir(identity)
        if sub == "env":
            return _print_env(cli, workdir, run)
        if sub == "status":
            return run([*cli, "status", *rest, "--workdir", workdir]).returncode
        local = ["--local"] if tuple(argv[:2]) in FORCE_LOCAL else []
        return run([*cli, sub, *rest, *local, "--workdir", workdir]).returncode
    except LocalError as error:
        _fail(str(error))
        return error.code
    except RuntimeError as error:
        _fail(str(error))
        return 1
    except subprocess.CalledProcessError as error:
        _fail(f"{' '.join(error.cmd)} failed: {(error.stderr or '').strip()}")
        return error.returncode


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
