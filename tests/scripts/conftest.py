# ABOUTME: Shared pytest helpers for the script tests, notably a throwaway git host repo.
# ABOUTME: Lets orchestrator tests run real git against a sandbox instead of the developer's own clone.
import subprocess
import tempfile
from pathlib import Path

# Identity passed on every commit so the sandbox never depends on (or writes)
# the developer's global git config.
_GIT_IDENTITY = ["-c", "user.name=Sandbox", "-c", "user.email=sandbox@example.invalid"]


def _git(*args, cwd):
    subprocess.run(
        ["git", *_GIT_IDENTITY, *args],
        cwd=cwd, check=True, capture_output=True, text=True,
    )


def make_host_repo(parent, stale_main=False):
    """Create a sandbox host repo with `main` checked out and a local bare
    `origin`, in a fresh directory under `parent`, and return the host repo
    path. Each call gets its own directory, so one test may create several.

    The orchestrator's base-reachability check fetches from origin and
    compares against the checked-out `main`; pointing it here keeps those
    calls offline and independent of the developer's real clone. With
    `stale_main`, origin gains a commit the host's `main` lacks.
    """
    Path(parent).mkdir(parents=True, exist_ok=True)
    path = Path(tempfile.mkdtemp(prefix="host-repo-", dir=parent))
    origin = path / "origin.git"
    host = path / "host"
    seed = path / "seed"

    _git("init", "--quiet", "--bare", "--initial-branch=main", str(origin), cwd=path)
    _git("init", "--quiet", "--initial-branch=main", str(seed), cwd=path)
    _git("commit", "--quiet", "--allow-empty", "-m", "initial", cwd=seed)
    _git("remote", "add", "origin", str(origin), cwd=seed)
    _git("push", "--quiet", "origin", "main", cwd=seed)
    _git("clone", "--quiet", str(origin), str(host), cwd=path)

    if stale_main:
        _git("commit", "--quiet", "--allow-empty", "-m", "ahead of host", cwd=seed)
        _git("push", "--quiet", "origin", "main", cwd=seed)

    return host
