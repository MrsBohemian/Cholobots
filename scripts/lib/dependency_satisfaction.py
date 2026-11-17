#!/usr/bin/env python3
# ABOUTME: Decides whether an out-of-run epic dependency is satisfied: Done, or merged into the base branch
# ABOUTME: with every open task gated (a ``## Gate`` section).
# ABOUTME: Used by /implement-epic Step 2a-bis; claims are read with taiga-sync's own parser.
"""Out-of-run dependency satisfaction for /implement-epic.

A prerequisite outside the run is satisfied when its code is already in the
run's base branch. Taiga ``Done`` says so, but a merged story can stay at
Ready for review / Ready for test (no ``mark-done-on-merge``, or ``## Gate``
human tasks still open). So a dependency is satisfied when:

1. its story is ``Done``; or
2. at least one *merged* PR into the base branch claims it, no *open* PR (into
   any base) still claims it, and every open task of the story has a
   ``## Gate`` section. A story can ship in several PRs, and a merged first
   part with agent tasks still open is not finished; gated tasks (human or
   post-merge steps) are allowed and reported in ``gated_tasks``.

It is never satisfied when the story is ``Archived``, cannot be found, or a
lookup (story, PRs or tasks) fails; the reason is reported. "Claims" uses
``scripts/taiga_closing_refs.py``, the parser ``taiga-sync.yml`` runs, so a
bare ``#ref`` mention in prose never counts.

CLI::

    python3 scripts/lib/dependency_satisfaction.py [--base <branch>] [<ref>...]

prints ``{"satisfied_refs": [...], "dependencies": {"<ref>": {...}}}``.
``--base`` defaults to ``$BASE_BRANCH``, then ``main``.
"""
import argparse
import json
import os
import re
import subprocess
import sys
from collections.abc import Callable
from typing import Any

LIB_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(LIB_DIR)
REPO_ROOT = os.path.dirname(SCRIPTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)

from taiga_closing_refs import extract_closing_refs  # noqa: E402

GH_REPO = os.environ.get("GH_REPO", "MrsBohemian/Cholobots")
PROJECT_ID = 39
TAIGA_NOT_FOUND_EXIT = 44
PR_LIST_LIMIT = 1000
GATE_HEADING = re.compile(r"^##\s+Gate\b", re.MULTILINE)


class LookupFailed(Exception):
    """A Taiga or GitHub lookup failed; the dependency counts as not satisfied."""


def has_gate_section(description: str | None) -> bool:
    """True when a task description has a ``## Gate`` heading (it stays open after merge by design)."""
    return bool(GATE_HEADING.search(description or ""))


def claimed_refs(pr: dict[str, Any]) -> Any:
    """Taiga refs a PR claims, by the same rules as taiga-sync.yml."""
    return extract_closing_refs(pr.get("headRefName", ""), pr.get("title", ""), pr.get("body", ""))


def _status_name(story: dict[str, Any]) -> str:
    info = story.get("status_extra_info") or {}
    return info.get("name") or str(story.get("status", "unknown"))


def _open_tasks(story: dict[str, Any]) -> list[dict[str, Any]]:
    tasks = []
    for t in story.get("tasks") or []:
        info = t.get("status_extra_info") or {}
        if not info.get("is_closed", t.get("is_closed", False)):
            tasks.append({"id": t.get("id"), "ref": t.get("ref"), "subject": t.get("subject", "")})
    return tasks


def classify_dependency(
    ref: int,
    story: dict[str, Any] | None,
    prs: list[dict[str, Any]] | None,
    base_branch: str = "main",
    story_error: str | None = None,
    prs_error: str | None = None,
    gated_tasks: Callable[[list[dict[str, Any]]], list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Classify one out-of-run dependency.

    ``story`` is the ``scripts/taiga-story.sh <ref> 39 --with-tasks`` JSON, or
    None with ``story_error`` set. ``prs`` is the ``gh pr list --state all``
    JSON list, or None with ``prs_error`` set. ``gated_tasks`` is a callable
    taking the open tasks and returning the gated ones (``{"ref", "subject"}``)
    or raising ``LookupFailed``; it is only called on the merged path when a
    task is open. Returns a dict with ``satisfied``, ``via`` ("done" |
    "merged" | None), ``reason``, ``status``, ``merged_prs``, ``open_prs``,
    ``open_tasks``, ``gated_tasks`` and ``open_agent_tasks`` (open, not gated).
    """
    result = {
        "ref": ref,
        "satisfied": False,
        "via": None,
        "reason": "",
        "status": None,
        "merged_prs": [],
        "open_prs": [],
        "open_tasks": [],
        "gated_tasks": [],
        "open_agent_tasks": [],
    }

    if story is None:
        result["reason"] = f"Taiga lookup failed: {story_error or 'no story returned'}"
        return result

    status = _status_name(story)
    result["status"] = status
    result["open_tasks"] = _open_tasks(story)

    if status == "Archived":
        result["reason"] = "story is Archived"
        return result

    if status == "Done":
        result["satisfied"] = True
        result["via"] = "done"
        result["reason"] = "story is Done"
        return result

    if prs is None:
        result["reason"] = f"story is {status} and the GitHub lookup failed: {prs_error or 'no PR list'}"
        return result

    claiming = [p for p in prs if ref in claimed_refs(p)]
    result["open_prs"] = sorted(p["number"] for p in claiming if p.get("state") == "OPEN")
    result["merged_prs"] = sorted(
        p["number"] for p in claiming if p.get("state") == "MERGED" and p.get("baseRefName") == base_branch
    )

    if result["open_prs"]:
        opened = ", ".join(f"#{n}" for n in result["open_prs"])
        result["reason"] = f"story is {status} and PR {opened} still claims it"
    elif not result["merged_prs"]:
        result["reason"] = f"story is {status} and no merged PR into {base_branch} claims it"
    else:
        _check_tasks_after_merge(result, status, base_branch, gated_tasks)
    return result


def _check_tasks_after_merge(
    result: dict[str, Any],
    status: str,
    base_branch: str,
    gated_tasks: Callable[[list[dict[str, Any]]], list[dict[str, Any]]] | None,
) -> None:
    """Merged path: satisfied only when every open task is gated."""
    merged = ", ".join(f"#{n}" for n in result["merged_prs"])
    open_tasks = result["open_tasks"]
    if open_tasks:
        if gated_tasks is None:
            result["reason"] = f"story is {status}; merged PR {merged} but no task lookup was available"
            return
        try:
            result["gated_tasks"] = gated_tasks(open_tasks)
        except LookupFailed as exc:
            result["gated_tasks_error"] = str(exc)
            result["reason"] = f"story is {status}; merged PR {merged} but the task lookup failed: {exc}"
            return
        gated_refs = {t["ref"] for t in result["gated_tasks"]}
        result["open_agent_tasks"] = [t for t in open_tasks if t["ref"] not in gated_refs]
        if result["open_agent_tasks"]:
            refs = ", ".join(f"#{t['ref']}" for t in result["open_agent_tasks"])
            result["reason"] = f"story is {status}; merged PR {merged} but agent tasks open: {refs}"
            return
    result["satisfied"] = True
    result["via"] = "merged"
    result["reason"] = f"story is {status}; merged into {base_branch} by PR {merged}, no open PR claims it"
    if result["gated_tasks"]:
        gated = ", ".join(f"#{t['ref']}" for t in result["gated_tasks"])
        result["reason"] += f"; only gated tasks open: {gated}"


def _run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=False)


def fetch_story(ref: int) -> dict[str, Any]:
    """The story JSON from scripts/taiga-story.sh, with its tasks."""
    proc = _run(["scripts/taiga-story.sh", str(ref), str(PROJECT_ID), "--with-tasks"])
    if proc.returncode == TAIGA_NOT_FOUND_EXIT:
        raise LookupFailed(f"story #{ref} not found in project {PROJECT_ID}")
    if proc.returncode != 0:
        raise LookupFailed(f"taiga-story.sh exited {proc.returncode}: {proc.stderr.strip()[:200]}")
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise LookupFailed(f"taiga-story.sh printed invalid JSON: {exc}") from exc


def fetch_prs(limit: int = PR_LIST_LIMIT) -> list[dict[str, Any]]:
    """Every PR on the upstream repo, open, closed or merged."""
    proc = _run([
        "env", "-u", "GH_TOKEN", "gh", "pr", "list",
        "--repo", GH_REPO,
        "--state", "all",
        "--limit", str(limit),
        "--json", "number,title,body,headRefName,baseRefName,state",
    ])
    if proc.returncode != 0:
        raise LookupFailed(f"gh pr list exited {proc.returncode}: {proc.stderr.strip()[:200]}")
    try:
        prs = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise LookupFailed(f"gh pr list printed invalid JSON: {exc}") from exc
    if not isinstance(prs, list):
        raise LookupFailed("gh pr list did not return a list")
    if len(prs) >= limit:
        raise LookupFailed(f"gh pr list returned {len(prs)} PRs, the limit; the list may be truncated")
    return prs


def fetch_gated_tasks(open_tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Open tasks whose description has a ``## Gate`` section (human or live steps)."""
    from taiga_client import TaigaClient, TaigaError

    try:
        client = TaigaClient.from_env()
        gated = []
        for t in open_tasks:
            detail = client.get(f"/api/v1/tasks/{t['id']}")
            if has_gate_section((detail or {}).get("description")):
                gated.append({"ref": t["ref"], "subject": t["subject"]})
        return gated
    except TaigaError as exc:
        raise LookupFailed(f"task lookup failed: {exc}") from exc


def resolve(
    refs: list[int],
    base_branch: str = "main",
    fetch_story: Callable[[int], dict[str, Any]] = fetch_story,
    fetch_prs: Callable[[], list[dict[str, Any]]] = fetch_prs,
    gated_tasks: Callable[[list[dict[str, Any]]], list[dict[str, Any]]] | None = fetch_gated_tasks,
) -> dict[str, Any]:
    """Classify every ref. The PR list is fetched at most once, and only when needed."""
    stories = {}
    for ref in refs:
        try:
            stories[ref] = (fetch_story(ref), None)
        except LookupFailed as exc:
            stories[ref] = (None, str(exc))

    needs_prs = any(s is not None and _status_name(s) not in ("Done", "Archived") for s, _ in stories.values())
    prs, prs_error = None, None
    if needs_prs:
        try:
            prs = fetch_prs()
        except LookupFailed as exc:
            prs_error = str(exc)

    dependencies = {}
    satisfied = []
    for ref in refs:
        story, story_error = stories[ref]
        result = classify_dependency(
            ref, story, prs, base_branch=base_branch, story_error=story_error, prs_error=prs_error,
            gated_tasks=gated_tasks,
        )
        if result["satisfied"]:
            satisfied.append(ref)
        dependencies[str(ref)] = result

    return {"base_branch": base_branch, "satisfied_refs": satisfied, "dependencies": dependencies}


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base", default=os.environ.get("BASE_BRANCH") or "main",
                        help="base branch a merged PR must target (default: $BASE_BRANCH or main)")
    parser.add_argument("refs", nargs="*", type=int,
                        help="out-of-run dependency refs; none prints an empty result")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    print(json.dumps(resolve(args.refs, base_branch=args.base), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
