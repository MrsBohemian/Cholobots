#!/usr/bin/env python3
# ABOUTME: Brings Taiga project 39 (cholo-bots) up to the pipeline's status, priority, tag and wiki set.
# ABOUTME: Default dry run; --apply creates only what is missing and renumbers order; --check exits 2 when incomplete.
"""Taiga project setup for the Cholobots pipeline.

Usage:
  python3 scripts/taiga_project_setup.py [--project 39] [--check | --apply]

Modes:
  (default)  Dry run: print the before/after table and the writes --apply would make.
  --apply    Make those writes. Needs a Taiga project admin account.
  --check    Print what is missing; exit 0 when complete, 2 when anything is missing.

Credentials come from the shell environment only: TAIGA_API_URL, TAIGA_USERNAME
and TAIGA_PASSWORD. In this checkout direnv exports the agent account from the
jon-dev-env vault, which is enough for the dry run and --check. --apply needs an
admin, so Jon loads the admin scope into a subshell for that one command.

Idempotent by case-insensitive name. It creates missing story statuses, issue
statuses, priorities, tags and the roadmap wiki page, and PATCHes `order` so the
statuses and priorities follow the target sequence. It never deletes, renames or
recolours anything, and never changes `is_closed` on an existing status.

Exit codes: 0 success (or complete, for --check), 1 error, 2 incomplete (--check).

Jon's one-off run, after storing the admin scope with envtool set:
  (eval "$(dv export project/taiga-growthgarage-jonroberts)" && python3 scripts/taiga_project_setup.py --apply)
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence, Tuple

DEFAULT_PROJECT_ID = 39
EXIT_OK = 0
EXIT_ERROR = 1
EXIT_INCOMPLETE = 2
ENV_KEYS = ("TAIGA_API_URL", "TAIGA_USERNAME", "TAIGA_PASSWORD")
# Printed for Jon wherever the project is incomplete; the skills print the same line.
APPLY_COMMAND = (
    '(eval "$(dv export project/taiga-growthgarage-jonroberts)"'
    " && python3 scripts/taiga_project_setup.py --apply)"
)


@dataclass(frozen=True)
class Target:
    name: str
    color: str
    is_closed: Optional[bool] = None  # None for priorities, which have no is_closed


STORY_STATUSES: Tuple[Target, ...] = (
    Target("New", "#70728F", False),
    Target("Ready for Analysis", "#9B59B6", False),
    Target("Ready", "#E44057", False),
    Target("In progress", "#E47C40", False),
    Target("Ready for review", "#40E4CE", False),
    Target("Ready for test", "#E4CE40", False),
    Target("Blocked", "#B83A3A", False),
    Target("Done", "#A8E440", True),
    Target("Archived", "#A9AABC", True),
)

ISSUE_STATUSES: Tuple[Target, ...] = (
    Target("New", "#70728F", False),
    Target("In progress", "#40A8E4", False),
    Target("Ready for review", "#40E4CE", False),
    Target("Ready for test", "#E47C40", False),
    Target("Blocked", "#B83A3A", False),
    Target("Needs Info", "#E44057", False),
    Target("Closed", "#A8E440", True),
    Target("Rejected", "#A9AABC", True),
    Target("Postponed", "#5178D3", False),
)

PRIORITIES: Tuple[Target, ...] = (
    Target("Low", "#A9AABC"),
    Target("Normal", "#A8E440"),
    Target("High", "#E44057"),
    Target("Urgent", "#B83A3A"),
)

# Pipeline tags, each with its own colour.
TAGS: Tuple[Tuple[str, str], ...] = (
    ("triaged", "#2E86C1"),
    ("llm-analyzed", "#8E44AD"),
    ("escalation-needed", "#C0392B"),
    ("epic-candidate", "#D68910"),
    ("mark-done-on-merge", "#229954"),
    ("close-tasks-on-merge", "#17A589"),
    ("product", "#F1C40F"),
    ("devx", "#5DADE2"),
    ("pipeline", "#A569BD"),
    ("ci", "#566573"),
    ("src:user", "#E67E22"),
    ("src:monitor", "#CB4335"),
    ("src:manual-test", "#28B463"),
    ("src:pr-review", "#2874A6"),
    ("src:agent", "#7D3C98"),
    ("src:jon", "#B7950B"),
    ("rebase-conflict", "#922B21"),
    ("design-reviewed", "#1ABC9C"),
    ("analysis-rejected", "#7F8C8D"),
    ("spike-complete", "#2ECC71"),
)

WIKI_SLUG = "cross-project-execution-order"
WIKI_CONTENT = """# Cross-project execution order

## Dependency Diagram

Execution order of open stories across epics, as `cholobots#N` refs; roadmap-wiki-update maintains it and the overseer reads it.

## Cross-project dependency register

Dependencies on work outside this project, one line each; refs in other namespaces are recorded as unknown and never looked up.
"""

# (label, list endpoint, target list)
ORDERED_KINDS = (
    ("story status", "/api/v1/userstory-statuses", STORY_STATUSES),
    ("issue status", "/api/v1/issue-statuses", ISSUE_STATUSES),
    ("priority", "/api/v1/priorities", PRIORITIES),
)


class SetupError(RuntimeError):
    """A request failed or the credentials are unusable."""


# -- transport -------------------------------------------------------------

Transport = Callable[[str, str, Dict[str, str], Optional[bytes]], Tuple[int, object]]


def urllib_transport(method: str, url: str, headers: Dict[str, str], body: Optional[bytes]) -> Tuple[int, object]:
    """Send one HTTP request; return (status, parsed JSON body or None)."""
    request = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            status, raw = response.status, response.read()
    except urllib.error.HTTPError as exc:
        status, raw = exc.code, exc.read()
    except urllib.error.URLError as exc:
        raise SetupError(f"{method} {url} failed: {exc.reason}") from exc
    try:
        return status, json.loads(raw) if raw else None
    except json.JSONDecodeError:
        return status, None


class TaigaApi:
    def __init__(self, base_url: str, transport: Transport):
        self.base_url = base_url.rstrip("/")
        self.transport = transport
        self.token: Optional[str] = None
        self.writes: List[str] = []

    def _call(self, method: str, path: str, payload: Optional[dict] = None) -> object:
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        body = json.dumps(payload).encode() if payload is not None else None
        status, data = self.transport(method, f"{self.base_url}{path}", headers, body)
        if status >= 400:
            detail = json.dumps(data)[:300] if data is not None else ""
            raise SetupError(f"{method} {path} returned HTTP {status} {detail}")
        if method != "GET":
            self.writes.append(f"{method} {path}")
        return data

    def login(self, username: str, password: str) -> None:
        data = self._call("POST", "/api/v1/auth", {"type": "normal", "username": username, "password": password})
        self.writes.pop()  # logging in is not a write to the project
        if not isinstance(data, dict) or not data.get("auth_token"):
            raise SetupError("Taiga auth response did not include auth_token")
        self.token = data["auth_token"]

    def get(self, path: str) -> object:
        return self._call("GET", path)

    def post(self, path: str, payload: dict) -> object:
        return self._call("POST", path, payload)

    def patch(self, path: str, payload: dict) -> object:
        return self._call("PATCH", path, payload)


# -- credentials -------------------------------------------------------------


def resolve_credentials(environ: Dict[str, str]) -> Dict[str, str]:
    creds: Dict[str, str] = {}
    for key in ENV_KEYS:
        value = environ.get(key, "")
        if not value.strip():
            raise SetupError(f"{key} is not set: export it, or load it with direnv or envtool")
        creds[key] = value
    return creds


# -- planning -----------------------------------------------------------------


@dataclass
class Plan:
    creates: List[Tuple[str, str, dict]] = field(default_factory=list)  # (label, path, payload)
    reorders: List[Tuple[str, str, dict]] = field(default_factory=list)  # (label, path, payload)
    tag_creates: List[Tuple[str, str, dict]] = field(default_factory=list)
    wiki_create: Optional[Tuple[str, str, dict]] = None
    missing: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    table: List[Tuple[str, str, str, str]] = field(default_factory=list)  # kind, name, before, after

    @property
    def complete(self) -> bool:
        return not self.missing


def _index_by_name(items: Sequence[dict]) -> Dict[str, dict]:
    return {item["name"].lower(): item for item in items}


def plan_ordered(plan: Plan, label: str, path: str, targets: Sequence[Target], existing: List[dict], project: int) -> None:
    by_name = _index_by_name(existing)
    present = [t for t in targets if t.name.lower() in by_name]
    orders = [by_name[t.name.lower()]["order"] for t in present]
    relative_ok = all(a < b for a, b in zip(orders, orders[1:]))
    absent = [t for t in targets if t.name.lower() not in by_name]

    for target in absent:
        payload = {"project": project, "name": target.name, "order": targets.index(target) + 1, "color": target.color}
        if target.is_closed is not None:
            payload["is_closed"] = target.is_closed
        plan.creates.append((label, path, payload))
        plan.missing.append(f"{label} '{target.name}'")

    if not relative_ok:
        plan.missing.append(f"{label} order (not {', '.join(t.name for t in targets)})")

    for target in present:
        item = by_name[target.name.lower()]
        if target.is_closed is not None and item.get("is_closed") != target.is_closed:
            # Counted as missing so --check (the pipeline prerequisite) exits 2:
            # a "Done" status that is not closed makes the Taiga gates skip PRs.
            # --apply never rewrites is_closed, so this one is fixed in the admin.
            plan.missing.append(
                f"{label} '{item['name']}' is_closed={item.get('is_closed')}, expected {target.is_closed} "
                "(--apply does not change it; fix it in the Taiga admin)"
            )

    # Renumber to 1..n whenever something is inserted or the relative order is wrong.
    renumber = bool(absent) or not relative_ok
    for position, target in enumerate(targets, start=1):
        item = by_name.get(target.name.lower())
        before = f"order {item['order']}" if item else "missing"
        after = f"order {position}" if renumber else before
        if item and renumber and item["order"] != position:
            plan.reorders.append((label, f"{path}/{item['id']}", {"order": position}))
        if not item:
            after = f"create, order {position}"
        plan.table.append((label, target.name, before, after))

    extras = [i for i in existing if i["name"].lower() not in {t.name.lower() for t in targets}]
    for offset, item in enumerate(sorted(extras, key=lambda i: i["order"]), start=len(targets) + 1):
        if renumber and item["order"] != offset:
            plan.reorders.append((label, f"{path}/{item['id']}", {"order": offset}))
        plan.table.append((label, item["name"] + " (not in target)", f"order {item['order']}",
                           f"order {offset}" if renumber else f"order {item['order']}"))


def build_plan(api: TaigaApi, project: int) -> Plan:
    plan = Plan()
    for label, path, targets in ORDERED_KINDS:
        existing = api.get(f"{path}?project={project}")
        if not isinstance(existing, list):
            raise SetupError(f"Expected a list from {path}")
        plan_ordered(plan, label, path, targets, existing, project)

    project_data = api.get(f"/api/v1/projects/{project}")
    existing_tags = {name.lower() for name in ((project_data or {}).get("tags_colors") or {})}
    for tag, color in TAGS:
        if tag.lower() in existing_tags:
            plan.table.append(("tag", tag, "exists", "exists"))
        else:
            plan.tag_creates.append(("tag", f"/api/v1/projects/{project}/create_tag", {"tag": tag, "color": color}))
            plan.missing.append(f"tag '{tag}'")
            plan.table.append(("tag", tag, "missing", f"create {color}"))

    pages = api.get(f"/api/v1/wiki?project={project}")
    slugs = {page.get("slug") for page in (pages or [])}
    if WIKI_SLUG in slugs:
        plan.table.append(("wiki", WIKI_SLUG, "exists", "exists"))
    else:
        plan.wiki_create = ("wiki", "/api/v1/wiki", {"project": project, "slug": WIKI_SLUG, "content": WIKI_CONTENT})
        plan.missing.append(f"wiki page '{WIKI_SLUG}'")
        plan.table.append(("wiki", WIKI_SLUG, "missing", "create"))
    return plan


def apply_plan(api: TaigaApi, plan: Plan) -> None:
    # Creates first (each with its target order), then the renumbering PATCHes.
    for _, path, payload in plan.creates:
        api.post(path, payload)
    for _, path, payload in plan.reorders:
        api.patch(path, payload)
    for _, path, payload in plan.tag_creates:
        api.post(path, payload)
    if plan.wiki_create:
        api.post(plan.wiki_create[1], plan.wiki_create[2])


def render_table(plan: Plan) -> str:
    rows = [("kind", "name", "before", "after")] + plan.table
    widths = [max(len(row[i]) for row in rows) for i in range(4)]
    lines = ["  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)).rstrip() for row in rows]
    lines.insert(1, "  ".join("-" * w for w in widths))
    return "\n".join(lines)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Set up Taiga project statuses, priorities, tags and the roadmap wiki page.")
    parser.add_argument("--project", type=int, default=DEFAULT_PROJECT_ID)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true", help="make the writes (default is a dry run)")
    mode.add_argument("--check", action="store_true", help="exit 0 when complete, 2 when anything is missing")
    return parser.parse_args(list(argv))


def main(
    argv: Sequence[str],
    transport: Transport = urllib_transport,
    environ: Optional[Dict[str, str]] = None,
) -> int:
    args = parse_args(argv)
    environ = dict(os.environ) if environ is None else environ
    try:
        creds = resolve_credentials(environ)
        api = TaigaApi(creds["TAIGA_API_URL"], transport)
        api.login(creds["TAIGA_USERNAME"], creds["TAIGA_PASSWORD"])
        plan = build_plan(api, args.project)
    except (SetupError, OSError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return EXIT_ERROR

    for warning in plan.warnings:
        print(f"Warning: {warning}", file=sys.stderr)

    if args.check:
        if plan.complete:
            print(f"Taiga project {args.project} is complete.")
            return EXIT_OK
        print(f"Taiga project {args.project} is missing {len(plan.missing)} item(s):")
        for item in plan.missing:
            print(f"  - {item}")
        print(f"Run: {APPLY_COMMAND}")
        return EXIT_INCOMPLETE

    print(f"Taiga project {args.project}{'' if args.apply else ' (dry run)'} as {creds['TAIGA_USERNAME']}")
    print(render_table(plan))
    writes = (
        [f"POST {p} {json.dumps(d)}" for _, p, d in plan.creates]
        + [f"PATCH {p} {json.dumps(d)}" for _, p, d in plan.reorders]
        + [f"POST {p} {json.dumps(d)}" for _, p, d in plan.tag_creates]
        + ([f"POST {plan.wiki_create[1]} (slug {WIKI_SLUG})"] if plan.wiki_create else [])
    )
    if not writes:
        print("\nNothing to do.")
        return EXIT_OK
    if not args.apply:
        print(f"\n{len(writes)} write(s) --apply would make:")
        for write in writes:
            print(f"  {write}")
        return EXIT_OK

    try:
        apply_plan(api, plan)
    except SetupError as exc:
        print(f"Error: {exc} (after {len(api.writes)} successful write(s); re-run to finish)", file=sys.stderr)
        return EXIT_ERROR
    print(f"\nApplied {len(api.writes)} write(s).")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
