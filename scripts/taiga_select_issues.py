#!/usr/bin/env python3
# ABOUTME: Read-only selector that picks the Taiga issues a bug sweep skill should process.
# ABOUTME: Applies per-purpose eligibility, include/exclude filters, the Urgent split and sorting; prints JSON.
import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "lib"))

from taiga_client import TaigaClient, TaigaError  # noqa: E402

# Tag spellings defined by docs/bug-triage.md; every other value is resolved live from Taiga.
SURFACE_TAGS = ("product", "devx", "pipeline", "ci")
SOURCE_TAGS = ("src:user", "src:monitor", "src:manual-test", "src:pr-review", "src:agent", "src:jon")
URGENT_PRIORITY_NAME = "Urgent"
NOT_FOUND_REASON = "not found among fetched issues"


class OptionError(ValueError):
    """Raised when a command-line option value is invalid."""


@dataclass(frozen=True)
class Purpose:
    bug_only: bool
    include_closed: bool
    require_tags: Tuple[str, ...]
    exclude_tags: Tuple[str, ...]
    exclude_statuses: Tuple[str, ...]
    split_urgent: bool
    requires_milestone: bool


PURPOSES = {
    # Open Bugs nobody has triaged yet: the inverse of analyze/fix's triaged gate.
    "triage": Purpose(True, False, (), ("triaged",), (), False, False),
    "analyze": Purpose(True, False, ("triaged",), ("llm-analyzed",), (), False, False),
    "fix": Purpose(True, False, ("triaged",), (), ("Ready for review", "Blocked"), True, False),
    "sprint": Purpose(False, True, (), (), (), False, True),
}


@dataclass(frozen=True)
class Status:
    id: int
    is_closed: bool


@dataclass(frozen=True)
class Level:
    id: int
    order: int


@dataclass
class Metadata:
    bug_type_id: int
    type_names: Dict[int, str]
    statuses: Dict[str, Status]
    priorities: Dict[str, Level]
    severities: Dict[str, Level]
    milestones: List[dict] = field(default_factory=list)


@dataclass
class SelectionOptions:
    purpose: str
    surface: List[str] = field(default_factory=list)
    exclude_surface: List[str] = field(default_factory=list)
    source: List[str] = field(default_factory=list)
    exclude_source: List[str] = field(default_factory=list)
    tag: List[str] = field(default_factory=list)
    exclude_tag: List[str] = field(default_factory=list)
    refs: List[int] = field(default_factory=list)
    exclude_refs: List[int] = field(default_factory=list)
    milestone: List[int] = field(default_factory=list)
    exclude_milestone: List[int] = field(default_factory=list)
    status: List[str] = field(default_factory=list)
    exclude_status: List[str] = field(default_factory=list)
    min_priority: Optional[int] = None
    max_priority: Optional[int] = None
    min_severity: Optional[int] = None
    max_severity: Optional[int] = None
    names: Dict[str, str] = field(default_factory=dict)

    @property
    def purpose_rules(self) -> Purpose:
        return PURPOSES[self.purpose]

    def effective_excluded_statuses(self) -> List[str]:
        defaults = [] if self.status else list(self.purpose_rules.exclude_statuses)
        return _dedupe(defaults + self.exclude_status)

    def to_json(self) -> dict:
        rules = self.purpose_rules
        return {
            "purpose": self.purpose,
            "surface": self.surface,
            "excludeSurface": self.exclude_surface,
            "source": self.source,
            "excludeSource": self.exclude_source,
            "tag": self.tag,
            "excludeTag": self.exclude_tag,
            "refs": self.refs,
            "excludeRefs": self.exclude_refs,
            "milestone": self.milestone,
            "excludeMilestone": self.exclude_milestone,
            "status": self.status,
            "excludeStatus": self.effective_excluded_statuses(),
            "minPriority": self.names.get("min_priority"),
            "maxPriority": self.names.get("max_priority"),
            "minSeverity": self.names.get("min_severity"),
            "maxSeverity": self.names.get("max_severity"),
            "requireTags": list(rules.require_tags),
            "excludeTagsDefault": list(rules.exclude_tags),
        }


@dataclass
class SelectionResult:
    selected: List[dict]
    urgent: List[dict]
    skipped_refs: List[dict]
    fetched: int


def _dedupe(values: Sequence) -> list:
    seen: list = []
    for value in values:
        if value not in seen:
            seen.append(value)
    return seen


def _split_list(raw: Optional[List[str]]) -> List[str]:
    values: List[str] = []
    for chunk in raw or []:
        values.extend(part.strip() for part in chunk.split(",") if part.strip())
    return _dedupe(values)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Select Taiga issues for a bug sweep (read-only).")
    parser.add_argument("--purpose", required=True, choices=sorted(PURPOSES))
    parser.add_argument("--project", type=int, default=39)
    for name in (
        "surface", "exclude-surface", "source", "exclude-source", "refs", "exclude-refs",
        "tag", "exclude-tag", "milestone", "exclude-milestone", "status", "exclude-status",
    ):
        parser.add_argument(f"--{name}", action="append")
    for name in ("min-priority", "max-priority", "min-severity", "max-severity"):
        parser.add_argument(f"--{name}")
    return parser.parse_args(list(argv))


def _resolve_name(value: str, valid: Sequence[str], flag: str) -> str:
    by_lower = {name.lower(): name for name in valid}
    if value.lower() not in by_lower:
        raise OptionError(f"Unknown value '{value}' for {flag}. Valid values: {', '.join(valid)}")
    return by_lower[value.lower()]


def _resolve_enum(values: List[str], valid: Sequence[str], flag: str) -> List[str]:
    return _dedupe([_resolve_name(value, valid, flag) for value in values])


def _resolve_refs(values: List[str], flag: str) -> List[int]:
    try:
        return [int(v) for v in values]
    except ValueError as exc:
        raise OptionError(f"{flag} takes integer refs, got: {', '.join(values)}") from exc


def _resolve_milestones(values: List[str], milestones: List[dict], flag: str) -> List[int]:
    ids: List[int] = []
    for value in values:
        match = next((m for m in milestones if str(m["id"]) == value or (m.get("slug") or "").lower() == value.lower()), None)
        if match is None:
            valid = ", ".join(f"{m['id']} ({m.get('slug')})" for m in milestones)
            raise OptionError(f"Unknown milestone '{value}' for {flag}. Valid milestones: {valid}")
        ids.append(match["id"])
    return _dedupe(ids)


def resolve_options(args: argparse.Namespace, metadata: Metadata) -> SelectionOptions:
    def lst(name: str) -> List[str]:
        raw = getattr(args, name)
        values = _split_list(raw)
        if raw is not None and not values:
            raise OptionError(f"--{name.replace('_', '-')} was given no values")
        return values

    status_names = list(metadata.statuses)
    options = SelectionOptions(
        purpose=args.purpose,
        surface=_resolve_enum(lst("surface"), SURFACE_TAGS, "--surface"),
        exclude_surface=_resolve_enum(lst("exclude_surface"), SURFACE_TAGS, "--exclude-surface"),
        source=_resolve_enum(lst("source"), SOURCE_TAGS, "--source"),
        exclude_source=_resolve_enum(lst("exclude_source"), SOURCE_TAGS, "--exclude-source"),
        tag=lst("tag"),
        exclude_tag=lst("exclude_tag"),
        refs=_resolve_refs(lst("refs"), "--refs"),
        exclude_refs=_resolve_refs(lst("exclude_refs"), "--exclude-refs"),
        milestone=_resolve_milestones(lst("milestone"), metadata.milestones, "--milestone"),
        exclude_milestone=_resolve_milestones(lst("exclude_milestone"), metadata.milestones, "--exclude-milestone"),
        status=[_resolve_name(v, status_names, "--status") for v in lst("status")],
        exclude_status=[_resolve_name(v, status_names, "--exclude-status") for v in lst("exclude_status")],
    )

    for dimension, levels in (("priority", metadata.priorities), ("severity", metadata.severities)):
        orders: Dict[str, Optional[int]] = {}
        for bound in ("min", "max"):
            key = f"{bound}_{dimension}"
            raw = getattr(args, key)
            if raw is None:
                orders[bound] = None
                continue
            name = _resolve_name(raw, list(levels), f"--{bound}-{dimension}")
            options.names[key] = name
            orders[bound] = levels[name].order
            setattr(options, key, levels[name].order)
        if orders["min"] is not None and orders["max"] is not None and orders["min"] > orders["max"]:
            raise OptionError(
                f"--min-{dimension} ({options.names[f'min_{dimension}']}) is above "
                f"--max-{dimension} ({options.names[f'max_{dimension}']})"
            )

    if options.purpose_rules.requires_milestone and not options.milestone:
        raise OptionError("--purpose sprint requires --milestone")

    return options


def _tag_names(issue: dict) -> List[str]:
    return [t[0] if isinstance(t, (list, tuple)) else t for t in issue.get("tags") or []]


def _status_name(issue: dict, metadata: Metadata) -> str:
    info = issue.get("status_extra_info") or {}
    if info.get("name"):
        return info["name"]
    return next((n for n, s in metadata.statuses.items() if s.id == issue.get("status")), "")


def _level_name(levels: Dict[str, Level], level_id: Optional[int]) -> Optional[str]:
    return next((n for n, l in levels.items() if l.id == level_id), None)


def _level_order(levels: Dict[str, Level], level_id: Optional[int]) -> Optional[int]:
    name = _level_name(levels, level_id)
    return levels[name].order if name else None


def _failing_reason(issue: dict, options: SelectionOptions, metadata: Metadata) -> Optional[str]:
    rules = options.purpose_rules
    tags = _tag_names(issue)
    status = _status_name(issue, metadata)
    is_closed = (issue.get("status_extra_info") or {}).get("is_closed", False)

    # 1. Purpose eligibility
    if rules.bug_only and issue.get("type") != metadata.bug_type_id:
        return "not a Bug"
    if is_closed and not rules.include_closed:
        return "closed"
    for required in rules.require_tags:
        if required not in tags:
            return "untriaged" if required == "triaged" else f"missing tag {required}"
    for excluded in rules.exclude_tags:
        if excluded in tags:
            return f"tagged {excluded}"
    if options.status and status not in options.status:
        return f"status {status} not in --status"
    if status in options.effective_excluded_statuses():
        return f"status {status}"

    # 2. Include filters (OR within a dimension, AND across dimensions)
    if options.surface and not set(options.surface) & set(tags):
        return "not matched by --surface"
    if options.source and not set(options.source) & set(tags):
        return "not matched by --source"
    if options.tag and not set(options.tag) & set(tags):
        return "not matched by --tag"
    if options.refs and issue["ref"] not in options.refs:
        return "not matched by --refs"
    if options.milestone and issue.get("milestone") not in options.milestone:
        return "not matched by --milestone"

    # 3. Exclude filters
    if set(options.exclude_surface) & set(tags):
        return "excluded by --exclude-surface"
    if set(options.exclude_source) & set(tags):
        return "excluded by --exclude-source"
    if set(options.exclude_tag) & set(tags):
        return "excluded by --exclude-tag"
    if issue["ref"] in options.exclude_refs:
        return "excluded by --exclude-refs"
    if issue.get("milestone") in options.exclude_milestone:
        return "excluded by --exclude-milestone"

    # 4. Priority and severity bounds
    for dimension, levels in (("priority", metadata.priorities), ("severity", metadata.severities)):
        order = _level_order(levels, issue.get(dimension))
        minimum = getattr(options, f"min_{dimension}")
        maximum = getattr(options, f"max_{dimension}")
        if minimum is not None and (order is None or order < minimum):
            return f"below --min-{dimension}"
        if maximum is not None and order is not None and order > maximum:
            return f"above --max-{dimension}"

    return None


def _row(issue: dict, metadata: Metadata) -> dict:
    tags = _tag_names(issue)
    return {
        "ref": issue["ref"],
        "id": issue["id"],
        "subject": issue.get("subject"),
        "type": metadata.type_names.get(issue.get("type")),
        "status": _status_name(issue, metadata),
        "priority": _level_name(metadata.priorities, issue.get("priority")),
        "severity": _level_name(metadata.severities, issue.get("severity")),
        "surface": next((t for t in tags if t in SURFACE_TAGS), None),
        "source": next((t for t in tags if t in SOURCE_TAGS), None),
        "milestone": issue.get("milestone"),
        "tags": tags,
    }


def select_issues(issues: List[dict], options: SelectionOptions, metadata: Metadata) -> SelectionResult:
    reasons: Dict[int, str] = {}
    kept: List[dict] = []
    for issue in issues:
        reason = _failing_reason(issue, options, metadata)
        if reason is None:
            kept.append(issue)
        else:
            reasons[issue["ref"]] = reason

    def sort_key(issue: dict) -> Tuple[int, int, int]:
        return (
            -(_level_order(metadata.priorities, issue.get("priority")) or 0),
            -(_level_order(metadata.severities, issue.get("severity")) or 0),
            issue["ref"],
        )

    rows = [_row(i, metadata) for i in sorted(kept, key=sort_key)]
    urgent: List[dict] = []
    # A project without an "Urgent" priority (setup not yet applied) has no urgent
    # class: everything stays in selected and main() prints a note.
    if options.purpose_rules.split_urgent and URGENT_PRIORITY_NAME in metadata.priorities:
        urgent = [r for r in rows if r["priority"] == URGENT_PRIORITY_NAME]
        rows = [r for r in rows if r["priority"] != URGENT_PRIORITY_NAME]

    kept_refs = {i["ref"] for i in kept}
    skipped = [
        {"ref": ref, "reason": reasons.get(ref, NOT_FOUND_REASON)}
        for ref in options.refs
        if ref not in kept_refs
    ]
    return SelectionResult(selected=rows, urgent=urgent, skipped_refs=skipped, fetched=len(issues))


def fetch_metadata(client, project: int, include_milestones: bool) -> Metadata:
    types = client.get("/api/v1/issue-types", project=project)
    bug = next((t for t in types if t["name"].lower() == "bug"), None)
    if bug is None:
        raise TaigaError("Project has no 'Bug' issue type")
    statuses = client.get("/api/v1/issue-statuses", project=project)
    priorities = client.get("/api/v1/priorities", project=project)
    severities = client.get("/api/v1/severities", project=project)
    milestones = client.list_all("/api/v1/milestones", project=project) if include_milestones else []
    return Metadata(
        bug_type_id=bug["id"],
        type_names={t["id"]: t["name"] for t in types},
        statuses={s["name"]: Status(s["id"], s["is_closed"]) for s in statuses},
        priorities={p["name"]: Level(p["id"], p["order"]) for p in priorities},
        severities={s["name"]: Level(s["id"], s["order"]) for s in severities},
        milestones=[{"id": m["id"], "slug": m.get("slug"), "name": m.get("name")} for m in milestones],
    )


def fetch_issues(client, project: int, options: SelectionOptions, metadata: Metadata) -> List[dict]:
    purpose = options.purpose_rules
    server_filters: Dict[str, object] = {}
    # Explicit refs are fetched unfiltered so a closed or non-Bug ref is reported with its
    # eligibility reason instead of vanishing server-side.
    if options.refs:
        return client.list_all("/api/v1/issues", project=project)
    if purpose.bug_only:
        server_filters["type"] = metadata.bug_type_id
    if not purpose.include_closed:
        server_filters["status__is_closed"] = "false"
    return client.list_all("/api/v1/issues", project=project, **server_filters)


def main(argv: Sequence[str], client_factory: Callable[[], object] = TaigaClient.from_env) -> int:
    args = parse_args(argv)
    try:
        client = client_factory()
        wants_milestones = bool(args.milestone or args.exclude_milestone)
        metadata = fetch_metadata(client, args.project, wants_milestones)
        options = resolve_options(args, metadata)
        issues = fetch_issues(client, args.project, options, metadata)
    except OptionError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except TaigaError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    result = select_issues(issues, options, metadata)
    if options.purpose_rules.split_urgent and URGENT_PRIORITY_NAME not in metadata.priorities:
        print(
            f"Note: project {args.project} has no '{URGENT_PRIORITY_NAME}' priority, so the urgent list is empty. "
            "Run: (eval \"$(dv export project/taiga-growthgarage-jonroberts)\""
            " && python3 scripts/taiga_project_setup.py --apply)",
            file=sys.stderr,
        )
    print(json.dumps({
        "purpose": options.purpose,
        "options": options.to_json(),
        "ids": {
            "bugTypeId": metadata.bug_type_id,
            "statuses": {n: s.id for n, s in metadata.statuses.items()},
            "priorities": {n: l.id for n, l in metadata.priorities.items()},
            "severities": {n: l.id for n, l in metadata.severities.items()},
        },
        "selected": result.selected,
        "urgent": result.urgent,
        "skippedRefs": result.skipped_refs,
        "counts": {
            "fetched": result.fetched,
            "selected": len(result.selected),
            "urgent": len(result.urgent),
            "skipped": len(result.skipped_refs),
        },
    }, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
