# ABOUTME: Read-only Taiga audit script for milestone orphans and missing epic links.
# ABOUTME: Audits Taiga project 39 (cholo-bots); writes JSON/Markdown reports plus a manifest template under output/taiga-audit.
import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "lib"))

from taiga_client import TaigaClient, TaigaError, get_required_env, redact_cmd, run_json_command  # noqa: E402,F401

TaigaAuditError = TaigaError

DEFAULT_OUT_DIR = Path("output/taiga-audit")
DEFAULT_REPORT_JSON = "report.json"
DEFAULT_REPORT_MD = "report.md"
DEFAULT_MANIFEST_JSON = "remediation-manifest.json"

PROJECT_ID = 39
PROJECT_SLUGS = {
    PROJECT_ID: "cholo-bots",
}

# The first of these that exists is read as the roadmap. Milestone sections use
# "### M<n>: ..." headings with a "Key Taiga stories:" list; a roadmap without
# them (or no roadmap at all) just yields no roadmap evidence.
DEFAULT_ROADMAP_PATHS = ("docs/roadmap.md", "Cholobots_48_Hour_Development_Plan.md")

ROADMAP_MILESTONE_RE = re.compile(r"^###\s+(M[1-9]):.*$", re.M)
PLAIN_REF_RE = re.compile(r"#(\d+)")
MILESTONE_TOKEN_RE = re.compile(r"\b(M[1-9])\b")


@dataclass(frozen=True)
class EpicExpectation:
    epic_project_id: int
    epic_ref: int
    epic_subject: str
    story_project_id: int
    story_ref: int
    source: str
    milestone_token: Optional[str]


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit Taiga milestone placement and epic linkage.",
    )
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_OUT_DIR),
        help="Directory for generated report artifacts (default: output/taiga-audit).",
    )
    parser.add_argument(
        "--roadmap",
        default=None,
        help="Roadmap markdown to read milestone assignments from "
        f"(default: first existing of {', '.join(DEFAULT_ROADMAP_PATHS)}).",
    )
    return parser.parse_args(argv)


def is_story_open(story: dict) -> bool:
    return not bool((story.get("status_extra_info") or {}).get("is_closed"))


def extract_heading_body(text: str, heading: str, level: int) -> str:
    heading_re = re.compile(rf"^{'#' * level}\s+{re.escape(heading)}\s*$", re.M)
    match = heading_re.search(text)

    if not match:
        return ""

    rest = text[match.end():]
    next_heading_re = re.compile(rf"^#{{1,{level}}}\s+", re.M)
    next_match = next_heading_re.search(rest)
    return rest[: next_match.start()] if next_match else rest


def parse_roadmap_assignments(roadmap_text: str) -> Tuple[Dict[int, str], List[int]]:
    milestone_map: Dict[int, str] = {}

    for match in ROADMAP_MILESTONE_RE.finditer(roadmap_text):
        milestone_token = match.group(1)
        start = match.end()
        next_match = ROADMAP_MILESTONE_RE.search(roadmap_text, start)
        section = roadmap_text[start : next_match.start() if next_match else len(roadmap_text)]
        key_story_block = extract_label_block(section, "Key Taiga stories:")

        for story_ref in extract_list_refs(key_story_block):
            milestone_map[story_ref] = milestone_token

    reviewed_section = extract_heading_body(
        roadmap_text,
        "Reviewed But Still Outside Milestones",
        level=2,
    )
    reviewed_refs = extract_list_refs(reviewed_section)
    return milestone_map, reviewed_refs


def extract_label_block(section_text: str, label: str) -> str:
    lines = section_text.splitlines()
    block_lines: List[str] = []
    collecting = False

    for line in lines:
        if not collecting:
            if line.strip() == label:
                collecting = True
            continue

        stripped = line.strip()

        if not stripped:
            block_lines.append(line)
            continue

        if line.startswith("- ") or line.startswith("  "):
            block_lines.append(line)
            continue

        break

    return "\n".join(block_lines)


def extract_list_refs(section_text: str) -> List[int]:
    refs: List[int] = []

    for line in section_text.splitlines():
        if line.lstrip().startswith("- "):
            refs.extend(int(ref) for ref in PLAIN_REF_RE.findall(line))

    return refs


def parse_epic_expectations(epic_project_id: int, epic: dict) -> List[EpicExpectation]:
    section = extract_heading_body(epic.get("description") or "", "Milestone Alignment", level=2)

    if not section:
        return []

    expectations: List[EpicExpectation] = []
    default_milestone: Optional[str] = None
    current_milestone: Optional[str] = None

    for line in section.splitlines():
        milestone_match = MILESTONE_TOKEN_RE.search(line)

        if line.strip().startswith("Primary milestone:") and milestone_match:
            default_milestone = milestone_match.group(1)
            current_milestone = default_milestone
        elif milestone_match:
            current_milestone = milestone_match.group(1)

        milestone_token = current_milestone or default_milestone

        for ref in PLAIN_REF_RE.findall(line):
            expectations.append(
                EpicExpectation(
                    epic_project_id=epic_project_id,
                    epic_ref=epic["ref"],
                    epic_subject=epic["subject"],
                    story_project_id=epic_project_id,
                    story_ref=int(ref),
                    source="same-project",
                    milestone_token=milestone_token,
                ),
            )

    deduped = {}

    for expectation in expectations:
        key = (
            expectation.epic_project_id,
            expectation.epic_ref,
            expectation.story_project_id,
            expectation.story_ref,
            expectation.source,
        )
        if key not in deduped:
            deduped[key] = expectation

    return list(deduped.values())


def story_record(story: dict) -> dict:
    return {
        "id": story.get("id"),
        "ref": story["ref"],
        "subject": story["subject"],
        "milestone": story.get("milestone_name"),
        "milestone_slug": story.get("milestone_slug"),
        "status": (story.get("status_extra_info") or {}).get("name"),
        "epics": [
            {
                "ref": epic.get("ref"),
                "subject": epic.get("subject"),
            }
            for epic in (story.get("epics") or [])
        ],
    }


def build_expected_assignment_records(
    open_no_milestone: Iterable[dict],
    roadmap_assignments: Dict[int, str],
    epic_expectations: List[EpicExpectation],
) -> List[dict]:
    epic_assignments: Dict[int, List[Tuple[str, str]]] = {}

    for expectation in epic_expectations:
        if expectation.story_project_id != PROJECT_ID or not expectation.milestone_token:
            continue

        epic_assignments.setdefault(expectation.story_ref, []).append(
            (
                expectation.milestone_token,
                f"epic #{expectation.epic_ref}",
            ),
        )

    records = []

    for story in open_no_milestone:
        evidence: List[dict] = []

        if story["ref"] in roadmap_assignments:
            evidence.append(
                {
                    "source": "roadmap",
                    "milestone_token": roadmap_assignments[story["ref"]],
                },
            )

        for milestone_token, source in epic_assignments.get(story["ref"], []):
            evidence.append(
                {
                    "source": source,
                    "milestone_token": milestone_token,
                },
            )

        if not evidence:
            continue

        target_tokens = sorted({item["milestone_token"] for item in evidence})
        target_milestone = target_tokens[0] if len(target_tokens) == 1 else None

        record = story_record(story)
        record["target_milestones"] = target_tokens
        record["target_milestone"] = target_milestone
        record["evidence"] = evidence
        record["needs_review"] = target_milestone is None
        records.append(record)

    return sorted(records, key=lambda item: item["ref"])


def build_candidate_missing_links(
    open_story_cache: Dict[Tuple[int, int], dict],
    epic_expectations: List[EpicExpectation],
) -> List[dict]:
    candidates: List[dict] = []

    for expectation in epic_expectations:
        story = open_story_cache.get((expectation.story_project_id, expectation.story_ref))

        if not story:
            continue

        actual_epic_refs = {epic.get("ref") for epic in (story.get("epics") or [])}

        if expectation.epic_ref in actual_epic_refs:
            continue

        candidates.append(
            {
                "epic_project_id": expectation.epic_project_id,
                "epic_project_slug": PROJECT_SLUGS.get(expectation.epic_project_id),
                "epic_ref": expectation.epic_ref,
                "epic_subject": expectation.epic_subject,
                "story_project_id": expectation.story_project_id,
                "story_project_slug": PROJECT_SLUGS.get(expectation.story_project_id),
                "story_ref": story["ref"],
                "story_subject": story["subject"],
                "story_status": (story.get("status_extra_info") or {}).get("name"),
                "story_milestone": story.get("milestone_name"),
                "actual_epics": story_record(story)["epics"],
                "source": expectation.source,
                "expected_milestone_token": expectation.milestone_token,
            },
        )

    return sorted(
        candidates,
        key=lambda item: (
            item["story_project_id"],
            item["story_ref"],
            item["epic_project_id"],
            item["epic_ref"],
        ),
    )


def build_report(client: TaigaClient, roadmap_text: str) -> dict:
    roadmap_assignments, reviewed_outside_refs = parse_roadmap_assignments(roadmap_text)
    open_story_cache: Dict[Tuple[int, int], dict] = {}
    all_epic_expectations: List[EpicExpectation] = []
    project_key = f"project{PROJECT_ID}"
    report = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "summary": {},
        project_key: {
            "no_milestone": {},
            "zero_epic": [],
            "multi_epic_context": [],
        },
        "candidate_missing_epic_links": [],
    }

    project_id = PROJECT_ID
    milestone_list = client.list_all("/api/v1/milestones", project=project_id)
    epic_list = client.list_all("/api/v1/epics", project=project_id)
    story_list = client.list_all("/api/v1/userstories", project=project_id)

    # The userstory list endpoint already includes every field the audit
    # reads (ref, subject, milestone_name, milestone_slug, status_extra_info,
    # epics), so no per-story detail request is needed.
    detailed_stories = story_list

    for story in detailed_stories:
        if is_story_open(story):
            open_story_cache[(project_id, story["ref"])] = story

    for epic_stub in epic_list:
        epic_detail = client.get(f"/api/v1/epics/{epic_stub['id']}")

        if not isinstance(epic_detail, dict):
            raise TaigaAuditError("Expected dict response for epic detail")

        all_epic_expectations.extend(parse_epic_expectations(project_id, epic_detail))

    open_stories = [story for story in detailed_stories if is_story_open(story)]
    open_no_milestone = [story for story in open_stories if not story.get("milestone_name")]
    zero_epic = [story_record(story) for story in open_stories if not (story.get("epics") or [])]
    multi_epic = [story_record(story) for story in open_stories if len(story.get("epics") or []) > 1]

    report["summary"][f"project_{project_id}"] = {
        "project_slug": PROJECT_SLUGS.get(project_id),
        "total_stories": len(detailed_stories),
        "open_stories": len(open_stories),
        "open_no_milestone": len(open_no_milestone),
        "open_zero_epic": len(zero_epic),
        "open_multi_epic": len(multi_epic),
        "milestones": [
            {
                "id": milestone["id"],
                "name": milestone["name"],
                "slug": milestone["slug"],
                "closed": milestone.get("closed"),
            }
            for milestone in milestone_list
        ],
    }

    expected_assignment = build_expected_assignment_records(
        open_no_milestone,
        roadmap_assignments,
        all_epic_expectations,
    )
    expected_refs = {story["ref"] for story in expected_assignment}
    intentionally_outside = [
        story_record(story)
        for story in open_no_milestone
        if story["ref"] in reviewed_outside_refs and story["ref"] not in expected_refs
    ]
    intentionally_outside_refs = {story["ref"] for story in intentionally_outside}
    manual_review = [
        story_record(story)
        for story in open_no_milestone
        if story["ref"] not in expected_refs and story["ref"] not in intentionally_outside_refs
    ]

    report[project_key]["no_milestone"] = {
        "expected_assignment": expected_assignment,
        "intentionally_outside": sorted(intentionally_outside, key=lambda item: item["ref"]),
        "manual_review": sorted(manual_review, key=lambda item: item["ref"]),
    }
    report[project_key]["zero_epic"] = sorted(zero_epic, key=lambda item: item["ref"])
    report[project_key]["multi_epic_context"] = sorted(multi_epic, key=lambda item: item["ref"])

    report["candidate_missing_epic_links"] = build_candidate_missing_links(
        open_story_cache,
        all_epic_expectations,
    )
    return report


def render_markdown(report: dict) -> str:
    project_key = f"project{PROJECT_ID}"
    summary = report["summary"][f"project_{PROJECT_ID}"]
    no_milestone = report[project_key]["no_milestone"]
    lines = [
        "# Taiga Audit",
        "",
        f"Generated at: `{report['generated_at']}`",
        "",
        "## Summary",
        "",
        f"### {summary['project_slug']}",
        "",
        f"- Open stories: `{summary['open_stories']}`",
        f"- Open stories with no milestone: `{summary['open_no_milestone']}`",
        f"- Open stories with zero epic links: `{summary['open_zero_epic']}`",
        f"- Open stories with multiple epic links: `{summary['open_multi_epic']}`",
        "",
        f"## Project {PROJECT_ID} No-Milestone",
        "",
        f"- Expected assignment: `{len(no_milestone['expected_assignment'])}`",
        f"- Intentionally outside milestones: `{len(no_milestone['intentionally_outside'])}`",
        f"- Manual review: `{len(no_milestone['manual_review'])}`",
        "",
        "## Zero-Epic Stories",
        "",
        f"- Project {PROJECT_ID}: {format_story_refs(report[project_key]['zero_epic'])}",
        "",
        "## Candidate Missing Epic Links",
        "",
    ]

    if report["candidate_missing_epic_links"]:
        for candidate in report["candidate_missing_epic_links"]:
            lines.append(
                f"- `{candidate['story_project_slug']} #{candidate['story_ref']}` "
                f"should include `{candidate['epic_project_slug']} epic #{candidate['epic_ref']}`",
            )
    else:
        lines.append("- None")

    lines.append("")
    return "\n".join(lines)


def format_story_refs(stories: List[dict]) -> str:
    if not stories:
        return "`none`"

    return ", ".join(f"`#{story['ref']}`" for story in stories)


def ensure_output_dir(out_dir: Path) -> Dict[str, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    return {
        "report_json": out_dir / DEFAULT_REPORT_JSON,
        "report_md": out_dir / DEFAULT_REPORT_MD,
        "manifest_json": out_dir / DEFAULT_MANIFEST_JSON,
    }


def write_manifest_template(path: Path) -> None:
    if path.exists():
        return

    path.write_text(json.dumps({"actions": []}, indent=2) + "\n")


def read_roadmap(explicit: Optional[str]) -> str:
    """Return the roadmap text: an explicit path must exist; the defaults may all be absent."""
    if explicit:
        return Path(explicit).read_text()

    for candidate in DEFAULT_ROADMAP_PATHS:
        if Path(candidate).is_file():
            return Path(candidate).read_text()

    print(
        f"Note: no roadmap found ({', '.join(DEFAULT_ROADMAP_PATHS)}); no roadmap milestone evidence.",
        file=sys.stderr,
    )
    return ""


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    out_dir = Path(args.out_dir)
    paths = ensure_output_dir(out_dir)
    base_url = get_required_env("TAIGA_API_URL")
    username = get_required_env("TAIGA_USERNAME")
    password = get_required_env("TAIGA_PASSWORD")
    roadmap_text = read_roadmap(args.roadmap)
    client = TaigaClient(base_url=base_url, username=username, password=password)
    report = build_report(client, roadmap_text)
    markdown = render_markdown(report)

    paths["report_json"].write_text(json.dumps(report, indent=2) + "\n")
    paths["report_md"].write_text(markdown + "\n")
    write_manifest_template(paths["manifest_json"])

    print(markdown)
    print(f"JSON report: {paths['report_json']}")
    print(f"Markdown report: {paths['report_md']}")
    print(f"Manifest template: {paths['manifest_json']}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except TaigaAuditError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
