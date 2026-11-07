# ABOUTME: Finds Taiga stories that carry two parallel task sets covering the same work, left behind by re-analysis runs.
# ABOUTME: Dry-run by default; --apply deletes only the later-created, still-open member of an exact-subject duplicate group.
import argparse
import json
import os
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode


DEFAULT_OUT_DIR = Path("output/taiga-duplicate-tasks")
DEFAULT_PROJECT_ID = 39
# A legitimately incremental task list trips a gap check, which is why the
# reworded set is report-only. The threshold only tunes how much noise it makes.
DEFAULT_GAP_MINUTES = 10
PAGE_SIZE = 200
# Taiga answers a page past the end with 404 rather than an empty page.
HTTP_NOT_FOUND = 404
# Fewer than this many tasks on a side is incremental planning, not a parallel set.
MIN_CLUSTER_SIZE = 2

DECISION_DELETE = "delete"
DECISION_MANUAL_REVIEW = "manual-review"
DECISIONS = (DECISION_DELETE, DECISION_MANUAL_REVIEW)


class TaigaDuplicateTasksError(RuntimeError):
    """Raised when the duplicate-task scan cannot continue."""


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find (and optionally delete) duplicate Taiga tasks within a story.",
    )
    parser.add_argument(
        "--project",
        type=int,
        default=DEFAULT_PROJECT_ID,
        help="Taiga project id to scan (default: 39, cholo-bots).",
    )
    parser.add_argument(
        "--gap-minutes",
        type=float,
        default=DEFAULT_GAP_MINUTES,
        help=(
            "Creation gap that splits a story's tasks into two clusters worth reviewing "
            "as reworded duplicates (default: 10)."
        ),
    )
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_OUT_DIR),
        help="Directory for generated report artifacts (default: output/taiga-duplicate-tasks).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Delete the later-created, still-open member of each exact-subject duplicate group. "
            "Without this flag the run is a dry run. Reworded candidates are never deleted."
        ),
    )
    return parser.parse_args(argv)


# curl flags whose following argument carries a request body (the Taiga auth
# payload includes the password).
SENSITIVE_DATA_FLAGS = {"-d", "--data", "--data-binary", "--data-raw"}


def redact_cmd(cmd: List[str]) -> str:
    """Render a command for error messages with credentials masked.

    The curl invocations carry the Taiga password (in the auth request body) and
    Bearer tokens (in Authorization headers); these must never reach logs or
    terminal history via an exception message.
    """
    redacted: List[str] = []
    mask_next = False

    for arg in cmd:
        if mask_next:
            redacted.append("<redacted>")
            mask_next = False
        elif arg in SENSITIVE_DATA_FLAGS:
            redacted.append(arg)
            mask_next = True
        elif arg.lower().startswith("authorization:"):
            redacted.append("Authorization: <redacted>")
        else:
            redacted.append(arg)

    return " ".join(redacted)


def _quote_config_value(value: str) -> str:
    """Quote a value for a curl config file (backslash and double quote are escaped)."""
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def bearer_config(token: str) -> str:
    """A curl config (read from stdin with ``-K -``) carrying the bearer header.

    Keeps the token out of curl's argv, which any local user can read from
    /proc/<pid>/cmdline.
    """
    return f"header = {_quote_config_value(f'Authorization: Bearer {token}')}\n"


def run_request(cmd: List[str], stdin_text: Optional[str] = None) -> Tuple[int, str]:
    """Run a curl command that appends its HTTP status, returning (status, body).

    The status is carried out rather than turned into a curl failure because the
    paging loop has to tell a 404 (past the last page — expected) from any other
    error (not expected).
    """
    result = subprocess.run(cmd, input=stdin_text, check=False, capture_output=True, text=True)

    if result.returncode != 0:
        raise TaigaDuplicateTasksError(
            f"Command failed ({result.returncode}): {redact_cmd(cmd)}\n{result.stderr.strip()}",
        )

    body, _, status = result.stdout.rpartition("\n")

    try:
        return int(status.strip()), body
    except ValueError as exc:
        raise TaigaDuplicateTasksError(
            f"Command did not report an HTTP status: {redact_cmd(cmd)}",
        ) from exc


def run_json_request(cmd: List[str], stdin_text: Optional[str] = None) -> object:
    status, body = run_request(cmd, stdin_text)

    if status >= 400:
        raise TaigaDuplicateTasksError(f"Request failed (HTTP {status}): {redact_cmd(cmd)}")

    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise TaigaDuplicateTasksError(
            f"Command produced invalid JSON: {redact_cmd(cmd)}",
        ) from exc


def get_required_env(name: str) -> str:
    value = os.environ.get(name)

    if not value:
        raise TaigaDuplicateTasksError(f"Missing required environment variable: {name}")

    return value


# curl flags shared by every request: quiet, and the HTTP status on its own
# trailing line so run_request can read it.
CURL_STATUS_ARGS = ["-s", "-w", "\n%{http_code}"]


class TaigaClient:
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip("/")
        self.token = self._authenticate(username, password)

    def _authenticate(self, username: str, password: str) -> str:
        payload = {"type": "normal", "username": username, "password": password}
        # The body travels on stdin so the password never appears in curl's argv.
        response = run_json_request(
            [
                "curl",
                *CURL_STATUS_ARGS,
                "-H",
                "Content-Type: application/json",
                "-X",
                "POST",
                "--data-binary",
                "@-",
                f"{self.base_url}/api/v1/auth",
            ],
            stdin_text=json.dumps(payload),
        )
        token = (response or {}).get("auth_token")

        if not token:
            raise TaigaDuplicateTasksError("Taiga auth response did not include auth_token")

        return token

    def _get_page(self, path: str, **params: object) -> Tuple[int, str]:
        url = f"{self.base_url}{path}?{urlencode(params)}"
        return run_request(
            ["curl", *CURL_STATUS_ARGS, "-K", "-", url],
            stdin_text=bearer_config(self.token),
        )

    def list_all_tasks(self, project_id: int) -> List[dict]:
        tasks: List[dict] = []
        page = 1

        while True:
            status, body = self._get_page(
                "/api/v1/tasks",
                project=project_id,
                page=page,
                page_size=PAGE_SIZE,
            )

            # 404 ends the scan only past the first page. On page 1 it means a bad
            # project or endpoint, and treating it as "no tasks" would report a
            # clean duplicate-task result for a scan that never read anything.
            if status == HTTP_NOT_FOUND and page > 1:
                break

            if status >= 400:
                raise TaigaDuplicateTasksError(
                    f"Listing tasks for project {project_id} failed (HTTP {status}) on page {page}",
                )

            batch = json.loads(body)

            if not isinstance(batch, list):
                raise TaigaDuplicateTasksError("Expected a list response for /api/v1/tasks")

            if not batch:
                break

            # A server may cap page_size below what was asked for, so a short
            # page is not proof of the end; only 404 or an empty page is.
            tasks.extend(batch)
            page += 1

        return tasks

    def get_task(self, task_id: int) -> Optional[dict]:
        """Fetch one task; None when it no longer exists (someone else deleted it)."""
        cmd = ["curl", *CURL_STATUS_ARGS, "-K", "-", f"{self.base_url}/api/v1/tasks/{task_id}"]
        status, body = run_request(cmd, stdin_text=bearer_config(self.token))

        if status == HTTP_NOT_FOUND:
            return None

        if status >= 400:
            raise TaigaDuplicateTasksError(f"Fetching task {task_id} failed (HTTP {status})")

        try:
            task = json.loads(body)
        except json.JSONDecodeError as exc:
            raise TaigaDuplicateTasksError(f"Command produced invalid JSON: {redact_cmd(cmd)}") from exc

        if not isinstance(task, dict):
            raise TaigaDuplicateTasksError(f"Expected a dict response for task {task_id}")

        return task

    def delete_task(self, task_id: int) -> None:
        status, _ = run_request(
            ["curl", *CURL_STATUS_ARGS, "-K", "-", "-X", "DELETE", f"{self.base_url}/api/v1/tasks/{task_id}"],
            stdin_text=bearer_config(self.token),
        )

        if status >= 400:
            raise TaigaDuplicateTasksError(f"Deleting task {task_id} failed (HTTP {status})")


def normalise_subject(subject: Optional[str]) -> str:
    """Two subjects that differ only in surrounding whitespace or case name the same task."""
    return " ".join((subject or "").split()).casefold()


def parse_created(task: dict) -> datetime:
    """Taiga stamps `created_date` as ISO 8601 with a trailing `Z`."""
    raw = (task.get("created_date") or "").replace("Z", "+00:00")

    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        # A task with no usable stamp sorts first rather than sinking the run.
        return datetime.min.replace(tzinfo=timezone.utc)


def group_tasks_by_story(tasks: List[dict]) -> Dict[int, List[dict]]:
    """Bucket tasks by their parent story, each bucket ordered by creation."""
    by_story: Dict[int, List[dict]] = defaultdict(list)

    for task in tasks:
        story = task.get("user_story")

        if story is None:
            continue

        by_story[story].append(task)

    return {story: sorted(members, key=parse_created) for story, members in by_story.items()}


def story_ref(task: dict) -> Optional[int]:
    return (task.get("user_story_extra_info") or {}).get("ref")


def is_task_closed(task: dict) -> bool:
    """The status carries the closed state; the top-level flag is only a fallback.

    Taiga's top-level `is_closed` is derived from the status and can disagree
    with it, so `status_extra_info.is_closed` is authoritative when present.
    """
    status_info = task.get("status_extra_info")

    if isinstance(status_info, dict) and "is_closed" in status_info:
        return bool(status_info["is_closed"])

    return bool(task.get("is_closed"))


def is_same_task(snapshot: dict, current: Optional[dict]) -> bool:
    """True when a re-fetched task is still the duplicate the snapshot described."""
    return (
        current is not None
        and snapshot.get("version") == current.get("version")
        and normalise_subject(snapshot.get("subject")) == normalise_subject(current.get("subject"))
        and snapshot.get("user_story") == current.get("user_story")
        and is_task_closed(snapshot) == is_task_closed(current)
    )


def summarise_task(task: dict) -> dict:
    return {
        "id": task.get("id"),
        "ref": task.get("ref"),
        "subject": task.get("subject"),
        "status": (task.get("status_extra_info") or {}).get("name"),
        "is_closed": is_task_closed(task),
    }


def find_exact_subject_groups(tasks_by_story: Dict[int, List[dict]]) -> List[dict]:
    """Group same-normalised-subject tasks under one story. High confidence duplicates."""
    groups: List[dict] = []

    for story in sorted(tasks_by_story):
        by_subject: Dict[str, List[dict]] = defaultdict(list)

        for task in tasks_by_story[story]:
            by_subject[normalise_subject(task.get("subject"))].append(task)

        for subject in sorted(by_subject):
            members = by_subject[subject]

            if len(members) < 2:
                continue

            groups.append(
                {
                    "story": story,
                    "story_ref": story_ref(members[0]),
                    "subject": subject,
                    "tasks": sorted(members, key=parse_created),
                },
            )

    return groups


def find_reworded_candidates(tasks_by_story: Dict[int, List[dict]], gap_seconds: float) -> List[dict]:
    """Report stories whose tasks split into two creation clusters, for manual review only.

    A re-analysis that reworded every subject leaves no exact-subject evidence,
    but it does leave a second burst of creations. That is a hint, not a verdict —
    an incrementally built task list looks the same — so these are never acted on.
    """
    candidates: List[dict] = []

    for story in sorted(tasks_by_story):
        members = tasks_by_story[story]

        if len(members) < MIN_CLUSTER_SIZE * 2:
            continue

        gaps = [
            (parse_created(members[index]) - parse_created(members[index - 1])).total_seconds()
            for index in range(1, len(members))
        ]
        widest = max(gaps)
        split = gaps.index(widest) + 1

        if widest < gap_seconds:
            continue

        if split < MIN_CLUSTER_SIZE or len(members) - split < MIN_CLUSTER_SIZE:
            continue

        candidates.append(
            {
                "story": story,
                "story_ref": story_ref(members[0]),
                "gap_seconds": widest,
                "earlier": [summarise_task(task) for task in members[:split]],
                "later": [summarise_task(task) for task in members[split:]],
            },
        )

    return candidates


def plan_group_deletions(group: dict) -> Tuple[str, List[dict]]:
    """Decide what an --apply run may remove from one exact-subject group.

    The earliest member is always kept. A closed later member means someone did
    that work, and the record of it must survive — so the whole group falls back
    to manual review rather than losing the wrong half.
    """
    members = group["tasks"]

    if len(members) < 2:
        return DECISION_MANUAL_REVIEW, []

    later = members[1:]

    if any(is_task_closed(task) for task in later):
        return DECISION_MANUAL_REVIEW, []

    return DECISION_DELETE, later


def process_tasks(
    client,
    tasks: List[dict],
    gap_seconds: float,
    dry_run: bool,
) -> Tuple[List[dict], List[dict]]:
    """Decide, and under --apply carry out, the deletions for each duplicate group."""
    tasks_by_story = group_tasks_by_story(tasks)
    exact_records: List[dict] = []

    for group in find_exact_subject_groups(tasks_by_story):
        decision, doomed = plan_group_deletions(group)
        deleted: List[int] = []
        skipped_stale: List[int] = []
        failed: List[dict] = []

        if decision == DECISION_DELETE and not dry_run:
            # Deleting the later members only makes sense while the retained
            # earliest member still covers the work; if it has changed or gone,
            # the whole group is stale and nothing may be removed.
            retained = group["tasks"][0]

            if not is_same_task(retained, client.get_task(retained["id"])):
                skipped_stale.extend(task["ref"] for task in doomed)
                doomed_now: List[dict] = []
            else:
                doomed_now = doomed

            for task in doomed_now:
                # The snapshot was taken by the listing pass; anything that changed
                # since then is no longer the duplicate the report described.
                if not is_same_task(task, client.get_task(task["id"])):
                    skipped_stale.append(task["ref"])
                    continue

                # One refused delete must not abort the run: the report is the
                # only durable record of the deletions that already happened.
                try:
                    client.delete_task(task["id"])
                except TaigaDuplicateTasksError as exc:
                    failed.append({"ref": task["ref"], "error": str(exc)})
                    continue

                deleted.append(task["ref"])

        exact_records.append(
            {
                "story": group["story"],
                "story_ref": group["story_ref"],
                "subject": group["subject"],
                "decision": decision,
                "tasks": [summarise_task(task) for task in group["tasks"]],
                "would_delete": [task["ref"] for task in doomed],
                "deleted": deleted,
                "skipped_stale": skipped_stale,
                "failed": failed,
            },
        )

    return exact_records, find_reworded_candidates(tasks_by_story, gap_seconds)


def build_report(
    exact_records: List[dict],
    reworded: List[dict],
    dry_run: bool,
    project_id: int,
    gap_seconds: float,
) -> dict:
    summary = {
        "exact_subject_groups": len(exact_records),
        "reworded_candidates": len(reworded),
        "deleted": sum(len(record["deleted"]) for record in exact_records),
        "skipped_stale": sum(len(record["skipped_stale"]) for record in exact_records),
        "failed": sum(len(record["failed"]) for record in exact_records),
    }

    for decision in DECISIONS:
        summary[decision] = sum(1 for record in exact_records if record["decision"] == decision)

    return {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dry_run": dry_run,
        "project": project_id,
        "gap_seconds": gap_seconds,
        "summary": summary,
        "exact_subject_duplicates": exact_records,
        "reworded_candidates": reworded,
    }


def render_task_cell(task: dict) -> str:
    return f"#{task['ref']} {task['subject']} ({task['status']}{', closed' if task['is_closed'] else ''})"


def render_markdown(report: dict) -> str:
    lines = [
        "# Taiga Duplicate Tasks",
        "",
        f"Generated at: `{report['generated_at']}`",
        f"Project: `{report['project']}`",
        f"Mode: `{'dry-run' if report['dry_run'] else 'apply'}`",
        "",
        "## Summary",
        "",
    ]

    for key, value in report["summary"].items():
        lines.append(f"- {key}: `{value}`")

    lines.extend(
        [
            "",
            "## Exact-subject duplicates",
            "",
            "| story | subject | decision | tasks | would delete | deleted | skipped (stale) | failed |",
            "|---|---|---|---|---|---|---|---|",
        ],
    )

    for record in report["exact_subject_duplicates"]:
        tasks = "<br>".join(render_task_cell(task) for task in record["tasks"])
        would_delete = ", ".join(f"#{ref}" for ref in record["would_delete"]) or "—"
        deleted = ", ".join(f"#{ref}" for ref in record["deleted"]) or "—"
        skipped = ", ".join(f"#{ref}" for ref in record["skipped_stale"]) or "—"
        failed = "<br>".join(f"#{item['ref']}: {item['error']}" for item in record["failed"]) or "—"
        lines.append(
            f"| #{record['story_ref']} | {record['subject']} | {record['decision']} | "
            f"{tasks} | {would_delete} | {deleted} | {skipped} | {failed} |",
        )

    lines.extend(
        [
            "",
            "## Suspected reworded duplicates (review only — never deleted)",
            "",
            "| story | gap (h) | earlier cluster | later cluster |",
            "|---|---|---|---|",
        ],
    )

    for candidate in report["reworded_candidates"]:
        earlier = "<br>".join(render_task_cell(task) for task in candidate["earlier"])
        later = "<br>".join(render_task_cell(task) for task in candidate["later"])
        lines.append(
            f"| #{candidate['story_ref']} | {candidate['gap_seconds'] / 3600:.1f} | {earlier} | {later} |",
        )

    lines.append("")
    return "\n".join(lines)


def write_report(report: dict, out_dir: Path) -> Dict[str, Path]:
    """Name a dry-run report apart from an apply report so neither overwrites the other."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = "report.dry-run" if report["dry_run"] else "report"
    paths = {
        "report_json": out_dir / f"{stem}.json",
        "report_md": out_dir / f"{stem}.md",
    }
    paths["report_json"].write_text(json.dumps(report, indent=2) + "\n")
    paths["report_md"].write_text(render_markdown(report) + "\n")
    return paths


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    base_url = get_required_env("TAIGA_API_URL")
    username = get_required_env("TAIGA_USERNAME")
    password = get_required_env("TAIGA_PASSWORD")
    client = TaigaClient(base_url=base_url, username=username, password=password)
    gap_seconds = args.gap_minutes * 60
    tasks = client.list_all_tasks(args.project)
    exact_records, reworded = process_tasks(
        client,
        tasks,
        gap_seconds=gap_seconds,
        dry_run=not args.apply,
    )
    report = build_report(
        exact_records,
        reworded,
        dry_run=not args.apply,
        project_id=args.project,
        gap_seconds=gap_seconds,
    )
    paths = write_report(report, Path(args.out_dir))

    print(render_markdown(report))
    print(f"JSON report: {paths['report_json']}")
    print(f"Markdown report: {paths['report_md']}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except TaigaDuplicateTasksError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
