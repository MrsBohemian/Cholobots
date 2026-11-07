# ABOUTME: Pytest tests for the taiga_find_duplicate_tasks.py script.
# ABOUTME: Verifies subject normalisation, duplicate grouping, reworded-candidate detection, and the dry-run/apply split.
import os
import sys

import pytest

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts"))

import taiga_find_duplicate_tasks
from taiga_find_duplicate_tasks import (
    DECISION_DELETE,
    DECISION_MANUAL_REVIEW,
    TaigaDuplicateTasksError,
    TaigaClient,
    build_report,
    find_exact_subject_groups,
    find_reworded_candidates,
    group_tasks_by_story,
    normalise_subject,
    plan_group_deletions,
    process_tasks,
    redact_cmd,
    render_markdown,
    write_report,
)


def task(task_id, ref, subject, story=1587, created="2026-08-19T05:00:00.000Z", is_closed=False):
    return {
        "id": task_id,
        "ref": ref,
        "subject": subject,
        "user_story": story,
        "user_story_extra_info": {"ref": story},
        "created_date": created,
        "is_closed": is_closed,
        "status_extra_info": {"name": "Closed" if is_closed else "New", "is_closed": is_closed},
    }


class TestNormaliseSubject:
    def test_subject_normalization(self):
        """Subjects differing only by surrounding whitespace or case are the same subject."""
        assert normalise_subject("  Document Runner Topology  ") == normalise_subject("document runner topology")

    def test_distinct_subjects_normalise_differently(self):
        assert normalise_subject("Add tests") != normalise_subject("Add docs")


class TestFindExactSubjectGroups:
    def test_exact_subject_duplicates_detected(self):
        tasks = [
            task(1, 1588, "Replace inline Step 6b", created="2026-07-01T00:00:00.000Z"),
            task(2, 1716, "Replace inline Step 6b", created="2026-07-02T00:00:00.000Z"),
        ]

        groups = find_exact_subject_groups(group_tasks_by_story(tasks))

        assert len(groups) == 1
        assert [member["ref"] for member in groups[0]["tasks"]] == [1588, 1716]
        assert groups[0]["story_ref"] == 1587

    def test_subject_normalization_groups_whitespace_and_case_variants(self):
        tasks = [
            task(1, 1732, "Document runner topology", created="2026-07-01T00:00:00.000Z"),
            task(2, 1733, "  DOCUMENT RUNNER TOPOLOGY ", created="2026-07-02T00:00:00.000Z"),
        ]

        groups = find_exact_subject_groups(group_tasks_by_story(tasks))

        assert len(groups) == 1
        assert [member["ref"] for member in groups[0]["tasks"]] == [1732, 1733]

    def test_distinct_subjects_not_flagged(self):
        tasks = [task(i, 1600 + i, f"Task number {i}", created="2026-07-01T00:00:00.000Z") for i in range(6)]

        assert find_exact_subject_groups(group_tasks_by_story(tasks)) == []

    def test_duplicates_across_different_stories_not_grouped(self):
        tasks = [
            task(1, 100, "Update documentation", story=1587),
            task(2, 200, "Update documentation", story=1565),
        ]

        assert find_exact_subject_groups(group_tasks_by_story(tasks)) == []

    def test_group_members_are_ordered_by_creation(self):
        tasks = [
            task(2, 1716, "Same work", created="2026-07-09T00:00:00.000Z"),
            task(1, 1588, "Same work", created="2026-07-01T00:00:00.000Z"),
        ]

        groups = find_exact_subject_groups(group_tasks_by_story(tasks))

        assert [member["ref"] for member in groups[0]["tasks"]] == [1588, 1716]


class TestFindRewordedCandidates:
    def test_reworded_candidate_detected_by_time_gap(self):
        tasks = [
            task(1, 766, "Add shared types", created="2026-08-01T10:00:00.000Z"),
            task(2, 767, "Add server endpoint", created="2026-08-01T10:01:00.000Z"),
            task(3, 788, "Introduce the shared request types", created="2026-08-19T09:00:00.000Z"),
            task(4, 789, "Wire up the API handler", created="2026-08-19T09:01:00.000Z"),
        ]

        candidates = find_reworded_candidates(group_tasks_by_story(tasks), gap_seconds=600)

        assert len(candidates) == 1
        assert [member["ref"] for member in candidates[0]["earlier"]] == [766, 767]
        assert [member["ref"] for member in candidates[0]["later"]] == [788, 789]

    def test_reworded_candidate_below_threshold_not_flagged(self):
        tasks = [
            task(1, 766, "Add shared types", created="2026-08-01T10:00:00.000Z"),
            task(2, 767, "Add server endpoint", created="2026-08-01T10:01:00.000Z"),
            task(3, 768, "Add client state", created="2026-08-01T10:02:00.000Z"),
            task(4, 769, "Add UI component", created="2026-08-01T10:03:00.000Z"),
        ]

        assert find_reworded_candidates(group_tasks_by_story(tasks), gap_seconds=600) == []

    def test_a_lone_later_task_is_not_a_cluster(self):
        """One task added later is incremental planning, not a parallel task set."""
        tasks = [
            task(1, 766, "Add shared types", created="2026-08-01T10:00:00.000Z"),
            task(2, 767, "Add server endpoint", created="2026-08-01T10:01:00.000Z"),
            task(3, 788, "Add a follow-up doc update", created="2026-08-19T09:00:00.000Z"),
        ]

        assert find_reworded_candidates(group_tasks_by_story(tasks), gap_seconds=600) == []


class TestPlanGroupDeletions:
    def test_only_the_later_open_members_are_doomed(self):
        group = {
            "tasks": [
                task(1, 1588, "Same work", created="2026-07-01T00:00:00.000Z"),
                task(2, 1716, "Same work", created="2026-07-09T00:00:00.000Z"),
            ],
        }

        decision, doomed = plan_group_deletions(group)

        assert decision == DECISION_DELETE
        assert [member["ref"] for member in doomed] == [1716]

    def test_a_closed_later_member_makes_the_group_manual_review(self):
        group = {
            "tasks": [
                task(1, 1588, "Same work", created="2026-07-01T00:00:00.000Z"),
                task(2, 1716, "Same work", created="2026-07-09T00:00:00.000Z", is_closed=True),
            ],
        }

        decision, doomed = plan_group_deletions(group)

        assert decision == DECISION_MANUAL_REVIEW
        assert doomed == []

    def test_status_level_closed_flag_is_authoritative(self):
        """Taiga's top-level is_closed can lag the status; a closed status must still protect the task."""
        closed_by_status = task(2, 1716, "Same work", created="2026-07-09T00:00:00.000Z")
        closed_by_status["status_extra_info"] = {"name": "Closed", "is_closed": True}
        group = {
            "tasks": [
                task(1, 1588, "Same work", created="2026-07-01T00:00:00.000Z"),
                closed_by_status,
            ],
        }

        decision, doomed = plan_group_deletions(group)

        assert decision == DECISION_MANUAL_REVIEW
        assert doomed == []

    def test_top_level_closed_flag_is_the_fallback_without_status_info(self):
        later = task(2, 1716, "Same work", created="2026-07-09T00:00:00.000Z", is_closed=True)
        del later["status_extra_info"]
        group = {
            "tasks": [
                task(1, 1588, "Same work", created="2026-07-01T00:00:00.000Z"),
                later,
            ],
        }

        decision, _ = plan_group_deletions(group)

        assert decision == DECISION_MANUAL_REVIEW


class FakeClient:
    """Duck-typed stand-in for TaigaClient covering only what process_tasks uses.

    `current` maps task id to what a re-fetch returns; a task not listed there
    is returned unchanged from the snapshot handed to process_tasks.
    """

    def __init__(self, current=None):
        self.deleted = []
        self.current = current or {}
        self.snapshot = {}

    def get_task(self, task_id):
        return self.current.get(task_id, self.snapshot[task_id])

    def delete_task(self, task_id):
        self.deleted.append(task_id)


def process_with_snapshot(client, tasks, **kwargs):
    client.snapshot = {item["id"]: item for item in tasks}
    return process_tasks(client, tasks, **kwargs)


class TestProcessTasks:
    def test_dry_run_is_default(self):
        tasks = [
            task(1, 1588, "Same work", created="2026-07-01T00:00:00.000Z"),
            task(2, 1716, "Same work", created="2026-07-09T00:00:00.000Z"),
        ]
        client = FakeClient()

        exact, _ = process_tasks(client, tasks, gap_seconds=600, dry_run=True)

        assert client.deleted == []
        assert exact[0]["decision"] == DECISION_DELETE
        assert exact[0]["would_delete"] == [1716]
        assert exact[0]["deleted"] == []

    def test_apply_deletes_only_later_open_duplicate(self):
        tasks = [
            task(1, 1588, "Same work", created="2026-07-01T00:00:00.000Z"),
            task(2, 1716, "Same work", created="2026-07-09T00:00:00.000Z"),
        ]
        client = FakeClient()

        exact, _ = process_with_snapshot(client, tasks, gap_seconds=600, dry_run=False)

        assert client.deleted == [2]
        assert exact[0]["deleted"] == [1716]
        assert exact[0]["skipped_stale"] == []

    def test_apply_revalidates_each_task_before_deleting_it(self):
        """The snapshot can go stale between listing and deleting; a task that changed is left alone."""
        snapshot = [
            task(1, 1588, "Same work", created="2026-07-01T00:00:00.000Z"),
            task(2, 1716, "Same work", created="2026-07-09T00:00:00.000Z"),
            task(3, 1717, "Same work", created="2026-07-10T00:00:00.000Z"),
        ]
        reworded = task(2, 1716, "Different work now", created="2026-07-09T00:00:00.000Z")
        reworded["version"] = 2
        client = FakeClient(current={2: reworded})

        exact, _ = process_with_snapshot(client, snapshot, gap_seconds=600, dry_run=False)

        assert client.deleted == [3]
        assert exact[0]["deleted"] == [1717]
        assert exact[0]["skipped_stale"] == [1716]

    @pytest.mark.parametrize(
        "change",
        [
            {"version": 5},
            {"user_story": 9999},
            {"is_closed": True, "status_extra_info": {"name": "Closed", "is_closed": True}},
        ],
    )
    def test_a_version_story_or_closed_state_change_skips_the_deletion(self, change):
        snapshot = [
            task(1, 1588, "Same work", created="2026-07-01T00:00:00.000Z"),
            task(2, 1716, "Same work", created="2026-07-09T00:00:00.000Z"),
        ]
        client = FakeClient(current={2: {**snapshot[1], **change}})

        exact, _ = process_with_snapshot(client, snapshot, gap_seconds=600, dry_run=False)

        assert client.deleted == []
        assert exact[0]["skipped_stale"] == [1716]

    @pytest.mark.parametrize(
        "retained_now",
        [
            None,
            {"subject": "Renamed since the scan", "version": 2},
        ],
        ids=["deleted", "renamed"],
    )
    def test_a_changed_or_missing_retained_task_aborts_the_whole_group(self, retained_now):
        """Deleting the later members only makes sense while the earliest one still covers the work."""
        snapshot = [
            task(1, 1588, "Same work", created="2026-07-01T00:00:00.000Z"),
            task(2, 1716, "Same work", created="2026-07-09T00:00:00.000Z"),
            task(3, 1717, "Same work", created="2026-07-10T00:00:00.000Z"),
        ]
        current = {1: None if retained_now is None else {**snapshot[0], **retained_now}}
        client = FakeClient(current=current)

        exact, _ = process_with_snapshot(client, snapshot, gap_seconds=600, dry_run=False)

        assert client.deleted == []
        assert exact[0]["deleted"] == []
        assert exact[0]["skipped_stale"] == [1716, 1717]

    def test_a_task_deleted_since_the_scan_is_skipped_as_stale(self):
        snapshot = [
            task(1, 1588, "Same work", created="2026-07-01T00:00:00.000Z"),
            task(2, 1716, "Same work", created="2026-07-09T00:00:00.000Z"),
            task(3, 1717, "Same work", created="2026-07-10T00:00:00.000Z"),
        ]
        client = FakeClient(current={2: None})

        exact, _ = process_with_snapshot(client, snapshot, gap_seconds=600, dry_run=False)

        assert client.deleted == [3]
        assert exact[0]["skipped_stale"] == [1716]
        assert exact[0]["deleted"] == [1717]

    def test_a_failed_deletion_is_recorded_and_the_run_continues(self):
        """One refused delete must not lose the record of the deletions that did happen."""
        snapshot = [
            task(1, 1588, "Same work", created="2026-07-01T00:00:00.000Z"),
            task(2, 1716, "Same work", created="2026-07-09T00:00:00.000Z"),
            task(3, 1717, "Same work", created="2026-07-10T00:00:00.000Z"),
        ]
        client = FakeClient()
        attempted = []

        def delete_task(task_id):
            attempted.append(task_id)
            if task_id == 2:
                raise TaigaDuplicateTasksError("Deleting task 2 failed (HTTP 403)")

        client.delete_task = delete_task

        exact, _ = process_with_snapshot(client, snapshot, gap_seconds=600, dry_run=False)

        assert attempted == [2, 3]
        assert exact[0]["deleted"] == [1717]
        assert exact[0]["failed"] == [{"ref": 1716, "error": "Deleting task 2 failed (HTTP 403)"}]

    def test_dry_run_never_refetches(self):
        tasks = [
            task(1, 1588, "Same work", created="2026-07-01T00:00:00.000Z"),
            task(2, 1716, "Same work", created="2026-07-09T00:00:00.000Z"),
        ]
        client = FakeClient()
        client.get_task = lambda task_id: pytest.fail("dry run must not re-fetch")

        process_tasks(client, tasks, gap_seconds=600, dry_run=True)

    def test_apply_skips_closed_duplicate(self):
        tasks = [
            task(1, 1588, "Same work", created="2026-07-01T00:00:00.000Z"),
            task(2, 1716, "Same work", created="2026-07-09T00:00:00.000Z", is_closed=True),
        ]
        client = FakeClient()

        exact, _ = process_tasks(client, tasks, gap_seconds=600, dry_run=False)

        assert client.deleted == []
        assert exact[0]["decision"] == DECISION_MANUAL_REVIEW

    def test_reworded_candidates_never_deleted_under_apply(self):
        tasks = [
            task(1, 766, "Add shared types", created="2026-08-01T10:00:00.000Z"),
            task(2, 767, "Add server endpoint", created="2026-08-01T10:01:00.000Z"),
            task(3, 788, "Introduce the shared request types", created="2026-08-19T09:00:00.000Z"),
            task(4, 789, "Wire up the API handler", created="2026-08-19T09:01:00.000Z"),
        ]
        client = FakeClient()

        exact, reworded = process_tasks(client, tasks, gap_seconds=600, dry_run=False)

        assert client.deleted == []
        assert exact == []
        assert len(reworded) == 1


class FakeCompletedProcess:
    def __init__(self, returncode, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class TestListAllTasks:
    def test_pagination_terminates_on_404(self, monkeypatch):
        """Taiga answers 404 past the last page; the paging loop must stop, not raise."""
        page_one = [task(i, 1000 + i, f"Task {i}", created="2026-07-01T00:00:00.000Z") for i in range(2)]
        responses = [
            (200, '{"auth_token": "token"}'),
            (200, taiga_find_duplicate_tasks.json.dumps(page_one)),
            (404, '{"_error_message": "Not found."}'),
        ]
        monkeypatch.setattr(
            taiga_find_duplicate_tasks,
            "run_request",
            lambda cmd, stdin_text=None: responses.pop(0),
        )
        monkeypatch.setattr(taiga_find_duplicate_tasks, "PAGE_SIZE", 2)
        client = TaigaClient(base_url="https://taiga.example", username="u", password="p")

        assert len(client.list_all_tasks(3)) == 2
        assert responses == []

    def test_a_short_page_does_not_end_the_scan(self, monkeypatch):
        """A server may cap page_size below what we asked for; only 404 or an empty page ends the scan."""
        page_one = [task(1, 1001, "Task 1", created="2026-07-01T00:00:00.000Z")]
        page_two = [task(2, 1002, "Task 2", created="2026-07-01T00:00:00.000Z")]
        responses = [
            (200, '{"auth_token": "token"}'),
            (200, taiga_find_duplicate_tasks.json.dumps(page_one)),
            (200, taiga_find_duplicate_tasks.json.dumps(page_two)),
            (404, '{"_error_message": "Not found."}'),
        ]
        monkeypatch.setattr(
            taiga_find_duplicate_tasks,
            "run_request",
            lambda cmd, stdin_text=None: responses.pop(0),
        )
        client = TaigaClient(base_url="https://taiga.example", username="u", password="p")

        assert [item["id"] for item in client.list_all_tasks(3)] == [1, 2]
        assert responses == []

    def test_a_non_404_http_error_still_raises(self, monkeypatch):
        responses = [
            (200, '{"auth_token": "token"}'),
            (500, "boom"),
        ]
        monkeypatch.setattr(
            taiga_find_duplicate_tasks,
            "run_request",
            lambda cmd, stdin_text=None: responses.pop(0),
        )
        client = TaigaClient(base_url="https://taiga.example", username="u", password="p")

        with pytest.raises(TaigaDuplicateTasksError):
            client.list_all_tasks(3)


class TestRedactCmd:
    def test_credentials_are_masked(self):
        cmd = ["curl", "-H", "Authorization: Bearer abc.def", "-d", '{"password": "hunter2"}', "https://taiga.example"]

        rendered = redact_cmd(cmd)

        assert "abc.def" not in rendered
        assert "hunter2" not in rendered


class TestReport:
    def test_report_and_markdown_carry_both_sections(self, tmp_path):
        exact = [
            {
                "story": 1587,
                "story_ref": 1587,
                "subject": "same work",
                "decision": DECISION_DELETE,
                "tasks": [
                    {"ref": 1588, "subject": "Same work", "status": "New", "is_closed": False},
                    {"ref": 1716, "subject": "Same work", "status": "New", "is_closed": False},
                ],
                "would_delete": [1716],
                "deleted": [],
                "skipped_stale": [],
                "failed": [],
            },
        ]
        reworded = [
            {
                "story": 765,
                "story_ref": 765,
                "gap_seconds": 1555200.0,
                "earlier": [{"ref": 766, "subject": "Add shared types", "status": "New", "is_closed": False}],
                "later": [{"ref": 788, "subject": "Introduce the shared request types", "status": "New", "is_closed": False}],
            },
        ]

        report = build_report(exact, reworded, dry_run=True, project_id=39, gap_seconds=600)

        assert report["dry_run"] is True
        assert report["summary"]["exact_subject_groups"] == 1
        assert report["summary"]["reworded_candidates"] == 1
        assert report["summary"]["deleted"] == 0

        markdown = render_markdown(report)

        assert "#1588" in markdown
        assert "#788" in markdown

        paths = write_report(report, tmp_path)

        assert paths["report_json"].exists()
        assert paths["report_md"].exists()
        assert "dry-run" in paths["report_json"].name


class TestFirstPage404IsAnError:
    def test_404_on_page_one_raises_instead_of_reporting_no_tasks(self, monkeypatch):
        responses = [
            (200, '{"auth_token": "token"}'),
            (404, '{"_error_message": "Not found."}'),
        ]
        monkeypatch.setattr(
            taiga_find_duplicate_tasks,
            "run_request",
            lambda cmd, stdin_text=None: responses.pop(0),
        )
        client = TaigaClient(base_url="https://taiga.example", username="u", password="p")

        with pytest.raises(TaigaDuplicateTasksError, match="404"):
            client.list_all_tasks(99999)


class TestSecretsStayOutOfArgv:
    def test_password_and_token_travel_on_stdin(self, monkeypatch):
        calls = []
        replies = [
            FakeCompletedProcess(0, '{"auth_token": "tok.en"}\n200'),
            FakeCompletedProcess(0, '[]\n200'),
        ]

        def fake_run(cmd, **kwargs):
            calls.append((cmd, kwargs.get("input")))
            return replies.pop(0)

        monkeypatch.setattr(taiga_find_duplicate_tasks.subprocess, "run", fake_run)
        client = TaigaClient(base_url="https://taiga.example", username="u", password="s3cret")
        client.list_all_tasks(3)

        for cmd, _ in calls:
            joined = " ".join(cmd)
            assert "s3cret" not in joined and "tok.en" not in joined
        assert "s3cret" in calls[0][1]
        assert "tok.en" in calls[1][1]
