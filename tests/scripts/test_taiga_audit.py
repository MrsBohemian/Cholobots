# ABOUTME: Pytest tests for the taiga_audit.py script.
# ABOUTME: Verifies roadmap parsing, epic expectation parsing, missing-link detection, and output defaults for project 39.
import json
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts"))

from taiga_audit import (
    DEFAULT_MANIFEST_JSON,
    DEFAULT_OUT_DIR,
    DEFAULT_REPORT_JSON,
    DEFAULT_REPORT_MD,
    EpicExpectation,
    build_candidate_missing_links,
    build_expected_assignment_records,
    build_report,
    ensure_output_dir,
    is_story_open,
    parse_epic_expectations,
    parse_roadmap_assignments,
    read_roadmap,
    render_markdown,
    write_manifest_template,
)


class TestIsStoryOpen:
    def test_uses_status_extra_info_is_closed(self):
        story = {
            "is_closed": False,
            "status_extra_info": {"is_closed": True},
        }
        assert is_story_open(story) is False


class TestParseRoadmapAssignments:
    def test_extracts_key_taiga_story_assignments(self):
        roadmap = """\
### M1: Functional MVP Complete

Key Taiga stories:
- #26 Progress Entry
- #27 Challenge Pages

Exit criteria:
- users can do things

### M2: Launch Readiness

Key Taiga stories:
- #32 Monitoring
"""
        assignments, reviewed = parse_roadmap_assignments(roadmap)
        assert assignments == {26: "M1", 27: "M1", 32: "M2"}
        assert reviewed == []

    def test_extracts_reviewed_outside_refs(self):
        roadmap = """\
## Reviewed But Still Outside Milestones

- #196 Story one
  Context line
- #211 Story two

## Maintenance
"""
        assignments, reviewed = parse_roadmap_assignments(roadmap)
        assert assignments == {}
        assert reviewed == [196, 211]


class TestParseEpicExpectations:
    def test_limits_membership_to_milestone_alignment_section(self):
        epic = {
            "ref": 9,
            "subject": "Phase 8 -- Notifications",
            "description": """\
## Milestone Alignment
M1 implementation stories: **#1105**, **#1106**
M2 story: **#1108**

## Dependency Diagram
```
#1105
  -> #9999
```
""",
        }
        expectations = parse_epic_expectations(39, epic)
        refs = sorted((item.story_project_id, item.story_ref, item.milestone_token) for item in expectations)
        assert refs == [
            (39, 1105, "M1"),
            (39, 1106, "M1"),
            (39, 1108, "M2"),
        ]

    def test_uses_primary_milestone_as_default(self):
        epic = {
            "ref": 6,
            "subject": "Phase 5 -- Daily Logging",
            "description": """\
## Milestone Alignment
Primary milestone: **M1 - Functional MVP Complete**
Primary story cluster:
- **#1457**
- **#26**
""",
        }
        expectations = parse_epic_expectations(39, epic)
        refs = sorted((item.story_ref, item.milestone_token) for item in expectations)
        assert refs == [(26, "M1"), (1457, "M1")]


class TestBuildExpectedAssignmentRecords:
    def test_combines_roadmap_and_epic_evidence(self):
        stories = [
            {
                "id": 1,
                "ref": 1105,
                "subject": "Challenge start reminder email",
                "status_extra_info": {"name": "New"},
                "milestone_name": None,
                "milestone_slug": None,
                "epics": [{"ref": 9, "subject": "Phase 8 -- Notifications"}],
            },
        ]
        records = build_expected_assignment_records(
            stories,
            {1105: "M1"},
            [
                EpicExpectation(
                    epic_project_id=39,
                    epic_ref=9,
                    epic_subject="Phase 8 -- Notifications",
                    story_project_id=39,
                    story_ref=1105,
                    source="same-project",
                    milestone_token="M1",
                ),
            ],
        )
        assert len(records) == 1
        assert records[0]["target_milestone"] == "M1"
        assert [item["source"] for item in records[0]["evidence"]] == ["roadmap", "epic #9"]

    def test_marks_conflicting_targets_for_review(self):
        stories = [
            {
                "id": 1,
                "ref": 1111,
                "subject": "Minimal beta moderation and support tooling",
                "status_extra_info": {"name": "New"},
                "milestone_name": None,
                "milestone_slug": None,
                "epics": [{"ref": 10, "subject": "Phase 9 -- Admin Panel"}],
            },
        ]
        records = build_expected_assignment_records(
            stories,
            {1111: "M2"},
            [
                EpicExpectation(
                    epic_project_id=39,
                    epic_ref=12,
                    epic_subject="Phase 11 -- Beta Launch",
                    story_project_id=39,
                    story_ref=1111,
                    source="same-project",
                    milestone_token="M3",
                ),
            ],
        )
        assert records[0]["target_milestone"] is None
        assert records[0]["needs_review"] is True


class TestBuildCandidateMissingLinks:
    def test_detects_expected_link_that_is_missing(self):
        open_story_cache = {
            (39, 24): {
                "ref": 24,
                "subject": "Scope chisme_contacts by business",
                "status_extra_info": {"name": "New"},
                "milestone_name": None,
                "epics": [{"ref": 46, "subject": "Developer experience"}],
            },
            (39, 1111): {
                "ref": 1111,
                "subject": "Minimal beta moderation and support tooling",
                "status_extra_info": {"name": "New"},
                "milestone_name": "M3 - Beta Launch",
                "epics": [
                    {"ref": 10, "subject": "Phase 9 -- Admin Panel"},
                    {"ref": 1593, "subject": "Application Security Hardening and Launch Verification"},
                ],
            },
        }
        expectations = [
            EpicExpectation(39, 3, "Multi-business isolation", 39, 24, "same-project", "M1"),
            EpicExpectation(39, 12, "Phase 11 -- Beta Launch", 39, 1111, "same-project", "M3"),
            EpicExpectation(39, 46, "Developer experience", 39, 24, "same-project", "M1"),
        ]
        candidates = build_candidate_missing_links(open_story_cache, expectations)
        assert [(item["story_project_id"], item["story_ref"], item["epic_ref"]) for item in candidates] == [
            (39, 24, 3),
            (39, 1111, 12),
        ]
        assert candidates[0]["story_project_slug"] == "cholo-bots"


class TestOutputDefaults:
    def test_ensure_output_dir_uses_gitignored_default(self, tmp_path):
        paths = ensure_output_dir(tmp_path / DEFAULT_OUT_DIR)
        assert paths["report_json"].name == DEFAULT_REPORT_JSON
        assert paths["report_md"].name == DEFAULT_REPORT_MD
        assert paths["manifest_json"].name == DEFAULT_MANIFEST_JSON
        assert paths["report_json"].parent.exists()

    def test_manifest_template_is_created_once(self, tmp_path):
        manifest_path = tmp_path / DEFAULT_MANIFEST_JSON
        write_manifest_template(manifest_path)
        assert json.loads(manifest_path.read_text()) == {"actions": []}

        manifest_path.write_text('{"actions":[{"type":"keep"}]}\n')
        write_manifest_template(manifest_path)
        assert json.loads(manifest_path.read_text()) == {"actions": [{"type": "keep"}]}


class TestRenderMarkdown:
    def test_renders_summary_and_missing_links(self):
        report = {
            "generated_at": "2026-11-07T00:00:00Z",
            "summary": {
                "project_39": {
                    "project_slug": "cholo-bots",
                    "open_stories": 44,
                    "open_no_milestone": 44,
                    "open_zero_epic": 10,
                    "open_multi_epic": 2,
                },
            },
            "project39": {
                "no_milestone": {
                    "expected_assignment": [1, 2],
                    "intentionally_outside": [1],
                    "manual_review": [1, 2, 3],
                },
                "zero_epic": [{"ref": 21}, {"ref": 33}],
            },
            "candidate_missing_epic_links": [
                {
                    "story_project_slug": "cholo-bots",
                    "story_ref": 24,
                    "epic_project_slug": "cholo-bots",
                    "epic_ref": 3,
                },
            ],
        }
        markdown = render_markdown(report)
        assert "### cholo-bots" in markdown
        assert "Project 39 No-Milestone" in markdown
        assert "- Manual review: `3`" in markdown
        assert "`#21`" in markdown
        assert "cholo-bots #24" in markdown
        assert "cholo-bots epic #3" in markdown


class FakeClient:
    """Minimal stand-in for TaigaClient. Records detail GETs so tests can assert
    the audit does not issue per-story requests."""

    def __init__(self, stories_by_project):
        self.stories_by_project = stories_by_project
        self.detail_get_calls = []

    def list_all(self, path, **params):
        if path == "/api/v1/userstories":
            return self.stories_by_project.get(params.get("project"), [])
        return []

    def get(self, path, **params):
        self.detail_get_calls.append(path)
        raise AssertionError(f"unexpected per-item detail GET: {path}")


def _story(ref, *, milestone=None, epics=None, closed=False):
    return {
        "id": ref,
        "ref": ref,
        "subject": f"Story {ref}",
        "milestone_name": milestone,
        "milestone_slug": None,
        "status_extra_info": {"name": "Closed" if closed else "New", "is_closed": closed},
        "epics": epics or [],
    }


class TestBuildReport:
    def test_uses_list_response_without_per_story_detail_requests(self):
        client = FakeClient({
            39: [_story(100, milestone="M1", epics=[{"ref": 6, "subject": "Phase 5"}]), _story(200), _story(201, closed=True)],
        })
        build_report(client, "")
        assert client.detail_get_calls == []

    def test_reads_only_project_39(self):
        client = FakeClient({3: [_story(1)], 39: [_story(2)]})
        report = build_report(client, "")
        assert set(report["summary"]) == {"project_39"}
        assert report["summary"]["project_39"]["total_stories"] == 1

    def test_lists_no_milestone_orphans_for_manual_review(self):
        client = FakeClient({
            39: [_story(202), _story(201, milestone="X"), _story(200), _story(203, closed=True)],
        })
        report = build_report(client, "")
        refs = [item["ref"] for item in report["project39"]["no_milestone"]["manual_review"]]
        assert refs == [200, 202]
        assert [item["ref"] for item in report["project39"]["zero_epic"]] == [200, 201, 202]


class TestReadRoadmap:
    def test_missing_default_roadmaps_yield_empty_text(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        assert read_roadmap(None) == ""
        assert "no roadmap found" in capsys.readouterr().err

    def test_explicit_roadmap_is_read(self, tmp_path):
        roadmap = tmp_path / "plan.md"
        roadmap.write_text("### M1: First\n")
        assert read_roadmap(str(roadmap)) == "### M1: First\n"
