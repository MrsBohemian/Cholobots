# ABOUTME: Pytest tests for scripts/taiga_select_issues.py, the read-only bug sweep selector.
# ABOUTME: Authoritative coverage of purposes, filters, validation, the Urgent split, sorting and JSON output.
import io
import json
import os
import sys
from contextlib import redirect_stderr, redirect_stdout

import pytest

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts"))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts", "lib"))

from taiga_client import TaigaError
from taiga_select_issues import (
    Level,
    Metadata,
    OptionError,
    Status,
    main,
    parse_args,
    resolve_options,
    select_issues,
)

# Project 39 (cholo-bots) after taiga_project_setup.py --apply. Pre-existing ids are
# real; the ids of the statuses and priority the setup adds are placeholders.
METADATA = Metadata(
    bug_type_id=50,
    type_names={50: "Bug", 51: "Question", 52: "Enhancement"},
    statuses={
        "New": Status(71, False),
        "In progress": Status(72, False),
        "Ready for review": Status(901, False),
        "Ready for test": Status(73, False),
        "Blocked": Status(902, False),
        "Closed": Status(74, True),
    },
    priorities={
        "Low": Level(49, 1),
        "Normal": Level(50, 3),
        "High": Level(51, 5),
        "Urgent": Level(903, 7),
    },
    severities={
        "Minor": Level(60, 2),
        "Normal": Level(61, 3),
        "Important": Level(62, 4),
        "Critical": Level(63, 5),
    },
    milestones=[{"id": 7, "slug": "sprint-7", "name": "Sprint 7"}, {"id": 8, "slug": "sprint-8", "name": "Sprint 8"}],
)

TYPE_IDS = {name: tid for tid, name in METADATA.type_names.items()}


def make_issue(
    ref,
    *,
    tags=("triaged",),
    status="New",
    priority="Normal",
    severity="Normal",
    type_="Bug",
    milestone=None,
):
    return {
        "ref": ref,
        "id": ref + 1000,
        "subject": f"Issue {ref}",
        "type": TYPE_IDS[type_],
        "status": METADATA.statuses[status].id,
        "status_extra_info": {"name": status, "is_closed": METADATA.statuses[status].is_closed},
        "priority": METADATA.priorities[priority].id if priority else None,
        "severity": METADATA.severities[severity].id if severity else None,
        "tags": [[t, None] for t in tags],
        "milestone": milestone,
    }


def run_select(purpose, issues, *flags):
    argv = ["--purpose", purpose, *flags]
    options = resolve_options(parse_args(argv), METADATA)
    return select_issues(issues, options, METADATA)


def refs(rows):
    return [r["ref"] for r in rows]


class TestPurposeDefaults:
    def test_analyze_drops_untriaged_analyzed_and_non_bugs(self):
        issues = [
            make_issue(1),
            make_issue(2, tags=()),
            make_issue(3, tags=("triaged", "llm-analyzed")),
            make_issue(4, type_="Question"),
        ]
        result = run_select("analyze", issues)
        assert refs(result.selected) == [1]

    def test_fix_drops_review_and_blocked_and_closed(self):
        issues = [
            make_issue(1),
            make_issue(2, status="Ready for review"),
            make_issue(3, status="Blocked"),
            make_issue(4, status="Closed"),
        ]
        assert refs(run_select("fix", issues).selected) == [1]

    def test_fix_moves_urgent_out_of_selected(self):
        issues = [make_issue(1), make_issue(2, priority="Urgent")]
        result = run_select("fix", issues)
        assert refs(result.selected) == [1]
        assert refs(result.urgent) == [2]

    def test_fix_filters_apply_before_urgent_split(self):
        issues = [make_issue(1, tags=("triaged", "pipeline")), make_issue(2, tags=("triaged", "product"), priority="Urgent")]
        result = run_select("fix", issues, "--surface", "pipeline")
        assert refs(result.selected) == [1]
        assert result.urgent == []

    def test_analyze_keeps_urgent_in_selected(self):
        result = run_select("analyze", [make_issue(1, priority="Urgent")])
        assert refs(result.selected) == [1]
        assert result.urgent == []

    def test_sprint_keeps_closed_and_non_bugs_without_triage_gate(self):
        issues = [
            make_issue(1, tags=(), status="Closed", milestone=7),
            make_issue(2, tags=(), type_="Question", milestone=7),
        ]
        result = run_select("sprint", issues, "--milestone", "7")
        assert refs(result.selected) == [1, 2]

    def test_triage_selects_open_untriaged_bugs_only(self):
        issues = [
            make_issue(1, tags=()),
            make_issue(2),
            make_issue(3, tags=(), type_="Question"),
            make_issue(4, tags=(), status="Closed"),
            make_issue(5, tags=("product",), priority="High"),
            make_issue(6, tags=(), priority="Urgent"),
        ]
        result = run_select("triage", issues)
        assert refs(result.selected) == [6, 5, 1]
        assert result.urgent == []

    def test_triage_reason_for_triaged_ref(self):
        result = run_select("triage", [make_issue(5)], "--refs", "5")
        assert result.skipped_refs == [{"ref": 5, "reason": "tagged triaged"}]

    def test_triage_needs_no_milestone(self):
        options = resolve_options(parse_args(["--purpose", "triage"]), METADATA)
        assert options.milestone == []

    def test_sprint_requires_milestone(self):
        with pytest.raises(OptionError, match="--milestone"):
            resolve_options(parse_args(["--purpose", "sprint"]), METADATA)


class TestIncludeFilters:
    def test_surface_values_are_or_within_dimension(self):
        issues = [make_issue(1, tags=("triaged", "product")), make_issue(2, tags=("triaged", "ci")), make_issue(3, tags=("triaged", "devx"))]
        assert refs(run_select("fix", issues, "--surface", "product,ci").selected) == [1, 2]

    def test_dimensions_are_and_across(self):
        issues = [
            make_issue(1, tags=("triaged", "product", "src:user")),
            make_issue(2, tags=("triaged", "product", "src:agent")),
            make_issue(3, tags=("triaged", "ci", "src:user")),
        ]
        result = run_select("fix", issues, "--surface", "product", "--source", "src:user")
        assert refs(result.selected) == [1]

    def test_tag_include(self):
        issues = [make_issue(1, tags=("triaged", "x")), make_issue(2)]
        assert refs(run_select("fix", issues, "--tag", "x").selected) == [1]

    def test_milestone_slug_is_case_insensitive(self):
        issues = [make_issue(1, milestone=7), make_issue(2, milestone=8)]
        assert refs(run_select("fix", issues, "--milestone", "Sprint-7").selected) == [1]

    def test_milestone_include_by_id_or_slug(self):
        issues = [make_issue(1, milestone=7), make_issue(2, milestone=8), make_issue(3)]
        assert refs(run_select("fix", issues, "--milestone", "sprint-7").selected) == [1]
        assert refs(run_select("fix", issues, "--milestone", "8").selected) == [2]


class TestExcludeFilters:
    def setup_method(self):
        self.issues = [
            make_issue(1, tags=("triaged", "product", "src:user", "t1"), milestone=7),
            make_issue(2, tags=("triaged", "ci", "src:agent"), milestone=8),
        ]

    def test_exclude_surface(self):
        assert refs(run_select("fix", self.issues, "--exclude-surface", "product").selected) == [2]

    def test_exclude_source(self):
        assert refs(run_select("fix", self.issues, "--exclude-source", "src:agent").selected) == [1]

    def test_exclude_tag(self):
        assert refs(run_select("fix", self.issues, "--exclude-tag", "t1").selected) == [2]

    def test_exclude_refs(self):
        assert refs(run_select("fix", self.issues, "--exclude-refs", "1").selected) == [2]

    def test_exclude_milestone(self):
        assert refs(run_select("fix", self.issues, "--exclude-milestone", "sprint-7").selected) == [2]


class TestPriorityAndSeverityBounds:
    def test_min_priority(self):
        issues = [make_issue(1, priority="Low"), make_issue(2, priority="High"), make_issue(3, priority="Normal")]
        assert refs(run_select("analyze", issues, "--min-priority", "Normal").selected) == [2, 3]

    def test_max_priority(self):
        issues = [make_issue(1, priority="Low"), make_issue(2, priority="High"), make_issue(3, priority="Normal")]
        assert refs(run_select("analyze", issues, "--max-priority", "Normal").selected) == [3, 1]

    def test_min_severity(self):
        issues = [make_issue(1, severity="Normal"), make_issue(2, severity="Important"), make_issue(3, severity="Critical")]
        assert refs(run_select("analyze", issues, "--min-severity", "Important").selected) == [3, 2]

    def test_unset_priority_fails_min_and_passes_max(self):
        issues = [make_issue(1, priority=None)]
        assert run_select("analyze", issues, "--min-priority", "Low").selected == []
        assert refs(run_select("analyze", issues, "--max-priority", "Low").selected) == [1]

    def test_min_above_max_raises(self):
        with pytest.raises(OptionError, match="min-priority"):
            resolve_options(parse_args(["--purpose", "fix", "--min-priority", "High", "--max-priority", "Low"]), METADATA)


class TestStatusFlags:
    def test_status_replaces_default_set(self):
        issues = [make_issue(1, status="In progress"), make_issue(2), make_issue(3, status="Blocked")]
        assert refs(run_select("fix", issues, "--status", "In progress").selected) == [1]

    def test_status_can_select_a_default_excluded_status(self):
        issues = [make_issue(1, status="Blocked"), make_issue(2)]
        assert refs(run_select("fix", issues, "--status", "Blocked").selected) == [1]

    def test_exclude_status_adds_to_defaults(self):
        issues = [make_issue(1), make_issue(2, status="In progress"), make_issue(3, status="Blocked")]
        assert refs(run_select("fix", issues, "--exclude-status", "New").selected) == [2]


class TestRefs:
    def test_refs_only_narrow_and_report_skipped_reason(self):
        issues = [make_issue(5), make_issue(6, tags=()), make_issue(7)]
        result = run_select("fix", issues, "--refs", "5,6")
        assert refs(result.selected) == [5]
        assert result.skipped_refs == [{"ref": 6, "reason": "untriaged"}]

    def test_missing_ref_is_reported(self):
        result = run_select("fix", [make_issue(5)], "--refs", "5,99")
        assert result.skipped_refs == [{"ref": 99, "reason": "not found among fetched issues"}]

    def test_filter_failure_reason_names_flag(self):
        issues = [make_issue(5, tags=("triaged", "product"))]
        result = run_select("fix", issues, "--refs", "5", "--exclude-surface", "product")
        assert result.skipped_refs == [{"ref": 5, "reason": "excluded by --exclude-surface"}]

    def test_blocked_status_reason(self):
        result = run_select("fix", [make_issue(5, status="Blocked")], "--refs", "5")
        assert result.skipped_refs == [{"ref": 5, "reason": "status Blocked"}]


class TestValidation:
    def test_surface_and_source_match_case_insensitively(self):
        options = resolve_options(
            parse_args(["--purpose", "fix", "--surface", "Product", "--exclude-surface", "CI",
                        "--source", "SRC:USER", "--exclude-source", "Src:Agent"]),
            METADATA,
        )
        assert options.surface == ["product"]
        assert options.exclude_surface == ["ci"]
        assert options.source == ["src:user"]
        assert options.exclude_source == ["src:agent"]

    def test_surface_typo_lists_valid_values(self):
        with pytest.raises(OptionError) as exc:
            resolve_options(parse_args(["--purpose", "fix", "--surface", "prodcut"]), METADATA)
        assert "product" in str(exc.value) and "pipeline" in str(exc.value)

    def test_source_typo_lists_valid_values(self):
        with pytest.raises(OptionError, match="src:user"):
            resolve_options(parse_args(["--purpose", "fix", "--source", "src:nope"]), METADATA)

    def test_unknown_priority_lists_valid_values(self):
        with pytest.raises(OptionError, match="Urgent"):
            resolve_options(parse_args(["--purpose", "fix", "--min-priority", "Hgh"]), METADATA)

    def test_unknown_severity_lists_valid_values(self):
        with pytest.raises(OptionError, match="Critical"):
            resolve_options(parse_args(["--purpose", "fix", "--max-severity", "Huge"]), METADATA)

    def test_unknown_status_lists_valid_values(self):
        with pytest.raises(OptionError, match="Blocked"):
            resolve_options(parse_args(["--purpose", "fix", "--status", "Nope"]), METADATA)

    def test_unknown_milestone_lists_valid_values(self):
        with pytest.raises(OptionError, match="sprint-7"):
            resolve_options(parse_args(["--purpose", "fix", "--milestone", "nope"]), METADATA)

    def test_matching_is_case_insensitive(self):
        options = resolve_options(parse_args(["--purpose", "fix", "--min-priority", "high"]), METADATA)
        assert options.min_priority == 5

    @pytest.mark.parametrize("flag", [
        "--surface", "--exclude-surface", "--source", "--exclude-source", "--refs", "--exclude-refs",
        "--tag", "--exclude-tag", "--milestone", "--exclude-milestone", "--status", "--exclude-status",
    ])
    @pytest.mark.parametrize("value", ["", ",", " , "])
    def test_list_flag_with_no_values_raises(self, flag, value):
        with pytest.raises(OptionError, match=f"{flag} was given no values"):
            resolve_options(parse_args(["--purpose", "fix", flag, value]), METADATA)


class TestSortAndNormalization:
    def test_sort_priority_desc_severity_desc_ref_asc_unset_last(self):
        issues = [
            make_issue(1, priority="Normal", severity="Normal"),
            make_issue(2, priority="High", severity="Normal"),
            make_issue(3, priority="High", severity="Critical"),
            make_issue(4, priority=None, severity=None),
            make_issue(5, priority="Normal", severity="Normal"),
        ]
        assert refs(run_select("analyze", issues).selected) == [3, 2, 1, 5, 4]

    def test_repeated_and_comma_flags_merge_deduplicated_in_order(self):
        options = resolve_options(parse_args(["--purpose", "fix", "--surface", "product", "--surface", "ci,product"]), METADATA)
        assert options.to_json()["surface"] == ["product", "ci"]


TAIGA_DEFAULT_PAGE_SIZE = 30


class FakeClient:
    def __init__(self, issues, fail=False, milestones=None):
        self.issues = issues
        self.fail = fail
        self.milestones = METADATA.milestones if milestones is None else milestones
        self.list_params = None

    def get(self, path, **params):
        if self.fail:
            raise TaigaError("boom")
        if path == "/api/v1/issue-types":
            return [{"id": tid, "name": n} for tid, n in METADATA.type_names.items()]
        if path == "/api/v1/issue-statuses":
            return [{"id": s.id, "name": n, "is_closed": s.is_closed} for n, s in METADATA.statuses.items()]
        if path == "/api/v1/priorities":
            priorities = getattr(self, "priorities", METADATA.priorities)
            return [{"id": l.id, "name": n, "order": l.order} for n, l in priorities.items()]
        if path == "/api/v1/severities":
            return [{"id": l.id, "name": n, "order": l.order} for n, l in METADATA.severities.items()]
        if path == "/api/v1/milestones":
            return self.milestones[:TAIGA_DEFAULT_PAGE_SIZE]
        raise AssertionError(f"unexpected path {path}")

    def list_all(self, path, **params):
        if path == "/api/v1/milestones":
            return self.milestones
        assert path == "/api/v1/issues"
        self.list_params = params
        return self.issues


def run_main(argv, client):
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        code = main(argv, client_factory=lambda: client)
    return code, out.getvalue(), err.getvalue()


class TestMain:
    def test_outputs_full_json_shape(self):
        issues = [make_issue(1, tags=("triaged", "product", "src:user")), make_issue(2, priority="Urgent")]
        code, out, _ = run_main(["--purpose", "fix"], FakeClient(issues))
        assert code == 0
        data = json.loads(out)
        assert set(data) == {"purpose", "options", "ids", "selected", "urgent", "skippedRefs", "counts"}
        assert data["ids"]["bugTypeId"] == 50
        assert data["ids"]["statuses"]["Closed"] == 74
        assert data["ids"]["priorities"]["Urgent"] == 903
        assert data["ids"]["severities"]["Critical"] == 63
        row = data["selected"][0]
        assert row == {
            "ref": 1,
            "id": 1001,
            "subject": "Issue 1",
            "type": "Bug",
            "status": "New",
            "priority": "Normal",
            "severity": "Normal",
            "surface": "product",
            "source": "src:user",
            "milestone": None,
            "tags": ["triaged", "product", "src:user"],
        }
        assert refs(data["urgent"]) == [2]
        assert data["counts"] == {"fetched": 2, "selected": 1, "urgent": 1, "skipped": 0}

    def test_empty_selection_exits_zero(self):
        code, out, _ = run_main(["--purpose", "fix"], FakeClient([]))
        assert code == 0
        assert json.loads(out)["selected"] == []

    def test_bad_option_exits_two_with_stderr(self):
        code, out, err = run_main(["--purpose", "fix", "--surface", "prodcut"], FakeClient([]))
        assert code == 2
        assert out == ""
        assert "product" in err

    def test_empty_refs_exits_two_instead_of_selecting_everything(self):
        code, out, err = run_main(["--purpose", "fix", "--refs", ""], FakeClient([make_issue(1)]))
        assert code == 2
        assert out == ""
        assert "--refs" in err

    def test_default_fetch_uses_server_side_type_and_open_filters(self):
        client = FakeClient([])
        run_main(["--purpose", "fix"], client)
        assert client.list_params == {"project": 39, "type": 50, "status__is_closed": "false"}

    def test_explicit_refs_fetch_without_server_filters_so_reasons_are_reported(self):
        issues = [make_issue(5, status="Closed"), make_issue(6, type_="Question")]
        client = FakeClient(issues)
        code, out, _ = run_main(["--purpose", "fix", "--refs", "5,6"], client)
        assert code == 0
        assert client.list_params == {"project": 39}
        assert json.loads(out)["skippedRefs"] == [
            {"ref": 5, "reason": "closed"},
            {"ref": 6, "reason": "not a Bug"},
        ]

    def test_milestone_beyond_first_page_resolves(self):
        milestones = [
            {"id": 100 + n, "slug": f"sprint-{100 + n}", "name": f"Sprint {100 + n}"}
            for n in range(TAIGA_DEFAULT_PAGE_SIZE + 1)
        ]
        last = milestones[-1]
        issues = [make_issue(1, milestone=last["id"]), make_issue(2, milestone=milestones[0]["id"])]
        code, out, err = run_main(["--purpose", "fix", "--milestone", last["slug"]], FakeClient(issues, milestones=milestones))
        assert err == ""
        assert code == 0
        assert refs(json.loads(out)["selected"]) == [1]

    def test_missing_urgent_priority_is_a_note_not_an_error(self):
        client = FakeClient([make_issue(1), make_issue(2, priority="High")])
        client.priorities = {n: l for n, l in METADATA.priorities.items() if n != "Urgent"}
        code, out, err = run_main(["--purpose", "fix"], client)
        assert code == 0
        data = json.loads(out)
        assert data["urgent"] == []
        assert refs(data["selected"]) == [2, 1]
        assert "no 'Urgent' priority" in err
        assert "taiga_project_setup.py" in err

    def test_triage_purpose_end_to_end(self):
        code, out, _ = run_main(["--purpose", "triage"], FakeClient([make_issue(1, tags=()), make_issue(2)]))
        assert code == 0
        assert refs(json.loads(out)["selected"]) == [1]

    def test_taiga_error_exits_one(self):
        code, _, err = run_main(["--purpose", "fix"], FakeClient([], fail=True))
        assert code == 1
        assert "boom" in err
