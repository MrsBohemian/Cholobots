# ABOUTME: Pytest tests for the taiga_strip_duplicated_description.py script.
# ABOUTME: Verifies normalisation, separator splitting, strip-point selection, history extraction, and report shape.
import json
import os
import sys

import pytest

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts"))

import taiga_strip_duplicated_description
from taiga_strip_duplicated_description import (
    RE_CLEAN_MODE,
    TaigaStripError,
    build_report,
    choose_strip_point,
    choose_structural_strip_point,
    coverage,
    extract_description_change,
    extract_prior_description_html,
    fold_description,
    is_analyzed,
    normalise_for_comparison,
    plan_reconstruction,
    process_items,
    process_reclean_items,
    redact_cmd,
    run_json_command,
    split_candidates,
    split_sections,
    write_report,
)


ORIGINAL_MARKDOWN = """## Steps to Reproduce

1. Open the **pledge** page for a live challenge.
2. Click [Pledge now](https://example.test/pledge).
3. Observe the amount field is _empty_.

Expected: the amount field keeps the previously entered value.
"""

ORIGINAL_HTML = (
    '<h2 id="steps-to-reproduce">Steps to Reproduce</h2>\n'
    "<ol>\n"
    "<li>Open the <strong>pledge</strong> page for a live challenge.</li>\n"
    '<li>Click <a href="https://example.test/pledge">Pledge now</a>.</li>\n'
    "<li>Observe the amount field is <em>empty</em>.</li>\n"
    "</ol>\n"
    "<p>Expected: the amount field keeps the previously entered value.</p>"
)


class TestNormaliseForComparison:
    def test_markdown_and_rendered_html_normalise_equal(self):
        assert normalise_for_comparison(ORIGINAL_MARKDOWN) == normalise_for_comparison(ORIGINAL_HTML)

    def test_adjacent_inline_spans_do_not_gain_a_space(self):
        """`A`/`B` is one word; replacing each tag with a space made the rendered
        form three."""
        assert normalise_for_comparison("The `Uploading`/`Uploaded` branches.") == (
            normalise_for_comparison("<p>The <code>Uploading</code>/<code>Uploaded</code> branches.</p>")
        )

    def test_a_bracketed_span_normalises_the_same_from_either_syntax(self):
        """Stripping a tag leaves a space that the markdown never had, on both
        sides of the bracket."""
        assert normalise_for_comparison("Run (`TestChisme`) and see [#499].") == (
            normalise_for_comparison("Run (<code>TestChisme</code>) and see [<a>#499</a>].")
        )

    def test_a_raw_less_than_is_not_mistaken_for_a_tag(self):
        """`git grep '<<<<<<<'` in markdown must not be eaten as far as the next
        `>`, or the text stops comparing equal to its own rendering."""
        markdown = "Run `git grep -l '<<<<<<<' -- .` and read <the output>."
        rendered = "<p>Run <code>git grep -l '&lt;&lt;&lt;&lt;&lt;&lt;&lt;' -- .</code> and read &lt;the output&gt;.</p>"

        assert normalise_for_comparison(markdown) == normalise_for_comparison(rendered)
        assert "<<<<<<<" in normalise_for_comparison(markdown)

    def test_collapses_whitespace_and_lowercases(self):
        assert normalise_for_comparison("##  Plan\n\n\nThe   PLAN.\n") == "plan the plan."


class TestSplitCandidates:
    def test_one_candidate_per_top_level_rule_with_tail(self):
        description = "## Plan\n\nBody.\n\n---\n\n## Original\n\nOld text.\n"
        candidates = split_candidates(description)

        assert len(candidates) == 1
        assert candidates[0].tail.strip() == "## Original\n\nOld text."

    def test_ignores_rules_inside_fenced_code_blocks(self):
        description = (
            "## Plan\n"
            "\n"
            "```markdown\n"
            "---\n"
            "front matter\n"
            "---\n"
            "```\n"
            "\n"
            "---\n"
            "\n"
            "## Original\n"
        )
        candidates = split_candidates(description)

        assert len(candidates) == 1
        assert candidates[0].tail.strip() == "## Original"

    def test_a_fenced_line_with_an_info_string_does_not_close_the_fence(self):
        """A nested example fence must not end the real block, or a `---` still
        inside it becomes a strip point and the rest of the item is deleted."""
        description = (
            "## Implementation Plan\n"
            "\n"
            "````markdown\n"
            "An example of the front matter we emit:\n"
            "```sh\n"
            "\n"
            "---\n"
            "\n"
            "still inside the outer fence\n"
            "````\n"
            "\n"
            "Done.\n"
        )

        assert split_candidates(description) == []


class TestChooseStripPoint:
    def test_returns_candidate_when_similarity_meets_threshold(self):
        description = "## Plan\n\nBody.\n\n---\n\n" + ORIGINAL_MARKDOWN
        candidates = split_candidates(description)

        chosen, similarity = choose_strip_point(candidates, ORIGINAL_HTML, threshold=0.9)

        assert chosen is candidates[0]
        assert similarity >= 0.9

    def test_returns_none_when_best_similarity_is_below_threshold(self):
        description = (
            "## Plan\n"
            "\n"
            "Body.\n"
            "\n"
            "---\n"
            "\n"
            "## Verification\n"
            "\n"
            "Run the affected suite and read the XML.\n"
        )
        candidates = split_candidates(description)

        assert choose_strip_point(candidates, ORIGINAL_HTML, threshold=0.9) is None

    def test_picks_earliest_qualifying_rule_when_copies_are_stacked(self):
        design_summary = "## Design\n\nWireframes live in the design doc.\n"
        prior_markdown = design_summary + "\n---\n\n" + ORIGINAL_MARKDOWN
        prior_html = (
            '<h2 id="design">Design</h2>\n'
            "<p>Wireframes live in the design doc.</p>\n"
            "<hr>\n" + ORIGINAL_HTML
        )
        description = "## Plan\n\nBody.\n\n---\n\n" + prior_markdown
        candidates = split_candidates(description)

        assert len(candidates) == 2

        chosen, _ = choose_strip_point(candidates, prior_html, threshold=0.5)

        assert chosen is candidates[0]


class TestExtractPriorDescriptionHtml:
    def test_returns_old_html_from_newest_description_change(self):
        history = [
            {
                "created_date": "2026-01-01T00:00:00Z",
                "values_diff": {"description_html": ["<p>oldest</p>", "<p>middle</p>"]},
            },
            {
                "created_date": "2026-03-01T00:00:00Z",
                "values_diff": {"description_html": ["<p>middle</p>", "<p>newest</p>"]},
            },
            {
                "created_date": "2026-04-01T00:00:00Z",
                "values_diff": {"status": [1, 2]},
            },
        ]

        assert extract_prior_description_html(history) == "<p>middle</p>"

    def test_returns_none_when_no_entry_changed_the_description(self):
        history = [{"created_date": "2026-04-01T00:00:00Z", "values_diff": {"status": [1, 2]}}]

        assert extract_prior_description_html(history) is None


class TestSplitSections:
    def test_splits_at_top_level_headings_and_keeps_a_leading_preamble(self):
        text = "Intro line.\n\n## First\n\nOne.\n\n### Nested\n\nStill first.\n\n## Second\n\nTwo.\n"

        sections = split_sections(text)

        assert [section.heading for section in sections] == [None, "first", "second"]
        assert sections[0].text == "Intro line.\n\n"
        assert "### Nested" in sections[1].text
        assert sections[2].text.strip().endswith("Two.")

    def test_ignores_headings_inside_fenced_code_blocks(self):
        text = "## Real\n\n```sh\n# not a heading\n## also not\n```\n\nDone.\n"

        sections = split_sections(text)

        assert [section.heading for section in sections] == ["real"]

    def test_normalises_heading_text_for_comparison(self):
        sections = split_sections("##   Acceptance Criteria   ##\n\nBody.\n")

        assert sections[0].heading == "acceptance criteria"

    def test_a_fenced_line_with_an_info_string_does_not_close_the_fence(self):
        """A closing fence is only the marker itself, not a line that merely starts
        with it -- otherwise a fenced block showing an example fence with a language
        tag (` ```sh `) would prematurely close the real fence."""
        text = "## Real\n\n```\nExample:\n```sh\n## still inside the fence\n```\n\nDone.\n"

        sections = split_sections(text)

        assert [section.heading for section in sections] == ["real"]


class TestCoverage:
    def test_text_the_haystack_restates_verbatim_is_fully_covered(self):
        head = "## Plan\n\n### Restated\n\n" + ORIGINAL_MARKDOWN

        assert coverage(ORIGINAL_MARKDOWN, head) == 1.0

    def test_text_the_haystack_does_not_carry_scores_low(self):
        head = "## Implementation Plan\n\n### Overview\n\nAdd the pledge endpoint.\n"
        original = "## Acceptance Criteria\n\n- A supporter cannot pledge a negative amount.\n"

        assert coverage(original, head) < 0.9

    def test_empty_text_is_trivially_covered(self):
        assert coverage("", "anything") == 1.0


class TestFoldDescription:
    def test_drops_a_tail_whose_sections_the_head_already_covers(self):
        plan = "## Implementation Plan\n\n### Restated\n\n" + ORIGINAL_MARKDOWN
        description = plan + "\n---\n\n" + ORIGINAL_MARKDOWN
        candidates = split_candidates(description)

        assert fold_description(description, candidates[0]) == plan

    def test_keeps_a_section_whose_heading_collides_but_whose_content_differs(self):
        """A shared heading name is not proof of duplication: the plan's own
        `### Tests` subsection says nothing about the original's `## Tests`."""
        plan = (
            "## Implementation Plan\n"
            "\n"
            "### Tests\n"
            "\n"
            "Add pytest cases to tests/ for the new validator.\n"
        )
        original = (
            "## Tests\n"
            "\n"
            "Manual QA only so far: the reviewer walked the pledge flow on staging\n"
            "and confirmed the receipt email renders.\n"
        )
        description = plan + "\n---\n\n" + original
        candidates = split_candidates(description)

        folded = fold_description(description, candidates[0])

        assert "walked the pledge flow on staging" in folded

    def test_keeps_a_tail_whose_headings_are_all_deeper_than_top_level(self):
        """Regression for #2141: a tail opening at `###` was one headingless
        section and was dropped whole."""
        plan = PLAN_HEAD
        original = (
            "### Acceptance Criteria\n"
            "\n"
            "- The host sees the pledge total update without a reload.\n"
            "- A supporter cannot pledge a negative amount.\n"
        )
        description = plan + "\n---\n\n" + original
        candidates = split_candidates(description)

        folded = fold_description(description, candidates[0])

        assert "### Acceptance Criteria" in folded
        assert "cannot pledge a negative amount" in folded

    def test_keeps_a_prose_tail_that_has_no_headings_at_all(self):
        """Regression for #2141: the common bug-report shape is prose with no
        headings, which collapsed to a single headingless section and vanished."""
        plan = PLAN_HEAD
        original = (
            "Reported by a supporter: after submitting a pledge of 25 GBP the\n"
            "confirmation screen shows 0 GBP until the page is reloaded.\n"
        )
        description = plan + "\n---\n\n" + original
        candidates = split_candidates(description)

        folded = fold_description(description, candidates[0])

        assert "confirmation screen shows 0 GBP" in folded

    def test_folds_acceptance_criteria_forward_instead_of_deleting_it(self):
        """Regression for #2141: the cleanup truncated at the separator, destroying
        acceptance criteria that the plan template has no section for."""
        plan = PLAN_HEAD
        original = "We want a thing.\n\n## Acceptance Criteria\n\n- Criterion one.\n- Criterion two.\n"
        description = plan + "\n---\n\n" + original
        candidates = split_candidates(description)

        folded = fold_description(description, candidates[0])

        assert folded.startswith(plan.rstrip())
        assert "## Acceptance Criteria" in folded
        assert "- Criterion one." in folded
        assert "- Criterion two." in folded
        # The restatement above the criteria survives too: the plan's Overview says
        # something else, so nothing here proves it is a second copy.
        assert "We want a thing." in folded

    def test_folds_a_section_the_surviving_plan_still_points_at(self):
        """Regression for #2141 (#2025/#2026): truncation left the plan citing a
        section that no longer existed."""
        plan = "## Implementation Plan\n\n### Overview\n\nSee the MERGE rows in the section below.\n"
        original = "## MERGE Rows\n\n| row | spec |\n|---|---|\n| 1 | merge a into b |\n"
        description = plan + "\n---\n\n" + original
        candidates = split_candidates(description)

        folded = fold_description(description, candidates[0])

        assert "## MERGE Rows" in folded
        assert "| 1 | merge a into b |" in folded

    def test_leaves_the_head_byte_identical(self):
        plan = "## Implementation Plan\n\nBody with  deliberate   spacing.\n"
        description = plan + "\n---\n\n## Acceptance Criteria\n\n- Keep me.\n"

        folded = fold_description(description, split_candidates(description)[0])

        assert folded.startswith(plan.rstrip())


class TestRedactCmd:
    def test_masks_auth_payload_with_password(self):
        auth_cmd = [
            "curl", "-sf", "-H", "Content-Type: application/json", "-X", "POST",
            "-d", json.dumps({"type": "normal", "username": "u", "password": "s3cr3t"}),
            "https://taiga.example/api/v1/auth",
        ]
        rendered = redact_cmd(auth_cmd)

        assert "s3cr3t" not in rendered
        assert "-d <redacted>" in rendered
        assert "https://taiga.example/api/v1/auth" in rendered

    def test_masks_authorization_bearer_token(self):
        get_cmd = ["curl", "-sf", "-H", "Authorization: Bearer abc.def.ghi", "https://taiga.example/api/v1/x"]
        rendered = redact_cmd(get_cmd)

        assert "abc.def.ghi" not in rendered
        assert "Authorization: <redacted>" in rendered


PLAN_HEAD = "## Implementation Plan\n\n### Overview\n\nAdd the thing.\n"


class TestChooseStructuralStripPoint:
    def test_returns_the_rule_whose_head_is_a_plan_and_whose_tail_is_not(self):
        description = PLAN_HEAD + "\n---\n\n## Original Bug Report\n\nIt breaks.\n"
        candidates = split_candidates(description)

        chosen = choose_structural_strip_point(description, candidates)

        assert chosen is not None
        assert fold_description(description, chosen).startswith(PLAN_HEAD.rstrip())

    def test_returns_none_when_the_tail_is_itself_a_plan(self):
        """Guards the inverted layout — original on top, plan below the rule."""
        description = "## Original Story\n\nWe want a thing.\n\n---\n\n" + PLAN_HEAD
        candidates = split_candidates(description)

        assert choose_structural_strip_point(description, candidates) is None

    def test_returns_none_when_the_head_is_not_a_plan(self):
        description = "## Notes\n\nSome context.\n\n---\n\n## Original Bug Report\n\nIt breaks.\n"
        candidates = split_candidates(description)

        assert choose_structural_strip_point(description, candidates) is None

    def test_picks_the_earliest_qualifying_rule_when_copies_are_stacked(self):
        description = PLAN_HEAD + "\n---\n\n## Background\n\nWhy.\n\n---\n\n## Original Story\n\nWant.\n"
        candidates = split_candidates(description)

        chosen = choose_structural_strip_point(description, candidates)

        assert chosen is not None
        assert fold_description(description, chosen).startswith(PLAN_HEAD.rstrip())


class FakeCompletedProcess:
    def __init__(self, returncode, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class TestRunJsonCommand:
    def test_retries_a_transient_connection_failure_then_succeeds(self, monkeypatch):
        responses = [
            FakeCompletedProcess(7, stderr="Failed to connect"),
            FakeCompletedProcess(7, stderr="Failed to connect"),
            FakeCompletedProcess(0, stdout='{"ok": true}'),
        ]
        calls = []
        monkeypatch.setattr(
            taiga_strip_duplicated_description.subprocess,
            "run",
            lambda cmd, **kwargs: (calls.append(cmd), responses.pop(0))[1],
        )
        monkeypatch.setattr(taiga_strip_duplicated_description.time, "sleep", lambda _: None)

        assert run_json_command(["curl", "https://taiga.example/api/v1/x"]) == {"ok": True}
        assert len(calls) == 3

    def test_raises_with_credentials_masked_after_attempts_are_exhausted(self, monkeypatch):
        monkeypatch.setattr(
            taiga_strip_duplicated_description.subprocess,
            "run",
            lambda cmd, **kwargs: FakeCompletedProcess(7, stderr="Failed to connect"),
        )
        monkeypatch.setattr(taiga_strip_duplicated_description.time, "sleep", lambda _: None)
        cmd = ["curl", "-H", "Authorization: Bearer abc.def.ghi", "https://taiga.example/api/v1/x"]

        with pytest.raises(TaigaStripError) as excinfo:
            run_json_command(cmd)

        assert "abc.def.ghi" not in str(excinfo.value)

    def test_does_not_retry_a_deterministic_http_error(self, monkeypatch):
        calls = []
        monkeypatch.setattr(
            taiga_strip_duplicated_description.subprocess,
            "run",
            lambda cmd, **kwargs: (calls.append(cmd), FakeCompletedProcess(22, stderr="404 Not Found"))[1],
        )
        monkeypatch.setattr(taiga_strip_duplicated_description.time, "sleep", lambda _: None)

        with pytest.raises(TaigaStripError):
            run_json_command(["curl", "-sf", "https://taiga.example/api/v1/x"])

        assert len(calls) == 1


class FakeClient:
    """Duck-typed stand-in for TaigaClient covering only what process_items uses."""

    def __init__(self, details, histories):
        self.details = details
        self.histories = histories
        self.patches = []

    def get_item(self, kind, item_id):
        return self.details[item_id]

    def get_history(self, kind, item_id):
        return self.histories[item_id]

    def patch_description(self, kind, item_id, description, version):
        self.patches.append((kind, item_id, description, version))


class TestProcessItems:
    def test_dry_run_records_decisions_without_writing(self):
        details = {
            10: {
                "id": 10,
                "ref": 2136,
                "version": 4,
                "description": "## Plan\n\nBody.\n\n---\n\n" + ORIGINAL_MARKDOWN,
            },
            11: {
                "id": 11,
                "ref": 2100,
                "version": 2,
                "description": "## Plan\n\nBody.\n\n---\n\n## Verification\n\nRun the suite.\n",
            },
            12: {
                "id": 12,
                "ref": 2050,
                "version": 1,
                "description": "## Plan\n\nBody.\n\n---\n\n" + ORIGINAL_MARKDOWN,
            },
            13: {"id": 13, "ref": 2010, "version": 1, "description": "## Plan\n\nNo rule here.\n"},
        }
        histories = {
            10: [{"created_date": "2026-01-01T00:00:00Z", "values_diff": {"description_html": [ORIGINAL_HTML, "<p>new</p>"]}}],
            11: [{"created_date": "2026-01-01T00:00:00Z", "values_diff": {"description_html": [ORIGINAL_HTML, "<p>new</p>"]}}],
            12: [{"created_date": "2026-01-01T00:00:00Z", "values_diff": {"status": [1, 2]}}],
            13: [{"created_date": "2026-01-01T00:00:00Z", "values_diff": {"description_html": [ORIGINAL_HTML, "<p>new</p>"]}}],
        }
        client = FakeClient(details, histories)

        records = process_items(
            client,
            "userstories",
            [details[10], details[11], details[12], details[13]],
            threshold=0.9,
            dry_run=True,
        )

        assert client.patches == []
        assert {record["ref"]: record["decision"] for record in records} == {
            2136: "strip",
            2100: "manual-review",
            2050: "no-history",
            2010: "no-separator",
        }
        assert records[0]["new_length"] < records[0]["old_length"]

    def test_strips_on_structure_when_history_no_longer_matches(self):
        """Items edited after analysis have a prior version that already holds the plan,
        so history similarity fails even though the duplicated original is still there."""
        details = {
            20: {
                "id": 20,
                "ref": 2200,
                "version": 9,
                "description": PLAN_HEAD + "\n---\n\n## Original Bug Report\n\nIt breaks.\n",
            },
        }
        histories = {
            20: [{"created_date": "2026-01-01T00:00:00Z", "values_diff": {"description_html": ["<p>an unrelated later edit</p>", "<p>new</p>"]}}],
        }
        client = FakeClient(details, histories)

        records = process_items(client, "userstories", [details[20]], threshold=0.9, dry_run=True)

        assert records[0]["decision"] == "strip"
        assert records[0]["verification"] == "structure"

    def test_records_history_as_the_verification_when_similarity_matches(self):
        details = {
            10: {
                "id": 10,
                "ref": 2136,
                "version": 4,
                "description": "## Implementation Plan\n\nBody.\n\n---\n\n" + ORIGINAL_MARKDOWN,
            },
        }
        histories = {
            10: [{"created_date": "2026-01-01T00:00:00Z", "values_diff": {"description_html": [ORIGINAL_HTML, "<p>new</p>"]}}],
        }
        client = FakeClient(details, histories)

        records = process_items(client, "userstories", [details[10]], threshold=0.9, dry_run=True)

        assert records[0]["decision"] == "strip"
        assert records[0]["verification"] == "history"

    def test_apply_writes_the_stripped_description_with_the_current_version(self):
        details = {
            10: {
                "id": 10,
                "ref": 2136,
                "version": 4,
                "description": "## Plan\n\n### Restated\n\n"
                + ORIGINAL_MARKDOWN
                + "\n---\n\n"
                + ORIGINAL_MARKDOWN,
            },
        }
        histories = {
            10: [{"created_date": "2026-01-01T00:00:00Z", "values_diff": {"description_html": [ORIGINAL_HTML, "<p>new</p>"]}}],
        }
        client = FakeClient(details, histories)

        records = process_items(client, "userstories", [details[10]], threshold=0.9, dry_run=False)

        assert records[0]["decision"] == "strip"
        assert client.patches == [
            ("userstories", 10, "## Plan\n\n### Restated\n\n" + ORIGINAL_MARKDOWN, 4),
        ]

    def test_apply_writes_the_folded_sections_the_head_does_not_cover(self):
        """Regression for #2141: an apply run must carry the tail's uncovered
        sections forward rather than truncating them away."""
        original = "We want a thing.\n\n## Acceptance Criteria\n\n- Criterion one.\n"
        details = {
            10: {
                "id": 10,
                "ref": 2136,
                "version": 4,
                "description": PLAN_HEAD + "\n---\n\n" + original,
            },
        }
        histories = {
            10: [{"created_date": "2026-01-01T00:00:00Z", "values_diff": {"description_html": ["<p>old</p>", "<p>new</p>"]}}],
        }
        client = FakeClient(details, histories)

        records = process_items(client, "userstories", [details[10]], threshold=0.9, dry_run=False)

        assert records[0]["decision"] == "strip"
        # The restatement and the criteria both survive: neither is content the
        # plan carries.
        assert records[0]["folded_sections"] == 2
        written = client.patches[0][2]
        assert written.startswith(PLAN_HEAD.rstrip())
        assert "We want a thing." in written
        assert "## Acceptance Criteria\n\n- Criterion one." in written

    def test_apply_skips_an_item_whose_version_moved_since_the_decision(self):
        """Concurrent edit: the re-read before the write returns a bumped version."""
        detail = {
            "id": 10,
            "ref": 2136,
            "version": 4,
            "description": "## Plan\n\nBody.\n\n---\n\n" + ORIGINAL_MARKDOWN,
        }
        histories = {
            10: [{"created_date": "2026-01-01T00:00:00Z", "values_diff": {"description_html": [ORIGINAL_HTML, "<p>new</p>"]}}],
        }
        client = FakeClient({10: detail}, histories)
        reads = []

        def get_item_with_concurrent_edit(kind, item_id):
            reads.append(item_id)
            return detail if len(reads) == 1 else {**detail, "version": 5}

        client.get_item = get_item_with_concurrent_edit

        records = process_items(client, "userstories", [detail], threshold=0.9, dry_run=False)

        assert records[0]["decision"] == "skipped-version-conflict"
        assert client.patches == []
        # Nothing was written, so the report must not claim the item shrank.
        assert records[0]["new_length"] == records[0]["old_length"]

    def test_records_the_separator_count_behind_each_decision(self):
        details = {
            10: {
                "id": 10,
                "ref": 2136,
                "version": 4,
                "description": "## Plan\n\nBody.\n\n---\n\nTail one.\n\n---\n\n" + ORIGINAL_MARKDOWN,
            },
            13: {"id": 13, "ref": 2010, "version": 1, "description": "## Plan\n\nNo rule here.\n"},
        }
        histories = {
            10: [{"created_date": "2026-01-01T00:00:00Z", "values_diff": {"description_html": [ORIGINAL_HTML, "<p>new</p>"]}}],
            13: [{"created_date": "2026-01-01T00:00:00Z", "values_diff": {"description_html": [ORIGINAL_HTML, "<p>new</p>"]}}],
        }
        client = FakeClient(details, histories)

        records = process_items(client, "userstories", [details[10], details[13]], threshold=0.9, dry_run=True)

        assert {record["ref"]: record["separators"] for record in records} == {2136: 2, 2010: 0}


class TestIsAnalyzed:
    def test_matches_the_analyzed_tag_in_taigas_name_and_colour_pair_shape(self):
        assert is_analyzed({"tags": [["needs-design", "#ccc"], ["llm-analyzed", None]]}) is True

    def test_matches_a_bare_string_tag(self):
        assert is_analyzed({"tags": ["llm-analyzed"]}) is True

    def test_returns_false_for_untagged_and_otherwise_tagged_items(self):
        assert is_analyzed({"tags": None}) is False
        assert is_analyzed({}) is False
        assert is_analyzed({"tags": [["llm-analyzed-later", "#ccc"]]}) is False


class TestBuildReport:
    def test_records_each_item_with_decision_counts(self):
        records = [
            {
                "ref": 2136,
                "kind": "userstories",
                "old_length": 900,
                "new_length": 400,
                "decision": "strip",
                "similarity": 0.97,
            },
            {
                "ref": 2100,
                "kind": "issues",
                "old_length": 500,
                "new_length": 500,
                "decision": "manual-review",
                "similarity": 0.42,
            },
            {
                "ref": 2050,
                "kind": "issues",
                "old_length": 300,
                "new_length": 300,
                "decision": "no-history",
                "similarity": None,
            },
        ]

        report = build_report(records, dry_run=True)

        assert report["dry_run"] is True
        assert report["summary"] == {
            "total": 3,
            "strip": 1,
            "manual-review": 1,
            "no-separator": 0,
            "no-history": 1,
            "skipped-version-conflict": 0,
            "repaired": 0,
            "no-change": 0,
        }
        assert [item["ref"] for item in report["items"]] == [2050, 2100, 2136]
        assert set(report["items"][0]) >= {
            "ref",
            "kind",
            "old_length",
            "new_length",
            "decision",
            "similarity",
        }


class TestWriteReport:
    """The apply run's report is a committed audit trail, so a dry run must not
    write over it."""

    def test_apply_run_writes_the_audit_trail_filenames(self, tmp_path):
        paths = write_report(build_report([], dry_run=False), tmp_path)

        assert paths["report_json"] == tmp_path / "report.json"
        assert paths["report_md"] == tmp_path / "report.md"
        assert "Mode: `apply`" in paths["report_md"].read_text()

    def test_dry_run_writes_separate_filenames(self, tmp_path):
        paths = write_report(build_report([], dry_run=True), tmp_path)

        assert paths["report_json"] == tmp_path / "report.dry-run.json"
        assert paths["report_md"] == tmp_path / "report.dry-run.md"

    def test_a_re_clean_run_writes_beside_the_strip_run_reports(self, tmp_path):
        """The two passes answer different questions, so neither may overwrite the
        other's record."""
        strip_paths = write_report(build_report([], dry_run=True), tmp_path)
        reclean_paths = write_report(
            build_report([], dry_run=True, mode=RE_CLEAN_MODE), tmp_path,
        )

        assert reclean_paths["report_json"] == tmp_path / "report.re-clean.dry-run.json"
        assert reclean_paths["report_md"] == tmp_path / "report.re-clean.dry-run.md"
        assert reclean_paths["report_json"] != strip_paths["report_json"]

    def test_a_re_clean_apply_run_keeps_its_own_audit_filenames(self, tmp_path):
        paths = write_report(build_report([], dry_run=False, mode=RE_CLEAN_MODE), tmp_path)

        assert paths["report_json"] == tmp_path / "report.re-clean.json"
        assert "Mode: `apply`" in paths["report_md"].read_text()

    def test_dry_run_leaves_an_existing_apply_report_untouched(self, tmp_path):
        apply_paths = write_report(build_report([], dry_run=False), tmp_path)
        apply_json = apply_paths["report_json"].read_text()
        apply_md = apply_paths["report_md"].read_text()

        write_report(build_report([], dry_run=True), tmp_path)

        assert apply_paths["report_json"].read_text() == apply_json
        assert apply_paths["report_md"].read_text() == apply_md


class TestExtractDescriptionChange:
    def test_returns_both_sides_of_the_newest_description_change(self):
        history = [
            {"created_date": "2026-01-01T00:00:00Z", "values_diff": {"description_html": ["<p>a</p>", "<p>b</p>"]}},
            {"created_date": "2026-03-01T00:00:00Z", "values_diff": {"description_html": ["<p>b</p>", "<p>c</p>"]}},
        ]

        assert extract_description_change(history) == ("<p>b</p>", "<p>c</p>")

    def test_returns_none_when_no_entry_changed_the_description(self):
        assert extract_description_change([{"values_diff": {"status": [1, 2]}}]) is None


# The pre-strip description as Taiga rendered it: plan, rule, then the original
# whose Acceptance Criteria the truncation destroyed.
PRE_STRIP_HTML = (
    '<h2 id="implementation-plan">Implementation Plan</h2>\n'
    '<h3 id="overview">Overview</h3>\n'
    "<p>Add the pledge endpoint.</p>\n"
    "<hr>\n"
    '<h2 id="acceptance-criteria">Acceptance Criteria</h2>\n'
    "<ul>\n"
    "<li>The host sees the pledge total update without a reload.</li>\n"
    "<li>A supporter cannot pledge a negative amount.</li>\n"
    "</ul>"
)

# What the truncating run left behind.
POST_STRIP_MARKDOWN = "## Implementation Plan\n\n### Overview\n\nAdd the pledge endpoint.\n"


class TestPlanReconstruction:
    def test_recovers_the_sections_the_truncation_deleted(self):
        plan = plan_reconstruction(POST_STRIP_MARKDOWN, PRE_STRIP_HTML, threshold=0.9)

        assert plan.decision == "repaired"
        assert plan.description.startswith(POST_STRIP_MARKDOWN.rstrip())
        assert "## Acceptance Criteria" in plan.description
        assert "- A supporter cannot pledge a negative amount." in plan.description

    def test_reports_no_change_when_the_removed_tail_was_a_true_duplicate(self):
        pre_strip = (
            '<h2 id="implementation-plan">Implementation Plan</h2>\n'
            '<h3 id="overview">Overview</h3>\n'
            "<p>Add the pledge endpoint.</p>\n"
            "<hr>\n"
            '<h2 id="overview_1">Overview</h2>\n'
            "<p>Add the pledge endpoint.</p>"
        )

        plan = plan_reconstruction(POST_STRIP_MARKDOWN, pre_strip, threshold=0.9)

        assert plan.decision == "no-change"
        assert plan.description == POST_STRIP_MARKDOWN

    def test_refuses_an_item_whose_prior_version_does_not_contain_the_current_text(self):
        """The newest change was an ordinary edit, not our strip — leave it alone."""
        plan = plan_reconstruction(
            "## Something Else Entirely\n\nUnrelated words about invoices.\n",
            PRE_STRIP_HTML,
            threshold=0.9,
        )

        assert plan.decision == "manual-review"
        assert plan.description is None
        assert plan.reason

    def test_refuses_a_prior_version_with_no_separator_to_reconstruct_from(self):
        plan = plan_reconstruction(
            POST_STRIP_MARKDOWN,
            '<h2 id="implementation-plan">Implementation Plan</h2>\n<h3 id="overview">Overview</h3>\n<p>Add the pledge endpoint.</p>',
            threshold=0.9,
        )

        assert plan.decision == "manual-review"
        assert plan.reason


class TestProcessRecleanItems:
    def _client(self, description, history_html):
        details = {10: {"id": 10, "ref": 2136, "version": 4, "description": description}}
        histories = {
            10: [
                {
                    "created_date": "2026-08-10T15:03:12Z",
                    "values_diff": {"description_html": list(history_html)},
                },
            ],
        }
        return FakeClient(details, histories)

    def test_apply_writes_the_recovered_description(self):
        client = self._client(POST_STRIP_MARKDOWN, (PRE_STRIP_HTML, "<p>ignored</p>"))

        records = process_reclean_items(
            client, "userstories", [client.details[10]], threshold=0.9, dry_run=False,
        )

        assert records[0]["decision"] == "repaired"
        assert len(client.patches) == 1
        assert "A supporter cannot pledge a negative amount." in client.patches[0][2]
        assert client.patches[0][3] == 4

    def test_dry_run_decides_without_writing(self):
        client = self._client(POST_STRIP_MARKDOWN, (PRE_STRIP_HTML, "<p>ignored</p>"))

        records = process_reclean_items(
            client, "userstories", [client.details[10]], threshold=0.9, dry_run=True,
        )

        assert records[0]["decision"] == "repaired"
        assert client.patches == []

    def test_an_item_with_no_description_history_is_left_alone(self):
        details = {10: {"id": 10, "ref": 2136, "version": 4, "description": POST_STRIP_MARKDOWN}}
        client = FakeClient(details, {10: [{"values_diff": {"status": [1, 2]}}]})

        records = process_reclean_items(
            client, "userstories", [details[10]], threshold=0.9, dry_run=False,
        )

        assert records[0]["decision"] == "no-history"
        assert client.patches == []

    def test_skips_an_item_whose_version_moved_since_the_decision(self):
        client = self._client(POST_STRIP_MARKDOWN, (PRE_STRIP_HTML, "<p>ignored</p>"))
        detail = client.details[10]
        reads = []

        def get_item_with_concurrent_edit(kind, item_id):
            reads.append(item_id)
            return detail if len(reads) == 1 else {**detail, "version": 5}

        client.get_item = get_item_with_concurrent_edit

        records = process_reclean_items(
            client, "userstories", [detail], threshold=0.9, dry_run=False,
        )

        assert records[0]["decision"] == "skipped-version-conflict"
        assert client.patches == []
        assert records[0]["new_length"] == records[0]["old_length"]


class RecordingCurl:
    """Stand-in for subprocess.run that records argv and stdin and replays canned replies."""

    def __init__(self, replies):
        self.replies = list(replies)
        self.calls = []

    def __call__(self, cmd, **kwargs):
        self.calls.append((cmd, kwargs.get("input")))
        return self.replies.pop(0)


def _client_with(monkeypatch, replies):
    curl = RecordingCurl([FakeCompletedProcess(0, stdout='{"auth_token": "tok.en"}')] + replies)
    monkeypatch.setattr(taiga_strip_duplicated_description.subprocess, "run", curl)
    monkeypatch.setattr(taiga_strip_duplicated_description.time, "sleep", lambda _: None)
    client = taiga_strip_duplicated_description.TaigaClient("https://taiga.example", "user", "s3cret")
    return client, curl


class TestSecretsStayOutOfArgv:
    def test_login_password_travels_on_stdin(self, monkeypatch):
        _, curl = _client_with(monkeypatch, [])

        cmd, stdin = curl.calls[0]
        assert "s3cret" not in " ".join(cmd)
        assert "s3cret" in stdin

    def test_bearer_token_travels_on_stdin_for_reads_and_patches(self, monkeypatch):
        client, curl = _client_with(monkeypatch, [
            FakeCompletedProcess(0, stdout='{"id": 1}'),
            FakeCompletedProcess(0, stdout='{"id": 1}'),
        ])

        client.get_item("userstories", 1)
        client.patch_description("userstories", 1, 'body with "quotes" \\ and\nnewline', 3)

        for cmd, stdin in curl.calls[1:]:
            assert "tok.en" not in " ".join(cmd)
            assert "tok.en" in stdin
        patch_cmd, patch_stdin = curl.calls[2]
        assert "quotes" not in " ".join(patch_cmd), "the description must not be in argv"
        assert patch_stdin.startswith('header = "Authorization: Bearer tok.en"\n')
        assert patch_stdin.count("\n") == 2, "the body must stay on one config line"


class TestListAllPaging:
    @staticmethod
    def _page(rows, status=200):
        return FakeCompletedProcess(0, stdout=json.dumps(rows) + f"\n{status}")

    def test_short_pages_do_not_end_the_scan(self, monkeypatch):
        """Taiga may serve 30 rows when 100 were asked for; keep paging."""
        client, curl = _client_with(monkeypatch, [
            self._page([{"id": n} for n in range(30)]),
            self._page([{"id": n} for n in range(30, 45)]),
            FakeCompletedProcess(0, stdout='{"_error_message": "Not found."}\n404'),
        ])

        items = client.list_all("/api/v1/userstories", project=39)

        assert [item["id"] for item in items] == list(range(45))
        assert len(curl.calls) == 4  # auth + 3 pages

    def test_an_empty_page_ends_the_scan(self, monkeypatch):
        client, _ = _client_with(monkeypatch, [self._page([{"id": 1}]), self._page([])])

        assert client.list_all("/api/v1/issues") == [{"id": 1}]

    def test_a_404_on_the_first_page_is_an_error(self, monkeypatch):
        client, _ = _client_with(monkeypatch, [
            FakeCompletedProcess(0, stdout='{"_error_message": "Not found."}\n404'),
        ])

        with pytest.raises(TaigaStripError):
            client.list_all("/api/v1/nope")

    def test_a_mid_scan_server_error_is_not_end_of_data(self, monkeypatch):
        client, _ = _client_with(monkeypatch, [
            self._page([{"id": 1}]),
            FakeCompletedProcess(0, stdout="boom\n500"),
        ])

        with pytest.raises(TaigaStripError):
            client.list_all("/api/v1/issues")
