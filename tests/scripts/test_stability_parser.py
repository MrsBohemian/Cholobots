# ABOUTME: Pytest tests for the shared stability_parser module.
# ABOUTME: Verifies parsing of monitor-pr-stability.sh output into structured dicts.
import os
import sys

import pytest

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts", "lib"))

from stability_parser import parse_stability_result


class TestParseStable:
    """Parses STABLE output correctly."""

    def test_parse_stable(self):
        result = parse_stability_result("STABLE")
        assert result["status"] == "stable"
        assert result["failing_checks"] == []

    def test_parse_stable_with_whitespace(self):
        result = parse_stability_result("  STABLE  \n")
        assert result["status"] == "stable"
        assert result["failing_checks"] == []


class TestParseChecksFailing:
    """Parses CHECKS_FAILING:<checks> output correctly."""

    def test_parse_checks_failing(self):
        result = parse_stability_result("CHECKS_FAILING:build,test")
        assert result["status"] == "checks_failing"
        assert result["failing_checks"] == ["build", "test"]

    def test_parse_single_check_failing(self):
        result = parse_stability_result("CHECKS_FAILING:build")
        assert result["status"] == "checks_failing"
        assert result["failing_checks"] == ["build"]

    def test_parse_checks_failing_with_spaces(self):
        result = parse_stability_result("CHECKS_FAILING: build , test ")
        assert result["status"] == "checks_failing"
        assert result["failing_checks"] == ["build", "test"]


class TestParseCommentsFound:
    """Parses COMMENTS_FOUND:<count> output correctly."""

    def test_parse_comments_found(self):
        result = parse_stability_result("COMMENTS_FOUND:3")
        assert result["status"] == "comments_found"
        assert result["comment_count"] == 3
        assert result["failing_checks"] == []

    def test_parse_comments_found_zero(self):
        result = parse_stability_result("COMMENTS_FOUND:0")
        assert result["status"] == "comments_found"
        assert result["comment_count"] == 0


class TestParseNeedsRebase:
    """Parses NEEDS_REBASE output correctly."""

    def test_parse_needs_rebase(self):
        result = parse_stability_result("NEEDS_REBASE")
        assert result["status"] == "needs_rebase"
        assert result["failing_checks"] == []

    def test_parse_needs_rebase_with_whitespace(self):
        result = parse_stability_result("  NEEDS_REBASE  \n")
        assert result["status"] == "needs_rebase"
        assert result["failing_checks"] == []


class TestParsePending:
    """Parses PENDING output correctly."""

    def test_pending(self):
        result = parse_stability_result("PENDING")
        assert result["status"] == "pending"
        assert result["failing_checks"] == []


class TestParseUnknown:
    """Handles unexpected output gracefully."""

    def test_parse_unknown(self):
        result = parse_stability_result("SOMETHING_UNEXPECTED")
        assert result["status"] == "unknown"
        assert result["raw"] == "SOMETHING_UNEXPECTED"
        assert result["failing_checks"] == []

    def test_parse_empty_string(self):
        result = parse_stability_result("")
        assert result["status"] == "unknown"
        assert result["raw"] == ""
        assert result["failing_checks"] == []
