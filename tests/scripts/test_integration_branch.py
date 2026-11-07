# ABOUTME: Pytest tests for the shared integration_branch module.
# ABOUTME: Verifies the epic/<epic-ref>-integration-<story-ref> naming convention.
import os
import sys

import pytest

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts", "lib"))

from integration_branch import integration_branch_name, parse_integration_branch


class TestIntegrationBranchName:
    """integration_branch_name builds the epic/<epic-ref>-integration-<story-ref> name."""

    def test_name_is_epic_integration_story(self):
        assert integration_branch_name(2004, 2748) == "epic/2004-integration-2748"


class TestParseIntegrationBranch:
    """parse_integration_branch round-trips and rejects non-matching branches."""

    def test_parse_round_trips(self):
        assert parse_integration_branch(integration_branch_name(2004, 2748)) == (2004, 2748)

    def test_parse_rejects_feature_branch(self):
        assert parse_integration_branch("feature/stacking-2747") is None

    @pytest.mark.parametrize(
        "branch",
        [
            "epic/2004-integration-2748-extra",
            "epic/integration-2748",
            None,
        ],
    )
    def test_parse_rejects_near_miss(self, branch):
        assert parse_integration_branch(branch) is None
