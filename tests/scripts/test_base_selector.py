# ABOUTME: Pytest tests for the shared base_selector module.
# ABOUTME: Verifies DAG-aware base branch selection from a story's dependency list.
import json
import os
import sys

import pytest

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts", "lib"))

from base_selector import (
    MultiParentDependencyError,
    UnresolvedDependencyError,
    select_base_branch,
)


def test_no_dependencies_bases_on_main():
    base, reason = select_base_branch(300, [], {})
    assert base == "main"
    assert reason


def test_no_dependencies_honours_default_base():
    base, reason = select_base_branch(300, [], {}, default_base="develop")
    assert base == "develop"
    assert reason


def test_single_dependency_uses_parent_branch():
    base, reason = select_base_branch(300, [100], {100: "feature/foo-100"})
    assert base == "feature/foo-100"
    assert "#100" in reason


def test_multi_parent_raises():
    story_branches = {100: "feature/foo-100", 200: "feature/bar-200"}

    with pytest.raises(MultiParentDependencyError) as exc_info:
        select_base_branch(300, [100, 200], story_branches)

    message = str(exc_info.value)
    assert "300" in message
    assert "integration" in message


def test_multi_parent_raises_even_with_unrecorded_parents():
    with pytest.raises(MultiParentDependencyError):
        select_base_branch(300, [100, 200], {})


def test_multi_parent_error_is_not_an_unresolved_dependency_error():
    assert not issubclass(MultiParentDependencyError, UnresolvedDependencyError)


def test_unrecorded_dependency_raises():
    with pytest.raises(UnresolvedDependencyError) as exc_info:
        select_base_branch(300, [100], {})
    message = str(exc_info.value)
    assert "300" in message
    assert "100" in message


@pytest.mark.parametrize("blank_value", ["", None])
def test_blank_recorded_branch_raises(blank_value):
    with pytest.raises(UnresolvedDependencyError):
        select_base_branch(300, [100], {100: blank_value})


def test_string_keyed_story_branches_resolves_single_dependency():
    story_branches = json.loads(json.dumps({100: "feature/foo-100"}))

    base, reason = select_base_branch(300, [100], story_branches)

    assert base == "feature/foo-100"
    assert "#100" in reason


def test_string_keyed_blank_branch_still_raises():
    story_branches = json.loads(json.dumps({100: ""}))

    with pytest.raises(UnresolvedDependencyError):
        select_base_branch(300, [100], story_branches)
