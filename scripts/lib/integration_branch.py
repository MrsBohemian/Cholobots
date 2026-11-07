# ABOUTME: Shared integration-branch naming helper for multi-parent stories.
# ABOUTME: Defines the epic/<epic-ref>-integration-<story-ref> naming convention.
import re

INTEGRATION_BRANCH_RE = re.compile(r"^epic/(\d+)-integration-(\d+)$")


def integration_branch_name(epic_ref, story_ref):
    """Branch that merges every parent of a multi-parent story."""
    return f"epic/{epic_ref}-integration-{story_ref}"


def parse_integration_branch(branch):
    """Return (epic_ref, story_ref) as ints, or None if not an integration branch."""
    if not branch:
        return None
    m = INTEGRATION_BRANCH_RE.match(branch)
    return (int(m.group(1)), int(m.group(2))) if m else None
