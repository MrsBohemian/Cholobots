# ABOUTME: Chooses the base branch for a story from the epic dependency graph.
# ABOUTME: Shared by the /implement-epic skill, whose parallel slots are git worktrees.


class UnresolvedDependencyError(Exception):
    """A dependency has no usable branch recorded, so the story cannot be based."""


class MultiParentDependencyError(Exception):
    """A story with 2+ dependencies was passed in; its base is an integration
    branch built by the caller (integration_branch.py), never one parent's branch."""


def select_base_branch(story_ref, dependencies, story_branches, default_base="main"):
    """Return (base_branch, reason) for story_ref.

    dependencies   -- refs this story depends on, from parse_dependency_diagram()
    story_branches -- ref -> branch name, populated only for stories that reached
                      `complete` or `handed_off` (both mean the branch exists and is pushed).
                      Keys may be ints or the strings a JSON round-trip produces; both
                      spellings resolve against the int refs the dependency parser yields.
    default_base   -- base for a story with no dependencies

    Raises MultiParentDependencyError when given 2+ dependencies -- the caller
    builds an integration branch instead.

    Raises UnresolvedDependencyError when a dependency has no non-empty branch recorded.
    The caller treats that as "do not run this story" and emits
    SKIPPED:<ref>:dependency_failed:<dep_ref>.
    """
    if not dependencies:
        return default_base, "no dependencies"

    if len(dependencies) > 1:
        raise MultiParentDependencyError(
            f"story #{story_ref} has {len(dependencies)} dependencies; its base is an "
            "integration branch (integration_branch_name), not select_base_branch's to choose"
        )

    # A state file read back through JSON has string keys, while the dependency
    # parser yields int refs; index by str so either spelling resolves.
    branches = {str(ref): branch for ref, branch in story_branches.items()}

    for dep in dependencies:
        if not branches.get(str(dep)):
            raise UnresolvedDependencyError(
                f"story #{story_ref} depends on #{dep}, which has no usable branch recorded"
            )

    dep = dependencies[0]
    return branches[str(dep)], f"single dependency #{dep}"
