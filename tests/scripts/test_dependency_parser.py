# ABOUTME: Pytest tests for the shared dependency_parser module.
# ABOUTME: Verifies parsing of dependency diagrams from epic description markdown.
import os
import sys

import pytest

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts", "lib"))

from dependency_parser import (
    find_dependency_cycle,
    parse_dependency_diagram,
    resolve_out_of_run_dependencies,
    split_ref,
)

FIXTURES_DIR = os.path.join(SCRIPT_DIR, "fixtures")


class TestParseSimpleDiagram:
    """Linear dependency chain parsing."""

    def test_linear_chain(self):
        """A -> B -> C produces correct deps."""
        description = """\
## Dependency Diagram

```
#100 (Foundation)
  └─► #200 (Depends on 100)
        └─► #300 (Depends on 200)
```
"""
        deps = parse_dependency_diagram(description)
        assert deps.get(100, []) == []
        assert 100 in deps[200]
        assert 200 in deps[300]
        assert 100 not in deps[300]


class TestParseBranchingDeps:
    """One parent, multiple children."""

    def test_one_parent_multiple_children(self):
        """A -> B, C, D produces correct deps."""
        description = """\
## Dependency Diagram

```
#100 (Base)
  └─► #200 (Child 1)
  └─► #300 (Child 2)
  └─► #400 (Child 3)
```
"""
        deps = parse_dependency_diagram(description)
        assert deps.get(100, []) == []
        assert 100 in deps[200]
        assert 100 in deps[300]
        assert 100 in deps[400]

    def test_comma_separated_children(self):
        """A -> B, C, D as comma-separated refs."""
        description = """\
## Dependency Diagram

```
#100 (Base)
  └─► #200, #300, #400 (consumers)
```
"""
        deps = parse_dependency_diagram(description)
        assert 100 in deps[200]
        assert 100 in deps[300]
        assert 100 in deps[400]


class TestParseNoDepsSection:
    """Returns empty dict when no dependency section exists."""

    def test_no_section(self):
        description = "## Goal\n\nJust a regular epic.\n\n## Scope\n\nSome scope."
        assert parse_dependency_diagram(description) == {}

    def test_no_code_block(self):
        description = "## Dependency Diagram\n\nNo code block here.\n\n## Next Section"
        assert parse_dependency_diagram(description) == {}

    def test_empty_description(self):
        assert parse_dependency_diagram("") == {}

    def test_none_description(self):
        assert parse_dependency_diagram(None) == {}


class TestCircularDependencyDetection:
    """Circular dependencies are detected and return empty dict."""

    def test_no_circular_deps_in_well_formed_diagram(self):
        """Well-formed diagrams do not trigger circular detection
        (that logic lives in StateManager, not the parser itself)."""
        description = """\
## Dependency Diagram

```
#100 (Base)
  └─► #200 (Child)
```
"""
        deps = parse_dependency_diagram(description)
        assert deps == {100: [], 200: [100]}


class TestAlternativeHeading:
    """Also handles ## Dependencies heading."""

    def test_dependencies_heading(self):
        description = """\
## Dependencies

```
#100 (Base)
  └─► #200 (Consumer)
```
"""
        deps = parse_dependency_diagram(description)
        assert 200 in deps
        assert 100 in deps[200]


class TestDependencyGraphHeading:
    """## Dependency Graph heading is recognized alongside Diagram/Dependencies."""

    def test_dependency_graph_heading(self):
        description = """\
## Dependency Graph

```
#100 (Base)
  └─► #200 (Consumer)
```
"""
        deps = parse_dependency_diagram(description)
        assert 200 in deps
        assert 100 in deps[200]


class TestNonMatchingHeadingIgnored:
    """Headings that look similar but don't match the parser's expected
    headings are silently ignored."""

    def test_dependency_tree_for_claude_heading(self):
        description = """\
## Dependency Tree For Claude

```
#100 (Base)
  └─► #200 (Consumer)
```
"""
        assert parse_dependency_diagram(description) == {}


class TestForkArrow:
    """├─► arrows are parsed identically to └─► arrows."""

    def test_fork_arrow_parsed(self):
        description = """\
## Dependency Diagram

```
#100 (Base)
  ├─► #200 (Child 1)
  └─► #300 (Child 2)
```
"""
        deps = parse_dependency_diagram(description)
        assert 100 in deps
        assert deps[100] == []
        assert 100 in deps[200]
        assert 100 in deps[300]

    def test_mixed_arrows_nested(self):
        description = """\
## Dependency Diagram

```
#100 (Root)
  ├─► #200 (Child 1)
  │     └─► #300 (Grandchild)
  └─► #400 (Child 2)
```
"""
        deps = parse_dependency_diagram(description)
        assert 100 in deps
        assert deps[100] == []
        assert 100 in deps[200]
        assert 200 in deps[300]
        assert 100 in deps[400]


class TestRootNodesHaveNoDeps:
    """Root nodes have empty dependency lists."""

    def test_root_has_empty_deps(self):
        description = """\
## Dependency Diagram

```
#100 (Base)
  └─► #200 (Consumer)
```
"""
        deps = parse_dependency_diagram(description)
        assert deps.get(100, []) == []


class TestAnnotationRefsAreNotEdges:
    """Refs in trailing annotation prose are not treated as graph nodes/edges."""

    def test_trailing_annotation_ref_is_not_a_node(self):
        """A ref inside trailing prose does not become a node or edge."""
        description = """\
## Dependency Diagram

```
#100 (Base)
  └─► #200 Some story (also needs #300)
```
"""
        deps = parse_dependency_diagram(description)
        assert deps[200] == [100]
        assert 300 not in deps

    def test_annotation_ref_is_not_pushed_as_parent(self):
        """A deeper child's parent is the node ref, not an annotation ref."""
        description = """\
## Dependency Diagram

```
#100 (Base)
  └─► #200 Some story (also needs #300)
        └─► #400 (Deeper child)
```
"""
        deps = parse_dependency_diagram(description)
        assert deps[400] == [200]


class TestContinuationLines:
    """Ref-bearing lines that aren't arrow children or leading-ref roots are ignored."""

    def test_continuation_line_does_not_reset_tree(self):
        """A wrapped continuation line does not clear the ancestry stack."""
        description = """\
## Dependency Diagram

```
#100 (Base)
  └─► #400 (Child)
        │      [needs #200 + #300]
        ├─► #500 (Grandchild)
```
"""
        deps = parse_dependency_diagram(description)
        assert deps[500] == [400]

    def test_bracketed_note_with_ref_creates_no_node(self):
        """A standalone bracketed note with a ref creates no node."""
        description = """\
## Dependency Diagram

```
#100 (Base)
  └─► #200 (Child)
[BLOCKED: see #999]
```
"""
        deps = parse_dependency_diagram(description)
        assert 999 not in deps


class TestSiblingRefRuns:
    """Comma- and plus-separated leading refs are siblings sharing a parent."""

    def test_comma_and_plus_separated_siblings(self):
        description = """\
## Dependency Diagram

```
#100 (Base)
  └─► #200, #300 + #400 (consumers)
```
"""
        deps = parse_dependency_diagram(description)
        assert deps[200] == [100]
        assert deps[300] == [100]
        assert deps[400] == [100]

    def test_child_of_multi_ref_root_depends_on_all_refs(self):
        description = """\
## Dependency Diagram

```
#100, #200 + #300 (Base)
  └─► #400 (child)
```
"""
        deps = parse_dependency_diagram(description)
        assert deps[400] == [100, 200, 300]
        assert deps[100] == []
        assert deps[200] == []
        assert deps[300] == []

    def test_child_of_multi_ref_arrow_line_depends_on_all_refs(self):
        description = """\
## Dependency Diagram

```
#100 (Base)
  └─► #200, #300 (siblings)
        └─► #400 (grandchild)
```
"""
        deps = parse_dependency_diagram(description)
        assert deps[400] == [200, 300]
        assert deps[200] == [100]
        assert deps[300] == [100]

    def test_epic_2280_shape(self):
        description = """\
## Dependency Diagram

```
#2292, #2293, #2294, #2506, #2508, #2510
  └─► #2540 (coverage audit)
```
"""
        deps = parse_dependency_diagram(description)
        assert deps[2540] == [2292, 2293, 2294, 2506, 2508, 2510]


class TestRootDetection:
    """A line is only a root if it starts with a ref; otherwise it's ignored."""

    def test_line_starting_with_prose_is_ignored(self):
        description = """\
## Dependency Diagram

```
Note: #100 and #200 are independent
```
"""
        deps = parse_dependency_diagram(description)
        assert 100 not in deps
        assert 200 not in deps

    def test_second_root_still_starts_a_new_tree(self):
        description = """\
## Dependency Diagram

```
#100 (Base)
  └─► #200 (Child)
#500 (Second root)
  └─► #600 (Its child)
```
"""
        deps = parse_dependency_diagram(description)
        assert deps[200] == [100]
        assert deps[600] == [500]


class TestEpic2167Regression:
    """Regression test for epic #2167 v7 — annotations, continuation lines, and cycles."""

    def test_v7_diagram_parses_to_intended_dag(self):
        description = """\
## Dependency Diagram

```
#2168  Planning umbrella — model, config, evaluation point, payout rail
  │
  ├─► #2209  Challenge fee amount: set event, change event, fee on the challenge API
  │     │      [no deps — foundation; every challenge records its own fee in cents]
  │     └─► #2206  Pre-publish fee-share motivation  (also needs #2170)
  │
  └─► #2169  Fee-share schedule + tranche evaluator (Shared domain)   [no deps]
        │
        ├─► #2170  Snapshot the schedule onto the challenge stream at fee payment
        │     ├─► #2204  Host-facing fee-share tranche display and gamification
        │     └─► #2206  Pre-publish fee-share motivation  (also needs #2209)
        │
        └─► #2171  CharityPayable aggregate: accrual, month-end close, paid lifecycle
              └─► #2172  Payables projection: line-item table, handler, balance queries
                    └─► #2173  Accrue the earned fee share at challenge finalization
                          │      [needs #2170 + #2171 + #2172]
                          ├─► #2174  Month-end close scan for open charity payables
                          │     └─► #2175  Admin payables API: statements + record remittance
                          │           │      [BLOCKED: payout-rail decision (#2168 §6)
                          │           │       + must establish the admin auth boundary itself]
                          │           └─► #2205  Payables admin UI
                          ├─► #2176  Refund-after-accrual invariant guard
                          └─► #2204  Host-facing tranche display  [reads #2170 + #2173 data]
```
"""
        deps = parse_dependency_diagram(description)
        assert deps == {
            2168: [],
            2169: [2168],
            2170: [2169],
            2171: [2169],
            2172: [2171],
            2173: [2172],
            2174: [2173],
            2175: [2174],
            2176: [2173],
            2204: [2170, 2173],
            2205: [2175],
            2206: [2170, 2209],
            2209: [2168],
        }
        assert find_dependency_cycle(deps) is None


class TestMultiParentStory:
    """A story hanging under two distinct roots unions both parents' deps."""

    def test_story_under_two_roots_unions_both_parents(self):
        description = """\
## Dependency Diagram

```
#2747 (Root)
  └─► #2748 (Depends on 2747 and 2750)
#2750 (Root)
  └─► #2748 (Depends on 2747 and 2750)
```
"""
        deps = parse_dependency_diagram(description)
        assert deps[2748] == [2747, 2750]


class TestFindDependencyCycle:
    """find_dependency_cycle detects cycles and ignores out-of-map deps."""

    def test_detects_cycle(self):
        deps = {2170: [2169, 2209], 2209: [2168, 2170], 2168: [], 2169: [2168]}
        cycle = find_dependency_cycle(deps)
        assert cycle is not None
        assert cycle[0] == cycle[-1]
        assert {2170, 2209}.issubset(set(cycle))

    def test_acyclic_returns_none(self):
        deps = {100: [], 200: [100]}
        assert find_dependency_cycle(deps) is None

    def test_ignores_deps_outside_the_map(self):
        deps = {1: [999]}
        assert find_dependency_cycle(deps) is None


class TestResolveOutOfRunDependencies:
    """Out-of-run dependency classification by merged (Done) status."""

    def test_done_out_of_run_dep_is_dropped(self):
        """A prerequisite verified Done is safe to drop from the run's deps."""
        run_deps, excluded = resolve_out_of_run_dependencies(
            {200: [100]}, {200}, {100}
        )
        assert run_deps == {200: []}
        assert excluded == {}

    def test_unfinished_out_of_run_dep_excludes_story(self):
        """An unfinished prerequisite keeps the dependent out of the run."""
        run_deps, excluded = resolve_out_of_run_dependencies(
            {200: [100]}, {200}, set()
        )
        assert excluded == {200: 100}
        assert 200 not in run_deps

    def test_in_run_dep_is_kept(self):
        """A dependency on a story in this run stays in the deps map."""
        run_deps, excluded = resolve_out_of_run_dependencies(
            {200: [100]}, {100, 200}, set()
        )
        assert run_deps[200] == [100]
        assert excluded == {}

    def test_exclusion_cascades_to_in_run_dependents(self):
        """Excluding a story excludes the in-run stories that depend on it."""
        run_deps, excluded = resolve_out_of_run_dependencies(
            {200: [100], 300: [200]}, {200, 300}, set()
        )
        assert excluded == {200: 100, 300: 200}
        assert run_deps == {}

    def test_mixed_done_and_unfinished_parents(self):
        """The first not-Done out-of-run parent, in diagram order, blocks."""
        run_deps, excluded = resolve_out_of_run_dependencies(
            {300: [100, 200]}, {300}, {100}
        )
        assert excluded == {300: 200}

    def test_eligible_story_absent_from_map_gets_empty_deps(self):
        """An eligible story with no diagram entry runs with no deps."""
        run_deps, excluded = resolve_out_of_run_dependencies({}, {500}, set())
        assert run_deps == {500: []}
        assert excluded == {}

    def test_unrelated_stories_unaffected(self):
        """One story's exclusion leaves independent stories in the run."""
        run_deps, excluded = resolve_out_of_run_dependencies(
            {200: [100]}, {200, 400}, set()
        )
        assert excluded == {200: 100}
        assert run_deps == {400: []}


class TestNamespacedMode:
    """default_namespace= opts into str `ns#N` keys/deps for cross-project diagrams."""

    def test_namespaced_basics(self):
        """A namespaced ref and a bare ref (using the default namespace) form an edge."""
        description = """\
## Dependency Diagram

```
homelab#100 (Base)
  └─► alterna#200 (Consumer)
```
"""
        deps = parse_dependency_diagram(description, default_namespace="homelab")
        assert deps["homelab#100"] == []
        assert deps["alterna#200"] == ["homelab#100"]

    def test_bare_ref_uses_default_namespace_and_does_not_collide(self):
        """A bare `#N` takes the default namespace and stays distinct from
        another namespace's ref that happens to share the same number."""
        description = """\
## Dependency Diagram

```
#100 (Homelab base, bare ref)
  └─► alterna#100 (Different namespace, same number)
```
"""
        deps = parse_dependency_diagram(description, default_namespace="homelab")
        assert deps["homelab#100"] == []
        assert deps["alterna#100"] == ["homelab#100"]

    def test_sort_key_is_namespace_then_int_not_lexicographic(self):
        """Dep lists sort by (namespace, int), not lexicographically on the string."""
        description = """\
## Dependency Diagram

```
alterna#4 (Root A)
  └─► alterna#900 (Child)
alterna#33 (Root B)
  └─► alterna#900 (Child)
alterna#330 (Root C)
  └─► alterna#900 (Child)
```
"""
        deps = parse_dependency_diagram(description, default_namespace="homelab")
        # Lexicographic string sort would give ["alterna#33", "alterna#330", "alterna#4"].
        assert deps["alterna#900"] == ["alterna#4", "alterna#33", "alterna#330"]

    def test_empty_and_none_description_return_empty_dict(self):
        assert parse_dependency_diagram(None, default_namespace="homelab") == {}
        assert parse_dependency_diagram("", default_namespace="homelab") == {}

    def test_fixture_parses_to_documented_graph_with_str_types(self):
        """The seeded cross-project fixture parses to the documented graph.

        This is the roadmap-graph proof: the same fixture text destined for
        homelab wiki page 1 (`cross-project-execution-order`) must, parsed
        with `default_namespace="homelab"`, produce the exact edge set the
        story's Implementation Plan documents.
        """
        fixture_path = os.path.join(
            FIXTURES_DIR, "cross-project-execution-order-diagram.md"
        )
        with open(fixture_path, encoding="utf-8") as f:
            text = f.read()

        deps = parse_dependency_diagram(text, default_namespace="homelab")

        # Namespaced-mode type invariant: every key and every dep-list
        # element is a str -- never mixed with legacy ints.
        for key, values in deps.items():
            assert type(key) is str, f"non-str key: {key!r}"
            for value in values:
                assert type(value) is str, f"non-str dep: {value!r}"

        expected = {
            "alterna#1": ["gg#1"],
            "alterna#17": ["gg#3", "gg#4"],
            "alterna#190": [],
            "alterna#191": [],
            "alterna#328": [],
            "gg#1": [],
            "gg#3": [],
            "gg#4": [],
            "homelab#20": [
                "alterna#328",
                "gg#3",
                "gg#4",
                "homelab#28",
            ],
            "homelab#27": ["alterna#328"],
            "homelab#28": [],
            "homelab#106": ["alterna#328"],
            "homelab#164": ["alterna#190", "alterna#191"],
            "homelab#258": ["alterna#328"],
            "homelab#367": ["alterna#328"],
            "alterna#329": [],
            "alterna#330": ["alterna#329", "homelab#380"],
            "alterna#331": ["homelab#368"],
            "alterna#332": ["alterna#331"],
            "alterna#333": ["alterna#332"],
            "alterna#334": ["alterna#332"],
            "alterna#335": ["alterna#329"],
            "alterna#347": ["alterna#329"],
            "homelab#368": [
                "alterna#330",
                "alterna#335",
                "alterna#347",
                "homelab#369",
                "homelab#380",
            ],
            "homelab#369": ["alterna#329"],
            "homelab#380": ["alterna#329"],
            "alterna#183": ["alterna#413", "alterna#414", "homelab#178"],
            "alterna#406": ["alterna#183"],
            "alterna#412": [],
            "alterna#413": ["alterna#412"],
            "alterna#414": ["alterna#412"],
            "homelab#173": [],
            "homelab#174": ["homelab#173"],
            "homelab#175": ["homelab#174"],
            "homelab#177": ["homelab#175"],
            "homelab#178": ["homelab#177"],
        }
        assert deps == expected

    def test_annotation_ref_ignored_in_namespaced_mode(self):
        """Trailing annotation refs are ignored in namespaced mode too."""
        description = """\
## Dependency Diagram

```
homelab#100 (Base)
  └─► alterna#200 Some story (also needs alterna#300)
```
"""
        deps = parse_dependency_diagram(description, default_namespace="homelab")
        assert deps["alterna#200"] == ["homelab#100"]
        assert "alterna#300" not in deps


class TestSplitRef:
    """split_ref maps a namespaced ref back to (namespace, int)."""

    @pytest.mark.parametrize(
        "ref, expected",
        [
            ("alterna#330", ("alterna", 330)),
            ("homelab#1", ("homelab", 1)),
            ("cholobots#24", ("cholobots", 24)),
            ("gg#3", ("gg", 3)),
        ],
    )
    def test_split_ref(self, ref, expected):
        assert split_ref(ref) == expected


class TestLegacyModeUnchanged:
    """Legacy one-argument mode (no default_namespace) stays int-keyed, even
    when run against text that also contains namespaced refs."""

    def test_legacy_call_on_cross_project_fixture_stays_int_keyed(self):
        fixture_path = os.path.join(
            FIXTURES_DIR, "cross-project-execution-order-diagram.md"
        )
        with open(fixture_path, encoding="utf-8") as f:
            text = f.read()

        deps = parse_dependency_diagram(text)

        assert deps  # bare `#N` refs in the fixture are still recognized
        for key, values in deps.items():
            assert type(key) is int, f"non-int key: {key!r}"
            for value in values:
                assert type(value) is int, f"non-int dep: {value!r}"
