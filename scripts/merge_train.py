# ABOUTME: Deterministic merge-train script that drives sequential PR rebase, CI-validate, and merge.
# ABOUTME: Handles argument parsing, state management, PR discovery, dependency ordering, and summary reporting.
"""
Usage: python3 merge_train.py <PR numbers... | epic:<ref>> [flags]

Modes:
  <PR numbers>       Space-separated PR numbers to process
  epic:<ref>         Discover PRs from the stories named in an epic's dependency diagram

Flags:
  --dry-run          Show what would be merged without acting
  --target <branch>  Target branch for rebase (default: main)
  --resume <path>    Resume from an existing state file

Exit codes:
  0   All PRs processed (or --dry-run complete)
  10  Conflicts need agent resolution (action-request JSON written)
  11  Remediation needed (action-request JSON written)
  1   Unexpected error

Environment:
  GH_REPO              GitHub repo for every gh call (default: MrsBohemian/Cholobots);
                       gh always runs as `env -u GH_TOKEN gh ... --repo $GH_REPO`
  MERGE_TRAIN_REMOTE   git remote that holds PR branches and the target (default: upstream)

Post-rebase check: `python -m py_compile cholobots.py config.py db/database.py bots/*.py`,
then `python -m pytest -q tests --ignore=tests/scripts` once a unit-test suite exists (#34).
After a merge into main the mirror is synced with `git push origin main` (failure ignored).
"""

import glob
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone

# Add shared lib to path for dependency_parser import
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "lib"))
from dependency_parser import parse_dependency_diagram
from integration_branch import parse_integration_branch
from private_files import write_json_private
from stability_parser import parse_stability_result


# -- Repository --
# Pipeline branches and PRs live on MrsBohemian/Cholobots, reached through the
# `upstream` git remote. The shell's GH_TOKEN cannot see that repo, so every gh
# call drops it (env -u GH_TOKEN) and names the repo explicitly.
GH_REPO = os.environ.get("GH_REPO", "MrsBohemian/Cholobots")
REMOTE = os.environ.get("MERGE_TRAIN_REMOTE", "upstream")

# Post-rebase checks. Cholobots has no build step: the compile check always runs,
# and pytest runs once a suite exists outside the helper-script tests (story #34).
# gh pr list stops silently at --limit; discovery treats a full page as truncation.
EPIC_PR_LIST_LIMIT = 1000
# Upper bound for the all-states PR listing used to verify an integration PR's parents.
PARENT_PR_LIST_LIMIT = 1000
COMPILE_CHECK_CMD = "python -m py_compile cholobots.py config.py db/database.py bots/*.py"
UNIT_TEST_CMD = "python -m pytest -q tests --ignore=tests/scripts"

# -- Exit codes --
EXIT_OK = 0
EXIT_CONFLICTS = 10
EXIT_REMEDIATION = 11
EXIT_ERROR = 1

# -- Auto-merge polling (module-level so tests can monkeypatch) --
MERGE_POLL_INTERVAL_SECONDS = 30
MERGE_POLL_TIMEOUT_SECONDS = 1800

# Exit code scripts/taiga-story.sh uses for a by_ref 404 (item not found).
TAIGA_NOT_FOUND_EXIT = 44

# `gh pr list` caps results at --limit rather than paginating past it, so a
# result of exactly this size may be truncated. find_open_children() fails
# closed on that rather than reasoning about a child set it cannot see.
OPEN_CHILDREN_LIMIT = 100


class ParsedArgs:
    """Holds parsed command-line arguments."""

    def __init__(self):
        self.mode = None  # "explicit", "epic", "resume"
        self.pr_numbers = []
        self.epic_ref = None
        self.dry_run = False
        self.target = "main"
        self.resume_path = None


def parse_args(argv):
    """Parse command-line arguments into a ParsedArgs object.

    Supports three modes:
    - explicit: space-separated PR numbers
    - epic: epic:<ref> to discover PRs from an epic
    - resume: --resume <path> to continue from a state file
    """
    args = ParsedArgs()
    positional = []
    i = 0

    while i < len(argv):
        arg = argv[i]
        if arg == "--dry-run":
            args.dry_run = True
        elif arg == "--target":
            i += 1
            if i < len(argv):
                args.target = argv[i]
        elif arg == "--resume":
            i += 1
            if i < len(argv):
                args.resume_path = argv[i]
                args.mode = "resume"
        else:
            positional.append(arg)
        i += 1

    # --resume takes priority over everything
    if args.mode == "resume":
        return args

    # Check for epic:<ref> mode
    for p in positional:
        epic_match = re.match(r"^epic:(\d+)$", p)
        if epic_match:
            args.mode = "epic"
            args.epic_ref = int(epic_match.group(1))
            return args

    # Otherwise, treat positional args as PR numbers
    args.mode = "explicit"
    args.pr_numbers = [int(p) for p in positional if p.isdigit()]

    return args


class StateManager:
    """Manages the merge-train state file with atomic writes."""

    def __init__(self, state_path):
        self.state_path = state_path
        self._state = None

    @property
    def state(self):
        return self._state

    def init(self, prs, target="main"):
        """Initialize a new state file from a list of PR dicts.

        Each PR dict must have: number, title, branch. body and base are optional.
        """
        self._state = {
            "startedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "target": target,
            "prs": [
                {
                    "number": pr["number"],
                    "title": pr["title"],
                    "branch": pr["branch"],
                    "body": pr.get("body", ""),
                    "status": "queued",
                    "error": None,
                    "base": pr.get("base") or target,
                    "stacked": bool(pr.get("base")) and pr["base"] != target,
                    "sweptChildren": [],
                }
                for pr in prs
            ],
            "currentIndex": 0,
            "summary": {"merged": 0, "skipped": 0, "failed": 0},
            "mergedBranches": [],
        }
        self._save()

    def load(self):
        """Load state from an existing file."""
        with open(self.state_path) as f:
            self._state = json.load(f)
        # A state file may omit mergedBranches; default it. Per-PR keys need no
        # equivalent here: every reader of sweptChildren defaults it in place.
        self._state.setdefault("mergedBranches", [])
        return self._state

    def index_of(self, number):
        """Queue index of the PR numbered ``number``, or None when not queued."""
        for index, queued in enumerate(self._state["prs"]):
            if queued["number"] == number:
                return index
        return None

    def update_pr(self, index, status=None, error=None, base=None, stacked=None):
        """Update a PR's status, error, base and/or stacked flag, then persist."""
        if status is not None:
            self._state["prs"][index]["status"] = status
        if error is not None:
            self._state["prs"][index]["error"] = error
        if base is not None:
            self._state["prs"][index]["base"] = base
        if stacked is not None:
            self._state["prs"][index]["stacked"] = stacked
        self._save()

    def record_merged_branch(self, branch):
        """Remember that a branch has landed, so stacked children can retarget.

        Only a MERGED PR's branch belongs here. A closed-unmerged PR's branch
        looks identical from the remote, and retargeting a child past it would
        land abandoned commits inside the child's diff.
        """
        if branch and branch not in self._state.setdefault("mergedBranches", []):
            self._state["mergedBranches"].append(branch)
            self._save()

    def add_swept_child(self, index, child):
        """Record that ``child`` was retargeted onto the train's target ahead of
        this PR's merge.

        The sweep loop in process_pr() builds this list in a local variable too,
        but a local variable does not survive a crash or a remediation timeout.
        On resume, ``find_open_children(branch)`` no longer finds a child already
        moved to target (its base is no longer ``branch``), so without this
        persisted record a resumed run would believe it swept nothing and skip
        restoring the child if this PR then fails to land.
        """
        swept = self._state["prs"][index].setdefault("sweptChildren", [])
        if child not in swept:
            swept.append(child)
            self._save()

    def clear_swept_children(self, index):
        """Drop the persisted swept-children record once it is no longer needed:
        either the children were restored, or this PR merged and they are
        correctly based on target already.

        Callers that are about to record a terminal status (failed/skipped/
        merged) for a parent whose restore left unresolvable children must
        persist that status first and clear this record after -- clearing it
        first and then crashing before the status save loses the only durable
        trace that those children still need attention.
        """
        self._state["prs"][index]["sweptChildren"] = []
        self._save()

    def halt(self, reason):
        """Record that the train stopped and must not resume unattended.

        The fatal paths exit without advancing currentIndex, so a --resume
        re-enters process_pr on the same PR. Without a durable marker that
        resume refuses, that re-entry reads the PR's persisted "failed" status,
        skips it and advances -- turning a deliberate stop into a silent pass
        over exactly the condition the stop exists to surface.
        """
        self._state["halted"] = {
            "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "reason": reason,
        }
        self._save()

    def advance(self):
        """Advance currentIndex and update summary counts, then persist."""
        pr = self._state["prs"][self._state["currentIndex"]]
        status = pr["status"]
        if status in self._state["summary"]:
            self._state["summary"][status] += 1
        self._state["currentIndex"] += 1
        self._save()

    def _save(self):
        """Save state to disk atomically (write temp + rename)."""
        write_json_private(self.state_path, self._state)


def extract_taiga_ref(branch, title, body):
    """Extract a Taiga ref number from PR branch, title, or body.

    Order: integration branch, conventional feature/fix branch, #<ref> in
    title/body, then any delimiter-bounded ref in the branch name.
    Returns the ref as an int, or None if not found.
    """
    # An integration branch names the child story, not the epic in its own
    # name, so check it before the title/body #<ref> scan below.
    parsed = parse_integration_branch(branch)
    if parsed:
        return parsed[1]

    # A conventional feature/<ref>-<slug> or fix/<ref>-<slug> branch names its
    # own story unambiguously. A title or body may mention a parent or sibling
    # story, so it must not outrank that.
    if branch:
        conventional = re.match(r"(?:feature|fix)/(\d+)(?:-|$)", branch)
        if conventional:
            return int(conventional.group(1))

    # Check title and body for #<ref> pattern
    for text in [title, body]:
        if not text:
            continue
        match = re.search(r"#(\d+)", text)
        if match:
            return int(match.group(1))

    # Check branch name for delimiter-bounded ref
    if branch:
        # Match ref at start, end, or between delimiters (-, /, _)
        for match in re.finditer(r"(\d+)", branch):
            ref = int(match.group(1))
            start = match.start()
            end = match.end()
            # Check boundaries: start of string, end of string, or delimiter
            left_ok = start == 0 or branch[start - 1] in "-/_"
            right_ok = end == len(branch) or branch[end] in "-/_"
            if left_ok and right_ok:
                return ref

    return None


def match_pr_to_story_ref(pr, story_ref):
    """Check if a PR matches a given story ref.

    Requires #<story-ref> in the title or body, or a delimiter-bounded
    occurrence in the branch name. Avoids plain substring matches that
    embed the ref in a larger number.
    """
    branch = pr.get("headRefName", "")
    title = pr.get("title", "")
    body = pr.get("body", "") or ""

    # Check title and body for #<ref> as distinct token
    ref_pattern = rf"#\b{story_ref}\b"
    if re.search(ref_pattern, title) or re.search(ref_pattern, body):
        return True

    # Check branch name for delimiter-bounded ref
    if branch:
        # Patterns: -<ref>, /<ref>, _<ref>, <ref> at start/end
        branch_pattern = rf"(?:^|[-/_]){story_ref}(?:[-/_]|$)"
        if re.search(branch_pattern, branch):
            return True

    return False


def order_prs_by_dependencies(prs, deps):
    """Order PRs based on a dependency map (ref -> list of dependency refs).

    PRs without dependencies come first (ordered by ref number).
    PRs with dependencies come after all their dependencies.
    Falls back to ref-number order when no deps are provided.

    A story with multiple dependency parents may have both an
    epic/<epic>-integration-<ref> PR and its own feature PR sharing the same
    story ref. Nodes are keyed by (ref, is_integration) rather than by ref
    alone so the two PRs don't collapse into a single node; the integration
    PR is ordered after every parent and the story's own PR depends on its
    integration PR when one is present.
    """

    def node_for(pr):
        ref = pr.get("_story_ref", 0)
        is_integration = parse_integration_branch(pr.get("headRefName", "")) is not None
        return (ref, 0 if is_integration else 1)

    if not deps:
        return sorted(prs, key=node_for)

    # Topological sort via Kahn's algorithm
    node_to_pr = {node_for(pr): pr for pr in prs if "_story_ref" in pr}
    all_nodes = set(node_to_pr.keys())

    # Build in-degree counts (only for nodes in our PR set)
    in_degree = {node: 0 for node in all_nodes}
    adj = {node: [] for node in all_nodes}
    for ref, is_integration in all_nodes:
        node = (ref, is_integration)
        if is_integration == 0:
            # Integration node depends on every parent's own PR.
            dep_nodes = [(dep_ref, 1) for dep_ref in deps.get(ref, [])]
        else:
            # A story's own PR depends on its integration PR when one
            # exists in this queue; otherwise it depends on its parents
            # directly.
            integration_node = (ref, 0)
            if integration_node in all_nodes:
                dep_nodes = [integration_node]
            else:
                dep_nodes = [(dep_ref, 1) for dep_ref in deps.get(ref, [])]
        for dep_node in dep_nodes:
            if dep_node in all_nodes:
                adj[dep_node].append(node)
                in_degree[node] += 1

    # Process nodes with zero in-degree
    queue = sorted([node for node in all_nodes if in_degree[node] == 0])
    ordered_nodes = []

    while queue:
        node = queue.pop(0)
        ordered_nodes.append(node)
        for child in sorted(adj.get(node, [])):
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)
        queue.sort()

    # Include any remaining nodes (circular deps fallback)
    leftover = [node for node in sorted(all_nodes) if node not in ordered_nodes]
    if leftover:
        leftover_refs = sorted({ref for ref, _ in leftover})
        log(f"WARNING: circular dependencies among {leftover_refs}; appending them in ref order")
    ordered_nodes.extend(leftover)

    return [node_to_pr[node] for node in ordered_nodes if node in node_to_pr]


def claim_story_ref(pr, refs, epic_ref):
    """Pick the one ref from ``refs`` that ``pr`` belongs to, or None.

    An integration branch names the epic it merges parents for, so one belonging
    to a different epic is rejected outright -- ``epic/999-integration-200``
    carries ``-200`` delimiter-bounded and would otherwise be accepted for story
    #200, putting another epic's commits on this train.

    Otherwise the branch is consulted first, then the title, and the body never.
    The three carry decreasing authority about what a PR implements. A branch
    (``fix/<slug>-<ref>``) names one story. A title usually does too, but may
    cite a parent alongside it ("feat: child (#200) -- builds on #100"). A body
    routinely cites refs the PR does not implement at all -- a child names its
    parents, a bug fix names the stories it sequences against.

    Honouring the weaker sources claims a PR for a story it has nothing to do
    with, and the consequence is silent: the queue keys nodes by
    ``(ref, is_integration)``, so a wrongly claimed PR does not sort oddly, it
    displaces the PR that ref really belongs to and vanishes from the state
    file. Reading the body at all also lets a PR belonging to no diagram ref be
    merged as one of the epic's stories.

    A pass claims the PR only when it names exactly one of the epic's refs. A
    title naming two ("child (#200) builds on #100") cannot say which story the
    PR implements, and picking either is a guess that lands in the queue as
    fact -- so the pass declines and the next one is tried.

    A story PR that names its ref unambiguously nowhere is therefore reported as
    having no open PR -- a visible skip rather than a wrong merge. Each pass
    blanks the fields it does not consult and reuses match_pr_to_story_ref
    rather than restating its patterns, so the passes cannot drift from it.
    """
    parsed = parse_integration_branch(pr.get("headRefName", ""))
    if parsed:
        branch_epic_ref, story_ref = parsed
        if branch_epic_ref != epic_ref:
            return None
        return story_ref if story_ref in refs else None

    branch_only = dict(pr, title="", body="")
    title_only = dict(pr, headRefName="", body="")
    for candidate in (branch_only, title_only):
        hits = [ref for ref in sorted(refs) if match_pr_to_story_ref(candidate, ref)]
        if len(hits) == 1:
            return hits[0]
    return None


def read_epic_dependencies(epic_ref, project_id=39):
    """Read an epic and return the {story_ref: [parent refs]} map of its dependency diagram.

    Raises RuntimeError rather than degrading: the queue's order, and the check
    that an integration PR's parents landed, both come from this epic, so an
    unreadable epic or a missing diagram must stop the caller.
    """
    rc, stdout, stderr = run_cmd(
        f"scripts/taiga-story.sh {_q(epic_ref)} {_q(project_id)} --epic",
        check=False,
    )
    if rc == TAIGA_NOT_FOUND_EXIT:
        raise RuntimeError(
            f"No epic #{epic_ref} in project {project_id}. Epic mode refuses to run "
            f"rather than merge in an arbitrary order; pass explicit PR numbers instead."
        )
    if rc != 0 or not stdout.strip():
        raise RuntimeError(
            f"Failed to read epic #{epic_ref} (exit {rc}): {stderr[:200]}. "
            f"Epic mode refuses to run rather than merge in an arbitrary order."
        )

    try:
        epic_data = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Epic #{epic_ref} lookup returned unparseable output: {stdout[:200]}"
        ) from exc

    epic_description = epic_data.get("description", "") or ""
    deps = parse_dependency_diagram(epic_description)
    if not deps:
        raise RuntimeError(
            f"Epic #{epic_ref} has no parseable '## Dependency Diagram' section, so the "
            f"train has no order to merge in. Add a diagram to the epic, or re-run with "
            f"explicit PR numbers."
        )

    return deps


def discover_epic_prs(epic_ref, project_id=39):
    """Build an ordered PR queue for an epic from its dependency diagram.

    Reads the epic's description via ``scripts/taiga-story.sh <ref> <project> --epic``,
    parses its ``## Dependency Diagram`` with lib.dependency_parser, matches every
    story ref named in the diagram to the open PRs carrying that ref, and orders
    the result topologically with order_prs_by_dependencies.

    Raises RuntimeError rather than degrading: the queue's order comes from this
    epic, and an unreadable epic or a missing diagram would otherwise land a
    stack in arbitrary ref order, or queue every open PR in the repo. There is
    deliberately no fallback to "every open PR carrying a Taiga ref".
    """
    deps = read_epic_dependencies(epic_ref, project_id)

    rc, stdout, stderr = run_cmd(
        gh_cmd(f"pr list --state open --json number,title,headRefName,body --limit {EPIC_PR_LIST_LIMIT}"),
        check=False,
    )
    if rc != 0:
        raise RuntimeError(f"Failed to list open PRs: {stderr[:200]}")
    try:
        all_prs = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Unparseable PR list: {stdout[:200]}") from exc

    if len(all_prs) >= EPIC_PR_LIST_LIMIT:
        # gh stops at --limit without saying so; a truncated list would silently
        # drop an epic PR from the queue. Fail closed instead.
        raise RuntimeError(
            f"The open-PR listing reached its {EPIC_PR_LIST_LIMIT}-PR limit, so it may be "
            f"incomplete. Epic mode refuses to build a queue from a possibly truncated "
            f"list; pass explicit PR numbers instead."
        )

    # Every ref the diagram names, whether as a node or as a dependency.
    refs = set(deps) | {dep for dep_list in deps.values() for dep in dep_list}

    # A story with multiple dependency parents has both an integration PR and
    # its own feature PR, and both carry its ref -- order_prs_by_dependencies
    # keys nodes by (ref, is_integration) precisely so the pair stays distinct.
    # So each open PR is claimed by exactly one ref, and a ref may collect both
    # of its PRs. Claiming has to be per-PR rather than per-ref: two PRs sharing
    # one (ref, is_integration) node collapse into one entry in the ordering, so
    # a PR claimed by the wrong ref does not merely sort oddly, it silently
    # displaces the PR that ref really belongs to.
    matched = []
    by_node = {}
    for pr in all_prs:
        ref = claim_story_ref(pr, refs, epic_ref)
        if ref is None:
            continue
        matched.append(dict(pr, _story_ref=ref))
        # The same key order_prs_by_dependencies sorts by. Two PRs sharing one
        # node collapse there into a single entry, so a duplicate would drop a
        # PR from the queue without a word. Which of the two to merge is an
        # operator decision, so refuse and name both rather than pick one.
        is_integration = parse_integration_branch(pr.get("headRefName", "")) is not None
        by_node.setdefault((ref, is_integration), []).append(pr["number"])

    for (ref, is_integration), numbers in sorted(by_node.items()):
        if len(numbers) > 1:
            kind = "integration PRs" if is_integration else "PRs"
            raise RuntimeError(
                f"Story #{ref} has {len(numbers)} open {kind} "
                f"({', '.join(f'#{n}' for n in sorted(numbers))}); the train cannot tell "
                f"which to merge. Close the ones that are superseded, or re-run with "
                f"explicit PR numbers."
            )

    for ref in sorted(refs - {pr["_story_ref"] for pr in matched}):
        # Normal: the story already merged, or has not been started.
        log(f"No open PR found for story #{ref}")

    ordered = order_prs_by_dependencies(matched, deps)

    return [
        {
            "number": pr["number"],
            "title": pr["title"],
            "branch": pr["headRefName"],
            "body": pr.get("body", "") or "",
        }
        for pr in ordered
    ]


def story_pr_landed(ref):
    """True only when story ``ref`` has a MERGED PR and no PR still open.

    Fails closed: a failed or unparseable listing, a story with no PR at all,
    and a story whose only PRs were closed unmerged all answer False. A PR is
    attributed to the story with extract_taiga_ref (branch, then title/body),
    and integration PRs never count as a story's own PR.

    Raises RuntimeError when the listing reached PARENT_PR_LIST_LIMIT: gh stops
    there without saying so, so a merged parent may be missing, and "not
    landed" would be a wrong answer rather than a failed lookup.
    """
    rc, stdout, _ = run_cmd(
        gh_cmd(f"pr list --state all --limit {PARENT_PR_LIST_LIMIT} "
               "--json number,state,title,headRefName,body"),
        check=False,
    )
    if rc != 0:
        return False
    try:
        listed = json.loads(stdout)
    except ValueError:
        return False
    if len(listed) >= PARENT_PR_LIST_LIMIT:
        raise RuntimeError(
            f"The all-state PR listing reached its {PARENT_PR_LIST_LIMIT}-PR limit, so it "
            f"may be missing story #{ref}'s PR"
        )
    states = set()
    for pr in listed:
        branch = pr.get("headRefName", "") or ""
        if parse_integration_branch(branch) is not None:
            continue
        if extract_taiga_ref(branch, pr.get("title", "") or "", pr.get("body", "") or "") == ref:
            states.add(pr.get("state"))
    return "MERGED" in states and "OPEN" not in states


def unlanded_integration_parents(branch):
    """The parent story refs of an integration PR that have not landed, or None.

    The integration branch was built by merging every parent's branch, so
    merging it before a parent lands would put that parent's unreviewed
    commits on the target. The parents are the story's entries in its epic's
    dependency diagram (the branch names both the epic and the story). An empty
    list means every parent landed; None means the parents could not be
    determined, which the caller must treat as a failure to verify.
    """
    parsed = parse_integration_branch(branch)
    if parsed is None:
        return None
    epic_ref, story_ref = parsed
    try:
        deps = read_epic_dependencies(epic_ref)
    except RuntimeError as exc:
        log(f"Cannot read parents of integration branch {branch}: {exc}")
        return None
    parents = deps.get(story_ref)
    if not parents:
        log(f"Epic #{epic_ref} declares no parents for story #{story_ref}")
        return None
    try:
        return [ref for ref in sorted(parents) if not story_pr_landed(ref)]
    except RuntimeError as exc:
        log(f"Cannot verify parents of integration branch {branch}: {exc}")
        return None


def generate_summary(state):
    """Generate a human-readable summary from the final state.

    Returns a markdown-formatted string.
    """
    prs = state["prs"]
    summary = state["summary"]
    target = state["target"]
    total = len(prs)

    lines = [
        "## Merge Train Complete",
        "",
        f"**Target:** {target}",
        f"**Total PRs:** {total}",
        "",
    ]

    # Merged
    merged = [pr for pr in prs if pr["status"] == "merged"]
    lines.append(f"### Merged ({summary['merged']})")
    if merged:
        for pr in merged:
            lines.append(f"- #{pr['number']} -- {pr['title']} (branch: {pr['branch']})")
    else:
        lines.append("- (none)")
    lines.append("")

    # Skipped
    skipped = [pr for pr in prs if pr["status"] == "skipped"]
    lines.append(f"### Skipped ({summary['skipped']})")
    if skipped:
        for pr in skipped:
            reason = pr.get("error", "unknown reason")
            lines.append(f"- #{pr['number']} -- {reason}")
    else:
        lines.append("- (none)")
    lines.append("")

    # Failed
    failed = [pr for pr in prs if pr["status"] == "failed"]
    lines.append(f"### Failed ({summary['failed']})")
    if failed:
        for pr in failed:
            reason = pr.get("error", "unknown reason")
            lines.append(f"- #{pr['number']} -- {reason} (branch: {pr['branch']})")
    else:
        lines.append("- (none)")
    lines.append("")

    lines.append(f"**State file:** {state.get('_state_path', 'N/A')}")

    return "\n".join(lines)


def log(msg):
    """Write a log message to stderr."""
    print(f"[merge-train] {msg}", file=sys.stderr, flush=True)


def run_cmd(cmd, check=True, capture=True):
    """Run a command, returning (returncode, stdout, stderr).

    A string is split with shlex and run WITHOUT a shell, so a branch name or Taiga
    status that contains ``;``, ``$(...)`` or quotes (all legal in git refs or
    Taiga data) can never become a second command. Callers interpolating
    externally supplied text into a string must wrap it in ``shlex.quote``
    (``_q``); wildcards are not expanded, use ``expand_globs``.
    """
    if isinstance(cmd, str):
        cmd = shlex.split(cmd)
    result = subprocess.run(
        cmd,
        shell=False,
        capture_output=capture,
        text=True,
    )
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode, cmd, result.stdout, result.stderr
        )
    return result.returncode, result.stdout.strip() if result.stdout else "", result.stderr.strip() if result.stderr else ""


def _q(text):
    """Shell-quote one externally supplied value for a run_cmd command string."""
    return shlex.quote(str(text))


def expand_globs(argv):
    """Expand wildcard tokens in an argv list the way a shell would (no match -> kept)."""
    out = []
    for token in argv:
        matches = sorted(glob.glob(token)) if any(c in token for c in "*?[") else []
        out.extend(matches or [token])
    return out


def gh_cmd(args):
    """A gh command line for the pipeline repo: GH_TOKEN dropped, --repo appended."""
    return f"env -u GH_TOKEN gh {args} --repo {GH_REPO}"


def has_unit_tests(root="."):
    """True when tests/ holds a test suite besides the helper-script tests."""
    tests_dir = os.path.join(root, "tests")
    if not os.path.isdir(tests_dir):
        return False
    for dirpath, dirnames, filenames in os.walk(tests_dir):
        if os.path.relpath(dirpath, tests_dir).split(os.sep)[0] == "scripts":
            dirnames[:] = []
            continue
        if any(f.startswith("test_") and f.endswith(".py") for f in filenames):
            return True
    return False


def run_build_checks():
    """Run the post-rebase checks; return (rc, stdout, stderr) of the first failure or the last run."""
    # No shell runs the command, so expand bots/*.py here and hand run_cmd a
    # fully quoted string.
    compile_cmd = " ".join(_q(a) for a in expand_globs(shlex.split(COMPILE_CHECK_CMD)))
    rc, out, err = run_cmd(compile_cmd, check=False)
    if rc != 0 or not has_unit_tests():
        return rc, out, err
    return run_cmd(UNIT_TEST_CMD, check=False)


def action_path_for(state_path):
    """The action-request path that belongs to ``state_path``.

    The state file name is unique per train (mkstemp), so deriving the action
    path from it keeps two trains started in the same second from replacing
    each other's request. It still matches /tmp/merge-train-*-action.json.
    """
    root = state_path[:-5] if state_path.endswith(".json") else state_path
    return f"{root}-action.json"


def write_action_request(action_path, action, pr, state_file, details):
    """Write an action-request JSON file for the skill wrapper.

    The skill wrapper reads this file to determine what agent-only action
    is needed (conflict resolution or build remediation).
    """
    data = {
        "action": action,
        "pr": {
            "number": pr["number"],
            "branch": pr["branch"],
            "title": pr["title"],
        },
        "state_file": state_file,
        "details": details,
    }
    write_json_private(action_path, data)


BUILD_LOG_TAIL_LINES = 100
BUILD_LOG_CONSOLE_LINES = 20


def write_build_log(log_path, stdout, stderr):
    """Persist a failed build's stdout+stderr to log_path and return the last
    BUILD_LOG_TAIL_LINES lines as a string.

    py_compile and pytest split their errors across both streams, so both are kept.
    """
    combined = "\n".join(part for part in (stdout, stderr) if part)
    with open(log_path, "w") as f:
        f.write(combined)
    lines = combined.splitlines()
    return "\n".join(lines[-BUILD_LOG_TAIL_LINES:])


def wait_for_merge(pr_num, timeout_seconds=None):
    """Poll a PR until it reports MERGED. Returns "merged", "closed", or "timeout".

    timeout_seconds defaults to MERGE_POLL_TIMEOUT_SECONDS, resolved at call
    time so tests can monkeypatch the module-level constant.
    """
    if timeout_seconds is None:
        timeout_seconds = MERGE_POLL_TIMEOUT_SECONDS
    deadline = time.time() + timeout_seconds
    while True:
        rc, state, _ = run_cmd(gh_cmd(f"pr view {pr_num} --json state -q .state"), check=False)
        if rc == 0 and state == "MERGED":
            return "merged"
        if rc == 0 and state == "CLOSED":
            return "closed"
        if time.time() >= deadline:
            return "timeout"
        time.sleep(MERGE_POLL_INTERVAL_SECONDS)


def check_taiga_status(ref, project_id=39):
    """Check Taiga item status for a given ref.

    Returns (is_closed, status_name, error) tuple.
    Tries user story first, falls back to issue.
    """
    for item_flag in ["", "--issue"]:
        cmd = f"scripts/taiga-story.sh {ref} {project_id}"
        if item_flag:
            cmd += f" {item_flag}"
        rc, stdout, stderr = run_cmd(cmd, check=False)

        if rc == 0 and stdout:
            try:
                data = json.loads(stdout)
                status_name = "unknown"
                if "status_extra_info" in data and data["status_extra_info"]:
                    status_name = data["status_extra_info"].get("name", str(data.get("status", "unknown")))
                else:
                    status_name = str(data.get("status", "unknown"))
                # Use status_extra_info.is_closed (status-level) rather than
                # data.is_closed (item-level) — they can diverge when an item
                # is in a closed status but the item itself hasn't been toggled.
                sei = data.get("status_extra_info") or {}
                is_closed = sei.get("is_closed", data.get("is_closed", False))
                return is_closed, status_name, None
            except json.JSONDecodeError:
                return False, "unknown", f"Invalid JSON from taiga-story.sh: {stdout[:100]}"

        # Distinguish 404 from other errors. taiga-story.sh exits 44 for a
        # by_ref 404 so this does not depend on the wording of its stderr.
        if rc != 0:
            if rc != TAIGA_NOT_FOUND_EXIT:
                return False, "unknown", f"Taiga lookup failed: {stderr[:200]}"
            # 404 — try next item type
            continue

    return False, "not_found", None


def decide_retarget(pr, merged_branches, target="main"):
    """Return the branch a stacked PR should be retargeted at, or None.

    An epic's PRs form a linear stack: each PR's base is its predecessor's head
    branch. Once that predecessor lands, its branch is deleted and the child's
    base is stale, so the child is pointed at the train's target instead.

    Retarget when the PR's base is neither the target nor a branch still in
    flight -- i.e. it is a branch this train has already merged, or one that no
    longer exists on the remote (pass it in ``merged_branches`` either way).
    Returns None when the PR already targets ``target`` or its base is still
    open, since retargeting then would jump the queue.
    """
    base = (pr.get("base") or pr.get("baseRefName") or "").strip()
    if not base or base == target:
        return None
    if base in set(merged_branches or ()):
        return target
    return None


def find_open_children(branch):
    """PR numbers of every open PR whose base is ``branch``.

    A branch deleted by GitHub's own delete_branch_on_merge has its open
    children retargeted onto the merged PR's base and left open. A branch
    deleted any other way -- gh --delete-branch, git push --delete, REST --
    has them closed instead, even when the parent PR merged successfully
    (#2801). The sweep performs the retarget itself, before arming, for two
    things GitHub's retargeting does not give: it is synchronous, so the
    train never advances while a child is still pointed at a ref that is
    being deleted, and it records every child it moved in `sweptChildren`,
    so a parent that then fails to land can have them put back (see
    ``restore_children_base``). #2801 settled the open question of whether
    the sweep still earns its keep: it stays, and deleting it is a deferred
    follow-up once the measured retargeting rule has been confirmed in a
    real train run.

    The children are read from GitHub rather than from the queue so a stacked
    PR that was never enqueued is caught too.

    Returns ``(ok, children)``. ``ok`` is False only when the query itself
    failed, which is **not** the same as a successful empty result: a network,
    auth or rate-limit failure must not read as "this parent has no children"
    and let the parent's branch deletion through. The caller fails the PR
    instead.

    An empty branch is ``(True, [])`` without asking: `gh pr list --base
    --state open` lists every open PR in the repo, and the caller would
    retarget the lot.

    --limit is explicit because `gh pr list` defaults to 30, and a silently
    truncated page is a child this sweep would leave unswept when the parent
    lands and its base branch is deleted. It is also checked: a result of
    exactly ``OPEN_CHILDREN_LIMIT`` may have been cut short, so it is reported
    as a query failure -- ``(False, [])`` -- rather than as a complete result.
    """
    if not branch:
        return True, []
    rc, stdout, _ = run_cmd(
        gh_cmd(f"pr list --base {_q(branch)} --state open --limit {OPEN_CHILDREN_LIMIT} "
        f"--json number -q '.[].number'"),
        check=False,
    )
    if rc != 0:
        return False, []
    children = [int(line.strip()) for line in stdout.splitlines() if line.strip().isdigit()]
    if len(children) >= OPEN_CHILDREN_LIMIT:
        # Exactly --limit results means `gh` may have stopped short of the real
        # set. The sweep would then retarget a subset while reporting success,
        # leaving the train to reason about children it never saw, so a
        # possibly-partial answer is treated as no answer at all.
        log(
            f"gh pr list returned {len(children)} open PRs based on {branch}, "
            f"at the --limit of {OPEN_CHILDREN_LIMIT}; the list may be truncated "
            f"and an unswept child would go unrecorded by this train"
        )
        return False, []
    return True, children


def restore_children_base(sm, children, branch):
    """Point ``children`` back at ``branch`` after their parent failed to land.

    The pre-arming sweep moves a stacked child onto the train's target before
    the parent is armed. That is only correct if the parent actually lands: a
    child branched off the parent carries the parent's commits, so a child left
    on the target after the parent failed can be merged straight into main and
    land those commits with no PR of their own -- the same hazard
    branch_merged_on_remote() fails closed on.

    Call this only when the parent is definitively not landing and the queue
    advances past it (arming failed, or the PR was closed unmerged). Do NOT call
    it while auto-merge is still armed and merely pending: the PR can land at any
    moment, and a child put back onto a branch about to be deleted is then at the
    mercy of who performs that deletion -- GitHub's own delete_branch_on_merge
    retargets it, but any client-side deletion closes it (#2801), and a closed
    child cannot be reopened until the deleted ref is recreated.

    A failed restore is logged and the loop continues to the remaining
    children -- halting here would not un-stick a `gh` that is refusing
    edits, and the other children still need to go back onto `branch`. But a
    child that could not be restored is left with its queue record claiming
    `branch` -- e.g. its base is *actually* still `target` on GitHub, which
    carries the same hazard the docstring above describes: it can be merged
    straight into main and land the unlanded parent's commits with no PR of
    their own. That child's queue entry is marked failed so the queue does
    not later arm it as if the restore had succeeded.

    ``find_open_children()`` deliberately includes PRs that were never
    enqueued (a stacked PR that was never queued by this train), so a restore
    failure for one of those has no queue record to mark failed -- there is
    nowhere in state to remember it is still wrongly based on `target`, and a
    later train run that discovers and enqueues it would have no idea. Rather
    than silently drop that danger, the PR itself is warned with a comment and
    its number is returned so the caller can fail the whole run instead of
    advancing past it.

    Returns the list of child PR numbers that could not be restored and have
    no queue record to carry the failure.
    """
    unresolvable = []
    for child in children:
        log(f"Restoring child PR {child} to base {branch} -- its parent did not land")
        rc, _, stderr = run_cmd(gh_cmd(f"pr edit {child} --base {_q(branch)}"), check=False)
        if rc != 0:
            log(f"Could not restore PR {child} to base {branch}: {stderr[:200]}")
            child_index = sm.index_of(child)
            if child_index is not None:
                sm.update_pr(
                    child_index,
                    status="failed",
                    error=f"could not restore base to {branch} after parent failed: {stderr[:200]}",
                )
            else:
                run_cmd(
                    gh_cmd(f"pr comment {child} --body " + _q(
                        f"Merge train: could not restore this PR"
                        f" to base {branch} after its parent failed to land. It is still based on"
                        f" the train's target, not {branch}, and was never part of this train's own"
                        f" queue, so the train has no way to track or automatically fix this."
                        f" Do NOT merge until its base is verified manually.")),
                    check=False,
                )
                unresolvable.append(child)
            continue
        child_index = sm.index_of(child)
        if child_index is not None:
            sm.update_pr(child_index, base=branch)
    return unresolvable


def branch_merged_on_remote(branch, target):
    """True when ``branch`` had exactly one pull request, GitHub merged it, and
    it merged into ``target``.

    A branch missing from the remote is not evidence that its commits landed: a PR
    that was closed unmerged and then had its branch deleted looks identical to
    one that merged. Retargeting a child on that evidence would absorb the
    abandoned parent's commits into the child's diff and land them with no
    review of their own -- the very thing process_pr() refuses when it declines
    to record a CLOSED PR's branch.

    ``state == MERGED`` alone is not enough either: a stacked PR can merge into
    its own parent branch rather than ``target`` (e.g. merged by hand before
    this train ever retargeted it). Treating that as "landed" would let a
    descendant retarget straight onto ``target`` and carry whatever of that
    parent's ancestry never made it past its own still-unlanded parent branch.
    Only a PR that merged directly into ``target`` counts.

    So the base branch's own PR decides, and anything short of a single MERGED
    PR into ``target`` -- none, several, merged elsewhere, or a failed query --
    fails closed. The PR then keeps its stale base, arming fails, and the
    arming error names that base.
    """
    if not branch:
        return False
    rc, stdout, _ = run_cmd(
        gh_cmd(f"pr list --head {_q(branch)} --state all --limit 2 --json state,baseRefName "
        "-q '.[] | .state + \"\\t\" + .baseRefName'"),
        check=False,
    )
    if rc != 0:
        return False
    return [line.strip() for line in stdout.splitlines() if line.strip()] == [f"MERGED\t{target}"]


def branch_contained_in_target(branch, target):
    """True when the branch's tree is identical to the target's -- it adds nothing.

    Two-dot, not three-dot: a parent merged with --squash leaves its commits
    out of the target's ancestry, so a merge-base diff still reports its files.
    """
    rc, _, _ = run_cmd(f"git diff --quiet {_q(f"{REMOTE}/{target}")} {_q(f"{REMOTE}/{branch}")}", check=False)
    return rc == 0


def auto_merge_state(pr_num):
    """Whether GitHub reports auto-merge armed on ``pr_num``: True, False or None.

    A --resume after an auto-merge timeout re-enters process_pr on a PR that is
    still armed from the first pass and issues `gh pr merge --auto` a second
    time. A duplicate enable -- or any transient API error on that call --
    returns non-zero without the PR having become unarmed, so a non-zero rc is
    not by itself proof that the sweep can be undone.

    None means the question could not be answered. Neither answer is safe to
    assume there: restoring a child onto a branch a live auto-merge is about to
    delete is the closure the sweep exists to prevent, and leaving one on the
    target under a parent that never armed lets it merge that parent's unlanded
    commits. The caller stops the train instead of guessing.
    """
    rc, stdout, _ = run_cmd(
        gh_cmd(f"pr view {pr_num} --json autoMergeRequest -q '.autoMergeRequest'"),
        check=False,
    )
    if rc != 0:
        return None
    return stdout.strip() not in ("", "null")


def fail_pr_and_restore_children(sm, index, branch, target, error, status="failed"):
    """Put this PR's swept children back on ``branch``, record ``status``, advance.

    Every path that gives up on a PR after the sweep owes this same sequence,
    and the order within it is load-bearing: the parent's terminal status must
    persist before the swept-children record is cleared (see
    clear_swept_children), and a child that could not be restored and has no
    queue record to carry the failure halts the train rather than letting it
    advance past a PR sitting on ``target`` with this parent's unlanded commits.

    The children come from the persisted record rather than a caller-supplied
    list so a run that crashed or timed out mid-sweep still restores what its
    predecessor moved -- find_open_children(branch) cannot rediscover those,
    their base being ``target`` by then.

    Returns the exit code the caller should return. It does not return when the
    train halts.
    """
    pr = sm.state["prs"][index]
    pr_num = pr["number"]
    unresolvable = restore_children_base(sm, list(pr.get("sweptChildren", [])), branch)
    if unresolvable:
        status = "failed"
        error = f"{error}; could not restore child PR(s) {unresolvable} off {target}"
    sm.update_pr(index, status=status, error=error)
    sm.clear_swept_children(index)
    if unresolvable:
        reason = (
            f"child PR(s) {unresolvable} could not be restored off {target} and are not "
            f"queued, so they still carry PR #{pr_num}'s unlanded commits"
        )
        sm.halt(reason)
        log(f"Fatal: {reason}; stopping the train for manual intervention")
        sys.exit(EXIT_ERROR)
    sm.advance()
    return EXIT_OK


def monitor_infrastructure_error(sm, index, pr_num, phase, stdout, stderr):
    """Handle monitor-pr-stability.sh exiting nonzero (auth, network, bad JSON).

    That is a failure of the monitor, not a verdict on the PR's build, so the PR
    is neither failed nor sent to remediation, and the train does not advance.
    It stays in_progress with the error recorded and the train stops with
    EXIT_ERROR; the merge-train skill retries a transient one with --resume,
    which re-enters this PR.
    """
    # The monitor prints "ERROR:..." on stdout, so stderr alone is usually empty.
    detail = " ".join(part for part in (stdout, stderr) if part)[:200]
    log(f"CI monitoring ({phase}) failed for PR #{pr_num}: {detail}")
    sm.update_pr(index, status="in_progress", error=f"CI monitoring failed ({phase}): {detail}")
    return EXIT_ERROR


def process_pr(sm, index, target, state_path):
    """Process a single PR: Taiga gate, pre-rebase CI check, rebase, force-push, CI, merge.

    Returns an exit code: EXIT_OK to continue, EXIT_CONFLICTS or EXIT_REMEDIATION
    to request agent intervention, or EXIT_ERROR for unexpected failures.
    """
    pr = sm.state["prs"][index]
    pr_num = pr["number"]
    branch = pr["branch"]
    ts = int(time.time())
    action_path = action_path_for(state_path)

    # A halt is a stop the train may not drive past on its own. The paths that
    # set one exit without advancing, so --resume comes straight back here.
    halted = sm.state.get("halted")
    if halted:
        log(f"Train is halted: {halted.get('reason', 'reason not recorded')}")
        log('Resolve the condition, then remove the "halted" record from the state file to resume.')
        sys.exit(EXIT_ERROR)

    # A PR can already be "failed" here without process_pr having touched it
    # this run: restore_children_base() marks a child failed in place when it
    # could not move that child back off the target after its parent didn't
    # land, because the child's actual GitHub base is still the target and
    # carries the parent's unlanded commits. Reprocessing it normally would
    # rediscover an OPEN PR and arm it, landing exactly what the restore
    # failure was trying to prevent.
    if pr.get("status") == "failed":
        log(f"PR #{pr_num} was already marked failed; skipping. Recorded error: {pr.get('error')}")
        sm.advance()
        return EXIT_OK

    # 3a. Update state to in_progress
    sm.update_pr(index, status="in_progress")
    log(f"Processing PR #{pr_num}: {pr['title']} (branch: {branch})")

    # 3b. Fetch latest target
    rc, _, stderr = run_cmd(f"git fetch {REMOTE}", check=False)
    if rc != 0:
        log(f"git fetch failed: {stderr}")
        sm.update_pr(index, status="failed", error=f"git fetch {REMOTE} failed; unable to update target branch")
        sm.advance()
        return EXIT_OK

    # 3c. Check if PR is still open. State and base come from one call: the
    # retarget below needs the live base, and the merged-branch bookkeeping
    # needs it too -- state alone cannot say where a merged PR landed.
    rc, stdout, _ = run_cmd(
        gh_cmd(f"pr view {pr_num} --json state,baseRefName"),
        check=False,
    )
    pr_state, live_base = "", ""
    if rc == 0 and stdout.strip():
        try:
            view = json.loads(stdout)
        except ValueError:
            view = {}
        if isinstance(view, dict):
            pr_state = (view.get("state") or "").strip()
            live_base = (view.get("baseRefName") or "").strip()
    # A failed or incomplete lookup says nothing about the PR. Carrying on as if
    # it were open would rebase, push and merge on stale state, so stop here
    # before anything mutates it.
    if not pr_state or not live_base:
        log(f"Could not read live state of PR #{pr_num} (gh exit {rc}); not touching it")
        return fail_pr_and_restore_children(
            sm, index, branch, target,
            f"gh pr view {pr_num} failed or returned incomplete data; live PR state unknown",
        )
    if pr_state != "OPEN":
        # A PR that merged outside this run -- by hand, by an earlier train --
        # still has to feed mergedBranches, or its stacked child is never
        # retargeted and cannot arm auto-merge. A CLOSED PR's branch must not
        # be recorded: its commits never landed. Neither may a PR that merged
        # into something other than the target: a stacked PR merged by hand
        # into its own parent branch is MERGED without its commits having
        # reached the target, and recording it would let a descendant retarget
        # straight onto the target and carry that unlanded ancestry with it.
        if pr_state == "MERGED" and live_base == target:
            sm.record_merged_branch(branch)
            sm.clear_swept_children(index)
        elif pr_state == "MERGED":
            log(
                f"PR #{pr_num} merged into {live_base or 'an unknown branch'}, not {target}; "
                f"not recording {branch} as landed"
            )
            sm.clear_swept_children(index)
        else:
            # This PR closed unmerged. A prior invocation of this same PR may
            # have swept children onto target before crashing, timing out, or
            # otherwise exiting before it could restore them itself -- that
            # in-memory list does not survive across runs, but sweptChildren
            # does. Restore now so a resume does not leave them stranded on
            # target carrying this PR's unlanded commits.
            swept = pr.get("sweptChildren", [])
            if swept:
                log(
                    f"PR #{pr_num} closed unmerged with {len(swept)} child PR(s) still "
                    f"swept onto {target} from an earlier run; restoring them to {branch}"
                )
            return fail_pr_and_restore_children(
                sm, index, branch, target, "already merged/closed", status="skipped"
            )
        sm.update_pr(index, status="skipped", error="already merged/closed")
        sm.advance()
        return EXIT_OK

    # An integration branch that already merged every parent it exists to
    # combine contributes nothing further once all of those parents have
    # landed on target -- close it rather than let the stability check or
    # retarget logic treat it as a normal, still-live PR.
    if parse_integration_branch(branch) and branch_contained_in_target(branch, target):
        log(f"Integration PR #{pr_num} adds nothing to {target}; closing as already landed")
        run_cmd(
            gh_cmd(f"pr close {pr_num} --comment " + _q(
                "Merge train closed this integration PR -- "
                f"every parent branch it merged has landed on {target}, so it carries no remaining diff.")),
            check=False,
        )
        # Proven contained above, so the child PR based on this branch may safely
        # retarget onto the target. This is the one place a CLOSED branch is
        # recorded as landed; see branch_merged_on_remote() for why that is
        # otherwise refused.
        sm.record_merged_branch(branch)
        sm.update_pr(
            index,
            status="merged",
            error="integration branch fully contained in target; closed as empty",
        )
        sm.advance()
        return EXIT_OK

    # Retarget a stacked PR whose parent has already landed. Do this before the
    # stability check so the check reads the base branch the PR will merge into.
    if live_base:
        # Sticky: a PR that was stacked arms --merge later even after being
        # retargeted onto the target, so base and flag persist together -- a
        # crash between two writes would leave exactly the PR the flag protects
        # recorded as standalone.
        sm.update_pr(index, base=live_base, stacked=True if live_base != target else None)
        pr = sm.state["prs"][index]
    # A base merged outside this train's own bookkeeping -- by hand, or by an
    # earlier train run -- is kept in a local copy rather than persisted:
    # mergedBranches is the record of what this train itself landed. Checked
    # regardless of whether the branch still exists on the remote: a merge done
    # without "Automatically delete head branches" (or one a human merged and
    # deliberately kept) leaves the branch in place, and skipping the merged-PR
    # check whenever the branch exists would leave the child based on a branch
    # whose PR has already landed instead of retargeting it onto the target.
    landed = list(sm.state.get("mergedBranches", []))
    current = (pr.get("base") or "").strip()
    if current and current != target and current not in landed and branch_merged_on_remote(current, target):
        log(f"PR #{pr_num} base {current} has already merged; treating it as landed")
        landed.append(current)
    new_base = decide_retarget(pr, landed, target)
    if new_base:
        log(f"Retargeting PR #{pr_num} from {pr['base']} at {new_base}")
        rc, _, stderr = run_cmd(gh_cmd(f"pr edit {pr_num} --base {_q(new_base)}"), check=False)
        if rc != 0:
            sm.update_pr(index, status="failed", error=f"retarget to {new_base} failed: {stderr[:200]}")
            sm.advance()
            return EXIT_OK
        sm.update_pr(index, base=new_base)
        pr = sm.state["prs"][index]

    # A base that is neither the target nor landed belongs to a PR the queue
    # has not reached. Stop here rather than at the pre-merge base guard: every
    # step between the two rewrites this PR's branch onto the target -- rebase,
    # force-push, a full CI cycle -- replaying the unlanded parent's commits
    # into a diff that is supposed to be this story's alone, and burning a CI
    # run to produce it. Skipped rather than failed so a later run, once the
    # parent has landed, still picks the PR up.
    if (pr.get("base") or "").strip() != target:
        log(f"PR #{pr_num} is based on {pr.get('base')}, which has not landed yet; leaving it alone")
        sm.update_pr(
            index,
            status="skipped",
            error=f"base {pr.get('base')} has not landed; re-queue this PR after it does",
        )
        sm.advance()
        return EXIT_OK

    # Taiga gate: verify the item is Done/Closed before spending CI, agents,
    # or a force-push on this PR. A PR this gate will skip must not first get
    # a remediation dispatch, a rebase, or a force-push -- its story may still
    # be under active work, and rewriting the branch disrupts it (bug #2900).
    # An integration branch has no Taiga item of its own -- extract_taiga_ref
    # resolves it to the *child* story's ref, which normally stays out of
    # Done/Closed until the child's own PR merges. Gating on that ref would
    # skip every integration PR forever, so it is treated as synthetic and
    # exempted from this check.
    if parse_integration_branch(branch) is not None:
        log(f"PR #{pr_num} is a synthetic integration PR; bypassing Taiga status gate")
        # It has no Taiga item to gate on, so the gate it needs instead is that
        # every parent it merged has itself landed.
        unlanded = unlanded_integration_parents(branch)
        if unlanded is None or unlanded:
            reason = (
                "could not verify that the integration PR's parent stories landed"
                if unlanded is None else
                "parent stories not merged yet: " + ", ".join(f"#{r}" for r in unlanded)
            )
            sm.update_pr(index, status="skipped", error=reason)
            sm.advance()
            return EXIT_OK
    else:
        log(f"Checking Taiga status for PR #{pr_num}...")
        taiga_ref = extract_taiga_ref(branch, pr["title"], pr.get("body", ""))
        if taiga_ref is None:
            sm.update_pr(index, status="skipped", error="no Taiga item reference found in PR")
            sm.advance()
            run_cmd(
                gh_cmd(f"pr comment {pr_num} --body " + _q("Merge train skipped this PR -- could not find a Taiga story or bug reference in the branch name, title, or body. Add #<ref> to the PR title and re-queue.")),
                check=False,
            )
            return EXIT_OK

        is_closed, status_name, taiga_error = check_taiga_status(taiga_ref)
        if taiga_error:
            sm.update_pr(index, status="failed", error=taiga_error)
            sm.advance()
            return EXIT_OK

        if status_name == "not_found":
            sm.update_pr(index, status="skipped", error=f"Taiga item #{taiga_ref} not found in project")
            sm.advance()
            run_cmd(
                gh_cmd(f"pr comment {pr_num} --body " + _q(f"Merge train skipped this PR -- Taiga item #{taiga_ref} was not found as either a user story or issue. Verify the ref number and re-queue.")),
                check=False,
            )
            return EXIT_OK

        if not is_closed:
            sm.update_pr(index, status="skipped", error=f"Taiga item #{taiga_ref} not in Done/Closed status (current: {status_name})")
            sm.advance()
            run_cmd(
                gh_cmd(f"pr comment {pr_num} --body " + _q(f"Merge train skipped this PR -- Taiga item #{taiga_ref} is in '{status_name}' status (expected: Done/Closed). Move the item to Done after review, then re-queue.")),
                check=False,
            )
            return EXIT_OK

    # Pre-rebase stability check: verify CI is passing before rebase
    log(f"Checking pre-rebase CI status for PR #{pr_num}...")
    rc, stdout, stderr = run_cmd(
        f"scripts/monitor-pr-stability.sh {pr_num} 0 --poll-interval 30",
        check=False,
    )
    if rc != 0:
        return monitor_infrastructure_error(sm, index, pr_num, "pre-rebase", stdout, stderr)

    stability = parse_stability_result(stdout)
    if stability["status"] == "checks_failing":
        log(f"PR #{pr_num} has failing CI checks before rebase: {stability['failing_checks']}")
        write_action_request(
            action_path,
            action="remediate_build",
            pr=pr,
            state_file=state_path,
            details={
                "failure_type": "ci_pre_rebase",
                "failing_checks": stability["failing_checks"],
            },
        )
        sm.update_pr(index, status="in_progress", error=f"pre-rebase CI failing: {','.join(stability['failing_checks'])}")
        sm._save()
        log(f"Action request written to {action_path}")
        return EXIT_REMEDIATION

    if stability["status"] == "comments_found":
        log(f"PR #{pr_num} has {stability['comment_count']} unresolved review comment thread(s) before rebase")
        write_action_request(
            action_path,
            action="resolve_comments",
            pr=pr,
            state_file=state_path,
            details={
                "failure_type": "comments_pre_rebase",
                "comment_count": stability["comment_count"],
            },
        )
        sm.update_pr(index, status="in_progress", error=f"review comments found ({stability['comment_count']})")
        log(f"Action request written to {action_path}")
        return EXIT_REMEDIATION

    # 3d. Checkout branch and rebase onto target (if needed)
    rc, _, checkout_stderr = run_cmd(f"git checkout {_q(branch)}", check=False)
    if rc != 0:
        err_detail = checkout_stderr.strip()
        if "is already checked out at" in err_detail:
            error_msg = f"branch is checked out in another worktree ({err_detail}). Remove the worktree first."
        else:
            error_msg = f"failed to checkout branch: {err_detail or 'non-zero exit code'}"
        sm.update_pr(index, status="failed", error=error_msg)
        sm.advance()
        return EXIT_OK

    # The remote (upstream) is the authority for what the PR contains: every branch mutation the
    # train makes is force-pushed before it moves on, and the remediation and
    # rebase agents push their work before returning.  A prior run can still leave
    # the local branch rebased-but-unpushed (a post-rebase build failure exits
    # before the force-push), so sync to the remote by reset.  `git pull --ff-only`
    # cannot fast-forward such a branch and would knock a green PR out of the
    # train (bug #2166).  The reset discards only a rebase that is re-derived
    # below, so it is safe to repeat on every resume.
    rc, remote_head, stderr = run_cmd(f"git rev-parse {_q(f'{REMOTE}/{branch}')}", check=False)
    if rc != 0:
        log(f"git rev-parse {REMOTE}/{branch} failed: {stderr.strip()}")
        sm.update_pr(index, status="failed", error=f"{REMOTE}/{branch} not found: {stderr.strip() or 'non-zero exit code'}")
        sm.advance()
        return EXIT_OK

    _, local_head, _ = run_cmd("git rev-parse HEAD", check=False)
    if local_head != remote_head:
        log(
            f"Local {branch} ({local_head[:8]}) has diverged from {REMOTE}/{branch} "
            f"({remote_head[:8]}); resetting to {REMOTE}. Discarded local state is "
            f"recoverable from the reflog at {local_head}."
        )

    rc, _, stderr = run_cmd(f"git reset --hard {_q(f'{REMOTE}/{branch}')}", check=False)
    if rc != 0:
        log(f"git reset --hard {REMOTE}/{branch} failed: {stderr.strip()}")
        sm.update_pr(index, status="failed", error=f"git reset --hard {REMOTE}/{branch} failed: {stderr.strip() or 'non-zero exit code'}")
        sm.advance()
        return EXIT_OK

    # Check if branch is already up-to-date with target.
    # git merge-base --is-ancestor exits 0 (is ancestor), 1 (not ancestor), or 128 (error).
    rc, _, stderr = run_cmd(f"git merge-base --is-ancestor {_q(f'{REMOTE}/{target}')} HEAD", check=False)
    if rc == 0:
        log(f"PR #{pr_num} is already up-to-date with {REMOTE}/{target}, skipping rebase")
    elif rc != 1:
        # rc 128 or any unexpected code means git itself failed (missing ref, etc.)
        log(f"git merge-base --is-ancestor failed with rc={rc}: {stderr.strip()}")
        sm.update_pr(index, status="failed", error=f"git merge-base --is-ancestor failed (rc={rc}): {stderr.strip() or 'no output'}")
        sm.advance()
        return EXIT_OK
    else:
        # rc == 1: {REMOTE}/{target} is not an ancestor of HEAD, bring the branch
        # forward is needed. An integration branch is a merge commit by
        # construction; rebasing it would flatten that merge commit and
        # re-apply an already-squashed parent's commits on top of target, so
        # merge target into it instead of rebasing it onto target.
        is_integration = parse_integration_branch(branch) is not None
        if is_integration:
            log(f"Merging {REMOTE}/{target} into PR #{pr_num} (integration branch)...")
        else:
            log(f"Rebasing PR #{pr_num} onto {REMOTE}/{target}...")

        # Record pre-rebase ref for rollback
        _, pre_rebase_ref, _ = run_cmd("git rev-parse HEAD", check=False)

        if is_integration:
            rc, _, stderr = run_cmd(f"git merge {_q(f'{REMOTE}/{target}')} --no-edit", check=False)
        else:
            rc, _, stderr = run_cmd(f"git rebase {_q(f'{REMOTE}/{target}')}", check=False)
        if rc != 0:
            # Merge/rebase conflicts detected
            log(f"{'Merge' if is_integration else 'Rebase'} conflicts detected for PR #{pr_num}")

            # Get conflicting files
            _, conflict_files_str, _ = run_cmd("git diff --name-only --diff-filter=U", check=False)
            conflict_files = [f for f in conflict_files_str.split("\n") if f.strip()]

            # Abort so the branch is clean for agent resolution
            if is_integration:
                run_cmd("git merge --abort", check=False)
            else:
                run_cmd("git rebase --abort", check=False)

            write_action_request(
                action_path,
                action="resolve_conflicts",
                pr=pr,
                state_file=state_path,
                details={
                    "files": conflict_files,
                    "pre_rebase_ref": pre_rebase_ref,
                    "target": target,
                },
            )
            sm.update_pr(index, status="in_progress", error="rebase conflicts")
            sm._save()
            log(f"Action request written to {action_path}")
            return EXIT_CONFLICTS

        # Post-rebase verification: check for conflict markers and build
        log(f"Post-rebase verification for PR #{pr_num}...")
        # Use git diff --check which detects lines starting with conflict markers
        # (<<<<<<<, =======, >>>>>>>) in the PR diff.  Unlike `git grep '<<<<<<<'`,
        # this ignores binary files, unrelated tracked files, and string literals
        # that happen to contain the marker characters.
        # --check also reports whitespace errors (trailing whitespace, space
        # before tab), which must not read as a failed rebase: only the
        # "leftover conflict marker" lines count. LC_ALL=C pins git's wording.
        rc, markers, _ = run_cmd(
            f"env LC_ALL=C git diff --check {_q(f'{REMOTE}/{target}')}...HEAD", check=False
        )
        if rc != 0 and "leftover conflict marker" in markers:
            log(f"Conflict markers found:\n{markers}")
            run_cmd(f"git reset --hard {_q(pre_rebase_ref)}", check=False)
            sm.update_pr(index, status="failed", error="conflict markers remain after rebase")
            sm.advance()
            return EXIT_OK

        # Build check
        rc, build_stdout, build_stderr = run_build_checks()
        if rc != 0:
            build_log = f"/tmp/merge-train-{ts}-build-{pr_num}.log"
            tail = write_build_log(build_log, build_stdout, build_stderr)
            console_tail = "\n".join(tail.splitlines()[-BUILD_LOG_CONSOLE_LINES:])
            log(f"Build failed after rebase for PR #{pr_num} (full output: {build_log}):\n{console_tail}")
            write_action_request(
                action_path,
                action="remediate_build",
                pr=pr,
                state_file=state_path,
                details={
                    "failure_type": "build_post_rebase",
                    "failing_checks": ["build"],
                    "pre_rebase_ref": pre_rebase_ref,
                    "build_log": build_log,
                    "build_output_tail": tail,
                },
            )
            sm.update_pr(index, status="in_progress", error="build failure after rebase")
            sm._save()
            log(f"Action request written to {action_path}")
            return EXIT_REMEDIATION

        # 3e. Force-push the rebased branch
        log(f"Force-pushing PR #{pr_num}...")
        rc, _, stderr = run_cmd(f"git push --force-with-lease {REMOTE} {_q(branch)}", check=False)
        if rc != 0:
            sm.update_pr(index, status="failed", error="force-push rejected")
            sm.advance()
            return EXIT_OK

        # 3f. Wait for CI to pass
        log(f"Waiting for CI on PR #{pr_num}...")
        rc, stdout, stderr = run_cmd(
            f"scripts/monitor-pr-stability.sh {pr_num} 0",
            check=False,
        )
        if rc != 0:
            return monitor_infrastructure_error(sm, index, pr_num, "post-rebase", stdout, stderr)

        stability = parse_stability_result(stdout)
        if stability["status"] == "checks_failing":
            log(f"PR #{pr_num} has failing CI checks after rebase: {stability['failing_checks']}")
            write_action_request(
                action_path,
                action="remediate_build",
                pr=pr,
                state_file=state_path,
                details={
                    "failure_type": "ci_post_rebase",
                    "failing_checks": stability["failing_checks"],
                    "pre_rebase_ref": pre_rebase_ref,
                },
            )
            sm.update_pr(index, status="in_progress", error=f"CI failing after rebase: {','.join(stability['failing_checks'])}")
            log(f"Action request written to {action_path}")
            return EXIT_REMEDIATION

        if stability["status"] == "comments_found":
            log(f"PR #{pr_num} has {stability['comment_count']} unresolved review comment thread(s) after rebase")
            write_action_request(
                action_path,
                action="resolve_comments",
                pr=pr,
                state_file=state_path,
                details={
                    "failure_type": "comments_post_rebase",
                    "comment_count": stability["comment_count"],
                    "pre_rebase_ref": pre_rebase_ref,
                },
            )
            sm.update_pr(index, status="in_progress", error=f"review comments found ({stability['comment_count']})")
            log(f"Action request written to {action_path}")
            return EXIT_REMEDIATION

    # Retarget the stack BEFORE arming. GitHub's own delete_branch_on_merge
    # still deletes this PR's head branch when it lands, and that deletion
    # retargets rather than closes the open PRs based on it (#2801). The
    # sweep is kept anyway because --auto does not reliably engage: on a PR
    # already at mergeStateStatus CLEAN it merges client-side within seconds
    # of arming, with no auto_merge_enabled event to react to, so a retarget
    # after arming can be too late.
    children_ok, children = find_open_children(branch)
    if not children_ok:
        # A failed lookup is not "no children". Arming on it would delete the
        # branch anyway and let GitHub close whatever is stacked on it.
        sm.update_pr(
            index,
            status="failed",
            error=f"could not list open PRs based on {branch}; not arming auto-merge",
        )
        sm.advance()
        return EXIT_OK
    # Anything a prior, crashed or timed-out invocation of this same PR already
    # swept is already in sweptChildren, which is what the restore paths read:
    # find_open_children(branch) cannot rediscover those, their base being
    # target rather than branch by now.
    for child in children:
        if child == pr_num:
            continue
        log(
            f"Retargeting child PR #{child} from {branch} at {target} "
            f"before merging PR #{pr_num}"
        )
        rc, _, stderr = run_cmd(gh_cmd(f"pr edit {child} --base {_q(target)}"), check=False)
        if rc != 0:
            # Fail this PR rather than merge anyway: an unlanded parent is
            # recoverable, a closed child is not -- GitHub refuses to reopen
            # one until its parent's deleted ref has been recreated.
            return fail_pr_and_restore_children(
                sm,
                index,
                branch,
                target,
                f"retargeting child PR #{child} off {branch} failed: {stderr[:200]}",
            )
        sm.add_swept_child(index, child)
        child_index = sm.index_of(child)
        if child_index is not None:
            # The sticky `stacked` flag is owned by enqueue, not the sweep: a
            # queued child necessarily had base != target when enqueued, so
            # StateManager.init already recorded it. The sweep only updates
            # the base to reflect the retarget it just performed.
            sm.update_pr(child_index, base=target)

    # 3h. Merge the PR. --auto tolerates a required check that is still running
    # from a push made moments earlier (a bare merge is rejected by branch
    # policy). Because --auto returns immediately, wait for GitHub to report
    # MERGED before advancing so the next PR rebases onto a target that
    # actually contains this one (bug #2103).
    #
    # A PR that was ever stacked merges, it does not squash: squashing a parent
    # replaces its commits with one new commit the child does not carry, so the
    # child's diff after retarget would contain the parent's changes again. The
    # decision reads the sticky `stacked` flag, not the current base -- by the
    # time a child arms, the sweep or the top-of-process_pr retarget has already
    # pointed it at the target. A standalone PR squashes, keeping main's history
    # one commit per PR.
    #
    # The same artifact applies to a PR with descendants of its own, even when
    # its own `stacked` flag is False (a stack root: its base was the target
    # from the start). Squashing it would replace its commits with one new
    # commit its children do not carry, putting its changes back into every
    # descendant's diff after retarget -- and the train's own pre-merge rebase
    # of those descendants would then replay the root's original commits onto
    # a target that already holds their squashed equivalent, which conflicts
    # rather than being skipped (patch-ids do not match a squash). `children`
    # was already bound above by find_open_children(branch); a self-entry
    # (child == pr_num) is excluded, matching the sweep loop's own skip. On a
    # --resume, find_open_children() answers "who is based on this branch
    # right now" and returns [] once the sweep has already retargeted the
    # children, so sweptChildren -- the durable record of what this PR swept
    # -- is also consulted to keep the decision stable across a resume.
    descendants = [child for child in children if child != pr_num] or list(
        sm.state["prs"][index].get("sweptChildren", [])
    )
    # gh pr merge lands the PR into its *actual* base on GitHub, not into
    # `target` directly -- re-check the live base immediately before arming so
    # a state bug (or a base changed out-of-band since the checks above) can't
    # record this PR's branch as landed onto target when it in fact merged
    # into some other, possibly-unlanded, ancestor branch.
    rc, arming_base, _ = run_cmd(gh_cmd(f"pr view {pr_num} --json baseRefName -q .baseRefName"), check=False)
    if rc != 0 or arming_base != target:
        return fail_pr_and_restore_children(
            sm,
            index,
            branch,
            target,
            f"refusing to merge: live base is {arming_base or 'unknown'}, not {target}",
        )

    merge_method = "--merge" if pr.get("stacked") or descendants else "--squash"
    log(f"Merging PR #{pr_num} ({merge_method})...")
    rc, _, stderr = run_cmd(gh_cmd(f"pr merge {pr_num} {merge_method} --auto"), check=False)
    merge_result = None
    if rc != 0:
        # A non-zero rc does not prove this PR is unarmed: a --resume after an
        # auto-merge timeout re-issues this call on a PR that is still armed
        # from the first pass. Restoring children then would put them back on a
        # branch GitHub is about to delete -- the closure the sweep exists to
        # prevent -- so ask GitHub before treating this as "never armed".
        armed = auto_merge_state(pr_num)
        if armed is None:
            reason = (
                f"PR #{pr_num} arming returned {rc} and GitHub could not say whether "
                "auto-merge is enabled; neither restoring nor keeping the swept children "
                "is safe without knowing"
            )
            sm.update_pr(index, status="failed", error=reason)
            sm.halt(reason)
            log(f"Fatal: {reason}; stopping the train for manual intervention")
            sys.exit(EXIT_ERROR)
        if armed:
            log(f"PR #{pr_num} arming returned {rc}, but auto-merge is already enabled; waiting on it")
        else:
            # Never armed, so this PR's branch is not going to be deleted and
            # the children must go back onto it -- see restore_children_base().
            # GitHub's enablePullRequestAutoMerge error names neither the base
            # nor the target, so a stale base reads as a ruleset problem
            # without these.
            return fail_pr_and_restore_children(
                sm,
                index,
                branch,
                target,
                f"merge failed (base={pr.get('base')}, target={target}): {stderr[:200]}",
            )
    else:
        # A zero exit is not proof that auto-merge was armed. On a PR already
        # at mergeStateStatus CLEAN, `gh pr merge --auto` exits 0, prints
        # nothing, emits no auto_merge_enabled event, and merges the PR
        # immediately instead (#2801). Ask GitHub rather than assuming.
        # None (GitHub could not say) falls through to the poll: unlike the
        # rc != 0 path the arming call itself succeeded, the children stay
        # swept either way, and wait_for_merge answers definitively -- a
        # transient gh failure must not stop the train.
        if auto_merge_state(pr_num) is False:
            rc_state, live_state, _ = run_cmd(
                gh_cmd(f"pr view {pr_num} --json state -q .state"), check=False
            )
            if rc_state == 0 and live_state == "MERGED":
                log(f"PR #{pr_num} merged immediately rather than arming auto-merge")
                merge_result = "merged"
            elif rc_state == 0 and live_state == "OPEN":
                # Exit 0, no auto-merge recorded, still open: arming did not
                # take. This branch is not going to be deleted, so the swept
                # children must go back onto it -- and the train must not sit
                # out MERGE_POLL_TIMEOUT_SECONDS and then hand off an
                # auto_merge_timeout for a PR where nothing is pending.
                return fail_pr_and_restore_children(
                    sm,
                    index,
                    branch,
                    target,
                    f"auto-merge was not armed: `gh pr merge` exited 0 but GitHub "
                    f"reports no auto-merge and PR #{pr_num} is still OPEN",
                )

    if merge_result is None:
        log(f"Waiting for PR #{pr_num} to reach MERGED state...")
        merge_result = wait_for_merge(pr_num)
    if merge_result == "closed":
        return fail_pr_and_restore_children(
            sm,
            index,
            branch,
            target,
            "PR was closed while waiting for auto-merge (auto-merge cancelled or PR closed)",
        )
    if merge_result == "timeout":
        # A PR that has not merged must never be silently skipped: keep it at
        # the current index so a resume revisits it, and hand off to the agent.
        write_action_request(
            action_path,
            action="remediate_build",
            pr=pr,
            state_file=state_path,
            details={
                "failure_type": "auto_merge_timeout",
                "timeout_seconds": MERGE_POLL_TIMEOUT_SECONDS,
            },
        )
        sm.update_pr(index, status="in_progress", error=f"auto-merge still pending after {MERGE_POLL_TIMEOUT_SECONDS}s")
        sm._save()
        log(f"Action request written to {action_path}")
        return EXIT_REMEDIATION

    sm.update_pr(index, status="merged")
    sm.record_merged_branch(branch)
    sm.clear_swept_children(index)
    sm.advance()

    # Update local target branch
    run_cmd(f"git checkout {_q(target)}", check=False)
    run_cmd(f"git pull {REMOTE} {_q(target)}", check=False)
    if target == "main":
        # origin (Alterna-Dev-Studio/Cholobots) is only a mirror; keep its main in
        # step when it is configured. A failure here never affects the train.
        rc, _, stderr = run_cmd("git push origin main", check=False)
        if rc != 0:
            log(f"Mirror sync (git push origin main) failed, ignoring: {stderr[:200]}")

    log(f"PR #{pr_num} merged successfully")
    return EXIT_OK


def main():
    """Main entry point for the merge-train script."""
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        sys.exit(EXIT_ERROR)

    args = parse_args(sys.argv[1:])

    if args.mode == "resume":
        if not args.resume_path or not os.path.exists(args.resume_path):
            log(f"State file not found: {args.resume_path}")
            sys.exit(EXIT_ERROR)
        sm = StateManager(args.resume_path)
        sm.load()
        state_path = args.resume_path
        log(f"Resumed from {args.resume_path} at index {sm.state['currentIndex']}")
    elif args.mode in ("explicit", "epic"):
        if args.mode == "explicit":
            if not args.pr_numbers:
                log("No PR numbers provided")
                sys.exit(EXIT_ERROR)
            # Fetch PR details from GitHub
            prs = []
            lookup_failures = []
            for pr_num in args.pr_numbers:
                rc, stdout, stderr = run_cmd(
                    gh_cmd(f"pr view {pr_num} --json number,title,headRefName,baseRefName,state,url,body"),
                    check=False,
                )
                if rc != 0:
                    log(f"Failed to fetch PR #{pr_num}: {stderr}")
                    lookup_failures.append(pr_num)
                    continue
                try:
                    pr_data = json.loads(stdout)
                except ValueError:
                    log(f"Unparseable response for PR #{pr_num}: {stdout[:200]}")
                    lookup_failures.append(pr_num)
                    continue
                if pr_data.get("state") != "OPEN":
                    log(f"PR #{pr_num} is {pr_data.get('state', 'unknown')} — skipped")
                    continue
                prs.append({
                    "number": pr_data["number"],
                    "title": pr_data["title"],
                    "branch": pr_data["headRefName"],
                    "body": pr_data.get("body", ""),
                    "base": pr_data.get("baseRefName", args.target),
                })
            if lookup_failures:
                # A transient gh or auth failure must not look like an empty,
                # successful train, or silently drop one PR from a stack.
                log(
                    "Could not look up requested PR(s) "
                    + ", ".join(f"#{n}" for n in lookup_failures)
                    + "; refusing to start a partial train"
                )
                sys.exit(EXIT_ERROR)
        else:
            log(f"Epic mode: discovering PRs for epic #{args.epic_ref}")
            try:
                prs = discover_epic_prs(args.epic_ref)
            except RuntimeError as exc:
                log(str(exc))
                sys.exit(EXIT_ERROR)

        if not prs:
            log("No open PRs to process")
            sys.exit(EXIT_OK)

        if args.dry_run:
            log("Dry run — PR queue:")
            for i, pr in enumerate(prs, 1):
                log(f"  {i}. #{pr['number']} — {pr['title']} (branch: {pr['branch']})")
            sys.exit(EXIT_OK)

        ts = int(time.time())
        # mkstemp makes the name unique and the file private (0600), so two
        # trains started in the same second cannot share or overwrite state.
        fd, state_path = tempfile.mkstemp(prefix=f"merge-train-{ts}-", suffix=".json", dir="/tmp")
        os.close(fd)
        sm = StateManager(state_path)
        sm.init(prs, target=args.target)
        log(f"State file: {state_path}")

    else:
        log("Unknown mode")
        sys.exit(EXIT_ERROR)

    # Step 3: Process each PR sequentially
    target = sm.state["target"]
    while sm.state["currentIndex"] < len(sm.state["prs"]):
        index = sm.state["currentIndex"]
        exit_code = process_pr(sm, index, target, state_path)
        if exit_code != EXIT_OK:
            # Agent intervention needed — script exits for wrapper to handle
            sys.exit(exit_code)

    # Step 4: Final summary
    sm.state["_state_path"] = state_path
    summary = generate_summary(sm.state)
    print(summary)
    sm._save()

    sys.exit(EXIT_OK)


if __name__ == "__main__":
    main()
