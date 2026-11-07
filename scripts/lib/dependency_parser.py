# ABOUTME: Shared dependency diagram parser for epic and cross-project wiki markdown.
# ABOUTME: Extracts inter-story dependency trees from ## Dependency Diagram sections; supports opt-in namespaced refs.
import re


def parse_dependency_diagram(description, default_namespace=None):
    """Parse a dependency diagram from an epic description's markdown.

    Looks for a ``## Dependency Diagram``, ``## Dependency Graph``, or
    ``## Dependencies`` section, extracts the fenced code block within it,
    and builds a dict mapping each story ref to a list of refs it depends on.

    The diagram format uses indented trees with ``└─►`` and ``├─►`` arrows::

        #100 (Root)
          ├─► #200 (Child of 100)
          │     └─► #300 (Child of 200)
          └─► #400 (Another child of 100)

    Multiple refs on one line (``#200, #300, #400``) are treated as siblings
    that all share the same parent.

    Only the leading run of refs in a line's node text is treated as a node
    declaration; refs in trailing annotation prose (``(also needs #200)``,
    ``[reads #200 + #300 data]``) are ignored and never become graph edges.
    This leading-run rule applies identically in both modes below.

    Returns an empty dict if no section is found or it cannot be parsed.

    Two modes, controlled by ``default_namespace``:

    * ``default_namespace is None`` (legacy mode): the leading-run token is
      a bare ``#\\d+`` and keys/values are plain ``int``. This is the
      byte-unchanged code path ``merge_train.py`` and ``implement-epic``'s
      Step 2a rely on. A node whose leading run
      starts with a namespace prefix (``alterna#330``) does not match this
      token and is not registered as a node in legacy mode -- namespaced
      refs require ``default_namespace`` mode.
    * ``default_namespace`` set to a project slug (``"cholobots"``,
      ``"homelab"``, ``"alterna"``): the leading-run token becomes
      an optional namespace prefix plus ``#\\d+``; a bare ``#368`` is
      treated as ``<default_namespace>#368``. Every key and every dep-list
      element is a ``str`` of the form ``"<ns>#<n>"`` -- never mixed with
      ints. Sort order in this mode is ``(namespace, int(number))`` (see
      ``split_ref``), so ``alterna#4 < alterna#33 < alterna#330`` rather
      than the lexicographic string order.

    Namespace -> Taiga project id mapping (for callers that resolve refs to
    API URLs): ``cholobots``=39 (other namespaces belong to other Taiga projects).
    """
    if not description:
        return {}

    # Find the dependency section (## Dependency Diagram/Graph or ## Dependencies)
    section_match = re.search(
        r"^##\s+(?:Dependency\s+(?:Diagram|Graph)|Dependencies)\s*$",
        description,
        re.MULTILINE | re.IGNORECASE,
    )
    if not section_match:
        return {}

    # Extract content from section start to the next ## heading (or end)
    section_start = section_match.end()
    next_heading = re.search(r"^##\s+", description[section_start:], re.MULTILINE)
    section_text = description[section_start:section_start + next_heading.start()] if next_heading else description[section_start:]

    # Find the fenced code block
    code_match = re.search(r"```[^\n]*\n(.*?)```", section_text, re.DOTALL)
    if not code_match:
        return {}

    code_block = code_match.group(1)
    return _parse_dependency_tree(code_block, default_namespace=default_namespace)


ARROW_RE = re.compile(r"[├└]─►")

# Leading-run tokens: a node declaration is the leading run of refs joined
# only by "," or "+" (e.g. "#714, #715, #719"). The run ends at the first
# non-ref token, so refs in trailing prose ("(also needs #2209)",
# "[reads #2170 + #2173 data]") are annotations, not nodes, and never become
# graph edges. This applies identically to both the legacy and namespaced
# token grammars below.
_REF = r"#\d+"  # legacy token
_NS_REF = r"(?:[A-Za-z][A-Za-z0-9_-]*)?#\d+"  # namespaced token


def split_ref(ref):
    """Split a namespaced ref (``"cholobots#24"``) into ``("cholobots", 24)``.

    Namespace -> Taiga project id: ``cholobots``=39. Used as the sort key in namespaced mode so the
    parser emits ``alterna#4, alterna#33, alterna#330`` rather than the
    lexicographic ``alterna#33, alterna#330, alterna#4``.
    """
    namespace, _, number = ref.partition("#")
    return namespace, int(number)


def _make_node_refs(default_namespace):
    """Build a node-ref extractor for the given mode.

    Returns a function mapping line text to the leading run of refs it
    starts with (``[]`` if it doesn't start with one), in the type
    appropriate for the mode: ``int`` in legacy mode, namespaced ``str`` in
    ``default_namespace`` mode.
    """
    token = _REF if default_namespace is None else _NS_REF
    run_re = re.compile(rf"{token}(?:\s*[,+]\s*{token})*")

    if default_namespace is None:
        def extract(run):
            return [int(m) for m in re.findall(r"#(\d+)", run)]
    else:
        tok_re = re.compile(r"(?:([A-Za-z][A-Za-z0-9_-]*))?#(\d+)")

        def extract(run):
            return [f"{ns or default_namespace}#{num}" for ns, num in tok_re.findall(run)]

    def node_refs(text):
        match = run_re.match(text.lstrip())
        return extract(match.group(0)) if match else []

    return node_refs


def _parse_dependency_tree(code_block, default_namespace=None):
    """Parse a dependency tree from a code block into a ref->deps mapping.

    Processes lines containing ref tokens and tracks indentation to
    determine parent-child relationships via ``├─►`` and ``└─►`` arrows. A
    ref at an indented arrow level is a child of every ref on the nearest
    line at a shallower indent level above it. Only the leading run of refs in a
    line's node text is a node declaration; a line with no arrow is a root
    only if it starts with a ref, otherwise it is ignored (not treated as
    a new root, and it does not reset the ancestry stack).

    ``default_namespace`` switches between the two token grammars documented
    on ``parse_dependency_diagram``.
    """
    node_refs = _make_node_refs(default_namespace)
    sort_key = (lambda ref: ref) if default_namespace is None else split_ref

    deps = {}  # ref -> set of dependency refs
    # Stack of (indent_level, refs) pairs tracking the current ancestry
    stack = []

    for line in code_block.split("\n"):
        if not line.strip():
            continue

        arrow_match = ARROW_RE.search(line)
        if arrow_match:
            # Child node — refs must lead the text after the arrow.
            refs_on_line = node_refs(line[arrow_match.end():])
            if not refs_on_line:
                continue
            indent = arrow_match.start()
            while stack and stack[-1][0] >= indent:
                stack.pop()
            parent_refs = stack[-1][1] if stack else []
            for ref in refs_on_line:
                deps.setdefault(ref, set()).update(parent_refs)
            # Push all siblings as the parent set for deeper children.
            stack.append((indent, refs_on_line))
        else:
            # A root only if the line *starts* with refs. A ref-bearing line
            # that starts with anything else is a continuation/annotation
            # (e.g. "│   [needs #200 + #300]") and must not reset the stack.
            refs_on_line = node_refs(line)
            if not refs_on_line:
                continue
            indent = len(line) - len(line.lstrip())
            for ref in refs_on_line:
                deps.setdefault(ref, set())
            stack = [(indent, refs_on_line)]

    # Convert sets to sorted lists for deterministic output
    return {ref: sorted(dep_set, key=sort_key) for ref, dep_set in deps.items()}


def find_dependency_cycle(deps_map):
    """Return a cycle as a list of refs [a, ..., a], or None if the graph is acyclic.

    Refs that appear only as dependencies (not as keys) are ignored — they
    refer to stories outside the current set.
    """
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {ref: WHITE for ref in deps_map}
    path = []

    def dfs(node):
        color[node] = GRAY
        path.append(node)
        for dep in deps_map.get(node, []):
            if dep not in color:
                continue
            if color[dep] == GRAY:
                return path[path.index(dep):] + [dep]
            if color[dep] == WHITE:
                found = dfs(dep)
                if found:
                    return found
        color[node] = BLACK
        path.pop()
        return None

    for ref in sorted(deps_map):
        if color[ref] == WHITE:
            found = dfs(ref)
            if found:
                return found
    return None


def resolve_out_of_run_dependencies(deps_map, eligible_refs, done_refs):
    """Split a run's dependencies into resolved in-run deps and blocked stories.

    ``deps_map`` maps ref -> [dep refs], as returned by
    ``parse_dependency_diagram``. It may include refs outside this run, and may
    omit eligible refs that have no dependencies. ``eligible_refs`` is the run's
    eligible story list. ``done_refs`` are the refs verified Done (merged) in
    Taiga -- only those out-of-run deps are safe to drop, because only a merged
    prerequisite's changes are already in the base branch.

    Returns ``(run_deps, excluded)``:

    ``run_deps``
        ``{ref: [in-run dep refs]}`` for every eligible ref that is not
        excluded, with an empty list when it has none. Diagram order preserved.
    ``excluded``
        ``{ref: blocking_ref}`` for each eligible story that cannot run. The
        blocking ref is an out-of-run prerequisite that is not Done, or an
        excluded in-run story it depends on (transitively).
    """
    eligible = set(eligible_refs)
    done = set(done_refs)
    excluded = {}

    for ref in eligible:
        for dep in deps_map.get(ref, []):
            if dep not in eligible and dep not in done:
                excluded[ref] = dep
                break

    # Cascade: a story depending on an excluded in-run story cannot run either.
    changed = True
    while changed:
        changed = False
        for ref in eligible:
            if ref in excluded:
                continue
            for dep in deps_map.get(ref, []):
                if dep in excluded:
                    excluded[ref] = dep
                    changed = True
                    break

    run_deps = {}
    for ref in eligible:
        if ref in excluded:
            continue
        run_deps[ref] = [
            dep
            for dep in deps_map.get(ref, [])
            if dep in eligible and dep not in excluded
        ]
    return run_deps, excluded
