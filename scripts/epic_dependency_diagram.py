#!/usr/bin/env python3
# ABOUTME: Merges reported story dependencies into an epic description's dependency diagram, validates it, and renders it.
# ABOUTME: Uses the shared dependency parser for round-trip validation; the caller patches the epic only when this exits 0.
"""
Usage: python3 epic_dependency_diagram.py --epic-json <epic.json> --deps <deps.json> --out <description.md>

Inputs
  --epic-json  `scripts/taiga-epic.sh get <ref>` output (uses its `description`).
  --deps       {"epicRef": 46,
                "stories": [{"ref": 47, "subject": "...", "inEpic": true}, ...],
                "deps": [{"ref": 48, "dependsOn": 47, "reason": "..."}],
                "softDeps": [{"ref": 50, "after": 48, "reason": "..."}]}

Output
  stdout  one JSON object: status (written|unchanged|invalid), errors, graph, added,
          outOfEpic, diagram, softDeps.
  --out   the full new epic description; written only for `written` and `unchanged`
          (`unchanged` holds the original description byte-for-byte).

Existing edges are never removed: the result is the union of the diagram already in the
description and the reported edges. Soft orderings are listed under the diagram but are
never edges.

Exit codes: 0 ok, 2 invalid (nothing written), 1 usage or I/O error.

Fallback mode
  python3 epic_dependency_diagram.py --fallback-state <state.json> --epic-refs 47,48,...
  Reads the /fully-analyze-stories state file and prints {"<ref>": ["upstream", "soft"], ...}:
  for each in-epic story, the dependency categories that were not reported (null, or the
  story is not in the state file) and so must be read from the story's description. Each
  category is decided on its own: `upstreamDeps: []` with `softDeps: null` needs ["soft"].
  Exit 0, or 1 on a usage or I/O error.
"""

import argparse
import json
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "lib"))
from dependency_parser import find_dependency_cycle, parse_dependency_diagram  # noqa: E402

SECTION_RE = re.compile(
    r"^##\s+(?:Dependency\s+(?:Diagram|Graph)|Dependencies)\s*$",
    re.MULTILINE | re.IGNORECASE,
)
NEXT_HEADING_RE = re.compile(r"^##\s+", re.MULTILINE)
CODE_RE = re.compile(r"```[^\n]*\n(.*?)```", re.DOTALL)
SOFT_HEADING = "Soft orderings (not enforced):"
SOFT_BLOCK_RE = re.compile(re.escape(SOFT_HEADING) + r"[ \t]*\n(?:[ \t]*- [^\n]*(?:\n|$))*")
SOFT_BULLET_RE = re.compile(r"^[ \t]*-\s+#(\d+)\s+after\s+#(\d+)(?::\s*(.*))?$", re.MULTILINE)
REF_RUN_RE = re.compile(r"#\d+(?:\s*[,+]\s*#\d+)*")
LABEL_RE = re.compile(r"\((.*?)\)(?=\s*(?:\[outside epic\]|\(see above\)|:|$))")
OUTSIDE = "[outside epic]"


def clean(text):
    """Strip backticks and newlines so a label or reason cannot break the code block."""
    return " ".join(str(text).replace("`", "").split())


def _existing_labels(description):
    """Map ref -> subject text from the first line each ref has in the existing diagram."""
    section = _section_bounds(description)
    if not section:
        return {}
    code = CODE_RE.search(description[section[0]:section[1]])
    if not code:
        return {}
    labels = {}
    for line in code.group(1).split("\n"):
        arrow = re.search(r"[├└]─►", line)
        text = line[arrow.end():] if arrow else line
        run = REF_RUN_RE.match(text.lstrip())
        if not run:
            continue
        refs = [int(r) for r in re.findall(r"#(\d+)", run.group(0))]
        if len(refs) != 1 or refs[0] in labels:
            continue
        label = LABEL_RE.match(text.lstrip()[run.end():].lstrip())
        if label:
            labels[refs[0]] = label.group(1)
    return labels


def _section_bounds(description):
    """(start, end) of the dependency section body, or None."""
    match = SECTION_RE.search(description)
    if not match:
        return None
    start = match.end()
    nxt = NEXT_HEADING_RE.search(description, start)
    return start, nxt.start() if nxt else len(description)


def _existing_soft(description):
    section = _section_bounds(description)
    if not section:
        return {}
    block = SOFT_BLOCK_RE.search(description[section[0]:section[1]])
    if not block:
        return {}
    return {
        (int(a), int(b)): (reason or "").strip()
        for a, b, reason in SOFT_BULLET_RE.findall(block.group(0))
    }


def merge_graph(existing, stories, deps):
    """Union of the existing graph and reported edges; every story and edge ref is a key."""
    graph = {ref: set(d) for ref, d in existing.items()}
    for s in stories:
        graph.setdefault(s["ref"], set())
    for d in deps:
        graph.setdefault(d["ref"], set()).add(d["dependsOn"])
        graph.setdefault(d["dependsOn"], set())
    for ref, dset in list(graph.items()):
        for dep_ref in dset:
            graph.setdefault(dep_ref, set())
    return {ref: sorted(dset) for ref, dset in graph.items()}


def render_diagram(graph, labels, reasons, out_of_epic):
    """Render the /implement-epic format: roots ascending, children under each parent."""
    children = {ref: [] for ref in graph}
    for ref, dlist in graph.items():
        for dep_ref in dlist:
            children[dep_ref].append(ref)
    for ref in children:
        children[ref].sort()

    expanded = set()

    def text(ref, parent=None, again=False):
        out = f"#{ref}"
        label = labels.get(ref)
        if label:
            out += f" ({clean(label)})"
        if ref in out_of_epic:
            out += f" {OUTSIDE}"
        if again:
            out += " (see above)"
        reason = reasons.get((ref, parent)) if parent is not None else None
        if reason:
            out += f": {clean(reason)}"
        return out

    lines = []

    def walk(ref, depth):
        for child in children[ref]:
            indent = " " * (2 + 6 * (depth - 1))
            seen = child in expanded
            lines.append(f"{indent}└─► {text(child, ref, again=seen)}")
            if not seen:
                expanded.add(child)
                walk(child, depth + 1)

    roots = sorted(ref for ref, dlist in graph.items() if not dlist)
    for i, root in enumerate(roots):
        if i:
            lines.append("")
        lines.append(text(root))
        expanded.add(root)
        walk(root, 1)
    return "\n".join(lines)


def _soft_text(soft):
    if not soft:
        return ""
    bullets = []
    for (ref, after) in sorted(soft):
        reason = clean(soft[(ref, after)])
        bullets.append(f"- #{ref} after #{after}" + (f": {reason}" if reason else ""))
    return SOFT_HEADING + "\n" + "\n".join(bullets) + "\n"


def splice(description, block, soft):
    """Put the diagram code block (and soft orderings) into the description."""
    fenced = f"```\n{block}\n```\n"
    soft_text = _soft_text(soft)
    tail = f"\n{soft_text}" if soft_text else ""
    bounds = _section_bounds(description)
    if not bounds:
        base = description.rstrip()
        prefix = base + "\n\n" if base else ""
        return f"{prefix}## Dependency Diagram\n\n{fenced}{tail}"

    start, end = bounds
    body = description[start:end]
    body = SOFT_BLOCK_RE.sub("", body)
    code = CODE_RE.search(body)
    if code:
        before, after = body[:code.start()], body[code.end():]
        after = after.lstrip("\n")
        new_body = before + fenced + tail + ("\n" + after if after else "")
    else:
        rest = body.strip("\n")
        new_body = "\n\n" + fenced + tail + ("\n" + rest + "\n" if rest else "")
    if end < len(description) and not new_body.endswith("\n\n"):
        new_body = new_body.rstrip("\n") + "\n\n"
    return description[:start] + new_body + description[end:]


def build_description(description, stories, deps, soft_deps):
    """Merge, validate and render. Returns the result dict described in the module doc."""
    description = description or ""
    existing = parse_dependency_diagram(description)
    errors = []

    for d in deps:
        if d["ref"] == d["dependsOn"]:
            errors.append(f"#{d['ref']} cannot depend on itself")
    for ref, dlist in existing.items():
        if ref in dlist:
            errors.append(f"#{ref} cannot depend on itself (existing diagram)")

    graph = merge_graph(existing, stories, deps)
    soft = _existing_soft(description)
    for sd in soft_deps:
        key = (sd["ref"], sd["after"])
        if sd["ref"] == sd["after"]:
            errors.append(f"#{sd['ref']} cannot be ordered after itself")
            continue
        soft[key] = sd.get("reason", "") or soft.get(key, "")

    result = {
        "status": "invalid",
        "errors": errors,
        "graph": {str(k): v for k, v in sorted(graph.items())},
        "added": [],
        "outOfEpic": sorted(s["ref"] for s in stories if not s.get("inEpic", True)),
        "diagram": "",
        "softDeps": [[a, b] for a, b in sorted(soft)],
        "description": description,
    }
    if errors:
        return result

    cycle = find_dependency_cycle(graph)
    if cycle:
        errors.append("dependency cycle: " + " -> ".join(f"#{r}" for r in cycle))
        return result

    result["added"] = sorted(
        [ref, dep_ref]
        for ref, dlist in graph.items()
        for dep_ref in dlist
        if dep_ref not in existing.get(ref, [])
    )

    # Compare reasons too: a soft ordering whose reason alone changed must be rewritten.
    if graph == existing and soft == _existing_soft(description):
        result["status"] = "unchanged"
        result["diagram"] = _current_block(description)
        return result

    labels = _existing_labels(description)
    for s in stories:
        if s.get("subject"):
            labels[s["ref"]] = s["subject"]
    reasons = {(d["ref"], d["dependsOn"]): d.get("reason", "") for d in deps}
    block = render_diagram(graph, labels, reasons, set(result["outOfEpic"]))
    new_description = splice(description, block, soft)

    if parse_dependency_diagram(new_description) != graph:
        errors.append("rendered diagram does not round-trip through parse_dependency_diagram")
    missing = sorted(s["ref"] for s in stories if s.get("inEpic", True)
                     and s["ref"] not in parse_dependency_diagram(new_description))
    if missing:
        errors.append("in-epic stories missing from the diagram: " + ", ".join(f"#{r}" for r in missing))
    if errors:
        return result

    result.update(status="written", diagram=block, description=new_description)
    return result


def _current_block(description):
    bounds = _section_bounds(description)
    if not bounds:
        return ""
    code = CODE_RE.search(description[bounds[0]:bounds[1]])
    return code.group(1).rstrip("\n") if code else ""


def _load_json(path):
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError) as exc:
        print(f"ERROR: cannot read {path}: {exc}", file=sys.stderr)
        sys.exit(1)


def description_fallback(state_stories, epic_refs):
    """Map each in-epic ref to the dependency categories it must take from its description.

    A category is unreported when its field is null or the story is absent from the state
    file. Each category is decided on its own, so a story that reported `upstreamDeps: []`
    but `softDeps: null` still falls back for its soft orderings. Refs with nothing to
    recover are omitted.
    """
    by_ref = {s["ref"]: s for s in state_stories}
    fallback = {}
    for ref in epic_refs:
        entry = by_ref.get(ref, {})
        missing = [
            name for name, field in (("upstream", "upstreamDeps"), ("soft", "softDeps"))
            if entry.get(field) is None
        ]
        if missing:
            fallback[ref] = missing
    return fallback


def _fallback_main(args):
    state = _load_json(args.fallback_state)
    try:
        refs = [int(r) for r in args.epic_refs.split(",") if r.strip()]
        result = description_fallback(state.get("stories", []), refs)
    except (KeyError, TypeError, AttributeError, ValueError) as exc:
        print(f"ERROR: malformed input: {exc!r}", file=sys.stderr)
        return 1
    print(json.dumps({str(ref): cats for ref, cats in result.items()}))
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description="Merge and validate an epic dependency diagram.")
    parser.add_argument("--epic-json")
    parser.add_argument("--deps")
    parser.add_argument("--out")
    parser.add_argument("--fallback-state")
    parser.add_argument("--epic-refs")
    args = parser.parse_args(argv)

    # Usage errors exit 1, never argparse's 2, which this script reserves for `invalid`.
    merge_args = (args.epic_json, args.deps, args.out)
    if args.fallback_state or args.epic_refs:
        if any(merge_args) or not (args.fallback_state and args.epic_refs):
            print("ERROR: fallback mode takes exactly --fallback-state and --epic-refs", file=sys.stderr)
            return 1
        return _fallback_main(args)
    if not all(merge_args):
        print("ERROR: --epic-json, --deps and --out are required", file=sys.stderr)
        return 1

    epic = _load_json(args.epic_json)
    spec = _load_json(args.deps)
    try:
        result = build_description(
            epic.get("description") or "",
            spec.get("stories", []),
            spec.get("deps", []),
            spec.get("softDeps", []),
        )
    except (KeyError, TypeError, AttributeError) as exc:
        print(f"ERROR: malformed input: {exc!r}", file=sys.stderr)
        return 1

    description = result.pop("description")
    print(json.dumps(result, ensure_ascii=False))
    if result["status"] == "invalid":
        return 2
    try:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(description)
    except OSError as exc:
        print(f"ERROR: cannot write {args.out}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
