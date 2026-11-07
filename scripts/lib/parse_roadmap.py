#!/usr/bin/env python3
# ABOUTME: Permission-scoped CLI over dependency_parser.parse_dependency_diagram —
# ABOUTME: reads diagram markdown from stdin (or a file) and prints the graph as JSON.
"""parse_roadmap.py

Exists so conductor-tier agents can be granted exactly
`Bash(python3 scripts/lib/parse_roadmap.py *)` instead of an arbitrary-code
`python3 -c` / bare `python3` surface. Content arrives on stdin by default so
a caller without a file-write grant can pipe a diagram straight in; pass a
file path when the content is already on disk, which is what
roadmap-wiki-update does to keep the page out of model output.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from dependency_parser import parse_dependency_diagram  # noqa: E402

# Same slug grammar as the namespace prefix of dependency_parser._NS_REF.
_NAMESPACE_SLUG = re.compile(r"[A-Za-z][A-Za-z0-9_-]*")


def namespace_slug(value):
    if not _NAMESPACE_SLUG.fullmatch(value):
        raise argparse.ArgumentTypeError(
            f"invalid namespace slug {value!r} (expected a letter followed by letters, digits, '_' or '-')"
        )
    return value


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Parse a '## Dependency Diagram' section into a JSON dependency graph."
    )
    parser.add_argument(
        "file",
        nargs="?",
        default="-",
        help="input file, or '-' (default) to read stdin",
    )
    parser.add_argument(
        "--default-namespace",
        default=None,
        type=namespace_slug,
        help="enable namespaced mode with this default project slug "
        "(e.g. cholobots); omit for legacy bare-int mode",
    )
    args = parser.parse_args(argv)

    if args.file == "-":
        content = sys.stdin.read()
    else:
        try:
            content = pathlib.Path(args.file).read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            parser.error(f"cannot read {args.file}: {exc}")

    print(json.dumps(parse_dependency_diagram(content, default_namespace=args.default_namespace)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
