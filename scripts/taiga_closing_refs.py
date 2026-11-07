#!/usr/bin/env python3
# ABOUTME: Extract the Taiga refs a merged PR claims to close, from its title, body, and branch.
# ABOUTME: A bare #ref mention is never a claim — only our conventional claim positions count.
import os
import re
import sys

# A ref counts only where our PR conventions put one to mean "this PR is the
# work for that item". A bare #ref anywhere else is a mention: PR #839 named
# #2204 and #2206 in a docs title and wrongly closed #2204 and its 8 tasks.
TITLE_CLAIM_PATTERNS = [
    # "#2256: Accrual idempotency ..." — story PR title (finalize-story-pr)
    re.compile(r"^\s*#(\d+)(?=[:\s]|$)"),
    # "fix(#2119): name the curl exit code" — conventional-commit scope
    re.compile(r"^\s*[a-z]+\(#(\d+)\)\s*:"),
    # "fix: Dashboard layout (bug #1350)" — trailing parenthetical
    re.compile(r"\((?:bug|story|issue|task)?\s*#(\d+)\)\s*$", re.IGNORECASE),
]

# A body line that is *nothing but* a closing declaration:
#   "Fixes bug [#2269](https://backlog.growthgarage.org/project/cholo-bots/issue/2269)"
# Whole-line anchoring is the point: a bug PR body quotes the Taiga description
# under "## Bug context", and such prose contains sentences like "wrongly closed
# story #2204 and its 8 tasks" that must not read as a claim.
BODY_CLAIM_PATTERN = re.compile(
    r"^\s*(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)\s+"
    r"(?:bug|story|issue|task)?\s*\[?#(\d+)\]?"
    r"(?:\s*\([^)]*\))?\s*[.:]?\s*$",
    re.IGNORECASE,
)

# Only branch prefixes that carry implementation. "docs/story-2206-..." and
# "design/epic-2167-..." name an item without implementing it, and
# "dependabot/..." never refers to Taiga at all.
# Both branch shapes claim: "fix/<ref>-<slug>" (this repo's convention) and
# "fix/<slug>-<ref>" (you-thon's). A leading ref wins when both are present.
BRANCH_CLAIM_PATTERN = re.compile(r"^(?:feature|fix)/(?:(\d+)(?:[-/_].*)?|.*[-/_](\d+))$")


def extract_closing_refs(branch, title, body):
    """Refs this PR claims to close, in the order found, de-duplicated."""
    refs = []

    for pattern in TITLE_CLAIM_PATTERNS:
        match = pattern.search(title or "")
        if match:
            refs.append(int(match.group(1)))

    for line in (body or "").splitlines():
        match = BODY_CLAIM_PATTERN.match(line)
        if match:
            refs.append(int(match.group(1)))

    # The branch is the weakest signal, so it only speaks when nothing else did.
    if not refs and branch:
        match = BRANCH_CLAIM_PATTERN.match(branch)
        if match:
            refs.append(int(match.group(1) or match.group(2)))

    seen = set()
    return [ref for ref in refs if not (ref in seen or seen.add(ref))]


def main():
    refs = extract_closing_refs(
        os.environ.get("PR_BRANCH", ""),
        os.environ.get("PR_TITLE", ""),
        os.environ.get("PR_BODY", ""),
    )
    print(" ".join(str(ref) for ref in refs))
    return 0


if __name__ == "__main__":
    sys.exit(main())
