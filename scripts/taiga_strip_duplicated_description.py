# ABOUTME: Strips the duplicated copy of a Taiga item's prior description that analyze/design skills appended below a horizontal rule.
# ABOUTME: Dry-run by default; verifies each candidate against the item's version history before writing, and reports the rest for manual review.
import argparse
import difflib
import html
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode

from taiga_html_to_markdown import html_to_markdown


DEFAULT_OUT_DIR = Path("output/taiga-strip")
# Each pass names its own reports, so a dry run never writes over the record of
# an apply run and the two passes never write over each other.
STRIP_MODE = "strip"
RE_CLEAN_MODE = "re-clean"
DEFAULT_PROJECT_ID = 39
DEFAULT_THRESHOLD = 0.9
ANALYZED_TAG = "llm-analyzed"
REQUEST_ATTEMPTS = 4
RETRY_BACKOFF_SECONDS = 1.0
# curl's exit code for "-f" (fail on HTTP error): the server answered with a 4xx/5xx,
# so retrying won't change the outcome. Every other non-zero exit is a transport-level
# failure (DNS, connection refused, timeout, ...) and is worth retrying.
CURL_HTTP_ERROR_EXIT_CODE = 22

KINDS = ("userstories", "issues")

# Taiga's history endpoint is keyed by a singular item type, unlike the list and
# detail endpoints.
HISTORY_TYPES = {
    "userstories": "userstory",
    "issues": "issue",
}

DECISION_STRIP = "strip"
DECISION_MANUAL_REVIEW = "manual-review"
DECISION_NO_SEPARATOR = "no-separator"
DECISION_NO_HISTORY = "no-history"
DECISION_SKIPPED_CONFLICT = "skipped-version-conflict"
# Re-clean decisions: restoring what an earlier truncating run removed.
DECISION_REPAIRED = "repaired"
DECISION_NO_CHANGE = "no-change"

DECISIONS = (
    DECISION_STRIP,
    DECISION_MANUAL_REVIEW,
    DECISION_NO_SEPARATOR,
    DECISION_NO_HISTORY,
    DECISION_SKIPPED_CONFLICT,
    DECISION_REPAIRED,
    DECISION_NO_CHANGE,
)

FENCE_RE = re.compile(r"^\s{0,3}(```|~~~)")
RULE_RE = re.compile(r"^-{3,}\s*$")
HEADING_RE = re.compile(r"^\s{0,3}(#{1,6})\s+(.*?)\s*#*\s*$")

# Sections at this depth or shallower open a new top-level section; deeper
# headings stay inside the section that owns them.
TOP_LEVEL_HEADING_DEPTH = 2

VERIFICATION_HISTORY = "history"
VERIFICATION_STRUCTURE = "structure"

# The headings the analyze/design skills open their written-back section with.
# Text under one of these is the plan; text under anything else is the original
# the skill preserved below the rule.
PLAN_HEADINGS = (
    "## implementation plan",
    "## implementation analysis",
    "## analysis",
    "## analysis findings",
    "## design",
    "## rejection",
)


class TaigaStripError(RuntimeError):
    """Raised when the description cleanup cannot continue."""


@dataclass(frozen=True)
class Candidate:
    """A top-level horizontal rule and the text that follows it."""

    start: int
    tail: str


@dataclass(frozen=True)
class Reconstruction:
    """What a re-clean pass decided for one item.

    `description` is the text to write, or None when there is nothing to write.
    """

    decision: str
    description: Optional[str]
    reason: Optional[str] = None


@dataclass(frozen=True)
class Section:
    """A run of markdown owned by one top-level heading.

    `heading` is the normalised heading text, or None for text that precedes the
    first heading.
    """

    heading: Optional[str]
    text: str


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Strip duplicated prior descriptions from analyzed Taiga items.",
    )
    parser.add_argument(
        "--project-id",
        type=int,
        default=DEFAULT_PROJECT_ID,
        help="Taiga project id to clean up (default: 39).",
    )
    parser.add_argument(
        "--kind",
        choices=[*KINDS, "both"],
        default="both",
        help="Item kind to process (default: both).",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_THRESHOLD,
        help="Similarity at or above which a tail counts as a duplicate (default: 0.9).",
    )
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_OUT_DIR),
        help="Directory for generated report artifacts (default: output/taiga-strip).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the stripped descriptions back to Taiga. Without this flag the run is a dry run.",
    )
    parser.add_argument(
        "--re-clean",
        action="store_true",
        help=(
            "Restore what an earlier truncating run removed: rebuild each item's pre-strip "
            "description from history and fold the removed tail back in."
        ),
    )
    return parser.parse_args(argv)


# curl flags whose following argument carries a request body (the Taiga auth
# payload includes the password).
SENSITIVE_DATA_FLAGS = {"-d", "--data", "--data-binary", "--data-raw"}


def redact_cmd(cmd: List[str]) -> str:
    """Render a command for error messages with credentials masked.

    The curl invocations carry the Taiga password (in the auth request body) and
    Bearer tokens (in Authorization headers); these must never reach logs or
    terminal history via an exception message.
    """
    redacted: List[str] = []
    mask_next = False

    for arg in cmd:
        if mask_next:
            redacted.append("<redacted>")
            mask_next = False
        elif arg in SENSITIVE_DATA_FLAGS:
            redacted.append(arg)
            mask_next = True
        elif arg.lower().startswith("authorization:"):
            redacted.append("Authorization: <redacted>")
        else:
            redacted.append(arg)

    return " ".join(redacted)


def _quote_config_value(value: str) -> str:
    """Quote a value for a curl config file (backslash and double quote are escaped)."""
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def bearer_config(token: str, body: Optional[str] = None) -> str:
    """A curl config (read from stdin with ``-K -``) carrying the bearer header and optional body.

    Putting the token and the request body in a config on stdin keeps both out
    of curl's argv, which any local user can read from /proc/<pid>/cmdline.
    """
    config = f"header = {_quote_config_value(f'Authorization: Bearer {token}')}\n"

    if body is not None:
        config += f"data-binary = {_quote_config_value(body)}\n"

    return config


def _run_curl(
    cmd: List[str],
    attempts: int = REQUEST_ATTEMPTS,
    stdin_text: Optional[str] = None,
) -> "subprocess.CompletedProcess[str]":
    """Run a curl command, retrying transport failures; return the successful process.

    A full cleanup run makes two requests per item across hundreds of items, and
    the occasional connection is refused. Retrying with a backoff keeps a whole
    run from dying on one dropped connection; a non-zero exit that persists
    across every attempt is still an error. A deterministic HTTP error (curl
    exit code 22, from "-f") is not retried — the server already answered, and
    retrying would just add backoff delay before failing anyway.

    ``stdin_text`` carries anything sensitive (the login body, the bearer
    header) so it never appears in the command line.
    """
    for attempt in range(1, attempts + 1):
        result = subprocess.run(cmd, input=stdin_text, check=False, capture_output=True, text=True)

        if result.returncode == 0:
            return result

        if result.returncode == CURL_HTTP_ERROR_EXIT_CODE or attempt == attempts:
            raise TaigaStripError(
                f"Command failed ({result.returncode}) after {attempt} attempt(s): "
                f"{redact_cmd(cmd)}\n{result.stderr.strip()}",
            )

        time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    raise TaigaStripError(f"Command was never attempted: {redact_cmd(cmd)}")


def run_json_command(
    cmd: List[str],
    attempts: int = REQUEST_ATTEMPTS,
    stdin_text: Optional[str] = None,
) -> object:
    """Run a curl command and parse its JSON, retrying transport failures."""
    result = _run_curl(cmd, attempts, stdin_text)

    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise TaigaStripError(
            f"Command produced invalid JSON: {redact_cmd(cmd)}",
        ) from exc


def run_status_command(
    cmd: List[str],
    attempts: int = REQUEST_ATTEMPTS,
    stdin_text: Optional[str] = None,
) -> Tuple[int, str]:
    """Run a curl command that appends its HTTP status line; return (status, body).

    The command omits ``-f`` so an HTTP error comes back as a status rather than
    a curl failure; the paging loop needs that to tell the 404 Taiga answers
    past the last page from any other error.
    """
    result = _run_curl(cmd, attempts, stdin_text)
    body, _, status_text = result.stdout.rpartition("\n")

    try:
        return int(status_text.strip()), body
    except ValueError as exc:
        raise TaigaStripError(
            f"Command did not report an HTTP status: {redact_cmd(cmd)}",
        ) from exc


def get_required_env(name: str) -> str:
    value = os.environ.get(name)

    if not value:
        raise TaigaStripError(f"Missing required environment variable: {name}")

    return value


class TaigaClient:
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip("/")
        self.token = self._authenticate(username, password)

    def _authenticate(self, username: str, password: str) -> str:
        payload = {
            "type": "normal",
            "username": username,
            "password": password,
        }
        # The body travels on stdin so the password never appears in curl's argv.
        response = run_json_command(
            [
                "curl",
                "-sf",
                "-H",
                "Content-Type: application/json",
                "-X",
                "POST",
                "--data-binary",
                "@-",
                f"{self.base_url}/api/v1/auth",
            ],
            stdin_text=json.dumps(payload),
        )
        token = response.get("auth_token")

        if not token:
            raise TaigaStripError("Taiga auth response did not include auth_token")

        return token

    def _url(self, path: str, params: Dict[str, object]) -> str:
        url = f"{self.base_url}{path}"

        if params:
            url = f"{url}?{urlencode(params)}"

        return url

    def get(self, path: str, **params: object) -> object:
        # The bearer token goes in a curl config on stdin, keeping it out of argv.
        return run_json_command(
            ["curl", "-sf", "-K", "-", self._url(path, params)],
            stdin_text=bearer_config(self.token),
        )

    def list_all(self, path: str, **params: object) -> List[dict]:
        items: List[dict] = []
        page = 1

        # Taiga may cap page_size below the requested value (30 rows is common), so a
        # short page is not the last page. Page until an empty page, or the 404 Taiga
        # answers past the end; a 404 on page 1 is a bad path, not an empty collection.
        while True:
            status, body = run_status_command(
                [
                    "curl",
                    "-s",
                    "-w",
                    "\n%{http_code}",
                    "-K",
                    "-",
                    self._url(path, {"page": page, "page_size": 100, **params}),
                ],
                stdin_text=bearer_config(self.token),
            )

            if status == 404 and page > 1:
                break

            if status >= 400:
                raise TaigaStripError(f"Listing {path} failed (HTTP {status}) on page {page}")

            try:
                batch = json.loads(body)
            except json.JSONDecodeError as exc:
                raise TaigaStripError(f"Invalid JSON listing {path} on page {page}") from exc

            if not isinstance(batch, list):
                raise TaigaStripError(f"Expected list response for {path}")

            if not batch:
                break

            items.extend(batch)
            page += 1

        return items

    def get_item(self, kind: str, item_id: int) -> dict:
        item = self.get(f"/api/v1/{kind}/{item_id}")

        if not isinstance(item, dict):
            raise TaigaStripError(f"Expected dict response for {kind}/{item_id}")

        return item

    def get_history(self, kind: str, item_id: int) -> List[dict]:
        history = self.get(f"/api/v1/history/{HISTORY_TYPES[kind]}/{item_id}")

        if not isinstance(history, list):
            raise TaigaStripError(f"Expected list response for history of {kind}/{item_id}")

        return history

    def patch_description(self, kind: str, item_id: int, description: str, version: int) -> None:
        """PATCH the description, passing the version Taiga requires for optimistic concurrency."""
        payload = {"description": description, "version": version}
        # Token and body (the item's description) go in a curl config on stdin.
        run_json_command(
            [
                "curl",
                "-sf",
                "-K",
                "-",
                "-H",
                "Content-Type: application/json",
                "-X",
                "PATCH",
                f"{self.base_url}/api/v1/{kind}/{item_id}",
            ],
            stdin_text=bearer_config(self.token, json.dumps(payload)),
        )


def normalise_for_comparison(text: Optional[str]) -> str:
    """Reduce markdown or rendered HTML to comparable plain text.

    Taiga's history returns the prior description as rendered HTML while the live
    description is markdown, so the two can only be compared once both are
    stripped of their respective syntax.
    """
    if not text:
        return ""

    # Entities resolve first so both sides of a comparison reach the tag pass in
    # the same shape: rendered HTML escapes the angle brackets that appear raw in
    # the markdown it was rendered from.
    normalised = html.unescape(text)
    normalised = re.sub(r"<br\s*/?>", " ", normalised, flags=re.I)
    # Only markup opens a tag. Text carrying a raw `<` — a conflict marker, a
    # `<1s` timing — must not be eaten as far as the next `>`. A tag leaves nothing
    # behind: `A`/`B` is one word, and so is its <code>A</code>/<code>B</code>
    # rendering. Taiga's renderer puts a newline between block elements, which is
    # what keeps them apart.
    normalised = re.sub(r"</?[A-Za-z!][^>]*>", "", normalised)
    # Fence markers and their info strings.
    normalised = re.sub(r"^\s{0,3}(```|~~~).*$", " ", normalised, flags=re.M)
    # Table alignment rows have no rendered-HTML counterpart.
    normalised = re.sub(r"^\s*[-:|\s]+$", " ", normalised, flags=re.M)
    # Images and links keep their label only.
    normalised = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", normalised)
    normalised = re.sub(r"^\s{0,3}#{1,6}\s+", " ", normalised, flags=re.M)
    normalised = re.sub(r"^\s{0,3}>\s?", " ", normalised, flags=re.M)
    normalised = re.sub(r"^\s*(?:[-*+]|\d+[.)])\s+", " ", normalised, flags=re.M)
    normalised = re.sub(r"[*_`~|]", "", normalised)
    normalised = re.sub(r"\s+", " ", normalised)
    # Stripping a tag leaves a space the markdown never had, on whichever side of
    # the tag the punctuation sat.
    normalised = re.sub(r"\s+([.,;:!?)\]])", r"\1", normalised)
    normalised = re.sub(r"([(\[])\s+", r"\1", normalised)
    return normalised.strip().lower()


def iter_lines_outside_fences(text: str):
    """Yield (offset, line) for every line, flagging the ones inside a fenced block.

    Yields (offset, line, in_fence) so callers agree on where the code blocks are.
    A fence opens on a marker line and closes only on a line that is the marker
    itself: a nested example fence carrying an info string (```` ```sh ````) must
    not close the block that is displaying it.
    """
    offset = 0
    fence: Optional[str] = None

    for line in text.splitlines(keepends=True):
        fence_match = FENCE_RE.match(line)
        opens_or_closes = False

        if fence_match:
            if fence is None:
                fence = fence_match.group(1)
                opens_or_closes = True
            elif line.strip() == fence:
                fence = None
                opens_or_closes = True

        yield offset, line, fence is not None or opens_or_closes
        offset += len(line)


def split_candidates(description: str) -> List[Candidate]:
    """Return one candidate per top-level horizontal rule, each carrying the text after it.

    Rules inside fenced code blocks are skipped — plans legitimately contain
    fenced blocks whose contents include `---`.
    """
    if not description:
        return []

    return [
        Candidate(start=offset, tail=description[offset + len(line) :])
        for offset, line, in_fence in iter_lines_outside_fences(description)
        if not in_fence and RULE_RE.match(line.strip())
    ]


def extract_description_change(history: List[dict]) -> Optional[Tuple[str, str]]:
    """Return (before, after) HTML for the newest history entry that changed the description."""
    entries = sorted(history, key=lambda entry: entry.get("created_date") or "", reverse=True)

    for entry in entries:
        diff = (entry.get("values_diff") or {}).get("description_html")

        if isinstance(diff, list) and diff:
            return diff[0], diff[1] if len(diff) > 1 else ""

    return None


def extract_prior_description_html(history: List[dict]) -> Optional[str]:
    """Return the pre-change HTML from the newest history entry that changed the description."""
    change = extract_description_change(history)
    return change[0] if change else None


def similarity(tail: str, prior_html: Optional[str]) -> float:
    return difflib.SequenceMatcher(
        None,
        normalise_for_comparison(tail),
        normalise_for_comparison(prior_html),
    ).ratio()


def choose_strip_point(
    candidates: List[Candidate],
    prior_html: Optional[str],
    threshold: float,
) -> Optional[Tuple[Candidate, float]]:
    """Return the earliest candidate whose tail matches the prior description, with its similarity.

    Taking the earliest qualifying rule removes stacked copies (items that were
    both designed and analyzed) in a single pass.
    """
    for candidate in candidates:
        ratio = similarity(candidate.tail, prior_html)

        if ratio >= threshold:
            return candidate, ratio

    return None


def starts_with_plan_heading(text: str) -> bool:
    """True when the text opens with one of the headings the skills write."""
    stripped = text.lstrip("-\n \t").lower()
    return any(stripped.startswith(heading) for heading in PLAN_HEADINGS)


def choose_structural_strip_point(
    description: str,
    candidates: List[Candidate],
) -> Optional[Candidate]:
    """Return the earliest rule that separates a plan above from a non-plan below.

    History similarity only identifies duplication while the pre-analyze version
    is still the most recent description change; once an item is edited again,
    the prior version already contains the plan and the comparison fails even
    though the duplicated original is still sitting there. The layout the skills
    wrote is itself the evidence: plan above the rule, preserved original below.

    Requiring the tail *not* to be a plan is what protects the inverted items,
    where the original sits on top and the plan below — stripping those would
    delete the plan.
    """
    for candidate in candidates:
        if starts_with_plan_heading(description[: candidate.start]) and not starts_with_plan_heading(
            candidate.tail,
        ):
            return candidate

    return None


def best_similarity(candidates: List[Candidate], prior_html: Optional[str]) -> Optional[float]:
    if not candidates:
        return None

    return max(similarity(candidate.tail, prior_html) for candidate in candidates)


def normalise_heading(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip().lower()


def split_sections(text: str) -> List[Section]:
    """Split markdown into a leading preamble plus one section per top-level heading.

    Headings inside fenced code blocks are not section boundaries.
    """
    if not text:
        return []

    sections: List[Section] = []
    heading: Optional[str] = None
    buffer: List[str] = []

    for _, line, in_fence in iter_lines_outside_fences(text):
        heading_match = None if in_fence else HEADING_RE.match(line.rstrip("\n"))

        if heading_match and len(heading_match.group(1)) <= TOP_LEVEL_HEADING_DEPTH:
            if buffer:
                sections.append(Section(heading=heading, text="".join(buffer)))

            heading = normalise_heading(heading_match.group(2))
            buffer = []

        buffer.append(line)

    if buffer:
        sections.append(Section(heading=heading, text="".join(buffer)))

    return [section for section in sections if section.text.strip()]


def coverage(needle: str, haystack: str) -> float:
    """Return the fraction of the needle's words that the haystack already carries.

    Matching runs over word lists rather than characters so an alignment cannot
    accumulate a high score out of shared punctuation and single letters.
    """
    needle_words = normalise_for_comparison(needle).split()

    if not needle_words:
        return 1.0

    matcher = difflib.SequenceMatcher(
        None,
        needle_words,
        normalise_for_comparison(haystack).split(),
        autojunk=False,
    )
    return sum(block.size for block in matcher.get_matching_blocks()) / len(needle_words)


def fold_sections(head: str, tail: str, threshold: float = DEFAULT_THRESHOLD) -> List[Section]:
    """Return the tail sections whose content the head does not already carry.

    The layout the skills wrote is not proof that the whole tail is redundant:
    the plan template has no Acceptance Criteria section of its own, and some
    plans delegate outright to "the section below". Nor is a shared heading —
    a plan's own `### Tests` says nothing about the original's `## Tests`. Only
    content the head demonstrably restates is a second copy; everything else
    survives, because a duplicate left in place is reviewable and a deleted
    section is not.
    """
    return [
        section for section in split_sections(tail) if coverage(section.text, head) < threshold
    ]


def fold_description(
    description: str,
    candidate: Candidate,
    threshold: float = DEFAULT_THRESHOLD,
) -> str:
    """Return the text above the separator, followed by the tail sections it does not cover."""
    head = description[: candidate.start].rstrip()
    kept = fold_sections(head, candidate.tail, threshold)

    if not kept:
        return head + "\n"

    return head + "\n\n" + "\n\n".join(section.text.strip() for section in kept) + "\n"


def choose_reconstruction_point(
    prior_markdown: str,
    current: str,
    threshold: float,
) -> Optional[Candidate]:
    """Return the rule in the prior version whose head is the text that survived.

    The earliest qualifying rule is the right one: at the true strip point the
    head is the current description, and every later rule's head still contains
    it, so taking the earliest recovers the whole removed remainder.
    """
    for candidate in split_candidates(prior_markdown):
        if coverage(current, prior_markdown[: candidate.start]) >= threshold:
            return candidate

    return None


def plan_reconstruction(current: str, prior_html: str, threshold: float) -> Reconstruction:
    """Decide how to restore what the truncating run removed from one item.

    Taiga keeps the pre-strip description only as rendered HTML, so recovery
    converts that back to markdown, locates the rule the run cut at, and folds
    the removed tail into the text that is live now. The live text is never
    rewritten — it becomes the head verbatim — so a conversion that is merely
    close cannot damage what survived.
    """
    prior_markdown = html_to_markdown(prior_html)

    if coverage(prior_html, prior_markdown) < threshold:
        return Reconstruction(DECISION_MANUAL_REVIEW, None, "prior version did not convert faithfully")

    candidate = choose_reconstruction_point(prior_markdown, current, threshold)

    if candidate is None:
        return Reconstruction(
            DECISION_MANUAL_REVIEW,
            None,
            "prior version does not read as this item's text followed by a removed tail",
        )

    head = current.rstrip()
    rejoined = Candidate(start=len(head) + len("\n\n"), tail=candidate.tail)
    reconstructed = fold_description(head + "\n\n---\n\n" + candidate.tail, rejoined, threshold)

    if normalise_for_comparison(reconstructed) == normalise_for_comparison(current):
        return Reconstruction(DECISION_NO_CHANGE, current, "removed tail was a true duplicate")

    return Reconstruction(DECISION_REPAIRED, reconstructed, None)


def process_reclean_items(
    client,
    kind: str,
    items: List[dict],
    threshold: float,
    dry_run: bool,
) -> List[dict]:
    """Restore, and under --apply write, what the truncating run removed from each item."""
    records: List[dict] = []

    for item in items:
        detail = client.get_item(kind, item["id"])
        description = detail.get("description") or ""
        change = extract_description_change(client.get_history(kind, item["id"]))

        if change is None:
            plan = Reconstruction(DECISION_NO_HISTORY, None, "no description change in history")
        else:
            plan = plan_reconstruction(description, change[0], threshold)

        decision = plan.decision
        new_description = plan.description if plan.decision == DECISION_REPAIRED else description

        if plan.decision == DECISION_REPAIRED and not dry_run:
            decision = apply_write(client, kind, detail, new_description, DECISION_REPAIRED)

            if decision == DECISION_SKIPPED_CONFLICT:
                new_description = description

        records.append(
            {
                "id": detail.get("id"),
                "ref": detail.get("ref"),
                "kind": kind,
                "old_length": len(description),
                "new_length": len(new_description),
                "separators": len(split_candidates(description)),
                "decision": decision,
                "verification": VERIFICATION_HISTORY if change else None,
                "folded_sections": 0,
                "similarity": None,
                "reason": plan.reason,
            },
        )

    return records


def process_items(
    client,
    kind: str,
    items: List[dict],
    threshold: float,
    dry_run: bool,
) -> List[dict]:
    """Decide, and under --apply write, the stripped description for each item."""
    records: List[dict] = []

    for item in items:
        detail = client.get_item(kind, item["id"])
        description = detail.get("description") or ""
        candidates = split_candidates(description)
        prior_html = extract_prior_description_html(client.get_history(kind, item["id"]))
        chosen = choose_strip_point(candidates, prior_html, threshold) if prior_html else None
        candidate = None
        verification = None
        folded_sections = 0
        score = best_similarity(candidates, prior_html) if prior_html else None

        if chosen is not None:
            candidate, score = chosen
            verification = VERIFICATION_HISTORY
        else:
            candidate = choose_structural_strip_point(description, candidates)

            if candidate is not None:
                verification = VERIFICATION_STRUCTURE

        if not candidates:
            # Nothing to strip at all — distinct from an item that has a separator
            # the script could not clear.
            decision = DECISION_NO_SEPARATOR
            new_description = description
        elif candidate is None:
            decision = DECISION_MANUAL_REVIEW if prior_html else DECISION_NO_HISTORY
            new_description = description
        else:
            decision = DECISION_STRIP
            new_description = fold_description(description, candidate)
            folded_sections = len(fold_sections(description[: candidate.start], candidate.tail))

            if not dry_run:
                decision = apply_strip(client, kind, detail, new_description)

                if decision == DECISION_SKIPPED_CONFLICT:
                    # The write was refused, so the item still holds its original
                    # text — report its length, not the length we would have written.
                    new_description = description
                    folded_sections = 0

        records.append(
            {
                "id": detail.get("id"),
                "ref": detail.get("ref"),
                "kind": kind,
                "old_length": len(description),
                "new_length": len(new_description),
                "separators": len(candidates),
                "decision": decision,
                "verification": verification,
                "folded_sections": folded_sections,
                "similarity": round(score, 4) if score is not None else None,
            },
        )

    return records


def apply_write(client, kind: str, detail: dict, new_description: str, decision: str) -> str:
    """Re-read the item so the write carries a current version, then PATCH it.

    A version that moved between the decision and the write means someone else
    edited the item; the item is skipped rather than blindly overwritten.
    """
    current = client.get_item(kind, detail["id"])

    if current.get("version") != detail.get("version"):
        return DECISION_SKIPPED_CONFLICT

    client.patch_description(kind, detail["id"], new_description, current["version"])
    return decision


def apply_strip(client, kind: str, detail: dict, new_description: str) -> str:
    return apply_write(client, kind, detail, new_description, DECISION_STRIP)


def build_report(records: List[dict], dry_run: bool, mode: str = STRIP_MODE) -> dict:
    summary = {"total": len(records)}

    for decision in DECISIONS:
        summary[decision] = sum(1 for record in records if record["decision"] == decision)

    return {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dry_run": dry_run,
        "mode": mode,
        "summary": summary,
        "items": sorted(records, key=lambda record: record["ref"]),
    }


def render_markdown(report: dict) -> str:
    lines = [
        "# Taiga Duplicated-Description "
        + ("Re-clean" if report.get("mode") == RE_CLEAN_MODE else "Strip"),
        "",
        f"Generated at: `{report['generated_at']}`",
        f"Mode: `{'dry-run' if report['dry_run'] else 'apply'}`",
        "",
        "## Summary",
        "",
    ]

    for key, value in report["summary"].items():
        lines.append(f"- {key}: `{value}`")

    lines.extend(
        [
            "",
            "## Items",
            "",
            "| ref | kind | decision | verified by | separators | old | new | folded | similarity | reason |",
            "|---|---|---|---|---|---|---|---|---|---|",
        ],
    )

    for item in report["items"]:
        score = "n/a" if item["similarity"] is None else f"{item['similarity']:.3f}"
        lines.append(
            f"| #{item['ref']} | {item['kind']} | {item['decision']} | {item.get('verification') or 'n/a'} | "
            f"{item.get('separators', 'n/a')} | {item['old_length']} | {item['new_length']} | "
            f"{item.get('folded_sections', 'n/a')} | {score} | {item.get('reason') or ''} |",
        )

    lines.append("")
    return "\n".join(lines)


def report_stem(report: dict) -> str:
    """Name a report after the pass that produced it, so no run overwrites another's."""
    stem = "report"

    if report.get("mode") == RE_CLEAN_MODE:
        stem += ".re-clean"

    if report["dry_run"]:
        stem += ".dry-run"

    return stem


def write_report(report: dict, out_dir: Path) -> Dict[str, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = report_stem(report)
    paths = {
        "report_json": out_dir / f"{stem}.json",
        "report_md": out_dir / f"{stem}.md",
    }
    paths["report_json"].write_text(json.dumps(report, indent=2) + "\n")
    paths["report_md"].write_text(render_markdown(report) + "\n")
    return paths


def is_analyzed(item: dict) -> bool:
    """Taiga returns tags as [["name", color], ...]."""
    for tag in item.get("tags") or []:
        name = tag[0] if isinstance(tag, (list, tuple)) and tag else tag

        if name == ANALYZED_TAG:
            return True

    return False


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    kinds = KINDS if args.kind == "both" else (args.kind,)
    base_url = get_required_env("TAIGA_API_URL")
    username = get_required_env("TAIGA_USERNAME")
    password = get_required_env("TAIGA_PASSWORD")
    client = TaigaClient(base_url=base_url, username=username, password=password)
    process = process_reclean_items if args.re_clean else process_items
    records: List[dict] = []

    for kind in kinds:
        # The list endpoints omit `description`, so each analyzed item is fetched
        # individually inside the processing pass.
        listed = client.list_all(f"/api/v1/{kind}", project=args.project_id)
        analyzed = [item for item in listed if is_analyzed(item)]
        records.extend(process(client, kind, analyzed, args.threshold, not args.apply))

    report = build_report(
        records,
        dry_run=not args.apply,
        mode=RE_CLEAN_MODE if args.re_clean else STRIP_MODE,
    )
    paths = write_report(report, Path(args.out_dir))

    print(render_markdown(report))
    print(f"JSON report: {paths['report_json']}")
    print(f"Markdown report: {paths['report_md']}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except TaigaStripError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
