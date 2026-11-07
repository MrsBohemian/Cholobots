# ABOUTME: Converts the rendered HTML Taiga stores in its history endpoint back into markdown.
# ABOUTME: Covers only the subset Taiga's renderer emits; callers verify each result against the source before using it.
import re
from html.parser import HTMLParser
from typing import List, Optional


HEADING_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6"}
INLINE_WRAPPERS = {"strong": "**", "b": "**", "em": "*", "i": "*"}
LIST_INDENT = "    "


class _MarkdownWriter(HTMLParser):
    """Rebuild markdown from Taiga's rendered HTML.

    Text accumulates into an inline buffer that block-level tags flush; code
    blocks and table cells collect separately because their whitespace and
    layout survive into the output.
    """

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.blocks: List[str] = []
        self.inline: List[str] = []
        self.list_stack: List[dict] = []
        self.list_group = 0
        self.pending_prefix = ""
        self.quote_depth = 0
        self.code_lines: List[str] = []
        self.in_code_block = False
        self.pending_link: Optional[str] = None
        self.link_start = 0
        self.table_rows: List[List[str]] = []
        self.table_header_rows = 0
        self.in_table = False
        self.in_cell = False

    # -- buffers ---------------------------------------------------------

    def _flush_inline(self, prefix: Optional[str] = None) -> None:
        """Emit the buffered inline text as a block, consuming any pending list prefix."""
        if prefix is None:
            prefix = self.pending_prefix

        self.pending_prefix = ""
        text = re.sub(r"[ \t]+", " ", "".join(self.inline))
        text = "\n".join(line.strip() for line in text.split("\n")).strip()
        self.inline = []

        if not text:
            return

        if self.quote_depth:
            text = "\n".join(f"> {line}" if line else ">" for line in text.split("\n"))

        # Items of one list sit on consecutive lines; every other pair of blocks,
        # including two adjacent lists, is separated by a blank line.
        self.blocks.append((prefix + text, self.list_group if self.list_stack else None))

    def _list_prefix(self) -> str:
        level = self.list_stack[-1]
        indent = LIST_INDENT * (len(self.list_stack) - 1)

        if level["ordered"]:
            level["index"] += 1
            return f"{indent}{level['index']}. "

        return f"{indent}- "

    # -- HTMLParser hooks ------------------------------------------------

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)

        if self.in_code_block:
            return

        if tag == "pre" or (tag == "code" and self.pre_is_open()):
            self.in_code_block = True
            self.code_lines = []
        elif tag in HEADING_TAGS:
            self._flush_inline()
        elif tag == "p":
            self._flush_inline()
        elif tag == "br":
            self.inline.append("\n")
        elif tag == "hr":
            self._flush_inline()
            self.blocks.append(("---", False))
        elif tag in ("ul", "ol"):
            self._flush_inline()

            if not self.list_stack:
                self.list_group += 1

            self.list_stack.append({"ordered": tag == "ol", "index": 0})
        elif tag == "li":
            self._flush_inline()
            self.pending_prefix = self._list_prefix() if self.list_stack else ""
        elif tag == "blockquote":
            self._flush_inline()
            self.quote_depth += 1
        elif tag == "table":
            self._flush_inline()
            self.in_table = True
            self.table_rows = []
            self.table_header_rows = 0
        elif tag == "tr" and self.in_table:
            self.table_rows.append([])
        elif tag in ("td", "th") and self.in_table:
            self.in_cell = True
            self.inline = []
        elif tag == "input" and attributes.get("type") == "checkbox":
            # Taiga renders a `- [ ]` task item as a checkbox inside a label.
            self.inline.append("[x] " if "checked" in attributes else "[ ] ")
        elif tag in INLINE_WRAPPERS:
            self.inline.append(INLINE_WRAPPERS[tag])
        elif tag == "code":
            self.inline.append("`")
        elif tag == "a":
            self.pending_link = attributes.get("href")
            self.link_start = len(self.inline)

    def pre_is_open(self) -> bool:
        return self.in_code_block

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)

    def handle_endtag(self, tag):
        if tag in ("pre", "code") and self.in_code_block:
            if tag == "pre":
                self._close_code_block()

            return

        if tag in HEADING_TAGS:
            self._flush_inline("#" * int(tag[1]) + " ")
        elif tag == "p":
            self._flush_inline()
        elif tag == "li":
            self._flush_inline()
        elif tag in ("ul", "ol"):
            if self.list_stack:
                self.list_stack.pop()
        elif tag == "blockquote":
            self._flush_inline()
            self.quote_depth = max(0, self.quote_depth - 1)
        elif tag in ("td", "th") and self.in_table:
            self.in_cell = False
            cell = re.sub(r"\s+", " ", "".join(self.inline)).strip()
            self.inline = []

            if self.table_rows:
                self.table_rows[-1].append(cell)
        elif tag == "thead":
            self.table_header_rows = len(self.table_rows)
        elif tag == "table":
            self._close_table()
        elif tag in INLINE_WRAPPERS:
            self.inline.append(INLINE_WRAPPERS[tag])
        elif tag == "code":
            self.inline.append("`")
        elif tag == "a":
            self._close_link()

    def handle_data(self, data):
        if self.in_code_block:
            self.code_lines.append(data)
        elif self.in_cell or self.inline or data.strip():
            self.inline.append(data if self.in_code_block else data.replace("\n", " "))

    # -- block closers ---------------------------------------------------

    def _close_code_block(self) -> None:
        code = "".join(self.code_lines).strip("\n")
        self.in_code_block = False
        self.code_lines = []
        self.blocks.append((f"```\n{code}\n```", False))

    def _close_link(self) -> None:
        text = "".join(self.inline[self.link_start :]).strip()
        href = self.pending_link
        self.pending_link = None
        del self.inline[self.link_start :]

        if not text:
            return

        self.inline.append(f"[{text}]({href})" if href else text)

    def _close_table(self) -> None:
        rows = [row for row in self.table_rows if row]
        self.in_table = False
        self.table_rows = []

        if not rows:
            return

        header_rows = self.table_header_rows or 1
        width = max(len(row) for row in rows)
        lines = [self._render_row(row, width) for row in rows[:header_rows]]
        lines.append("|" + "---|" * width)
        lines.extend(self._render_row(row, width) for row in rows[header_rows:])
        self.blocks.append(("\n".join(lines), False))

    @staticmethod
    def _render_row(row: List[str], width: int) -> str:
        cells = row + [""] * (width - len(row))
        return "| " + " | ".join(cells) + " |"

    def result(self) -> str:
        self._flush_inline()
        rendered = ""

        for index, (text, list_group) in enumerate(self.blocks):
            if not text.strip():
                continue

            if not rendered:
                rendered = text
                continue

            same_list = list_group is not None and self.blocks[index - 1][1] == list_group
            separator = "\n" if same_list else "\n\n"
            rendered += separator + text

        return rendered


def html_to_markdown(source: str) -> str:
    """Return markdown for Taiga's rendered HTML, or an empty string for empty input."""
    if not source:
        return ""

    writer = _MarkdownWriter()
    writer.feed(source)
    writer.close()
    return writer.result()
