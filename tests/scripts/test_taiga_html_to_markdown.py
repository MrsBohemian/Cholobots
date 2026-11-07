# ABOUTME: Pytest tests for the taiga_html_to_markdown.py converter.
# ABOUTME: Verifies the rendered-HTML subset Taiga returns from its history endpoint round-trips back to markdown.
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts"))

from taiga_html_to_markdown import html_to_markdown


class TestHeadings:
    def test_converts_every_heading_depth_and_drops_the_slug_id(self):
        html = '<h1 id="a">One</h1>\n<h2 id="b">Two</h2>\n<h6 id="c">Six</h6>'

        assert html_to_markdown(html) == "# One\n\n## Two\n\n###### Six"


class TestParagraphsAndInline:
    def test_converts_emphasis_code_and_links(self):
        html = (
            "<p>Use <code>python cholobots.py</code> to start, see "
            '<strong>the docs</strong> and <em>note</em> '
            '<a href="https://example.test/x">this page</a>.</p>'
        )

        assert html_to_markdown(html) == (
            "Use `python cholobots.py` to start, see **the docs** and *note* "
            "[this page](https://example.test/x)."
        )

    def test_drops_an_empty_anchor(self):
        """Taiga renders a bare reference as an empty <a> followed by the real one."""
        html = '<p>Epic: <a href="https://taiga.test/epic/698" target="_blank"></a><a href="https://taiga.test/epic/698">&num;698</a></p>'

        assert html_to_markdown(html) == "Epic: [#698](https://taiga.test/epic/698)"

    def test_treats_br_as_a_line_break(self):
        html = "<p>First line.<br>\nSecond line.</p>"

        assert html_to_markdown(html) == "First line.\nSecond line."

    def test_unescapes_entities(self):
        html = "<p>a &amp; b &lt; c &gt; d &quot;e&quot;</p>"

        assert html_to_markdown(html) == 'a & b < c > d "e"'


class TestLists:
    def test_converts_unordered_and_ordered_lists(self):
        html = "<ul>\n<li>First</li>\n<li>Second</li>\n</ul>\n<ol>\n<li>Step one</li>\n<li>Step two</li>\n</ol>"

        assert html_to_markdown(html) == "- First\n- Second\n\n1. Step one\n2. Step two"

    def test_indents_a_nested_list(self):
        html = "<ul>\n<li>Outer<ul>\n<li>Inner</li>\n</ul>\n</li>\n</ul>"

        assert html_to_markdown(html) == "- Outer\n    - Inner"

    def test_restores_task_list_checkboxes(self):
        """Taiga renders `- [ ]` as a checkbox input wrapped in a label."""
        html = (
            '<ul class="task-list">\n'
            '<li class="task-list-item"><label class="task-list-control">'
            '<input type="checkbox"><span class="task-list-indicator"></span></label> Not done</li>\n'
            '<li class="task-list-item"><label class="task-list-control">'
            '<input type="checkbox" checked><span class="task-list-indicator"></span></label> Done</li>\n'
            "</ul>"
        )

        assert html_to_markdown(html) == "- [ ] Not done\n- [x] Done"

    def test_keeps_inline_markup_inside_a_list_item(self):
        html = "<ul>\n<li><strong>No F# code.</strong> No migration.</li>\n</ul>"

        assert html_to_markdown(html) == "- **No F# code.** No migration."


class TestTables:
    def test_converts_a_table_to_a_pipe_table(self):
        html = (
            "<table>\n<thead>\n<tr>\n<th>File</th>\n<th>Site</th>\n</tr>\n</thead>\n"
            "<tbody>\n<tr>\n<td><code>SKILL.md</code></td>\n<td>Step 10</td>\n</tr>\n</tbody>\n</table>"
        )

        assert html_to_markdown(html) == (
            "| File | Site |\n|---|---|\n| `SKILL.md` | Step 10 |"
        )

    def test_renders_an_empty_header_cell(self):
        html = (
            "<table>\n<thead>\n<tr>\n<th></th>\n<th>count</th>\n</tr>\n</thead>\n"
            "<tbody>\n<tr>\n<td>Stories</td>\n<td>251</td>\n</tr>\n</tbody>\n</table>"
        )

        assert html_to_markdown(html) == "|  | count |\n|---|---|\n| Stories | 251 |"


class TestCodeBlocks:
    def test_converts_a_codehilite_block_to_a_fenced_block(self):
        html = (
            '<div class="codehilite"><pre><span></span><code>'
            '<span class="c1"># ABOUTME: a comment</span>\n'
            '<span class="kn">import</span> <span class="nn">json</span>\n'
            "</code></pre></div>"
        )

        assert html_to_markdown(html) == "```\n# ABOUTME: a comment\nimport json\n```"

    def test_keeps_entities_and_blank_lines_inside_a_code_block(self):
        html = "<pre><code>if (a &lt; b) {\n\n  go();\n}\n</code></pre>"

        assert html_to_markdown(html) == "```\nif (a < b) {\n\n  go();\n}\n```"

    def test_keeps_a_raw_less_than_that_cannot_open_a_tag(self):
        """Taiga leaves some `<` unescaped; a parser that reads `<<<<<<<` or `<1s`
        as a tag swallows the rest of the block."""
        html = "<pre><code>git grep -l '<<<<<<<' -- .\n</code></pre>"

        assert html_to_markdown(html) == "```\ngit grep -l '<<<<<<<' -- .\n```"

    def test_keeps_a_raw_less_than_in_prose(self):
        html = "<p>Build time is <1s after the change.</p>"

        assert html_to_markdown(html) == "Build time is <1s after the change."

    def test_does_not_treat_an_inline_code_span_as_a_block(self):
        html = "<p>Run <code>ls</code> now.</p>"

        assert html_to_markdown(html) == "Run `ls` now."


class TestBlockquotesAndRules:
    def test_converts_a_horizontal_rule(self):
        html = "<p>Above.</p>\n<hr>\n<p>Below.</p>"

        assert html_to_markdown(html) == "Above.\n\n---\n\nBelow."

    def test_converts_a_blockquote(self):
        html = "<blockquote>\n<p>Quoted text.</p>\n</blockquote>"

        assert html_to_markdown(html) == "> Quoted text."


class TestWholeDocument:
    def test_converts_a_representative_taiga_description(self):
        html = (
            '<h2 id="implementation-plan">Implementation Plan</h2>\n'
            "<p>The <code>analyze-story</code> skill must <strong>replace</strong> the text.</p>\n"
            '<h3 id="tasks">Tasks</h3>\n'
            "<ol>\n<li>Replace the prepend sites.</li>\n<li>Add the script.</li>\n</ol>\n"
            "<hr>\n"
            '<h2 id="original">Original</h2>\n'
            "<p>We want the skills to stop duplicating.</p>"
        )

        assert html_to_markdown(html) == (
            "## Implementation Plan\n"
            "\n"
            "The `analyze-story` skill must **replace** the text.\n"
            "\n"
            "### Tasks\n"
            "\n"
            "1. Replace the prepend sites.\n"
            "2. Add the script.\n"
            "\n"
            "---\n"
            "\n"
            "## Original\n"
            "\n"
            "We want the skills to stop duplicating."
        )

    def test_returns_an_empty_string_for_empty_input(self):
        assert html_to_markdown("") == ""
