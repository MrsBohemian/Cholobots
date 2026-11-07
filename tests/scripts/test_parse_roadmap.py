# ABOUTME: Pytest tests for scripts/lib/parse_roadmap.py, the CLI wrapper over dependency_parser.
# ABOUTME: Verifies stdin/file input, namespaced vs legacy mode, and error handling via subprocess.
import json
import os
import subprocess
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
SCRIPT = os.path.join(REPO_ROOT, "scripts", "lib", "parse_roadmap.py")


def run_cli(args, stdin_text=""):
    return subprocess.run(
        [sys.executable, SCRIPT, *args], input=stdin_text, capture_output=True, text=True
    )


class TestParseRoadmapCli:
    """CLI behavior over parse_dependency_diagram."""

    def test_stdin_namespaced_mode(self):
        """--default-namespace homelab parses namespaced refs from stdin."""
        diagram = """\
## Dependency Diagram

```
#368 (Foundation)
  └─► alterna#331 (Depends on homelab#368)

alterna#392 (Root)
  └─► alterna#393 (Depends on alterna#392)
```
"""
        result = run_cli(["--default-namespace", "homelab", "-"], stdin_text=diagram)
        assert result.returncode == 0
        graph = json.loads(result.stdout)
        assert graph["alterna#331"] == ["homelab#368"]

    def test_cholobots_default_namespace(self):
        """A bare ref in --default-namespace cholobots mode becomes cholobots#N."""
        diagram = """\
## Dependency Diagram

```
#21 (Shared helpers)
  └─► #24 (Depends on cholobots#21)
```
"""
        result = run_cli(["--default-namespace", "cholobots", "-"], stdin_text=diagram)
        assert result.returncode == 0
        graph = json.loads(result.stdout)
        assert graph["cholobots#24"] == ["cholobots#21"]
        assert graph["cholobots#21"] == []

    def test_stdin_is_the_default_input(self):
        """Omitting the file argument still reads from stdin."""
        diagram = """\
## Dependency Diagram

```
#100 (Foundation)
  └─► #200 (Depends on 100)
```
"""
        result = run_cli([], stdin_text=diagram)
        assert result.returncode == 0
        graph = json.loads(result.stdout)
        assert graph["200"] == [100]

    def test_file_argument_legacy_mode(self, tmp_path):
        """A file path argument is read instead of stdin, in legacy int mode."""
        diagram = """\
## Dependency Diagram

```
#331 (Foundation)
  └─► #368 (Depends on 331)
```
"""
        path = tmp_path / "diagram.md"
        path.write_text(diagram, encoding="utf-8")
        result = run_cli([str(path)])
        assert result.returncode == 0
        graph = json.loads(result.stdout)
        assert graph["368"] == [331]

    def test_empty_input_yields_empty_graph(self):
        """Unparseable/empty input exits 0 with an empty JSON object."""
        result = run_cli([], stdin_text="")
        assert result.returncode == 0
        assert json.loads(result.stdout) == {}

    def test_unknown_flag_exits_nonzero(self):
        """An unrecognized flag causes argparse to exit non-zero."""
        result = run_cli(["--bogus-flag"], stdin_text="")
        assert result.returncode == 2

    def test_missing_file_is_a_clean_usage_error(self, tmp_path):
        """A nonexistent file path exits 2 with a one-line error and no traceback."""
        result = run_cli([str(tmp_path / "does-not-exist.md")])
        assert result.returncode == 2
        assert "parse_roadmap.py: error:" in result.stderr
        assert "Traceback" not in result.stderr
        assert result.stdout == ""

    def test_directory_argument_is_a_clean_usage_error(self, tmp_path):
        """A directory path exits 2 with a one-line error and no traceback."""
        result = run_cli([str(tmp_path)])
        assert result.returncode == 2
        assert "parse_roadmap.py: error:" in result.stderr
        assert "Traceback" not in result.stderr
        assert result.stdout == ""

    def test_non_utf8_file_is_a_clean_usage_error(self, tmp_path):
        """A file that is not valid UTF-8 exits 2 with a one-line error and no traceback."""
        path = tmp_path / "diagram.md"
        path.write_bytes(b"\xff\xfe## Dependency Diagram\n")
        result = run_cli([str(path)])
        assert result.returncode == 2
        assert "parse_roadmap.py: error:" in result.stderr
        assert "Traceback" not in result.stderr
        assert result.stdout == ""

    def test_empty_default_namespace_is_rejected(self):
        """--default-namespace '' is rejected instead of emitting bare '#N' keys."""
        result = run_cli(["--default-namespace", "", "-"], stdin_text="")
        assert result.returncode == 2
        assert "parse_roadmap.py: error:" in result.stderr
        assert result.stdout == ""

    def test_malformed_default_namespace_is_rejected(self):
        """--default-namespace must match the namespace slug grammar of dependency_parser."""
        for bad in ["1homelab", "home lab", "homelab#", "-x"]:
            result = run_cli([f"--default-namespace={bad}", "-"], stdin_text="")
            assert result.returncode == 2, bad
            assert "parse_roadmap.py: error:" in result.stderr, bad
            assert result.stdout == "", bad
