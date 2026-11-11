# ABOUTME: Static checks on the cholobots.py entry point, without importing it.
# ABOUTME: Importing cholobots needs config env vars and builds Supabase clients, so read its AST.
import ast
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[2] / "cholobots.py"


def _is_main_guard(node):
    return (
        isinstance(node, ast.If)
        and isinstance(node.test, ast.Compare)
        and isinstance(node.test.left, ast.Name)
        and node.test.left.id == "__name__"
        and any(isinstance(c, ast.Constant) and c.value == "__main__" for c in node.test.comparators)
    )


def _run_calls(nodes):
    return [
        n
        for top in nodes
        for n in ast.walk(top)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "run"
    ]


def test_cholobots_runs_bot_only_under_main_guard():
    body = ast.parse(SOURCE.read_text()).body
    guarded = [n for n in body if _is_main_guard(n)]
    unguarded = [
        n
        for n in body
        if not _is_main_guard(n) and not isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    ]
    assert _run_calls(unguarded) == []
    assert len(_run_calls(guarded)) == 1
