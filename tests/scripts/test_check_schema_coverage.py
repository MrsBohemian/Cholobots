# ABOUTME: Pytest tests for scripts/check_schema_coverage.py, the baseline-vs-code table coverage checker.
# ABOUTME: Covers the reference extractors, the DDL parser (kinds, RLS, policies) and the CLI exit codes.
import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "check_schema_coverage.py"
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "schema_coverage"

sys.path.insert(0, str(REPO_ROOT / "scripts"))
import check_schema_coverage as csc  # noqa: E402


def run_cli(*args):
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args], capture_output=True, text=True
    )


def fixture_args(migrations):
    return [
        "--repo-root", str(FIXTURES / "repo"),
        "--migrations", str(migrations),
        "--mobile-dir", str(FIXTURES / "mobile"),
    ]


def test_extracts_bot_tables():
    paths = [FIXTURES / "repo" / "bots" / "sample_bot.py", FIXTURES / "repo" / "db" / "database.py"]
    assert csc.bot_tables(paths) == {"alpha", "beta", "gamma"}


def test_extracts_dashboard_tables():
    assert csc.dashboard_tables([FIXTURES / "mobile" / "page.html"]) == {"alpha", "dash_view"}


def test_finds_tables_and_views_in_dump():
    sql = (FIXTURES / "migrations" / "20260101000000_baseline.sql").read_text()
    objects = csc.schema_objects(sql)
    kinds = {name: info["kind"] for name, info in objects.items()}
    assert kinds == {
        "alpha": "table",
        "beta": "table",
        "gamma": "table",
        "dash_view": "view",
        "stats": "materialized_view",
        "orphan": "table",
    }


def test_rls_and_policies():
    sql = (FIXTURES / "migrations" / "20260101000000_baseline.sql").read_text()
    objects = csc.schema_objects(sql)
    assert objects["alpha"]["rls"] is True
    assert objects["alpha"]["policies"] == 2
    assert objects["beta"]["rls"] is True
    assert objects["beta"]["policies"] == 0
    assert objects["gamma"]["rls"] is False


def test_rls_toggle_inside_a_function_body_is_ignored():
    sql = (
        'CREATE TABLE "public"."t" (id int);\n'
        "    EXECUTE format('alter table if exists %s enable row level security', x);\n"
    )
    assert csc.schema_objects(sql)["t"]["rls"] is False


def test_missing_object_exits_1(tmp_path):
    (tmp_path / "20260101000000_partial.sql").write_text('CREATE TABLE "public"."alpha" (id int);\n')
    result = run_cli(*fixture_args(tmp_path))
    assert result.returncode == 1
    missing = [line for line in result.stdout.splitlines() if line.startswith("MISSING")]
    assert any("beta" in line for line in missing)
    assert any("dash_view" in line for line in missing)


def test_all_present_exits_0():
    result = run_cli(*fixture_args(FIXTURES / "migrations"), "--json")
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads(result.stdout)
    expected = {row["name"]: row for row in report["expected"]}
    assert set(expected) == {"alpha", "beta", "gamma", "dash_view"}
    assert expected["alpha"]["status"] == "OK"
    assert expected["alpha"]["referenced_by"] == ["bots", "dashboards"]
    assert expected["dash_view"]["kind"] == "view"
    assert sorted(report["unreferenced"]) == ["orphan", "stats"]


def test_dashboard_table_without_rls_is_a_warning_not_a_failure(tmp_path):
    (tmp_path / "20260101000000_all.sql").write_text(
        'CREATE TABLE "public"."alpha" (id int);\n'
        'CREATE TABLE "public"."beta" (id int);\n'
        'CREATE TABLE "public"."gamma" (id int);\n'
        'CREATE VIEW "public"."dash_view" AS SELECT 1;\n'
    )
    result = run_cli(*fixture_args(tmp_path))
    assert result.returncode == 0
    assert any(line.startswith("WARN") and "alpha" in line for line in result.stdout.splitlines())


def test_missing_dashboard_dir_is_reported(tmp_path):
    result = run_cli(
        "--repo-root", str(FIXTURES / "repo"),
        "--migrations", str(FIXTURES / "migrations"),
        "--mobile-dir", str(tmp_path / "nope"),
    )
    assert result.returncode == 2
    assert "nope" in result.stderr
