# ABOUTME: Tests for scripts/supabase_migrations.py, the migration linter and push-plan guard.
# ABOUTME: Builds migration directories in tmp_path; git cases use a sandbox repo, no network.
import subprocess
import sys
from pathlib import Path

from conftest import _GIT_IDENTITY, make_host_repo

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "supabase_migrations.py"

sys.path.insert(0, str(REPO_ROOT / "scripts"))
import supabase_migrations as sm  # noqa: E402

BASELINE = "20261107120000_remote_baseline.sql"


def run(*args, stdin=None, cwd=None):
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        input=stdin, capture_output=True, text=True, cwd=cwd,
    )


def write_dir(directory, names):
    directory.mkdir(parents=True, exist_ok=True)
    for name in names:
        (directory / name).write_text("select 1;\n")
    return directory


# --- lint_names -----------------------------------------------------------

def test_valid_directory_passes(tmp_path):
    d = write_dir(tmp_path / "m", [BASELINE, "20261108000000_add_a.sql", "20261109000000_add_b.sql"])
    result = run("lint", "--dir", str(d))
    assert result.returncode == 0, result.stdout + result.stderr


def test_bad_filename_fails():
    for bad in ["2026_add.sql", "20261108000000-Add.sql", "20261108000000_add.txt"]:
        errors = sm.lint_names([BASELINE, bad])
        assert any(bad in e for e in errors), bad


def test_gitkeep_is_allowed():
    assert sm.lint_names([BASELINE, ".gitkeep"]) == []


def test_duplicate_timestamp_fails():
    errors = sm.lint_names([BASELINE, "20261108000000_a.sql", "20261108000000_b.sql"])
    assert any("duplicate" in e.lower() for e in errors)


def test_missing_baseline_fails():
    errors = sm.lint_names(["20261108000000_a.sql"])
    assert any("baseline" in e.lower() for e in errors)


def test_double_baseline_fails():
    errors = sm.lint_names([BASELINE, "20261108000000_remote_baseline.sql"])
    assert any("baseline" in e.lower() for e in errors)


def test_baseline_not_earliest_fails():
    errors = sm.lint_names(["20261106000000_early.sql", BASELINE])
    assert any("earliest" in e.lower() for e in errors)


def test_missing_dir_exits_2(tmp_path):
    result = run("lint", "--dir", str(tmp_path / "nope"))
    assert result.returncode == 2


# --- lint_against_base ----------------------------------------------------

def test_modified_merged_migration_fails():
    errors = sm.lint_against_base([("M", f"supabase/migrations/{BASELINE}")], [BASELINE])
    assert errors and "new migration" in errors[0]


def test_deleted_and_renamed_merged_migration_fail():
    base = [BASELINE]
    assert sm.lint_against_base([("D", f"supabase/migrations/{BASELINE}")], base)
    assert sm.lint_against_base(
        [("R100", f"supabase/migrations/{BASELINE}", "supabase/migrations/20261110000000_x.sql")], base
    )


def test_added_migration_older_than_base_newest_fails():
    errors = sm.lint_against_base(
        [("A", "supabase/migrations/20261106000000_old.sql")],
        [BASELINE],
    )
    assert errors and "skipped" in errors[0]


def test_added_newer_migration_passes():
    assert sm.lint_against_base(
        [("A", "supabase/migrations/20261108000000_new.sql")], [BASELINE]
    ) == []


def _git(*args, cwd):
    return subprocess.run(
        ["git", *_GIT_IDENTITY, *args], cwd=cwd, check=True, capture_output=True, text=True
    )


def _repo_with_migrations(tmp_path):
    host = make_host_repo(tmp_path)
    write_dir(host / "supabase" / "migrations", [BASELINE])
    _git("add", ".", cwd=host)
    _git("commit", "--quiet", "-m", "baseline", cwd=host)
    _git("checkout", "--quiet", "-b", "feature", cwd=host)
    return host


def test_cli_base_added_newer_passes(tmp_path):
    host = _repo_with_migrations(tmp_path)
    write_dir(host / "supabase" / "migrations", ["20261108000000_new.sql"])
    _git("add", ".", cwd=host)
    _git("commit", "--quiet", "-m", "add", cwd=host)
    result = run("lint", "--base", "main", cwd=host)
    assert result.returncode == 0, result.stdout + result.stderr


def test_cli_base_modified_merged_fails(tmp_path):
    host = _repo_with_migrations(tmp_path)
    (host / "supabase" / "migrations" / BASELINE).write_text("select 2;\n")
    _git("commit", "--quiet", "-am", "edit", cwd=host)
    result = run("lint", "--base", "main", cwd=host)
    assert result.returncode == 1
    assert BASELINE in result.stdout + result.stderr


def test_cli_base_renamed_merged_fails(tmp_path):
    host = _repo_with_migrations(tmp_path)
    m = host / "supabase" / "migrations"
    (m / BASELINE).rename(m / "20261107120001_remote_baseline.sql")
    _git("add", "-A", cwd=host)
    _git("commit", "--quiet", "-m", "rename", cwd=host)
    result = run("lint", "--base", "main", cwd=host)
    assert result.returncode == 1


def test_cli_base_older_added_fails(tmp_path):
    host = _repo_with_migrations(tmp_path)
    write_dir(host / "supabase" / "migrations", ["20261101000000_old.sql"])
    _git("add", ".", cwd=host)
    _git("commit", "--quiet", "-m", "old", cwd=host)
    result = run("lint", "--base", "main", cwd=host)
    assert result.returncode == 1


def test_cli_unknown_base_exits_2(tmp_path):
    host = _repo_with_migrations(tmp_path)
    result = run("lint", "--base", "no-such-ref", cwd=host)
    assert result.returncode == 2


# --- check-push-plan ------------------------------------------------------

def test_push_plan_with_baseline_fails():
    plan = f"Would push these migrations:\n • {BASELINE}\n"
    result = run("check-push-plan", "-", stdin=plan)
    assert result.returncode == 1
    assert "migration repair" in result.stdout + result.stderr


def test_push_plan_without_baseline_passes_and_lists_files():
    plan = "Would push these migrations:\n • 20261108000000_add_idx.sql\n"
    result = run("check-push-plan", "-", stdin=plan)
    assert result.returncode == 0
    assert "20261108000000_add_idx.sql" in result.stdout


def test_push_plan_up_to_date_passes():
    result = run("check-push-plan", "-", stdin="Remote database is up to date.\n")
    assert result.returncode == 0
    assert "nothing to push" in result.stdout.lower()


def test_empty_push_plan_exits_2():
    assert run("check-push-plan", "-", stdin="").returncode == 2


def test_push_plan_reads_file(tmp_path):
    f = tmp_path / "plan.txt"
    f.write_text("Would push these migrations:\n • 20261108000000_add_idx.sql\n")
    assert run("check-push-plan", str(f)).returncode == 0


def test_pending_from_plan_dedupes_in_order():
    text = "20261108000000_a.sql\n20261109000000_b.sql\n20261108000000_a.sql\n"
    assert sm.pending_from_plan(text) == ["20261108000000_a.sql", "20261109000000_b.sql"]
