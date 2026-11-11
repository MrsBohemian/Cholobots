# ABOUTME: Pins the Makefile's target recipes with `make -n` (dry run, nothing executes).
# ABOUTME: Also guards the operator rule that no target touches the hosted Supabase project.
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
MAKEFILE = ROOT / "Makefile"
TARGETS = (
    "help",
    "setup",
    "check-tools",
    "db-up",
    "db-down",
    "db-reset",
    "db-env",
    "run",
    "compile",
    "lint",
    "test",
    "test-integration",
    "check",
)

pytestmark = pytest.mark.skipif(shutil.which("make") is None, reason="make is not installed")


# Variables a parent make (for example `make test`) passes down; they would turn on
# "Entering directory" lines and leak its flags into the make under test.
PARENT_MAKE_VARS = ("MAKEFLAGS", "MAKELEVEL", "MFLAGS", "MAKEOVERRIDES")
CLEAN_ENV = {key: value for key, value in os.environ.items() if key not in PARENT_MAKE_VARS}
CLEAN_ENV.pop("SUPABASE", None)
CLEAN_ENV.pop("PYTHON", None)


def make(*args, cwd=ROOT):
    return subprocess.run(
        ["make", *args], cwd=cwd, env=CLEAN_ENV, capture_output=True, text=True, check=False
    )


def dry_run(*args):
    result = make("-n", *args)
    assert result.returncode == 0, result.stderr
    return result.stdout


def test_help_lists_every_target():
    result = make("help")
    assert result.returncode == 0, result.stderr
    listed = {line.split()[0] for line in result.stdout.splitlines() if line.strip()}
    assert set(TARGETS) <= listed


def test_default_goal_is_help():
    assert make().stdout == make("help").stdout


def test_setup_installs_pinned_requirements_and_pre_commit():
    out = dry_run("setup")
    assert "-m venv .venv" in out
    assert "-m pip install -r requirements.txt -r requirements-dev.txt" in out
    assert "-m pre_commit install" in out
    assert "[ -f .env ] || cp .env.example .env" in out


def test_setup_builds_venv_with_pinned_python_version():
    version = (ROOT / ".python-version").read_text(encoding="utf-8").strip()
    assert f"python{version} -m venv .venv" in dry_run("setup")


@pytest.mark.parametrize(
    ("target", "command"),
    [("db-up", "start"), ("db-down", "stop"), ("db-reset", "db reset"), ("db-env", "status -o env")],
)
def test_supabase_targets_use_the_per_checkout_script(target, command):
    assert f"scripts/supabase-local.sh {command}" in dry_run(target)


def test_makefile_exports_the_pinned_cli_for_the_script():
    text = MAKEFILE.read_text(encoding="utf-8")
    version = re.search(r"^SUPABASE_CLI_VERSION := (\S+)$", text, re.M).group(1)
    # The script's own default (used outside make) must pin the same CLI as the Makefile.
    script = (ROOT / "scripts" / "supabase_local.py").read_text(encoding="utf-8")
    assert f'DEFAULT_CLI = "npx --yes supabase@{version}"' in script
    assert re.search(r"^SUPABASE_CLI \?= npx --yes supabase@\$\(SUPABASE_CLI_VERSION\)$", text, re.M)
    assert re.search(r"^export SUPABASE_CLI$", text, re.M)


def test_supabase_cli_can_be_overridden():
    out = dry_run("db-up", "SUPABASE=supabase")
    assert out.strip() == "supabase start"


def test_db_env_prints_only_the_two_env_lines():
    out = dry_run("db-env")
    assert "--override-name api.url=SUPABASE_URL" in out
    assert "--override-name auth.service_role_key=SUPABASE_SERVICE_ROLE_KEY" in out
    assert "grep -E '^(SUPABASE_URL|SUPABASE_SERVICE_ROLE_KEY)='" in out


def test_nothing_targets_the_hosted_project():
    text = MAKEFILE.read_text(encoding="utf-8")
    for forbidden in ("--linked", "--db-url", "supabase link", "db push", "SUPABASE_ACCESS_TOKEN"):
        assert forbidden not in text


def test_run_checks_env_before_starting_bot():
    out = dry_run("run")
    assert ".venv/bin/python scripts/check_local_env.py && .venv/bin/python cholobots.py" in out


def test_test_target_matches_ci_scope():
    assert ".venv/bin/python -m pytest -q tests/unit tests/scripts" in dry_run("test")


def test_test_target_runs_every_shell_test_and_fails_on_the_first_failure():
    out = dry_run("test")
    assert "for t in tests/scripts/test-*.sh;" in out
    assert 'bash "$t" || exit 1' in out
    assert out.index("-m pytest -q") < out.index("tests/scripts/test-*.sh")


def test_check_runs_compile_lint_and_test():
    out = dry_run("check")
    compile_at = out.index("-m py_compile cholobots.py config.py db/database.py bots/*.py")
    lint_at = out.index("-m ruff check .")
    test_at = out.index("-m pytest -q tests/unit tests/scripts")
    assert compile_at < lint_at < test_at


def test_integration_target_reports_missing_harness(tmp_path):
    shutil.copy(MAKEFILE, tmp_path / "Makefile")
    result = make("test-integration", cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "No integration tests yet (#51)."


def test_integration_target_exports_local_stack_env_and_requires_it():
    out = dry_run("test-integration")
    assert "scripts/supabase-local.sh status -o env" in out
    assert "--override-name api.url=SUPABASE_URL" in out
    assert "--override-name auth.service_role_key=SUPABASE_SERVICE_ROLE_KEY" in out
    assert "--override-name db.url=SUPABASE_DB_URL" in out
    assert "|| exit 1" in out
    assert (
        "CHOLOBOTS_INTEGRATION_REQUIRED=1 .venv/bin/python -m pytest -q -m integration tests/integration"
        in out
    )
    assert out.index("|| exit 1") < out.index("-m pytest")


def test_integration_target_keeps_env_the_caller_already_exported():
    # ':=' only assigns an unset or empty variable, so an exported SUPABASE_URL survives.
    assert '${\\1:=\\2}' in dry_run("test-integration")


def test_integration_target_fails_without_running_pytest_when_the_stack_is_down():
    result = make("test-integration", "SUPABASE=false", "VENV_PY=echo")
    assert result.returncode != 0
    assert "pytest" not in result.stdout


def test_cli_version_matches_readme():
    version = re.search(r"^SUPABASE_CLI_VERSION := (\S+)$", MAKEFILE.read_text(), re.M).group(1)
    assert f"supabase@{version}" in (ROOT / "README.md").read_text(encoding="utf-8")


def test_setup_and_test_run_the_tool_preflight_first():
    assert dry_run("setup").index("command -v") < dry_run("setup").index("-m venv .venv")
    out = dry_run("test")
    assert out.index("for tool in jq timeout setsid curl") < out.index("-m pytest -q")


def test_check_tools_names_the_missing_binary_and_how_to_install_it(tmp_path):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    for tool in ("jq", "curl", "setsid"):  # everything but timeout is present
        (fake_bin / tool).write_text("#!/bin/sh\n")
        (fake_bin / tool).chmod(0o755)
    env = {**CLEAN_ENV, "PATH": f"{fake_bin}:/usr/bin:/bin"}
    if shutil.which("timeout", path=env["PATH"]):
        pytest.skip("a system timeout is on the minimal PATH")
    result = subprocess.run(
        ["make", "check-tools"], cwd=ROOT, env=env, capture_output=True, text=True, check=False
    )
    assert result.returncode != 0
    assert "Missing required tool 'timeout'" in result.stderr
    assert "coreutils" in result.stderr
    assert "'jq'" not in result.stderr


def test_check_tools_passes_when_all_present(tmp_path):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    for tool in ("jq", "curl", "setsid", "timeout"):
        (fake_bin / tool).write_text("#!/bin/sh\n")
        (fake_bin / tool).chmod(0o755)
    env = {**CLEAN_ENV, "PATH": f"{fake_bin}:/usr/bin:/bin"}
    result = subprocess.run(
        ["make", "check-tools"], cwd=ROOT, env=env, capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
