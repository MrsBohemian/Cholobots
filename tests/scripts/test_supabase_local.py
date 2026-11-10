# ABOUTME: Tests the per-checkout Supabase stack helper: identity, slots, ports, config rendering.
# ABOUTME: Pure functions and real git in tmp_path; no Docker, no network.
import os
import socket
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts"))
import supabase_local  # noqa: E402

CONFIG_TEXT = """\
# A comment mentioning project_id = "nope"
project_id = "cholobots"

[api]
enabled = true
port = 54321

[db]
port = 54322
shadow_port = 54320

[db.pooler]
enabled = false
port = 54329

[studio]
port = 54323

[local_smtp]
port = 54324
# smtp_port = 54325

[edge_runtime]
inspector_port = 8083

[analytics]
port = 54327
"""

GIT_ID = ["-c", "user.name=Sandbox", "-c", "user.email=sandbox@example.invalid"]


def git(*args, cwd):
    subprocess.run(["git", *GIT_ID, *args], cwd=cwd, check=True, capture_output=True, text=True)


@pytest.fixture
def repo(tmp_path):
    main = tmp_path / "Cholobots"
    main.mkdir()
    git("init", "--quiet", "--initial-branch=main", cwd=main)
    git("commit", "--quiet", "--allow-empty", "-m", "initial", cwd=main)
    return main


def add_worktree(repo, name):
    path = repo.parent / name
    git("worktree", "add", "--quiet", "--detach", str(path), cwd=repo)
    return path


def test_main_checkout_is_cholobots_slot_zero(repo):
    ident = supabase_local.checkout_identity(str(repo), {})
    assert ident.is_main is True
    assert ident.project_id == "cholobots"
    assert Path(ident.toplevel) == repo.resolve()


def test_worktree_project_id_uses_sanitized_worktree_name(repo):
    wt = add_worktree(repo, "Story_72")
    ident = supabase_local.checkout_identity(str(wt), {})
    assert ident.is_main is False
    assert ident.name == "story-72"
    assert ident.project_id == "cholobots-story-72"


def test_devcontainer_env_forces_main_identity(repo):
    wt = add_worktree(repo, "bug-12")
    ident = supabase_local.checkout_identity(str(wt), {"CHOLOBOTS_DEVCONTAINER": "1"})
    assert ident.is_main is True
    assert ident.project_id == "cholobots"


def test_registry_path_lives_in_git_common_dir(repo):
    wt = add_worktree(repo, "bug-12")
    ident = supabase_local.checkout_identity(str(wt), {})
    assert supabase_local.registry_path(ident) == os.path.join(
        str((repo / ".git").resolve()), "cholobots-supabase-slots.json"
    )


def test_allocate_slot_returns_lowest_free_and_is_stable_per_name(tmp_path):
    reg = str(tmp_path / "slots.json")
    assert supabase_local.allocate_slot(reg, "a") == 1
    assert supabase_local.allocate_slot(reg, "b") == 2
    assert supabase_local.allocate_slot(reg, "a") == 1


def test_release_slot_frees_it_for_the_next_worktree(tmp_path):
    reg = str(tmp_path / "slots.json")
    supabase_local.allocate_slot(reg, "a")
    supabase_local.allocate_slot(reg, "b")
    supabase_local.release_slot(reg, "a")
    assert supabase_local.allocate_slot(reg, "c") == 1
    supabase_local.release_slot(reg, "missing")  # no error


def test_allocate_slot_fails_when_all_99_are_taken(tmp_path):
    reg = str(tmp_path / "slots.json")
    for i in range(99):
        supabase_local.allocate_slot(reg, f"w{i}")
    with pytest.raises(RuntimeError, match="all 99 Supabase slots in use"):
        supabase_local.allocate_slot(reg, "one-too-many")


def test_port_block_slot_zero_is_cli_defaults():
    assert supabase_local.port_block(0) == {
        "shadow": 54320, "api": 54321, "db": 54322, "studio": 54323,
        "smtp": 54324, "analytics": 54327, "pooler": 54329, "inspector": 8083,
    }


def test_port_block_slots_never_overlap():
    seen = {}
    for slot in range(0, 100):
        for key, port in supabase_local.port_block(slot).items():
            assert port not in seen, f"{key}@{slot} collides with {seen[port]}"
            seen[port] = f"{key}@{slot}"
    assert max(p for p in seen if p > 54000) <= 55319


def test_render_config_shifts_every_default_port_and_sets_project_id():
    out = tomllib.loads(supabase_local.render_config(CONFIG_TEXT, "cholobots-story-72", 3))
    assert out["project_id"] == "cholobots-story-72"
    assert out["api"]["port"] == 54351
    assert out["db"]["port"] == 54352
    assert out["db"]["shadow_port"] == 54350
    assert out["db"]["pooler"]["port"] == 54359
    assert out["studio"]["port"] == 54353
    assert out["local_smtp"]["port"] == 54354
    assert out["analytics"]["port"] == 54357
    assert out["edge_runtime"]["inspector_port"] == 8086


def test_render_config_leaves_comments_and_other_keys_untouched():
    out = supabase_local.render_config(CONFIG_TEXT, "cholobots-x", 2)
    assert '# A comment mentioning project_id = "nope"' in out
    assert "# smtp_port = 54325" in out
    assert "enabled = true" in out
    changed = [
        (a, b) for a, b in zip(CONFIG_TEXT.splitlines(), out.splitlines()) if a != b
    ]
    assert len(changed) == 9  # project_id, 7 ports in 54320..54329, inspector_port
    assert len(CONFIG_TEXT.splitlines()) == len(out.splitlines())


def test_render_config_slot_zero_returns_text_unchanged():
    assert supabase_local.render_config(CONFIG_TEXT, "cholobots", 0) == CONFIG_TEXT


def test_render_repo_config_parses():
    cfg = Path(REPO_ROOT) / "supabase" / "config.toml"
    if not cfg.exists():
        pytest.skip("supabase/config.toml absent")
    out = tomllib.loads(supabase_local.render_config(cfg.read_text(), "cholobots-x", 3))

    def ports(node):
        for key, value in node.items():
            if isinstance(value, dict):
                yield from ports(value)
            elif key == "port" or key.endswith("_port"):
                yield value

    assert out["project_id"] == "cholobots-x"
    assert not [p for p in ports(out) if 54320 <= p <= 54329]


def test_mirror_entries_skip_config_and_cli_state(tmp_path):
    src = tmp_path / "supabase"
    src.mkdir()
    for name in ("config.toml", ".temp", ".branches", "migrations", "seed.sql"):
        (src / name).write_text("")
    assert sorted(supabase_local.mirror_entries(str(src))) == ["migrations", "seed.sql"]


def test_default_excludes_worktree_vs_main_and_env_override(repo):
    main = supabase_local.checkout_identity(str(repo), {})
    wt = supabase_local.checkout_identity(str(add_worktree(repo, "w1")), {})
    assert supabase_local.default_excludes(main, {}) == []
    assert supabase_local.default_excludes(wt, {}) == [
        "studio", "mailpit", "imgproxy", "logflare", "vector", "edge-runtime",
    ]
    assert supabase_local.default_excludes(wt, {"SUPABASE_LOCAL_EXCLUDE": ""}) == []
    assert supabase_local.default_excludes(main, {"SUPABASE_LOCAL_EXCLUDE": "studio, vector"}) == [
        "studio", "vector",
    ]


STATUS = """\
API_URL="http://127.0.0.1:54351"
DB_URL="postgresql://postgres:postgres@127.0.0.1:54352/postgres"
SERVICE_ROLE_KEY=eyJhbGciOi
Stopped services: [supabase_studio_x]
"""


def test_parse_status_env_handles_quoted_values():
    parsed = supabase_local.parse_status_env(STATUS)
    assert parsed["API_URL"] == "http://127.0.0.1:54351"
    assert parsed["SERVICE_ROLE_KEY"] == "eyJhbGciOi"
    assert "Stopped services: [supabase_studio_x]" not in parsed


def test_env_lines_map_to_bot_variable_names():
    lines = supabase_local.env_lines(supabase_local.parse_status_env(STATUS))
    assert lines == [
        "SUPABASE_URL=http://127.0.0.1:54351",
        "SUPABASE_SERVICE_ROLE_KEY=eyJhbGciOi",
        "SUPABASE_DB_URL=postgresql://postgres:postgres@127.0.0.1:54352/postgres",
    ]
    with pytest.raises(KeyError, match="DB_URL"):
        supabase_local.env_lines({"API_URL": "x", "SERVICE_ROLE_KEY": "y"})


def test_split_args_checkout_path_vs_cli_flags(tmp_path):
    d = str(tmp_path)
    assert supabase_local.split_args(["start", d], {}, "/cwd") == ("start", d, [])
    assert supabase_local.split_args(["status", "-o", "env"], {}, "/cwd") == (
        "status", "/cwd", ["-o", "env"],
    )
    assert supabase_local.split_args(["env"], {"CHOLOBOTS_CHECKOUT": "/co"}, "/cwd") == (
        "env", "/co", [],
    )
    assert supabase_local.split_args(["db", "reset"], {}, "/cwd") == ("db", "/cwd", ["reset"])


def test_port_in_use_detects_a_bound_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        assert supabase_local.port_in_use(port) is True
    assert supabase_local.port_in_use(port) is False


# --- main() with a stub CLI runner -------------------------------------------------

STATUS_ENV = 'API_URL="http://127.0.0.1:54331"\nDB_URL="postgresql://postgres:postgres@127.0.0.1:54332/postgres"\nSERVICE_ROLE_KEY="svc-key"\n'
CLI = "fakecli"
ENV = {"SUPABASE_CLI": CLI}


class StubRunner:
    def __init__(self, status_rc=0, docker_out=""):
        self.calls = []
        self.status_rc = status_rc
        self.docker_out = docker_out

    def __call__(self, cmd, **kwargs):
        self.calls.append(list(cmd))
        if cmd[0] == "docker":
            return subprocess.CompletedProcess(cmd, 0, stdout=self.docker_out, stderr="")
        if cmd[0] == "git":
            return subprocess.run(cmd, **kwargs)
        if "status" in cmd:
            err = "supabase start is not running." if self.status_rc else ""
            return subprocess.CompletedProcess(cmd, self.status_rc, stdout=STATUS_ENV, stderr=err)
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    def cli_calls(self):
        return [c for c in self.calls if c[0] == CLI]


def commit_supabase(repo):
    sdir = repo / "supabase"
    (sdir / "migrations").mkdir(parents=True)
    (sdir / "migrations" / "001_init.sql").write_text("select 1;\n")
    (sdir / "seed.sql").write_text("select 2;\n")
    (sdir / "config.toml").write_text(CONFIG_TEXT)
    git("add", "supabase", cwd=repo)
    git("commit", "--quiet", "-m", "supabase", cwd=repo)


@pytest.fixture
def stack_repo(repo):
    commit_supabase(repo)
    return repo


def test_start_runs_cli_with_workdir_and_excludes(stack_repo, capsys):
    wt = add_worktree(stack_repo, "story-72")
    stub = StubRunner()
    rc = supabase_local.main(["start", str(wt)], environ=ENV, run=stub)
    assert rc == 0
    workdir = Path(wt.resolve()) / ".supabase-local"
    (start,) = [c for c in stub.cli_calls() if "start" in c]
    assert start[:4] == [CLI, "start", "--workdir", str(workdir)]
    assert start[4:] == ["-x", "studio,mailpit,imgproxy,logflare,vector,edge-runtime"]
    rendered = tomllib.loads((workdir / "supabase" / "config.toml").read_text())
    assert rendered["project_id"] == "cholobots-story-72"
    assert rendered["api"]["port"] == 54331
    assert (workdir / "supabase" / "migrations").is_symlink()
    assert (workdir / "supabase" / "migrations" / "001_init.sql").read_text() == "select 1;\n"
    assert (workdir / "supabase" / "seed.sql").read_text() == "select 2;\n"
    out = capsys.readouterr().out.splitlines()
    assert out == [
        "SUPABASE_URL=http://127.0.0.1:54331",
        "SUPABASE_SERVICE_ROLE_KEY=svc-key",
        "SUPABASE_DB_URL=postgresql://postgres:postgres@127.0.0.1:54332/postgres",
    ]


def test_start_in_main_checkout_uses_repo_workdir_and_no_excludes(stack_repo):
    stub = StubRunner()
    assert supabase_local.main(["start", str(stack_repo)], environ=ENV, run=stub) == 0
    (start,) = [c for c in stub.cli_calls() if "start" in c]
    assert start == [CLI, "start", "--workdir", str(stack_repo.resolve())]
    assert not (stack_repo / ".supabase-local").exists()


def test_start_without_supabase_dir_is_a_noop(repo, capsys):
    stub = StubRunner()
    assert supabase_local.main(["start", str(repo)], environ=ENV, run=stub) == 0
    assert stub.cli_calls() == []
    assert "no supabase/config.toml" in capsys.readouterr().err


def test_start_refuses_a_port_in_use(stack_repo, monkeypatch, capsys):
    wt = add_worktree(stack_repo, "story-72")
    monkeypatch.setattr(supabase_local, "port_in_use", lambda port: port == 54332)
    stub = StubRunner()
    rc = supabase_local.main(["start", str(wt)], environ=ENV, run=stub)
    assert rc == 1
    err = capsys.readouterr().err
    assert "port 54332 (slot 1) is in use" in err
    assert [c for c in stub.cli_calls() if "start" in c] == []


def test_start_skips_port_probe_when_this_stack_is_already_running(stack_repo, monkeypatch):
    wt = add_worktree(stack_repo, "story-72")
    monkeypatch.setattr(supabase_local, "port_in_use", lambda port: True)
    stub = StubRunner(docker_out="supabase_db_cholobots-story-72\n")
    assert supabase_local.main(["start", str(wt)], environ=ENV, run=stub) == 0
    assert [c for c in stub.cli_calls() if "start" in c]


def test_start_probes_only_services_that_are_not_excluded(stack_repo, monkeypatch):
    wt = add_worktree(stack_repo, "story-72")
    probed = []
    monkeypatch.setattr(supabase_local, "port_in_use", lambda port: probed.append(port) or False)
    supabase_local.main(["start", str(wt)], environ=ENV, run=StubRunner())
    # api, db, shadow only: pooler disabled in config, studio/smtp/analytics/inspector excluded
    assert sorted(probed) == [54330, 54331, 54332]
    probed.clear()
    supabase_local.main(
        ["start", str(wt)], environ={**ENV, "SUPABASE_LOCAL_EXCLUDE": ""}, run=StubRunner()
    )
    assert {54333, 54334, 54337, 8084} <= set(probed)
    assert 54339 not in probed


def test_stop_removes_only_this_project_with_no_backup_and_releases_slot(stack_repo):
    wt = add_worktree(stack_repo, "story-72")
    stub = StubRunner()
    supabase_local.main(["start", str(wt)], environ=ENV, run=stub)
    ident = supabase_local.checkout_identity(str(wt), {})
    reg = supabase_local.registry_path(ident)
    assert supabase_local.registered_slots(reg) == {"story-72": 1}
    stub = StubRunner()
    assert supabase_local.main(["stop", str(wt)], environ=ENV, run=stub) == 0
    (stop,) = stub.cli_calls()
    workdir = str(Path(wt.resolve()) / ".supabase-local")
    assert stop == [CLI, "stop", "--workdir", workdir, "--project-id", "cholobots-story-72", "--no-backup"]
    assert supabase_local.registered_slots(reg) == {}
    assert not os.path.exists(workdir)


def test_stop_in_main_checkout_targets_cholobots_project(stack_repo):
    stub = StubRunner()
    assert supabase_local.main(["stop", str(stack_repo)], environ=ENV, run=stub) == 0
    (stop,) = stub.cli_calls()
    assert stop == [
        CLI, "stop", "--workdir", str(stack_repo.resolve()), "--project-id", "cholobots", "--no-backup",
    ]


def test_status_passes_flags_through_with_workdir(stack_repo):
    stub = StubRunner()
    rc = supabase_local.main(
        ["status", str(stack_repo), "-o", "env", "--override-name", "api.url=X"], environ=ENV, run=stub
    )
    assert rc == 0
    (status,) = stub.cli_calls()
    assert status == [
        CLI, "status", "--workdir", str(stack_repo.resolve()), "-o", "env", "--override-name", "api.url=X",
    ]


def test_passthrough_appends_workdir(stack_repo):
    stub = StubRunner()
    rc = supabase_local.main(["db", "reset"], environ={**ENV, "CHOLOBOTS_CHECKOUT": str(stack_repo)}, run=stub)
    assert rc == 0
    (call,) = stub.cli_calls()
    assert call == [CLI, "db", "reset", "--workdir", str(stack_repo.resolve())]


def test_passthrough_in_unstarted_worktree_explains_itself(stack_repo, capsys):
    wt = add_worktree(stack_repo, "story-72")
    stub = StubRunner()
    rc = supabase_local.main(["db", "reset", str(wt)], environ=ENV, run=stub)
    assert rc == 1
    assert "run scripts/supabase-local.sh start" in capsys.readouterr().err
    assert stub.cli_calls() == []


def test_env_prints_only_the_three_variables(stack_repo, capsys):
    stub = StubRunner()
    assert supabase_local.main(["env", str(stack_repo)], environ=ENV, run=stub) == 0
    (call,) = stub.cli_calls()
    assert call == [CLI, "status", "--workdir", str(stack_repo.resolve()), "-o", "env"]
    assert capsys.readouterr().out.splitlines()[0] == "SUPABASE_URL=http://127.0.0.1:54331"


def test_env_fails_when_status_fails(stack_repo, capsys):
    stub = StubRunner(status_rc=1)
    rc = supabase_local.main(["env", str(stack_repo)], environ=ENV, run=stub)
    assert rc == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "not running" in captured.err


def test_prune_stops_entries_without_worktree(stack_repo):
    wt = add_worktree(stack_repo, "story-72")
    ident = supabase_local.checkout_identity(str(wt), {})
    reg = supabase_local.registry_path(ident)
    supabase_local.allocate_slot(reg, "story-72")
    supabase_local.allocate_slot(reg, "bug-12")
    stub = StubRunner()
    assert supabase_local.main(["prune"], environ={**ENV, "CHOLOBOTS_CHECKOUT": str(stack_repo)}, run=stub) == 0
    (stop,) = stub.cli_calls()
    assert stop == [CLI, "stop", "--project-id", "cholobots-bug-12", "--no-backup"]
    assert supabase_local.registered_slots(reg) == {"story-72": 1}


@pytest.mark.parametrize(
    "argv",
    [["link", "--project-ref", "x"], ["login"], ["db", "push"], ["db", "reset", "--linked"], ["db", "dump", "--db-url=postgres://h"], ["start", "--db-url", "x"]],
)
def test_refuses_hosted_subcommands(stack_repo, capsys, argv):
    stub = StubRunner()
    rc = supabase_local.main(argv, environ={**ENV, "CHOLOBOTS_CHECKOUT": str(stack_repo)}, run=stub)
    assert rc == 2
    assert "refusing hosted operation" in capsys.readouterr().err
    assert stub.cli_calls() == []


def test_no_hosted_flags_ever_emitted(stack_repo):
    wt = add_worktree(stack_repo, "story-72")
    stub = StubRunner()
    for argv in (["start", str(wt)], ["env", str(wt)], ["status", str(wt)], ["stop", str(wt)], ["prune"]):
        supabase_local.main(argv, environ={**ENV, "CHOLOBOTS_CHECKOUT": str(stack_repo)}, run=stub)
    emitted = {tok for call in stub.cli_calls() for tok in call}
    assert emitted.isdisjoint({"--linked", "--db-url", "link", "login", "push"})
