# ABOUTME: Tests the devcontainer .env merge helper that points the bot at the in-container Supabase.
# ABOUTME: Pure functions take strings and dicts; main() runs against tmp_path with stdin and env patched.
import io
import os
import stat
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "devcontainer"))
import write_env  # noqa: E402

LOCAL_URL = "http://127.0.0.1:54321"
STATUS_TEXT = f'API_URL="{LOCAL_URL}"\nSERVICE_ROLE_KEY="eyJ.service"\nANON_KEY="anon"\n'
HEADER = "# Managed by .devcontainer/post-start.sh (scripts/devcontainer/write_env.py)"
PASSED = {"DISCORD_TOKEN": "test-token", "OPENAI_API_KEY": "openai-key"}


def values(environ=None):
    status = write_env.parse_status_env(STATUS_TEXT)
    managed, _ = write_env.managed_values(status, PASSED if environ is None else environ)
    return managed


def test_parse_status_env_strips_quotes():
    parsed = write_env.parse_status_env(STATUS_TEXT)
    assert parsed["API_URL"] == LOCAL_URL
    assert parsed["SERVICE_ROLE_KEY"] == "eyJ.service"


def test_parse_status_env_accepts_unquoted_values_and_skips_noise():
    parsed = write_env.parse_status_env(f"API_URL={LOCAL_URL}\nSERVICE_ROLE_KEY=k\n\nnoise\n")
    assert parsed == {"API_URL": LOCAL_URL, "SERVICE_ROLE_KEY": "k"}


@pytest.mark.parametrize("missing", ["API_URL", "SERVICE_ROLE_KEY"])
def test_missing_required_key_is_an_error(missing):
    lines = {"API_URL": LOCAL_URL, "SERVICE_ROLE_KEY": "k"}
    del lines[missing]
    text = "".join(f'{k}="{v}"\n' for k, v in lines.items())
    with pytest.raises(ValueError, match=missing):
        write_env.parse_status_env(text)


# Contract check only: the full table of local and lookalike hosts is owned by
# test_check_local_env.py (is_local_url). Every merge test below covers the accepted path.
def test_non_local_api_url_is_refused():
    with pytest.raises(ValueError, match="local"):
        write_env.managed_values({"API_URL": "https://abc.supabase.co", "SERVICE_ROLE_KEY": "k"}, {})


def test_merge_into_empty_env_writes_managed_keys():
    out = write_env.merge_env("", values())
    assert out == (
        f"{HEADER}\n"
        f"SUPABASE_URL={LOCAL_URL}\n"
        "SUPABASE_SERVICE_ROLE_KEY=eyJ.service\n"
        "DISCORD_TOKEN=test-token\n"
        "OPENAI_API_KEY=openai-key\n"
    )


def test_merge_replaces_hosted_url_and_keeps_other_lines():
    existing = "# mine\nSUPABASE_URL=https://x.supabase.co\n\nGUARDABOT_DB=/tmp/g.db\n"
    out = write_env.merge_env(existing, values())
    lines = out.splitlines()
    assert lines[:4] == ["# mine", f"SUPABASE_URL={LOCAL_URL}", "", "GUARDABOT_DB=/tmp/g.db"]
    assert "https://x.supabase.co" not in out


def test_passed_discord_token_replaces_existing_token():
    out = write_env.merge_env("DISCORD_TOKEN=prod\n", values())
    assert "DISCORD_TOKEN=test-token" in out
    assert "prod" not in out


def test_export_prefixed_line_is_replaced():
    out = write_env.merge_env("export DISCORD_TOKEN=prod\n", values())
    assert "prod" not in out
    assert out.count("DISCORD_TOKEN") == 1


def test_empty_passed_secret_keeps_existing_line_and_warns():
    status = write_env.parse_status_env(STATUS_TEXT)
    managed, warnings = write_env.managed_values(status, {"DISCORD_TOKEN": "  ", "OPENAI_API_KEY": "k"})
    assert "DISCORD_TOKEN" not in managed
    assert any("DISCORD_TOKEN" in w for w in warnings)
    out = write_env.merge_env("DISCORD_TOKEN=keepme\n", managed)
    assert "DISCORD_TOKEN=keepme" in out


def test_never_writes_pipeline_credentials():
    environ = {**PASSED, "TAIGA_PASSWORD": "pw", "GH_TOKEN": "ghp", "CHOLOBOTS_DEVCONTAINER": "1", "SUPABASE": "supabase"}
    out = write_env.merge_env("", values(environ))
    for name in ("TAIGA_PASSWORD", "GH_TOKEN", "CHOLOBOTS_DEVCONTAINER", "SUPABASE=", "pw", "ghp"):
        assert name not in out


def test_merge_is_idempotent():
    once = write_env.merge_env("# mine\nSUPABASE_URL=https://x.supabase.co\nFOO=bar", values())
    assert write_env.merge_env(once, values()) == once
    assert once.count(HEADER) == 1
    assert once.endswith("\n") and not once.endswith("\n\n")


def test_duplicate_managed_key_lines_collapse_to_one():
    existing = "SUPABASE_URL=https://a.supabase.co\nFOO=1\nSUPABASE_URL=https://b.supabase.co\n"
    out = write_env.merge_env(existing, values())
    assert out.count("SUPABASE_URL=") == 1
    assert out.splitlines()[0] == f"SUPABASE_URL={LOCAL_URL}"
    assert "FOO=1" in out


def run_main(monkeypatch, path, stdin_text, environ):
    for name in ("DISCORD_TOKEN", "OPENAI_API_KEY"):
        monkeypatch.delenv(name, raising=False)
    for name, value in environ.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(sys, "stdin", io.StringIO(stdin_text))
    return write_env.main([str(path)])


def test_main_writes_file_with_mode_0600(tmp_path, monkeypatch, capsys):
    target = tmp_path / ".env"
    target.write_text("FOO=bar\n")
    os.chmod(target, 0o644)
    assert run_main(monkeypatch, target, STATUS_TEXT, PASSED) == 0
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    assert f"SUPABASE_URL={LOCAL_URL}" in target.read_text()
    captured = capsys.readouterr()
    assert "test-token" not in captured.out + captured.err
    assert "eyJ.service" not in captured.out + captured.err
    assert not (tmp_path / ".env.tmp").exists()


def test_main_exits_nonzero_and_leaves_file_untouched_on_refusal(tmp_path, monkeypatch, capsys):
    target = tmp_path / ".env"
    target.write_text("SUPABASE_URL=https://x.supabase.co\n")
    hosted = 'API_URL="https://x.supabase.co"\nSERVICE_ROLE_KEY="k"\n'
    assert run_main(monkeypatch, target, hosted, PASSED) == 1
    assert target.read_text() == "SUPABASE_URL=https://x.supabase.co\n"
    assert "error:" in capsys.readouterr().err


def test_main_warns_on_missing_token_without_failing(tmp_path, monkeypatch, capsys):
    target = tmp_path / ".env"
    assert run_main(monkeypatch, target, STATUS_TEXT, {"OPENAI_API_KEY": "k"}) == 0
    assert "warning: DISCORD_TOKEN is not set" in capsys.readouterr().err
    assert "DISCORD_TOKEN" not in target.read_text()
