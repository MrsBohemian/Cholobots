# ABOUTME: Tests the make-run preflight that keeps the bot on the local Supabase stack.
# ABOUTME: Pure functions take dicts; only the .env-file tests need python-dotenv.
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import check_local_env

LOCAL_URL = "http://127.0.0.1:54321"
SHELL_VARS = (
    "SUPABASE_URL",
    "SUPABASE_SERVICE_ROLE_KEY",
    "DISCORD_TOKEN",
    "OPENAI_API_KEY",
    "TRANSPORTATION_CHANNEL_ID",
)


def valid_env(**overrides):
    env = {
        "SUPABASE_URL": LOCAL_URL,
        "SUPABASE_SERVICE_ROLE_KEY": "service-role",
        "DISCORD_TOKEN": "discord-token",
        "OPENAI_API_KEY": "openai-key",
    }
    env.update(overrides)
    return env


def test_dotenv_value_overrides_shell():
    env = check_local_env.effective_env(
        {"SUPABASE_URL": LOCAL_URL}, {"SUPABASE_URL": "https://x.supabase.co"}
    )
    assert env["SUPABASE_URL"] == LOCAL_URL


def test_shell_value_used_when_dotenv_lacks_key():
    env = check_local_env.effective_env({}, {"DISCORD_TOKEN": "from-shell"})
    assert env["DISCORD_TOKEN"] == "from-shell"


def test_dotenv_key_without_value_does_not_override():
    env = check_local_env.effective_env({"DISCORD_TOKEN": None}, {"DISCORD_TOKEN": "from-shell"})
    assert env["DISCORD_TOKEN"] == "from-shell"


@pytest.mark.parametrize(
    "url", ["http://127.0.0.1:54321", "http://localhost:54321", "http://[::1]:54321"]
)
def test_local_urls_accepted(url):
    assert check_local_env.is_local_url(url)


@pytest.mark.parametrize(
    "url",
    [
        "https://abc.supabase.co",
        "http://127.0.0.1.evil.example",
        "http://localhost.evil.example",
        "ftp://127.0.0.1:54321",
        "http://[::1",
        "",
        None,
    ],
)
def test_hosted_and_lookalike_urls_rejected(url):
    assert not check_local_env.is_local_url(url)


def test_problems_empty_for_valid_local_env():
    assert check_local_env.problems(valid_env()) == []


def test_problems_reports_hosted_supabase_url():
    found = check_local_env.problems(valid_env(SUPABASE_URL="https://abc.supabase.co"))
    assert len(found) == 1
    assert "SUPABASE_URL" in found[0]
    assert "https://abc.supabase.co" in found[0]


@pytest.mark.parametrize("name", check_local_env.REQUIRED_SECRETS)
@pytest.mark.parametrize("value", ["", "   "])
def test_problems_reports_each_missing_secret(name, value):
    assert check_local_env.problems(valid_env(**{name: value})) == [f"{name} is empty in .env"]


def test_problems_reports_unset_secret():
    env = valid_env()
    del env["OPENAI_API_KEY"]
    assert check_local_env.problems(env) == ["OPENAI_API_KEY is empty in .env"]


@pytest.mark.parametrize("value", ["", "abc"])
def test_problems_reports_non_integer_transportation_channel_id(value):
    found = check_local_env.problems(valid_env(TRANSPORTATION_CHANNEL_ID=value))
    assert len(found) == 1
    assert "TRANSPORTATION_CHANNEL_ID" in found[0]


def test_transportation_channel_id_integer_or_missing_is_fine():
    assert check_local_env.problems(valid_env(TRANSPORTATION_CHANNEL_ID="123")) == []
    assert check_local_env.problems(valid_env()) == []


def _write_env(path, env):
    path.write_text("".join(f"{key}={value}\n" for key, value in env.items()), encoding="utf-8")


def _clear_shell(monkeypatch):
    for name in SHELL_VARS:
        monkeypatch.delenv(name, raising=False)


def test_main_exits_nonzero_and_prints_problems_for_hosted_url(tmp_path, monkeypatch, capsys):
    pytest.importorskip("dotenv")
    _clear_shell(monkeypatch)
    env_file = tmp_path / ".env"
    _write_env(env_file, valid_env(SUPABASE_URL="https://abc.supabase.co"))

    assert check_local_env.main(["--env-file", str(env_file)]) == 1
    err = capsys.readouterr().err
    assert "SUPABASE_URL" in err
    assert ".env.example" in err


def test_main_dotenv_beats_local_shell_value(tmp_path, monkeypatch):
    pytest.importorskip("dotenv")
    _clear_shell(monkeypatch)
    monkeypatch.setenv("SUPABASE_URL", LOCAL_URL)
    env_file = tmp_path / ".env"
    _write_env(env_file, valid_env(SUPABASE_URL="https://abc.supabase.co"))

    assert check_local_env.main(["--env-file", str(env_file)]) == 1


def test_main_exits_zero_for_valid_local_env(tmp_path, monkeypatch, capsys):
    pytest.importorskip("dotenv")
    _clear_shell(monkeypatch)
    env_file = tmp_path / ".env"
    _write_env(env_file, valid_env())

    assert check_local_env.main(["--env-file", str(env_file)]) == 0
    assert capsys.readouterr().err == ""


def test_main_without_env_file_uses_shell_and_hints_setup(tmp_path, monkeypatch, capsys):
    pytest.importorskip("dotenv")
    _clear_shell(monkeypatch)
    for key, value in valid_env().items():
        monkeypatch.setenv(key, value)

    assert check_local_env.main(["--env-file", str(tmp_path / "missing.env")]) == 0
    assert "make setup" in capsys.readouterr().err
