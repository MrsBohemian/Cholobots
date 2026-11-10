# ABOUTME: Unit tests for the guard that restricts the integration harness to the local Supabase stack.
# ABOUTME: The guard is stdlib-only, so these tests import no bot module and need no environment.
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "integration"))

from local_guard import DEFAULT_DB_URL, MAKE_HINT, integration_settings, is_local_url  # noqa: E402

LOCAL_API = "http://127.0.0.1:54321"


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1:54321",
        "http://localhost:54321",
        "http://[::1]:54321",
        "postgresql://postgres:postgres@127.0.0.1:54322/postgres",
    ],
)
def test_is_local_url_accepts_loopback_hosts(url):
    assert is_local_url(url) is True


@pytest.mark.parametrize(
    "url",
    [
        "https://abcd.supabase.co",
        "http://127.0.0.1.evil.example",
        "http://localhost.example.com",
        "",
        None,
    ],
)
def test_is_local_url_rejects_hosted_and_lookalike_hosts(url):
    assert is_local_url(url) is False


@pytest.mark.parametrize(
    "environ",
    [
        {},
        {"SUPABASE_URL": LOCAL_API},
        {"SUPABASE_SERVICE_ROLE_KEY": "key"},
        {"SUPABASE_URL": "", "SUPABASE_SERVICE_ROLE_KEY": "key"},
    ],
)
def test_integration_settings_requires_url_and_key(environ):
    settings, reason = integration_settings(environ)

    assert settings is None
    assert "make test-integration" in reason
    assert MAKE_HINT in reason


def test_integration_settings_rejects_non_local_url():
    settings, reason = integration_settings(
        {"SUPABASE_URL": "https://abcd.supabase.co", "SUPABASE_SERVICE_ROLE_KEY": "key"}
    )

    assert settings is None
    assert "abcd.supabase.co" in reason


def test_integration_settings_rejects_non_local_db_url():
    settings, reason = integration_settings(
        {
            "SUPABASE_URL": LOCAL_API,
            "SUPABASE_SERVICE_ROLE_KEY": "key",
            "SUPABASE_DB_URL": "postgresql://postgres:pw@db.abcd.supabase.co:5432/postgres",
        }
    )

    assert settings is None
    assert "db.abcd.supabase.co" in reason


def test_integration_settings_defaults_db_url_to_local_cli_port():
    settings, reason = integration_settings(
        {"SUPABASE_URL": LOCAL_API, "SUPABASE_SERVICE_ROLE_KEY": "key"}
    )

    assert settings is not None
    assert settings.url == LOCAL_API
    assert settings.service_key == "key"
    assert settings.db_url == DEFAULT_DB_URL == "postgresql://postgres:postgres@127.0.0.1:54322/postgres"
