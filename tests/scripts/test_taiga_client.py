# ABOUTME: Pytest tests for the shared scripts/lib/taiga_client.py Taiga REST client.
# ABOUTME: Verifies from_env fails fast on missing configuration and that command redaction masks credentials.
import json
import os
import subprocess
import sys

import pytest

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts", "lib"))

from taiga_client import TaigaClient, TaigaError, redact_cmd


class TestFromEnv:
    def test_missing_api_url_raises_before_any_request(self, monkeypatch):
        monkeypatch.delenv("TAIGA_API_URL", raising=False)
        monkeypatch.setenv("TAIGA_USERNAME", "u")
        monkeypatch.setenv("TAIGA_PASSWORD", "p")
        with pytest.raises(TaigaError, match="TAIGA_API_URL"):
            TaigaClient.from_env()


class TestRedactCmd:
    def test_masks_auth_payload_with_password(self):
        auth_cmd = [
            "curl", "-sf", "-H", "Content-Type: application/json", "-X", "POST",
            "-d", json.dumps({"type": "normal", "username": "u", "password": "s3cr3t"}),
            "https://taiga.example/api/v1/auth",
        ]
        rendered = redact_cmd(auth_cmd)
        assert "s3cr3t" not in rendered
        assert "-d <redacted>" in rendered
        # non-sensitive args are preserved
        assert "curl" in rendered
        assert "https://taiga.example/api/v1/auth" in rendered

    def test_masks_authorization_bearer_token(self):
        get_cmd = ["curl", "-sf", "-H", "Authorization: Bearer abc.def.ghi", "https://taiga.example/api/v1/x"]
        rendered = redact_cmd(get_cmd)
        assert "abc.def.ghi" not in rendered
        assert "Authorization: <redacted>" in rendered
        assert "https://taiga.example/api/v1/x" in rendered


class FakeCurl:
    """Stands in for subprocess.run: records calls and replays (status, body) responses."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, cmd, input=None, **kwargs):
        self.calls.append({"cmd": cmd, "input": input})
        status, body = self.responses.pop(0)
        return subprocess.CompletedProcess(cmd, 0, stdout=f"{body}\n{status}", stderr="")


def make_client(monkeypatch, responses):
    fake = FakeCurl([(200, json.dumps({"auth_token": "tok-123"})), *responses])
    monkeypatch.setattr(subprocess, "run", fake)
    return TaigaClient("https://taiga.example", "user", "s3cr3t-pw"), fake


class TestCredentialsStayOffArgv:
    def test_password_and_token_never_appear_in_curl_argv(self, monkeypatch):
        client, fake = make_client(monkeypatch, [(200, "[]")])
        client.get("/api/v1/issues")
        assert len(fake.calls) == 2
        for call in fake.calls:
            argv = " ".join(call["cmd"])
            assert "s3cr3t-pw" not in argv
            assert "tok-123" not in argv

    def test_credentials_are_delivered_on_stdin(self, monkeypatch):
        client, fake = make_client(monkeypatch, [(200, "[]")])
        client.get("/api/v1/issues")
        assert "s3cr3t-pw" in fake.calls[0]["input"]
        assert "tok-123" in fake.calls[1]["input"]


class TestListAll:
    def test_continues_past_a_short_page_until_an_empty_page(self, monkeypatch):
        client, fake = make_client(monkeypatch, [
            (200, json.dumps([{"id": 1}, {"id": 2}])),
            (200, json.dumps([{"id": 3}])),
            (200, "[]"),
        ])
        assert [i["id"] for i in client.list_all("/api/v1/issues")] == [1, 2, 3]
        assert len(fake.calls) == 4  # auth + 3 pages

    def test_404_past_the_last_page_ends_paging(self, monkeypatch):
        client, _ = make_client(monkeypatch, [
            (200, json.dumps([{"id": 1}])),
            (404, json.dumps({"_error_message": "Not found"})),
        ])
        assert [i["id"] for i in client.list_all("/api/v1/issues")] == [1]

    def test_404_on_the_first_page_is_an_error(self, monkeypatch):
        client, _ = make_client(monkeypatch, [(404, "{}")])
        with pytest.raises(TaigaError, match="404"):
            client.list_all("/api/v1/issues")

    def test_server_error_is_an_error(self, monkeypatch):
        client, _ = make_client(monkeypatch, [(500, "{}")])
        with pytest.raises(TaigaError, match="500"):
            client.list_all("/api/v1/issues")
