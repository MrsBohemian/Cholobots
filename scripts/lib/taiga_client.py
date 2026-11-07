# ABOUTME: Shared curl-backed Taiga REST client used by scripts that read Taiga.
# ABOUTME: Authenticates with TAIGA_API_URL/TAIGA_USERNAME/TAIGA_PASSWORD and pages list endpoints.
import json
import os
import subprocess
from typing import List
from urllib.parse import urlencode


class TaigaError(RuntimeError):
    """Raised when a Taiga request cannot be completed."""


# curl flags whose following argument carries a request body (the Taiga auth
# payload includes the password).
SENSITIVE_DATA_FLAGS = {"-d", "--data", "--data-binary", "--data-raw"}


def redact_cmd(cmd: List[str]) -> str:
    """Render a command for error messages with credentials masked.

    The curl invocations carry the Taiga password (in the auth request body) and
    Bearer tokens (in Authorization headers); these must never reach logs or
    terminal history via an exception message.
    """
    redacted: List[str] = []
    mask_next = False

    for arg in cmd:
        if mask_next:
            redacted.append("<redacted>")
            mask_next = False
        elif arg in SENSITIVE_DATA_FLAGS:
            redacted.append(arg)
            mask_next = True
        elif arg.lower().startswith("authorization:"):
            redacted.append("Authorization: <redacted>")
        else:
            redacted.append(arg)

    return " ".join(redacted)


def run_json_command(cmd: List[str]) -> object:
    result = subprocess.run(cmd, check=False, capture_output=True, text=True)

    if result.returncode != 0:
        raise TaigaError(
            f"Command failed ({result.returncode}): {redact_cmd(cmd)}\n{result.stderr.strip()}",
        )

    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise TaigaError(
            f"Command produced invalid JSON: {redact_cmd(cmd)}",
        ) from exc


class TaigaHttpError(TaigaError):
    """Raised when Taiga answers with an HTTP error status."""

    def __init__(self, status: int, url: str):
        super().__init__(f"Taiga returned HTTP {status} for {url}")
        self.status = status


def _quote_config_value(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _request_json(curl_args: List[str], stdin_text: str) -> object:
    """Run curl with the given args (sensitive data on stdin) and return the parsed JSON body."""
    url = curl_args[-1]
    cmd = ["curl", "-sS", "-w", "\n%{http_code}", *curl_args]
    result = subprocess.run(cmd, input=stdin_text, check=False, capture_output=True, text=True)

    if result.returncode != 0:
        raise TaigaError(f"Command failed ({result.returncode}): {redact_cmd(cmd)}\n{result.stderr.strip()}")

    body, _, status_text = result.stdout.rpartition("\n")

    if not status_text.strip().isdigit():
        raise TaigaError(f"Command produced no HTTP status: {redact_cmd(cmd)}")

    status = int(status_text)

    if status >= 400:
        raise TaigaHttpError(status, url)

    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise TaigaError(f"Command produced invalid JSON: {redact_cmd(cmd)}") from exc


def get_required_env(name: str) -> str:
    value = os.environ.get(name)

    if not value:
        raise TaigaError(f"Missing required environment variable: {name}")

    return value


class TaigaClient:
    @classmethod
    def from_env(cls) -> "TaigaClient":
        return cls(
            get_required_env("TAIGA_API_URL"),
            get_required_env("TAIGA_USERNAME"),
            get_required_env("TAIGA_PASSWORD"),
        )

    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip("/")
        self.token = self._authenticate(username, password)

    def _authenticate(self, username: str, password: str) -> str:
        payload = {
            "type": "normal",
            "username": username,
            "password": password,
        }
        # The body travels on stdin so the password never appears in curl's argv
        # (readable from /proc/<pid>/cmdline).
        response = _request_json(
            [
                "-H",
                "Content-Type: application/json",
                "-X",
                "POST",
                "--data-binary",
                "@-",
                f"{self.base_url}/api/v1/auth",
            ],
            stdin_text=json.dumps(payload),
        )
        token = response.get("auth_token")

        if not token:
            raise TaigaError("Taiga auth response did not include auth_token")

        return token

    def get(self, path: str, **params: object) -> object:
        url = f"{self.base_url}{path}"

        if params:
            url = f"{url}?{urlencode(params)}"

        # The bearer token goes in a curl config read from stdin, keeping it out of argv.
        return _request_json(
            ["-K", "-", url],
            stdin_text=f"header = {_quote_config_value(f'Authorization: Bearer {self.token}')}\n",
        )

    def list_all(self, path: str, **params: object) -> List[dict]:
        items: List[dict] = []
        page = 1

        # Taiga may cap page_size below the requested value, so a short page is not
        # the last page. Page until an empty page, or the 404 Taiga returns past the end.
        while True:
            try:
                batch = self.get(path, page=page, page_size=100, **params)
            except TaigaHttpError as exc:
                if exc.status == 404 and page > 1:
                    break
                raise

            if not isinstance(batch, list):
                raise TaigaError(f"Expected list response for {path}")

            if not batch:
                break

            items.extend(batch)
            page += 1

        return items
