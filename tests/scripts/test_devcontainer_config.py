# ABOUTME: Contract test for .devcontainer/: image pins, tool installs, mounts, env pass-through and lifecycle hooks.
# ABOUTME: Reads the files as text and strict JSON only; it never builds the image or starts Docker.
import json
import os
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
DEVCONTAINER = ROOT / ".devcontainer"
DIND = "ghcr.io/devcontainers/features/docker-in-docker:2.17.0"
PASS_THROUGH_ENV = {
    "DISCORD_TOKEN",
    "OPENAI_API_KEY",
    "TAIGA_API_URL",
    "TAIGA_USERNAME",
    "TAIGA_PASSWORD",
    "GH_TOKEN",
    "CHOLOBOTS_DEVCONTAINER",
    "SUPABASE",
}
SECRET_ENV = PASS_THROUGH_ENV - {"CHOLOBOTS_DEVCONTAINER", "SUPABASE"}


@pytest.fixture(scope="module")
def dockerfile():
    return (DEVCONTAINER / "Dockerfile").read_text()


@pytest.fixture(scope="module")
def config():
    return json.loads((DEVCONTAINER / "devcontainer.json").read_text())


@pytest.fixture(scope="module")
def lock():
    return json.loads((DEVCONTAINER / "devcontainer-lock.json").read_text())


def test_devcontainer_json_is_strict_json():
    json.loads((DEVCONTAINER / "devcontainer.json").read_text())


def test_base_image_python_matches_python_version(dockerfile):
    python_version = (ROOT / ".python-version").read_text().strip()
    assert re.search(
        rf"^FROM mcr\.microsoft\.com/devcontainers/python:2-{re.escape(python_version)}-trixie@sha256:[0-9a-f]{{64}}$",
        dockerfile,
        re.M,
    ), "base image must be python:2-<.python-version>-trixie pinned by digest"


def test_dockerfile_installs_required_tools(dockerfile):
    assert "postgresql-client" in dockerfile
    assert "cli.github.com/packages" in dockerfile and "install -y --no-install-recommends gh" in dockerfile
    assert "deb.nodesource.com/setup_22.x" in dockerfile
    assert '"registry-mirrors"' in dockerfile and "/etc/docker/daemon.json" in dockerfile
    assert re.search(r"^ENV CHROME_PATH=/usr/bin/google-chrome-stable$", dockerfile, re.M)


def test_chrome_install_is_behind_the_amd64_guard(dockerfile):
    match = re.search(r'if \[ "\$\(dpkg --print-architecture\)" = amd64 \]; then(.*?)\n\s*else', dockerfile, re.S)
    assert match, "Chrome install must sit in an amd64 if/else"
    body = match.group(1)
    assert "google-chrome-stable" in body
    assert '"$CHROME_PATH" --version' in body


def test_supabase_cli_pin_matches_the_rest_of_the_repo(dockerfile):
    match = re.search(r"^ARG SUPABASE_CLI_VERSION=(\S+)$", dockerfile, re.M)
    assert match and match.group(1) == "2.121.0"
    version = match.group(1)
    assert "sha256sum -c" in dockerfile and "checksums.txt" in dockerfile
    readme = (ROOT / "README.md").read_text()
    assert f"supabase@{version}" in readme
    makefile = ROOT / "Makefile"
    if not makefile.exists():
        pytest.skip("Makefile (#50) not present")
    assert re.search(rf"^SUPABASE_CLI_VERSION\s*:?=\s*{re.escape(version)}$", makefile.read_text(), re.M)


def test_docker_in_docker_feature_enabled_and_locked(config, lock):
    assert DIND in config["features"]
    entry = lock["features"][DIND]
    assert entry["version"] == "2.17.0"
    assert re.fullmatch(r"sha256:[0-9a-f]{64}", entry["integrity"])
    assert entry["resolved"].endswith("@" + entry["integrity"])


def test_docker_in_docker_does_not_use_moby_on_trixie(config):
    # The moby packages do not exist for Debian trixie, so the feature install fails with the default.
    assert config["features"][DIND].get("moby") is False


def test_mounts_claude_and_codex_config(config):
    mounts = config["mounts"]
    for name in (".claude", ".claude.json", ".codex"):
        assert f"source=${{localEnv:HOME}}/{name},target=/home/vscode/{name},type=bind" in mounts
    init = config["initializeCommand"]
    assert "mkdir -p" in init and ".claude" in init and ".codex" in init and "touch" in init


def test_container_env_is_exactly_the_pass_through_set(config):
    env = config["containerEnv"]
    assert set(env) == PASS_THROUGH_ENV
    for key in SECRET_ENV:
        assert re.fullmatch(r"\$\{localEnv:[A-Z_]+\}", env[key]), f"{key} must be a localEnv reference"
    assert env["DISCORD_TOKEN"] == "${localEnv:CHOLOBOTS_TEST_DISCORD_TOKEN}"
    assert env["CHOLOBOTS_DEVCONTAINER"] == "1"
    assert env["SUPABASE"] == "supabase"


def test_forwards_api_and_studio_only(config):
    assert config["forwardPorts"] == [54321, 54323]
    postgres = config.get("portsAttributes", {}).get("54322")
    if postgres is not None:
        assert postgres["onAutoForward"] == "ignore"


def test_venv_is_a_named_volume_on_the_path(config):
    target = "target=${containerWorkspaceFolder}/.venv"
    assert any(m.startswith("source=cholobots-venv-${devcontainerId}") and target in m and "type=volume" in m
               for m in config["mounts"])
    assert config["remoteEnv"]["PATH"].startswith("${containerWorkspaceFolder}/.venv/bin:")


@pytest.mark.parametrize("key,script", [("postCreateCommand", "post-create.sh"), ("postStartCommand", "post-start.sh")])
def test_lifecycle_commands_point_at_the_scripts(config, key, script):
    assert config[key] == f".devcontainer/{script}"
    path = DEVCONTAINER / script
    assert path.is_file()
    assert os.access(path, os.X_OK)


def test_nothing_from_you_thon_remains():
    banned = ("dotnet", "paket", "playwright", "zitadel", "docker compose", "ionide", "stripe", "youthon")
    for path in DEVCONTAINER.iterdir():
        if path.is_file():
            text = path.read_text().lower()
            for word in banned:
                assert word not in text, f"{word!r} found in {path.name}"
