# ABOUTME: Contract test for .devcontainer/Dockerfile.prebuild and the root .dockerignore.
# ABOUTME: Reads the files as text only; it never builds the image or starts Docker.
import re
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
DEVCONTAINER = ROOT / ".devcontainer"
PREBUILD = DEVCONTAINER / "Dockerfile.prebuild"
VERSION_ARGS = ("CLAUDE_CODE_VERSION", "CODEX_VERSION", "CHROME_DEVTOOLS_MCP_VERSION", "UV_VERSION")
ALLOWED_COPY_SOURCES = {"requirements.txt", "requirements-dev.txt", "tools/pytaiga-mcp/install.sh"}
PIP_WARMUP = "pip install -r requirements.txt -r requirements-dev.txt"


@pytest.fixture(scope="module")
def prebuild():
    return PREBUILD.read_text()


def instructions(text):
    """Dockerfile instructions with comments dropped and line continuations joined."""
    joined = re.sub(r"\\\n", " ", text)
    return [ln.strip() for ln in joined.splitlines() if ln.strip() and not ln.strip().startswith("#")]


def arg_value(text, name):
    match = re.search(rf"^ARG {name}=(\S+)$", text, re.M)
    assert match, f"ARG {name}= missing"
    return match.group(1)


def index_of(lines, prefix, start=0):
    for i in range(start, len(lines)):
        if lines[i].startswith(prefix):
            return i
    raise AssertionError(f"no instruction starting with {prefix!r} after line {start}")


def test_prebuild_has_aboutme_and_base_image_arg(prebuild):
    assert len(re.findall(r"^# ABOUTME: ", prebuild, re.M)) == 2
    assert prebuild.splitlines()[0].startswith("# ABOUTME: ")
    assert "ARG BASE_IMAGE=devcontainer-base" in prebuild
    assert "FROM ${BASE_IMAGE}" in instructions(prebuild)


def test_prebuild_links_package_to_repo(prebuild):
    assert 'LABEL org.opencontainers.image.source="https://github.com/MrsBohemian/Cholobots"' in prebuild


def test_tool_pins_are_exact_versions(prebuild):
    for name in VERSION_ARGS:
        assert re.fullmatch(r"\d+\.\d+\.\d+", arg_value(prebuild, name)), name
    npm = [ln for ln in instructions(prebuild) if "npm install -g" in ln]
    assert len(npm) == 1
    for pkg, arg in (
        ("chrome-devtools-mcp", "CHROME_DEVTOOLS_MCP_VERSION"),
        ("@anthropic-ai/claude-code", "CLAUDE_CODE_VERSION"),
        ("@openai/codex", "CODEX_VERSION"),
    ):
        assert f"{pkg}@${{{arg}}}" in npm[0]
    assert "uv==${UV_VERSION}" in prebuild


def tracked_text_files():
    out = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
        cwd=ROOT, check=True, capture_output=True, text=True,
    ).stdout.split("\n")
    for rel in filter(None, out):
        if rel.startswith("tests/") or not (ROOT / rel).is_file():
            continue
        try:
            yield rel, (ROOT / rel).read_text()
        except UnicodeDecodeError:
            continue


def test_each_pin_is_declared_once(prebuild):
    pins = [arg_value(prebuild, name) for name in VERSION_ARGS]
    install = (ROOT / "tools/pytaiga-mcp/install.sh").read_text()
    pins.append(re.search(r"^PYTAIGA_MCP_COMMIT=([0-9a-f]{40})$", install, re.M).group(1))
    files = list(tracked_text_files())
    for pin in pins:
        pattern = re.compile(rf"(?<![\w.]){re.escape(pin)}(?![\w.])")
        hits = [rel for rel, text in files for _ in pattern.finditer(text)]
        assert len(hits) == 1, f"{pin} appears in {hits}"


def test_prebuild_copies_only_dependency_files(prebuild):
    copies = [ln for ln in instructions(prebuild) if ln.startswith("COPY ")]
    assert copies
    for ln in copies:
        parts = [p for p in ln.split()[1:] if not p.startswith("--")]
        for src in parts[:-1]:
            assert src in ALLOWED_COPY_SOURCES, ln


def test_pip_cache_warmed_as_vscode(prebuild):
    lines = instructions(prebuild)
    user_vscode = index_of(lines, "USER vscode")
    warm = index_of(lines, "RUN", user_vscode)
    while PIP_WARMUP not in lines[warm]:
        warm = index_of(lines, "RUN", warm + 1)
    assert "--no-cache-dir" not in lines[warm]
    user_root = index_of(lines, "USER root", warm)
    cleanup = index_of(lines, "RUN rm -rf /tmp/prebuild", user_root)
    assert cleanup > warm
    users = [ln for ln in lines if ln.startswith("USER ")]
    assert users[-1] == "USER root"
    assert lines[-1] == "WORKDIR /"


def test_post_create_does_not_bypass_the_pip_cache():
    text = (DEVCONTAINER / "post-create.sh").read_text()
    assert "--no-cache-dir" not in text
    assert "PIP_NO_CACHE_DIR" not in text


def test_pytaiga_mcp_installed_to_opt_and_owned_by_vscode(prebuild):
    lines = instructions(prebuild)
    assert any(ln.startswith("ENV ") and "PYTAIGA_MCP_HOME=/opt/pytaiga-mcp" in ln for ln in lines)
    run = [ln for ln in lines if "/tmp/pytaiga-mcp/install.sh" in ln and ln.startswith("RUN")]
    assert len(run) == 1
    assert "chown -R vscode:vscode /opt/pytaiga-mcp" in run[0]
    assert run[0].index("install.sh") < run[0].index("chown")


def test_supabase_registry_points_at_docker_hub(prebuild):
    assert "ENV SUPABASE_INTERNAL_IMAGE_REGISTRY=docker.io" in instructions(prebuild)


def test_tool_version_guard_runs_at_build(prebuild):
    guards = [ln for ln in instructions(prebuild) if ln.startswith("RUN") and "claude --version" in ln]
    assert len(guards) == 1
    assert "codex --version" in guards[0]


def test_base_dockerfile_has_no_volatile_tools():
    base = (DEVCONTAINER / "Dockerfile").read_text()
    body = "\n".join(ln for ln in base.splitlines() if not ln.lstrip().startswith("#"))
    for forbidden in ("npm install -g", "pytaiga-mcp", "claude-code", "@openai/codex", "uv=="):
        assert forbidden not in body, forbidden


def test_dockerignore_keeps_secrets_and_junk_out():
    path = ROOT / ".dockerignore"
    lines = [ln.strip() for ln in path.read_text().splitlines() if ln.strip() and not ln.startswith("#")]
    for required in (".git", ".env", ".envrc", ".venv", ".claude/worktrees", "*.db"):
        assert required in lines, required
    for needed in (".devcontainer", "requirements.txt", "requirements-dev.txt", "tools"):
        assert not any(ln.strip("/") == needed for ln in lines), needed
    if ".env.*" in lines:
        assert "!.env.example" in lines
