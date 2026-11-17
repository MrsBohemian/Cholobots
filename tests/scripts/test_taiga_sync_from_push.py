# ABOUTME: Pytest tests for scripts/taiga_sync_from_push.py, the fork workflow's push-to-Taiga driver.
# ABOUTME: Covers the PR floor, de-duplication, skip rules, push ranges and how the update script is driven.
import io
import json
import os
import sys
import urllib.error
from email.message import Message
from types import TracebackType

import pytest

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts"))

import taiga_sync_from_push as sync  # noqa: E402
from taiga_sync_from_push import (  # noqa: E402
    ZERO_SHA,
    GitHubError,
    commits_in_push,
    fetch_pulls,
    min_pr_from_env,
    sync_pr,
    upstream_prs_for_commits,
)

BEFORE = "b" * 40
AFTER = "a" * 40


def gh_pr(number, title="", body="", branch="feature/x", labels=(), merged=True, base="main"):
    """A PR in the shape GET repos/{repo}/commits/{sha}/pulls returns."""
    return {
        "number": number,
        "title": title,
        "body": body,
        "head": {"ref": branch},
        "base": {"ref": base},
        "labels": [{"name": name} for name in labels],
        "merged_at": "2026-11-09T12:00:00Z" if merged else None,
        "html_url": f"https://github.com/MrsBohemian/Cholobots/pull/{number}",
    }


def fake_fetch(mapping):
    return lambda sha: mapping.get(sha, [])


class Recorder:
    """Captures update-script calls and returns a preset exit code per ref."""

    def __init__(self, failing_refs=()):
        self.calls = []
        self.failing_refs = {str(ref) for ref in failing_refs}

    def __call__(self, argv, env_extra):
        self.calls.append((argv, env_extra))
        ref = argv[-3]
        return 1 if ref in self.failing_refs else 0


def numbers(prs):
    return [pr["number"] for pr in prs]


# --- floor and filtering ----------------------------------------------------


def test_floor_skips_pr_7_and_keeps_pr_8():
    logs = []
    prs = upstream_prs_for_commits(
        ["s6", "s7", "s8"],
        fake_fetch({"s6": [gh_pr(6)], "s7": [gh_pr(7)], "s8": [gh_pr(8)]}),
        min_pr=8,
        log=logs.append,
    )
    assert numbers(prs) == [8]
    assert "PR #7 is below TAIGA_SYNC_MIN_UPSTREAM_PR=8, skipped" in logs
    assert "PR #6 is below TAIGA_SYNC_MIN_UPSTREAM_PR=8, skipped" in logs


def test_dedupes_pr_seen_through_merge_and_head_commits():
    prs = upstream_prs_for_commits(
        ["head", "merge"],
        fake_fetch({"head": [gh_pr(9)], "merge": [gh_pr(9)]}),
        min_pr=8,
        log=lambda _: None,
    )
    assert numbers(prs) == [9]


def test_skips_unmerged_and_non_main_base():
    logs = []
    prs = upstream_prs_for_commits(
        ["s1"],
        fake_fetch({"s1": [gh_pr(10, merged=False), gh_pr(11, base="feature/x"), gh_pr(12)]}),
        min_pr=8,
        log=logs.append,
    )
    assert numbers(prs) == [12]
    assert any("PR #10" in line and "not merged" in line for line in logs)
    assert any("PR #11" in line and "feature/x" in line for line in logs)


def test_commit_without_pr_is_skipped():
    logs = []
    prs = upstream_prs_for_commits(["lonely"], fake_fetch({}), min_pr=8, log=logs.append)
    assert prs == []
    assert "commit lonely has no upstream PR, skipped" in logs


def test_order_follows_commit_order():
    prs = upstream_prs_for_commits(
        ["s1", "s2", "s3"],
        fake_fetch({"s1": [gh_pr(14)], "s2": [gh_pr(9)], "s3": [gh_pr(14), gh_pr(11)]}),
        min_pr=8,
        log=lambda _: None,
    )
    assert numbers(prs) == [14, 9, 11]


def test_returned_pr_carries_what_the_update_needs():
    [pr] = upstream_prs_for_commits(
        ["s1"],
        fake_fetch({"s1": [gh_pr(8, title="#184: x", body=None, branch="feature/184-x",
                                   labels=("close-tasks-on-merge",))]}),
        min_pr=8,
        log=lambda _: None,
    )
    assert pr == {
        "number": 8,
        "title": "#184: x",
        "body": "",
        "branch": "feature/184-x",
        "labels": ["close-tasks-on-merge"],
        "url": "https://github.com/MrsBohemian/Cholobots/pull/8",
    }


def test_min_pr_env_defaults_to_8_and_rejects_non_int():
    assert min_pr_from_env({}) == 8
    assert min_pr_from_env({"TAIGA_SYNC_MIN_UPSTREAM_PR": ""}) == 8
    assert min_pr_from_env({"TAIGA_SYNC_MIN_UPSTREAM_PR": "12"}) == 12
    with pytest.raises(ValueError):
        min_pr_from_env({"TAIGA_SYNC_MIN_UPSTREAM_PR": "eight"})


# --- push range ---------------------------------------------------------------


class FakeGit:
    def __init__(self, known=(), rev_list=""):
        self.known = set(known)
        self.rev_list = rev_list
        self.calls = []

    def __call__(self, args):
        self.calls.append(args)
        if args[0] == "cat-file":
            return (0 if args[2].split("^")[0] in self.known else 1), ""
        if args[0] == "rev-list":
            return 0, self.rev_list
        raise AssertionError(f"unexpected git call {args}")


def test_zero_before_processes_only_after():
    git = FakeGit()
    logs = []
    assert commits_in_push(ZERO_SHA, AFTER, git, log=logs.append) == [AFTER]
    assert not any(call[0] == "rev-list" for call in git.calls)
    assert logs


def test_unknown_before_processes_only_after():
    git = FakeGit(known=())
    logs = []
    assert commits_in_push(BEFORE, AFTER, git, log=logs.append) == [AFTER]
    assert any(BEFORE in line for line in logs)


def test_known_before_lists_range_oldest_first():
    git = FakeGit(known={BEFORE}, rev_list="c1\nc2\n")
    assert commits_in_push(BEFORE, AFTER, git, log=lambda _: None) == ["c1", "c2"]
    assert ["rev-list", "--reverse", f"{BEFORE}..{AFTER}"] in git.calls


# --- driving the update script ------------------------------------------------


def test_drives_update_script_per_claimed_ref():
    run_update = Recorder()
    pr = {
        "number": 8,
        "title": "#184: Run the Taiga sync on the fork",
        "body": "Some text\nFixes bug #12\n",
        "branch": "feature/184-fork-taiga-sync",
        "labels": ["close-tasks-on-merge", "mark-done-on-merge"],
        "url": "https://github.com/MrsBohemian/Cholobots/pull/8",
    }
    assert sync_pr(pr, run_update, log=lambda _: None) is True

    assert [argv[1:] for argv, _ in run_update.calls] == [
        ["--skip-if-synced", "184", "8", pr["url"]],
        ["--skip-if-synced", "12", "8", pr["url"]],
    ]
    for argv, env_extra in run_update.calls:
        assert argv[0].endswith("taiga-update-on-merge.sh")
        assert env_extra == {"PR_LABELS": "close-tasks-on-merge mark-done-on-merge"}


def test_bare_mention_drives_nothing():
    run_update = Recorder()
    logs = []
    pr = {
        "number": 9,
        "title": "docs: mention #184 in passing",
        "body": "See #12 for context.",
        "branch": "docs/notes",
        "labels": [],
        "url": "https://github.com/MrsBohemian/Cholobots/pull/9",
    }
    assert sync_pr(pr, run_update, log=logs.append) is True
    assert run_update.calls == []
    assert any("claims no Taiga item" in line for line in logs)


def test_dry_run_is_passed_through():
    run_update = Recorder()
    pr = {"number": 8, "title": "#184: x", "body": "", "branch": "", "labels": [],
          "url": "https://github.com/MrsBohemian/Cholobots/pull/8"}
    sync_pr(pr, run_update, dry_run=True, log=lambda _: None)
    [(argv, _)] = run_update.calls
    assert argv[1:] == ["--dry-run", "--skip-if-synced", "184", "8", pr["url"]]


def test_update_failure_continues_then_exits_nonzero(monkeypatch, capsys):
    run_update = Recorder(failing_refs={184})
    fetch = fake_fetch({
        "c1": [gh_pr(8, title="#184: first", body="Fixes bug #12")],
        "c2": [gh_pr(9, title="fix(#13): second")],
    })
    git = FakeGit(known={BEFORE}, rev_list="c1\nc2\n")
    monkeypatch.setenv("GITHUB_TOKEN", "t")
    monkeypatch.delenv("TAIGA_SYNC_MIN_UPSTREAM_PR", raising=False)

    rc = sync.main(["--before", BEFORE, "--after", AFTER],
                   run_git=git, fetch=lambda sha, token: fetch(sha), run_update=run_update)

    assert rc == 1
    assert [argv[-3] for argv, _ in run_update.calls] == ["184", "12", "13"]


def test_main_reads_push_range_from_env_and_succeeds(monkeypatch):
    run_update = Recorder()
    seen = []

    def fetch(sha, token):
        seen.append((sha, token))
        return [gh_pr(8, title="#184: x")]

    monkeypatch.setenv("GITHUB_TOKEN", "tok")
    monkeypatch.setenv("PUSH_BEFORE", ZERO_SHA)
    monkeypatch.setenv("PUSH_AFTER", AFTER)
    rc = sync.main([], run_git=FakeGit(), fetch=fetch, run_update=run_update)

    assert rc == 0
    assert seen == [(AFTER, "tok")]
    assert len(run_update.calls) == 1


def test_main_requires_github_token(monkeypatch):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    rc = sync.main(["--before", ZERO_SHA, "--after", AFTER],
                   run_git=FakeGit(), fetch=lambda s, t: [], run_update=Recorder())
    assert rc == 2


def test_main_github_error_is_a_red_run(monkeypatch, capsys):
    def fetch(sha, token):
        raise GitHubError("GET commits/aaa/pulls failed: HTTP 403")

    monkeypatch.setenv("GITHUB_TOKEN", "tok")
    rc = sync.main(["--before", ZERO_SHA, "--after", AFTER],
                   run_git=FakeGit(), fetch=fetch, run_update=Recorder())
    assert rc == 1
    assert "HTTP 403" in capsys.readouterr().err


# --- GitHub read --------------------------------------------------------------


class FakeResponse(io.BytesIO):
    status = 200

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        return None


def test_fetch_pulls_requests_upstream_commit_pulls(monkeypatch):
    captured = {}

    def urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["headers"] = dict(request.header_items())
        return FakeResponse(json.dumps([gh_pr(8)]).encode())

    monkeypatch.setattr(sync.urllib.request, "urlopen", urlopen)
    assert numbers(fetch_pulls("abc", "tok")) == [8]
    assert captured["url"] == "https://api.github.com/repos/MrsBohemian/Cholobots/commits/abc/pulls"
    assert captured["headers"]["Authorization"] == "Bearer tok"
    assert captured["headers"]["Accept"] == "application/vnd.github+json"


def test_fetch_pulls_http_error_names_the_status(monkeypatch):
    def urlopen(request, timeout):
        raise urllib.error.HTTPError(request.full_url, 404, "Not Found", Message(), io.BytesIO(b"{}"))

    monkeypatch.setattr(sync.urllib.request, "urlopen", urlopen)
    with pytest.raises(GitHubError, match="HTTP 404"):
        fetch_pulls("abc", "tok")
