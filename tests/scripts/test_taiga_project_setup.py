# ABOUTME: Pytest tests for scripts/taiga_project_setup.py against an in-memory fake Taiga transport.
# ABOUTME: Covers dry run (no writes), apply (only missing items), order renumbering, --check exit codes and shell-env credentials.
import io
import json
import os
import sys
from contextlib import redirect_stderr, redirect_stdout
from urllib.parse import parse_qs, urlparse

import pytest

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts"))

import taiga_project_setup as setup  # noqa: E402

BASE = "https://taiga.stub.test"
CREDS = {"TAIGA_API_URL": BASE, "TAIGA_USERNAME": "env-user", "TAIGA_PASSWORD": "env-pass"}


def _status(id_, name, order, closed=False):
    return {"id": id_, "name": name, "order": order, "is_closed": closed, "project": 39}


class FakeTaiga:
    """In-memory Taiga: project 39 as it stood before setup (the real pre-setup state)."""

    def __init__(self):
        self.story_statuses = [
            _status(76, "New", 1), _status(77, "Ready", 2), _status(78, "In progress", 3),
            _status(79, "Ready for test", 4), _status(80, "Done", 5, True), _status(81, "Archived", 6, True),
        ]
        self.issue_statuses = [
            _status(71, "New", 1), _status(72, "In progress", 2), _status(73, "Ready for test", 3),
            _status(74, "Closed", 4, True), _status(75, "Needs Info", 5), _status(76, "Rejected", 6, True),
            _status(77, "Postponed", 7),
        ]
        self.priorities = [
            {"id": 49, "name": "Low", "order": 1, "project": 39},
            {"id": 50, "name": "Normal", "order": 3, "project": 39},
            {"id": 51, "name": "High", "order": 5, "project": 39},
        ]
        self.tags = {"ci": None, "pipeline": None, "metichebot": None}
        self.wiki = [{"id": 5, "slug": "home", "project": 39}]
        self.next_id = 1000
        self.calls = []
        self.auth_bodies = []

    @property
    def writes(self):
        return [c for c in self.calls if c[0] != "GET" and not c[1].endswith("/api/v1/auth")]

    def _collection(self, path):
        return {
            "/api/v1/userstory-statuses": self.story_statuses,
            "/api/v1/issue-statuses": self.issue_statuses,
            "/api/v1/priorities": self.priorities,
        }.get(path)

    def __call__(self, method, url, headers, body):
        parsed = urlparse(url)
        path = parsed.path
        payload = json.loads(body) if body else None
        self.calls.append((method, path, payload))
        if path == "/api/v1/auth":
            self.auth_bodies.append(payload)
            return 200, {"auth_token": "tok"}
        assert headers.get("Authorization") == "Bearer tok"
        if method == "GET":
            if self._collection(path) is not None:
                assert parse_qs(parsed.query) == {"project": ["39"]}
                return 200, [dict(i) for i in self._collection(path)]
            if path == "/api/v1/projects/39":
                return 200, {"id": 39, "tags_colors": dict(self.tags)}
            if path == "/api/v1/wiki":
                return 200, list(self.wiki)
        if method == "POST" and self._collection(path) is not None:
            item = dict(payload, id=self.next_id)
            self.next_id += 1
            self._collection(path).append(item)
            return 201, item
        if method == "PATCH":
            base, _, item_id = path.rpartition("/")
            for item in self._collection(base) or []:
                if str(item["id"]) == item_id:
                    item.update(payload)
                    return 200, item
        if method == "POST" and path == "/api/v1/projects/39/create_tag":
            self.tags[payload["tag"]] = payload["color"]
            return 200, None
        if method == "POST" and path == "/api/v1/wiki":
            self.wiki.append({"id": self.next_id, "slug": payload["slug"], "project": payload["project"]})
            return 201, payload
        return 404, {"detail": "not found"}


def run(argv, fake, environ=None):
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        code = setup.main(argv, transport=fake, environ=CREDS if environ is None else environ)
    return code, out.getvalue(), err.getvalue()


def names_in_order(items):
    return [i["name"] for i in sorted(items, key=lambda i: i["order"])]


class TestDryRun:
    def test_default_dry_run_makes_no_writes(self):
        fake = FakeTaiga()
        code, out, _ = run([], fake)
        assert code == 0
        assert fake.writes == []
        assert "(dry run)" in out
        assert "write(s) --apply would make" in out
        assert "POST /api/v1/priorities" in out

    def test_dry_run_prints_before_after_table(self):
        code, out, _ = run([], FakeTaiga())
        assert code == 0
        assert "Ready for Analysis" in out and "create, order 2" in out
        assert "before" in out and "after" in out


class TestApply:
    def test_apply_creates_only_missing_items(self):
        fake = FakeTaiga()
        code, _, err = run(["--apply"], fake)
        assert code == 0, err
        posts = [(p, d) for m, p, d in fake.writes if m == "POST"]
        created_statuses = [d["name"] for p, d in posts if p == "/api/v1/userstory-statuses"]
        assert created_statuses == ["Ready for Analysis", "Ready for review", "Blocked"]
        assert [d["name"] for p, d in posts if p == "/api/v1/issue-statuses"] == ["Ready for review", "Blocked"]
        assert [d["name"] for p, d in posts if p == "/api/v1/priorities"] == ["Urgent"]
        created_tags = [d["tag"] for p, d in posts if p.endswith("/create_tag")]
        assert "ci" not in created_tags and "pipeline" not in created_tags
        assert len(created_tags) == len(setup.TAGS) - 2
        assert [d["slug"] for p, d in posts if p == "/api/v1/wiki"] == ["cross-project-execution-order"]

    def test_apply_sets_closed_flags_and_colours(self):
        fake = FakeTaiga()
        run(["--apply"], fake)
        blocked = next(s for s in fake.story_statuses if s["name"] == "Blocked")
        assert blocked["is_closed"] is False
        assert all("color" in d for m, p, d in fake.writes if m == "POST" and p != "/api/v1/wiki")
        tag_colours = [d["color"] for m, p, d in fake.writes if p.endswith("/create_tag")]
        assert len(set(tag_colours)) == len(tag_colours)

    def test_apply_renumbers_order_to_target_sequence(self):
        fake = FakeTaiga()
        run(["--apply"], fake)
        assert names_in_order(fake.story_statuses) == [t.name for t in setup.STORY_STATUSES]
        assert names_in_order(fake.issue_statuses) == [t.name for t in setup.ISSUE_STATUSES]
        assert names_in_order(fake.priorities) == ["Low", "Normal", "High", "Urgent"]
        assert [s["order"] for s in sorted(fake.story_statuses, key=lambda s: s["order"])] == list(range(1, 10))

    def test_apply_is_idempotent(self):
        fake = FakeTaiga()
        run(["--apply"], fake)
        fake.calls.clear()
        code, out, _ = run(["--apply"], fake)
        assert code == 0
        assert fake.writes == []
        assert "Nothing to do." in out

    def test_apply_matches_names_case_insensitively(self):
        fake = FakeTaiga()
        fake.priorities.append({"id": 52, "name": "urgent", "order": 7, "project": 39})
        run(["--apply"], fake)
        assert not any(p == "/api/v1/priorities" for m, p, d in fake.writes if m == "POST")

    def test_apply_never_changes_is_closed_or_names(self):
        fake = FakeTaiga()
        run(["--apply"], fake)
        for method, _, payload in fake.writes:
            if method == "PATCH":
                assert set(payload) == {"order"}

    def test_complete_relative_order_is_not_renumbered(self):
        fake = FakeTaiga()
        run(["--apply"], fake)
        # Spread the orders out while keeping the sequence: nothing to fix.
        for status in fake.story_statuses:
            status["order"] *= 10
        fake.calls.clear()
        code, _, _ = run(["--apply"], fake)
        assert code == 0
        assert fake.writes == []


class TestCheck:
    def test_check_exits_2_listing_missing_items(self):
        fake = FakeTaiga()
        code, out, _ = run(["--check"], fake)
        assert code == 2
        assert fake.writes == []
        assert "story status 'Ready for Analysis'" in out
        assert "priority 'Urgent'" in out
        assert "issue status order" in out
        assert "story status order" not in out
        assert "tag 'triaged'" in out and "tag 'ci'" not in out
        assert "wiki page 'cross-project-execution-order'" in out
        assert f"Run: {setup.APPLY_COMMAND}" in out
        assert "dv export project/taiga-growthgarage-jonroberts" in setup.APPLY_COMMAND

    def test_check_exits_0_when_complete(self):
        fake = FakeTaiga()
        run(["--apply"], fake)
        code, out, _ = run(["--check"], fake)
        assert code == 0
        assert "is complete" in out

    def test_check_exits_2_when_a_closed_status_is_flagged_open(self):
        fake = FakeTaiga()
        run(["--apply"], fake)
        done = next(i for i in fake.story_statuses if i["name"] == "Done")
        done["is_closed"] = False
        code, out, _ = run(["--check"], fake)
        assert code == 2
        assert "story status 'Done' is_closed=False, expected True" in out
        # --apply must still never rewrite is_closed.
        fake.calls.clear()
        run(["--apply"], fake)
        assert fake.writes == []

    def test_check_and_apply_are_exclusive(self):
        with pytest.raises(SystemExit):
            setup.parse_args(["--check", "--apply"])

    def test_http_error_exits_1(self):
        def broken(method, url, headers, body):
            if url.endswith("/api/v1/auth"):
                return 401, {"detail": "bad credentials"}
            raise AssertionError("no further calls expected")

        code, _, err = run(["--check"], broken)
        assert code == 1
        assert "HTTP 401" in err


class TestCredentials:
    def test_credentials_come_from_the_shell_env(self):
        fake = FakeTaiga()
        code, _, _ = run(["--check"], fake)
        assert code == 2
        assert fake.auth_bodies[0] == {"type": "normal", "username": "env-user", "password": "env-pass"}
        assert fake.calls[0][1] == "/api/v1/auth"

    def test_dry_run_names_the_account_in_use(self):
        _, out, _ = run([], FakeTaiga())
        assert "(dry run) as env-user" in out

    @pytest.mark.parametrize("key", setup.ENV_KEYS)
    def test_missing_or_blank_key_exits_1_before_any_request(self, key):
        fake = FakeTaiga()
        environ = dict(CREDS, **{key: "  "})
        code, _, err = run(["--check"], fake, environ=environ)
        assert code == 1
        assert f"{key} is not set" in err
        assert fake.calls == []
        del environ[key]
        code, _, err = run(["--check"], fake, environ=environ)
        assert code == 1
        assert f"{key} is not set" in err
        assert fake.calls == []
