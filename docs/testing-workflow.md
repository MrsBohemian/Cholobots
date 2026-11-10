# Testing Workflow

When each test level runs and the exact commands. Placement rules are in
[testing-strategy.md](testing-strategy.md); naming, fixtures and fakes are in
[testing-conventions.md](testing-conventions.md).

## Guiding principle

Run the narrowest check that covers your change, every time, before you push. The floor is the
compile check and `ruff check .`; the bot has no full unit suite yet (#34). CI then runs the
whole scope on the PR: workflow `checks` (compile, lint, unit and script tests, the same scope as
`make check`) and workflow `integration` (`make test-integration` on a fresh local Supabase stack).

## Commands

| Check | Command | `make` | Status |
|---|---|---|---|
| Compile | `python -m py_compile cholobots.py config.py db/database.py bots/*.py` | `make compile` | Always. The floor for every change |
| Unit | `python -m pytest tests/unit -q` | (in `make test`) | Once `tests/unit/` exists (#34); before that record "no test suite yet (#34)" |
| Integration | `python -m pytest tests/integration -q -m integration` | `make test-integration` (the raw pytest command needs `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` exported) | When a change touches a command, listener or query; needs `make db-up`; local stack only |
| Script tests | `python -m pytest tests/scripts -q` and `bash tests/scripts/test-<name>.sh` | (in `make test`, which stops at the first failing `.sh`) | Exist today |
| Lint | `ruff check .` (also the pre-commit hook) | `make lint` | Always. Config and exceptions in `ruff.toml` |
| Unit + script tests | `python -m pytest -q tests/unit tests/scripts`, then `for t in tests/scripts/test-*.sh; do bash "$t" || exit 1; done` | `make test` | Same scope as CI's `Unit tests` and `Script tests` steps |
| Compile, lint, test | | `make check` | Before every push |
| Whole suite | `python -m pytest -q` | | Once #34 lands |

**Which database.** Integration tests run against the local stack of the checkout you are in. In the main checkout and in the devcontainer that is the default stack (API on 54321, `make db-up`). In a worktree it is that worktree's own stack: `make db-up` or `scripts/supabase-local.sh start` starts it on its own port block (API `54321+10n`, DB `54322+10n`) and `make test-integration` uses it. Without make: `eval "$(scripts/supabase-local.sh env | sed 's/^/export /')"` then `CHOLOBOTS_INTEGRATION_REQUIRED=1 python -m pytest tests/integration -q -m integration`. The harness refuses to guess the DB URL when `SUPABASE_URL` is off port 54321: export `SUPABASE_DB_URL` too (`env` prints it). Never point a worktree at another checkout's stack, and never at the hosted project. Details: `.claude/skills/shared/taiga-project.md` under "Local Supabase stacks".

Set up the environment once with `make setup`. The `make` targets call `.venv/bin/python` directly, so no activation is needed.
Never run `python cholobots.py` or `make run` from an automated run.

## What to run after a change

| You changed | Run |
|---|---|
| `bots/<module>.py`, `config.py`, `db/database.py`, `cholobots.py` | Compile; unit tests for that module (`python -m pytest tests/unit -q -k <module>`) once they exist; integration tests for the commands you touched once they exist |
| A pure helper | Compile; its unit tests (write the failing test first) |
| A command handler or listener with no automated test possible yet | Compile; write manual verification steps on the Taiga item and in the PR |
| `scripts/**` | `python -m pytest tests/scripts -q` plus the matching `bash tests/scripts/test-*.sh` |
| `tests/**` | The tests you changed |
| `scripts/supabase_local.py`, `scripts/supabase-local.sh` | `python -m pytest tests/scripts -q` plus `bash tests/scripts/test-supabase-local.sh` (two-worktree isolation test; it skips when Docker is unavailable, and its data-isolation and concurrent-suite steps need a host where the stack's API routes correctly) |
| `.devcontainer/**`, `scripts/devcontainer/**` | `python -m pytest tests/scripts -q -k devcontainer` and `bash tests/scripts/test-devcontainer-lifecycle.sh`; a `devcontainer up` smoke when the Dockerfile or lifecycle scripts change |
| `.claude/**`, `docs/**` | Nothing to run |
| `requirements.txt`, `requirements-dev.txt`, `.python-version` | Fresh venv, compile, `ruff check .`, script tests, whole suite once it exists |

## When each level runs

| Moment | What runs |
|---|---|
| During implementation (`implement-task`, `fix-single-bug`, `fix-story-bug`) | The affected rows above, after each change |
| Pre-push (`finalize-story-pr`, any skill that pushes) | Compile and lint always; unit when it exists; script tests if `scripts/` changed |
| In the PR | Codex review (`.claude/skills/shared/codex-review.md`) and GitHub Actions: `checks` (steps `Compile`, `Lint`, `Unit tests`, `Script tests`) and `integration` (steps `Start local Supabase`, `Integration tests`), reported by `gh pr checks`. A PR whose branch predates the workflows has no runs: the monitor counts that as passed after its two-minute run-registration grace when its base requires no checks; otherwise missing required runs stay `PENDING` until the branch is rebased onto a base that has the workflows |
| Failing check on a PR (`fix-build-failure`) | Reproduce the one failing check locally, fix, re-run it |
| Rebase (`rebase-branch`) | Compile, plus the tests for files with conflicts |
| Post-merge | Manual scripts (`docs/manual-tests/`, #36) for the commands the PR touched, in the test Discord server (#31), by the manual-testing agent or a person. The push to `main` reruns `checks` and `integration` |

## Reading results

pytest prints a summary line (`N passed, M failed`) and exits non-zero on any failure; read both.
`py_compile` prints nothing on success and a `SyntaxError` with file and line on failure. Bash
script tests exit non-zero on failure and print the failing case.

## Failures

A failure you believe is not caused by your change goes through
`.claude/skills/shared/test-failure-triage.md`: verify it on the base branch in a throwaway
worktree, then fix it or file it as `pre-existing-failure`. Never dismiss it unverified.

## In the PR description

List what ran and its result, for example:

```
Checks: py_compile clean; pytest tests/scripts 42 passed; no test suite yet (#34).
Manual verification: steps posted on #<ref>.
```
