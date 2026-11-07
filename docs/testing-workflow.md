# Testing Workflow

When each test level runs and the exact commands. Placement rules are in
[testing-strategy.md](testing-strategy.md); naming, fixtures and fakes are in
[testing-conventions.md](testing-conventions.md).

## Guiding principle

Run the narrowest check that covers your change, every time, before you push. Today that is mostly
the compile check, because the bot has no test suite (#34), no linter (#33) and no CI (#35). As
each lands, its row below stops being conditional.

## Commands

| Check | Command | Status |
|---|---|---|
| Compile | `python -m py_compile cholobots.py config.py db/database.py bots/*.py` | Always. The floor for every change |
| Unit | `python -m pytest tests/unit -q` | Once `tests/unit/` exists (#34); before that record "no test suite yet (#34)" |
| Integration | `python -m pytest tests/integration -q -m integration` | Once the local Supabase stack (#47) and harness (#51) exist; needs `supabase start` |
| Script tests | `python -m pytest tests/scripts -q` and `bash tests/scripts/test-<name>.sh` | Exist today |
| Lint | `ruff check .` | Once configured (#33); before that skip without comment |
| Whole suite | `python -m pytest -q` | Once #34 lands |

Set up the environment once: `python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt`.
Never run `python cholobots.py` from an automated run.

## What to run after a change

| You changed | Run |
|---|---|
| `bots/<module>.py`, `config.py`, `db/database.py`, `cholobots.py` | Compile; unit tests for that module (`python -m pytest tests/unit -q -k <module>`) once they exist; integration tests for the commands you touched once they exist |
| A pure helper | Compile; its unit tests (write the failing test first) |
| A command handler or listener with no automated test possible yet | Compile; write manual verification steps on the Taiga item and in the PR |
| `scripts/**` | `python -m pytest tests/scripts -q` plus the matching `bash tests/scripts/test-*.sh` |
| `tests/**` | The tests you changed |
| `.claude/**`, `docs/**` | Nothing to run |
| `requirements.txt` | Fresh venv, compile, whole suite once it exists |

## When each level runs

| Moment | What runs |
|---|---|
| During implementation (`implement-task`, `fix-single-bug`, `fix-story-bug`) | The affected rows above, after each change |
| Pre-push (`finalize-story-pr`, any skill that pushes) | Compile always; unit and lint when they exist; script tests if `scripts/` changed |
| In the PR | Codex review (`.claude/skills/shared/codex-review.md`). CI from #35 will run compile, lint and unit tests; until then `gh pr checks` reports no checks, which skills treat as a pass with a note, never a failure and never an endless wait |
| Failing check on a PR (`fix-build-failure`) | Reproduce the one failing check locally, fix, re-run it |
| Rebase (`rebase-branch`) | Compile, plus the tests for files with conflicts |
| Post-merge | Manual scripts (`docs/manual-tests/`, #36) for the commands the PR touched, in the test Discord server (#31), by the manual-testing agent or a person. Integration tests in CI once #35 and #51 land |

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
