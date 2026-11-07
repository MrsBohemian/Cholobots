# Test Failure Triage Protocol

When a check fails (a `py_compile` error, a pytest failure, a script test, or `ruff` once story #33
configures it) and you suspect it is unrelated to your changes, you MUST follow this protocol before
dismissing it. Do NOT claim a failure is "pre-existing" or "flaky" without verification.

## Step 1: Verify on the base branch

Never stash or switch branches in your own worktree: the stash is shared across worktrees and other
agents may be working in them. Check the base in a throwaway worktree instead:

```bash
git fetch upstream
git worktree add --detach /tmp/base-check-<ref> upstream/main   # or the story's base branch
cd /tmp/base-check-<ref>
# Run ONLY the specific failing check, not the full suite, for example:
python -m py_compile <failing file>
python -m pytest -q <tests/path/test_file.py>::<test_name>
bash tests/scripts/<test-script>.sh
```

## Step 2: Evaluate the result

### If the check PASSES on the base branch

Your changes caused the failure. Remove the throwaway worktree, return to your own worktree and fix
the regression:

```bash
git worktree remove --force /tmp/base-check-<ref>
```

### If the check FAILS on the base branch

The failure is genuinely pre-existing. Proceed to Step 3.

## Step 3: File or find a Taiga bug for the pre-existing failure

1. Record the base commit hash in the throwaway worktree: `git rev-parse HEAD`
2. Search Taiga for an existing bug about this check: `search(project=39, query="<test or file name>")`,
   one or two distinctive tokens at a time.
3. If no existing bug is found, file one following `.claude/skills/shared/bug-filing.md`:
   - Subject: `Pre-existing test failure: <test name>`
   - Description: the test name, test file, error message, commit hash where verified broken, and
     which branch/PR discovered it
   - Severity per `docs/bug-triage.md`: Important if the failure is deterministic on `main` (main
     is red for that check), Normal if it is flaky or environment-dependent
   - Priority: from the matrix (surface is `ci` once CI exists (story #35) and the failure shows
     there; `devx` for a failure in the local suite or setup)
   - Tags: surface tag, `src:agent`, and `pre-existing-failure`
4. Note the bug ref number, then remove the throwaway worktree:
   `git worktree remove --force /tmp/base-check-<ref>`

## Step 4: Return to your worktree and continue

Continue with your current task. Add the following to the PR description (or note it for whoever
creates the PR):

> **Known pre-existing failure:** #NNN: <test name>

## Summary

- NEVER dismiss a failure as pre-existing without running it on the base branch
- NEVER use `git stash` or switch branches in a shared worktree to check the base
- NEVER leave a genuinely pre-existing failure untracked: always file a Taiga bug
- ALWAYS reference pre-existing failure bugs in the PR description
