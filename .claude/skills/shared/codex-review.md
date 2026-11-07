# Codex Review Protocol

Run this immediately after `env -u GH_TOKEN gh pr create --repo MrsBohemian/Cholobots ...` succeeds, before any Taiga status update or final report.

## Step 1: Get the PR number

`gh pr create` prints the PR URL as its last output line; take the trailing number. Alternatively, on the just-pushed branch:

```bash
env -u GH_TOKEN gh pr view <branch> --repo MrsBohemian/Cholobots --json number -q .number
```

## Step 2: Run the review

Run this as a single foreground Bash call with `timeout: 600000` (a review pass takes 10-20 minutes on large PRs; the script hands back `REVIEW_PENDING` before the 10-minute Bash cap, so the call itself always returns in time):

```bash
scripts/codex-review-pr.sh <pr-number>
```

The script makes its own `gh` calls with `env -u GH_TOKEN` against `MrsBohemian/Cholobots`. Do NOT background it.

## Step 3: Read the contract line

The last line of stdout is exactly one of:

- `REVIEW_POSTED:<pass>/<max>:<inline>:<body_only>`: review threads were posted.
- `REVIEW_SKIPPED:cap`: the PR already has the maximum number of passes.
- `REVIEW_SKIPPED:same-head`: this head SHA was already reviewed.
- `REVIEW_PENDING:<pass>/<max>`: the review is still running detached; it will post on its own even if nothing re-invokes it.

Include that line verbatim in the skill's final report.

## Step 3a: Handle REVIEW_PENDING (re-invoke to join)

On `REVIEW_PENDING`, re-run the exact same command (`scripts/codex-review-pr.sh <pr-number>`). Each call either joins the still-running worker and waits again, or finds the worker's result and prints it. Keep re-invoking until the call reports `REVIEW_POSTED`, `REVIEW_SKIPPED:same-head`, or a non-zero exit; then proceed as in Step 3/Step 4. Do not skip straight to the rest of the skill on `REVIEW_PENDING`: the detached worker still posts on its own, but the caller should join rather than move on prematurely when a foreground wait is available.

## Step 4: Handle failure (non-fatal)

A non-zero exit means nothing was posted. Report the exit code and the stderr message and continue with the rest of the skill: never abort PR creation, never delete the PR, never retry.

## Note

The pass cap and the one-review-per-head-SHA guard live inside `scripts/codex-review-pr.sh`; callers must not track passes themselves. Codex is the only reviewer: GitHub runs no review or CI on push yet (CI arrives with story #35).
