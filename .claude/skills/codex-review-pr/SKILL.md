---
name: codex-review-pr
description: Run a Codex (GPT-5.6 Luna) code review on a GitHub PR and post the findings as review threads
argument-hint: "<pr-number> [--dry-run]"
allowed-tools:
  - Bash(scripts/codex-review-pr.sh *)
  - Bash(env -u GH_TOKEN gh pr view *)
---

# Codex Review PR — #$ARGUMENTS

Trigger or re-run a Codex review on a PR without remembering the script invocation. The PR is on `MrsBohemian/Cholobots`; the script runs its `gh` calls as `env -u GH_TOKEN gh ... --repo MrsBohemian/Cholobots` itself. Any `gh pr view` you run here needs the same prefix and `--repo`. GitHub-native CI review does not exist for this repo; Codex is the only automated reviewer.

## Step 1: Parse arguments

Parse `<pr-number>` and the optional `--dry-run` flag from `$ARGUMENTS`. If no PR number is given, ask for one.

## Step 2: Run the review

Run `scripts/codex-review-pr.sh <pr> [--dry-run]` as a foreground Bash call with `timeout: 600000` (xhigh reasoning is thorough, so a pass takes 10-20 minutes on large PRs; the script hands back `REVIEW_PENDING` before the 10-minute Bash cap, so the call itself always returns in time).

## Step 3: Report the result

Report the last stdout line verbatim, and translate it for the reader:

- `REVIEW_POSTED:<pass>/<max>:<inline>:<body_only>` — pass N of M posted, X inline threads and Y body-only findings.
- `REVIEW_SKIPPED:same-head` — this head SHA was already reviewed; push a change first.
- `REVIEW_SKIPPED:cap` — the per-PR pass cap is exhausted; note that `CODEX_REVIEW_MAX_PASSES` can override it for a deliberate extra pass.
- `REVIEW_DRYRUN:<pass>/<max>:<inline>:<body_only>` — `--dry-run` only: the review that pass N of M *would* have posted, with X inline threads and Y body-only findings. Nothing was posted. The lines before it are the GitHub review payload JSON; summarize it rather than pasting it, and offer to re-run without `--dry-run` to post.
- `REVIEW_PENDING:<pass>/<max>` — the review is still running in a detached worker; it posts on its own even without a re-invoke. Re-run the exact same command to join the worker and wait again; keep re-invoking until the result changes to `REVIEW_POSTED`, `REVIEW_SKIPPED:same-head`, or a non-zero exit.

On a non-zero exit, report the stderr message and state that nothing was posted. Do not retry automatically.

Note that the posted threads are worked by `/pr-comments-resolve`.

For script internals or env var defaults beyond the override mentioned above, point at `scripts/codex-review-pr.sh --help`/`usage()` rather than restating them here. The shared protocol other skills use to call this script is `.claude/skills/shared/codex-review.md`.
