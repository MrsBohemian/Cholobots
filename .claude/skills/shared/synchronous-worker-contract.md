# Synchronous Worker Contract

You are spawned in the foreground and the orchestrator treats your return as "the work is done". Therefore:

- **Every Agent call MUST pass `run_in_background: false` explicitly.** The Agent tool backgrounds sub-agents by default, so omitting the flag is the same as backgrounding. **NEVER use `run_in_background: true`** on Bash calls for any work you were asked to complete (compile checks, test runs, scripts). Ending your turn can kill background tasks in headless mode, and the orchestrator will proceed as if the task finished.
- For slow commands (`python -m pytest`, `bash tests/scripts/test-*.sh`, `scripts/codex-review-pr.sh`), use a long foreground `timeout` (up to `600000` ms) instead of backgrounding. If a command needs longer, split it or re-invoke it; never background it. Never start the bot (`python cholobots.py`) from an automated run.
- Do not return until every step of the skill you were told to follow has actually completed and been verified against the artifacts that skill defines. Your agent definition lists the completion evidence expected of you. A skill that intentionally leaves work uncommitted (analysis output held for review, a review thread resolved by disagreeing) does not require a commit, but it still requires its own completion evidence.
- Your final message must state what was **verified**, not what was **started**. "Tests are running" or "check kicked off" is never an acceptable final state.
- When you were given a worktree path, work only there: never `cd` to the main checkout, check out another branch, `git reset --hard` or `git stash` (the stash is shared across worktrees).
