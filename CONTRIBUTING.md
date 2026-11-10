# Contributing to Cholobots

Who can change the Cholobots repositories and how a change reaches `main`.
Ownership and licensing terms are still pending.

## Access

- Both repositories, `MrsBohemian/Cholobots` and `MrsBohemian/Cholobots-Mobile`, belong to Heaven's
  GitHub account `MrsBohemian`, which is the admin of both.
- Growth Garage collaborators get write access to both repositories (today: `bigjonroberts`).
  Heaven adds and removes collaborators.
- The Claude Code agent pipeline works under Jon's GitHub account. Agents never receive the hosted
  Supabase keys or the production Discord token.
- The `Alterna-Dev-Studio/Cholobots` fork is a mirror. Changes land only through pull requests
  opened in `MrsBohemian/Cholobots`.

## How a change gets to main

1. Track the work in Taiga project `cholo-bots`
   (<https://backlog.growthgarage.org/project/cholo-bots>): a user story or a bug.
2. Branch from `main`: `feature/<ref>-<slug>` for a story, `fix/<ref>-<slug>` for a bug, where
   `<ref>` is the Taiga ref. A story that builds on another unmerged story branches from that
   story's branch instead, and its pull request targets that branch (a stacked pull request).
3. Push the branch to `MrsBohemian/Cholobots` (the `upstream` remote in a clone of the fork) and
   open the pull request there. Pushing there rather than to a fork lets stacked pull requests
   target each other.
4. Title the pull request with the Taiga ref: `#<ref>: <story subject>` or
   `fix(#<ref>): <bug subject>`.
5. Before opening or updating the pull request, run the checks
   [docs/testing-workflow.md](docs/testing-workflow.md) lists for what you changed. The floor for
   every change is `python -m py_compile cholobots.py config.py db/database.py bots/*.py`.
   Never start the bot (`python cholobots.py`) to test a change; it connects to Discord.
6. Changes reach `main` only by merging a pull request. Never push to `main` directly.

A few more habits:

- One Taiga item per pull request, built from small commits.
- `make check` runs the same compile, lint and test steps as the `checks` workflow in CI; run it
  before opening the pull request.
- The labels `close-tasks-on-merge` and `mark-done-on-merge` close Taiga work when the pull request
  merges; when to use them is in
  [.claude/skills/shared/merge-side-effects.md](.claude/skills/shared/merge-side-effects.md).
- Never commit `.env`. A local `.env` never holds the production Supabase keys or the production
  Discord token: `config.py` loads it with `override=True`, so it wins over anything your shell
  exports.

If your shell exports a `GH_TOKEN` that cannot see `MrsBohemian/Cholobots`, unset it for GitHub CLI
calls and name the repository explicitly: `env -u GH_TOKEN gh pr create --repo MrsBohemian/Cholobots ...`.

The agent pipeline follows the same flow; see
[.claude/autonomous-pipeline.md](.claude/autonomous-pipeline.md).

## Database changes

- Never change the schema in the Supabase dashboard or SQL editor.
- Every schema change is a new migration file under `supabase/migrations/`, in the same pull request
  as the code that needs it. The rule, the review checklist and the local loop are in
  [docs/database-migrations.md](docs/database-migrations.md).
- Agents never get production Supabase credentials. Pushing migrations to the hosted project is a
  gated step that only Jon approves.

## Adding or renaming a command

- Search every `name=` and `aliases=` in `cholobots.py` and `bots/*.py` first; several personas use
  unprefixed names (`note`, `lead`, `claim`, `cancel`, ...).
- Update that persona's help command (`!metichebot`, `!chismebot`, `!guardabot`, `!crudobot`,
  `!obijuan`, `!vueltabot`) and, for a headline feature, `!cholobots` in `cholobots.py`.
- Keep the persona's Spanglish voice.
- A command that takes an attachment must be added to the `attachment_commands` whitelist in
  `on_message` in `cholobots.py`.
- `crudo_projects`, `crudo_project_tasks`, `crudo_labor_entries` and `chisme_contacts` are read by
  other bots and by the dashboards in `Cholobots-Mobile`; check every reader before changing what a
  column means.
- The full list of hazards (listeners that eat replies, sessions, blocking calls, identity, time
  zones) is in [CLAUDE.md](CLAUDE.md).

## Commit attribution

- Commits written by a person need nothing extra.
- A commit made with an AI coding tool ends with a `Co-Authored-By: <model> <noreply@anthropic.com>`
  trailer naming the model actually used, and a pull request body it writes ends with the tool's
  generated-with line. Details:
  [.claude/skills/shared/commit-attribution.md](.claude/skills/shared/commit-attribution.md).

## Keeping docs current

- A pull request that changes setup, environment variables, commands or a shared table updates
  `README.md`, `CLAUDE.md` and the in-bot help text in the same pull request.
- `AGENTS.md` is a symlink to `CLAUDE.md` so Codex and Claude Code read the same notes. Edit
  `CLAUDE.md`. On Windows without symlink support `AGENTS.md` checks out as a one-line text file;
  never commit it as a regular file.
- `tests/unit/test_docs_contract.py` checks the required sections and that relative links resolve.

## Testing

- What to run for which change: [docs/testing-workflow.md](docs/testing-workflow.md). Which level a
  test belongs at: [docs/testing-strategy.md](docs/testing-strategy.md).
- Try Discord changes in the test server against the local Supabase stack:
  [docs/test-discord-server.md](docs/test-discord-server.md). Never start the bot against the
  production Discord server or the hosted Supabase project.
