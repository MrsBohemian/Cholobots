# Contributing to Cholobots

This file explains who can change the Cholobots repositories and how a change reaches `main`.
Ownership and licensing are being decided under Taiga story #58 and will be added here once settled.

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
   open the pull request in that repository. Pushing there directly, rather than from a fork, is
   what lets stacked pull requests target each other.
4. Title the pull request with the Taiga ref: `#<ref>: <story subject>` or
   `fix(#<ref>): <bug subject>`.
5. Before opening or updating the pull request, run the checks
   [docs/testing-workflow.md](docs/testing-workflow.md) lists for what you changed. The floor for
   every change is `python -m py_compile cholobots.py config.py db/database.py bots/*.py`.
   Never start the bot (`python cholobots.py`) to test a change; it connects to Discord.
6. Changes reach `main` only by merging a pull request, never by pushing to `main` directly. Branch
   protection that enforces this, and any automated checks it requires, are set up under Taiga
   story #179.

If your shell exports a `GH_TOKEN` that cannot see `MrsBohemian/Cholobots`, unset it for GitHub CLI
calls and name the repository explicitly: `env -u GH_TOKEN gh pr create --repo MrsBohemian/Cholobots ...`.

The agent pipeline follows the same flow; its details are in
[.claude/autonomous-pipeline.md](.claude/autonomous-pipeline.md). Further rules (database
migrations, in-bot help text, commit attribution) are added to this file by Taiga story #52.
