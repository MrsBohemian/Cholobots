# Cholobots Taiga Project Reference

The single place that names Cholobots' Taiga project, its statuses, fields, tags and access routes.
Skills and agents cite this file instead of fetching ids at runtime. When Jon adds or renames a
status, type, severity, priority or tag in Taiga, update this file (and the target list in
`scripts/taiga_project_setup.py`) and nothing else.

## Project

| Item | Value |
|---|---|
| Instance | `https://backlog.growthgarage.org` |
| Project | id **39**, slug `cholo-bots` |
| Ref namespace (cross-project roadmap) | `cholobots#N` |
| Agent account | `slave1-codex` (user id 20) |
| Escalation owner | Jon Roberts (user id 13) |
| Product owner, upstream repo owner | Heaven Handley (user id 22; full name in Taiga is spelled `Heavan Handly`) |
| Milestones (sprints) | none yet |

Every MCP call passes `project=39`. Every REST script defaults to project 39.

**Assignees resolve by full name.** The workflow server matches the `username` / `assignee`
argument against member full names or emails, not Taiga usernames. Always write
`"Jon Roberts"`; Heaven is `"Heavan Handly"` (sic).

## Prerequisite: project setup

The pipeline uses statuses, a priority, tags and a wiki page that project 39 did not start with.
Jon (Taiga admin) adds them once with `scripts/taiga_project_setup.py` (idempotent, never deletes
or renames):

```bash
cp scripts/taiga-auth.env.example scripts/taiga-auth.env && $EDITOR scripts/taiga-auth.env
python3 scripts/taiga_project_setup.py --env-file scripts/taiga-auth.env --apply
```

**Preflight rule.** Orchestrating skills (`overseer`, `fully-*`, `implement-epic`, `merge-train`,
`triage-bugs`, `fix-bugs`, `parallel-fix-bugs`) run `python3 scripts/taiga_project_setup.py --check`
once at startup. It checks the statuses, the Urgent priority, the pipeline/surface/source tags and
the roadmap wiki page. Exit 0: proceed. Exit 2: stop, print the missing list and
`Run: python3 scripts/taiga_project_setup.py --env-file scripts/taiga-auth.env --apply` for Jon
(`triage-bugs --dry-run` prints them as a warning and continues, since it writes nothing).
Leaf skills do not preflight.

## Statuses

Names are exact. MCP tools take the name. Ids are listed only for statuses that existed before the
setup script; ids of the added ones (marked *new*) are resolved by name at runtime and never
hardcoded.

**User stories (target order):** `New`(76) → `Ready for Analysis`(*new*) → `Ready`(77) →
`In progress`(78) → `Ready for review`(*new*) → `Ready for test`(79) → `Done`(80, closed);
`Blocked`(*new*, open); `Archived`(81, closed).

**Issues (target order):** `New`(71) → `In progress`(72) → `Ready for review`(*new*) →
`Ready for test`(73) → `Closed`(74, closed); `Blocked`(*new*, open), `Needs Info`(75),
`Rejected`(76, closed), `Postponed`(77).

**Tasks:** `New`(59), `In progress`(60), `Ready for test`(61), `Closed`(62, closed), `Needs Info`(63).
`implement-task` closes a finished task with `Closed`.

**Epics:** `New`, `Ready`, `In progress`, `Ready for test`, `Done`. The workflow MCP server cannot
edit epic descriptions; use `scripts/taiga-epic.sh` (below).

Issue and story status ids overlap numerically (76 and 77 exist in both lists); always pair an id
with its entity type.

### Lifecycle

| Stage | Story | Issue (bug) |
|---|---|---|
| Waiting for analysis | `Ready for Analysis` (analysis skills also accept `New`) | `New` + `triaged` |
| Analyzed | `Ready` + `llm-analyzed` | `New` + `triaged` + `llm-analyzed` |
| Implementing / fixing | `In progress` | `In progress` |
| PR open | `Ready for review` | `Ready for review` |
| Merged, awaiting QA | `Ready for test` | `Ready for test` (only when QA is wanted) |
| Finished | `Done` | `Closed` on merge |
| Escalated | `Blocked` + `escalation-needed` + assigned `"Jon Roberts"` | same |
| Missing information | comment, stays in place | `Needs Info` |
| Not a bug / duplicate | `Archived` | `Rejected` |
| Deferred | stays in the backlog | `Postponed` |

## Issue fields

| Field | Values (low to high) |
|---|---|
| Type | `Bug`(50), `Question`(51), `Enhancement`(52) |
| Severity | `Wishlist`(59), `Minor`(60), `Normal`(61), `Important`(62), `Critical`(63) |
| Priority | `Low`(49), `Normal`(50), `High`(51), `Urgent`(*new*) |

`Urgent` bugs are reported as `Run /fix-single-bug <ref> now` and excluded from fix batches. The
rubric is `docs/bug-triage.md`.

## Tags

Taiga tags are free text; spell them exactly. Writes replace the whole tag list, so always
read-modify-write. `get_issue` / `get_story` return tags as `[name, color]` pairs;
`update_issue(tags=[...])` / `update_story(tags=[...])` take plain names, so flatten before writing.

**Pipeline tags** (created by the setup script)

| Tag | Set by | Meaning |
|---|---|---|
| `triaged` | `/triage-bugs` only | Bug satisfies the definition of triaged in `docs/bug-triage.md` |
| `llm-analyzed` | `analyze-story`, `analyze-bug` | Analysis and implementation plan are on the item |
| `escalation-needed` | any orchestrator or the overseer on escalation | Automation gave up; Jon owns the next step |
| `epic-candidate` | `analyze-story`, `fully-analyze-stories` | Story is too large for one PR and should become an epic |
| `mark-done-on-merge` | `analyze-story` (single-PR stories) | Post-merge sync marks the story Done / issue Closed |
| `close-tasks-on-merge` | `analyze-story` | Post-merge sync closes the story's open tasks |
| `pre-existing-failure` | `test-failure-triage` | Test failure verified on the base branch |
| `rebase-conflict` | `rebase-branch` on escalation | Rebase could not be resolved automatically; accompanies `escalation-needed` |
| `design-reviewed` | `design-story` | Discord interaction design is on the story; analysis may proceed |
| `analysis-rejected` | `fully-analyze-stories` | `analyze-story` returned `ANALYSIS_REJECTED`; the story needs rework before analysis |
| `spike-complete` | `research-spike` | Every deliverable of a research spike landed |

The two merge tags are defined in `.claude/skills/shared/merge-side-effects.md`.

**Surface** (exactly one per bug): `product` (a bot command or dashboard behaviour; add every
per-bot tag that applies alongside it, or none with a triage comment saying no bot could be
determined), `devx` (local setup, venv, env vars, Supabase local stack, seeding), `pipeline`
(skills, agents, scripts, Taiga and GitHub automation), `ci` (GitHub Actions, once story #35 adds
them). `ci` and `devx` are the surface tags. The pre-existing topic tag `devex` (different
spelling) and the other topic tags below are kept alongside the surface tag; "drop any second
surface tag" applies only to `product`, `devx`, `pipeline` and `ci`.

**Source** (exactly one per bug): `src:user`, `src:monitor`, `src:manual-test`, `src:pr-review`,
`src:agent`, `src:jon`. The default when none is set is in `docs/bug-triage.md` ("Fields and tags").

**Per-bot tags** (one or more on a `product` bug): `metichebot`, `chismebot`, `guardabot`,
`crudobot`, `obijuan`, `vueltabot`, `mobile`.

**Topic tags in use:** `listeners`, `data-loss`, `timezone`, `devex`, `testing`, `ci`,
`multi-business`, `help-text`, `closeout`, `receipts`, `leads`, `pings`, `time-tracking`,
`startup`, `openai`. Reuse an existing topic tag before coining a new one.

## Epics and wiki

| Epic | Subject |
|---|---|
| #2 | Assessment baseline (2026-11-06) |
| #3 | Multi-business isolation |
| #46 | Developer experience (local Supabase, migrations, tests, CI) |
| #59 | Natural-language entry point |
| #60 | Wearable notification MVP |
| #61 | Website to Discord onboarding |
| #62 | Quarterly guild quest |
| #63 | Punch lists |

Stories are #21 to #64, issues #1 to #57 at the time of writing. Key infrastructure stories:
#31 test Discord server and env, #33 pinning and lint, #34 unit tests, #35 CI, #36 manual test
scripts, #47 local Supabase stack, #48 seed fixtures, #51 integration harness.

Wiki slugs: `home`, `deep-dive-metichebot`, `deep-dive-chismebot-guardabot`,
`deep-dive-crudobot-obijuan-vueltabot`, `deep-dive-mobile`, `repo-hygiene-2026-11-06`, and
`cross-project-execution-order`: the roadmap page (sections `## Dependency Diagram` and
`## Cross-project dependency register`) that `roadmap-wiki-update` appends to and the overseer
reads for its advisory. It is looked up by slug in project 39; this repo writes only `cholobots#N`
nodes, and refs in any other namespace are recorded as unknown and never looked up.

## Access route 1: MCP tools (`taiga-py`, workflow server)

Ref-based; never needs internal ids. The workflow server resolves status, severity, priority and
member names itself.

| Purpose | Tool |
|---|---|
| Read a story | `get_story(project=39, ref=N)` |
| Update a story | `update_story(project=39, ref=N, status="<name>", tags=[...], description=..., epic=<epic ref>)` |
| Status-only change | `set_story_status(project=39, ref=N, status="<name>")` / `set_task_status(...)` |
| Assign | `assign_item(project=39, ref=N, username="Jon Roberts", entity_type="story"\|"issue"\|"task")` |
| Read / update an issue | `get_issue(project=39, ref=N)` / `update_issue(project=39, ref=N, status=, severity=, priority=, tags=, description=, assignee=)` |
| File a bug | `create_issue(project=39, subject=, description=, issue_type="Bug", priority="<name>", severity="<name>", tags=[...])` |
| Tasks | `get_task(project=39, ref=N)`, `update_task(project=39, ref=N, status=, ...)`, `create_task(project=39, story_ref=N, subject=, description=, status=)`, `break_down_story(project=39, story_ref=N, tasks=[...])` |
| Epic and its stories | `get_epic_overview(project=39, ref=N)` |
| Link story to epic | `update_story(project=39, ref=N, epic=<epic ref>)` |
| Create a story | `create_story(project=39, subject=, description=, epic=, tags=, assignee=)` |
| Search | `search(project=39, query=)` |
| Backlog listing | `browse_backlog(project=39, status=, epic=, tags=, assignee=)` |
| Sprints | `get_sprint_board(project=39, sprint=)`, `move_to_sprint(project=39, story_refs=[...], sprint=)`, `plan_sprint` |
| Wiki | `get_wiki(project=39, slug=)`, `upsert_wiki(project=39, slug=, content=)` |
| Comment | `add_comment(project=39, ref=N, text=, entity_type="story"\|"issue"\|"task")` |

Mapping from the full-server tool names some older skill text uses:

| Full server | Workflow server |
|---|---|
| `get_user_story_by_ref` / `get_user_story` | `get_story` |
| `update_user_story` | `update_story` / `set_story_status` |
| `assign_user_story_to_user` / `assign_issue_to_user` | `assign_item(username="Jon Roberts")` |
| `get_*_statuses`, `get_issue_types`, `get_issue_severities`, `get_issue_priorities`, `get_filters_data`, `get_project_members` | not needed; use the names in this file |
| `get_issue_by_ref` / `get_issue(id)` | `get_issue(project=39, ref=N)` |
| `create_issue(..., type_id, status_id, severity_id, priority_id, kwargs)` | `create_issue(..., issue_type=, severity=, priority=, tags=)` |
| `get_task(id)` / `update_task(id, kwargs)` / `create_task(..., user_story)` | ref-based `get_task` / `update_task` / `create_task(story_ref=)` |
| `get_epic_by_ref` / `list_user_stories(epic=)` | `get_epic_overview` |
| `update_epic` (description) | `scripts/taiga-epic.sh patch` |
| `link_user_story_to_epic` | `update_story(epic=)` |
| `create_user_story` | `create_story` |
| `search(project_id, text)` | `search(project, query)` |
| `list_milestones` / `bulk_update_story_sprint` | `get_sprint_board` / `move_to_sprint` |
| `list_wiki_pages` / `get_wiki_page` / `update_wiki_page` | `get_wiki` / `upsert_wiki` |
| `list_issues` | `search`, or `scripts/taiga_select_issues.py` |

Where a state file keeps a `taiga_id` field for a later script call, keep the field; the REST
scripts below resolve refs to ids themselves (`scripts/taiga-story.sh` returns the `id`).

## Access route 2: REST scripts

They read `TAIGA_API_URL`, `TAIGA_USERNAME` and `TAIGA_PASSWORD` from the shell (the repo `.envrc`
exports them through direnv from the SOPS vault). Default project is 39.

| Script | Use |
|---|---|
| `scripts/taiga_project_setup.py --check\|--apply [--env-file F]` | Project setup and the orchestrator preflight (above) |
| `scripts/taiga-story.sh <ref> [39] [--issue\|--epic] [--with-tasks]` | O(1) fetch by ref; JSON with `id`, status, tags, `created_date`, `modified_date`, `version` (none of which the MCP `get_issue` returns), tasks. The second argument is always read as the project id, so write `39` whenever a flag follows (`<ref> 39 --with-tasks`) |
| `scripts/taiga-epic.sh get <ref>` / `patch <ref> --description-file <path> [--version <n>]` | Epic description edits (dependency diagram, `## Overseer hand-off`) |
| `scripts/taiga-comment.sh <id> "<text>" [--issue]` | Comment by internal id (take `id` from `taiga-story.sh`) |
| `scripts/taiga-wiki.sh get\|patch <slug\|page-id> ...` | Wiki page body via a file; a slug resolves in project 39 |
| `scripts/taiga_select_issues.py --purpose triage\|analyze\|fix\|sprint ...` | Bug selection (`.claude/skills/shared/bug-sweep-options.md`) |
| `scripts/taiga_audit.py` | Backlog consistency audit |
| `scripts/taiga-update-on-merge.sh` | Post-merge sync (also run by `.github/workflows/taiga-sync.yml`) |

Raw REST callers use `https://backlog.growthgarage.org/api/v1/...` with header
`x-disable-pagination: True`; see `.claude/skills/shared/taiga-pagination.md`.

## GitHub conventions

- **Branches and PRs live on `upstream`** (`MrsBohemian/Cholobots`, Heaven's repo; this account has
  push rights; `main` is unprotected). `origin` (the Alterna-Dev-Studio fork) is only a mirror;
  skills never need it.
- `upstream` must use SSH: `git@github.com:MrsBohemian/Cholobots.git`. Git over HTTPS picks up the
  shell's `GH_TOKEN` PAT, which cannot push there. Fix it once per clone (worktrees share the
  clone's remotes): `git remote set-url upstream git@github.com:MrsBohemian/Cholobots.git`.
- The merge labels `mark-done-on-merge` and `close-tasks-on-merge` do not exist on the repo yet;
  create them once with the `gh label create` step in `.claude/skills/shared/merge-side-effects.md`.
- The shell's `GH_TOKEN` cannot see the upstream repo. Every `gh` call is
  `env -u GH_TOKEN gh ... --repo MrsBohemian/Cholobots`; scripts use `GH_REPO="${GH_REPO:-MrsBohemian/Cholobots}"`.
- Base new branches on `upstream/main` after `git fetch upstream`. Local `main` tracks `upstream/main`.
- Push: `git push -u upstream <branch>`; after a rebase, `git push --force-with-lease upstream <branch>`.
- Create: `env -u GH_TOKEN gh pr create --repo MrsBohemian/Cholobots --base <base> --head <branch> ...`.
  No owner prefix on `--head` and no `--reviewer` (the token owner opens the PR).
- Check out a PR branch: `git fetch upstream <branch>`, then work on it in a worktree.
- Merge: `env -u GH_TOKEN gh pr merge <n> --repo MrsBohemian/Cholobots --squash` (`--merge` for stacked PRs in the merge train).
- Branch names: `feature/<ref>-<slug>`, `fix/<ref>-<slug>`, `epic/<epic>-integration-<story>`.
- Optional after a merge: `git push origin main` keeps the mirror in sync.
- No CI yet (story #35): `gh pr checks` reports no checks, which counts as a pass with a note.
