# Merge Side Effects

`.github/workflows/taiga-sync.yml` runs `scripts/taiga-update-on-merge.sh` for every Taiga ref a merged PR claims. Its two status side effects are **opt-in**. This file is the single source for what enables them and which skill sets which signal. Skills link here instead of restating the rules.

**The workflow is not live yet.** It needs repository secrets `TAIGA_API_URL`, `TAIGA_USERNAME` and `TAIGA_PASSWORD` on `MrsBohemian/Cholobots`, which do not exist yet. Until they do, whoever merges (the merge train, the overseer, or Jon) runs the same script by hand after the merge: `scripts/taiga-update-on-merge.sh` (`--dry-run` first; `--help` shows the arguments). Skills still set the signals below exactly as if the workflow ran.

## Semantics

| Signal | Effect on merge | Applies to |
|---|---|---|
| `close-tasks-on-merge` | Closes the story's open tasks (task `Closed`, 62) | Stories only; ignored for issues |
| `mark-done-on-merge` | Story → `Done` (80), issue → `Closed` (74) | Stories and issues |

- Each effect is enabled by **either** a GitHub label on the merged PR **or** a Taiga tag of the same name on the claimed story/issue. Either one is enough; both together act once.
- Tags are read live from Taiga at merge time, so a tag added or removed before the merge counts. Labels are taken from the PR as it was at merge.
- With neither signal, merge changes no status and closes no tasks. It only posts a "Merged in PR #N" comment on the item.
- The two effects are independent. `close-tasks-on-merge` still closes tasks on a story that is already Done.
- A signal changed after the merge does nothing. Every decision below is made before the PR merges.

## The deciding rule

**Post-merge validation** is anything that can only happen after the merge: a check in the test Discord server or production server against the deployed bot, a dashboard check against live Supabase data, a human acceptance step by Heaven or Jon, or evidence that can only be gathered post-merge. Until a test Discord server exists (story #31), any change to a command handler that has no automated test needs a live Discord check, which is post-merge validation.

For a story:

- `close-tasks-on-merge` applies iff merging the PR completes every task still open on the story. A task that must stay open (gated on a live step, a human, or another epic) says so under a `## Gate` heading in its description, and its presence means the signal does not apply. `scripts/taiga-story.sh <ref> 39 --with-tasks` omits task descriptions, so check each open task for `## Gate` with `get_task(project=39, ref=<task ref>)`.
- `mark-done-on-merge` applies iff `close-tasks-on-merge` applies **and** no post-merge validation remains.
- Tasks complete but story-level post-merge validation pending: `close-tasks-on-merge` only.
- Record the post-merge validation, if any, in the plan's `### Merge Side Effects` section so later skills can read it.
- A tag fires on **every** PR that claims the story. When a story ships through more than one PR, analysis sets neither tag; the PR that finishes the story carries the labels instead.

For a bug, the analyze-bug plan records `**Closes on merge:** eligible | not eligible: <reason>`. A bug is **not eligible** when post-merge validation remains. A missing line counts as not eligible. Bug auto-close is reserved for unattended sweeps (see below), so no bug skill sets a Taiga tag.

## Who sets what

| Skill | Signal it may set | When |
|---|---|---|
| `analyze-story` | Tags `close-tasks-on-merge` and `mark-done-on-merge` | Per the deciding rule, single-PR stories only |
| `analyze-bug` | None | Records `Closes on merge` eligibility for the fix skills |
| `finalize-story-pr`, `implement-story` | PR labels, and removes stale tags | At `gh pr create`, re-applying the deciding rule to the story as implemented |
| `implement-task` | Removes both tags | When it reports a task `GATED:` (a gated task means neither signal holds) |
| `fully-implement-story` | None | Treats a `GATED:` task report as finished, then finalizes |
| `fix-single-bug` | Label `mark-done-on-merge` | Only with its `--mark-done-on-merge` argument **and** an eligible plan |
| `fix-and-remediate-bug` | Label `mark-done-on-merge` | Eligible plan (it runs only under the unattended `parallel-fix-bugs`) |
| `fix-story-bug` | None | Its sub-PR targets a feature branch; the bug is verified via `Ready for test` |
| `fully-fix-bugs` | None itself | Invokes `fix-single-bug` with `--mark-done-on-merge` |
| `overseer` | None itself | Its story workers apply the deciding rule; its bug spawns pass no flag, so overseer bug PRs never carry `mark-done-on-merge` |
| `merge-train` | None | Its own gate (Done/Closed before merge) is unchanged |

"Only Jon marks stories Done" yields to this explicit opt-in: a skill still never sets Done itself, but a `mark-done-on-merge` signal set by the deciding rule lets the sync do it at merge.

When implementation finds post-merge validation the plan did not foresee, drop `mark-done-on-merge` (no label, and remove the tag). When it leaves a task open, drop both.

## Commands

- Labels must exist on the upstream repo first, and neither exists yet. Create each once: `env -u GH_TOKEN gh label create <name> --repo MrsBohemian/Cholobots`.
- Add labels at creation: `env -u GH_TOKEN gh pr create --repo MrsBohemian/Cholobots ... --label close-tasks-on-merge --label mark-done-on-merge` (one `--label` per signal that applies).
- Change them on an open PR: `env -u GH_TOKEN gh pr edit <n> --repo MrsBohemian/Cholobots --add-label <name>` / `--remove-label <name>`.
- Remove a tag: re-fetch the story (`get_story(project=39, ref=<ref>)`), then `update_story(project=39, ref=<ref>, tags=<current tags without the name>)`. Taiga replaces the whole list, so always send every tag you are keeping.
- Add a tag: the same call with the name appended to the current tags.
