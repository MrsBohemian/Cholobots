---
name: merge-train
description: Sequentially rebase, CI-validate, and merge a queue of PRs — accepts explicit PR numbers or discovers them from an epic's dependency diagram
argument-hint: "<PR numbers... | epic:<ref>>"
allowed-tools:
  - Agent
  - Bash(python3 scripts/merge_train.py *)
  - Bash(python3 scripts/taiga_project_setup.py --check)
  - Bash(scripts/taiga-story.sh *)
  - Bash(scripts/monitor-pr-stability.sh *)
  - Bash(python -m py_compile *)
  - Bash(python -m pytest *)
  - Bash(git *)
  - Bash(env -u GH_TOKEN gh *)
  - mcp__taiga-py__get_story
  - mcp__taiga-py__update_story
  - mcp__taiga-py__set_story_status
  - mcp__taiga-py__assign_item
  - mcp__taiga-py__add_comment
  - Read(//tmp/**)
  - Edit(//tmp/**)
  - Read
  - Edit
  - Write
  - Grep
  - Glob
---

# Merge Train — $ARGUMENTS

This skill is a thin wrapper around `scripts/merge_train.py`. The Python script handles all deterministic operations (argument parsing, git operations, CI monitoring, Taiga checks, state management, merging). This wrapper only invokes Claude for agent-only steps: conflict resolution and build remediation.

All PRs and branches live on `MrsBohemian/Cholobots` (the `upstream` remote); every `gh` call is `env -u GH_TOKEN gh ... --repo MrsBohemian/Cholobots`.

## Step 0: Preflight

Run `python3 scripts/taiga_project_setup.py --check` once. Exit 2 means the Taiga project lacks the
statuses the train gates on: stop and print
`Run: (eval "$(dv export project/taiga-growthgarage-jonroberts)" && python3 scripts/taiga_project_setup.py --apply)` for Jon.

## Step 1: Invoke the script

Run the merge-train script with the provided arguments:

```bash
python3 scripts/merge_train.py $ARGUMENTS
```

Capture both the exit code and stdout/stderr.

## Step 2: Handle the result

Check the exit code from Step 1:

### Exit 0 — Complete

The script processed all PRs successfully (or `--dry-run` completed). Print the summary output from stdout and stop. Until story #35 adds GitHub Actions, the summary notes "no checks configured" for each PR; the script treats that as stable, not as a failure or a reason to wait.

### Exit 10 — Conflicts need resolution

The script hit rebase conflicts and needs agent intervention.

1. Locate the action-request JSON file under `/tmp/merge-train-*-action.json`:
   - Enumerate all matching files.
   - For each candidate, parse the JSON and read its `state_file` field.
   - Select the file whose `state_file` matches the state file path you are resuming from. If multiple match, use the most recently modified among those matches.
2. The JSON contains:
   - `pr.number`, `pr.branch`, `pr.title` — the PR with conflicts
   - `details.files` — list of conflicting files
   - `details.pre_rebase_ref` — the commit ref before rebase was attempted
   - `details.target` — the target branch
   - `state_file` — path to the state file for resume (this field is the canonical way to associate an action-request file with a given state)
3. Delegate conflict resolution to a `senior-software-developer` agent:
   - `subagent_type: "senior-software-developer"`
   - Prompt: `"Rebase PR #<number> onto <target>. Follow the rebase-branch skill instructions with arguments '<number> <target>'."`
4. When agent returns, check its output:
   - If the rebase was **escalated** (conflict too complex): abandon the PR per [Abandoning a PR by hand](#abandoning-a-pr-by-hand) with a descriptive error, then jump to **Step 3** to resume.
   - If the rebase **succeeded**: jump to **Step 3** to resume.

### Exit 11 — Remediation needed

The script detected a build failure or unresolved review comments and needs agent intervention.

1. Locate the action-request JSON file under `/tmp/merge-train-*-action.json`:
   - Enumerate all matching files.
   - For each candidate, parse the JSON and read its `state_file` field.
   - Select the file whose `state_file` matches the state file path you are resuming from. If multiple match, use the most recently modified among those matches.
2. The JSON contains:
   - `action` — either `remediate_build` or `resolve_comments`
   - `pr.number`, `pr.branch`, `pr.title` — the PR needing remediation
   - `details.failure_type` — `ci_pre_rebase`, `build_post_rebase`, `ci_post_rebase`, `comments_pre_rebase`, `comments_post_rebase`, or `auto_merge_timeout`
   - `details.failing_checks` — list of failing CI check names (for build actions)
   - `details.comment_count` — number of unresolved comment threads (for comment actions)
   - `details.pre_rebase_ref` (for post-rebase failures) — commit ref for rollback
   - `details.build_log` (for `build_post_rebase`) — path to the full output of the post-rebase local build: `python -m py_compile cholobots.py config.py db/database.py bots/*.py`, plus `python -m pytest -q` when `tests/` exists
   - `details.build_output_tail` (for `build_post_rebase`) — last 100 lines of that output
   - `state_file` — path to the state file for resume (this field is the canonical way to associate an action-request file with a given state)
3. **If `action` is `remediate_build`:**
   - Track remediation attempts per PR. Increment `buildFailureCount` for this PR. **If `buildFailureCount >= 5`:** abandon the PR per [Abandoning a PR by hand](#abandoning-a-pr-by-hand) with the error "5+ build failures — escalating", post a PR comment (`env -u GH_TOKEN gh pr comment <number> --repo MrsBohemian/Cholobots --body "Skipping — 5+ build failures. Manual intervention needed. cc @bigjonroberts"`), and jump to **Step 3** to resume with the next PR.
   - If `details.failure_type` is `build_post_rebase`, read `details.build_output_tail` before dispatching. If it shows an environment failure rather than a code failure (e.g. `No module named pytest`, a `ModuleNotFoundError` for a package in `requirements.txt`, a missing `.venv`), stop and report to Jon rather than dispatching — a PR change cannot fix it.
   - Delegate to a `remediation-developer` agent:
     - `subagent_type: "remediation-developer"`
     - Prompt: `"Fix the build failure on PR #<number>. Follow the fix-build-failure skill instructions with argument 'pr-<number>'."` For `build_post_rebase`, append: `"The local post-rebase build output is at <details.build_log>."`
4. **If `action` is `resolve_comments`:** (Codex review threads are authored by the token owner, so they do trigger this action; the exclusion list for advisory reviewers is still controlled by the `MONITOR_ADVISORY_REVIEWERS` env var on `monitor-pr-stability.sh`)
   - Delegate to a `remediation-developer` agent:
     - `subagent_type: "remediation-developer"`
     - Prompt: `"Resolve review comments on PR #<number>. Follow the pr-comments-resolve skill instructions with argument '<number>'."`
5. When agent returns, jump to **Step 3** to resume. The script will re-run CI monitoring and exit again if the fix didn't resolve all issues.

### Exit 1 — Unexpected error

The script hit an unexpected error. Read stderr for details. If transient (network, rate limit), retry once. If persistent, report the error and stop.

## Abandoning a PR by hand

Both escalation paths above give up on a PR by editing the state file rather than letting the
script finish with it. The script's own restore-on-failure only runs inside `process_pr`, so
these paths owe the same cleanup by hand. For the PR being abandoned, in this order:

1. Read its `sweptChildren` from the state file. For each child listed, run
   `env -u GH_TOKEN gh pr edit <child> --repo MrsBohemian/Cholobots --base <parent-branch>` to put
   it back on the abandoned PR's head branch. A child left on the target still carries the
   abandoned PR's unlanded commits and would merge them into the target on its own when the queue
   reaches it.
2. If any of those `gh pr edit` calls fails, do **not** advance. Set `halted` in the state
   file to `{"at": "<UTC timestamp>", "reason": "<what could not be restored>"}`, report to
   Jon, and stop — `--resume` will refuse to run until that record is removed.
3. Mark the PR `failed` with the error, **then** set its `sweptChildren` to `[]`, then
   advance `currentIndex`. Status before the clear: a crash in between must leave a record
   that the PR is done for, not a cleared list with no terminal status.

## Step 3: Resume the script

After agent intervention (conflict resolution or remediation), re-invoke the script with `--resume`:

```bash
python3 scripts/merge_train.py --resume <state_file_path>
```

Go back to **Step 2** to handle the new exit code.

## Notes

- The script writes state after every PR transition, so `--resume` always picks up where it left off.
- `monitor-pr-stability.sh` is called by the script for CI polling — the wrapper does not call it directly.
- `taiga-story.sh` is called by the script for Taiga status checks — the wrapper does not call it directly.
- After the train finishes, the fork mirror can be synced with `git push origin main` (optional; no skill needs `origin`).
- The merge train does not update Taiga after merging. The `taiga-sync` GitHub Actions workflow moves an item to Done/Closed only when the PR or item opts in with `mark-done-on-merge` (see [`../shared/merge-side-effects.md`](../shared/merge-side-effects.md)), and it needs `TAIGA_*` secrets on `MrsBohemian/Cholobots` that do not exist yet. The train's own gate is unchanged: each item must already be Done/Closed before the train merges it, so a partial-story PR that Jon has not marked Done is merged by hand.
- A **standalone** PR — one whose base was never another branch during the run, and which has no open PR based on its own head branch — is merged with `--squash --auto` — one-off fixes keep main's history to one commit per PR.
- A **stacked** PR — one whose base was another branch at any point during the run — is merged with `--merge --auto`. Squashing a parent replaces its commits with one new commit the child does not carry, so the child's diff after retarget would contain the parent's changes again. The choice is made from the PR's recorded stack membership, not its base at arming time: by the time a child arms, it has already been pointed at the target.
- A PR with at least one open PR based on its own head branch is also merged with `--merge --auto`, regardless of its own base — a stack root arms `--merge` on this route even though its own `stacked` flag is `False`. Squashing it would replace its commits with one new commit its children do not carry, so every descendant's diff would show its changes again, and the train's own pre-merge rebase of those descendants would replay commits whose squashed equivalent is already on the target and conflict. The children are read from GitHub (`find_open_children(branch)`), so a stacked PR this train never enqueued still counts, and a child already swept onto the target earlier in the same run still counts on a `--resume`, via the PR's recorded `sweptChildren`.
- **No CI means most PRs are already CLEAN.** With no checks configured (#35), expect the immediate-merge outcome described below rather than a pending auto-merge. If the arming call fails because "Allow auto-merge" is off on `MrsBohemian/Cholobots`, report it to Jon rather than working around it.
- **Retargeting is automatic.** When a queued PR's base has landed — whoever merged it: this train, an earlier one, or a human — the script retargets it onto the target before polling its CI. It fails closed on anything short of proof that the base landed: the base branch has to have had exactly one pull request, that PR has to be `MERGED`, and it has to have merged into the target rather than into some branch of its own. The check does not depend on whether the base branch still exists on the remote — a merge that kept its branch still has to retarget the child. A branch deleted after its PR was *closed* is not retargeted past, since that would land the abandoned parent's commits inside the child's diff. A PR whose base is still open is skipped deliberately, before its CI is polled or its branch is rebased — retargeting it early jumps the queue, and rebasing it onto the target would replay the unlanded parent's commits into its own diff. Re-queue it once the parent lands. **Do not retarget a PR by hand while a train is running**; the script owns PR bases for the duration of the run.
- **Children are swept before arming.** If the repo's `delete_branch_on_merge` setting is on, this PR's head branch goes away on merge, so the script points every open PR based on that branch at the target first. It is **who deletes the branch** that decides whether children survive: GitHub's own auto-delete retargets open children onto the merged PR's base, but any client-side deletion closes them even when the parent merged. The sweep earns its place because `gh pr merge --auto` can silently fail to engage on an already-CLEAN PR, exiting 0 with no output and no `auto_merge_enabled` event, and merge client-side instead, which closes the children. A child it cannot retarget — or cannot even list — fails the parent rather than closing the child. If the parent then fails to arm, or is closed unmerged, the script puts the children back on the parent's branch, because a child carries its parent's commits and must not merge them into the target on its own. A **pending** auto-merge is not a failure and does not trigger a restore, and neither is an arming call that returns non-zero on a PR GitHub reports as already armed — a `--resume` after an auto-merge timeout re-issues that call. If a child cannot be put back and was never in this train's queue, there is nowhere to record the danger, so the script marks the state file `halted` and stops; `--resume` refuses to run until that record is removed by hand.
- **A zero exit from the arming call is not proof that auto-merge was armed.** The script asks GitHub (`gh pr view --json autoMergeRequest`) after every successful arming call. Auto-merge recorded means the merge is pending and the script waits on it. No auto-merge recorded means it reads the PR's live state: `MERGED` is the immediate-merge outcome and is recorded as a merge without waiting; still `OPEN` means arming did not take, so the swept children go back onto the PR's branch and the PR is failed rather than left to time out as a pending auto-merge. An unreadable answer falls through to the merge poll, which settles it either way.
- A story with multiple dependency parents may have both an `epic/<epic>-integration-<ref>` PR (merging every parent) and its own feature PR — both belong in the queue. The integration PR is queued after every one of that story's parents and before the story's own PR. Getting this backwards breaks the retarget: closing a contained integration PR records its branch in `mergedBranches`, which is what lets the child retarget off that closed-but-undeleted branch onto the target when the queue reaches it — process the child first and it is skipped with "base has not landed" instead. The child's base *is* the integration branch, so to the rest of this section it is an ordinary stacked PR: it arms `--merge` and is retargeted by the existing machinery above.
- After each merge, the script updates the local target branch from `upstream` so the next rebase is against the true latest.
- **The script performs epic discovery itself** — `discover_epic_prs()` reads the epic via `scripts/taiga-story.sh <ref> 39 --epic`, so the wrapper passes `epic:<ref>` straight through and does no Taiga lookup of its own.
- **Epic mode queues the stories named in the epic's `## Dependency Diagram`, not every Done story in the epic.** A story the diagram omits will not have its PR queued, and no Taiga story listing happens at all. There is also no Done filter in discovery: `check_taiga_status()` already gates every queued PR on its story's status, and filtering twice would hide skips from the summary.
- **Epic mode exits `EXIT_ERROR` (1) when the epic cannot be read or has no parseable dependency diagram** — it refuses to merge in an arbitrary order rather than degrade to ref order or queue every open PR in the repo. The remedy is to add a `## Dependency Diagram` section to the epic (`scripts/taiga-epic.sh patch`), or re-run with explicit PR numbers.
- **An integration PR has no Taiga story of its own**, but `discover_epic_prs()` still finds it: its branch `epic/<epic-ref>-integration-<story-ref>` carries the story ref delimiter-bounded, so it matches that ref and is queued alongside the story's own PR. To check one by hand, compute its integration branch name with `integration_branch_name(<epic ref>, <story ref>)` (`scripts/lib/integration_branch.py`'s naming convention: `epic/<epic-ref>-integration-<story-ref>`). Use the epic **ref** from `epic:<ref>` here — not the epic's internal Taiga id; a branch name built from the internal id matches no PR. Look it up directly: `env -u GH_TOKEN gh pr list --repo MrsBohemian/Cholobots --head epic/<epic-ref>-integration-<story-ref> --state all --json number,state`. If it does not exist, the story never reached the integration-branch step (still queued upstream, or blocked); the story's own PR then has no base to merge into, so leave both for a later run.
- **Verify every parent actually landed before enqueuing the integration PR.** `merge_train.py` bypasses the Taiga status gate for integration PRs (they have no Taiga item of their own), and the integration PR's own base is `main`, not a parent's branch, so the script's own base-not-landed guard (which only compares a PR's recorded base against the target) cannot catch a parent that never merged either. Epic discovery only orders the queue by the diagram, but `process_pr` now also refuses an integration PR whose parents have not all landed: it reads the story's declared parents from the epic's dependency diagram and requires each parent story to have a `MERGED` PR and none open, skipping the integration PR (fail closed) otherwise or when that cannot be verified. Check by hand here as well, so the wrapper can report the reason before enqueuing. For each of the story's declared parents, confirm its own PR state is `MERGED` (`env -u GH_TOKEN gh pr view <parent-pr> --repo MrsBohemian/Cholobots --json state`) before letting the integration PR through. If any parent is still open, was closed unmerged, or was skipped, do not enqueue the integration PR or the child — leave both for a later run once the parent lands, the same as the "not found" case above.
