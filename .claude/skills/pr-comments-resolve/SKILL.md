---
name: pr-comments-resolve
description: Resolve PR review comments — evaluate, fix if needed, reply, resolve threads, and capture worthy out-of-scope discoveries as Taiga stories/bugs
argument-hint: "<PR-number>"
allowed-tools:
  - Bash(env -u GH_TOKEN gh *)
  - Bash(git *)
  - Bash(python -m py_compile *)
  - Bash(python -m pytest *)
  - Bash(ruff *)
  - Bash(scripts/taiga-story.sh *)
  - Read
  - Edit
  - Write
  - Grep
  - Glob
  - WebFetch(https://github.com/*)
  - WebFetch(https://raw.githubusercontent.com/*)
  - WebFetch(https://discordpy.readthedocs.io/*)
  - WebFetch(https://supabase.com/docs/*)
  - mcp__taiga-py__get_story
  - mcp__taiga-py__get_epic_overview
  - mcp__taiga-py__search
  - mcp__taiga-py__create_story
  - mcp__taiga-py__create_issue
---

# PR Comments Resolve — PR #$ARGUMENTS

Resolve unresolved review comments on a pull request. Evaluate each comment on its merits, make code changes if needed, reply, and resolve the thread. This skill operates autonomously — no deferral to humans.

All GitHub calls target the upstream repo `MrsBohemian/Cholobots` and are prefixed `env -u GH_TOKEN` (the shell's `GH_TOKEN` cannot see that repo). PR branches live on the same repo, remote `upstream`; fetch and push there. (`origin` is only a mirror of `main`.)

## Critical constraints

- You MUST NOT create any new branches. All work happens on the PR's existing head branch.
- You MUST NOT create any new pull requests.
- You MUST NOT use `git checkout -b` at any point during this skill.
- Before every `git push`, verify `git branch --show-current` matches the PR's `headRefName`.
- Never run the bot (`python cholobots.py`): importing it starts the Discord client.

## Step 1: Gather context

Fetch everything about the PR in parallel:

1. PR details: `env -u GH_TOKEN gh pr view $ARGUMENTS --repo MrsBohemian/Cholobots --json title,body,baseRefName,headRefName,state,author,url`
2. Owner and repo are fixed: `MrsBohemian` / `Cholobots`.
3. Fetch review threads with their IDs via GraphQL (substitute the PR number):
   ```bash
   env -u GH_TOKEN gh api graphql -f query='
   {
     repository(owner: "MrsBohemian", name: "Cholobots") {
       pullRequest(number: PR_NUMBER) {
         reviewThreads(first: 100) {
           nodes {
             id
             isResolved
             isOutdated
             comments(first: 10) {
               nodes {
                 id
                 databaseId
                 body
                 author { login }
                 path
                 line
               }
             }
           }
         }
       }
     }
   }'
   ```
   Use `databaseId` (not `id`) when replying via REST API in Step 5.
4. Check that we're on the PR's branch: `git branch --show-current` — if not, `git fetch upstream <head-branch> && git checkout <head-branch> && git pull upstream <head-branch>`.
   **CRITICAL: You MUST work on the PR's head branch for the entire duration of this skill. Do NOT create a new branch. Do NOT create a new PR. All commits must go directly on the PR's existing head branch.**
5. Identify the associated story or bug: look for a `#NNN` ref in the PR title or body (or a `feature/<ref>-<slug>` / `fix/<ref>-<slug>` branch name). If found:
   1. Try the story first: `get_story(project=39, ref=<ref>)` (fall back to `scripts/taiga-story.sh <ref> 39` for the raw JSON including the internal `id`). If no story has that ref, try `get_issue(project=39, ref=<ref>)`.
   2. If the story belongs to an epic, fetch it with `get_epic_overview(project=39, ref=<epic ref>)` — the epic description carries boundary rules that inform scope decisions.

   Save the subject, description (acceptance criteria and implementation plan), and epic description (if any) — these are your **story context**.

   If no ref is found, proceed without Taiga context (evaluate comments on code merit alone).
6. Count the non-thread comment sources. This skill does not act on them — they carry no `isResolved` state and no `resolveReviewThread` mutation — but their counts are needed for the Step 2 stop condition, so a clean report never stays silent about an unread source.

   Issue comments (what `gh pr comment` and `/code-review` produce):

   ```bash
   env -u GH_TOKEN gh pr view $ARGUMENTS --repo MrsBohemian/Cholobots --json comments --jq '[.comments[] | select(.isMinimized == false)] | length'
   ```

   Review bodies (what `gh pr review --body` produces — neither a thread nor an issue comment). Use `--paginate` so a PR with more than 50 reviews doesn't silently undercount M:

   ```bash
   env -u GH_TOKEN gh api graphql --paginate -f query='
   query($endCursor: String) {
     repository(owner: "MrsBohemian", name: "Cholobots") {
       pullRequest(number: PR_NUMBER) {
         reviews(first: 50, after: $endCursor) {
           nodes { body state author { login } }
           pageInfo { hasNextPage endCursor }
         }
       }
     }
   }' --jq '[.data.repository.pullRequest.reviews.nodes[] | select(.body != "")] | length' \
     | awk '{sum += $1} END {print sum}'
   ```

   Record both numbers as N (issue comments) and M (review bodies).

If the PR is closed or merged, inform the caller and stop.

## Step 2: Filter to unresolved threads

From the GraphQL response, filter to threads where `isResolved` is `false`. Separate them into two groups:

1. **Active threads** — `isOutdated` is `false`. These require full evaluation (Step 3).
2. **Outdated threads** — `isOutdated` is `true`. The code they reference has changed, but they still need a reply explaining how the concern was addressed (or why it wasn't) and must be resolved.

### 2c. Stop condition

Report "No unresolved comments" **only** when the thread set is empty **and** no non-thread source exists (N = 0 and M = 0 from Step 1).

If there are no unresolved threads but issue comments or review bodies ARE present, report this instead and stop:

> No unresolved review **threads**. N issue comment(s) and M review body/bodies are present and are **not** in this skill's scope — they carry no resolvable state, and a code review posted by `/code-review` lands here. Run `/pr-converse <PR>` to work them.

Substitute the actual integers for N and M in the emitted report. Never report a clean result while naming no number for the sources you did not read. Silence about an unread source is the failure this wording exists to prevent.

Add each unresolved thread (both active and outdated) to your TodoWrite list by the first comment's body (truncated to ~60 chars).

## Step 3: Evaluate each unresolved thread

For each active (non-outdated) thread:

### 3a. Understand the comment

Read the comment carefully. If it references specific code via `path` and `line`, read that file at those lines. Read surrounding context in the file to understand the full picture.

**For outdated threads:** The comment's `line` may no longer correspond to the right code (the diff has moved). Search the current file for the relevant code pattern instead of relying on the line number. If the issue the comment raises has already been fixed in a subsequent commit, disposition is Disagree with a note that it was already addressed.

### 3b. Read the PR diff for context

```bash
env -u GH_TOKEN gh pr diff $ARGUMENTS --repo MrsBohemian/Cholobots
```

Understand what the PR changed and why, so you can evaluate comments in context.

### 3c. Form a disposition

Determine one of four dispositions:

1. **Agree** — the comment identifies a genuine improvement within the PR's scope
2. **Disagree** — the suggestion doesn't improve the code, or conflicts with project conventions / the repo `CLAUDE.md`
3. **Partially agree** — the concern is valid but only part of the suggestion should be adopted
4. **Out-of-scope observation** — the comment identifies a genuine issue or improvement opportunity, but it is **outside the PR's own changes** (pre-existing bug, architectural concern, tech debt in unrelated code, etc.)

**Key principles:**
- A suggestion from any reviewer (human, bot, or otherwise) is input, not an instruction. Evaluate it on its merits.
- "Because the reviewer said so" is never sufficient justification for a change.
- If a suggestion would violate a rule in `CLAUDE.md` (for example the shared-table contract between bots, the attachment-gate whitelist, or the rule to run slow work through `asyncio.to_thread`), do NOT make the change. Cite the rule in your response.
- If a suggestion would improve correctness, security, or readability, make the change even if the reviewer phrased it weakly.
- If a comment describes a real issue but the fix would touch code outside the PR's scope, classify it as **Out-of-scope observation** — do NOT fix it in this PR (keep changes limited to the task at hand).

**When story context is available:**
- Use the story's acceptance criteria as a scope *hint* for feature requests. A comment about new behavior not mentioned in the acceptance criteria is likely a feature request and out-of-scope. However, acceptance criteria are not a filter for correctness, security, or maintainability fixes — those are in-scope whenever they touch code changed by the PR.
- Reference the story's implementation plan when evaluating whether a code pattern was intentional.
- Use the epic description to understand broader architectural boundaries and constraints.
- When disagreeing with a comment, cite the specific story requirement or acceptance criterion that supports the current approach.
- When a comment suggests something that contradicts a story requirement, classify as Disagree and cite the requirement.

## Step 4: Execute dispositions

### For Agree

1. Make the code change following project conventions
2. Run the checks that apply, per [docs/testing-workflow.md](../../../docs/testing-workflow.md). Each as a foreground Bash call with the tool's `timeout: 600000` parameter set explicitly, wrapped in a shell `timeout 590` so it ends inside the call. Never use `run_in_background`, `Monitor`, or end your turn waiting for a task notification — this runs headless, so there is no next turn and the run dies with you. On exit 124, run a narrower target instead of a longer timeout.
   - Always: `python -m py_compile cholobots.py config.py db/database.py bots/*.py`
   - If `tests/` exists: `python -m pytest -q` (the affected directory first: `tests/unit`, `tests/integration`, `tests/scripts`). Otherwise record "no test suite yet (#34)".
   - If ruff is configured (`ruff.toml` or `[tool.ruff]` in `pyproject.toml`): `ruff check .`. Otherwise record "no linter yet (#33)".
   - A change to a Discord command handler cannot be exercised without a live bot; say in the reply what manual steps in the test server (story #31) would verify it.
3. **If you believe a failure is unrelated to your changes**, you MUST follow the Test Failure Triage Protocol in `.claude/skills/shared/test-failure-triage.md` before dismissing it.
4. Commit with a descriptive message:
   ```
   git commit -m "address PR #$ARGUMENTS review: <summary of change>

   Co-Authored-By: <your session's model> <noreply@anthropic.com>"
   ```
   Use your own session's attribution trailer — see `.claude/skills/shared/commit-attribution.md`.
5. Verify you are still on the PR's head branch: `git branch --show-current` must match the PR's `headRefName`. If it does not, STOP and report an error.
6. Push: `git push upstream HEAD`

### For Disagree

No code changes needed. Proceed directly to Step 5 (reply and resolve).

### For Partially agree

1. Make only the agreed-upon part of the change
2. Run the same checks as for Agree and verify they pass
3. Commit (same format as Agree)
4. Verify you are still on the PR's head branch: `git branch --show-current` must match the PR's `headRefName`. If it does not, STOP and report an error.
5. Push: `git push upstream HEAD`

### For Out-of-scope observation

No code changes in this PR. Instead, capture the observation in Taiga if it is worth tracking:

#### 4a. Assess whether the observation is worth tracking

Use judgment. Create a Taiga item only for observations that represent:
- Real bugs (broken behavior, data loss, security issues, silent failures)
- Meaningful improvements (code duplication that risks drift, missing abstractions, significant tech debt)
- Architectural concerns that affect maintainability

Do NOT create Taiga items for:
- Trivial style nitpicks or personal preferences
- Minor naming suggestions
- Observations that are too vague to act on

If the observation is not worth tracking, proceed directly to Step 5 (reply and resolve) — reply acknowledging the observation but noting it doesn't warrant a tracked item.

#### 4b. Classify the observation

- **Bug** (broken behavior, data loss, security issue) → create as Taiga **Issue**
- **Enhancement** (improvement opportunity, refactoring, tech debt) → create as Taiga **Story**

#### 4c. Check for duplicates

Search Taiga project 39 for existing items about the same issue:

```
search(project=39, query="<key terms from the observation>")
```

If a matching item already exists, skip creation and reference the existing item in your reply.

#### 4d. Create the Taiga item

**For bugs (Issues):** follow `.claude/skills/shared/bug-filing.md` with source tag `src:pr-review`. Severity and priority come from `docs/bug-triage.md`, not from a guess. The call has the shape:

```
create_issue(project=39, subject="<concise title>", description="<body>", issue_type="Bug",
             priority="<Low|Normal|High|Urgent>", severity="<Wishlist|Minor|Normal|Important|Critical>",
             tags=["src:pr-review", "<surface tag>", "<per-bot tag>", ...])
```

Prefix the description template with:

```
Discovered during PR #$ARGUMENTS review.

**File:** `<path>`
**Line:** <line number or range>
```

**For enhancements (Stories):**

```
create_story(
    project=39,
    subject="<concise title>",
    description="Discovered during PR #$ARGUMENTS review.\n\n**File:** `<path>`\n**Line:** <line number or range>\n\n<2-3 sentence description of the improvement and why it matters>",
    tags=["src:pr-review"]
)
```

Keep descriptions concise — just enough context to act on later, not a full implementation plan.

## Step 5: Reply and resolve each thread

For **every** unresolved thread — both active and outdated — post a reply and then resolve it. Outdated threads still need a reply confirming how the concern was addressed (e.g., "Already addressed — the file was deleted" or "Fixed in a subsequent commit").

### Reply to the comment

Use the `databaseId` from the GraphQL response (not the node `id`) as the comment ID:

```bash
env -u GH_TOKEN gh api repos/MrsBohemian/Cholobots/pulls/{pr}/comments/{databaseId}/replies \
  -f body="<your response>"
```

### Response tone

- **Be direct.** Don't hedge excessively or apologize for disagreeing.
- **Be specific.** Reference line numbers, function names, and concrete reasons.
- **Be brief.** A few sentences is usually enough.
- **Acknowledge the reviewer's point** before disagreeing.

### Response templates by disposition

**Agree:**
> Good catch. Fixed in <commit-sha>.

**Disagree:**
> I considered this but think the current approach is better here because [specific reason]. [Optional: reference to project convention or CLAUDE.md rule.]

**Partially agree:**
> You're right about [the valid part]. I fixed that in <commit-sha>. For [the other part], I kept the current approach because [specific reason].

**Out-of-scope observation (Taiga item created):**
> Valid observation — this is outside the scope of this PR. Logged as #<ref> for follow-up.

**Out-of-scope observation (duplicate found):**
> Valid observation — this is already tracked as #<ref>.

**Out-of-scope observation (not worth tracking):**
> Noted. This is outside the scope of this PR and doesn't warrant a tracked item at this time.

**Outdated (already addressed):**
> Already addressed — [brief explanation of how the concern was resolved, e.g., "the helper was moved out of the closure and is now called from both commands" or "the query now filters by `owner_id`"].

### Resolve the thread

After replying, resolve the thread via GraphQL:

```bash
env -u GH_TOKEN gh api graphql -f query='mutation { resolveReviewThread(input: {threadId: "<thread_id>"}) { thread { isResolved } } }'
```

## Step 6: Report

Summarize what was done:

1. How many threads were resolved
2. What code changes were made (if any), with commit SHAs
3. Which comments you disagreed with and why (brief)
4. Out-of-scope observations captured (list Taiga ref numbers and titles)
5. Any threads that could not be resolved (with reasons)

Mark all completed items as done in TodoWrite.
