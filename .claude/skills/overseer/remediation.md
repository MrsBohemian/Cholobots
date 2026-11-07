# Overseer Remediation Loop

`SKILL.md` **Step 4** — driving `scripts/remediate_pr.py` by its exit-code contract until the PR
is `STABLE`. Extracted so `SKILL.md` stays inside the post-compaction re-attachment window (see
its **Retention budget** note). `Read` this before entering `phase: "remediating"`.

`remediate_pr.py` is the deterministic PR stability loop: a deduplicated build-failure counter,
`monitor-pr-stability.sh`'s CI-wait semantics, and a Codex review pass built in (2 of the 3
expected reviews come from the script itself), with **no Taiga writes of its own**. Until story #35
adds GitHub Actions, a PR has no checks; the script treats "no checks configured" as stable with a
note, never as a failure or an endless wait. Driving its exit codes here — rather than
hand-rolling a `gh pr checks` loop, or delegating the whole loop to the `remediate-pr` **skill** —
keeps the human gates and the escalation decision at the conductor level while still reusing the
pipeline. The `remediate-pr` skill wrapper is still never delegated from here, because that
wrapper owns its own escalation writes (it moves the item to Blocked, tags `escalation-needed` and
assigns Jon itself) — routing Step 4 through it would put two owners on the same item's
escalation.

## Step 4: Remediate — drive scripts/remediate_pr.py until STABLE

Only if a PR exists. Record `remediateStateFile = /tmp/remediate-pr-<prNumber>.json` in the state
file the first time you call the script for this PR.

First call:

```bash
python3 scripts/remediate_pr.py <prNumber> --max-failures 5
```

Every later call for the same PR:

```bash
python3 scripts/remediate_pr.py <prNumber> --resume /tmp/remediate-pr-<prNumber>.json
```

**Never** pass `--taiga-id` / `--taiga-type` — Taiga escalation is this conductor's job, not the
script's.

**Exit codes:**

- **Exit 0 — STABLE** → advance to **Gate B**. If the script noted that no CI checks are
  configured, carry that note to Gate B.
- **Exit 10 — build fix needed** → read `/tmp/remediate-pr-<prNumber>-action.json` for the failing
  check details, then spawn a fresh `remediation-developer` (sonnet): `"Follow the fix-build-failure skill with arguments 'pr-<prNumber>'. End your final message with exactly one sentinel line: COMPLETE, NEEDS_CLARIFICATION: <question>, or BLOCKED: <reason>."` On `COMPLETE`, call the script again with `--resume`.
- **Exit 11 — unresolved comments** → spawn `remediation-developer`: `"Follow the pr-comments-resolve skill with arguments '<prNumber>'."` plus the same sentinel sentence. On `COMPLETE`, `--resume`.
- **Exit 12 — ESCALATED (5+ build failures)** → go to **Escalation** with `escalationReason = "5+ build failures"`.
- **Exit 13 — NEEDS_REBASE** → if `rebaseAttempted` is already `true` in the state file, go to **Escalation** (a second rebase request on the same PR is a loop, not progress). Otherwise: `base = env -u GH_TOKEN gh pr view <prNumber> --repo MrsBohemian/Cholobots --json baseRefName -q .baseRefName`, spawn `remediation-developer`: `"Follow the rebase-branch skill with arguments '<prNumber> <base>'."` plus the sentinel sentence, set `rebaseAttempted = true` in the state file, then `--resume`.
- **Exit 14 — PENDING** (no CI verdict yet, or a Codex `REVIEW_PENDING` inside the script's wait window) → call again with `--resume`. No sleep grant is needed — the script itself waits out the window before returning.
- **Exit 1 — tooling error** → re-run once with `--resume`. If it exits `1` again, go to **Escalation** with `escalationReason = "remediate_pr.py error: <stderr>"`.

Any `remediation-developer` spawn returning `BLOCKED` → go to **Escalation**.

## QA-fix merge wait

`SKILL.md` Step 5's "another remediation round": once each QA-round sub-PR (opened by
`fix-story-bug` against `featureBranch`) reaches `STABLE` via this loop, record every
`{bugRef, prNumber}` pair in the state file's `qaFixPRs` and set `phase: "awaiting-qa-merge"` —
its own persisted phase, distinct from `remediating`, because confirming the merge into
`featureBranch` is a human gate too (`AskUserQuestion`; under `overseer-conductor`,
stop-and-report) and a resume must not silently skip it — you cannot merge it yourself.

**Resuming at `awaiting-qa-merge`:** re-check every `qaFixPRs` entry live (`env -u GH_TOKEN gh pr
view <n> --repo MrsBohemian/Cholobots --json mergedAt`) before doing anything else — do not just
re-ask. If any entry is still unmerged, stop-and-report again (or re-ask) rather than re-driving
Step 4 or re-offering QA. Only once **every** entry shows a non-null `mergedAt` do you re-drive
Step 4 on the original PR and re-offer QA at Gate B as a second pass (passing `qaBugRefs`).
