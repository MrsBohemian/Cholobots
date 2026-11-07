---
name: roadmap-wiki-update
description: Append one project-39 (cholobots#N) node + edges to the cross-project execution-order wiki page's ## Dependency Diagram section, plus a matching entry in ## Cross-project dependency register. Append-only, idempotent, optimistic-concurrency with one retry.
argument-hint: "<project-39 ref> [--depends-on <ns#ref>,…]"
allowed-tools:
  - mcp__taiga-py__get_wiki
  - mcp__taiga-py__get_story
  - mcp__taiga-py__get_issue
  - mcp__taiga-py__get_epic_overview
  - Read
  - Grep
  - Bash(python3 scripts/lib/parse_roadmap.py *)
  - Bash(scripts/taiga-wiki.sh *)
  - Bash(scripts/taiga-story.sh *)
  - Bash(diff *)
  - Read(//tmp/**)
  - Edit(//tmp/**)
---

# Roadmap Wiki Update — $ARGUMENTS

Append one project-39 (Cholobots, `cholobots#N`) item's node and its
upstream dependency edges to the `cross-project-execution-order` wiki page
in Taiga project 39 — specifically, its `## Dependency Diagram` fenced block
and its `## Cross-project dependency register` bullet list. Invoked by the
overseer (or any workflow that mints a project-39 Taiga item at runtime) so
the cross-project roadmap keeps up.

**This skill only writes to the wiki page.** Its only write grant, `Edit`, is scoped
to `/tmp/**` — the scratch file the page is fetched into and spliced in —
so it can never modify a file in this repository. Its `Read` and `Grep`
grants are unscoped but read-only.

The register bullet and node stanza this skill appends are prose: write them
current-state, with no dated narration. (The append-only discipline below is
how this page records history — its stanzas are line-stable by design and
are not rewritten in place.)

## The core discipline — append stanzas, never edit lines

The parser (`scripts/lib/dependency_parser.py`,
`parse_dependency_diagram(content, default_namespace="cholobots")`) accumulates
fan-in via node **repetition**: `deps[ref]` is a `set`, so re-declaring a
node under another root **unions** its dep sets. To record
"`cholobots#38` depends on `cholobots#37`", append a **new root stanza**:

```
cholobots#37
  └─► cholobots#38
```

**even when `cholobots#37` already appears elsewhere in the section.** The
repeated root is harmless — the parser resets its stack on a root line and
unions the dep sets. This rule is what mechanically satisfies the acceptance
criterion "preserves every byte outside the diagram section + register entry
it touches", and it guarantees this skill never rewrites a hand-authored
line.

**Ownership guard.** Every **child** line in an appended stanza MUST be a
`cholobots#N` ref. A parent/root line may name any namespace (that is how a
cross-project edge is expressed), but this skill MUST **refuse** to append a
stanza that would introduce a non-`cholobots` node as a dependent. Refuse and
report — do not guess.

**Self-exclusion.** The Cholobots `.claude/` pipeline tooling is intentionally
not on the master roadmap: decline (`SKIP: cholobots#<ref> is pipeline tooling …`)
when the resolved item is tagged `pipeline`. If the caller passes one, decline
with a one-line explanation and exit.

## The page

- Taiga **project 39**, slug `cross-project-execution-order`, created by
  `scripts/taiga_project_setup.py` with the two sections this skill appends to.
  Address it by slug; read its numeric page id from the `get` metadata for the
  `patch` call.
- The prose (→ / ∥ / gate notation, wave rationale, register bullets) is
  **hand-authored** and is the authoritative narrative. **Never** regenerate
  the page or any prose section — the fenced diagram is an additive machine
  mirror carrying hard prereqs only.
- Concurrent writers (overseer runs in several sessions) coordinate through
  the `version` field alone.
- **REST is the only write path.** `scripts/taiga-wiki.sh` covers `GET`/`PATCH`
  on `/api/v1/wiki` and carries the `version` optimistic-concurrency field.
  The page is fetched to a file and spliced in place, so it never passes
  through model output, which keeps the box-drawing glyphs, em-dashes and `∥`
  separators byte-exact. The workflow MCP server's `upsert_wiki` takes no
  `version`, so it cannot do the conflict check below and is never used to
  write this page; `get_wiki` is a read-only fallback for the idempotency check.

## Ref grammar

Parsed with `--default-namespace cholobots`:

- bare `#N` or `cholobots#N` = Cholobots (project 39). This skill always
  writes the explicit `cholobots#N` form.
- any other `<ns>#N` (`homelab#N`, `alterna#N`, `gg#N`, …) = an item in
  another Taiga project. Accepted as a parent and recorded verbatim; never
  looked up.

## Argument parsing

`$ARGUMENTS` is:

- **positional**: `<project-39 ref>` — the integer ref of the Cholobots item to
  add (e.g. `38` — no `cholobots#` prefix on the argument itself; the skill
  scopes to project 39 unconditionally).
- **optional**: `--depends-on <ns#ref>,…` — comma-separated list of upstream
  refs (each in the namespaced grammar above). If omitted, the skill adds
  the node with no edges (the bare `cholobots#<ref>` line, useful when the
  item has no roadmap upstreams but should still appear as a node).

Reject any argument that does not fit these two shapes — do not silently
drop a malformed dep.

## Procedure

### 0. Preflight

1. **Check the wiki path.** `TAIGA_API_URL`, `TAIGA_USERNAME` and
   `TAIGA_PASSWORD` must all be set for `scripts/taiga-wiki.sh`. If they are
   not, stop: `BLOCKED: no wiki write path available — TAIGA_API_URL/USERNAME/PASSWORD
   unset (the taiga-py MCP cannot write with a version check)`.
2. **Self-exclusion check.** Resolve the project-39 ref through
   `get_story(project=39, ref=<ref>)`, then `get_issue`, then
   `get_epic_overview`. If the item is tagged `pipeline`, decline:
   `SKIP: cholobots#<ref> is pipeline tooling (intentionally not on the master
   roadmap)`. Exit without writing.

   Without the MCP, resolve the ref over REST instead. Probe all three item
   types in turn — plain `scripts/taiga-story.sh <ref> 39`, then `--issue`,
   then `--epic` — stopping at the first probe that exits **0** and using its
   output (it prints `tags`); do not assume the ref is a user story. A probe
   exiting 44 (not found) means try the next type. A probe exiting anything
   else (1, or any other non-44 non-zero code) is a real failure, not a "try
   the next type" signal: stop probing immediately and report `BLOCKED:
   cholobots#<ref> lookup failed (<probe invocation> exited <code>) — cannot
   verify item kind`. If all three probes exit 44, the ref does not resolve to
   any item kind in project 39: report `BLOCKED: cholobots#<ref> not found as a
   user story, issue, or epic in project 39` and stop — do not append a node
   for an item that doesn't exist.

### 1. Fetch the wiki page to a file

```bash
scripts/taiga-wiki.sh get --slug cross-project-execution-order --project-id 39 --out /tmp/roadmap-wiki.md
```

The command prints only metadata — `{"id","version","slug","project","bytes"}`
— on stdout; the page body goes straight to the file and is never echoed.
Capture **`id`** and **`version`** from that JSON. Never reuse a remembered
version: the page moves independently as other sessions write, and the whole
conflict protocol below depends on the version being the one you just read.

A not-found result means the setup has not been applied: stop with
`BLOCKED: cross-project-execution-order wiki page missing in project 39 — Run:
python3 scripts/taiga_project_setup.py --env-file scripts/taiga-auth.env --apply`.

### 2. Idempotency check

Parse the fetched file with
`parse_dependency_diagram(content, default_namespace="cholobots")` via the
permission-scoped wrapper, passing the **file path**:

```bash
python3 scripts/lib/parse_roadmap.py --default-namespace cholobots /tmp/roadmap-wiki.md
```

The `Bash(python3 scripts/lib/parse_roadmap.py *)` row in `allowed-tools`
covers the file form and the stdin form alike; it does **not** cover an
ad-hoc `python3 -c` / heredoc-into-python variant. Use the file form — a
heredoc re-emits the page as generated tokens. Compare the resulting graph
against the intended `(cholobots#<ref>, {deps})`:

- If `cholobots#<ref>` is already a key **and** every intended dep is already
  in its value set, the diagram side is complete. Locate the
  `## Cross-project dependency register` section the same way step 6 does
  (heading to the next `## ` heading or end-of-file) and `Grep` **only that
  section's bullet list** for the item's Taiga URL (the same string step 6
  composes) to check the register side too — not the whole file, since a
  narrative or historical mention of the same URL elsewhere on the page
  must not count as the register entry. Match the URL with a right-hand
  non-digit boundary, as the regex `<url>([^0-9]|$)` (dots in `<url>`
  escaped): a bare substring match lets the URL for `cholobots#3` hit an
  existing `cholobots#38` bullet and silently skip the register append:
  - If the register bullet is **also** already present, the update is fully
    complete: **no-op**. Report `already-present: cholobots#<ref> carries
    every intended edge` and exit without writing.
  - If the register bullet is **missing** (a prior run appended the diagram
    stanza but was interrupted before step 6 ran), skip steps 3–5 — there is
    no diagram delta left to splice — and go straight to step 6 so the
    register entry still lands.
- Otherwise compute the delta: the set of intended deps not yet present.
  Only these need appending. (If the node is a key but with a **subset** of
  intended deps, only the missing edges are appended — never re-add ones
  the parser already sees.)

Re-running the skill MUST be safe, including a rerun after a prior run
completed the diagram append but was interrupted before the register bullet
landed.

### 3. Locate and validate the section

Locate the literal heading `## Dependency Diagram` in `/tmp/roadmap-wiki.md`
(`Grep` it — do not read the whole file into context), then find
the fenced code block **inside** its section (up to the next `## ` heading
or end-of-file, whichever comes first).

- If the heading is missing, **STOP and report** `BLOCKED: ## Dependency
  Diagram section is not present on the cross-project-execution-order page —
  refusing to create the section; re-run scripts/taiga_project_setup.py --apply`.
  Do **not** create the section — that is the setup script's job.
- If the heading exists but its section has no fenced block yet (a freshly
  seeded page), step 5 appends a new fenced block at the end of the section
  instead of splicing before a closing fence.

### 4. Ownership-guard the stanzas

For each edge to append, form a stanza of the shape:

```
<parent-ref>
  └─► cholobots#<ref>
```

where `<parent-ref>` is the upstream (`cholobots#N`, or `<ns>#N` for another
project).

**Reject** any stanza whose child line is not `cholobots#N`. If
`--depends-on` includes an entry where the intended *dependent* is not
project 39 (impossible for this skill's caller shape, but defensive), stop
with `BLOCKED: ownership guard — child line must be cholobots#N`.

If `--depends-on` is empty, the stanza is simply:

```
cholobots#<ref>
```

— a bare root node. This is fine and creates a key with an empty dep set.

### 5. Splice the append

Insert the new stanzas **before the closing fence** of the diagram's fenced
block, separated from the preceding stanza by exactly one blank line. Do not
touch any byte outside that splice point. (No fenced block yet: append one
fenced block holding the stanzas as the last lines of the section.)

Apply it as an **anchored `Edit` on `/tmp/roadmap-wiki.md`**: `old_string` is
the last stanza plus the closing fence (a handful of lines, read with
`Grep`/`Read` on that region); `new_string` is that same text with the new
stanzas spliced in. Author only the inserted text — never re-emit the page to
compose the new content.

- Order of appended stanzas: in the same order the caller supplied them via
  `--depends-on`. Determinism helps human review of the resulting diff.
- Do not attempt to alphabetise, group, or otherwise "tidy" — the file is a
  cumulative append log; historical order is meaningful for reviewers.

### 6. Splice the register bullet

Locate `## Cross-project dependency register` and append **one** bullet at
the end of its list (or as the first line after the heading when the list is
empty), in the same voice as any existing bullets, for example:

```
- Cholobots [#38](https://backlog.growthgarage.org/project/cholo-bots/us/38)
  (tenant migration + backfill) → needs cholobots#37 tenant-model decision
```

Compose one line naming the Cholobots item (Taiga URL:
`https://backlog.growthgarage.org/project/cholo-bots/us/<ref>` for a story,
`/issue/<ref>` for a bug, `/epic/<ref>` for an epic — pick the shape
matching the resolved kind from step 0), a one-clause purpose, and any
notable upstream(s) if `--depends-on` was passed. If the item already has a
register bullet whose text contains the item's Taiga URL — matched with the
same right-hand non-digit boundary as step 2, the regex `<url>([^0-9]|$)`,
never as a bare substring — **no-op the bullet append** (the diagram append
still lands if it was needed).

Splice it the same way as step 5 — an anchored `Edit` on
`/tmp/roadmap-wiki.md` whose `old_string` is the final existing bullet (or
the heading line, for an empty list).

### 7. Write

```bash
scripts/taiga-wiki.sh patch <page id from step 1> --version <the version captured in step 1> --content-file /tmp/roadmap-wiki.md
```

The script sends only `content` and `version` — never `slug`, `owner`, etc.
Pass `--dry-run` first if you want to inspect the resolved body; the content
is redacted to a character count there, so the page still stays out of your
output.

### 8. Conflict handling — retry-once-then-stop

`scripts/taiga-wiki.sh patch` exits **3** — distinct from the generic
failure code 1 — when Taiga rejects the write for a version mismatch (its
optimistic concurrency check: another writer bumped the version between
step 1 and step 7). Key the retry on that exit code; any other non-zero exit
is a real failure, not contention, and must be reported rather than retried.

1. Re-fetch the page to a **fresh** file:
   `scripts/taiga-wiki.sh get --slug cross-project-execution-order --project-id 39 --out /tmp/roadmap-wiki-refetch.md`,
   capturing the new version.
2. **Re-run the idempotency check** on the fresh file — the other writer
   may have already added your node and edges, in which case **no-op** and
   report `already-present-after-refetch: another writer landed the same
   changes; nothing to do`.
3. If work remains, re-splice against the fresh file (never against the
   stale `/tmp/roadmap-wiki.md` from step 1 — another writer may have
   appended new stanzas in between) and retry the `patch` **once**, with the
   refetched version and `--content-file /tmp/roadmap-wiki-refetch.md`.
4. If the second attempt also exits 3, **STOP and report** `BLOCKED: two
   consecutive optimistic-concurrency conflicts on cross-project-execution-order
   — hand off to human resolution`. Never loop, never force.

### 9. Verify

Re-fetch the page to a third file and diff it against the file you actually
sent in the successful `patch` call — `/tmp/roadmap-wiki.md` if step 7
succeeded on the first try, or `/tmp/roadmap-wiki-refetch.md` if step 8's
retry is what landed:

```bash
scripts/taiga-wiki.sh get --slug cross-project-execution-order --project-id 39 --out /tmp/roadmap-wiki-verify.md
diff /tmp/roadmap-wiki.md /tmp/roadmap-wiki-verify.md          # first-try success
# or, after a step-8 retry:
diff /tmp/roadmap-wiki-refetch.md /tmp/roadmap-wiki-verify.md  # retry success
```

Diffing against the stale `/tmp/roadmap-wiki.md` after a retry compares
against bytes that were never sent — any legitimate concurrent edit folded
into the refetch would show up as a false verification failure. Always diff
against the content file named in the PATCH that actually returned 200.

An **empty diff** is the expected result: the server holds exactly the bytes
you sent, byte-for-byte. A non-empty diff means another writer landed between
your write and this read — report it, do not "fix" it.

Steps 5–6 splice `/tmp/roadmap-wiki.md` in place, so there is no pre-write
baseline; the anchored, uniquely-matching `Edit`s already bound what moved.
Confirm the intended change landed by re-running the step-2 parse against the
verify file:

```bash
python3 scripts/lib/parse_roadmap.py --default-namespace cholobots /tmp/roadmap-wiki-verify.md
```

`cholobots#<ref>` must now be a key carrying every intended dep. If it is not,
report the discrepancy rather than retrying. Hand-authored prose is out of
scope either way.

## Report

At exit, tell the caller in one paragraph:

- The Cholobots ref and its subject.
- The action taken: `appended`, `already-present`, `already-present-after-refetch`,
  `skipped-pipeline`, or `blocked: <reason>`.
- If `appended`: which stanza(s) and the register bullet, plus the new page
  `version`.
- If `blocked`: the reason and the exact recovery step (e.g., "run
  `scripts/taiga_project_setup.py --apply`", "hand off to Jon", "set the
  `TAIGA_*` env vars").

End your final message with exactly one sentinel line:
`COMPLETE`, `SKIP: <reason>`, or `BLOCKED: <reason>`.

## Notes

- **Never regenerate the page or any prose section.** This skill is
  strictly additive inside the two named sections. If the wiki drifts (a
  human edits the diagram stanzas or the register bullets), that drift is
  authoritative and this skill still appends around it — the ownership guard
  on child lines is what makes that safe.
- **Cost:** 2 reads (fetch + verify) + 1 write on the happy path; +1 read
  +1 write on a single retry. Never poll.
- **Coordination with the overseer.** The overseer's Taiga-discipline
  section names this skill: "when you create items at runtime, apply the
  roadmap-wiki rule". This skill is the mechanical half of that rule — the
  human decides *whether* a new item belongs on the roadmap; this skill
  executes the append.
