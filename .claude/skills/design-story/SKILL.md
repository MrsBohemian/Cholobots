---
name: design-story
description: Interactive Discord interaction design for a story — produces command syntax, message transcripts, listener ownership, persona voice and dashboard specs
argument-hint: "<story-ref> or 'new'"
allowed-tools:
  - Bash(scripts/taiga-story.sh *)
  - mcp__taiga-py__get_story
  - mcp__taiga-py__create_story
  - mcp__taiga-py__update_story
  - mcp__taiga-py__get_task
  - mcp__taiga-py__create_task
  - mcp__taiga-py__update_task
  - mcp__taiga-py__set_task_status
  - mcp__taiga-py__get_epic_overview
  - mcp__taiga-py__get_wiki
  - Bash(git diff *)
  - Bash(git log *)
  - Bash(git status *)
  - Bash(git show *)
  - Read
  - Grep
  - Glob
  - Write
  - Edit
  - AskUserQuestion
  - WebFetch(https://github.com/*)
  - WebFetch(https://raw.githubusercontent.com/*)
  - WebFetch(https://discordpy.readthedocs.io/*)
---

# Design Story $ARGUMENTS

Interactively design how a story's feature behaves in Discord (and, where it applies, on the static dashboards) before implementation planning. Produces command syntax, message transcripts, listener ownership, cancel/timeout behaviour, persona voice and dashboard specs.

**This skill runs BEFORE `/analyze-story`.** The workflow is: `/design-story` → `/analyze-story` → `/fully-implement-story`.

**THIS SKILL IS DESIGN ONLY.** You MUST NOT write application code, create branches, or run the bot. Your only outputs are: the design document in the repo, Taiga story updates, and the conversation with the user.

**THIS SKILL IS INTERACTIVE.** Whenever this skill says "ask the user" or "wait for the user", call the `AskUserQuestion` tool to pause and collect the answer. Every question in Steps 3 to 8 requires an `AskUserQuestion` call; do not proceed until the answer is incorporated.

Taiga names: [`../shared/taiga-project.md`](../shared/taiga-project.md). Every MCP call passes `project=39`.

**Scope of this skill.** It designs user-facing interaction: commands, prompts and replies, embeds, dashboards. Stories tagged `design` (#21 to #30) are architecture decisions (error handling, I/O, dispatcher, sessions, clock); they go straight to `/analyze-story`, which records the decision. Use this skill on one of them only when the decision changes what users see (e.g. the reply on a failed command for #21, timeout wording for #24).

## Step 1: Get the story

### If the argument is a ref number (existing story)

1. Fetch the story: `get_story(project=39, ref=<ref>)`.
2. If the story is closed (`Done` or `Archived`), inform the user and stop.
3. If the story belongs to an epic, read it: `get_epic_overview(project=39, ref=<epic ref>)`. Epics #59 (natural-language entry), #60 (wearable notifications), #61 (onboarding), #62 (quarterly quest) and #3 (multi-business) carry plan text and constraints the design must respect.
4. Proceed to Step 2.

### If the argument is "new"

1. Ask the user to describe the feature they want designed.
2. Proceed to Step 2 using the description as context.

## Step 1b: Review design tasks

Fetch the story's tasks (`scripts/taiga-story.sh <ref> 39 --with-tasks`) and read each open one with `get_task(project=39, ref=<task ref>)`. Tasks tagged `design` are design-phase actions to complete during this skill (extracting notes to another story, cross-referencing a related design, asking Heaven something). Complete each at the appropriate point (usually Step 3 or Step 9) and mark it done with `set_task_status(project=39, ref=<task ref>, status="Closed")`.

## Step 2: Understand the interaction context

### 2a. Read the conventions

Read the root `CLAUDE.md` (Architecture and Conventions), `docs/assessment-2026-11-06.md`, and the Taiga wiki deep dive for the persona involved (`get_wiki(project=39, slug="deep-dive-metichebot")`, `deep-dive-chismebot-guardabot`, `deep-dive-crudobot-obijuan-vueltabot`, `deep-dive-mobile`).

### 2b. Read the existing interaction patterns

Read the persona module the story touches and note:

- **Command style**: prefix (`m*`, `c*`, `g*`, `crudo*`, `o*`, or unprefixed), argument parsing, aliases
- **Prompt style**: numbered menus, `bot.wait_for` loops, session dicts and their verbs (`done`, `cancel`, `save`), which flows have timeouts
- **Reply style**: plain text vs `discord.Embed` (Vueltabot uses embeds, around `bots/vueltabot.py:162`; most others send plain text), message length, emoji
- **Persona voice**: the Spanglish register, catchphrases and how the persona addresses people; quote two or three real replies
- **Help text**: the persona's `!<persona>` help command and how entries are written
- **Listeners**: which `on_message` listeners would see replies in the channel this flow runs in (metichebot time listener, chismebot session listener, crudobot receipt listener; see `CLAUDE.md`)

### 2c. For dashboard stories

Read the relevant page in `../Cholobots-Mobile` (or `mobile/` after story #64): layout, the Supabase tables and columns it queries, how it handles empty and error states, and the shared styling in that repo. The dashboards are static HTML with a publishable Supabase key; there is no component framework.

### 2d. Check for existing designs

`Glob("docs/designs/*.md")`, and check the story description for transcripts or design notes.

## Step 3: Story intent and expected behaviour (interactive)

Present your initial read of the story, then ask:

1. **"What are we trying to accomplish?"** The outcome for the crew, not the command ("Daniel can log a job's time from his phone without opening a menu", not "add !mlog").
2. **"How do you expect this to work?"** Walk through it message by message: what the person types first, what the bot answers, where it ends.
3. **"What existing commands does this build on or replace?"**
4. **"What's explicitly out of scope?"**
5. **"Any constraints?"** Which channel or DM, mobile typing (short commands, few prompts), attachments, wearable or notification needs (#60), whether it must work for a second business (#3).

Summarise back:

```
## Story Intent

**Goal**: [1-2 sentences: the outcome]
**Journey**: [Numbered steps from trigger to completion]
**Builds on**: [Existing commands, tables, listeners]
**Out of scope**: [What we're not doing]
**Constraints**: [Channel/DM, mobile, attachments, tenancy]
```

**Wait for the user to confirm or correct before proceeding.**

### Scope check #1

If the story has several distinct goals (e.g. a new capture flow *and* a dashboard view for a different audience), say so and offer to split. If the user agrees, create the new stories with `create_story(project=39, subject=..., description=..., epic=<epic ref>, tags=[<per-bot tag>])` and continue with the primary one only.

## Step 4: People and tenancy (interactive)

Cholobots has no role system. People are the four hardcoded Discord IDs in `DISCORD_USER_TO_PERSON` (`bots/metichebot.py:120`); anyone else becomes `UNKNOWN:<id>` and today falls back to Heaven's data. Epic #3 replaces this with per-business profiles (#40). For each person or class of user the feature serves:

```
## People

### [Person or class, e.g. "Daniel (field crew)", "Heaven (owner)", "Unknown Discord user"]
- **Goal**: [what they're trying to do]
- **Where**: [channel, DM, dashboard page]
- **Sees / does**: [commands they run, replies they get]
- **Edge cases**: [unknown user, second business, two flows at once, after a redeploy]
```

Ask:

1. "Who uses this? Should an unknown user be refused, onboarded, or treated as someone?"
2. "Does it need to be scoped per business now, or can it follow epic #3 later?"
3. "Any person-specific behaviour (Daniel's wakeups, Heaven's calendar)?"

**Wait for the answers before proceeding.**

## Step 5: Command inventory (interactive)

List every command, subcommand, listener trigger and dashboard page this story touches:

```
## Commands

### `!<name> <args>`: [new/modified]
- **Persona**: [which bot module owns it]
- **Syntax**: `!<name> <required> [optional]`, aliases: [...]
- **Collisions checked**: [grep result for the name and aliases across cholobots.py and bots/*.py]
- **Attachments**: [none | required → add to the attachment-gate whitelist in cholobots.py]
- **Where**: [any channel | session channel | DM]
- **Variants**: [no args, bad args, unknown person, nothing found, Supabase failure]

### Dashboard: `<page>.html`: [new/modified]
- **Reads**: [tables/columns]
- **Variants**: [empty, loading, error, populated]
```

Ask:

1. "Does this inventory look right? Anything missing?"
2. "Do these command names read naturally in the persona's voice and on a phone keyboard?"

**Wait for the answers before proceeding.**

### Scope check #2

If the inventory shows pieces that could ship independently (a read-only view before the capture flow, one persona before another), offer to split and create the stories as in Scope check #1.

## Step 6: Message transcripts (interactive, iterative)

For each command or flow, write Discord transcripts: the happy path, then cancel, timeout, bad input, and Supabase failure. These replace wireframes.

### Transcript conventions

```
#channel-name                          channel or DM header
Daniel: !mtoday                        a person's message
Metichebot: Órale Daniel, ¿qué vas     bot reply, in persona voice, exact wording
  a hacer hoy?
  1. Kitchen remodel (Smith)           numbered menu
  2. Fence repair (Lopez)
Daniel: 1
Metichebot: [embed]                    an embed, then its parts:
  ┌ Title: Kitchen remodel
  │ Field "Tasks": 3 open
  │ Footer: Reply `done` when finished
  └
  ⏱ 5 min, no reply → "Ni modo, I'll close this one. Run !mtoday again."
                                       timeout behaviour
  ✋ `cancel` → "Cancelado, carnal."    cancel behaviour
  ⚠ other listener: chismebot session  a listener that also sees this reply
    listener would also answer `cancel`
```

### For each flow, produce:

```markdown
### [Flow name]: happy path

#jobs
Daniel: !<command> ...
<Persona>: ...

### [Flow name]: cancel / timeout / bad input / failure

...

**Reply format:** [plain text | embed: title, fields, footer], [ephemeral? no: discord.py prefix commands cannot send ephemeral messages]
**Length:** [fits a phone screen without scrolling? 2000-char message limit]
```

For a dashboard page, keep an ASCII layout instead (phone width first, the pages are used from the truck):

```
┌──── phone (375px) ────────────┐
│ Header / date                 │
│ ┌──────────────────────────┐  │
│ │ Card: job, hours, status │  │
│ └──────────────────────────┘  │
│ (empty: "No hay nada hoy")    │
└───────────────────────────────┘
```

After each flow ask:

1. "Is this the conversation you had in mind? Is the wording right for <persona>?"
2. "What should happen if they walk away mid-flow, or type a different command?"

**Iterate until the user approves each flow.**

## Step 7: Interaction flows and listener ownership

For each flow, document the mechanics the transcript implies:

```markdown
## Interaction Flows

### [Flow name]

**Trigger**: [command, free text in a session channel, scheduler tick, reaction]
**Pre-conditions**: [known person? active session? linked Crudo project?]
**State**: [where the in-progress flow lives: a `bot.wait_for` with `timeout=`, a session dict keyed by `author.id` (and guild/channel?), or a Supabase row]
**Reply ownership**: [which code consumes each reply; which other `on_message` listeners also see it and why they ignore it]

1. Person sends [message]
2. → Supabase read: [table.columns] (offloaded with `asyncio.to_thread`)
3. → Reply: [transcript ref]
4. → Supabase write: [table.columns]
5. → On failure: [exact reply; the flow ends or retries]

**Cancel**: [words accepted, reply, state cleared]
**Timeout**: [duration, reply, state cleared]
**After a redeploy**: [what the person sees if they reply to a dead flow]
```

Ask the user about any flow with ambiguous cancel, timeout or failure handling.

## Step 8: Help text and voice

```markdown
## Help Text

### `!<persona>` additions/changes
- `!<name> <args>`: [one-line description in persona voice]

### `!cholobots` change
[none | the line to add]

## Voice Notes
- [Phrases this persona uses for confirm / cancel / error, quoted from the module]
- [Words to avoid: anything that collides with listener keywords (`cancel`, `stop`, `save`, `yes`, `1`) unless the flow owns them]
```

Ask: "Does the help text read right? Anything Heaven would phrase differently?"

## Step 9: Save the design artifact

### 9a. Create the design file

Create or update `docs/designs/story-<ref>.md`:

```markdown
# Design: <Story Subject>

**Story**: #<ref>
**Date**: <today>
**Status**: Approved / Draft

## Story Intent
[Step 3]

## People and tenancy
[Step 4]

## Commands
[Step 5]

## Transcripts
[Step 6: all approved transcripts and dashboard layouts]

## Interaction Flows
[Step 7, including listener ownership]

## Help Text
[Step 8]

## Dashboard changes
[pages, tables/columns read, empty/error states; or "None"]

## Design Decisions
[decisions made during the session, with rationale]

## Open Questions
[anything unresolved that needs answers before implementation]
```

### 9b. Update the Taiga story

Write a design summary into the story description:

```
## Design

Design document: `docs/designs/story-<ref>.md`

### Flow Summary
[One line per command/flow and dashboard page]

### Key Design Decisions
[Bullets that affect implementation: listener ownership, timeouts, embed vs text, tenancy]
```

**Replace the description; do not append to it or preserve a copy of the original.** Keep the story's capability description and acceptance criteria above the `## Design` section (or fold them into it), along with any fact the design depends on.

Save with `update_story(project=39, ref=<ref>, description=...)`.

### 9c. Update dependent stories with data requirements

If this story depends on another story for data (a new table or column, a migration under #32/#49, a tenant column under #38, a shared helper such as #22 or #30), add a `## Data Requirements from #<ref> Design` section to that story's description, implementation-ready:

1. **Tables and columns**: name, Postgres type, nullable, default, and which existing query or table it parallels
2. **Writes**: which command writes which columns, and whether the write must be atomic with another (#25)
3. **Reads**: which command or dashboard reads them, with the filter (person, business, date range in `America/Chicago`)
4. **Access**: service-role from the bot, or publishable key from a dashboard (RLS implications, #43)
5. **Cardinality and scale**: one row per what, rough rows per week

Sections accumulate as several designs feed the same story.

### 9d. Add the design-reviewed tag and advance status

1. Read the story's current tags and add `design-reviewed`.
2. Advance the story to `Ready for Analysis` if its current status is pre-analysis (`New`).
3. Apply both together:
   ```
   update_story(project=39, ref=<ref>, tags=[<existing tags...>, "design-reviewed"], status="Ready for Analysis")
   ```
   If the story is already at `Ready for Analysis` or later, omit `status`.

## Step 10: Report

Tell the user:

- Which design file was created or updated (it is uncommitted in the working tree)
- The design decisions made
- Open questions that still need resolution
- To run `/analyze-story <ref>` next

## STOP — Do Not Implement

This skill ends here. Do NOT proceed to implementation planning, code changes, or branch creation beyond the design artifact and Taiga updates above.
