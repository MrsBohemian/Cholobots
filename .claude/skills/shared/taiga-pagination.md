# Taiga Listing and Pagination

Taiga's REST list endpoints paginate at 30 rows and return page 1 silently. Once a collection
exceeds 30 rows, a client-side filter over a single page silently under-selects. Cholobots
already has more than 30 issues and 30 stories, so this matters today.

**Rule: NEVER filter a Taiga listing client-side unless you know the listing is complete: a
ref-based read, a workflow tool that pages for you, or a REST call with pagination disabled.**

## Route 1: workflow MCP tools (preferred)

The `taiga-py` workflow server is ref-based and requests full collections (it sends
`x-disable-pagination` itself), so most skills never touch a raw listing:

| Need | Tool |
|---|---|
| One story, issue, task or epic | `get_story` / `get_issue` / `get_task(project=39, ref=N)` (O(1), no listing) |
| An epic's stories | `get_epic_overview(project=39, ref=N)` |
| Backlog stories by status, epic, tags or assignee | `browse_backlog(project=39, status="<name>", epic=<ref>, tags="a,b", assignee=...)` |
| Sprint contents | `get_sprint_board(project=39, sprint=...)` |
| Anything by text | `search(project=39, query="<one or two tokens>")` |

Push conditions into the tool's arguments rather than filtering a broader result, then re-check the
same conditions client-side on whatever comes back: a filter the server does not honour may be
ignored rather than rejected. `browse_backlog` covers stories not in a sprint; for stories in a
sprint use `get_sprint_board`. `search` is a text match, not a filter: use it to find candidates,
never to prove that no item matches a condition.

## Route 2: REST scripts

For bug selection use `scripts/taiga_select_issues.py` (see
`.claude/skills/shared/bug-sweep-options.md`); it fetches the full issue collection and applies the
eligibility rules in one place. For a single item use `scripts/taiga-story.sh <ref> 39 [--issue|--epic]
[--with-tasks]`, which reads by ref and never lists. Both default to project 39, but `taiga-story.sh`
reads its second argument as the project id, so pass `39` whenever a flag follows the ref.

## Raw REST callers

Scripts calling `https://backlog.growthgarage.org/api/v1/...` directly (curl, `scripts/*.py` via
`scripts/lib/taiga_client.py`) send the `x-disable-pagination: True` header to receive the full
collection in one response, or follow the `x-pagination-next` response header page by page. Without
either, loop pages until one returns fewer than 30 rows or the API answers
`404 Page is not 'last'`, which means "no more pages", not an error. Narrow server-side with query
parameters where the endpoint supports them (`project=39`, `status__is_closed=false`, `type=50` for
Bugs, `epic`, `milestone`, `tags`, `assigned_to`) and re-check client-side.

## Summary

- Prefer ref-based reads and the workflow tools; they never under-select a page
- A REST listing without `x-disable-pagination: True` is page 1 only
- Exactly 30 rows back from a paged call means page again, never proceed
- `404 Page is not 'last'` is end-of-data, not an error
