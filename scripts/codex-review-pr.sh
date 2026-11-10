#!/usr/bin/env bash
# ABOUTME: Runs a Codex CLI code review against a GitHub PR and posts the
# ABOUTME: findings as a review with inline comments, capped at N passes per PR.
#
# Structure borrowed from efaulhaber/codex-pr-review (MIT).
set -uo pipefail

print_usage() {
    echo "Usage: scripts/codex-review-pr.sh <pr-number> [--dry-run]"
    echo "Env:   CODEX_REVIEW_MODEL             (default gpt-5.6-luna)"
    echo "       CODEX_REVIEW_REASONING_EFFORT  (default xhigh)"
    echo "       CODEX_REVIEW_MAX_PASSES        (default 3)"
    echo "       CODEX_REVIEW_INLINE_MAX_PRIORITY (default 2)"
    echo "       CODEX_REVIEW_WAIT_SECONDS      (default 540)"
    echo "       CODEX_REVIEW_STATE_DIR         (default \${TMPDIR:-/tmp}/codex-review-state)"
    echo "       CODEX_REVIEW_JOB_INIT_TIMEOUT_SECONDS (default 120)"
    echo "       CODEX_REVIEW_WORKER_TIMEOUT_SECONDS   (default 1800)"
    echo "       CODEX_REVIEW_REMOTE            (default upstream; git remote holding PR branches)"
    echo "       GH_REPO                        (default MrsBohemian/Cholobots; gh runs as 'env -u GH_TOKEN gh')"
    echo "Worktrees go under <checkout>/.claude/worktrees/codex-review-*."
    echo "Last stdout line:"
    echo "  REVIEW_POSTED:<pass>/<max>:<inline>:<body_only>"
    echo "  REVIEW_DRYRUN:<pass>/<max>:<inline>:<body_only>  (--dry-run, after the payload)"
    echo "  REVIEW_PENDING:<pass>/<max>  (worker still running; re-invoke the same command to join it)"
    echo "  REVIEW_SKIPPED:cap"
    echo "  REVIEW_SKIPPED:same-head"
    echo "Exit 1 with a stderr message on codex or gh failure; nothing is posted on failure."
    echo "Review body marker: <!-- codex-review pass=K head=SHA model=M -->"
    echo "Internal: '--worker <jobdir>' runs the detached review worker; do not invoke it directly."
}

usage() {
    print_usage
    exit 1
}

for arg in "$@"; do
    case "$arg" in
        -h|--help)
            print_usage
            exit 0
            ;;
    esac
done

# The dead-worker reclaim path (kill_worker_session, worker_session_alive)
# has to signal and poll a whole session, not just a single pid, to reach the
# codex tree `timeout` moves into its own process group. Without pgrep/pkill,
# a plain `kill -- "-$sid"` reaches only the worker's own process group and
# silently leaves that orphaned tree running against the removed worktree and
# the Codex API. Every machine this script runs on (developer boxes, CI
# runners) ships procps, so require it and fail loudly rather than falling
# back to a signal that's known to miss processes.
for tool in pgrep pkill; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "Error: '$tool' is required (part of procps) but not found on PATH" >&2
        exit 1
    fi
done

# Runs the codex CLI against the PR's worktree, writing its raw output to
# $OUT_JSON. Only ever called from the detached worker (run_worker), after it
# has sourced the job's env file, so WORKTREE/PR/TITLE/BASE_REF/OUT_JSON and
# the CODEX_REVIEW_* knobs are all already set.
run_codex() {
    # Supplement Codex's built-in review rubric (bug criteria, comment style,
    # P0-P3 tagging) rather than restating it. The heredoc is unquoted so
    # $BASE_REF expands; it must never contain backticks, which would execute.
    #
    # `codex exec review` treats --base, --commit, --uncommitted, and a positional
    # prompt as mutually exclusive review targets, so the prompt names the base
    # branch itself. Codex parses the model's final message as its own
    # ReviewOutputEvent ({findings, overall_*}) and, when that succeeds, writes a
    # prose rendering to -o instead of the JSON; text it cannot parse is written
    # through verbatim (parse_review_output_event in
    # codex-rs/core/src/session/review.rs). Wrapping the native shape under a
    # "review" key defeats that parse so the JSON survives.
    PROMPT=$(cat <<PROMPTEOF
Review the changes on this branch against $CODEX_REVIEW_REMOTE/$BASE_REF, using
git diff $CODEX_REVIEW_REMOTE/$BASE_REF...HEAD. Report findings only on lines changed by this
PR, using new-file line numbers and absolute file paths.
Give each finding a terse title plus a concrete fix.
The codebase is Cholobots: a single Python 3.14 discord.py bot hosting six personas,
backed by Supabase (synchronous client, service-role key) and OpenAI. Read the root
CLAUDE.md for its architecture and conventions. Watch in particular for blocking I/O
on the event loop, on_message listeners that consume replies meant for another flow,
in-memory session state, commands missing from the attachment whitelist or the help
text, and Supabase queries with the wrong column names.

Output ONLY one JSON object, no prose and no code fence, of this exact shape:
{"review": {"findings": [{"title": "...", "body": "...", "confidence_score": 0.0,
"priority": 0, "code_location": {"absolute_file_path": "...", "line_range":
{"start": 1, "end": 1}}}], "overall_correctness": "...", "overall_explanation": "...",
"overall_confidence_score": 0.0}}
The top-level key must be "review"; never emit "findings" at the top level.
PROMPTEOF
)

    (
        cd "$WORKTREE" || exit 1
        unset OPENAI_API_KEY CODEX_API_KEY # force the ChatGPT login
        # Bound a hung `codex` invocation instead of letting it block this
        # worker process (and thus $JOBDIR/pid's liveness) forever — that
        # would make every re-invocation report REVIEW_PENDING indefinitely,
        # with no way to ever reclaim the job. `timeout` (no --foreground)
        # runs `codex` in its own process group and signals that whole group
        # on expiry, so a child process `codex` spawns is bounded too; -k
        # backs that up with a KILL if the group ignores the first signal.
        printf '%s' "$PROMPT" | timeout -k 30 "$CODEX_REVIEW_WORKER_TIMEOUT_SECONDS" \
            codex -m "$CODEX_REVIEW_MODEL" -s read-only \
            -c model_reasoning_effort="$CODEX_REVIEW_REASONING_EFFORT" \
            exec review --ephemeral --ignore-user-config \
            --ignore-rules -o "$OUT_JSON" --title "PR #$PR: $TITLE" -
        # The prompt names $CODEX_REVIEW_REMOTE/$BASE_REF, which the model resolves inside the
        # detached worktree; the foreground already fetched that remote-tracking
        # ref before launching this worker, so this should resolve. A local
        # branch pointing at the fetched base SHA is the fallback if that ever
        # proves untrue.
    ) || {
        echo "Error: codex exec review failed" >&2
        return 1
    }
}

# Parses $OUT_JSON (written by run_codex), partitions findings into inline
# comments vs. body-only entries, builds the review payload, and either
# prints it (--dry-run) or POSTs it. Prints the REVIEW_POSTED/REVIEW_DRYRUN
# contract line as the last line of its stdout. Only ever called from the
# detached worker, after run_codex succeeds.
finish_review() {
    RAW_OUTPUT=$(cat "$OUT_JSON" 2>/dev/null || true)
    # Tolerate a fenced ```json block around the payload.
    JSON_OUTPUT=$(printf '%s\n' "$RAW_OUTPUT" | sed -e '/^```json$/d' -e '/^```$/d')

    if ! printf '%s' "$JSON_OUTPUT" | jq -e '.review.findings' >/dev/null 2>&1; then
        echo "Error: codex output is not valid JSON with a 'review.findings' field" >&2
        return 1
    fi
    JSON_OUTPUT=$(printf '%s' "$JSON_OUTPUT" | jq -c '.review')

    # Reviewable lines: the set of new-file "path:line" pairs GitHub will accept
    # an inline comment on. A comment outside this set gets a 422 from the API.
    REVIEWABLE_FILE="$JOBDIR/reviewable"
    FINDINGS_FILE="$JOBDIR/findings"
    PAYLOAD_FILE="$JOBDIR/payload.json"

    git diff -U3 "$CODEX_REVIEW_REMOTE/$BASE_REF...$HEAD_SHA" | awk '
        /^\+\+\+ / {
            path = $0
            sub(/^\+\+\+ /, "", path)
            sub(/^b\//, "", path)
            # git terminates the header path with a literal TAB when the pathname
            # contains a space, to keep the path unambiguous. Strip it so the
            # reviewable-line key matches the plain path on the finding.
            sub(/\t$/, "", path)
            next
        }
        /^@@/ {
            match($0, /\+[0-9]+(,[0-9]+)?/)
            spec = substr($0, RSTART + 1, RLENGTH - 1)
            n = split(spec, parts, ",")
            start = parts[1] + 0
            count = (n > 1) ? parts[2] + 0 : 1
            for (i = 0; i < count; i++) {
                print path ":" (start + i)
            }
            next
        }
    ' > "$REVIEWABLE_FILE"

    printf '%s' "$JSON_OUTPUT" | jq -c '.findings[]' > "$FINDINGS_FILE"

    INLINE_COMMENTS_JSON="[]"
    BODY_ONLY_LIST=""
    INLINE_COUNT=0
    BODY_ONLY_COUNT=0

    while IFS= read -r finding; do
        [[ -z "$finding" ]] && continue

        abs_path=$(printf '%s' "$finding" | jq -r '.code_location.absolute_file_path')
        path="${abs_path#$WORKTREE/}"
        path="${path#./}"
        start=$(printf '%s' "$finding" | jq -r '.code_location.line_range.start')
        end=$(printf '%s' "$finding" | jq -r '.code_location.line_range.end')
        priority=$(printf '%s' "$finding" | jq -r '.priority // "null"')
        title=$(printf '%s' "$finding" | jq -r '.title')
        finding_body=$(printf '%s' "$finding" | jq -r '.body')
        comment_body="${title}"$'\n\n'"${finding_body}"

        in_diff=false
        if grep -qxF "$path:$end" "$REVIEWABLE_FILE"; then
            in_diff=true
        fi

        is_inline=false
        # Codex's native output is not schema-validated, so priority may be absent,
        # null, or a non-numeric string such as "P1". Only an unsigned integer is
        # comparable; anything else is treated as not inline-eligible and demoted to
        # a body-only finding. The regex guard also keeps the arithmetic comparison
        # below from aborting the run under `set -u`.
        if [[ "$priority" =~ ^[0-9]+$ ]] && [[ "$priority" -le "$CODEX_REVIEW_INLINE_MAX_PRIORITY" ]] && [[ "$in_diff" == true ]]; then
            is_inline=true
        fi

        if [[ "$is_inline" == true ]]; then
            # GitHub accepts the multi-line form only when `start_line` is also a
            # line in the diff and is strictly less than `line`; otherwise the whole
            # review POST is rejected with a 422 and every finding in the pass is
            # lost. Codex can emit a start that is out of diff, inverted, or absent,
            # so a range that fails any of those checks collapses to a single-line
            # comment on `end`, which the in-diff check above already verified.
            multi_line=false
            if [[ "$start" =~ ^[0-9]+$ ]] && [[ "$start" -lt "$end" ]] \
                && grep -qxF "$path:$start" "$REVIEWABLE_FILE"; then
                multi_line=true
            fi

            if [[ "$multi_line" == true ]]; then
                comment_json=$(jq -n --arg path "$path" --argjson line "$end" --argjson start_line "$start" \
                    --arg body "$comment_body" \
                    '{path: $path, line: $line, side: "RIGHT", start_line: $start_line, start_side: "RIGHT", body: $body}')
            else
                comment_json=$(jq -n --arg path "$path" --argjson line "$end" --arg body "$comment_body" \
                    '{path: $path, line: $line, side: "RIGHT", body: $body}')
            fi
            INLINE_COMMENTS_JSON=$(printf '%s' "$INLINE_COMMENTS_JSON" | jq --argjson c "$comment_json" '. + [$c]')
            INLINE_COUNT=$((INLINE_COUNT + 1))
        else
            BODY_ONLY_LIST="${BODY_ONLY_LIST}"$'\n\n'"- **${path}:${end}** ${title}"$'\n\n'"  ${finding_body}"
            BODY_ONLY_COUNT=$((BODY_ONLY_COUNT + 1))
        fi
    done < "$FINDINGS_FILE"

    MARKER="<!-- codex-review pass=$PASS head=$HEAD_SHA model=$CODEX_REVIEW_MODEL -->"
    OVERALL_CORRECTNESS=$(printf '%s' "$JSON_OUTPUT" | jq -r '.overall_correctness // ""')
    OVERALL_EXPLANATION=$(printf '%s' "$JSON_OUTPUT" | jq -r '.overall_explanation // ""')

    REVIEW_BODY="## Codex Review (pass $PASS/$CODEX_REVIEW_MAX_PASSES)
$MARKER

**Correctness:** $OVERALL_CORRECTNESS

$OVERALL_EXPLANATION"

    if [[ -n "$BODY_ONLY_LIST" ]]; then
        REVIEW_BODY="${REVIEW_BODY}

### Additional findings${BODY_ONLY_LIST}"
    fi

    jq -n --arg commit_id "$HEAD_SHA" --arg body "$REVIEW_BODY" --argjson comments "$INLINE_COMMENTS_JSON" \
        '{commit_id: $commit_id, event: "COMMENT", body: $body, comments: $comments}' > "$PAYLOAD_FILE"

    if [[ "$DRY_RUN" == true ]]; then
        cat "$PAYLOAD_FILE"
        echo "REVIEW_DRYRUN:$PASS/$CODEX_REVIEW_MAX_PASSES:$INLINE_COUNT:$BODY_ONLY_COUNT"
        return 0
    fi

    # Same rationale as the `codex` call in run_codex: bound a stalled POST
    # so it can't keep this worker process (and $JOBDIR/pid) alive forever.
    if ! timeout -k 30 "$CODEX_REVIEW_WORKER_TIMEOUT_SECONDS" \
        env -u GH_TOKEN gh api -X POST "repos/$REPO/pulls/$PR/reviews" --input "$PAYLOAD_FILE" >/dev/null; then
        echo "Error: failed to POST review via 'gh api'" >&2
        return 1
    fi

    echo "REVIEW_POSTED:$PASS/$CODEX_REVIEW_MAX_PASSES:$INLINE_COUNT:$BODY_ONLY_COUNT"
}

# Refuses to trust a job directory that isn't exclusively ours: guards the
# unconditional `source` of $JOBDIR/env against a state directory that is
# shared with, or writable by, another local user or process, who could
# otherwise pre-create the (predictable, PR+SHA-derived) job dir or swap its
# env file for a symlink between our write and the worker's read.
job_dir_is_ours() {
    local dir="$1" uid
    uid="$(id -u)"
    [[ -d "$dir" && "$(stat -c %u "$dir" 2>/dev/null)" == "$uid" ]] || return 1
    [[ -f "$dir/env" && ! -L "$dir/env" && "$(stat -c %u "$dir/env" 2>/dev/null)" == "$uid" ]] || return 1
    return 0
}

# Runs as the detached background process (launched via 'setsid nohup bash
# "$0" --worker "$JOBDIR" >log 2>&1 &'). Owns the worktree it was handed and
# runs codex + the parse/post step, writing the caller-facing stdout to
# $JOBDIR/result and its exit code to $JOBDIR/exit. Never returns — always
# exits the process.
run_worker() {
    JOBDIR="$1"
    if ! job_dir_is_ours "$JOBDIR"; then
        echo "Error: refusing to use job dir not owned by the current user: $JOBDIR" >&2
        exit 1
    fi
    # shellcheck source=/dev/null
    source "$JOBDIR/env"
    OUT_JSON="$JOBDIR/out.json"

    trap 'git worktree remove --force "$WORKTREE" 2>/dev/null || rm -rf "$WORKTREE"' EXIT

    # The redirect wraps the whole group so $JOBDIR/result.tmp exists even
    # when run_codex fails and short-circuits finish_review.
    {
        run_codex && finish_review
    } > "$JOBDIR/result.tmp"
    WORKER_EXIT=$?

    echo "$WORKER_EXIT" > "$JOBDIR/exit"
    mv "$JOBDIR/result.tmp" "$JOBDIR/result"
    exit "$WORKER_EXIT"
}

if [[ "${1:-}" == "--worker" ]]; then
    if [[ -z "${2:-}" ]]; then
        echo "Error: --worker requires a job directory argument" >&2
        exit 1
    fi
    run_worker "$2"
    exit $?
fi

if [[ $# -lt 1 ]]; then
    usage
fi

PR="$1"
shift

if ! [[ "$PR" =~ ^[0-9]+$ ]]; then
    echo "Error: <pr-number> must be a positive integer, got '$PR'" >&2
    usage
fi

DRY_RUN=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        *)
            echo "Error: unknown flag '$1'" >&2
            usage
            ;;
    esac
done

CODEX_REVIEW_MODEL="${CODEX_REVIEW_MODEL:-gpt-5.6-luna}"
CODEX_REVIEW_REASONING_EFFORT="${CODEX_REVIEW_REASONING_EFFORT:-xhigh}"
CODEX_REVIEW_MAX_PASSES="${CODEX_REVIEW_MAX_PASSES:-3}"
CODEX_REVIEW_INLINE_MAX_PRIORITY="${CODEX_REVIEW_INLINE_MAX_PRIORITY:-2}"
CODEX_REVIEW_WAIT_SECONDS="${CODEX_REVIEW_WAIT_SECONDS:-540}"
CODEX_REVIEW_STATE_DIR="${CODEX_REVIEW_STATE_DIR:-${TMPDIR:-/tmp}/codex-review-state}"
CODEX_REVIEW_JOB_INIT_TIMEOUT_SECONDS="${CODEX_REVIEW_JOB_INIT_TIMEOUT_SECONDS:-120}"
CODEX_REVIEW_WORKER_TIMEOUT_SECONDS="${CODEX_REVIEW_WORKER_TIMEOUT_SECONDS:-1800}"

CODEX_REVIEW_REMOTE="${CODEX_REVIEW_REMOTE:-upstream}"

# PRs live on MrsBohemian/Cholobots; the shell's GH_TOKEN cannot see it, so
# every gh call drops it and names the repo explicitly.
REPO="${GH_REPO:-MrsBohemian/Cholobots}"

CHECKOUT_ROOT=$(git rev-parse --show-toplevel 2>/dev/null)
if [[ -z "$CHECKOUT_ROOT" ]]; then
    echo "Error: run this from inside the Cholobots git checkout" >&2
    exit 1
fi

PR_JSON=$(env -u GH_TOKEN gh pr view "$PR" --json headRefOid,baseRefName,title --repo "$REPO")
if [[ $? -ne 0 ]]; then
    echo "Error: failed to fetch PR #$PR via 'gh pr view'" >&2
    exit 1
fi

HEAD_SHA=$(echo "$PR_JSON" | jq -r .headRefOid)
BASE_REF=$(echo "$PR_JSON" | jq -r .baseRefName)
TITLE=$(echo "$PR_JSON" | jq -r .title)

REVIEW_BODIES=$(env -u GH_TOKEN gh api "repos/$REPO/pulls/$PR/reviews" --paginate --jq '.[].body')
if [[ $? -ne 0 ]]; then
    echo "Error: failed to fetch existing reviews via 'gh api'" >&2
    exit 1
fi

PRIOR_MARKERS=$(printf '%s\n' "$REVIEW_BODIES" | grep -o '<!-- codex-review pass=[0-9]* head=[^ ]* model=[^ ]* -->' || true)
PRIOR_COUNT=0
if [[ -n "$PRIOR_MARKERS" ]]; then
    PRIOR_COUNT=$(printf '%s\n' "$PRIOR_MARKERS" | grep -c '<!-- codex-review pass=' || true)
fi

if [[ -n "$PRIOR_MARKERS" ]] && printf '%s\n' "$PRIOR_MARKERS" | grep -q "head=$HEAD_SHA "; then
    echo "REVIEW_SKIPPED:same-head"
    exit 0
fi

if [[ "$PRIOR_COUNT" -ge "$CODEX_REVIEW_MAX_PASSES" ]]; then
    echo "REVIEW_SKIPPED:cap"
    exit 0
fi

PASS=$((PRIOR_COUNT + 1))

# DRY_RUN is part of the job identity: without it, a live invocation could
# join a pending dry-run worker and report REVIEW_DRYRUN without posting,
# while a dry-run invocation could join a live worker that posts to GitHub.
JOBDIR="$CODEX_REVIEW_STATE_DIR/pr-$PR-$HEAD_SHA-$DRY_RUN"

# Signals every process in the detached worker's session. $1 is the worker
# pid, which `setsid` also made its session id; $2 is the signal (-TERM,
# -KILL). pkill -s covers the descendants `timeout` moved into its own
# process group, which a plain process-group signal targeting only the
# worker's own group would miss; kill -- "-$sid" is kept alongside it to
# still reach the worker's own children if pkill's session match ever misses
# them for some other reason. Both pgrep and pkill are required at startup
# (see the preflight check above), so no "tool missing" fallback is needed
# here.
kill_worker_session() {
    local sid="$1" sig="$2"
    pkill "$sig" -s "$sid" 2>/dev/null || true
    kill "$sig" -- "-$sid" 2>/dev/null || true
}

# True while any process remains in the worker session $1.
worker_session_alive() {
    local sid="$1"
    pgrep -s "$sid" >/dev/null 2>&1
}

# Waits (bounded by CODEX_REVIEW_WAIT_SECONDS) for the worker owning $JOBDIR
# to produce a result, then behaves as one of:
#   - result already present (worker finished, possibly before this call even
#     started waiting) -> print it, clean up, exit with its saved exit code
#     (re-emitting the tail of its log to stderr first when that's non-zero)
#   - result seen but gone by the time it is read (a sibling joiner consumed
#     the job first) -> REVIEW_PENDING, exit 0; the sibling reports the review
#   - still running (or its pid file doesn't exist yet — see below) when the
#     wait expires -> REVIEW_PENDING, exit 0, job dir left alone so a later
#     invocation can join it
#   - pid recorded, process gone, and never wrote a result -> it died; clean
#     up, exit 1
# A missing pid file is treated the same as "still running", not "dead": the
# atomic `mkdir "$JOBDIR"` claim (see the caller) hands a brand-new job dir to
# its owner before that owner has written env/pid or launched the worker, so
# a second invocation racing the same job can reach here first. Concluding
# "dead" from an absent pid file would let that racer delete the job dir out
# from under its still-initializing owner. That grace period is bounded by
# CODEX_REVIEW_JOB_INIT_TIMEOUT_SECONDS: if the owner crashes between
# claiming $JOBDIR and ever persisting a pid (e.g. killed mid-`git fetch`), no
# pid file will ever appear, and without a bound every future invocation
# would wait out the full timeout and report REVIEW_PENDING forever.
#
# True while the job at $1 should still be waited on: its pid is alive, or it
# has none yet but is still within the grace period since it was claimed
# (creating a file under $dir, such as the env file, refreshes its mtime, so
# a slow-but-progressing setup keeps extending its own grace period). False
# means the job is dead: a live pid check failed, or the grace period elapsed
# with no pid ever appearing.
job_owner_alive() {
    local dir="$1"
    if [[ -f "$dir/pid" ]]; then
        kill -0 "$(cat "$dir/pid" 2>/dev/null)" 2>/dev/null
        return $?
    fi
    # No pid yet, and the job dir itself is already gone: the owner and a
    # joiner both call wait_and_emit on the same $JOBDIR, and only ever
    # remove it after concluding the job is resolved (result consumed, or
    # reclaimed as dead) — so a dir vanishing here means a sibling watcher
    # already reached that conclusion for us, not that this watcher's claim
    # was ever abandoned. Keep waiting, same as the "not created yet" case
    # below, rather than declaring the job dead ourselves.
    local now mtime
    now=$(date +%s)
    mtime=$(stat -c %Y "$dir" 2>/dev/null) || return 0
    [[ $((now - mtime)) -lt "$CODEX_REVIEW_JOB_INIT_TIMEOUT_SECONDS" ]]
}

# Never returns.
wait_and_emit() {
    local waited=0
    while [[ ! -f "$JOBDIR/result" ]] && job_owner_alive "$JOBDIR" \
        && [[ "$waited" -lt "$CODEX_REVIEW_WAIT_SECONDS" ]]; do
        sleep 2
        waited=$((waited + 2))
    done

    if [[ -f "$JOBDIR/result" ]]; then
        local job_exit result
        # Read the result before anything is printed or removed: the -f test
        # above is not atomic with respect to a sibling joiner's `rm -rf
        # "$JOBDIR"` below, so two invocations waiting on the same job can
        # both see the result and then race to consume it. Reading exit
        # first and result second matches the worker's write order (exit,
        # then result), so a readable result implies the exit code came from
        # the same, still-intact job dir.
        job_exit=$(cat "$JOBDIR/exit" 2>/dev/null || echo 1)
        if result=$(cat "$JOBDIR/result" 2>/dev/null); then
            [[ -n "$result" ]] && printf '%s\n' "$result"
            if [[ "$job_exit" != "0" ]]; then
                tail -n1 "$JOBDIR/log" >&2 2>/dev/null || true
            fi
            rm -rf "$JOBDIR"
            exit "$job_exit"
        fi
        # The result was there a moment ago and is now unreadable: a sibling
        # joiner consumed and removed the job dir underneath us. The review
        # itself is that sibling's to report, so emit a contract line saying
        # this invocation has nothing of its own to report rather than
        # falling through to the dead-worker path and exiting 1 as if the
        # review had failed.
        echo "REVIEW_PENDING:$PASS/$CODEX_REVIEW_MAX_PASSES"
        exit 0
    fi

    if job_owner_alive "$JOBDIR"; then
        echo "REVIEW_PENDING:$PASS/$CODEX_REVIEW_MAX_PASSES"
        exit 0
    fi

    cp "$JOBDIR/log" "$JOBDIR.log" 2>/dev/null || true
    # The worker's pid is gone, but the children it forked (the run_codex
    # subshell, its `timeout`, and the whole codex tree) are not: they were
    # orphaned by whatever killed the worker and would keep running — inside
    # the worktree removed just below, and against the Codex API — for the
    # rest of CODEX_REVIEW_WORKER_TIMEOUT_SECONDS. The worker is launched
    # under `setsid`, so its pid is also its session id, and the session is
    # what has to be signalled: `timeout` puts itself and the codex tree it
    # supervises into a *new process group* (that is how it kills the whole
    # tree on expiry), so a process-group signal aimed at the worker reaches
    # the run_codex subshell but not codex itself. The session survives that
    # setpgid, and nothing but this worker's own descendants is in it. Read
    # the pid before anything under $JOBDIR is removed, and finish the kill
    # before the worktree goes away so nothing is still reading it as it is
    # deleted.
    local dead_pid grace
    dead_pid=$(cat "$JOBDIR/pid" 2>/dev/null || true)
    if [[ "$dead_pid" =~ ^[1-9][0-9]*$ ]]; then
        kill_worker_session "$dead_pid" -TERM
        grace=0
        while worker_session_alive "$dead_pid" && [[ "$grace" -lt 25 ]]; do
            sleep 0.2
            grace=$((grace + 1))
        done
        kill_worker_session "$dead_pid" -KILL
    fi
    # A worker killed by something that bypasses its own EXIT trap (e.g.
    # SIGKILL, an OOM-killer) never removes the worktree it was handed;
    # reclaim it here so a dead worker never leaks a codex-review-* worktree.
    if job_dir_is_ours "$JOBDIR"; then
        (
            # shellcheck source=/dev/null
            source "$JOBDIR/env"
            if [[ -n "${WORKTREE:-}" && -d "$WORKTREE" ]]; then
                git worktree remove --force "$WORKTREE" 2>/dev/null || rm -rf "$WORKTREE"
            fi
        )
    fi
    rm -rf "$JOBDIR"
    echo "Error: detached review worker died (see $JOBDIR.log)" >&2
    exit 1
}

mkdir -p "$CODEX_REVIEW_STATE_DIR" || {
    echo "Error: failed to create state dir '$CODEX_REVIEW_STATE_DIR'" >&2
    exit 1
}

# Atomically claim this job's directory: plain `mkdir` fails if the path
# already exists, so at most one invocation ever becomes the owner that
# populates it and launches the worker. Every other invocation — a true race
# against this one, or a re-invocation of a job already under way — joins the
# existing job instead of overwriting its state or launching a duplicate
# worker for the same PR and pass. The job dir is named from the (public) PR
# number and head SHA, so a shared state dir would otherwise let another
# local user pre-create it and have us join, source, or clean up their
# content; -m 700 restricts it to the current user as part of the same
# mkdir call, rather than leaving a window at the default umask before a
# separate chmod. See job_dir_is_ours(), which the worker and the
# dead-worker reclaim path both check before trusting anything under here.
if ! mkdir -m 700 "$JOBDIR" 2>/dev/null; then
    # mkdir fails for reasons other than "already exists" too (a read-only or
    # missing state dir, no space left, ...). Only join an existing, owned
    # job dir; anything else is a real setup error, not a job to wait on.
    if [[ ! -d "$JOBDIR" ]]; then
        echo "Error: failed to create job dir '$JOBDIR'" >&2
        exit 1
    fi
    if [[ "$(stat -c %u "$JOBDIR" 2>/dev/null)" != "$(id -u)" ]]; then
        echo "Error: job dir '$JOBDIR' exists but is not owned by the current user" >&2
        exit 1
    fi
    wait_and_emit
fi

WORKTREE=""
WORKER_STARTED=false
HEARTBEAT_PID=""
stop_heartbeat() {
    if [[ -n "$HEARTBEAT_PID" ]]; then
        # `kill "$HEARTBEAT_PID"` alone only terminates the heartbeat's own
        # subshell — its `sleep` child is a separate process that a bare
        # SIGTERM to the subshell does not reach, so it survives as an
        # orphan and keeps running for up to its own sleep duration. Because
        # the heartbeat is launched under `setsid` below, $HEARTBEAT_PID is
        # that process group's leader, so signaling the negative pid targets
        # the whole group (subshell + its sleep child) at once. An orphaned
        # sleep here is not just a stray process: it inherited this script's
        # stdout/stderr unredirected, so a caller capturing this script's
        # output via command substitution would block until that orphan's
        # own sleep finally elapses, long after this script has exited.
        kill -- "-$HEARTBEAT_PID" 2>/dev/null || true
        wait "$HEARTBEAT_PID" 2>/dev/null || true
        HEARTBEAT_PID=""
    fi
}
cleanup() {
    # We claimed $JOBDIR with the atomic mkdir above. Once the worker has
    # been launched, it (and wait_and_emit's join path) own $JOBDIR and the
    # worktree instead — the foreground must not race it. If setup fails
    # before that handoff, remove both here so a half-populated job dir
    # never lingers to be joined by a later invocation.
    stop_heartbeat
    if [[ "$WORKER_STARTED" != true ]]; then
        rm -rf "$JOBDIR" 2>/dev/null || true
        if [[ -n "${WORKTREE:-}" && -d "$WORKTREE" ]]; then
            git worktree remove --force "$WORKTREE" 2>/dev/null || rm -rf "$WORKTREE"
        fi
    fi
}
trap cleanup EXIT

# job_owner_alive()'s pid-less grace period is anchored to $JOBDIR's mtime,
# which the atomic mkdir above set once, at claim time. The unbounded `git
# fetch`/`worktree add` below can outlast that grace period on a slow
# network, and env/pid aren't written until after both finish — so without
# something refreshing $JOBDIR in the meantime, a concurrent joiner can
# conclude this still-progressing claim is abandoned and reclaim it (and, if
# the worktree was already added, orphan it). Keep the mtime fresh for as
# long as setup is running by touching $JOBDIR on a period tied to the same
# grace window; stop_heartbeat retires this once env/pid exist and pid
# liveness takes over as the freshness signal.
#
# The period is a quarter of the grace window (at least 1s), not half of it:
# a joiner declares the claim dead once (now - mtime) reaches the grace
# window, both read at whole-second granularity, so whatever the period does
# not use is the only slack left to absorb the heartbeat being scheduled late
# on a loaded host. A half-window period left the small timeouts the test
# suite uses with ~2s of slack, which a loaded run exceeded (bug #3028).
#
# Launched under `setsid` so it (and the `sleep` child its loop spawns) form
# their own process group, letting stop_heartbeat signal that whole group at
# once instead of only the subshell — see the comment there for why a plain
# `kill` on the bare subshell is not enough.
# `touch -c` is load-bearing: a bare `touch` *creates* a missing path, so if a
# sibling watcher reclaimed this claim (rm -rf "$JOBDIR") while setup was still
# under way, the next tick would put a regular *file* back at the job-dir path
# — wedging the PR permanently, because this owner's own `> "$JOBDIR/env"` then
# fails with "Not a directory" and every later invocation's atomic mkdir fails
# against the leftover file. The heartbeat also receives this script's pid ($$)
# and exits once that process is gone. An owner killed hard enough to skip
# stop_heartbeat (SIGKILL, the OOM-killer) would otherwise leave the heartbeat
# refreshing a pid-less job dir forever: joiners would see it as live, report
# REVIEW_PENDING indefinitely and never reclaim it. The owner pid comes first
# and the job dir stays last so the process listing keeps its shape.
HEARTBEAT_PERIOD=$(( CODEX_REVIEW_JOB_INIT_TIMEOUT_SECONDS / 4 ))
[[ "$HEARTBEAT_PERIOD" -ge 1 ]] || HEARTBEAT_PERIOD=1
setsid bash -c '
    while :; do
        sleep "$2"
        kill -0 "$1" 2>/dev/null || exit 0
        [[ -d "$3" ]] || exit 0
        touch -c "$3" 2>/dev/null || exit 0
    done
' _ "$$" "$HEARTBEAT_PERIOD" "$JOBDIR" &
HEARTBEAT_PID=$!

git fetch "$CODEX_REVIEW_REMOTE" "$BASE_REF" "refs/pull/$PR/head" >/dev/null 2>&1 || {
    echo "Error: failed to fetch '$BASE_REF'/'refs/pull/$PR/head' from $CODEX_REVIEW_REMOTE" >&2
    exit 1
}

# Worktrees live under the checkout (.claude/worktrees is git-ignored), next to
# the pipeline's other worktrees, rather than in a system temp dir.
mkdir -p "$CHECKOUT_ROOT/.claude/worktrees" || {
    echo "Error: failed to create '$CHECKOUT_ROOT/.claude/worktrees'" >&2
    exit 1
}
WORKTREE="$(mktemp -d "$CHECKOUT_ROOT/.claude/worktrees/codex-review-XXXXXX")"
if ! git worktree add --detach "$WORKTREE" "$HEAD_SHA" >/dev/null 2>&1; then
    # Some git versions refuse to create a worktree in a directory that
    # already exists (even empty). Remove it and let git create it fresh.
    rmdir "$WORKTREE" 2>/dev/null || true
    git worktree add --detach "$WORKTREE" "$HEAD_SHA" >/dev/null 2>&1 || {
        echo "Error: failed to create worktree at '$WORKTREE' for $HEAD_SHA" >&2
        exit 1
    }
fi

if ! {
    printf 'PR=%q\n' "$PR"
    printf 'HEAD_SHA=%q\n' "$HEAD_SHA"
    printf 'BASE_REF=%q\n' "$BASE_REF"
    printf 'PASS=%q\n' "$PASS"
    printf 'TITLE=%q\n' "$TITLE"
    printf 'REPO=%q\n' "$REPO"
    printf 'CODEX_REVIEW_REMOTE=%q\n' "$CODEX_REVIEW_REMOTE"
    printf 'WORKTREE=%q\n' "$WORKTREE"
    printf 'DRY_RUN=%q\n' "$DRY_RUN"
    printf 'CODEX_REVIEW_MODEL=%q\n' "$CODEX_REVIEW_MODEL"
    printf 'CODEX_REVIEW_REASONING_EFFORT=%q\n' "$CODEX_REVIEW_REASONING_EFFORT"
    printf 'CODEX_REVIEW_MAX_PASSES=%q\n' "$CODEX_REVIEW_MAX_PASSES"
    printf 'CODEX_REVIEW_INLINE_MAX_PRIORITY=%q\n' "$CODEX_REVIEW_INLINE_MAX_PRIORITY"
    printf 'CODEX_REVIEW_WORKER_TIMEOUT_SECONDS=%q\n' "$CODEX_REVIEW_WORKER_TIMEOUT_SECONDS"
} > "$JOBDIR/env"; then
    echo "Error: failed to write job env file '$JOBDIR/env'" >&2
    exit 1
fi

# Setup's unbounded steps (fetch, worktree add) are done and env is on disk;
# the pid written just below is job_owner_alive()'s freshness signal from
# here on, so the heartbeat that stood in for it during setup is no longer
# needed.
stop_heartbeat

# Deliberately NOT wrapped in `timeout` here: $! below becomes $JOBDIR/pid,
# which job_owner_alive() and the "dead worker" reclaim path treat as the
# worker's own identity. Interposing a `timeout` process between this launch
# and the worker would make $! the monitor's pid, not the worker's — killing
# (or losing) that pid would no longer guarantee the worker itself is gone,
# since its process, once forked, would keep running as an orphan and could
# still complete and POST after being reclaimed as "dead". The worker's own
# runtime is bounded instead at each of its blocking external calls (`codex`
# in run_codex, `gh api POST` in finish_review), which keeps $! tied to the
# actual worker process for as long as any work remains to be done.
setsid nohup bash "$0" --worker "$JOBDIR" > "$JOBDIR/log" 2>&1 &
WORKER_PID=$!
if [[ -z "$WORKER_PID" ]]; then
    echo "Error: failed to launch detached review worker" >&2
    exit 1
fi
# A very fast worker (codex missing, an immediate auth failure, ...) can exit
# before we get here, so a live `kill -0 "$WORKER_PID"` check right after
# backgrounding it would be a false "launch failed" for a worker that
# actually ran to completion. wait_and_emit already distinguishes a genuine
# launch failure (pid dead, no result) from a real one (result present) once
# the pid is persisted, so let it make that call instead of duplicating it
# here.
#
# Write to a temp file and rename it into place rather than redirecting
# `echo` straight to "$JOBDIR/pid": a plain redirect creates the (empty) file
# before writing its contents, and a joiner reading that empty file between
# the two would `kill -0 ""`, conclude the worker is dead, and delete the
# live job and its worktree out from under it. The rename is atomic on the
# same filesystem, so joiners only ever see a fully-written pid file.
if ! { printf '%s' "$WORKER_PID" > "$JOBDIR/pid.tmp" && mv "$JOBDIR/pid.tmp" "$JOBDIR/pid"; }; then
    echo "Error: failed to persist worker pid for job '$JOBDIR'" >&2
    kill "$WORKER_PID" 2>/dev/null || true
    rm -f "$JOBDIR/pid.tmp" 2>/dev/null || true
    exit 1
fi
WORKER_STARTED=true

wait_and_emit
