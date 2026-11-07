#!/usr/bin/env bash
# ABOUTME: Polls a GitHub PR for build status and review comments, returning a
# ABOUTME: machine-parseable one-line result when an actionable event occurs.

set -uo pipefail

usage() {
    echo "Usage: $0 <pr-number> <quiet-minutes> [--poll-interval <seconds>] [--max-seconds <seconds>]"
    echo ""
    echo "Polls a PR until checks fail, review comments appear, or stability is reached."
    echo "With --max-seconds, gives up waiting after that many seconds and prints PENDING"
    echo "so callers bounded by a command timeout can re-invoke instead of blocking."
    echo ""
    echo "Output (one line, machine-parseable):"
    echo "  STABLE                          — required checks complete, no unresolved non-advisory threads, and the quiet period (0 = immediate) has elapsed"
    echo "  NEEDS_REBASE                    — PR has merge conflicts"
    echo "  CHECKS_FAILING:<check1>,<check2> — these checks are failing"
    echo "  COMMENTS_FOUND:<count>          — unresolved review comment threads (excludes advisory reviewers, see MONITOR_ADVISORY_REVIEWERS)"
    echo "  PENDING                         — no verdict within --max-seconds window"
    echo ""
    echo "An empty workflow-run list for the head SHA counts as pending for"
    echo "MONITOR_RUN_REGISTRATION_GRACE_SECONDS (default 120) after the monitor first"
    echo "sees it for that SHA, then as final: a base no workflow triggers on never gets runs."
    echo ""
    echo "No CI configured: when the repo has zero Actions workflows, no required status"
    echo "contexts and no runs for the head SHA, checks count as passed at once (no grace"
    echo "wait) and a 'NOTE: no CI checks configured ...' line goes to stderr. stdout keeps"
    echo "the one-line contract above."
    echo ""
    echo "Env: GH_REPO (default MrsBohemian/Cholobots). gh runs as 'env -u GH_TOKEN gh'."
    exit 1
}

if [[ $# -lt 2 ]]; then
    usage
fi

PR_NUMBER="$1"
QUIET_MINUTES="$2"
POLL_INTERVAL=60
MAX_SECONDS=""

# Validate numeric inputs
if ! [[ "$PR_NUMBER" =~ ^[0-9]+$ ]]; then
    echo "Error: <pr-number> must be a positive integer, got '$PR_NUMBER'" >&2
    usage
fi
if ! [[ "$QUIET_MINUTES" =~ ^[0-9]+$ ]]; then
    echo "Error: <quiet-minutes> must be a non-negative integer, got '$QUIET_MINUTES'" >&2
    usage
fi

shift 2
while [[ $# -gt 0 ]]; do
    case "$1" in
        --poll-interval)
            if [[ $# -lt 2 ]] || ! [[ "$2" =~ ^[1-9][0-9]*$ ]]; then
                echo "Error: --poll-interval requires a positive integer value" >&2
                usage
            fi
            POLL_INTERVAL="$2"
            shift 2
            ;;
        --max-seconds)
            if [[ $# -lt 2 ]] || ! [[ "$2" =~ ^[1-9][0-9]*$ ]]; then
                echo "Error: --max-seconds requires a positive integer value" >&2
                usage
            fi
            MAX_SECONDS="$2"
            shift 2
            ;;
        *)
            usage
            ;;
    esac
done

QUIET_SECONDS=$((QUIET_MINUTES * 60))

# How long an empty run list for the head SHA counts as pending (Mode B).
# GitHub registers a freshly pushed SHA's workflow runs a few seconds after
# the push, so an empty list at first means "not registered yet". A PR whose
# base no workflow triggers on (ci.yml only runs pull_request for main/master)
# never gets runs, so past this grace the empty list is final. Callers bound
# by --max-seconds need a window longer than this grace to see that verdict.
RUN_REGISTRATION_GRACE_SECONDS="${MONITOR_RUN_REGISTRATION_GRACE_SECONDS:-120}"
if ! [[ "$RUN_REGISTRATION_GRACE_SECONDS" =~ ^[0-9]+$ ]]; then
    echo "Error: MONITOR_RUN_REGISTRATION_GRACE_SECONDS must be a non-negative integer, got '$RUN_REGISTRATION_GRACE_SECONDS'" >&2
    exit 1
fi

# Repo owner and name. PRs land on the upstream repo; the exported GH_TOKEN
# cannot see it, so every gh call drops it with `env -u GH_TOKEN`.
GH_REPO="${GH_REPO:-MrsBohemian/Cholobots}"
# `gh run list` returns 20 runs by default. A busy PR can have more runs for one
# head SHA than that, and a missing failing or pending run would read as STABLE.
RUN_LIST_LIMIT=1000
OWNER="${GH_REPO%%/*}"
REPO="${GH_REPO#*/}"

# Whether the repo has any CI at all: "unknown" until probed, then "yes" or
# "no". Probed lazily, the first time a head SHA has no workflow runs. A repo
# with zero Actions workflows never gets runs, so waiting out the registration
# grace would only delay a verdict that cannot change.
ci_configured="unknown"
no_ci_noted=false

# Advisory checks that never gate merge, used only in the Mode B fallback
# (see the required-check determination in the poll loop). Overridable via a
# comma-separated MONITOR_ADVISORY_CHECKS env var. The default is empty: the
# pipeline's own review posts as the token owner and does gate. Operators name
# any check that merely posts suggestions; jq's index() match is exact-string.
IFS=',' read -ra _advisory_arr <<< "${MONITOR_ADVISORY_CHECKS:-}"
advisory_json=$(printf '%s\n' "${_advisory_arr[@]}" | jq -Rnc '[inputs | gsub("^\\s+|\\s+$"; "") | select(length > 0)]')

# Advisory reviewers whose unresolved review threads never gate merge,
# mirroring the required-vs-advisory scoping for checks above (#2092/#2103).
# Overridable via a comma-separated MONITOR_ADVISORY_REVIEWERS env var.
# The default is empty: the pipeline's own review threads gate merge. Operators
# name any bot that opens a fresh thread on every force-push and would
# otherwise loop the merge train indefinitely.
IFS=',' read -ra _advisory_reviewer_arr <<< "${MONITOR_ADVISORY_REVIEWERS:-}"
advisory_reviewers_json=$(printf '%s\n' "${_advisory_reviewer_arr[@]}" | jq -Rnc '[inputs | gsub("^\\s+|\\s+$"; "") | select(length > 0)]')

stability_start=""
empty_runs_start=""
empty_runs_sha=""
first_poll=true
if [[ -n "$MAX_SECONDS" ]]; then
    deadline=$(( $(date +%s) + MAX_SECONDS ))
fi

# Capture stderr from JSON-producing gh calls separately so warning/progress text
# can never contaminate the payload that jq parses.
gh_stderr=$(mktemp)
trap 'rm -f "$gh_stderr"' EXIT

while true; do
    # Check immediately on first iteration, then sleep between subsequent polls.
    # In bounded-wait mode, never sleep past the deadline: cap the sleep at the
    # remaining time so total runtime stays within MAX_SECONDS (plus one poll's
    # worth of API calls), not MAX_SECONDS + POLL_INTERVAL.
    if [[ "$first_poll" == true ]]; then
        first_poll=false
    elif [[ -n "$MAX_SECONDS" ]]; then
        remaining=$(( deadline - $(date +%s) ))
        if [[ "$remaining" -lt "$POLL_INTERVAL" ]]; then
            sleep "$(( remaining > 0 ? remaining : 0 ))"
        else
            sleep "$POLL_INTERVAL"
        fi
    else
        sleep "$POLL_INTERVAL"
    fi

    # Bounded-wait mode: give up without a verdict once the deadline passes so
    # callers with their own command timeout can re-invoke instead of blocking.
    if [[ -n "$MAX_SECONDS" ]] && [[ $(date +%s) -ge $deadline ]]; then
        echo "PENDING"
        exit 0
    fi

    # Check PR build status via Actions API (works with fine-grained PATs;
    # the Checks API requires a GitHub App token which fine-grained PATs cannot provide)
    pr_sha=$(env -u GH_TOKEN gh pr view "$PR_NUMBER" --json headRefOid -q .headRefOid --repo "$GH_REPO" 2>&1) || {
        echo "ERROR:gh pr view failed: ${pr_sha}"
        exit 1
    }

    # Check if PR has merge conflicts
    mergeable=$(env -u GH_TOKEN gh pr view "$PR_NUMBER" --json mergeable -q .mergeable --repo "$GH_REPO" 2>&1) || {
        echo "ERROR:gh pr view mergeable failed: ${mergeable}"
        exit 1
    }

    if [[ "$mergeable" == "CONFLICTING" ]]; then
        echo "NEEDS_REBASE"
        exit 0
    fi

    runs_json=$(env -u GH_TOKEN gh run list --commit "$pr_sha" --limit "$RUN_LIST_LIMIT" --json name,status,conclusion,createdAt --repo "$GH_REPO" 2>"$gh_stderr") || {
        # Output must stay one line for stability_parser.py; flatten any
        # embedded newlines from the captured stderr.
        echo "ERROR:gh run list failed: $(tr '\n' ' ' <"$gh_stderr")"
        exit 1
    }
    # A zero-exit gh call can still emit a non-JSON payload; fail loudly rather
    # than letting the downstream jq parses silently yield "nothing failing,
    # nothing pending" and report a false STABLE.
    if ! echo "$runs_json" | jq -e 'type == "array"' >/dev/null 2>&1; then
        echo "ERROR:gh run list returned unparseable JSON: ${runs_json}"
        exit 1
    fi

    # Determine the PR base branch's required status checks so we only treat a
    # non-success completed run as blocking when it actually gates merge.
    #   Mode A (required contexts readable + non-empty): block a run only when
    #     its .name is in the required set.
    #   Mode B (fallback — contexts empty/unreadable: a 404 on an unprotected
    #     base branch, common for merge-train sub-PRs, or a 403 from a
    #     fine-grained PAT): block every non-success run EXCEPT advisory ones.
    # Name-matching caveat: `gh run list --json name` returns workflow-run names
    # (e.g. CI) while branch-protection required contexts for GitHub Actions are
    # commonly reported as "<workflow> / <job>" (e.g. "CI / build"). Take the
    # part before " / " so Mode A contexts reconcile with workflow-run names;
    # contexts with no " / " (already a bare workflow or job name) pass through
    # unchanged. If the check granularity still doesn't align, Mode B (verified
    # on PR #763 that advisory checks are fully handled by the Mode B exclude
    # list) remains the fallback whenever contexts are unreadable.
    base_ref=$(env -u GH_TOKEN gh pr view "$PR_NUMBER" --json baseRefName -q .baseRefName --repo "$GH_REPO" 2>/dev/null || true)
    required_json="[]"
    if [[ -n "$base_ref" ]]; then
        # Branch names may legally contain "/" (e.g. "release/2026-07"); URL-encode
        # before interpolating into the REST path so GitHub doesn't misread it as an
        # extra path separator (which would 404 and force an unnecessary Mode B fallback).
        base_ref_encoded=$(jq -rn --arg s "$base_ref" '$s | @uri')
        # A 404 (unprotected base) or 403 (token lacks admin read) yields an
        # error object or empty output; normalize any non-array to [] so the
        # jq --argjson below always receives valid JSON (Mode B fallback).
        required_json=$(env -u GH_TOKEN gh api "repos/${OWNER}/${REPO}/branches/${base_ref_encoded}/protection/required_status_checks/contexts" 2>/dev/null \
            | jq -c 'if type == "array" then map(split(" / ")[0]) else [] end' 2>/dev/null || true)
        [[ -n "$required_json" ]] || required_json="[]"
    fi

    # Deduplicate to latest run per workflow (reruns can leave stale failures),
    # then select non-success completed runs — scoped to required checks
    # (Mode A) or excluding advisory checks (Mode B). A completed non-success
    # run blocks when it is in the required set (Mode A) — including
    # conclusion "skipped", which branch protection does not accept as
    # satisfying a required context — while Mode B ignores skipped runs
    # because it cannot distinguish gating from advisory ones.
    failing_checks=$(echo "$runs_json" \
        | jq -r --argjson required "$required_json" --argjson advisory "$advisory_json" '
            group_by(.name)
            | map(sort_by(.createdAt) | last)
            | .[]
            | select(.status == "completed" and .conclusion != "success")
            | .name as $n
            | select(
                if ($required | length) > 0
                then ($required | index($n)) != null
                else ($advisory | index($n)) == null and .conclusion != "skipped"
                end
              )
            | $n
        ' \
        | paste -sd, -)

    if [[ -n "$failing_checks" ]]; then
        echo "CHECKS_FAILING:${failing_checks}"
        exit 0
    fi

    # Check for unresolved review threads, scoped to non-advisory authors:
    # threads opened by advisory reviewers (MONITOR_ADVISORY_REVIEWERS)
    # never gate merge, matching the required-vs-advisory check scoping above
    # (#2092/#2103). Human and other-bot threads still block.
    # --paginate follows pageInfo.endCursor until hasNextPage is false and prints
    # one JSON document per page; they are merged below. A single page would
    # check only the first 100 threads and miss an unresolved one beyond them.
    threads_raw=$(env -u GH_TOKEN gh api graphql --paginate \
        -f owner="$OWNER" -f name="$REPO" -F number="$PR_NUMBER" \
        -f query='
    query($owner: String!, $name: String!, $number: Int!, $endCursor: String) {
      repository(owner: $owner, name: $name) {
        pullRequest(number: $number) {
          reviewThreads(first: 100, after: $endCursor) {
            nodes {
              id
              isResolved
              comments(first: 1) { nodes { author { login } } }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
      }
    }' 2>"$gh_stderr") || {
        echo "ERROR:GraphQL query failed: $(tr '\n' ' ' <"$gh_stderr")"
        exit 1
    }

    # Merge the pages into one document. Any page without a nodes array (an
    # error payload) or an empty response fails loudly instead of reading as
    # "no unresolved threads".
    if ! threads_json=$(jq -sc '
        if length == 0 then error("no pages") else . end
        | {data: {repository: {pullRequest: {reviewThreads: {nodes: [
            .[] | .data.repository.pullRequest.reviewThreads.nodes
            | if type == "array" then .[] else error("page without nodes") end
          ]}}}}}' <<<"$threads_raw" 2>/dev/null); then
        echo "ERROR:GraphQL query returned unparseable JSON: $(tr '\n' ' ' <<<"$threads_raw")"
        exit 1
    fi

    # Match advisory logins case-insensitively; a null author (deleted/ghost
    # user) is treated as non-advisory so the thread still blocks.
    unresolved_threads=$(echo "$threads_json" \
        | jq --argjson advisory "$advisory_reviewers_json" '
            ($advisory | map(ascii_downcase)) as $adv
            | [.data.repository.pullRequest.reviewThreads.nodes[]
               | select(.isResolved == false)
               | select((((.comments.nodes[0].author.login) // "") | ascii_downcase) as $a
                        | ($adv | index($a)) == null)]
            | length')

    if [[ "$unresolved_threads" -gt 0 ]]; then
        echo "COMMENTS_FOUND:${unresolved_threads}"
        exit 0
    fi

    # Check whether any check we care about is still non-terminal — scoped to
    # required checks (Mode A) or excluding advisory checks (Mode B), matching
    # the failing-checks scoping above so we never declare a terminal verdict
    # while a required check is in_progress/queued or has not started yet.
    #   Mode A: a required context is pending if its latest run is missing
    #     (not started) or not completed (in_progress/queued). Advisory or
    #     non-required pending runs are ignored so a stuck advisory check
    #     cannot keep the loop alive forever.
    #   Mode B: any non-completed run is pending, except advisory ones. An
    #     empty run list is also pending until the registration grace has
    #     passed: a freshly pushed SHA has no runs until GitHub registers
    #     them, and that must not read as "nothing pending" (the quiet window
    #     no longer papers over it), but a base no workflow triggers on never
    #     gets runs and must still reach a verdict.
    #     The grace belongs to the head SHA: a force-push restarts it so the
    #     new SHA never inherits an older SHA's expired grace.
    #   No CI configured (zero workflows, no required contexts): an empty
    #     run list is final at once, with a NOTE on stderr. An unreadable
    #     workflow list stays pending (never STABLE) until it can be read.
    awaiting_registration=false
    if [[ $(echo "$runs_json" | jq 'length') -eq 0 ]]; then
        if [[ "$ci_configured" == "unknown" ]]; then
            workflow_count=$(env -u GH_TOKEN gh api "repos/${OWNER}/${REPO}/actions/workflows" --jq '.total_count' 2>/dev/null || true)
            if [[ "$workflow_count" == "0" ]]; then
                ci_configured="no"
            elif [[ "$workflow_count" =~ ^[0-9]+$ ]]; then
                ci_configured="yes"
            fi
        fi
        if [[ -z "$empty_runs_start" ]] || [[ "$empty_runs_sha" != "$pr_sha" ]]; then
            empty_runs_start=$(date +%s)
            empty_runs_sha="$pr_sha"
        fi
        if [[ "$ci_configured" == "no" && "$required_json" == "[]" ]]; then
            if [[ "$no_ci_noted" == false ]]; then
                echo "NOTE: no CI checks configured for ${GH_REPO} (0 workflows, no required contexts); treating checks as passed" >&2
                no_ci_noted=true
            fi
        elif [[ "$ci_configured" == "unknown" ]]; then
            # The workflow probe failed (permissions, network), so "no CI" is
            # unproven. Stay pending, never STABLE; the probe is retried next
            # poll and --max-seconds bounds the wait with a PENDING verdict.
            awaiting_registration=true
        elif [[ $(( $(date +%s) - empty_runs_start )) -lt "$RUN_REGISTRATION_GRACE_SECONDS" ]]; then
            awaiting_registration=true
        fi
    else
        empty_runs_start=""
    fi
    pending_checks=$(echo "$runs_json" \
        | jq --argjson required "$required_json" --argjson advisory "$advisory_json" \
            --argjson awaiting_registration "$awaiting_registration" '
            (group_by(.name) | map(sort_by(.createdAt) | last)) as $latest
            | if ($required | length) > 0
              then
                [ $required[] as $c
                  | ($latest | map(select(.name == $c)) | .[0]) as $run
                  | select(($run == null) or ($run.status != "completed"))
                ] | length
              elif ($latest | length) == 0
              then (if $awaiting_registration then 1 else 0 end)
              else
                [ $latest[]
                  | select(.status != "completed")
                  | .name as $n
                  | select(($advisory | index($n)) == null)
                ] | length
              end
        ')

    if [[ "$pending_checks" -gt 0 ]]; then
        # Reset stability timer while checks we care about are still running
        stability_start=""
        continue
    fi

    # All checks passing, no comments — track stability
    if [[ -z "$stability_start" ]]; then
        stability_start=$(date +%s)
    fi

    elapsed=$(( $(date +%s) - stability_start ))
    if [[ "$elapsed" -ge "$QUIET_SECONDS" ]]; then
        echo "STABLE"
        exit 0
    fi
done
