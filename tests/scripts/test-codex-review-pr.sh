#!/usr/bin/env bash
# ABOUTME: Unit tests for codex-review-pr.sh script.
# ABOUTME: Verifies arg/env handling and the pass-count guards (cap, same-head).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SCRIPT="$REPO_ROOT/scripts/codex-review-pr.sh"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}✓ $1${NC}"; }
fail() { echo -e "${RED}✗ $1${NC}"; FAILURES=$((FAILURES + 1)); }
info() { echo -e "${YELLOW}→ $1${NC}"; }

# Polls a shell condition until it holds or the deadline passes, instead of
# sleeping a fixed interval and hoping the detached worker finished. A fixed
# sleep sized for this machine is exactly the non-deterministic wait that makes
# a suite flaky on a loaded CI runner (testing-strategy.md "Deterministic Time").
# Usage: wait_for <timeout_seconds> '<condition>'
wait_for() {
    local timeout="$1" condition="$2"
    local attempts=$((timeout * 5)) i=0
    while ! eval "$condition"; do
        if [[ "$i" -ge "$attempts" ]]; then
            return 1
        fi
        sleep 0.2
        i=$((i + 1))
    done
    return 0
}

# Upper bounds on how long a detached worker may take, not how long anything
# sleeps: the script's wait_and_emit polls for $JOBDIR/result and returns the
# moment it appears, and wait_for above polls every 0.2s. A ceiling sized for
# an unloaded laptop is what makes this suite flaky on a shared self-hosted
# runner, where a fork + `git worktree add` + codex + POST cycle that takes
# ~4s here can take minutes. These values are deliberately far larger than
# any worker needs; raising them costs no wall time on a fast machine and
# only delays how quickly a genuine hang surfaces.
#
# Do NOT reuse these for a budget a test needs to be SMALL (a
# CODEX_REVIEW_WAIT_SECONDS=1 that must expire before the worker finishes, a
# JOB_INIT_TIMEOUT that must elapse, a WORKER_TIMEOUT that must fire). Those
# are lower bounds and must stay as they are.
INVOCATION_TIMEOUT=180   # outer guard on one `bash "$SCRIPT"` invocation
ARG_TIMEOUT=60           # outer guard on arg-validation runs that never fork a worker
JOIN_WAIT=120            # CODEX_REVIEW_WAIT_SECONDS where a join must succeed
# The racing test below is the one place CODEX_REVIEW_WAIT_SECONDS is NOT a
# ceiling on a polling wait. Whichever racer loses keeps waiting on a job dir
# the winner has already consumed and removed: job_owner_alive() treats a
# vanished dir as "a sibling resolved this, keep waiting" rather than dead, so
# the loser spins out the whole budget before emitting REVIEW_PENDING. It is
# therefore paid in full, as wall time, on every run — $JOIN_WAIT here would
# add ~100s to the suite. Sized to be comfortably longer than a loaded runner
# needs to finish a 2s-codex worker, and no longer.
RACE_JOIN_WAIT=60        # CODEX_REVIEW_WAIT_SECONDS for the racers (see above)
POLL_DEADLINE=120        # wait_for deadline for a worker-side side effect
PROC_EXIT_DEADLINE=60    # wait_for deadline for a signalled process to die

FAILURES=0
MOCK_DIR=""
REPO_DIR=""

cleanup() {
    # Kill any detached review worker still running under this test's state
    # dir before removing it, so a slow/lingering test never orphans a
    # background process. A worker killed via SIGKILL (the "dead worker" test)
    # never runs its own EXIT trap, so its worktree is reaped here too rather
    # than left behind under /tmp.
    if [ -n "$MOCK_DIR" ] && [ -d "$MOCK_DIR/state" ]; then
        for jobdir in "$MOCK_DIR"/state/*/; do
            [[ -d "$jobdir" ]] || continue
            if [[ -f "${jobdir}pid" ]]; then
                pid=$(cat "${jobdir}pid" 2>/dev/null || true)
                if [[ -n "$pid" ]]; then
                    kill -9 "$pid" 2>/dev/null || true
                fi
            fi
            if [[ -f "${jobdir}env" ]]; then
                worktree=$(bash -c "source '${jobdir}env' 2>/dev/null; printf '%s' \"\$WORKTREE\"" 2>/dev/null || true)
                if [[ -n "$worktree" && -d "$worktree" ]]; then
                    rm -rf "$worktree"
                fi
            fi
        done
    fi
    if [ -n "$MOCK_DIR" ] && [ -d "$MOCK_DIR" ]; then
        rm -rf "$MOCK_DIR"
    fi
    if [ -n "$REPO_DIR" ] && [ -d "$REPO_DIR" ]; then
        rm -rf "$REPO_DIR"
    fi
}
trap cleanup EXIT

CURRENT_HEAD_SHA=""

# The script targets $GH_REPO (default MrsBohemian/Cholobots); point it at the
# mock's repo. GH_TOKEN is set so the mock can prove every gh call drops it.
export GH_REPO="TestOwner/TestRepo"
export GH_TOKEN="leaked-token"

# Create a mock gh command that returns canned responses from files in MOCK_DIR.
# Must run after setup_repo, since pr_view.json needs the real head SHA that
# setup_repo created (the script now runs `git worktree add` against it).
setup_mock_gh() {
    MOCK_DIR=$(mktemp -d "/tmp/test-codex-review-pr-XXXXXX")

    cat > "$MOCK_DIR/gh" << MOCKSCRIPT
#!/usr/bin/env bash
# Mock gh command that returns canned responses from MOCK_DIR files
echo "\${GH_TOKEN-unset}" >> "\$MOCK_DIR/gh_token_seen.log"
case "\$*" in
    "repo view --json nameWithOwner -q .nameWithOwner")
        echo "TestOwner/TestRepo"
        ;;
    "pr view "*"--json headRefOid,baseRefName,title"*)
        cat "\$MOCK_DIR/pr_view.json"
        ;;
    "api repos/TestOwner/TestRepo/pulls/"*"/reviews --paginate --jq .[].body")
        if [[ -f "\$MOCK_DIR/reviews.txt" ]]; then
            cat "\$MOCK_DIR/reviews.txt"
        fi
        ;;
    *"-X POST"*)
        echo "\$*" >> "\$MOCK_DIR/post_calls.log"
        prev=""
        for arg in "\$@"; do
            if [[ "\$prev" == "--input" ]]; then
                cp "\$arg" "\$MOCK_DIR/last_payload.json"
            fi
            prev="\$arg"
        done
        ;;
    *)
        echo "MOCK_ERROR: unexpected gh call: \$*" >&2
        exit 1
        ;;
esac
MOCKSCRIPT
    chmod +x "$MOCK_DIR/gh"

    # Mock codex CLI: logs its argv and cwd, drains stdin (the prompt) so the
    # caller's printf never blocks, then writes a canned findings JSON (or a
    # configured failure) to the -o output path.
    cat > "$MOCK_DIR/codex" << MOCKSCRIPT
#!/usr/bin/env bash
echo "\$@" >> "\$MOCK_DIR/codex_calls.log"
pwd >> "\$MOCK_DIR/codex_cwd.log"
cat > "\$MOCK_DIR/codex_stdin.log"

if [[ -f "\$MOCK_DIR/codex_exit_code" ]]; then
    exit "\$(cat "\$MOCK_DIR/codex_exit_code")"
fi

if [[ -f "\$MOCK_DIR/codex_sleep" ]]; then
    sleep "\$(cat "\$MOCK_DIR/codex_sleep")"
fi

OUT=""
prev=""
for arg in "\$@"; do
    if [[ "\$prev" == "-o" ]]; then
        OUT="\$arg"
    fi
    prev="\$arg"
done

if [[ -f "\$MOCK_DIR/codex_output_body" ]]; then
    sed "s#WORKTREE_PLACEHOLDER#\$(pwd)#g" "\$MOCK_DIR/codex_output_body" > "\$OUT"
else
    cat > "\$OUT" << 'JSONEOF'
{"review": {"findings": [], "overall_correctness": "patch is correct", "overall_explanation": "no issues found", "overall_confidence_score": 1}}
JSONEOF
fi
MOCKSCRIPT
    chmod +x "$MOCK_DIR/codex"

    cat > "$MOCK_DIR/pr_view.json" << EOF
{"headRefOid":"$CURRENT_HEAD_SHA","baseRefName":"main","title":"Example PR"}
EOF
}

# Write the list of prior review bodies (one gh --jq '.[].body' line per review).
write_reviews() {
    printf '%s\n' "$@" > "$MOCK_DIR/reviews.txt"
}

# Installs a mock `git` in MOCK_DIR that sleeps for $1 seconds before every
# `git fetch ...` call, then delegates to the real git for that call and
# every other subcommand. Used to hold the script inside its unbounded fetch
# step long enough to exercise the job-dir heartbeat. Must run before the
# real git binary is resolved via a MOCK_DIR-prefixed PATH, so it captures
# the real git's path up front while MOCK_DIR is not yet on PATH.
setup_mock_git_fetch_delay() {
    local sleep_seconds="$1" real_git
    real_git="$(command -v git)"
    cat > "$MOCK_DIR/git" << MOCKSCRIPT
#!/usr/bin/env bash
if [[ "\$1" == "fetch" ]]; then
    sleep "$sleep_seconds"
fi
exec "$real_git" "\$@"
MOCKSCRIPT
    chmod +x "$MOCK_DIR/git"
}

# Wrap the native review shape read from stdin under the "review" key the
# script's prompt demands, then hand it to the mock codex.
write_codex_review_output() {
    jq '{review: .}' > "$MOCK_DIR/codex_output_body"
}

# Build a real temp git repo with a base commit on main, a feature/example
# branch with a head commit changing a known file, and an "upstream" remote
# pointing at itself, so `git fetch upstream` and `git worktree add` work for
# real against a real head SHA.
#
# Two files are changed on the head commit:
#   known-file.txt — 2 lines, both inside the diff (reviewable lines 1-2)
#   wide-file.txt  — 20 lines with only line 15 changed, so the reviewable
#                    window is lines 12-18 and lines outside it (e.g. 2, 150)
#                    give the tests natural out-of-diff line numbers.
#   spaced dir/spaced file.txt — a path containing spaces, which git emits
#                    with a trailing TAB terminator in the diff header.
setup_repo() {
    REPO_DIR=$(mktemp -d "/tmp/test-codex-review-pr-repo-XXXXXX")
    git -C "$REPO_DIR" init -q -b main
    git -C "$REPO_DIR" config user.email "test@example.com"
    git -C "$REPO_DIR" config user.name "Test User"
    git -C "$REPO_DIR" remote add upstream "$REPO_DIR"
    echo "base content" > "$REPO_DIR/known-file.txt"
    seq 1 20 | sed 's/^/line /' > "$REPO_DIR/wide-file.txt"
    mkdir -p "$REPO_DIR/spaced dir"
    echo "base content" > "$REPO_DIR/spaced dir/spaced file.txt"
    git -C "$REPO_DIR" add known-file.txt wide-file.txt "spaced dir/spaced file.txt"
    git -C "$REPO_DIR" commit -q -m "base commit"
    git -C "$REPO_DIR" checkout -q -b feature/example
    echo "changed content" >> "$REPO_DIR/known-file.txt"
    sed -i '15s/.*/line 15 changed/' "$REPO_DIR/wide-file.txt"
    echo "changed content" >> "$REPO_DIR/spaced dir/spaced file.txt"
    git -C "$REPO_DIR" commit -aq -m "head commit"
    git -C "$REPO_DIR" checkout -q main
    CURRENT_HEAD_SHA=$(git -C "$REPO_DIR" rev-parse feature/example)
    # Serve refs/pull/42/head from the self-remote, mirroring what GitHub
    # exposes for both open and merged PRs (even after the head branch
    # is deleted).
    git -C "$REPO_DIR" update-ref refs/pull/42/head feature/example
}

run_review() {
    PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_STATE_DIR="$MOCK_DIR/state" \
        timeout "$INVOCATION_TIMEOUT" bash "$SCRIPT" 42 "$@"
}

# --- Test 1: cap reached ---
info "Test 1: cap reached skips the review"
setup_repo
setup_mock_gh
write_reviews \
    "<!-- codex-review pass=1 head=aaa111 model=gpt-5.6-luna -->" \
    "<!-- codex-review pass=2 head=bbb222 model=gpt-5.6-luna -->" \
    "<!-- codex-review pass=3 head=ccc333 model=gpt-5.6-luna -->"
output=$(cd "$REPO_DIR" && run_review)
if [[ "$output" == *"REVIEW_SKIPPED:cap"* ]]; then
    pass "Cap reached, prints REVIEW_SKIPPED:cap"
else
    fail "Expected REVIEW_SKIPPED:cap, got: $output"
fi
if [[ -f "$MOCK_DIR/codex_calls.log" ]]; then
    fail "codex should not have been invoked when cap reached"
else
    pass "codex not invoked when cap reached"
fi
if [[ -f "$MOCK_DIR/post_calls.log" ]]; then
    fail "no POST should have been made when cap reached"
else
    pass "no POST made when cap reached"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test 2: same head ---
info "Test 2: same head skips the review"
setup_repo
setup_mock_gh
write_reviews \
    "<!-- codex-review pass=1 head=$CURRENT_HEAD_SHA model=gpt-5.6-luna -->"
output=$(cd "$REPO_DIR" && run_review)
if [[ "$output" == *"REVIEW_SKIPPED:same-head"* ]]; then
    pass "Same head, prints REVIEW_SKIPPED:same-head"
else
    fail "Expected REVIEW_SKIPPED:same-head, got: $output"
fi
if [[ -f "$MOCK_DIR/codex_calls.log" ]]; then
    fail "codex should not have been invoked when same head"
else
    pass "codex not invoked when same head"
fi
if [[ -f "$MOCK_DIR/post_calls.log" ]]; then
    fail "no POST should have been made when same head"
else
    pass "no POST made when same head"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test 3: two in-diff findings -> two inline comments, correct counts ---
info "Test 3: two in-diff P0/P1 findings produce two inline comments"
setup_repo
setup_mock_gh
write_codex_review_output << 'JSONEOF'
{
  "findings": [
    {"title": "[P0] Bug one", "body": "Fix desc one", "confidence_score": 0.9, "priority": 0, "code_location": {"absolute_file_path": "WORKTREE_PLACEHOLDER/known-file.txt", "line_range": {"start": 1, "end": 1}}},
    {"title": "[P1] Bug two", "body": "Fix desc two", "confidence_score": 0.8, "priority": 1, "code_location": {"absolute_file_path": "WORKTREE_PLACEHOLDER/known-file.txt", "line_range": {"start": 2, "end": 2}}}
  ],
  "overall_correctness": "patch has issues",
  "overall_explanation": "two issues found",
  "overall_confidence_score": 0.85
}
JSONEOF
output=$(cd "$REPO_DIR" && run_review)
if [[ -f "$MOCK_DIR/last_payload.json" ]]; then
    pass "payload captured"
else
    fail "no payload captured"
fi
if jq -e . "$MOCK_DIR/last_payload.json" >/dev/null 2>&1; then
    pass "payload is valid JSON"
else
    fail "payload is not valid JSON"
fi
comments_count=$(jq '.comments | length' "$MOCK_DIR/last_payload.json")
if [[ "$comments_count" == "2" ]]; then
    pass "two inline comments emitted"
else
    fail "Expected 2 comments, got: $comments_count"
fi
comment1_ok=$(jq -e '.comments[] | select(.path == "known-file.txt" and .line == 1 and .side == "RIGHT")' "$MOCK_DIR/last_payload.json" >/dev/null 2>&1 && echo yes || echo no)
comment2_ok=$(jq -e '.comments[] | select(.path == "known-file.txt" and .line == 2 and .side == "RIGHT")' "$MOCK_DIR/last_payload.json" >/dev/null 2>&1 && echo yes || echo no)
if [[ "$comment1_ok" == "yes" && "$comment2_ok" == "yes" ]]; then
    pass "comments carry stripped worktree path, correct line, side RIGHT"
else
    fail "Expected comments for known-file.txt:1 and known-file.txt:2, got: $(jq -c '.comments' "$MOCK_DIR/last_payload.json")"
fi
comment1_body=$(jq -r '.comments[] | select(.line == 1) | .body' "$MOCK_DIR/last_payload.json")
comment2_body=$(jq -r '.comments[] | select(.line == 2) | .body' "$MOCK_DIR/last_payload.json")
if [[ "$comment1_body" == *"[P0] Bug one"* && "$comment1_body" == *"Fix desc one"* \
    && "$comment2_body" == *"[P1] Bug two"* && "$comment2_body" == *"Fix desc two"* ]]; then
    pass "comment bodies carry the finding title and description"
else
    fail "Expected finding title+body in each comment, got: $comment1_body / $comment2_body"
fi
commit_id=$(jq -r '.commit_id' "$MOCK_DIR/last_payload.json")
if [[ "$commit_id" == "$CURRENT_HEAD_SHA" ]]; then
    pass "payload commit_id is the PR head SHA"
else
    fail "Expected commit_id=$CURRENT_HEAD_SHA, got: $commit_id"
fi
body_marker=$(jq -r '.body' "$MOCK_DIR/last_payload.json")
if [[ "$body_marker" == *"pass=1 head=$CURRENT_HEAD_SHA model="* ]]; then
    pass "body carries marker with pass=1 and head SHA"
else
    fail "Expected marker with pass=1 head=$CURRENT_HEAD_SHA, got: $body_marker"
fi
if [[ "$output" == *"REVIEW_POSTED:1/3:2:0"* ]]; then
    pass "last line REVIEW_POSTED:1/3:2:0"
else
    fail "Expected REVIEW_POSTED:1/3:2:0, got: $output"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test 4: in-diff P3 and out-of-diff P0 both go to body ---
info "Test 4: in-diff P3 and out-of-diff P0 findings both go to the body"
setup_repo
setup_mock_gh
write_codex_review_output << 'JSONEOF'
{
  "findings": [
    {"title": "[P3] Nit", "body": "Minor style nit", "confidence_score": 0.5, "priority": 3, "code_location": {"absolute_file_path": "WORKTREE_PLACEHOLDER/known-file.txt", "line_range": {"start": 1, "end": 1}}},
    {"title": "[P0] Out of diff", "body": "Serious but out of diff", "confidence_score": 0.9, "priority": 0, "code_location": {"absolute_file_path": "WORKTREE_PLACEHOLDER/known-file.txt", "line_range": {"start": 99, "end": 99}}}
  ],
  "overall_correctness": "patch has issues",
  "overall_explanation": "two issues found, neither inline-eligible",
  "overall_confidence_score": 0.7
}
JSONEOF
output=$(cd "$REPO_DIR" && run_review)
comments_count=$(jq '.comments | length' "$MOCK_DIR/last_payload.json")
if [[ "$comments_count" == "0" ]]; then
    pass "no inline comments emitted"
else
    fail "Expected 0 comments, got: $comments_count"
fi
body_text=$(jq -r '.body' "$MOCK_DIR/last_payload.json")
if [[ "$body_text" == *"known-file.txt:1"* && "$body_text" == *"known-file.txt:99"* ]]; then
    pass "both findings listed in body with path:line"
else
    fail "Expected both known-file.txt:1 and known-file.txt:99 in body, got: $body_text"
fi
if [[ "$output" == *"REVIEW_POSTED:1/3:0:2"* ]]; then
    pass "last line REVIEW_POSTED:1/3:0:2"
else
    fail "Expected REVIEW_POSTED:1/3:0:2, got: $output"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test 5: zero findings still posts a body-only review ---
info "Test 5: zero findings still posts a body-only review"
setup_repo
setup_mock_gh
output=$(cd "$REPO_DIR" && run_review)
if [[ -f "$MOCK_DIR/post_calls.log" ]]; then
    pass "POST made even with zero findings"
else
    fail "Expected a POST for a zero-finding pass"
fi
comments_count=$(jq '.comments | length' "$MOCK_DIR/last_payload.json")
if [[ "$comments_count" == "0" ]]; then
    pass "zero comments for zero findings"
else
    fail "Expected 0 comments, got: $comments_count"
fi
if [[ "$output" == *"REVIEW_POSTED:1/3:0:0"* ]]; then
    pass "last line REVIEW_POSTED:1/3:0:0"
else
    fail "Expected REVIEW_POSTED:1/3:0:0, got: $output"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test 6: codex non-zero exit -> exit 1, no POST ---
info "Test 6: codex non-zero exit fails the run without posting"
setup_repo
setup_mock_gh
echo 1 > "$MOCK_DIR/codex_exit_code"
set +e
output=$(cd "$REPO_DIR" && run_review 2>&1)
status=$?
set -e
if [[ $status -ne 0 ]]; then
    pass "codex non-zero exit -> non-zero script exit"
else
    fail "Expected non-zero exit when codex fails, got status=$status"
fi
if [[ "$output" == *"codex exec review failed"* ]]; then
    pass "codex non-zero exit -> stderr message"
else
    fail "Expected a codex failure message, got: $output"
fi
if [[ -f "$MOCK_DIR/post_calls.log" ]]; then
    fail "no POST should have been made when codex fails"
else
    pass "no POST made when codex fails"
fi
argv=$(cat "$MOCK_DIR/codex_calls.log")
prefix="${argv%%exec review*}"
suffix="${argv#*exec review}"
if [[ "$prefix" == *"-m "* && "$prefix" == *"-s "* && "$prefix" == *"-c "* ]]; then
    pass "codex argv: -m/-s/-c precede exec review"
else
    fail "Expected -m/-s/-c before exec review, got: $argv"
fi
if [[ "$suffix" == *"--ephemeral"* && "$suffix" == *"--ignore-user-config"* && "$suffix" == *"--ignore-rules"* && "$suffix" == *"-o "* ]]; then
    pass "codex argv: --ephemeral/--ignore-user-config/--ignore-rules/-o follow exec review"
else
    fail "Expected --ephemeral/--ignore-user-config/--ignore-rules/-o after exec review, got: $argv"
fi
if [[ "$suffix" != *"--base"* ]]; then
    pass "codex argv: no --base (mutually exclusive with the positional prompt)"
else
    fail "Expected no --base alongside the positional prompt, got: $argv"
fi
if [[ "$argv" == *" -" ]]; then
    pass "codex argv: positional stdin prompt (trailing -)"
else
    fail "Expected a trailing positional - for the stdin prompt, got: $argv"
fi
stdin_prompt=$(cat "$MOCK_DIR/codex_stdin.log" 2>/dev/null || true)
if [[ -n "$stdin_prompt" && "$stdin_prompt" == *"git diff upstream/main...HEAD"* && "$stdin_prompt" == *'"review"'* ]]; then
    pass "codex stdin: prompt names upstream/main and the review wrapper key"
else
    fail "Expected the stdin prompt to name git diff upstream/main...HEAD and the \"review\" key, got: $stdin_prompt"
fi
if [[ "$stdin_prompt" == *"discord.py"* && "$stdin_prompt" == *"Supabase"* && "$stdin_prompt" != *"AGENTS.md"* ]]; then
    pass "codex stdin: prompt describes the Python discord.py + Supabase stack"
else
    fail "Expected the prompt to describe discord.py + Supabase and not cite AGENTS.md, got: $stdin_prompt"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test 7: invalid/unparseable JSON, and JSON missing findings -> exit 1, no POST ---
info "Test 7a: unparseable codex output fails the run without posting"
setup_repo
setup_mock_gh
echo 'not json at all' > "$MOCK_DIR/codex_output_body"
set +e
output=$(cd "$REPO_DIR" && run_review 2>&1)
status=$?
set -e
if [[ $status -ne 0 ]]; then
    pass "unparseable codex output -> non-zero script exit"
else
    fail "Expected non-zero exit for unparseable codex output, got status=$status"
fi
if [[ -f "$MOCK_DIR/post_calls.log" ]]; then
    fail "no POST should have been made for unparseable codex output"
else
    pass "no POST made for unparseable codex output"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

info "Test 7b: codex output wrapper missing 'review.findings' fails the run without posting"
setup_repo
setup_mock_gh
echo '{"review": {"overall_correctness": "patch is correct"}}' > "$MOCK_DIR/codex_output_body"
set +e
output=$(cd "$REPO_DIR" && run_review 2>&1)
status=$?
set -e
if [[ $status -ne 0 ]]; then
    pass "missing 'review.findings' -> non-zero script exit"
else
    fail "Expected non-zero exit when 'review.findings' is missing, got status=$status"
fi
if [[ -f "$MOCK_DIR/post_calls.log" ]]; then
    fail "no POST should have been made when 'review.findings' is missing"
else
    pass "no POST made when 'review.findings' is missing"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# Codex re-renders a final message shaped like its own ReviewOutputEvent into
# prose, so a top-level native shape can only mean the wrapper contract was
# not honoured; it must be rejected rather than posted.
info "Test 7c: unwrapped native review shape fails the run without posting"
setup_repo
setup_mock_gh
echo '{"findings": [], "overall_correctness": "patch is correct", "overall_explanation": "no issues found", "overall_confidence_score": 1}' > "$MOCK_DIR/codex_output_body"
set +e
output=$(cd "$REPO_DIR" && run_review 2>&1)
status=$?
set -e
if [[ $status -ne 0 ]]; then
    pass "unwrapped native shape -> non-zero script exit"
else
    fail "Expected non-zero exit for an unwrapped native shape, got status=$status"
fi
if [[ -f "$MOCK_DIR/post_calls.log" ]]; then
    fail "no POST should have been made for an unwrapped native shape"
else
    pass "no POST made for an unwrapped native shape"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test 8: --dry-run prints the payload and never POSTs ---
info "Test 8: --dry-run prints the payload without posting"
setup_repo
setup_mock_gh
set +e
output=$(cd "$REPO_DIR" && run_review --dry-run)
status=$?
set -e
if [[ $status -eq 0 ]]; then
    pass "--dry-run exits 0"
else
    fail "Expected exit 0 for --dry-run, got status=$status"
fi
payload_only=$(printf '%s\n' "$output" | sed '$d')
if printf '%s' "$payload_only" | jq -e . >/dev/null 2>&1; then
    pass "--dry-run prints valid JSON payload to stdout"
else
    fail "Expected valid JSON payload on stdout, got: $payload_only"
fi
last_line=$(printf '%s\n' "$output" | tail -1)
if [[ "$last_line" =~ ^REVIEW_DRYRUN:1/3:[0-9]+:[0-9]+$ ]]; then
    pass "--dry-run last stdout line is the REVIEW_DRYRUN contract line"
else
    fail "Expected last line to match REVIEW_DRYRUN:<pass>/<max>:<inline>:<body_only>, got: $last_line"
fi
if [[ -f "$MOCK_DIR/post_calls.log" ]]; then
    fail "no POST should have been made in --dry-run"
else
    pass "no POST made in --dry-run"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test: --dry-run and a live run for the same PR/head do not join each
# other's job (the job key must include DRY_RUN) ---
info "Test: --dry-run and a live run use separate job dirs"
setup_repo
setup_mock_gh
echo 3 > "$MOCK_DIR/codex_sleep"
live_output=$(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_STATE_DIR="$MOCK_DIR/state" \
    CODEX_REVIEW_WAIT_SECONDS=1 timeout "$INVOCATION_TIMEOUT" bash "$SCRIPT" 42)
live_last_line=$(printf '%s\n' "$live_output" | tail -1)
if [[ "$live_last_line" == "REVIEW_PENDING:1/3" ]]; then
    pass "live call reports REVIEW_PENDING:1/3 and leaves its worker running"
else
    fail "Expected REVIEW_PENDING:1/3 for the live call, got: $live_output"
fi
# A low wait budget here too (rather than run_review's default 540s) keeps
# this call from waiting out its own worker's sleep and cleaning its job dir
# up before the assertion below can see both job dirs at once.
dryrun_output=$(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_STATE_DIR="$MOCK_DIR/state" \
    CODEX_REVIEW_WAIT_SECONDS=1 timeout "$INVOCATION_TIMEOUT" bash "$SCRIPT" 42 --dry-run)
dryrun_last_line=$(printf '%s\n' "$dryrun_output" | tail -1)
if [[ "$dryrun_last_line" == "REVIEW_PENDING:1/3" ]]; then
    pass "concurrent --dry-run starts its own job instead of joining the live worker"
else
    fail "Expected the --dry-run call to report REVIEW_PENDING:1/3 on its own job, got: $dryrun_output"
fi
job_dirs=$(find "$MOCK_DIR/state" -maxdepth 1 -type d -name 'pr-*' | wc -l)
if [[ "$job_dirs" -eq 2 ]]; then
    pass "dry-run and live jobs get separate job dirs"
else
    fail "Expected 2 separate job dirs (dry-run and live), got: $job_dirs"
fi
if wait_for "$POLL_DEADLINE" '[[ "$(wc -l < "$MOCK_DIR/post_calls.log" 2>/dev/null || echo 0)" -eq 1 ]]'; then
    pass "the live worker posts exactly once; the dry-run job never posts"
else
    fail "Expected exactly 1 POST (from the live job only), got: $(cat "$MOCK_DIR/post_calls.log" 2>/dev/null | wc -l)"
fi
wait_for "$POLL_DEADLINE" '[[ "$(git -C "$REPO_DIR" worktree list | wc -l)" -eq 1 ]]' || fail "Expected both workers' worktrees to be cleaned up"
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test 9: worktree and temp dir cleanup on the failure path ---
info "Test 9: worktree and temp dir are removed after a failure"
setup_repo
setup_mock_gh
echo 1 > "$MOCK_DIR/codex_exit_code"
(cd "$REPO_DIR" && run_review >/dev/null 2>&1) || true
worktree_count=$(git -C "$REPO_DIR" worktree list | wc -l)
if [[ "$worktree_count" -eq 1 ]]; then
    pass "git worktree list shows no leftover worktree"
else
    fail "Expected only the main worktree, got: $(git -C "$REPO_DIR" worktree list)"
fi
codex_cwd=$(cat "$MOCK_DIR/codex_cwd.log")
if [[ ! -d "$codex_cwd" ]]; then
    pass "temp worktree directory removed after failure"
else
    fail "Expected temp worktree directory '$codex_cwd' to be removed"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test 10: pass numbering guard — one prior marker at a different head does NOT skip ---
info "Test 10: one prior marker at a different head does not skip"
setup_repo
setup_mock_gh
write_reviews \
    "<!-- codex-review pass=1 head=aaa111 model=gpt-5.6-luna -->"
output=$(cd "$REPO_DIR" && run_review || true)
if [[ "$output" == *"REVIEW_SKIPPED:cap"* || "$output" == *"REVIEW_SKIPPED:same-head"* ]]; then
    fail "Should not have been skipped, got: $output"
else
    pass "Not skipped when only one prior marker at a different head"
fi
if [[ -f "$MOCK_DIR/last_payload.json" ]]; then
    body_marker=$(jq -r '.body' "$MOCK_DIR/last_payload.json")
    if [[ "$body_marker" == *"pass=2 head=$CURRENT_HEAD_SHA model="* ]]; then
        pass "body says pass=2 for the second pass"
    else
        fail "Expected marker with pass=2 head=$CURRENT_HEAD_SHA, got: $body_marker"
    fi
else
    fail "Expected a payload to have been posted"
fi
if [[ "$output" == *"REVIEW_POSTED:2/3:"* ]]; then
    pass "last line reports REVIEW_POSTED:2/3"
else
    fail "Expected REVIEW_POSTED:2/3:..., got: $output"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test 11: CODEX_REVIEW_MAX_PASSES=1 honoured ---
info "Test 11: CODEX_REVIEW_MAX_PASSES=1 honoured"
setup_repo
setup_mock_gh
write_reviews \
    "<!-- codex-review pass=1 head=aaa111 model=gpt-5.6-luna -->"
output=$(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_MAX_PASSES=1 timeout "$INVOCATION_TIMEOUT" bash "$SCRIPT" 42)
if [[ "$output" == *"REVIEW_SKIPPED:cap"* ]]; then
    pass "CODEX_REVIEW_MAX_PASSES=1 honoured, prints REVIEW_SKIPPED:cap"
else
    fail "Expected REVIEW_SKIPPED:cap, got: $output"
fi
if [[ -f "$MOCK_DIR/codex_calls.log" ]]; then
    fail "codex should not have been invoked when cap=1 reached"
else
    pass "codex not invoked when cap=1 reached"
fi
if [[ -f "$MOCK_DIR/post_calls.log" ]]; then
    fail "no POST should have been made when cap=1 reached"
else
    pass "no POST made when cap=1 reached"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test 12: non-integer/missing priorities are demoted, not fatal ---
info "Test 12: non-integer and missing priorities demote to body-only without crashing"
setup_repo
setup_mock_gh
write_codex_review_output << 'JSONEOF'
{
  "findings": [
    {"title": "[P0] Real bug", "body": "Genuine issue", "confidence_score": 0.9, "priority": 0, "code_location": {"absolute_file_path": "WORKTREE_PLACEHOLDER/known-file.txt", "line_range": {"start": 1, "end": 1}}},
    {"title": "[P1] String priority", "body": "Priority emitted as a string", "confidence_score": 0.8, "priority": "P1", "code_location": {"absolute_file_path": "WORKTREE_PLACEHOLDER/known-file.txt", "line_range": {"start": 2, "end": 2}}},
    {"title": "No priority", "body": "Priority key omitted entirely", "confidence_score": 0.7, "code_location": {"absolute_file_path": "WORKTREE_PLACEHOLDER/known-file.txt", "line_range": {"start": 2, "end": 2}}}
  ],
  "overall_correctness": "patch has issues",
  "overall_explanation": "mixed priority shapes",
  "overall_confidence_score": 0.6
}
JSONEOF
set +e
output=$(cd "$REPO_DIR" && run_review 2>&1)
status=$?
set -e
if [[ $status -eq 0 ]]; then
    pass "script exits 0 despite a string priority"
else
    fail "Expected exit 0, got status=$status output=$output"
fi
if [[ "$output" != *"unbound variable"* ]]; then
    pass "no bash unbound variable error"
else
    fail "Expected no unbound variable error, got: $output"
fi
if [[ -f "$MOCK_DIR/last_payload.json" ]]; then
    pass "payload still captured with a string priority present"
else
    fail "no payload captured"
fi
comments_count=$(jq '.comments | length' "$MOCK_DIR/last_payload.json")
if [[ "$comments_count" == "1" ]]; then
    pass "only the integer-priority finding is inline"
else
    fail "Expected 1 comment, got: $comments_count"
fi
body_text=$(jq -r '.body' "$MOCK_DIR/last_payload.json")
if [[ "$body_text" == *"String priority"* && "$body_text" == *"No priority"* ]]; then
    pass "string-priority and missing-priority findings demoted to the body"
else
    fail "Expected both non-integer-priority findings in body, got: $body_text"
fi
if [[ "$output" == *"REVIEW_POSTED:1/3:1:2"* ]]; then
    pass "last line REVIEW_POSTED:1/3:1:2"
else
    fail "Expected REVIEW_POSTED:1/3:1:2, got: $output"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test 13: multi-line range fully inside the diff keeps start_line ---
info "Test 13: multi-line finding fully inside the diff emits start_line"
setup_repo
setup_mock_gh
write_codex_review_output << 'JSONEOF'
{
  "findings": [
    {"title": "[P0] Spans a valid range", "body": "Both ends are in the diff", "confidence_score": 0.9, "priority": 0, "code_location": {"absolute_file_path": "WORKTREE_PLACEHOLDER/wide-file.txt", "line_range": {"start": 13, "end": 15}}}
  ],
  "overall_correctness": "patch has issues",
  "overall_explanation": "one multi-line issue",
  "overall_confidence_score": 0.9
}
JSONEOF
output=$(cd "$REPO_DIR" && run_review)
if jq -e '.comments[0] | select(.path == "wide-file.txt" and .line == 15 and .start_line == 13 and .side == "RIGHT" and .start_side == "RIGHT")' "$MOCK_DIR/last_payload.json" >/dev/null 2>&1; then
    pass "in-diff multi-line finding keeps start_line=13 line=15"
else
    fail "Expected a multi-line comment on wide-file.txt 13-15, got: $(jq -c '.comments' "$MOCK_DIR/last_payload.json")"
fi
if [[ "$output" == *"REVIEW_POSTED:1/3:1:0"* ]]; then
    pass "last line REVIEW_POSTED:1/3:1:0"
else
    fail "Expected REVIEW_POSTED:1/3:1:0, got: $output"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test 14: multi-line range whose start is outside the diff collapses ---
info "Test 14: multi-line finding with an out-of-diff start collapses to the end line"
setup_repo
setup_mock_gh
write_codex_review_output << 'JSONEOF'
{
  "findings": [
    {"title": "[P0] Start outside the diff", "body": "Start line 2 is not reviewable", "confidence_score": 0.9, "priority": 0, "code_location": {"absolute_file_path": "WORKTREE_PLACEHOLDER/wide-file.txt", "line_range": {"start": 2, "end": 15}}}
  ],
  "overall_correctness": "patch has issues",
  "overall_explanation": "one issue with an out-of-diff start",
  "overall_confidence_score": 0.9
}
JSONEOF
output=$(cd "$REPO_DIR" && run_review)
if jq -e '.comments[0] | select(.path == "wide-file.txt" and .line == 15 and (has("start_line") | not))' "$MOCK_DIR/last_payload.json" >/dev/null 2>&1; then
    pass "out-of-diff start collapses to a single-line comment on line 15"
else
    fail "Expected a single-line comment on wide-file.txt:15 with no start_line, got: $(jq -c '.comments' "$MOCK_DIR/last_payload.json")"
fi
if [[ "$output" == *"REVIEW_POSTED:1/3:1:0"* ]]; then
    pass "last line REVIEW_POSTED:1/3:1:0"
else
    fail "Expected REVIEW_POSTED:1/3:1:0, got: $output"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test 15: inverted range (start > end) collapses to the end line ---
info "Test 15: inverted range start > end collapses to the end line"
setup_repo
setup_mock_gh
write_codex_review_output << 'JSONEOF'
{
  "findings": [
    {"title": "[P0] Inverted range", "body": "Start 150 is greater than end 15", "confidence_score": 0.9, "priority": 0, "code_location": {"absolute_file_path": "WORKTREE_PLACEHOLDER/wide-file.txt", "line_range": {"start": 150, "end": 15}}},
    {"title": "[P0] Null start", "body": "Start emitted as null", "confidence_score": 0.9, "priority": 0, "code_location": {"absolute_file_path": "WORKTREE_PLACEHOLDER/wide-file.txt", "line_range": {"start": null, "end": 14}}}
  ],
  "overall_correctness": "patch has issues",
  "overall_explanation": "two issues with unusable start lines",
  "overall_confidence_score": 0.9
}
JSONEOF
set +e
output=$(cd "$REPO_DIR" && run_review 2>&1)
status=$?
set -e
if [[ $status -eq 0 ]]; then
    pass "script exits 0 for inverted and null start lines"
else
    fail "Expected exit 0, got status=$status output=$output"
fi
if jq -e '.comments | length == 2 and all(.[]; has("start_line") | not)' "$MOCK_DIR/last_payload.json" >/dev/null 2>&1; then
    pass "inverted and null starts both collapse to single-line comments"
else
    fail "Expected 2 single-line comments with no start_line, got: $(jq -c '.comments' "$MOCK_DIR/last_payload.json")"
fi
if jq -e '[.comments[].line] | sort == [14, 15]' "$MOCK_DIR/last_payload.json" >/dev/null 2>&1; then
    pass "collapsed comments anchor on their end lines (14, 15)"
else
    fail "Expected comment lines 14 and 15, got: $(jq -c '[.comments[].line]' "$MOCK_DIR/last_payload.json")"
fi
if [[ "$output" == *"REVIEW_POSTED:1/3:2:0"* ]]; then
    pass "last line REVIEW_POSTED:1/3:2:0"
else
    fail "Expected REVIEW_POSTED:1/3:2:0, got: $output"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test 16: in-diff finding on a path containing spaces stays inline ---
info "Test 16: finding on a path with spaces produces an inline comment"
setup_repo
setup_mock_gh
write_codex_review_output << 'JSONEOF'
{
  "findings": [
    {"title": "[P0] Spaced path bug", "body": "Path contains spaces", "confidence_score": 0.9, "priority": 0, "code_location": {"absolute_file_path": "WORKTREE_PLACEHOLDER/spaced dir/spaced file.txt", "line_range": {"start": 2, "end": 2}}}
  ],
  "overall_correctness": "patch has issues",
  "overall_explanation": "one issue on a spaced path",
  "overall_confidence_score": 0.9
}
JSONEOF
output=$(cd "$REPO_DIR" && run_review)
if jq -e '.comments[0] | select(.path == "spaced dir/spaced file.txt" and .line == 2 and .side == "RIGHT")' "$MOCK_DIR/last_payload.json" >/dev/null 2>&1; then
    pass "spaced path finding emitted as an inline comment"
else
    fail "Expected an inline comment on 'spaced dir/spaced file.txt':2, got: $(jq -c '.comments' "$MOCK_DIR/last_payload.json")"
fi
if [[ "$output" == *"REVIEW_POSTED:1/3:1:0"* ]]; then
    pass "last line REVIEW_POSTED:1/3:1:0"
else
    fail "Expected REVIEW_POSTED:1/3:1:0, got: $output"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test 17: default reasoning effort is xhigh ---
info "Test 17: default effort is xhigh"
setup_repo
setup_mock_gh
output=$(cd "$REPO_DIR" && env -u CODEX_REVIEW_REASONING_EFFORT PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_STATE_DIR="$MOCK_DIR/state" timeout "$INVOCATION_TIMEOUT" bash "$SCRIPT" 42)
if grep -q "model_reasoning_effort=xhigh" "$MOCK_DIR/codex_calls.log"; then
    pass "unset env var defaults to xhigh"
else
    fail "Expected model_reasoning_effort=xhigh in codex_calls.log, got: $(cat "$MOCK_DIR/codex_calls.log")"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

setup_repo
setup_mock_gh
output=$(cd "$REPO_DIR" && CODEX_REVIEW_REASONING_EFFORT=high PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_STATE_DIR="$MOCK_DIR/state" timeout "$INVOCATION_TIMEOUT" bash "$SCRIPT" 42)
if grep -q "model_reasoning_effort=high" "$MOCK_DIR/codex_calls.log"; then
    pass "explicit CODEX_REVIEW_REASONING_EFFORT=high is honoured"
else
    fail "Expected model_reasoning_effort=high in codex_calls.log, got: $(cat "$MOCK_DIR/codex_calls.log")"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Invalid PR number prints usage and exits non-zero ---
info "Test: invalid PR number prints usage and exits non-zero"
setup_repo
setup_mock_gh
set +e
output=$(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" timeout "$ARG_TIMEOUT" bash "$SCRIPT" not-a-number 2>&1)
status=$?
set -e
if [[ $status -ne 0 && "$output" == *"Usage:"* ]]; then
    pass "Invalid PR number prints usage and exits non-zero"
else
    fail "Expected non-zero exit with usage text, got status=$status output=$output"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Unknown flag prints usage and exits non-zero ---
info "Test: unknown flag prints usage and exits non-zero"
setup_repo
setup_mock_gh
set +e
output=$(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" timeout "$ARG_TIMEOUT" bash "$SCRIPT" 42 --bogus-flag 2>&1)
status=$?
set -e
if [[ $status -ne 0 && "$output" == *"Usage:"* ]]; then
    pass "Unknown flag prints usage and exits non-zero"
else
    fail "Expected non-zero exit with usage text, got status=$status output=$output"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- --help and -h print usage and exit 0 ---
for help_flag in --help -h; do
    info "Test: $help_flag prints usage and exits 0"
    setup_repo
    setup_mock_gh
    set +e
    output=$(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" timeout "$ARG_TIMEOUT" bash "$SCRIPT" "$help_flag" 2>&1)
    status=$?
    set -e
    if [[ $status -eq 0 && "$output" == *"Usage:"* && "$output" != *"Error:"* ]]; then
        pass "$help_flag prints usage and exits 0"
    else
        fail "Expected exit 0 with usage text and no Error: line for $help_flag, got status=$status output=$output"
    fi
    cleanup; MOCK_DIR=""; REPO_DIR=""
done

# --- Test: deleted head branch still reviews ---
info "Test: deleted head branch still reviews"
setup_repo
setup_mock_gh
git -C "$REPO_DIR" branch -D feature/example
output=$(cd "$REPO_DIR" && run_review)
if [[ "$output" == *"REVIEW_POSTED:1/3:0:0"* ]]; then
    pass "last line REVIEW_POSTED:1/3:0:0 with the head branch deleted"
else
    fail "Expected REVIEW_POSTED:1/3:0:0, got: $output"
fi
codex_cwd=$(cat "$MOCK_DIR/codex_cwd.log")
if [[ "$codex_cwd" == *"codex-review-"* ]]; then
    pass "codex ran in a codex-review-* worktree"
else
    fail "Expected codex_cwd.log to name a codex-review-* worktree, got: $codex_cwd"
fi
if [[ "$codex_cwd" == "$(cd "$REPO_DIR" && pwd -P)/.claude/worktrees/codex-review-"* || "$codex_cwd" == "$REPO_DIR/.claude/worktrees/codex-review-"* ]]; then
    pass "codex worktree lives under the checkout's .claude/worktrees"
else
    fail "Expected the worktree under $REPO_DIR/.claude/worktrees, got: $codex_cwd"
fi
if grep -qv '^unset$' "$MOCK_DIR/gh_token_seen.log"; then
    fail "GH_TOKEN reached gh: $(sort -u "$MOCK_DIR/gh_token_seen.log" | tr '\n' ' ')"
else
    pass "every gh call ran without GH_TOKEN"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test: pending then join ---
info "Test: pending then join"
setup_repo
setup_mock_gh
echo 3 > "$MOCK_DIR/codex_sleep"
output=$(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_STATE_DIR="$MOCK_DIR/state" \
    CODEX_REVIEW_WAIT_SECONDS=1 timeout "$INVOCATION_TIMEOUT" bash "$SCRIPT" 42)
last_line=$(printf '%s\n' "$output" | tail -1)
if [[ "$last_line" == "REVIEW_PENDING:1/3" ]]; then
    pass "first call reports REVIEW_PENDING:1/3"
else
    fail "Expected REVIEW_PENDING:1/3, got: $output"
fi
if [[ ! -f "$MOCK_DIR/post_calls.log" ]]; then
    pass "no POST made while pending"
else
    fail "Unexpected POST while pending"
fi
JOBDIR=$(find "$MOCK_DIR/state" -maxdepth 1 -type d -name 'pr-*' -print -quit)
if [[ -n "$JOBDIR" && -d "$JOBDIR" ]]; then
    pass "job dir still exists after the pending call"
else
    fail "Expected a job dir under $MOCK_DIR/state to still exist"
fi
worktree_count=$(git -C "$REPO_DIR" worktree list | wc -l)
if [[ "$worktree_count" -eq 2 ]]; then
    pass "review worktree still exists while the worker runs"
else
    fail "Expected the review worktree to still exist, got: $(git -C "$REPO_DIR" worktree list)"
fi

output2=$(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_STATE_DIR="$MOCK_DIR/state" \
    CODEX_REVIEW_WAIT_SECONDS="$JOIN_WAIT" timeout "$INVOCATION_TIMEOUT" bash "$SCRIPT" 42)
last_line2=$(printf '%s\n' "$output2" | tail -1)
if [[ "$last_line2" == "REVIEW_POSTED:1/3:0:0" ]]; then
    pass "second call joins the worker and reports REVIEW_POSTED:1/3:0:0"
else
    fail "Expected REVIEW_POSTED:1/3:0:0, got: $output2"
fi
post_count=$(wc -l < "$MOCK_DIR/post_calls.log" 2>/dev/null || echo 0)
if [[ "$post_count" -eq 1 ]]; then
    pass "exactly one POST made across both calls"
else
    fail "Expected exactly 1 POST, got: $post_count"
fi
if [[ ! -d "$JOBDIR" ]]; then
    pass "job dir removed after the join"
else
    fail "Expected job dir removed, still present: $JOBDIR"
fi
worktree_count=$(git -C "$REPO_DIR" worktree list | wc -l)
if [[ "$worktree_count" -eq 1 ]]; then
    pass "worktree removed after the join"
else
    fail "Expected only the main worktree, got: $(git -C "$REPO_DIR" worktree list)"
fi
# The test repo's own path is named test-codex-review-pr-repo-XXXXXX, which
# contains "codex-review-" as a substring, so check the worktree basename
# prefix instead of grepping the whole listing for that substring.
worktree_names=$(git -C "$REPO_DIR" worktree list | awk '{print $1}' | xargs -n1 basename)
if ! grep -q '^codex-review-' <<< "$worktree_names"; then
    pass "git worktree list has no codex-review- entries after the join"
else
    fail "Expected no codex-review- worktrees, got: $(git -C "$REPO_DIR" worktree list)"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test: worker posts without a re-invoke ---
info "Test: worker posts without a re-invoke"
setup_repo
setup_mock_gh
echo 3 > "$MOCK_DIR/codex_sleep"
output=$(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_STATE_DIR="$MOCK_DIR/state" \
    CODEX_REVIEW_WAIT_SECONDS=1 timeout "$INVOCATION_TIMEOUT" bash "$SCRIPT" 42)
last_line=$(printf '%s\n' "$output" | tail -1)
if [[ "$last_line" == "REVIEW_PENDING:1/3" ]]; then
    pass "first call reports REVIEW_PENDING:1/3"
else
    fail "Expected REVIEW_PENDING:1/3, got: $output"
fi
if wait_for "$POLL_DEADLINE" '[[ -f "$MOCK_DIR/post_calls.log" ]]'; then
    post_count=$(wc -l < "$MOCK_DIR/post_calls.log")
    if [[ "$post_count" -eq 1 ]]; then
        pass "worker posted on its own without a re-invoke"
    else
        fail "Expected exactly 1 POST, got: $post_count"
    fi
else
    fail "Expected the detached worker to POST without a re-invoke"
fi
if wait_for "$POLL_DEADLINE" '[[ "$(git -C "$REPO_DIR" worktree list | wc -l)" -eq 1 ]]'; then
    pass "worktree removed by the worker without a re-invoke"
else
    fail "Expected only the main worktree, got: $(git -C "$REPO_DIR" worktree list)"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test: dead worker ---
info "Test: dead worker reported as an error without posting"
setup_repo
setup_mock_gh
echo 5 > "$MOCK_DIR/codex_sleep"
output=$(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_STATE_DIR="$MOCK_DIR/state" \
    CODEX_REVIEW_WAIT_SECONDS=1 timeout "$INVOCATION_TIMEOUT" bash "$SCRIPT" 42)
last_line=$(printf '%s\n' "$output" | tail -1)
if [[ "$last_line" == "REVIEW_PENDING:1/3" ]]; then
    pass "first call reports REVIEW_PENDING:1/3"
else
    fail "Expected REVIEW_PENDING:1/3, got: $output"
fi
JOBDIR=$(find "$MOCK_DIR/state" -maxdepth 1 -type d -name 'pr-*' -print -quit)
JOB_PID=$(cat "$JOBDIR/pid")
kill -9 "$JOB_PID" 2>/dev/null || true
# Wait for the OS to reap the process so kill -0 reliably reports dead.
wait_for "$PROC_EXIT_DEADLINE" '! kill -0 "$JOB_PID" 2>/dev/null' || fail "Worker PID $JOB_PID never exited after SIGKILL"
set +e
output2=$(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_STATE_DIR="$MOCK_DIR/state" \
    CODEX_REVIEW_WAIT_SECONDS=1 timeout "$INVOCATION_TIMEOUT" bash "$SCRIPT" 42 2>&1)
status=$?
set -e
if [[ $status -ne 0 ]]; then
    pass "second call exits non-zero for a dead worker"
else
    fail "Expected non-zero exit for a dead worker, got status=$status"
fi
if [[ "$output2" == *"worker died"* ]]; then
    pass "stderr reports the worker died"
else
    fail "Expected 'worker died' in stderr, got: $output2"
fi
if [[ ! -d "$JOBDIR" ]]; then
    pass "job dir removed after dead-worker detection"
else
    fail "Expected job dir removed, still present: $JOBDIR"
fi
if [[ ! -f "$MOCK_DIR/post_calls.log" ]]; then
    pass "no POST made for a dead worker"
else
    fail "Unexpected POST for a dead worker"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test: reclaiming a dead worker also takes down its orphaned children ---
# The worker runs codex inside a subshell of its own; SIGKILLing the worker
# leaves that subshell, its `timeout`, and the whole codex tree running as
# orphans inside the worktree the reclaim is about to delete. `timeout` puts
# itself and codex into a process group of their own, so the reclaim has to
# sweep the worker's whole *session* (its pid, since it was launched under
# `setsid`) — a process-group signal alone would miss codex itself.
info "Test: dead-worker reclaim kills the worker's orphaned process tree"
setup_repo
setup_mock_gh
echo 60 > "$MOCK_DIR/codex_sleep"
output=$(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_STATE_DIR="$MOCK_DIR/state" \
    CODEX_REVIEW_WAIT_SECONDS=1 timeout "$INVOCATION_TIMEOUT" bash "$SCRIPT" 42)
JOBDIR=$(find "$MOCK_DIR/state" -maxdepth 1 -type d -name 'pr-*' -print -quit)
JOB_PID=$(cat "$JOBDIR/pid")
# The worker must actually be inside its codex call, so there is a live
# orphan tree for the reclaim to deal with.
if wait_for "$POLL_DEADLINE" '[[ -f "$MOCK_DIR/codex_calls.log" ]]'; then
    pass "worker reached its codex call before being killed"
else
    fail "Expected the worker to invoke codex; got: $output"
fi
kill -9 "$JOB_PID" 2>/dev/null || true
wait_for "$PROC_EXIT_DEADLINE" '! kill -0 "$JOB_PID" 2>/dev/null' || fail "Worker PID $JOB_PID never exited after SIGKILL"
if [[ "$(pgrep -s "$JOB_PID" | wc -l)" -ge 2 ]]; then
    pass "killing the worker leaves its codex children orphaned"
else
    fail "Expected orphaned children in worker session $JOB_PID after SIGKILL, got: $(pgrep -a -s "$JOB_PID")"
fi
# The codex tree must be in a process group of its own, or this test would
# pass for a reclaim that only signals the worker's process group.
CODEX_PID=$(pgrep -s "$JOB_PID" -x timeout || true)
CODEX_PID=${CODEX_PID%%$'\n'*}
if [[ -n "$CODEX_PID" && "$(ps -o pgid= -p "$CODEX_PID" | tr -d ' ')" != "$JOB_PID" ]]; then
    pass "the orphaned codex tree sits outside the worker's own process group"
else
    fail "Expected the timeout-supervised codex tree in its own process group, got: $(pgrep -a -s "$JOB_PID")"
fi
set +e
output2=$(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_STATE_DIR="$MOCK_DIR/state" \
    CODEX_REVIEW_WAIT_SECONDS=1 timeout "$INVOCATION_TIMEOUT" bash "$SCRIPT" 42 2>&1)
status=$?
set -e
if [[ $status -ne 0 && "$output2" == *"worker died"* ]]; then
    pass "second call reclaims the dead worker"
else
    fail "Expected a dead-worker reclaim, got status=$status output=$output2"
fi
if wait_for "$PROC_EXIT_DEADLINE" '! pgrep -s "$JOB_PID" > /dev/null 2>&1'; then
    pass "reclaim left no process from the dead worker's session running"
else
    fail "Expected no surviving processes in session $JOB_PID, got: $(pgrep -a -s "$JOB_PID")"
fi
if [[ ! -f "$MOCK_DIR/post_calls.log" ]]; then
    pass "no POST made by the reclaimed worker's orphans"
else
    fail "Unexpected POST from a reclaimed worker's orphans"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test: two concurrent invocations for the same PR/head/mode race for one
# job instead of both launching a worker (the job dir claim must be atomic) ---
info "Test: concurrent invocations race for one job, not two"
setup_repo
setup_mock_gh
echo 2 > "$MOCK_DIR/codex_sleep"
(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_STATE_DIR="$MOCK_DIR/state" \
    CODEX_REVIEW_WAIT_SECONDS="$RACE_JOIN_WAIT" timeout "$INVOCATION_TIMEOUT" bash "$SCRIPT" 42 > "$MOCK_DIR/race_out1" 2>&1) &
race_pid1=$!
(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_STATE_DIR="$MOCK_DIR/state" \
    CODEX_REVIEW_WAIT_SECONDS="$RACE_JOIN_WAIT" timeout "$INVOCATION_TIMEOUT" bash "$SCRIPT" 42 > "$MOCK_DIR/race_out2" 2>&1) &
race_pid2=$!
# wait's own exit status reflects only the last-listed pid; run both under
# set +e regardless so neither invocation's exit code can trip this suite's
# set -e before its output is inspected below.
set +e
wait "$race_pid1"
wait "$race_pid2"
set -e
race_out1=$(cat "$MOCK_DIR/race_out1")
race_out2=$(cat "$MOCK_DIR/race_out2")
race_last1=$(printf '%s\n' "$race_out1" | tail -1)
race_last2=$(printf '%s\n' "$race_out2" | tail -1)
# Whichever of the two invocations loses the atomic mkdir claim joins the
# other via wait_and_emit; depending on scheduling either one can be the
# first to observe the (shared) job's result within its own wait budget, so
# either may report REVIEW_POSTED while the other reports REVIEW_PENDING (an
# invitation to join again) rather than a fixed winner/loser pairing. The
# invariants that must hold regardless of that ordering — exactly one codex
# invocation and one POST for both invocations combined — are checked below.
if [[ "$race_last1" =~ ^REVIEW_(POSTED:1/3:0:0|PENDING:1/3)$ && "$race_last2" =~ ^REVIEW_(POSTED:1/3:0:0|PENDING:1/3)$ ]]; then
    pass "both concurrent invocations report a valid posted/pending result"
else
    fail "Expected both to report REVIEW_POSTED:1/3:0:0 or REVIEW_PENDING:1/3, got: [$race_last1] / [$race_last2]"
fi
if [[ "$race_last1" == "REVIEW_POSTED:1/3:0:0" || "$race_last2" == "REVIEW_POSTED:1/3:0:0" ]]; then
    pass "at least one of the two invocations observed the posted result"
else
    fail "Expected at least one invocation to report REVIEW_POSTED, got: [$race_last1] / [$race_last2]"
fi
codex_calls=$(wc -l < "$MOCK_DIR/codex_calls.log" 2>/dev/null || echo 0)
if [[ "$codex_calls" -eq 1 ]]; then
    pass "codex was invoked exactly once across both racing invocations"
else
    fail "Expected exactly 1 codex invocation, got: $codex_calls"
fi
post_count=$(wc -l < "$MOCK_DIR/post_calls.log" 2>/dev/null || echo 0)
if [[ "$post_count" -eq 1 ]]; then
    pass "exactly one POST made across both racing invocations"
else
    fail "Expected exactly 1 POST, got: $post_count"
fi

# A REVIEW_PENDING outcome above leaves the job dir/worktree for the worker
# itself to clean up once it finishes, same as the "worker posts without a
# re-invoke" test — poll rather than asserting these are gone immediately.
if wait_for "$POLL_DEADLINE" '[[ "$(git -C "$REPO_DIR" worktree list | wc -l)" -eq 1 ]]'; then
    pass "no leftover review worktree after the race"
else
    fail "Expected only the main worktree after the race, got: $(git -C "$REPO_DIR" worktree list)"
fi
if wait_for "$POLL_DEADLINE" '[[ -z "$(find "$MOCK_DIR/state" -maxdepth 1 -type d -name "pr-*" 2>/dev/null)" ]]'; then
    pass "job dir removed after both invocations completed"
else
    fail "Expected no leftover job dir under $MOCK_DIR/state"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test: pid is persisted via a temp file + rename, never a bare redirect ---
# Regression guard for the atomic-pid-write fix: `echo "$X" > "$JOBDIR/pid"`
# creates the (empty) file before writing its contents, so a joiner reading
# it mid-write sees an empty pid, `kill -0 ""` fails, and it wrongly reclaims
# a live job. The real race can't be forced deterministically in bash (same
# limitation noted for the atomic job-dir claim), so this pins the structural
# invariant instead: the pid file is always written to a distinct temp path
# and moved into place, so no reader can ever observe it partially written.
info "Test: pid file is written via a temp file + rename, not a direct redirect"
if grep -qE '^\s*(echo|printf)[^|]*>\s*"\$JOBDIR/pid"\s*$' "$SCRIPT"; then
    fail "Found a direct redirect into \$JOBDIR/pid; it must go through a temp file + rename so a joiner never observes a partially-written pid file"
else
    pass "no direct redirect into \$JOBDIR/pid found"
fi
if grep -qE '\$JOBDIR/pid\.tmp".*&&.*mv "\$JOBDIR/pid\.tmp" "\$JOBDIR/pid"' "$SCRIPT"; then
    pass "pid is written to a temp file and renamed into place"
else
    fail "Expected the pid write to use a temp file + rename pattern"
fi

# --- Test: a claim abandoned before any pid is written is reclaimed, not waited on forever ---
# Before this fix, a claim whose owner crashed between the atomic `mkdir` and
# ever writing a pid (e.g. killed mid `git fetch`) would wedge the PR
# permanently: every re-invocation takes the missing-pid path, waits out the
# full timeout, and reports REVIEW_PENDING again, forever, since nothing ever
# reclaims it. The fix bounds that grace period, so a stale claim is instead
# surfaced as a clear dead-worker error (same as any other dead worker) and
# cleaned up — freeing a later invocation to start a real review.
info "Test: a stale pid-less claim (owner crashed before writing a pid) is reclaimed"
setup_repo
setup_mock_gh
mkdir -p "$MOCK_DIR/state"
STALE_JOBDIR="$MOCK_DIR/state/pr-42-$CURRENT_HEAD_SHA-false"
mkdir -m 700 "$STALE_JOBDIR"
# Back-date the claim well past a short init timeout, simulating an owner
# that mkdir'd the job dir and then crashed (e.g. killed mid `git fetch`)
# before ever getting to write env/pid.
touch -d "@$(($(date +%s) - 30))" "$STALE_JOBDIR"
set +e
output=$(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_STATE_DIR="$MOCK_DIR/state" \
    CODEX_REVIEW_JOB_INIT_TIMEOUT_SECONDS=5 CODEX_REVIEW_WAIT_SECONDS="$JOIN_WAIT" timeout "$INVOCATION_TIMEOUT" bash "$SCRIPT" 42 2>&1)
status=$?
set -e
if [[ $status -ne 0 ]]; then
    pass "stale claim reported as a dead-worker error (exit non-zero) instead of REVIEW_PENDING"
else
    fail "Expected non-zero exit when reclaiming a stale pid-less claim, got status=$status output=$output"
fi
if [[ "$output" == *"worker died"* ]]; then
    pass "stderr reports the worker died"
else
    fail "Expected 'worker died' in stderr, got: $output"
fi
if [[ "$output" != *"REVIEW_PENDING"* ]]; then
    pass "does not repeat REVIEW_PENDING for an abandoned claim"
else
    fail "Should not have reported REVIEW_PENDING for a stale pid-less claim, got: $output"
fi
if [[ ! -d "$STALE_JOBDIR" ]]; then
    pass "stale claim's job dir was removed as part of reclaiming it"
else
    fail "Expected the stale job dir to be removed, still present: $STALE_JOBDIR"
fi
# With the stale claim now cleaned up, a fresh invocation must be able to
# start a real review rather than staying wedged.
output2=$(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_STATE_DIR="$MOCK_DIR/state" \
    CODEX_REVIEW_JOB_INIT_TIMEOUT_SECONDS=5 CODEX_REVIEW_WAIT_SECONDS="$JOIN_WAIT" timeout "$INVOCATION_TIMEOUT" bash "$SCRIPT" 42)
if [[ "$output2" == *"REVIEW_POSTED:1/3:0:0"* ]]; then
    pass "a subsequent invocation starts a fresh review and posts normally"
else
    fail "Expected REVIEW_POSTED:1/3:0:0 on the next invocation, got: $output2"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# A claim that is merely slow to initialize (within the grace period) must
# NOT be reclaimed out from under its owner — only genuinely abandoned claims
# should be. This guards against an overly aggressive timeout.
info "Test: a pid-less claim still inside its init grace period is NOT reclaimed"
setup_repo
setup_mock_gh
mkdir -p "$MOCK_DIR/state"
FRESH_JOBDIR="$MOCK_DIR/state/pr-42-$CURRENT_HEAD_SHA-false"
mkdir -m 700 "$FRESH_JOBDIR"
set +e
output=$(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_STATE_DIR="$MOCK_DIR/state" \
    CODEX_REVIEW_JOB_INIT_TIMEOUT_SECONDS=60 CODEX_REVIEW_WAIT_SECONDS=1 timeout "$INVOCATION_TIMEOUT" bash "$SCRIPT" 42 2>&1)
status=$?
set -e
if [[ "$output" == *"REVIEW_PENDING:1/3"* ]]; then
    pass "fresh pid-less claim inside its grace period is waited on (REVIEW_PENDING), not reclaimed"
else
    fail "Expected REVIEW_PENDING:1/3 for a fresh pid-less claim, got status=$status output=$output"
fi
if [[ -d "$FRESH_JOBDIR" ]]; then
    pass "fresh claim's job dir is left alone for its owner to keep using"
else
    fail "Expected the fresh job dir to still exist, but it was removed"
fi
rm -rf "$FRESH_JOBDIR"
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test: an unwritable state dir fails fast instead of waiting out the timeout ---
# Regression guard: a plain `mkdir` failure was previously treated as "someone
# else already owns this job" unconditionally, so a real setup error (a
# read-only or missing state dir, disk full, ...) would silently make the
# caller wait the full CODEX_REVIEW_WAIT_SECONDS and report REVIEW_PENDING
# instead of surfacing the real problem.
info "Test: an unwritable state dir reports a setup error instead of REVIEW_PENDING"
setup_repo
setup_mock_gh
mkdir -p "$MOCK_DIR/state"
chmod 500 "$MOCK_DIR/state"
set +e
# The outer timeout here is itself the assertion: the script must surface the
# setup error well before it could have waited out CODEX_REVIEW_WAIT_SECONDS.
# The two values must stay far apart ($ARG_TIMEOUT well under $JOIN_WAIT) —
# collapsing them would let a script that silently waits still pass.
output=$(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_STATE_DIR="$MOCK_DIR/state" \
    CODEX_REVIEW_WAIT_SECONDS="$JOIN_WAIT" timeout "$ARG_TIMEOUT" bash "$SCRIPT" 42 2>&1)
status=$?
set -e
chmod 700 "$MOCK_DIR/state"
if [[ $status -ne 0 ]]; then
    pass "unwritable state dir -> non-zero exit"
else
    fail "Expected non-zero exit for an unwritable state dir, got status=$status"
fi
if [[ "$output" == *"failed to create job dir"* ]]; then
    pass "reports a job-dir creation error"
else
    fail "Expected a 'failed to create job dir' error, got: $output"
fi
if [[ "$output" != *"REVIEW_PENDING"* ]]; then
    pass "does not misreport REVIEW_PENDING for a real setup error"
else
    fail "Should not have reported REVIEW_PENDING for an unwritable state dir, got: $output"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test: a hung worker is bounded by CODEX_REVIEW_WORKER_TIMEOUT_SECONDS ---
# Regression guard: before `codex` was invoked under `timeout`, a codex
# invocation (or a stalled posting step) that never returned would keep the
# worker's pid alive forever, so job_owner_alive() would report it alive
# indefinitely and every re-invocation would report REVIEW_PENDING forever —
# the job could never be reclaimed or retried. The timeout is applied around
# the `codex` call itself (not the whole worker launch) so $JOBDIR/pid keeps
# tracking the actual worker process throughout — see the comment above the
# `setsid nohup bash "$0" --worker` launch for why wrapping the launch itself
# would break dead-worker detection.
info "Test: a hung worker is killed by its timeout instead of wedging the job forever"
setup_repo
setup_mock_gh
echo 60 > "$MOCK_DIR/codex_sleep"
output=$(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_STATE_DIR="$MOCK_DIR/state" \
    CODEX_REVIEW_WORKER_TIMEOUT_SECONDS=3 CODEX_REVIEW_WAIT_SECONDS=1 timeout "$INVOCATION_TIMEOUT" bash "$SCRIPT" 42)
last_line=$(printf '%s\n' "$output" | tail -1)
if [[ "$last_line" == "REVIEW_PENDING:1/3" ]]; then
    pass "first call reports REVIEW_PENDING:1/3 while the worker is (apparently) still running"
else
    fail "Expected REVIEW_PENDING:1/3, got: $output"
fi
JOBDIR=$(find "$MOCK_DIR/state" -maxdepth 1 -type d -name 'pr-*' -print -quit)
JOB_PID=$(cat "$JOBDIR/pid")
# The worker timeout should kill the hung codex process (and the worker
# shell under it) well before the mock's own 60s codex_sleep would return.
if wait_for "$PROC_EXIT_DEADLINE" '! kill -0 "$JOB_PID" 2>/dev/null'; then
    pass "worker process was killed by its timeout instead of running the full 60s codex_sleep"
else
    fail "Expected the worker to be killed by CODEX_REVIEW_WORKER_TIMEOUT_SECONDS, but pid $JOB_PID is still alive"
fi
set +e
output2=$(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_STATE_DIR="$MOCK_DIR/state" \
    CODEX_REVIEW_WAIT_SECONDS=1 timeout "$INVOCATION_TIMEOUT" bash "$SCRIPT" 42 2>&1)
status2=$?
set -e
# The worker completed on its own (its `codex` call was killed by the
# per-call timeout, not the worker process itself), so this joins a normal
# completed-with-error result rather than hitting the dead-pid reclaim path.
if [[ $status2 -ne 0 && "$output2" == *"codex exec review failed"* ]]; then
    pass "second call reports the timed-out codex call as a normal review failure instead of REVIEW_PENDING forever"
else
    fail "Expected a codex-failure error on re-invocation, got status=$status2 output=$output2"
fi
if wait_for "$POLL_DEADLINE" '[[ "$(git -C "$REPO_DIR" worktree list | wc -l)" -eq 1 ]]'; then
    pass "worktree reclaimed after the worker was killed by its timeout"
else
    fail "Expected only the main worktree after the timeout, got: $(git -C "$REPO_DIR" worktree list)"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test: a claim still inside a slow `git fetch` is not reclaimed out from under it ---
# Regression guard: job_owner_alive()'s pid-less grace period is anchored to
# $JOBDIR's mtime, which the atomic mkdir sets once, at claim time. Before the
# setup heartbeat, nothing touched $JOBDIR again until env/pid were written
# after `git fetch` and `git worktree add` both finished, so a joiner arriving
# after the grace period elapsed but before those finished would wrongly
# conclude the claim was abandoned and reclaim it (deleting the job dir, and
# orphaning the worktree if it had already been added) while the real owner
# was still actively fetching.
info "Test: a claim mid-fetch keeps its heartbeat alive instead of being reclaimed"
setup_repo
setup_mock_gh
setup_mock_git_fetch_delay 20
# The init timeout has to leave real slack for scheduling jitter, not just
# clear the heartbeat period: the heartbeat ticks every
# CODEX_REVIEW_JOB_INIT_TIMEOUT_SECONDS / 4 seconds (at least 1), and whatever
# the tick does not use is the delay a tick can suffer before the joiner's
# staleness check wrongly concludes the owner is dead. 8 gives a 2s tick and
# 6s of slack, and the fetch delay below is long enough that the joiner still
# arrives well after the timeout would have elapsed unrefreshed.
(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_STATE_DIR="$MOCK_DIR/state" \
    CODEX_REVIEW_JOB_INIT_TIMEOUT_SECONDS=8 CODEX_REVIEW_WAIT_SECONDS="$JOIN_WAIT" \
    timeout "$INVOCATION_TIMEOUT" bash "$SCRIPT" 42 > "$MOCK_DIR/owner_out" 2>&1) &
owner_pid=$!
# Wait for the owner to actually claim $JOBDIR (mtime set at claim time)
# rather than assuming it has within a fixed interval, then let the 8s init
# timeout above lapse. Both halves of the window this test needs — "claimed"
# and "past the init timeout" — are then observed rather than guessed, and
# the 20s fetch delay above still covers the joiner's own startup.
wait_for "$POLL_DEADLINE" '[[ -n "$(find "$MOCK_DIR/state" -maxdepth 1 -type d -name "pr-*" 2>/dev/null)" ]]' \
    || fail "Owner never claimed a job dir"
# The heartbeat's period, not just its existence, is what keeps a loaded host
# from reclaiming this claim: a joiner declares the claim dead once
# (now - mtime) reaches the init timeout, both read at whole-second
# granularity, so every second of the period is a second of scheduling jitter
# the heartbeat can no longer absorb. A period of timeout/2 + 1 (5s of 8)
# left ~2s, which a TestScripts run under load exceeded (bug #3028). Require
# at least half the grace window to remain as slack. The heartbeat is the only
# process whose argv ends with the job dir path (`bash -c ... _ <period> <dir>`).
MIDFETCH_JOBDIR=$(find "$MOCK_DIR/state" -maxdepth 1 -type d -name 'pr-*' -print -quit)
heartbeat_period=""
wait_for "$POLL_DEADLINE" \
    'heartbeat_period=$(ps -eo args= | awk -v d="$MIDFETCH_JOBDIR" '"'"'index($0, "bash -c") == 1 && $NF == d { print $(NF-1); exit }'"'"'); [[ -n "$heartbeat_period" ]]' \
    || fail "Never found the setup heartbeat process for $MIDFETCH_JOBDIR"
if [[ "$heartbeat_period" =~ ^[0-9]+$ && "$heartbeat_period" -le 4 ]]; then
    pass "setup heartbeat ticks every ${heartbeat_period}s, leaving at least half the 8s grace window as slack"
else
    fail "Expected a setup heartbeat period of at most 4s (half of CODEX_REVIEW_JOB_INIT_TIMEOUT_SECONDS=8), got '$heartbeat_period'"
fi
sleep 9   # > CODEX_REVIEW_JOB_INIT_TIMEOUT_SECONDS=8, the grace period under test
set +e
joiner_out=$(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_STATE_DIR="$MOCK_DIR/state" \
    CODEX_REVIEW_JOB_INIT_TIMEOUT_SECONDS=8 CODEX_REVIEW_WAIT_SECONDS=2 timeout "$INVOCATION_TIMEOUT" bash "$SCRIPT" 42 2>&1)
joiner_status=$?
set -e
if [[ $joiner_status -eq 0 && "$joiner_out" == "REVIEW_PENDING:1/3" ]]; then
    pass "joiner mid-fetch reports REVIEW_PENDING, not a dead-worker reclaim"
else
    fail "Expected REVIEW_PENDING:1/3 for a joiner arriving mid-fetch, got status=$joiner_status output=$joiner_out"
fi
JOBDIR=$(find "$MOCK_DIR/state" -maxdepth 1 -type d -name 'pr-*' -print -quit)
if [[ -n "$JOBDIR" && -d "$JOBDIR" ]]; then
    pass "job dir was not reclaimed out from under the still-fetching owner"
else
    fail "Expected the job dir to still exist while the owner was mid-fetch"
fi
set +e
wait "$owner_pid"
owner_status=$?
set -e
owner_out=$(cat "$MOCK_DIR/owner_out")
owner_last=$(printf '%s\n' "$owner_out" | tail -1)
if [[ $owner_status -eq 0 && "$owner_last" == "REVIEW_POSTED:1/3:0:0" ]]; then
    pass "owner completes and posts normally once its fetch finishes"
else
    fail "Expected the owner to finish with REVIEW_POSTED:1/3:0:0, got status=$owner_status output=$owner_out"
fi
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test: the setup heartbeat never resurrects a reclaimed job dir ---
# The heartbeat that keeps a mid-setup claim's mtime fresh used to call a bare
# `touch "$JOBDIR"`, which *creates* the path when it is missing. If a sibling
# watcher reclaimed the claim (rm -rf "$JOBDIR") while setup was still under
# way, the next heartbeat tick put a regular *file* back at the job-dir path.
# That turned a recoverable reclaim into a wedged PR: the owner's own
# `> "$JOBDIR/env"` fails with "Not a directory", and every later invocation
# for the same PR/head fails its atomic mkdir against the leftover file.
info "Test: the setup heartbeat does not recreate a reclaimed job dir"
setup_repo
setup_mock_gh
setup_mock_git_fetch_delay 15
HB_JOBDIR="$MOCK_DIR/state/pr-42-$CURRENT_HEAD_SHA-false"
(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_STATE_DIR="$MOCK_DIR/state" \
    CODEX_REVIEW_JOB_INIT_TIMEOUT_SECONDS=4 CODEX_REVIEW_WAIT_SECONDS=1 timeout "$INVOCATION_TIMEOUT" bash "$SCRIPT" 42 \
    > "$MOCK_DIR/hb_out" 2>&1) &
hb_owner_pid=$!
if wait_for "$POLL_DEADLINE" '[[ -d "$HB_JOBDIR" ]]'; then
    # Reclaim the claim exactly as a sibling watcher's dead-worker path does,
    # while the owner is still inside its 15s fetch with a live heartbeat.
    rm -rf "$HB_JOBDIR"
    # Several heartbeat periods (4/4 = 1s each): long enough for a `touch`
    # that creates missing paths to have put a regular file back.
    sleep 7
    if [[ ! -e "$HB_JOBDIR" ]]; then
        pass "heartbeat leaves a reclaimed job dir removed instead of recreating it"
    else
        fail "Heartbeat resurrected the job dir as a $(stat -c %F "$HB_JOBDIR"): $HB_JOBDIR"
    fi
else
    fail "Owner never claimed the job dir within $POLL_DEADLINE seconds"
fi
set +e
wait "$hb_owner_pid"
set -e
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test: the setup heartbeat stops when its owner is SIGKILLed ---
# An owner killed hard enough to skip stop_heartbeat used to leave the heartbeat
# touching the pid-less job dir forever, so joiners saw a live claim and never
# reclaimed it. The heartbeat now gets the owner's pid and exits once it is gone.
info "Test: the setup heartbeat exits after its owner is SIGKILLed"
setup_repo
setup_mock_gh
setup_mock_git_fetch_delay 30
HBK_JOBDIR="$MOCK_DIR/state/pr-42-$CURRENT_HEAD_SHA-false"
(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_STATE_DIR="$MOCK_DIR/state" \
    CODEX_REVIEW_JOB_INIT_TIMEOUT_SECONDS=4 CODEX_REVIEW_WAIT_SECONDS=1 timeout "$INVOCATION_TIMEOUT" bash "$SCRIPT" 42 \
    > "$MOCK_DIR/hbk_out" 2>&1) &
hbk_wrapper_pid=$!
hbk_owner=""; hbk_heartbeat=""
if wait_for "$POLL_DEADLINE" \
    'read -r hbk_heartbeat hbk_owner < <(ps -eo pid=,args= | awk -v d="$HBK_JOBDIR" '"'"'index($0, "bash -c") > 0 && $NF == d { print $1, $(NF-2); exit }'"'"'); [[ -n "$hbk_owner" ]]'; then
    kill -9 "$hbk_owner" 2>/dev/null || true
    # One heartbeat period is 1s (4/4); give it several to notice.
    if wait_for "$PROC_EXIT_DEADLINE" '! kill -0 "$hbk_heartbeat" 2>/dev/null'; then
        pass "heartbeat exits once its owner is gone"
    else
        fail "Heartbeat $hbk_heartbeat outlived its SIGKILLed owner $hbk_owner"
        kill -9 "$hbk_heartbeat" 2>/dev/null || true
    fi
else
    fail "Never found the setup heartbeat process for $HBK_JOBDIR"
fi
set +e
wait "$hbk_wrapper_pid"
set -e
cleanup; MOCK_DIR=""; REPO_DIR=""

# --- Test: a joiner that cannot read the finished job's result (a sibling
# joiner consumed and removed the job dir between the -f check and the read)
# must still emit a contract line rather than exiting 1 as if nothing was
# posted. Simulated deterministically by making the result file unreadable. ---
info "Test: joiner that loses the race to read the result reports REVIEW_PENDING"
if [[ "$EUID" -eq 0 ]]; then
    info "skipped: running as root, an unreadable result file cannot be simulated"
else
    setup_repo
    setup_mock_gh
    echo 1 > "$MOCK_DIR/codex_sleep"
    output=$(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_STATE_DIR="$MOCK_DIR/state" \
        CODEX_REVIEW_WAIT_SECONDS=0 timeout "$INVOCATION_TIMEOUT" bash "$SCRIPT" 42)
    JOBDIR=$(find "$MOCK_DIR/state" -maxdepth 1 -type d -name 'pr-*' -print -quit)
    if wait_for "$POLL_DEADLINE" '[[ -f "$JOBDIR/result" ]]'; then
        chmod 000 "$JOBDIR/result"
        set +e
        output2=$(cd "$REPO_DIR" && PATH="$MOCK_DIR:$PATH" MOCK_DIR="$MOCK_DIR" CODEX_REVIEW_STATE_DIR="$MOCK_DIR/state" \
            CODEX_REVIEW_WAIT_SECONDS=1 timeout "$INVOCATION_TIMEOUT" bash "$SCRIPT" 42 2>/dev/null)
        status=$?
        set -e
        chmod 600 "$JOBDIR/result" 2>/dev/null || true
        last_line=$(printf '%s\n' "$output2" | tail -1)
        if [[ "$last_line" == "REVIEW_PENDING:1/3" && "$status" -eq 0 ]]; then
            pass "unreadable result yields REVIEW_PENDING and exit 0, not a bare exit 1"
        else
            fail "Expected REVIEW_PENDING:1/3 with exit 0, got: [$last_line] status=$status"
        fi
    else
        fail "Expected the detached worker to write a result"
    fi
    cleanup; MOCK_DIR=""; REPO_DIR=""
fi

info "Test: autonomous-pipeline.md documents the job dir the script actually creates"
PIPELINE_DOC="$REPO_ROOT/.claude/autonomous-pipeline.md"
if grep -q 'JOBDIR="\$CODEX_REVIEW_STATE_DIR/pr-\$PR-\$HEAD_SHA-\$DRY_RUN"' "$SCRIPT"; then
    if grep -q 'pr-<pr>-<head>-<dry-run>/' "$PIPELINE_DOC"; then
        pass "the documented job-dir form matches the script's JOBDIR construction"
    else
        fail "The script builds pr-<pr>-<head>-<dry-run> but $PIPELINE_DOC does not document that form"
    fi
else
    fail "JOBDIR construction in $SCRIPT changed; update the doc and this test together"
fi

# --- Summary ---
echo ""
if [[ "$FAILURES" -eq 0 ]]; then
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}${FAILURES} test(s) failed.${NC}"
    exit 1
fi
