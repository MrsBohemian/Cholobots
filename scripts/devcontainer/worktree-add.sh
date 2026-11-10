#!/usr/bin/env bash
# ABOUTME: Creates a git worktree whose .git pointer is relative, so the devcontainers CLI can
# ABOUTME: mount the common dir on any git version. Also supports --relativize on an existing worktree.
set -euo pipefail

# Computes the relative path from $2 (base directory) to $1 (target path)
# using bash string manipulation only — no python3 or `realpath
# --relative-to`, neither of which is guaranteed present on every host that
# still needs this fallback. Both arguments must be absolute, slash-separated
# paths with no trailing slash.
relpath() {
    local target="$1" base="$2"
    local IFS=/
    local -a target_parts base_parts
    read -ra target_parts <<<"$target"
    read -ra base_parts <<<"$base"

    local i=0
    while [ "$i" -lt "${#target_parts[@]}" ] && [ "$i" -lt "${#base_parts[@]}" ] && [ "${target_parts[$i]}" = "${base_parts[$i]}" ]; do
        i=$((i + 1))
    done

    local rel="" j
    for ((j = i; j < ${#base_parts[@]}; j++)); do
        rel="../$rel"
    done
    for ((j = i; j < ${#target_parts[@]}; j++)); do
        rel="$rel${target_parts[$j]}/"
    done
    rel="${rel%/}"
    [ -z "$rel" ] && rel="."
    printf '%s' "$rel"
}

# Rewrites <worktree-path>/.git so its `gitdir:` line is relative, if it is
# currently absolute. A pointer that is already relative is left alone.
# Returns non-zero on failure instead of exiting, so callers that already
# registered a worktree (git worktree add succeeded) can clean it up before
# leaving a broken checkout behind.
relativize() {
    local worktree_path="$1"
    local git_file="$worktree_path/.git"

    if [ ! -f "$git_file" ]; then
        echo "Error: $git_file is not a file (is $worktree_path a worktree?)" >&2
        return 1
    fi

    local line gitdir
    line="$(cat "$git_file")"
    gitdir="${line#gitdir: }"

    case "$gitdir" in
        /*)
            local rel
            rel="$(relpath "$gitdir" "$worktree_path")"
            echo "gitdir: $rel" >"$git_file"
            ;;
        *)
            # Already relative; nothing to do.
            ;;
    esac
}

# Resolves <base> to a commit SHA, searching local branches first, then each
# remote's tracking refs, then any other revision git understands (tag, SHA).
# Printing a SHA rather than a name is what keeps `git worktree add -b` honest:
# handed a name that resolves only as a remote-tracking ref, git applies DWIM to
# it — on git 2.47 it silently discards the explicit `-b`, checks out the *base*
# branch, exits 0 and appends a `[branch "<base>"]` stanza to the shared
# `.git/config`; on older git it fails with "Not a valid object name".
# `--no-guess-remote` does not suppress either; a SHA does (bug #2830).
resolve_base_commit() {
    local repo="$1" base="$2" candidate remote sha
    local -a candidates=("refs/heads/$base")
    while read -r remote; do
        [ -n "$remote" ] && candidates+=("refs/remotes/$remote/$base")
    done < <(git -C "$repo" remote)
    candidates+=("$base")

    for candidate in "${candidates[@]}"; do
        if sha="$(git -C "$repo" rev-parse --verify --quiet "$candidate^{commit}" 2>/dev/null)"; then
            printf '%s' "$sha"
            return 0
        fi
    done
    return 1
}

if [ "${1:-}" = "--relativize" ]; then
    WORKTREE_PATH="${2:?Usage: worktree-add.sh --relativize <worktree-path>}"
    relativize "$WORKTREE_PATH" || exit 1
    exit 0
fi

REPO_ROOT_ARG="${1:?Usage: worktree-add.sh <repo-root> <worktree-path> <branch> [<base>]}"
WORKTREE_PATH_ARG="${2:?Usage: worktree-add.sh <repo-root> <worktree-path> <branch> [<base>]}"
BRANCH="${3:?Usage: worktree-add.sh <repo-root> <worktree-path> <branch> [<base>]}"
BASE="${4:-}"

REPO_ROOT="$(cd "$REPO_ROOT_ARG" && git rev-parse --show-toplevel)"

# A relative worktree path resolves under the repo root, not $PWD.
case "$WORKTREE_PATH_ARG" in
    /*)
        WORKTREE_PATH="$WORKTREE_PATH_ARG"
        ;;
    *)
        WORKTREE_PATH="$REPO_ROOT/$WORKTREE_PATH_ARG"
        ;;
esac

# `git worktree add --relative-paths` (git >= 2.48) is deliberately NOT used.
# Passing it makes git record the `relativeWorktrees` repository-format
# extension and bump `core.repositoryformatversion` to 1 in the *shared*
# `.git/config` — i.e. the main checkout's, since a worktree shares that file.
# That write is permanent (it survives `git worktree remove`) and makes every
# git older than 2.48 refuse the repository outright with "unknown repository
# extension found: relativeworktrees". relativize() below produces the same
# relative pointer with zero config mutation, so it is always used instead.

if git -C "$REPO_ROOT" show-ref --verify --quiet "refs/heads/$BRANCH"; then
    git -C "$REPO_ROOT" worktree add "$WORKTREE_PATH" "$BRANCH" >&2
elif [ -n "$BASE" ]; then
    if ! BASE_COMMIT="$(resolve_base_commit "$REPO_ROOT" "$BASE")"; then
        echo "Error: base '$BASE' does not resolve to a commit in $REPO_ROOT" >&2
        exit 1
    fi
    git -C "$REPO_ROOT" worktree add -b "$BRANCH" "$WORKTREE_PATH" "$BASE_COMMIT" >&2
else
    git -C "$REPO_ROOT" worktree add -b "$BRANCH" "$WORKTREE_PATH" >&2
fi

# `git worktree add` just registered the worktree under its own canonical,
# absolute resolution of $WORKTREE_PATH — which may differ textually from
# $WORKTREE_PATH itself if the caller passed a path containing `.`/`..` or a
# symlinked component (WORKTREE_PATH is never canonicalized above; it's only
# prefixed with $REPO_ROOT when relative). relpath()'s prefix comparison
# needs both sides in the same canonical form, so re-resolve WORKTREE_PATH
# now that the directory exists.
WORKTREE_PATH="$(realpath "$WORKTREE_PATH")"

# The .git pointer git just wrote is absolute. Rewrite only the worktree's own
# .git file to make it relative — do NOT touch .git/worktrees/<name>/gitdir,
# since a relative pointer there makes older git consider the worktree prunable
# and refuse `git worktree remove`. `git worktree add` above already created
# and registered the worktree, so a failure here must remove it — otherwise a
# retry finds the path already occupied by a broken checkout.
if ! relativize "$WORKTREE_PATH"; then
    git -C "$REPO_ROOT" worktree remove --force "$WORKTREE_PATH" 2>/dev/null || rm -rf "$WORKTREE_PATH"
    exit 1
fi

echo "WORKTREE=$WORKTREE_PATH"
