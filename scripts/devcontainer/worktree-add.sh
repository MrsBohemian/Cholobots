#!/usr/bin/env bash
# ABOUTME: Creates a git worktree with a relative .git pointer, so the devcontainer CLI can mount
# ABOUTME: the common dir on any git version. --relativize fixes an existing worktree.
set -euo pipefail

# Relative path from $2 (base dir) to $1 (target), in pure bash because python3
# and `realpath --relative-to` are not on every host. Both must be absolute,
# with no trailing slash.
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

# Makes <worktree-path>/.git's `gitdir:` line relative if it is absolute.
# Returns non-zero rather than exiting, so the caller can remove the worktree.
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
            ;;
    esac
}

# Resolves <base> to a commit SHA: local branch, then each remote's tracking ref,
# then any revision (tag, SHA). Pass `git worktree add -b` a SHA, never a name
# that exists only as a remote-tracking ref: git 2.47 then ignores -b, checks out
# the base branch and writes a [branch] stanza into the shared .git/config; older
# git fails with "Not a valid object name". --no-guess-remote prevents neither.
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

# Not `git worktree add --relative-paths` (git >= 2.48): it permanently adds the
# relativeWorktrees extension to the main checkout's shared .git/config, and git
# older than 2.48 then refuses the repository. relativize() changes no config.

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

# git registered the canonical path; relpath() needs the same form, which can
# differ when the argument holds `..` or a symlink.
WORKTREE_PATH="$(realpath "$WORKTREE_PATH")"

# Rewrite only the worktree's .git file, never .git/worktrees/<name>/gitdir:
# older git treats a relative path there as prunable and refuses
# `git worktree remove`. On failure, remove the worktree so a retry starts clean.
if ! relativize "$WORKTREE_PATH"; then
    git -C "$REPO_ROOT" worktree remove --force "$WORKTREE_PATH" 2>/dev/null || rm -rf "$WORKTREE_PATH"
    exit 1
fi

echo "WORKTREE=$WORKTREE_PATH"
