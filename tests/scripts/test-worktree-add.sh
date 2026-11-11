#!/usr/bin/env bash
# ABOUTME: Behavioural test for scripts/devcontainer/worktree-add.sh (relative-gitdir rewriting, config
# ABOUTME: integrity and remote-tracking bases such as upstream/main).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WORKTREE_ADD="$REPO_ROOT/scripts/devcontainer/worktree-add.sh"

PASS=0
FAIL=0

assert_true() {
  local test_name="$1" condition="$2"
  if eval "$condition"; then
    echo "  PASS: $test_name"
    PASS=$((PASS + 1))
  else
    echo "  FAIL: $test_name"
    FAIL=$((FAIL + 1))
  fi
}

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

export GIT_CONFIG_GLOBAL=/dev/null
export GIT_CONFIG_SYSTEM=/dev/null

git init -q -b main "$TMP/repo"
git -C "$TMP/repo" -c user.email=t@t -c user.name=t commit -q --allow-empty -m init

echo "=== worktree-add.sh behavioural tests ==="

# 1. Creates a worktree on a new branch from a given base and prints WORKTREE=<abs path>.
OUTPUT="$("$WORKTREE_ADD" "$TMP/repo" "$TMP/wt1" feature/x main)"
assert_true "prints WORKTREE=<abs path> for wt1" \
  "[ \"\$OUTPUT\" = \"WORKTREE=$TMP/wt1\" ]"
assert_true "wt1 has the repo's files" \
  "[ -e \"$TMP/wt1/.git\" ]"
assert_true "wt1 HEAD is feature/x" \
  "[ \"\$(git -C \"$TMP/wt1\" rev-parse --abbrev-ref HEAD)\" = feature/x ]"

# 2. The worktree's .git file's gitdir: is a relative path.
assert_true "wt1/.git is a file" \
  "[ -f \"$TMP/wt1/.git\" ]"
GITDIR_LINE="$(cat "$TMP/wt1/.git")"
GITDIR_VALUE="${GITDIR_LINE#gitdir: }"
assert_true "wt1/.git gitdir value is not absolute" \
  "[[ \"\$GITDIR_VALUE\" != /* ]]"

# 3. git worktree remove succeeds afterwards (the git < 2.48 regression guard).
assert_true "git worktree remove succeeds on wt1" \
  "git -C \"$TMP/repo\" worktree remove \"$TMP/wt1\""

# 4. Re-running against an already-existing branch checks it out instead of failing.
git -C "$TMP/repo" branch feature/y main
OUTPUT2="$("$WORKTREE_ADD" "$TMP/repo" "$TMP/wt2" feature/y main)"
assert_true "prints WORKTREE=<abs path> for wt2 (existing branch)" \
  "[ \"\$OUTPUT2\" = \"WORKTREE=$TMP/wt2\" ]"
assert_true "wt2 HEAD is feature/y" \
  "[ \"\$(git -C \"$TMP/wt2\" rev-parse --abbrev-ref HEAD)\" = feature/y ]"

# 5. --relativize rewrites an absolute pointer and leaves a relative one alone.
"$WORKTREE_ADD" "$TMP/repo" "$TMP/wt3" feature/z main >/dev/null
ABS_GITDIR="$TMP/repo/.git/worktrees/wt3"
echo "gitdir: $ABS_GITDIR" > "$TMP/wt3/.git"
"$WORKTREE_ADD" --relativize "$TMP/wt3"
REL_LINE="$(cat "$TMP/wt3/.git")"
REL_VALUE="${REL_LINE#gitdir: }"
assert_true "--relativize rewrites an absolute pointer to relative" \
  "[[ \"\$REL_VALUE\" != /* ]]"
assert_true "--relativize result still resolves" \
  "git -C \"$TMP/wt3\" status >/dev/null 2>&1"
"$WORKTREE_ADD" --relativize "$TMP/wt3"
REL_LINE_2="$(cat "$TMP/wt3/.git")"
assert_true "--relativize is a no-op on an already-relative pointer" \
  "[ \"\$REL_LINE\" = \"\$REL_LINE_2\" ]"

# 6. Adding a worktree must not change the source repo's shared `.git/config` (e.g. the
# relativeWorktrees extension that `--relative-paths` adds, which git < 2.48 refuses).
# Own repo, so the comparison is byte-exact.
git init -q -b main "$TMP/cfgrepo"
git -C "$TMP/cfgrepo" -c user.email=t@t -c user.name=t commit -q --allow-empty -m init
CFG_BEFORE="$(cat "$TMP/cfgrepo/.git/config")"
"$WORKTREE_ADD" "$TMP/cfgrepo" "$TMP/wt-cfg" feature/cfg main >/dev/null
CFG_AFTER="$(cat "$TMP/cfgrepo/.git/config")"
assert_true "worktree add leaves the source repo's .git/config byte-identical" \
  '[ "$CFG_BEFORE" = "$CFG_AFTER" ]'
assert_true "worktree add does not set extensions.relativeWorktrees" \
  "! git -C \"$TMP/cfgrepo\" config --get extensions.relativeWorktrees >/dev/null 2>&1"
assert_true "worktree add leaves core.repositoryformatversion at 0" \
  "[ \"\$(git -C \"$TMP/cfgrepo\" config --get core.repositoryformatversion)\" = 0 ]"
assert_true "git worktree remove succeeds on the config-integrity worktree" \
  "git -C \"$TMP/cfgrepo\" worktree remove \"$TMP/wt-cfg\""

# 7. A worktree path containing `..` still gets a correct relative gitdir.
mkdir -p "$TMP/nested/dir"
DOTDOT_PATH="$TMP/nested/dir/../../wt-dotdot"
"$WORKTREE_ADD" "$TMP/repo" "$DOTDOT_PATH" feature/dotdot main >/dev/null
assert_true "worktree created via a path with '..' has a relative gitdir" \
  "[[ \"\$(cat \"$TMP/wt-dotdot/.git\")\" != *'gitdir: /'* ]]"
assert_true "worktree created via a path with '..' resolves" \
  "git -C \"$TMP/wt-dotdot\" status >/dev/null 2>&1"
assert_true "git worktree remove succeeds on the '..'-created worktree" \
  "git -C \"$TMP/repo\" worktree remove \"$TMP/wt-dotdot\""

# 8. A <base> that exists only as a remote-tracking ref still yields a worktree on the
# requested branch and leaves `.git/config` byte-identical (see resolve_base_commit in
# worktree-add.sh for how git's DWIM breaks this). A plain clone, like a developer's
# checkout, has every unfetched branch only as `origin/<branch>`.
git init -q -b main "$TMP/dwim-origin"
git -C "$TMP/dwim-origin" -c user.email=t@t -c user.name=t commit -q --allow-empty -m init
git -C "$TMP/dwim-origin" branch base-only main
git clone -q "file://$TMP/dwim-origin" "$TMP/dwim-clone"
assert_true "base-only exists solely as a remote-tracking ref in the clone" \
  "git -C \"$TMP/dwim-clone\" show-ref --verify --quiet refs/remotes/origin/base-only && ! git -C \"$TMP/dwim-clone\" show-ref --verify --quiet refs/heads/base-only"
DWIM_CFG_BEFORE="$(sha256sum <"$TMP/dwim-clone/.git/config")"
set +e
"$WORKTREE_ADD" "$TMP/dwim-clone" "$TMP/wt-dwim" feature/dwim base-only >/dev/null 2>"$TMP/dwim-err"
DWIM_RC=$?
set -e
DWIM_CFG_AFTER="$(sha256sum <"$TMP/dwim-clone/.git/config")"
assert_true "remote-only base: worktree-add.sh succeeds" \
  "[ \"$DWIM_RC\" -eq 0 ]"
assert_true "remote-only base: worktree HEAD is the requested branch, not the base" \
  "[ \"\$(git -C \"$TMP/wt-dwim\" rev-parse --abbrev-ref HEAD)\" = feature/dwim ]"
assert_true "remote-only base: new branch points at the base commit" \
  "[ \"\$(git -C \"$TMP/dwim-clone\" rev-parse feature/dwim)\" = \"\$(git -C \"$TMP/dwim-clone\" rev-parse refs/remotes/origin/base-only)\" ]"
assert_true "remote-only base: main checkout's .git/config is byte-identical" \
  '[ "$DWIM_CFG_BEFORE" = "$DWIM_CFG_AFTER" ]'
assert_true "remote-only base: no tracking stanza written for the base branch" \
  "! git -C \"$TMP/dwim-clone\" config --get branch.base-only.remote >/dev/null 2>&1"
assert_true "git worktree remove succeeds on the remote-only-base worktree" \
  "git -C \"$TMP/dwim-clone\" worktree remove \"$TMP/wt-dwim\""

# 9. The Cholobots pipeline base: `upstream/main`, a remote-tracking ref of a remote named
# `upstream`, resolved through the final `<base>` candidate.
git init -q --bare -b main "$TMP/up-remote.git"
git -C "$TMP/repo" remote add upstream "file://$TMP/up-remote.git"
git -C "$TMP/repo" push -q upstream main
git -C "$TMP/repo" fetch -q upstream
"$WORKTREE_ADD" "$TMP/repo" "$TMP/wt-up" feature/z-up upstream/main >/dev/null
assert_true "upstream/main base: worktree HEAD is the requested branch" \
  "[ \"\$(git -C \"$TMP/wt-up\" rev-parse --abbrev-ref HEAD)\" = feature/z-up ]"
assert_true "upstream/main base: HEAD is at upstream/main's SHA" \
  "[ \"\$(git -C \"$TMP/wt-up\" rev-parse HEAD)\" = \"\$(git -C \"$TMP/repo\" rev-parse refs/remotes/upstream/main)\" ]"
assert_true "upstream/main base: no tracking stanza written for main" \
  "! git -C \"$TMP/repo\" config --get branch.upstream/main.remote >/dev/null 2>&1"

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ]
