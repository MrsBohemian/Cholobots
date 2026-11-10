#!/usr/bin/env python3
# ABOUTME: Migration linter and push-plan guard for supabase/migrations/.
# ABOUTME: `lint` keeps migrations pushable and append-only; `check-push-plan` blocks a push that would replay the baseline.
import argparse
import os
import re
import subprocess
import sys

MIGRATION_RE = re.compile(r"^(\d{14})_[a-z0-9_]+\.sql$")
PLAN_TOKEN_RE = re.compile(r"\d{14}_[A-Za-z0-9_]+\.sql")
BASELINE_SUFFIX = "_remote_baseline.sql"
IGNORED_NAMES = {".gitkeep"}


def _timestamp(name):
    match = MIGRATION_RE.match(os.path.basename(name))
    return match.group(1) if match else None


def lint_names(names):
    """Errors for a migrations directory listing (file names only)."""
    errors = []
    seen = {}
    valid = []
    for name in sorted(names):
        if name in IGNORED_NAMES:
            continue
        ts = _timestamp(name)
        if ts is None:
            errors.append(
                f"{name}: not a valid migration name (expected <14-digit UTC timestamp>_<lowercase_snake>.sql)"
            )
            continue
        if ts in seen:
            errors.append(f"{name}: duplicate timestamp {ts} (also used by {seen[ts]})")
        else:
            seen[ts] = name
        valid.append(name)

    baselines = [n for n in valid if n.endswith(BASELINE_SUFFIX)]
    if len(baselines) == 0:
        errors.append(f"no baseline migration (<timestamp>{BASELINE_SUFFIX}) found")
    elif len(baselines) > 1:
        errors.append(f"several baseline migrations found: {', '.join(baselines)}")
    elif valid and _timestamp(baselines[0]) != min(_timestamp(n) for n in valid):
        errors.append(f"{baselines[0]}: the baseline must be the earliest migration")
    return errors


def lint_against_base(changes, base_names):
    """Errors for a PR's changes relative to the migrations already at the base.

    `changes` are tuples from `git diff --name-status -M`: (status, path) or,
    for renames and copies, (status, old_path, new_path).
    """
    errors = []
    base_stamps = [ts for ts in (_timestamp(n) for n in base_names) if ts]
    newest = max(base_stamps) if base_stamps else None
    for change in changes:
        status, paths = change[0], change[1:]
        if status[0] in ("M", "D", "R"):
            errors.append(
                f"{paths[0]}: merged migrations are never edited, renamed or deleted; add a new migration"
            )
        elif status[0] in ("A", "C"):
            path = paths[-1]
            ts = _timestamp(path)
            if ts and newest and ts <= newest:
                errors.append(
                    f"{path}: timestamp {ts} is not newer than the newest merged migration {newest}; "
                    "it would be skipped by db push (regenerate with supabase migration new)"
                )
    return errors


def pending_from_plan(text):
    """Migration file names in a `supabase db push --dry-run` output, in order, de-duplicated."""
    found = []
    for token in PLAN_TOKEN_RE.findall(text):
        if token not in found:
            found.append(token)
    return found


def _git(*args):
    return subprocess.run(["git", *args], capture_output=True, text=True, check=False)


def _fail_git(result, what):
    print(f"error: {what}: {result.stderr.strip()}", file=sys.stderr)
    return 2


def cmd_lint(args):
    if not os.path.isdir(args.dir):
        print(f"error: migrations directory not found: {args.dir}", file=sys.stderr)
        return 2
    errors = lint_names(os.listdir(args.dir))

    if args.base:
        merge_base = _git("merge-base", args.base, "HEAD")
        if merge_base.returncode != 0:
            return _fail_git(merge_base, f"cannot resolve merge base with {args.base}")
        base_sha = merge_base.stdout.strip()
        tree_dir = args.dir.rstrip("/") + "/"
        listing = _git("ls-tree", "--name-only", base_sha, tree_dir)
        if listing.returncode != 0:
            return _fail_git(listing, f"cannot list {tree_dir} at {base_sha}")
        base_names = [os.path.basename(p) for p in listing.stdout.splitlines()]
        diff = _git("diff", "--name-status", "-M", f"{base_sha}..HEAD", "--", args.dir)
        if diff.returncode != 0:
            return _fail_git(diff, "git diff failed")
        changes = [tuple(line.split("\t")) for line in diff.stdout.splitlines() if line.strip()]
        errors += lint_against_base(changes, base_names)

    for error in errors:
        print(error)
    return 1 if errors else 0


def cmd_check_push_plan(args):
    text = sys.stdin.read() if args.plan == "-" else open(args.plan, encoding="utf-8").read()
    if not text.strip():
        print("error: empty push plan; refusing to treat it as a pass", file=sys.stderr)
        return 2
    pending = pending_from_plan(text)
    baselines = [n for n in pending if n.endswith(BASELINE_SUFFIX)]
    if baselines:
        name = baselines[0]
        print(
            f"Baseline {name} is not recorded as applied on the hosted project. "
            f"Run supabase migration repair --status applied {_timestamp(name)} first "
            "(docs/database-migrations.md, go-live checklist)."
        )
        return 1
    if pending:
        print("Pending migrations:")
        for name in pending:
            print(f"  {name}")
    elif "up to date" in text.lower():
        print("Nothing to push: remote database is up to date.")
    else:
        print("error: could not read a push plan from the input", file=sys.stderr)
        return 2
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    lint = sub.add_parser("lint", help="check supabase/migrations/")
    lint.add_argument("--dir", default="supabase/migrations")
    lint.add_argument("--base", help="git ref of the PR base; also checks append-only rules")
    lint.set_defaults(func=cmd_lint)

    plan = sub.add_parser("check-push-plan", help="fail when the baseline is pending")
    plan.add_argument("plan", help="file with `supabase db push --dry-run` output, or - for stdin")
    plan.set_defaults(func=cmd_check_push_plan)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
