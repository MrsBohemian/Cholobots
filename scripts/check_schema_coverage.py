#!/usr/bin/env python3
# ABOUTME: Lists every Supabase table/view the bots and dashboards reference and checks the migrations define it.
# ABOUTME: Reports each object's kind, RLS state and policy count; exit 1 when a referenced object is missing.
"""
Usage: python3 scripts/check_schema_coverage.py [--repo-root .] [--migrations supabase/migrations]
                                                [--mobile-dir <dir>] [--json]

Bot references are `.table("name")` calls in bots/*.py and db/database.py.
Dashboard references are `.from("name")` calls in <mobile-dir>/*.html and
<mobile-dir>/assets/*.js. --mobile-dir defaults to mobile/ when it exists,
else ../Cholobots-Mobile; a directory that does not exist is exit 2.

Only the public schema (or unqualified names) counts. Exit codes: 0 every
referenced object is defined, 1 at least one is missing, 2 usage error.
A dashboard-referenced table with RLS off is printed as WARN, not a failure.
"""
import argparse
import json
import re
import sys
from pathlib import Path

_IDENT = r'(?:"([A-Za-z_][A-Za-z0-9_$]*)"|([A-Za-z_][A-Za-z0-9_$]*))'
_QUALIFIED = rf"(?:{_IDENT}\s*\.\s*)?{_IDENT}"

_BOT_REF = re.compile(r"""\.table\(\s*["']([a-z_][a-z0-9_]*)["']""")
_DASHBOARD_REF = re.compile(r"""\.from\(\s*["']([a-z_][a-z0-9_]*)["']""")

_CREATE_TABLE = re.compile(
    rf"^\s*CREATE\s+(?:UNLOGGED\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?{_QUALIFIED}",
    re.I | re.M,
)
_CREATE_VIEW = re.compile(
    rf"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(MATERIALIZED\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?{_QUALIFIED}",
    re.I | re.M,
)
_ENABLE_RLS = re.compile(
    rf"^\s*ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?{_QUALIFIED}\s+ENABLE\s+ROW\s+LEVEL\s+SECURITY",
    re.I | re.M,
)
_CREATE_POLICY = re.compile(
    rf'^\s*CREATE\s+POLICY\s+(?:"(?:[^"]|"")*"|[A-Za-z_][A-Za-z0-9_]*)\s+ON\s+{_QUALIFIED}',
    re.I | re.M,
)


def _public_name(match, first_group):
    """Return the object name of a schema-qualified match, or None outside public."""
    schema = match.group(first_group) or match.group(first_group + 1)
    name = match.group(first_group + 2) or match.group(first_group + 3)
    if schema is not None and schema.lower() != "public":
        return None
    return name


def _refs(pattern, paths):
    found = set()
    for path in paths:
        found.update(pattern.findall(Path(path).read_text(encoding="utf-8")))
    return found


def bot_tables(paths):
    """Table names passed to `.table("...")` in the given Python files."""
    return _refs(_BOT_REF, paths)


def dashboard_tables(paths):
    """Table or view names passed to `.from("...")` in the given dashboard files."""
    return _refs(_DASHBOARD_REF, paths)


def schema_objects(sql):
    """Map each public table/view defined in `sql` to its kind, RLS state and policy count."""
    objects = {}
    for match in _CREATE_TABLE.finditer(sql):
        name = _public_name(match, 1)
        if name:
            objects.setdefault(name, {"kind": "table", "rls": False, "policies": 0})
    for match in _CREATE_VIEW.finditer(sql):
        name = _public_name(match, 2)
        if name:
            kind = "materialized_view" if match.group(1) else "view"
            objects.setdefault(name, {"kind": kind, "rls": False, "policies": 0})
    for match in _ENABLE_RLS.finditer(sql):
        name = _public_name(match, 1)
        if name in objects:
            objects[name]["rls"] = True
    for match in _CREATE_POLICY.finditer(sql):
        name = _public_name(match, 1)
        if name in objects:
            objects[name]["policies"] += 1
    return objects


def _default_mobile_dir(repo_root):
    local = repo_root / "mobile"
    return local if local.is_dir() else (repo_root / ".." / "Cholobots-Mobile").resolve()


def build_report(repo_root, migrations, mobile_dir):
    bot_paths = sorted((repo_root / "bots").glob("*.py"))
    legacy = repo_root / "db" / "database.py"
    if legacy.is_file():
        bot_paths.append(legacy)
    dash_paths = sorted(mobile_dir.glob("*.html")) + sorted((mobile_dir / "assets").glob("*.js"))

    sql = "\n".join(p.read_text(encoding="utf-8") for p in sorted(migrations.glob("*.sql")))
    objects = schema_objects(sql)
    bots = bot_tables(bot_paths)
    dashboards = dashboard_tables(dash_paths)

    expected = []
    for name in sorted(bots | dashboards):
        referenced_by = [label for label, refs in (("bots", bots), ("dashboards", dashboards)) if name in refs]
        info = objects.get(name)
        if info is None:
            status = "MISSING"
        elif info["kind"] == "table" and "dashboards" in referenced_by and not info["rls"]:
            status = "WARN"
        else:
            status = "OK"
        row = {"name": name, "status": status, "referenced_by": referenced_by}
        if info:
            row.update(info)
        expected.append(row)
    unreferenced = sorted(set(objects) - bots - dashboards)
    return {"expected": expected, "unreferenced": unreferenced, "objects": objects}


def _format_row(row):
    refs = ",".join(row["referenced_by"])
    if row["status"] == "MISSING":
        return f"MISSING {row['name']} [{refs}]"
    rls = ("on" if row["rls"] else "off") if row["kind"] == "table" else "n/a"
    return f"{row['status']} {row['kind']} {row['name']} rls={rls} policies={row['policies']} [{refs}]"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--repo-root", default=".", type=Path)
    parser.add_argument("--migrations", type=Path, help="default: <repo-root>/supabase/migrations")
    parser.add_argument("--mobile-dir", type=Path, help="default: <repo-root>/mobile, else ../Cholobots-Mobile")
    parser.add_argument("--json", action="store_true", help="print the report as JSON")
    args = parser.parse_args(argv)

    repo_root = args.repo_root.resolve()
    migrations = args.migrations or repo_root / "supabase" / "migrations"
    mobile_dir = args.mobile_dir or _default_mobile_dir(repo_root)
    for label, path in (("migrations", migrations), ("dashboard", mobile_dir)):
        if not path.is_dir():
            print(f"check_schema_coverage: {label} directory not found: {path}", file=sys.stderr)
            return 2

    report = build_report(repo_root, migrations, mobile_dir)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        for row in report["expected"]:
            print(_format_row(row))
        if report["unreferenced"]:
            print("\nDefined but not referenced by the bots or dashboards:")
            for name in report["unreferenced"]:
                info = report["objects"][name]
                rls = ("on" if info["rls"] else "off") if info["kind"] == "table" else "n/a"
                print(f"  {info['kind']} {name} rls={rls} policies={info['policies']}")
    return 1 if any(row["status"] == "MISSING" for row in report["expected"]) else 0


if __name__ == "__main__":
    sys.exit(main())
