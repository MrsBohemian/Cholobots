# Database migrations

The schema of record is `supabase/migrations/`. This page is the workflow for changing it (Taiga story #49).

## 1. The rule

No schema edits in the Supabase dashboard or SQL editor, ever. Every change is a migration file committed in the PR with the code that needs it:

- `supabase migration new <name>` and write the SQL by hand, or
- change the local database through local Studio, then `supabase db diff -f <name>`.

Merged migration files are never edited, renamed or deleted; fix forward with a new one. Use the pinned CLI, 2.121.0 (`npx --yes supabase@2.121.0 ...` when it is not installed). File names are `<14-digit UTC timestamp>_<lowercase_snake>.sql`, and a new file's timestamp must be newer than every merged migration or `db push` skips it.

`scripts/supabase_migrations.py lint` enforces the naming and append-only rules; with `--base <git ref>` it also rejects edits, renames and deletions of migrations that already exist at the base.

## 2. What a migration must include

Everything the change needs, not only DDL: tables, columns, indexes, constraints, RLS (`enable row level security`, `create policy`), grants, views (`chisme_dashboard_contacts` is a view the dashboards update with the publishable key), functions and triggers.

- Always write `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` explicitly on every new table. The baseline dump contains `public.rls_auto_enable()` but not the event trigger that calls it, so tables created locally do not get RLS automatically while the hosted project probably does. Do not rely on it.
- Read `db diff` output before committing it: check that it captured policies, grants and view definitions, and add by hand what it missed.
- Objects outside `public` (auth, storage and so on) come from the local stack's defaults, not from the baseline.

## 3. Review checklist for a migration PR

- `python3 scripts/supabase_migrations.py lint --base origin/main` passes.
- `supabase db reset` replays every migration locally without error.
- New tables enable RLS explicitly (section 2).
- For a shared table (`crudo_projects`, `crudo_project_tasks`, `crudo_labor_entries`, `chisme_contacts`) or any table the dashboards read, the PR lists every reader (bots and `../Cholobots-Mobile` pages) and confirms they still work.
- A destructive change (drop, rename, type change) says how existing rows survive.
- No production data in the file.

## 4. Local loop

1. `supabase start` (Docker; see README "Local development").
2. `supabase migration new <name>`, write the SQL.
3. `supabase db reset` to replay the baseline plus your migration.
4. `supabase db diff --local` must print no changes.

## 5. Deploy workflow and the gate

`.github/workflows/supabase-db-push.yml` runs on a push to `main` that touches `supabase/migrations/**`, and on manual dispatch. Its `db-push` job lints the migrations, links the hosted project, runs `supabase db push --dry-run` and pipes the plan through `scripts/supabase_migrations.py check-push-plan`, then runs `supabase db push`. The guard fails the run when the plan would apply the baseline, which would replay the whole schema over production.

`.github/workflows/supabase-migrations-lint.yml` is the PR-side check. It runs on `pull_request` (never `pull_request_target`) when `supabase/migrations/**` changes, holds no secrets and no environment, fetches the PR base and runs `scripts/supabase_migrations.py lint --base origin/<base>`, so editing, renaming or deleting an already-merged migration fails the PR. Make it a required status check in branch protection to enforce it.

The push workflow ships gated closed:

- The `db-push` job runs only when the repository variable `SUPABASE_DB_PUSH_ENABLED` is `true`. The variable does not exist yet; only Heaven, the repo owner, can set it. Otherwise the `gate-closed` job records a notice.
- The job uses the protected `supabase-production` environment, with Jon as required reviewer. Its secrets (`SUPABASE_ACCESS_TOKEN`, `SUPABASE_PROJECT_REF`, optionally `SUPABASE_DB_PASSWORD`) are environment secrets, never repository-wide.
- Runs queue (no cancel) so a push in flight is never interrupted.
- The workflow must never gain a `pull_request` or `pull_request_target` trigger: the repo is public and the job holds production credentials.

Current state: switched off. No production writes until Jon explicitly allows them. While the gate is closed, merged migrations queue up and the first push after go-live applies all of them.

`SUPABASE_ACCESS_TOKEN` is a personal Supabase token with account-wide power. It lives only as an environment secret limited to `main` and gated by Jon's approval.

The rehearsal migration `20261110020959_rehearsal_chisme_active_contact_id_idx.sql` (an index on `chisme_active.contact_id`) proved the loop on the local stack. It is the first migration the hosted project will receive.

## 6. Go-live checklist

Tracked in story #178 (tasks #114 to #117), gated on Jon allowing production writes.

1. Heaven creates the `supabase-production` environment (Jon as required reviewer, deployment limited to `main`) and its secrets.
2. Jon runs a read-only drift check: a fresh `supabase db dump --linked --schema public` diffed against `20261109194905_remote_baseline.sql`.
3. The hosted project does not record the baseline as applied. Jon runs `supabase migration repair --status applied 20261109194905`, then `supabase migration list --linked`, then `supabase db push --dry-run 2>&1` piped into `python3 scripts/supabase_migrations.py check-push-plan -` (the CLI prints the plan to stderr, so `2>&1` is required). Only the rehearsal migration (and later ones) may be pending. Without the repair, the workflow's guard fails the run.
4. Heaven sets `SUPABASE_DB_PUSH_ENABLED` to `true`.
5. Jon dispatches the workflow, approves the environment run and verifies the result.
6. A one-line migration merged through a PR proves the merge trigger.

## 7. Turning it off

Heaven deletes the variable or sets it to `false`. Jon rejects any in-flight run waiting for approval.
