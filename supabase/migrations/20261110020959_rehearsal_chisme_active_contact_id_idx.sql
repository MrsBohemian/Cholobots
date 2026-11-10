-- Rehearsal migration for story #49: proves the migration workflow end to end.
create index if not exists chisme_active_contact_id_idx
  on public.chisme_active (contact_id);
