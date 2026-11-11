-- First migration after the baseline; rehearses the deploy workflow.
create index if not exists chisme_active_contact_id_idx
  on public.chisme_active (contact_id);
