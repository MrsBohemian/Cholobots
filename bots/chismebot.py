-- 1. Archive old queue table instead of deleting it
alter table if exists chisme_communication_queue
rename to zz_archive_chisme_communication_queue;


-- 2. Finish shaping chisme_contacts for active index-card workflow

alter table chisme_contacts
add column if not exists active_communication boolean default false;

alter table chisme_contacts
add column if not exists active_reason text;

alter table chisme_contacts
add column if not exists active_priority int4 default 50;

alter table chisme_contacts
add column if not exists active_since date;

alter table chisme_contacts
add column if not exists active_owner text default 'Daniel';

alter table chisme_contacts
add column if not exists daily_touch_date date;

alter table chisme_contacts
add column if not exists daily_touch_counted boolean default false;

alter table chisme_contacts
add column if not exists next_followup_date date;

update chisme_contacts
set next_followup_date = next_contact_date
where next_followup_date is null
  and next_contact_date is not null;


-- 3. Add interaction fields for synthesis + command center load bar

alter table chisme_interactions
add column if not exists summary_delta text;

alter table chisme_interactions
add column if not exists touch_counts_for_today boolean default true;

alter table chisme_interactions
add column if not exists next_followup_date date;

update chisme_interactions
set next_followup_date = next_contact_date
where next_followup_date is null
  and next_contact_date is not null;


-- 4. Helpful indexes for mobile app + bot lookup

create index if not exists idx_chisme_contacts_active
on chisme_contacts(active_communication, active_priority, next_followup_date);

create index if not exists idx_chisme_contacts_name
on chisme_contacts(name);

create index if not exists idx_chisme_interactions_contact
on chisme_interactions(contact_id, created_at desc);

create index if not exists idx_chisme_interactions_today_touch
on chisme_interactions(interaction_date, touch_counts_for_today);
