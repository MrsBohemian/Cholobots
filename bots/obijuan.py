-- ObiJuan persistent quests and labor records
create extension if not exists pgcrypto;

create table if not exists public.obijuan_quests (
  quest_id text primary key,
  customer_name text not null,
  title text not null,
  status text not null default 'open',
  location text,
  customer_budget text,
  customer_willingness text,
  job_summary text,
  accepted_by_user_id bigint,
  accepted_by_name text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.obijuan_quest_assignments (
  id uuid primary key default gen_random_uuid(),
  quest_id text not null references public.obijuan_quests(quest_id) on delete cascade,
  worker_id text not null,
  worker_name text not null,
  accepted_price numeric(12,2) not null default 0,
  paid_amount numeric(12,2),
  status text not null default 'accepted',
  notes text,
  payout_notes text,
  accepted_at timestamptz not null default now(),
  paid_at timestamptz,
  updated_at timestamptz not null default now(),
  unique (quest_id, worker_id)
);

create table if not exists public.obijuan_owner_time (
  id uuid primary key default gen_random_uuid(),
  quest_id text not null references public.obijuan_quests(quest_id) on delete cascade,
  owner_name text not null,
  quantity numeric(10,2) not null default 0,
  unit text not null default 'days',
  notes text,
  created_by text,
  created_at timestamptz not null default now()
);

create table if not exists public.obijuan_quest_notes (
  id uuid primary key default gen_random_uuid(),
  quest_id text not null references public.obijuan_quests(quest_id) on delete cascade,
  author_id text,
  author_name text,
  note_type text not null default 'note',
  body text not null,
  created_at timestamptz not null default now()
);

create table if not exists public.obijuan_quest_updates (
  id uuid primary key default gen_random_uuid(),
  quest_id text not null references public.obijuan_quests(quest_id) on delete cascade,
  author_id text,
  author_name text,
  body text not null,
  created_at timestamptz not null default now()
);

create index if not exists obijuan_assignments_quest_idx
  on public.obijuan_quest_assignments (quest_id);

create index if not exists obijuan_owner_time_quest_idx
  on public.obijuan_owner_time (quest_id);

create index if not exists obijuan_notes_quest_idx
  on public.obijuan_quest_notes (quest_id);

create index if not exists obijuan_updates_quest_idx
  on public.obijuan_quest_updates (quest_id);
