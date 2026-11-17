-- Local development seed: one obviously fictitious contracting business.
--
-- Run by `supabase db reset` (supabase/config.toml [db.seed]) after the migrations,
-- against the LOCAL Docker database only. Never run it on the hosted project.
-- Every name ends in " (Test)" or reads "Test <Role>"; phones are in the reserved
-- 555-0100..0199 range; emails use example.com; Discord IDs are the placeholders
-- 100000000000000001..100000000000000004 and never a real account.
--
-- Shape rules: explicit column lists, ids from the table defaults, foreign keys resolved by
-- subselect on seeded natural keys, "today" and "this Monday" computed in America/Chicago.
-- There is no profiles table yet: any test account reaches Metichebot as UNKNOWN:<id>,
-- which reads the `heaven` calendar key, so the daily_tasks below use it.

-- Chismebot ---------------------------------------------------------------------------
insert into public.chisme_contacts
  (name, source, phone, email, address, status, hotlist_temperature, pipeline_stage, next_action, chisme_summary, updated_at)
values
  ('Fern Fridgeman (Test)', 'seed', '2105550101', 'fern.fridgeman@example.com', '101 Test St, Testville TX',
   'lead', 50, 'New Lead', 'Contact customer / schedule site visit', 'Fictitious fridge-tier lead for local testing.', now()),
  ('Stella Stovetop (Test)', 'seed', '2105550102', 'stella.stovetop@example.com', '102 Test St, Testville TX',
   'lead', 80, 'Estimate Sent', 'Follow up', 'Fictitious stovetop lead who wants a gate latch estimate.', now()),
  ('Olive Ovenmitt (Test)', 'seed', '2105550103', 'olive.ovenmitt@example.com', '103 Test St, Testville TX',
   'active_project', 100, 'Estimate Approved', 'Active project in oven', 'Fictitious active customer with two jobs.', now());

insert into public.chisme_active
  (contact_id, burner_position, active_reason, active_owner, next_followup_date, updated_at)
values
  ((select id from public.chisme_contacts where name = 'Olive Ovenmitt (Test)'),
   1, 'Kitchen faucet job in progress', 'Test Owner',
   (now() at time zone 'America/Chicago')::date + 3, now());

insert into public.chisme_journals
  (contact_id, journal_type, title, entry, created_by)
values
  ((select id from public.chisme_contacts where name = 'Stella Stovetop (Test)'),
   'memory', 'Stella Stovetop (Test)', 'Seed journal for Stella.', 'seed'),
  ((select id from public.chisme_contacts where name = 'Olive Ovenmitt (Test)'),
   'memory', 'Olive Ovenmitt (Test)', 'Seed journal for Olive.', 'seed');

insert into public.chisme_notes
  (journal_id, contact_id, note_date, note_type, note_text, created_by)
values
  ((select id from public.chisme_journals where contact_id = (select id from public.chisme_contacts where name = 'Stella Stovetop (Test)')),
   (select id from public.chisme_contacts where name = 'Stella Stovetop (Test)'),
   (now() at time zone 'America/Chicago')::date - 2, 'chisme',
   'Asked about a quote for a new gate latch.', 'seed'),
  ((select id from public.chisme_journals where contact_id = (select id from public.chisme_contacts where name = 'Olive Ovenmitt (Test)')),
   (select id from public.chisme_contacts where name = 'Olive Ovenmitt (Test)'),
   (now() at time zone 'America/Chicago')::date - 1, 'chisme',
   'Happy with the faucet work so far; asked about a deck rail.', 'seed');

-- Crudobot ----------------------------------------------------------------------------
insert into public.crudo_projects
  (contact_id, project_name, service_address, status, closeout_status, started_at, updated_at)
values
  ((select id from public.chisme_contacts where name = 'Olive Ovenmitt (Test)'),
   'Olive kitchen faucet (Test)', '103 Test St, Testville TX', 'active', 'open', now() - interval '7 days', now()),
  ((select id from public.chisme_contacts where name = 'Olive Ovenmitt (Test)'),
   'Olive deck rail (Test)', '103 Test St, Testville TX', 'closeout', 'ready', now() - interval '14 days', now());

insert into public.crudo_project_tasks
  (project_id, task_name, task_order, status, completed_at, updated_at)
values
  ((select id from public.crudo_projects where project_name = 'Olive kitchen faucet (Test)'),
   'Shut off water and remove old faucet', 1, 'completed', now() - interval '6 days', now()),
  ((select id from public.crudo_projects where project_name = 'Olive kitchen faucet (Test)'),
   'Install new faucet', 2, 'completed', now() - interval '5 days', now()),
  ((select id from public.crudo_projects where project_name = 'Olive kitchen faucet (Test)'),
   'Test for leaks and clean up', 3, 'pending', null, now()),
  ((select id from public.crudo_projects where project_name = 'Olive deck rail (Test)'),
   'Set rail posts', 1, 'completed', now() - interval '12 days', now()),
  ((select id from public.crudo_projects where project_name = 'Olive deck rail (Test)'),
   'Attach rails and stain', 2, 'completed', now() - interval '10 days', now());

insert into public.crudo_labor_entries
  (project_id, task_id, worker, started_at, ended_at, actual_minutes, source, notes)
values
  ((select id from public.crudo_projects where project_name = 'Olive kitchen faucet (Test)'),
   (select id from public.crudo_project_tasks where task_name = 'Shut off water and remove old faucet'
      and project_id = (select id from public.crudo_projects where project_name = 'Olive kitchen faucet (Test)')),
   'Test Worker', now() - interval '6 days 1 hour', now() - interval '6 days', 60, 'seed', 'Seed labor entry.'),
  ((select id from public.crudo_projects where project_name = 'Olive kitchen faucet (Test)'),
   (select id from public.crudo_project_tasks where task_name = 'Install new faucet'
      and project_id = (select id from public.crudo_projects where project_name = 'Olive kitchen faucet (Test)')),
   'Test Worker', now() - interval '5 days 90 minutes', now() - interval '5 days', 90, 'seed', 'Seed labor entry.'),
  ((select id from public.crudo_projects where project_name = 'Olive deck rail (Test)'),
   (select id from public.crudo_project_tasks where task_name = 'Set rail posts'
      and project_id = (select id from public.crudo_projects where project_name = 'Olive deck rail (Test)')),
   'Test Worker', now() - interval '12 days 2 hours', now() - interval '12 days', 120, 'seed', 'Seed labor entry.'),
  ((select id from public.crudo_projects where project_name = 'Olive deck rail (Test)'),
   (select id from public.crudo_project_tasks where task_name = 'Attach rails and stain'
      and project_id = (select id from public.crudo_projects where project_name = 'Olive deck rail (Test)')),
   'Test Worker', now() - interval '10 days 3 hours', now() - interval '10 days', 180, 'seed', 'Seed labor entry.');

insert into public.crudo_material_transactions
  (project_id, transaction_type, vendor, transaction_date, receipt_number, subtotal, tax, total, source, notes, updated_at)
values
  ((select id from public.crudo_projects where project_name = 'Olive kitchen faucet (Test)'),
   'purchase', 'Fake Hardware Supply', (now() at time zone 'America/Chicago')::date - 6,
   'SEED-0001', 54.00, 4.46, 58.46, 'seed', 'Seed receipt.', now()),
  ((select id from public.crudo_projects where project_name = 'Olive deck rail (Test)'),
   'purchase', 'Fake Hardware Supply', (now() at time zone 'America/Chicago')::date - 13,
   'SEED-0002', 140.00, 11.55, 151.55, 'seed', 'Seed receipt.', now());

insert into public.crudo_material_transaction_items
  (transaction_id, project_id, project_material_id, sku, item_name, quantity, unit, unit_price, line_total, category)
values
  ((select id from public.crudo_material_transactions where receipt_number = 'SEED-0001'),
   (select id from public.crudo_projects where project_name = 'Olive kitchen faucet (Test)'),
   null, 'FAKE-1001', 'Kitchen faucet, brushed nickel', 1, 'each', 54.00, 54.00, 'plumbing'),
  ((select id from public.crudo_material_transactions where receipt_number = 'SEED-0002'),
   (select id from public.crudo_projects where project_name = 'Olive deck rail (Test)'),
   null, 'FAKE-2001', 'Cedar rail 8ft', 10, 'each', 9.00, 90.00, 'lumber'),
  ((select id from public.crudo_material_transactions where receipt_number = 'SEED-0002'),
   (select id from public.crudo_projects where project_name = 'Olive deck rail (Test)'),
   null, 'FAKE-2002', 'Exterior stain, gallon', 1, 'each', 50.00, 50.00, 'finish');

-- Guardabot ---------------------------------------------------------------------------
insert into public.garage_zones (cell, zone_name, updated_at)
values
  ('A1', 'Electrical Shelf (Test)', now()),
  ('A2', 'Electrical Shelf (Test)', now()),
  ('B1', 'Plumbing Bin (Test)', now()),
  ('B2', 'Plumbing Bin (Test)', now());

insert into public.inventory_items (item_name, quantity, unit, location, category, notes, updated_at)
values
  ('wire nuts', 50, 'each', 'A1', 'electrical', 'Seed item.', now()),
  ('1/2in PEX elbow', 12, 'each', 'B1', 'plumbing', 'Seed item.', now());

-- Metichebot --------------------------------------------------------------------------
insert into public.metiche_weekly
  (ts, discord_user, channel_id, week_of, weekly_goal, jobs_json, pending_estimates_json, invoices_to_send_json,
   calendar_json, task_summary_json, quarterly_goals_json, yearly_goals_json, wants_accountant)
values
  (to_char(now() at time zone 'America/Chicago', 'YYYY-MM-DD"T"HH24:MI:SS'),
   'Test Owner', '100000000000000003',
   date_trunc('week', now() at time zone 'America/Chicago')::date, 2000,
   '["Olive kitchen faucet (Test)"]'::jsonb, '["Stella Stovetop (Test) gate latch"]'::jsonb, '[]'::jsonb,
   '{}'::jsonb, '{}'::jsonb, '[]'::jsonb, '[]'::jsonb, false);

insert into public.daily_tasks (user_id, calendar_key, date, task, completed)
values
  ('seed', 'heaven', (now() at time zone 'America/Chicago')::date, 'Olive kitchen faucet (Test)', false),
  ('seed', 'heaven', (now() at time zone 'America/Chicago')::date, 'Order wire nuts', true),
  ('seed', 'heaven', (now() at time zone 'America/Chicago')::date, 'Call Stella Stovetop (Test)', false);

-- Obi Juan ----------------------------------------------------------------------------
insert into public.obijuan_quests
  (quest_id, customer_name, title, status, location, job_summary, accepted_by_user_id, accepted_by_name)
values
  ('test-gate-latch', 'Olive Ovenmitt (Test)', 'Replace gate latch', 'accepted', '103 Test St, Testville TX',
   'Swap a broken gate latch for a new one.', 100000000000000002, 'Test Sub');

insert into public.obijuan_quest_assignments
  (quest_id, worker_id, worker_name, accepted_price, status, notes, accepted_at, updated_at)
values
  ('test-gate-latch', '100000000000000002', 'Test Sub', 150, 'accepted', 'Seed assignment.', now(), now());

-- Vueltabot ---------------------------------------------------------------------------
insert into public.vuelta_items
  (seller_discord_id, seller_discord_name, owner_name, item_name, description, category, condition, quantity,
   asking_price, pickup_area, status)
values
  ('100000000000000004', 'Test Seller', 'Test Seller', 'Used Ladder (Test)',
   'Six foot fiberglass ladder, fictitious listing for local testing.', 'tools', 'used', '1',
   40, 'Testville', 'available');

insert into public.vuelta_routes
  (item_id, route_type, status, recommended_price, recommendation_reason)
values
  ((select id from public.vuelta_items where item_name = 'Used Ladder (Test)'),
   'local_marketplace', 'recommended', 40, 'Seed route: local pickup first.');

insert into public.vuelta_interests
  (discord_user_id, discord_user_name, description, category, match_keywords, status)
values
  ('100000000000000001', 'Test Buyer', 'Looking for a ladder', 'tools', 'ladder', 'active');
