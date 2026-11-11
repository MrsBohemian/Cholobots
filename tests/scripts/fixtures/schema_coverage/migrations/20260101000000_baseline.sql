-- Fixture for test_check_schema_coverage: a dump-shaped baseline.
CREATE TABLE IF NOT EXISTS "public"."alpha" (
    "id" bigint NOT NULL
);
CREATE TABLE public.beta (id bigint);
CREATE TABLE gamma (id bigint);
CREATE TABLE IF NOT EXISTS "auth"."users" (id uuid);
CREATE OR REPLACE VIEW "public"."dash_view" AS SELECT * FROM "public"."alpha";
CREATE MATERIALIZED VIEW public.stats AS SELECT 1;
CREATE TABLE IF NOT EXISTS "public"."orphan" (id bigint);
ALTER TABLE "public"."alpha" ENABLE ROW LEVEL SECURITY;
ALTER TABLE ONLY "public"."beta" ENABLE ROW LEVEL SECURITY;
CREATE POLICY "read alpha" ON "public"."alpha" FOR SELECT TO "anon" USING (true);
CREATE POLICY "write alpha" ON "public"."alpha" FOR UPDATE TO "anon" USING (true);
