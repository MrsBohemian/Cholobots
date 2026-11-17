# Fixture for test_check_schema_coverage: Supabase calls the checker must find.
def load(supabase):
    supabase.table("alpha").select("*").execute()
    supabase.table( 'beta' ).insert({}).execute()
