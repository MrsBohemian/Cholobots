# Fixture for test_check_schema_coverage: the legacy module is scanned too.
def save(client):
    client.table("gamma").upsert({}).execute()
