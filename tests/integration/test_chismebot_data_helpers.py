# ABOUTME: Pins what Chismebot's contact, journal, oven and lead helpers read and write in Supabase.
# ABOUTME: Runs against the local stack; the OpenAI client is faked, so no request leaves the machine.
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.integration

NO_ID = "00000000-0000-0000-0000-000000000000"


class FakeOpenAI:
    def __init__(self, output_text):
        self.calls = []
        self.responses = SimpleNamespace(create=self._respond)
        self._output_text = output_text

    def _respond(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(output_text=self._output_text)


@pytest.fixture
def chismebot(bot_modules):
    return bot_modules["chismebot"]


def _contact(db, name, **fields):
    return db.table("chisme_contacts").insert({"name": name, **fields}).execute().data[0]


def _lead(db, **fields):
    return db.table("chisme_leads").insert(fields).execute().data[0]


def _rows(db, table, **eq):
    query = db.table(table).select("*")
    for column, value in eq.items():
        query = query.eq(column, value)
    return query.execute().data


def test_find_contacts_matches_phone_digits_then_name(chismebot, db):
    alpha = _contact(db, "Chisme Probe Alpha", phone="2105550191")
    beta = _contact(db, "Chisme Probe Beta")

    assert [c["id"] for c in chismebot.find_contacts("(210) 555-0191")] == [alpha["id"]]
    assert [c["id"] for c in chismebot.find_contacts("probe beta")] == [beta["id"]]
    assert chismebot.find_contacts("  ") == []


def test_create_contact_stub_inserts_placeholder_and_journal(chismebot, db):
    contact = chismebot.create_contact_stub("Chisme Probe Gamma", "call 210-555-0199")

    assert contact["name"] == "Chisme Probe Gamma"
    assert contact["phone"] == "2105550199"
    assert contact["status"] == "lead"
    assert contact["hotlist_temperature"] == 50
    assert contact["pipeline_stage"] == "New Lead"
    assert contact["source"] == "chismebot"
    stored = _rows(db, "chisme_contacts", id=contact["id"])
    assert stored == [contact]
    assert len(_rows(db, "chisme_journals", contact_id=contact["id"])) == 1


def test_ensure_journal_is_idempotent_and_get_journal_returns_newest_first(chismebot, db):
    contact = _contact(db, "Chisme Probe Journal")

    first = chismebot.ensure_journal(contact["id"])
    second = chismebot.ensure_journal(contact["id"])

    assert first["id"] == second["id"]
    assert len(_rows(db, "chisme_journals", contact_id=contact["id"])) == 1
    for day in ("01", "02"):
        db.table("chisme_notes").insert({
            "journal_id": first["id"],
            "contact_id": contact["id"],
            "note_text": f"note {day}",
            "created_at": f"2031-01-{day}T10:00:00+00:00",
        }).execute()

    journal, notes = chismebot.get_journal(contact["id"])

    assert journal["id"] == first["id"]
    assert [n["note_text"] for n in notes] == ["note 02", "note 01"]


def test_add_note_writes_note_and_updates_contact(chismebot, db, monkeypatch):
    fake = FakeOpenAI("Summary v2")
    monkeypatch.setattr(chismebot, "client", fake)
    contact = _contact(db, "Chisme Probe Notes")
    text = "Follow up 2031-02-01, cell 210-555-0193"

    note = chismebot.add_note(contact, text, created_by="tester")

    assert note["note_type"] == "chisme"
    assert note["journal_id"]
    assert note["created_by"] == "tester"
    row = _rows(db, "chisme_contacts", id=contact["id"])[0]
    assert row["chisme_summary"] == "Summary v2"
    assert row["last_outcome"] == text
    assert row["next_followup_date"] == "2031-02-01"
    assert row["next_contact_date"] == "2031-02-01"
    assert row["phone"] == "2105550193"
    assert len(fake.calls) == 1
    assert fake.calls[0]["model"] == "gpt-5-mini"


def test_set_active_upserts_and_next_burner_skips_used(chismebot, db):
    db.table("chisme_active").delete().neq("id", NO_ID).execute()
    contact = _contact(db, "Chisme Probe Oven")

    chismebot.set_active(contact, "hot", burner_position=1)
    chismebot.set_active(contact, "hot", burner_position=1)

    rows = _rows(db, "chisme_active", contact_id=contact["id"])
    assert len(rows) == 1
    assert rows[0]["burner_position"] == 1
    assert chismebot.next_available_burner() == 2
    assert chismebot.get_active()[0]["chisme_contacts"]["name"] == "Chisme Probe Oven"


def test_unprocessed_leads_and_lookup_by_id(chismebot, db):
    later = _lead(db, raw_notes="later", status="unprocessed", captured_at="2031-01-02T10:00:00+00:00")
    earlier = _lead(db, raw_notes="earlier", status="unprocessed", captured_at="2031-01-01T10:00:00+00:00")
    _lead(db, raw_notes="done", status="processed", captured_at="2031-01-03T10:00:00+00:00")

    leads = chismebot.get_unprocessed_leads()

    assert [lead["id"] for lead in leads] == [earlier["id"], later["id"]]
    assert chismebot.get_lead_by_id(earlier["id"])["raw_notes"] == "earlier"
    assert chismebot.get_lead_by_id(NO_ID) is None


def test_try_link_lead_links_unique_phone_match(chismebot, db):
    delta = _contact(db, "Chisme Probe Delta", phone="2105550177")
    lead = _lead(db, raw_notes="Called from +1 (210) 555-0177", status="unprocessed")

    linked = chismebot.try_link_lead_to_existing_contact(lead)

    assert linked["contact_id"] == delta["id"]
    assert _rows(db, "chisme_leads", id=lead["id"])[0]["contact_id"] == delta["id"]


def test_try_link_lead_leaves_ambiguous_phone_unlinked(chismebot, db):
    _contact(db, "Chisme Probe Delta", phone="2105550177")
    _contact(db, "Chisme Probe Delta Twin", phone="2105550177")
    lead = _lead(db, raw_notes="Called from +1 (210) 555-0177", status="unprocessed")

    result = chismebot.try_link_lead_to_existing_contact(lead)

    assert result == lead
    assert _rows(db, "chisme_leads", id=lead["id"])[0]["contact_id"] is None


def test_promote_lead_to_opportunity_creates_once_and_syncs_callback(chismebot, db):
    echo = _contact(db, "Chisme Probe Echo")
    lead = _lead(db, raw_notes="Kitchen remodel", status="unprocessed",
                 contact_id=echo["id"], callback_date="2031-03-03")

    opp = chismebot.promote_lead_to_opportunity(lead, "Kitchen")

    assert opp["opportunity_name"] == "Chisme Probe Echo — Kitchen"
    assert opp["status"] == "open"
    assert opp["pipeline_stage"] == "New Opportunity"
    assert opp["temperature"] == 50
    assert _rows(db, "chisme_leads", id=lead["id"])[0]["status"] == "processed"
    contact = _rows(db, "chisme_contacts", id=echo["id"])[0]
    assert contact["next_followup_date"] == "2031-03-03"
    assert contact["next_contact_date"] == "2031-03-03"

    again = chismebot.promote_lead_to_opportunity(chismebot.get_lead_by_id(lead["id"]), "Kitchen")

    assert again["id"] == opp["id"]
    assert len(_rows(db, "chisme_opportunities", lead_id=lead["id"])) == 1
