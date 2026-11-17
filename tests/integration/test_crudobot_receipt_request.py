# ABOUTME: Pins the OpenAI request parse_receipt_with_openai sends, so typing refactors cannot change it.
# ABOUTME: Fakes crudobot.client; no network, the local stack is only needed by the shared fixtures.
# Integration placement: tests/unit has no harness for importing bot modules (crudobot calls
# create_client at import), so this sits next to the other receipt tests.
import json
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.integration

PARSED = {"vendor": "Fake Hardware Supply 207", "transaction_type": "purchase", "items": []}


class FakeOpenAI:
    def __init__(self, output_text):
        self.calls = []
        self.files = SimpleNamespace(create=self._upload)
        self.responses = SimpleNamespace(create=self._respond)
        self._output_text = output_text

    def _upload(self, **kwargs):
        self.calls.append(("files.create", kwargs))
        return SimpleNamespace(id="file-test-1")

    def _respond(self, **kwargs):
        self.calls.append(("responses.create", kwargs))
        return SimpleNamespace(output_text=self._output_text)


def test_image_receipt_request_matches_the_contract(bot_modules, monkeypatch):
    crudobot = bot_modules["crudobot"]
    fake = FakeOpenAI(json.dumps(PARSED))
    monkeypatch.setattr(crudobot, "client", fake)

    result = crudobot.parse_receipt_with_openai("r.png", "image/png", b"\x89PNG fake")

    assert [name for name, _ in fake.calls] == ["responses.create"]
    kwargs = fake.calls[0][1]
    assert set(kwargs) == {"model", "input", "text"}
    assert kwargs["model"] == crudobot.RECEIPT_MODEL
    assert len(kwargs["input"]) == 1
    assert kwargs["input"][0]["role"] == "user"
    content = kwargs["input"][0]["content"]
    assert content[0] == {
        "type": "input_image",
        "image_url": "data:image/png;base64,iVBORyBmYWtl",
        "detail": "high",
    }
    assert content[1]["type"] == "input_text"
    assert content[1]["text"].startswith("Extract this contractor purchase or return receipt")
    assert kwargs["text"] == {"format": {
        "type": "json_schema",
        "name": "crudobot_contractor_receipt",
        "strict": True,
        "schema": crudobot.RECEIPT_JSON_SCHEMA,
    }}
    assert result["vendor"] == "Fake Hardware Supply 207"


def test_pdf_receipt_uploads_then_references_the_file(bot_modules, monkeypatch):
    crudobot = bot_modules["crudobot"]
    fake = FakeOpenAI(json.dumps(PARSED))
    monkeypatch.setattr(crudobot, "client", fake)

    crudobot.parse_receipt_with_openai("r.pdf", "application/pdf", b"%PDF-1.4 test")

    assert [name for name, _ in fake.calls] == ["files.create", "responses.create"]
    assert fake.calls[0][1] == {
        "file": ("r.pdf", b"%PDF-1.4 test", "application/pdf"),
        "purpose": "user_data",
    }
    content = fake.calls[1][1]["input"][0]["content"]
    assert content[0] == {"type": "input_file", "file_id": "file-test-1"}


def test_empty_parser_output_raises(bot_modules, monkeypatch):
    crudobot = bot_modules["crudobot"]
    monkeypatch.setattr(crudobot, "client", FakeOpenAI(""))

    with pytest.raises(ValueError, match="returned no data"):
        crudobot.parse_receipt_with_openai("r.png", "image/png", b"\x89PNG fake")
