"""Tests for the parser gateway (api/parsers.py).

The parser subsystem has no Redis dependency, so we mount only its router on a
bare FastAPI app rather than importing the full crane_manager.main.app.
"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from crane_manager.api.parsers import build_parsers_router
from crane_manager.parsers import registry
from crane_manager.parsers.config import parser_env


def client() -> TestClient:
    app = FastAPI()
    app.include_router(build_parsers_router(), prefix="/api/parsers")
    return TestClient(app)


def test_catalog_lists_the_two_parsers():
    resp = client().get("/api/parsers/")
    assert resp.status_code == 200
    names = {p["name"] for p in resp.json()}
    assert names == {"email", "regex"}


def test_parser_info_and_unknown_404():
    c = client()
    assert c.get("/api/parsers/email/info").status_code == 200
    # Unknown parser has no mount, so any URL under it is unrouted.
    assert c.post("/api/parsers/does-not-exist/parse", json={}).status_code == 404


def test_email_parse():
    raw = (
        "From: Alice <alice@example.com>\r\n"
        "To: bob@example.com\r\n"
        "Subject: Hello\r\n"
        "Message-ID: <abc@x>\r\n"
        "Date: Mon, 02 Jun 2026 10:00:00 +0000\r\n"
        "Content-Type: text/plain\r\n\r\n"
        "This is the body.\r\n"
    )
    resp = client().post("/api/parsers/email/parse", json={"raw": raw})
    assert resp.status_code == 200
    result = resp.json()["result"]
    assert result["subject"] == "Hello"
    assert result["from_addr"] == "Alice <alice@example.com>"
    assert "This is the body." in result["body_text"]


def test_email_parse_missing_input_returns_422():
    resp = client().post("/api/parsers/email/parse", json={})
    assert resp.status_code == 422


def test_regex_parse_named_groups():
    resp = client().post(
        "/api/parsers/regex/parse",
        json={"pattern": r"(?P<word>\w*event_api\w*)", "text": "typecheck_event_api and event_api_v2"},
    )
    assert resp.status_code == 200
    result = resp.json()["result"]
    assert result["count"] == 2
    assert result["matches"][0]["word"] == "typecheck_event_api"


def test_regex_invalid_pattern_returns_422():
    resp = client().post("/api/parsers/regex/parse", json={"pattern": "(", "text": "x"})
    assert resp.status_code == 422


# --- wire-crossing isolation guarantees ---

def test_config_is_namespaced_per_parser(monkeypatch):
    monkeypatch.setenv("PARSER_EMAIL_SECRET", "e")
    monkeypatch.setenv("PARSER_REGEX_SECRET", "r")
    assert parser_env("email") == {"secret": "e"}
    assert parser_env("regex") == {"secret": "r"}  # email's value never leaks in


def test_parser_crash_is_contained_as_scoped_500(monkeypatch):
    def boom(_payload):
        raise ValueError("kaboom")

    monkeypatch.setattr(registry.get("email"), "parse", boom)
    resp = client().post("/api/parsers/email/parse", json={"raw": "x"})
    assert resp.status_code == 500
    detail = resp.json()["detail"]
    assert detail["parser"] == "email"
    assert "kaboom" not in str(detail)  # internal error is not leaked to caller
    # other parsers still work on the same host
    ok = client().post("/api/parsers/regex/parse", json={"pattern": "a", "text": "aaa"})
    assert ok.status_code == 200
