"""v1 integration-contract tests (see docs/integration-contract.md).

Protects the stable boundary the academy app depends on. Field/data checks skip
when no database is reachable, so the suite stays green without a DB; they run
fully against the local Docker Postgres.
"""
from __future__ import annotations

import pytest

pytest.importorskip("httpx")  # starlette TestClient needs httpx
from fastapi.testclient import TestClient  # noqa: E402

from skating_api.app import app  # noqa: E402

client = TestClient(app)


def _db_ok() -> bool:
    try:
        from skating_api.db import connect

        with connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return True
    except Exception:
        return False


DB = _db_ok()
needs_db = pytest.mark.skipif(not DB, reason="no database reachable")


def test_health_on_v1_and_legacy():
    for path in ("/api/v1/health", "/api/health"):
        r = client.get(path)
        assert r.status_code == 200
        assert r.json().get("status") == "ok"


def test_v1_prefix_is_mounted():
    # The search route must exist under /api/v1 (200 even if empty), not 404.
    r = client.get("/api/v1/skaters", params={"limit": 1})
    assert r.status_code == 200
    assert "items" in r.json()


def test_match_route_is_before_id_route():
    # "/skaters/match" must resolve to the match endpoint, not be parsed as an int id.
    r = client.get("/api/v1/skaters/match", params={"name": "x"})
    assert r.status_code == 200
    body = r.json()
    assert set(["query", "match", "confidence", "candidates"]).issubset(body)


@needs_db
def test_meta_fields():
    r = client.get("/api/v1/meta")
    assert r.status_code == 200
    body = r.json()
    for f in ("events", "skaters", "clubs", "last_import_at", "latest_event_date", "api_version"):
        assert f in body, f"missing /meta field: {f}"
    assert body["api_version"] == "v1"


@needs_db
def test_skater_search_fields():
    r = client.get("/api/v1/skaters", params={"q": "a", "limit": 1})
    assert r.status_code == 200
    items = r.json()["items"]
    if items:
        it = items[0]
        for f in ("skater_key", "skater_name", "nation_code", "club_name", "identity_key"):
            assert f in it, f"missing skater field: {f}"


@needs_db
def test_event_schedule_shape():
    # Event 21 (Ice Peak Trophy 2026) has the full entry schedule: 32 categories, 3 segments.
    r = client.get("/api/v1/events/21/schedule")
    assert r.status_code == 200
    body = r.json()
    assert set(["event_key", "categories", "segments"]).issubset(body)
    assert len(body["categories"]) >= 10
    for c in body["categories"][:3]:
        assert "name" in c and "segments" in c
    for s in body["segments"]:
        assert "name" in s and "abbr" in s


@needs_db
def test_match_contract_shape():
    r = client.get("/api/v1/skaters/match", params={"name": "lora", "nation": "BUL", "limit": 3})
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body["candidates"], list)
    assert 0.0 <= body["confidence"] <= 1.0
    for c in body["candidates"]:
        assert "skater_key" in c and "skater_name" in c
