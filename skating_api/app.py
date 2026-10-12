from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from skating_api import repository
from skating_api.db import connection_dependency


STATIC_DIR = Path(__file__).resolve().parent / "static"

app = FastAPI(title="Skating Data API", version="1.0.0")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


# ---------------------------------------------------------------------------
# All data endpoints live on one router, mounted at BOTH /api (back-compat)
# and /api/v1 (the stable contract consumers like the academy pin to). v1 only
# ever gets ADDITIVE changes; breaking changes go to a future /api/v2.
# See docs/integration-contract.md.
# ---------------------------------------------------------------------------
api = APIRouter()


@api.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@api.get("/meta")
def meta(conn: Any = Depends(connection_dependency)) -> dict[str, Any]:
    """Data freshness + size, so consumers can show 'results as of <date>'."""
    m = repository.api_meta(conn) or {}
    m["api_version"] = "v1"
    return m


@api.get("/legend")
def legend(conn: Any = Depends(connection_dependency)) -> dict[str, Any]:
    return {"markers": repository.list_marker_legend(conn)}


@api.get("/dashboard")
def dashboard(
    from_year: int | None = None,
    to_year: int | None = None,
    conn: Any = Depends(connection_dependency),
) -> dict[str, Any]:
    if from_year is None and to_year is None:
        return {
            "period": None,
            "overview": repository.dashboard_overview(conn),
            "competition_mix": repository.competition_mix(conn),
            "clubs": repository.list_clubs(conn, limit=100),
            "nationalities": repository.list_nationalities(conn, limit=100),
        }
    fy = from_year if from_year is not None else 1900
    ty = to_year if to_year is not None else 2100
    if fy > ty:
        fy, ty = ty, fy
    return {
        "period": {"from_year": fy, "to_year": ty},
        "overview": repository.dashboard_overview_period(conn, fy, ty),
        "competition_mix": repository.competition_mix_period(conn, fy, ty),
        "clubs": repository.list_clubs_period(conn, fy, ty, limit=100),
        "nationalities": repository.list_nationalities_period(conn, fy, ty, limit=100),
    }


@api.get("/dashboard/competition-mix")
def competition_mix(conn: Any = Depends(connection_dependency)) -> dict[str, Any]:
    return {"items": repository.competition_mix(conn)}


@api.get("/nationalities")
def list_nationalities(
    q: str | None = None,
    limit: int | None = None,
    conn: Any = Depends(connection_dependency),
) -> dict[str, Any]:
    return {"items": repository.list_nationalities(conn, q, limit)}


@api.get("/nationalities/{nation_code}")
def nationality_detail(nation_code: str, conn: Any = Depends(connection_dependency)) -> dict[str, Any]:
    nationality = repository.nationality_detail(conn, nation_code)
    if nationality is None:
        raise HTTPException(status_code=404, detail="Nationality not found")
    nationality["skaters_list"] = repository.nationality_skaters(conn, nation_code, 2000)
    return nationality


@api.get("/clubs")
def list_clubs(
    q: str | None = None,
    scope: str | None = None,
    limit: int | None = None,
    conn: Any = Depends(connection_dependency),
) -> dict[str, Any]:
    return {"items": repository.list_clubs(conn, q, scope, limit)}


@api.get("/clubs/{club_name}")
def club_detail(club_name: str, conn: Any = Depends(connection_dependency)) -> dict[str, Any]:
    club = repository.club_detail(conn, club_name)
    if club is None:
        raise HTTPException(status_code=404, detail="Club not found")
    club["skater_rows"] = repository.club_skaters(conn, club_name, 500)
    return club


@api.get("/skaters")
def search_skaters(
    q: str | None = None,
    limit: int | None = None,
    conn: Any = Depends(connection_dependency),
) -> dict[str, Any]:
    return {"items": repository.search_skaters(conn, q, limit)}


def _norm_tokens(name: str) -> set[str]:
    return {t for t in (name or "").lower().replace(".", " ").replace(",", " ").split() if t}


def _name_score(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)  # Jaccard overlap of name tokens


# NOTE: must be declared BEFORE /skaters/{skater_key} so "match" is not parsed as an id.
@api.get("/skaters/match")
def match_skater(
    name: str,
    nation: str | None = None,
    limit: int = 5,
    conn: Any = Depends(connection_dependency),
) -> dict[str, Any]:
    """Best-effort link of an external (academy) skater to a platform skater.

    Returns a ranked candidate list plus the single best match and a 0-1
    confidence (name-token overlap, boosted when nationality matches). The
    caller decides whether to accept `match` (we only fill it at >= 0.6).
    """
    candidates = repository.search_skaters(conn, name, 25)
    qn = _norm_tokens(name)
    scored: list[tuple[float, dict[str, Any]]] = []
    for c in candidates:
        s = _name_score(qn, _norm_tokens(c.get("skater_name", "")))
        if nation and (c.get("nation_code") or "").upper() == nation.upper():
            s = min(1.0, s + 0.15)
        scored.append((s, c))
    scored.sort(key=lambda x: x[0], reverse=True)
    best = scored[0] if scored else None
    return {
        "query": {"name": name, "nation": nation},
        "match": best[1] if best and best[0] >= 0.6 else None,
        "confidence": round(best[0], 3) if best else 0.0,
        "candidates": [c for _, c in scored[:limit]],
    }


@api.get("/skaters/{skater_key}")
def get_skater(skater_key: int, conn: Any = Depends(connection_dependency)) -> dict[str, Any]:
    skater = repository.get_skater(conn, skater_key)
    if skater is None:
        raise HTTPException(status_code=404, detail="Skater not found")
    skater["scores"] = repository.skater_scores(conn, skater_key, 200)
    skater["elements"] = repository.skater_elements(conn, skater_key, 500)
    skater["component_summary"] = repository.skater_component_summary(conn, skater_key)
    return skater


@api.get("/skaters/{skater_key}/scores")
def skater_scores(
    skater_key: int,
    limit: int | None = None,
    conn: Any = Depends(connection_dependency),
) -> dict[str, Any]:
    return {"items": repository.skater_scores(conn, skater_key, limit)}


@api.get("/skaters/{skater_key}/elements")
def skater_elements(
    skater_key: int,
    limit: int | None = None,
    conn: Any = Depends(connection_dependency),
) -> dict[str, Any]:
    return {"items": repository.skater_elements(conn, skater_key, limit)}


@api.get("/events")
def list_events(
    q: str | None = None,
    limit: int | None = None,
    from_year: int | None = None,
    to_year: int | None = None,
    conn: Any = Depends(connection_dependency),
) -> dict[str, Any]:
    return {"items": repository.list_events(conn, q, limit, from_year, to_year)}


@api.get("/events/{event_key}")
def get_event(event_key: int, conn: Any = Depends(connection_dependency)) -> dict[str, Any]:
    event = repository.get_event(conn, event_key)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    event["results"] = repository.event_results(conn, event_key, 500)
    return event


@api.get("/events/{event_key}/results")
def event_results(
    event_key: int,
    limit: int | None = None,
    conn: Any = Depends(connection_dependency),
) -> dict[str, Any]:
    return {"items": repository.event_results(conn, event_key, limit)}


@api.get("/events/{event_key}/schedule")
def event_schedule(event_key: int, conn: Any = Depends(connection_dependency)) -> dict[str, Any]:
    """Categories + segments from the parsed entry schedule (for seeding a competition's config)."""
    return repository.event_schedule(conn, event_key)


@api.get("/scores/{segment_score_key}")
def score_detail(segment_score_key: int, conn: Any = Depends(connection_dependency)) -> dict[str, Any]:
    score = repository.score_detail(conn, segment_score_key)
    if score is None:
        raise HTTPException(status_code=404, detail="Score not found")
    return score


# Back-compat (unversioned) + the stable contract. Keep both; pin consumers to v1.
app.include_router(api, prefix="/api")
app.include_router(api, prefix="/api/v1")
