from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from skating_api import repository
from skating_api.db import connection_dependency


STATIC_DIR = Path(__file__).resolve().parent / "static"

app = FastAPI(title="Skating Data API", version="0.1.0")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/legend")
def legend(conn: Any = Depends(connection_dependency)) -> dict[str, Any]:
    return {"markers": repository.list_marker_legend(conn)}


@app.get("/api/dashboard")
def dashboard(conn: Any = Depends(connection_dependency)) -> dict[str, Any]:
    return {
        "overview": repository.dashboard_overview(conn),
        "competition_mix": repository.competition_mix(conn),
        "clubs": repository.list_clubs(conn, limit=100),
        "nationalities": repository.list_nationalities(conn, limit=100),
    }


@app.get("/api/dashboard/competition-mix")
def competition_mix(conn: Any = Depends(connection_dependency)) -> dict[str, Any]:
    return {"items": repository.competition_mix(conn)}


@app.get("/api/nationalities")
def list_nationalities(
    q: str | None = None,
    limit: int | None = None,
    conn: Any = Depends(connection_dependency),
) -> dict[str, Any]:
    return {"items": repository.list_nationalities(conn, q, limit)}


@app.get("/api/nationalities/{nation_code}")
def nationality_detail(nation_code: str, conn: Any = Depends(connection_dependency)) -> dict[str, Any]:
    nationality = repository.nationality_detail(conn, nation_code)
    if nationality is None:
        raise HTTPException(status_code=404, detail="Nationality not found")
    nationality["skaters_list"] = repository.nationality_skaters(conn, nation_code, 200)
    return nationality


@app.get("/api/clubs")
def list_clubs(
    q: str | None = None,
    scope: str | None = None,
    limit: int | None = None,
    conn: Any = Depends(connection_dependency),
) -> dict[str, Any]:
    return {"items": repository.list_clubs(conn, q, scope, limit)}


@app.get("/api/clubs/{club_name}")
def club_detail(club_name: str, conn: Any = Depends(connection_dependency)) -> dict[str, Any]:
    club = repository.club_detail(conn, club_name)
    if club is None:
        raise HTTPException(status_code=404, detail="Club not found")
    club["skater_rows"] = repository.club_skaters(conn, club_name, 500)
    return club


@app.get("/api/skaters")
def search_skaters(
    q: str | None = None,
    limit: int | None = None,
    conn: Any = Depends(connection_dependency),
) -> dict[str, Any]:
    return {"items": repository.search_skaters(conn, q, limit)}


@app.get("/api/skaters/{skater_key}")
def get_skater(skater_key: int, conn: Any = Depends(connection_dependency)) -> dict[str, Any]:
    skater = repository.get_skater(conn, skater_key)
    if skater is None:
        raise HTTPException(status_code=404, detail="Skater not found")
    skater["scores"] = repository.skater_scores(conn, skater_key, 50)
    skater["elements"] = repository.skater_elements(conn, skater_key, 50)
    return skater


@app.get("/api/skaters/{skater_key}/scores")
def skater_scores(
    skater_key: int,
    limit: int | None = None,
    conn: Any = Depends(connection_dependency),
) -> dict[str, Any]:
    return {"items": repository.skater_scores(conn, skater_key, limit)}


@app.get("/api/skaters/{skater_key}/elements")
def skater_elements(
    skater_key: int,
    limit: int | None = None,
    conn: Any = Depends(connection_dependency),
) -> dict[str, Any]:
    return {"items": repository.skater_elements(conn, skater_key, limit)}


@app.get("/api/events")
def list_events(
    q: str | None = None,
    limit: int | None = None,
    conn: Any = Depends(connection_dependency),
) -> dict[str, Any]:
    return {"items": repository.list_events(conn, q, limit)}


@app.get("/api/events/{event_key}")
def get_event(event_key: int, conn: Any = Depends(connection_dependency)) -> dict[str, Any]:
    event = repository.get_event(conn, event_key)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    event["results"] = repository.event_results(conn, event_key, 500)
    return event


@app.get("/api/events/{event_key}/results")
def event_results(
    event_key: int,
    limit: int | None = None,
    conn: Any = Depends(connection_dependency),
) -> dict[str, Any]:
    return {"items": repository.event_results(conn, event_key, limit)}


@app.get("/api/scores/{segment_score_key}")
def score_detail(segment_score_key: int, conn: Any = Depends(connection_dependency)) -> dict[str, Any]:
    score = repository.score_detail(conn, segment_score_key)
    if score is None:
        raise HTTPException(status_code=404, detail="Score not found")
    return score
