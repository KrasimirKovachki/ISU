#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from isu_parser.isu_api import direct_result_urls, extract_page, post_json, truthy
from isu_parser.local_config import database_dsn
from isu_parser.source_check import preflight_result_url


def parse_date(value: Any) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def none_if_blank(value: Any) -> Any:
    return None if value == "" else value


def choose_parser_profile(summary: dict[str, Any]) -> str | None:
    if summary.get("content_kind") == "pdf":
        return "pdf_result"
    if summary.get("content_kind") == "html":
        return "fs_manager"
    return None


def api_source_page_url(season: str) -> str:
    return f"https://api.isu-skating.com/api/event/mobile-list?discipline_title=FIGURE%20SKATING&season={season}"


def detail_url_for_row(row: dict[str, Any]) -> str | None:
    slug = row.get("slug")
    if not slug:
        return None
    return f"https://www.isu-skating.com/figure-skating/events/eventdetail/{slug}/"


def source_kind_for_row(row: dict[str, Any]) -> str:
    name = row.get("name") or row.get("event_name") or ""
    event_type = row.get("event_type") or ""
    event_sub_type = row.get("event_sub_type_name") or ""
    if "Adult Competition" in name:
        return "isu_adult_catalog"
    if "Challenger Series" in name or event_sub_type == "Challenger Series":
        return "isu_challenger_catalog"
    if event_type == "International" or "International" in name:
        return "isu_international_catalog"
    return "isu_api_event_catalog"


def fetch_event_page(season: str, page_size: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    raw = post_json(
        "event/mobile-list",
        {
            "pagesize": page_size,
            "discipline_title": "FIGURE SKATING",
            "season": season,
        },
    )
    page = extract_page(raw)
    return page.items, raw


def upsert_events(cur: psycopg.Cursor, rows: list[dict[str, Any]], season: str) -> int:
    loaded = 0
    for row in rows:
        name = row.get("name") or row.get("event_name")
        if not name:
            continue
        cur.execute(
            """
            INSERT INTO ingest.isu_api_event_catalog (
              api_id, event_id, slug, name, from_date, to_date, display_date,
              city, country_code, country_name, season, sport_code, sport_name,
              discipline_title, event_type, event_type_name, event_sub_type_name,
              state, is_event_result, is_entry_show,
              button_1_title, button_1_url, button_2_title, button_2_url,
              button_3_title, button_3_url, raw, last_seen_at
            )
            VALUES (
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
            )
            ON CONFLICT (
              coalesce(api_id::text, ''),
              coalesce(event_id::text, ''),
              coalesce(slug, ''),
              coalesce(season, '')
            )
            DO UPDATE SET
              name = EXCLUDED.name,
              from_date = EXCLUDED.from_date,
              to_date = EXCLUDED.to_date,
              display_date = EXCLUDED.display_date,
              city = EXCLUDED.city,
              country_code = EXCLUDED.country_code,
              country_name = EXCLUDED.country_name,
              sport_code = EXCLUDED.sport_code,
              sport_name = EXCLUDED.sport_name,
              discipline_title = EXCLUDED.discipline_title,
              event_type = EXCLUDED.event_type,
              event_type_name = EXCLUDED.event_type_name,
              event_sub_type_name = EXCLUDED.event_sub_type_name,
              state = EXCLUDED.state,
              is_event_result = EXCLUDED.is_event_result,
              is_entry_show = EXCLUDED.is_entry_show,
              button_1_title = EXCLUDED.button_1_title,
              button_1_url = EXCLUDED.button_1_url,
              button_2_title = EXCLUDED.button_2_title,
              button_2_url = EXCLUDED.button_2_url,
              button_3_title = EXCLUDED.button_3_title,
              button_3_url = EXCLUDED.button_3_url,
              raw = EXCLUDED.raw,
              last_seen_at = now()
            """,
            (
                none_if_blank(row.get("api_id")),
                row.get("event_id"),
                row.get("slug"),
                name,
                parse_date(row.get("from_date")),
                parse_date(row.get("to_date")),
                row.get("display_date"),
                row.get("city"),
                row.get("country_code"),
                row.get("country_name"),
                row.get("season") or season,
                row.get("sport_code"),
                row.get("sport_name"),
                row.get("discipline_title"),
                row.get("event_type"),
                row.get("event_type_name"),
                row.get("event_sub_type_name"),
                row.get("state"),
                truthy(row.get("is_event_result")),
                truthy(row.get("is_entry_show")),
                row.get("button_1_title"),
                row.get("button_1_url"),
                row.get("button_2_title"),
                row.get("button_2_url"),
                row.get("button_3_title"),
                row.get("button_3_url"),
                Jsonb(row),
            ),
        )
        loaded += 1
    return loaded


def upsert_event_discovery_catalog(cur: psycopg.Cursor, rows: list[dict[str, Any]], season: str) -> int:
    loaded = 0
    source_page_url = api_source_page_url(season)
    for row in rows:
        name = row.get("name") or row.get("event_name")
        if not name:
            continue
        direct_urls = direct_result_urls(row)
        result_url = direct_urls[0]["url"] if direct_urls else None
        cur.execute(
            """
            INSERT INTO ingest.event_discovery_catalog (
              source_name, source_page_url, source_kind, event_name, date_range,
              city, country_code, discipline, discovery_status, result_url,
              metadata, last_seen_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (
              source_name,
              source_page_url,
              event_name,
              (coalesce(date_range, '')),
              (coalesce(city, '')),
              (coalesce(country_code, ''))
            )
            DO UPDATE SET
              source_kind = EXCLUDED.source_kind,
              discipline = EXCLUDED.discipline,
              result_url = COALESCE(EXCLUDED.result_url, ingest.event_discovery_catalog.result_url),
              discovery_status = CASE
                WHEN ingest.event_discovery_catalog.discovery_status IN ('registered', 'skipped')
                  THEN ingest.event_discovery_catalog.discovery_status
                ELSE EXCLUDED.discovery_status
              END,
              metadata = ingest.event_discovery_catalog.metadata || EXCLUDED.metadata,
              last_seen_at = now()
            """,
            (
                "isu_api_events",
                source_page_url,
                source_kind_for_row(row),
                name,
                row.get("display_date"),
                row.get("city"),
                row.get("country_code"),
                row.get("discipline_title"),
                "candidate_result_url" if result_url else "catalog_only_needs_result_url",
                result_url,
                Jsonb(
                    {
                        "discovered_by": "scripts/discover_isu_api_events.py",
                        "api_id": none_if_blank(row.get("api_id")),
                        "event_id": row.get("event_id"),
                        "slug": row.get("slug"),
                        "detail_url": detail_url_for_row(row),
                        "season": row.get("season") or season,
                        "direct_result_urls": direct_urls,
                        "api_source": "event/mobile-list",
                    }
                ),
            ),
        )
        loaded += 1
    return loaded


def register_direct_results(cur: psycopg.Cursor, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    registered: list[dict[str, Any]] = []
    for row in rows:
        for candidate in direct_result_urls(row):
            source_check = preflight_result_url(candidate["url"])
            summary = source_check.summary()
            parser_profile = choose_parser_profile(summary)
            validation_status = source_check.status if source_check.status in {"passed", "failed"} else "warning"
            registry_status = "ready_for_import" if source_check.ok and parser_profile else "analyzed"
            cur.execute(
                """
                INSERT INTO ingest.source_url_registry (
                  url, resolved_url, parser_profile, competition_stream, status,
                  validation_status, last_validated_at, summary, notes
                )
                VALUES (%s, %s, %s, %s, %s, %s, now(), %s, %s)
                ON CONFLICT (url) DO UPDATE SET
                  resolved_url = COALESCE(EXCLUDED.resolved_url, ingest.source_url_registry.resolved_url),
                  parser_profile = COALESCE(EXCLUDED.parser_profile, ingest.source_url_registry.parser_profile),
                  status = CASE
                    WHEN ingest.source_url_registry.status = 'imported' THEN ingest.source_url_registry.status
                    ELSE EXCLUDED.status
                  END,
                  validation_status = EXCLUDED.validation_status,
                  last_validated_at = now(),
                  summary = ingest.source_url_registry.summary || EXCLUDED.summary,
                  notes = concat_ws(E'\\n', ingest.source_url_registry.notes, EXCLUDED.notes),
                  updated_at = now()
                """,
                (
                    candidate["url"],
                    summary.get("resolved_url"),
                    parser_profile,
                    "ISU",
                    registry_status,
                    validation_status,
                    Jsonb(
                        {
                            **summary,
                            "discovered_by": "scripts/discover_isu_api_events.py",
                            "event_api_id": row.get("api_id"),
                            "event_id": row.get("event_id"),
                            "event_name": row.get("name") or row.get("event_name"),
                            "season": row.get("season"),
                            "button": candidate["button"],
                            "button_title": candidate["title"],
                        }
                    ),
                    "Discovered from ISU API event/mobile-list button URL.",
                ),
            )
            registered.append(
                {
                    "url": candidate["url"],
                    "event_name": row.get("name") or row.get("event_name"),
                    "status": registry_status,
                    "validation_status": validation_status,
                    "parser_profile": parser_profile,
                }
            )
    return registered


def load_events(season: str, page_size: int, register_results: bool, dry_run: bool) -> dict[str, Any]:
    rows, raw = fetch_event_page(season, page_size)
    result = {
        "season": season,
        "events_seen": len(rows),
        "direct_result_url_candidates": sum(len(direct_result_urls(row)) for row in rows),
        "raw_keys": sorted(raw.keys()),
    }
    if dry_run:
        result["sample"] = rows[:3]
        return result

    with psycopg.connect(database_dsn()) as conn:
        with conn.cursor() as cur:
            result["events_loaded"] = upsert_events(cur, rows, season)
            result["discovery_rows_loaded"] = upsert_event_discovery_catalog(cur, rows, season)
            if register_results:
                result["registered_results"] = register_direct_results(cur, rows)
        conn.commit()
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Load ISU API event/mobile-list rows.")
    parser.add_argument("--season", default="2025/2026")
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--register-results", action="store_true", help="Validate direct button URLs and upsert importable ones.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and print without touching PostgreSQL.")
    args = parser.parse_args()

    print(json.dumps(load_events(args.season, args.page_size, args.register_results, args.dry_run), indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
