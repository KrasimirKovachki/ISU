#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from discover_isu_events import discover_detail_from_html, fetch_text
from isu_parser.local_config import database_dsn
from isu_parser.source_check import preflight_result_url


def choose_parser_profile(summary: dict[str, Any]) -> str | None:
    content_kind = summary.get("content_kind")
    if content_kind == "pdf":
        return "pdf_result"
    if content_kind == "html":
        return "fs_manager"
    return None


def upsert_registry_url(
    cur: psycopg.Cursor,
    *,
    result_url: str,
    catalog_id: int,
    event_name: str,
    detail_url: str,
) -> tuple[str, str, str, dict[str, Any]]:
    source_check = preflight_result_url(result_url)
    validation_summary = source_check.summary()
    parser_profile = choose_parser_profile(validation_summary)
    validation_status = source_check.status if source_check.status in {"passed", "failed"} else "warning"
    registry_status = "ready_for_import" if source_check.ok and parser_profile else "analyzed"
    cur.execute(
        """
        INSERT INTO ingest.source_url_registry (
          url,
          resolved_url,
          parser_profile,
          competition_stream,
          status,
          validation_status,
          last_validated_at,
          summary,
          notes
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
            result_url,
            validation_summary.get("resolved_url"),
            parser_profile,
            "ISU",
            registry_status,
            validation_status,
            Jsonb(
                {
                    **validation_summary,
                    "event_discovery_catalog_id": catalog_id,
                    "event_name": event_name,
                    "detail_url": detail_url,
                }
            ),
            "Discovered from ISU event detail Entries & Results link.",
        ),
    )
    return registry_status, validation_status, parser_profile or "", validation_summary


def register_detail(detail_url: str, register: bool = False) -> dict[str, Any]:
    detail = discover_detail_from_html(fetch_text(detail_url), detail_url)
    with psycopg.connect(database_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingest.event_discovery_catalog (
                  source_name,
                  source_page_url,
                  source_kind,
                  event_name,
                  date_range,
                  city,
                  country_code,
                  discipline,
                  discovery_status,
                  result_url,
                  metadata,
                  last_seen_at
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
                  discovery_status = EXCLUDED.discovery_status,
                  metadata = ingest.event_discovery_catalog.metadata || EXCLUDED.metadata,
                  last_seen_at = now()
                RETURNING id
                """,
                (
                    "isu_official_events",
                    detail_url,
                    detail.source_kind,
                    detail.event_name,
                    detail.date_range,
                    detail.city,
                    detail.country_code,
                    detail.discipline,
                    detail.discovery_status,
                    detail.result_url,
                    Jsonb(
                        {
                            "discovered_by": "scripts/resolve_isu_event_results.py",
                            "detail_url": detail_url,
                            "direct_detail_resolution": True,
                        }
                    ),
                ),
            )
            catalog_id = int(cur.fetchone()[0])
            registry_status = None
            validation_status = None
            validation_summary = None
            if register and detail.result_url:
                registry_status, validation_status, _, validation_summary = upsert_registry_url(
                    cur,
                    result_url=detail.result_url,
                    catalog_id=catalog_id,
                    event_name=detail.event_name,
                    detail_url=detail_url,
                )
                cur.execute(
                    """
                    UPDATE ingest.event_discovery_catalog
                    SET metadata = metadata || %s,
                        last_seen_at = now()
                    WHERE id = %s
                    """,
                    (Jsonb({"result_validation": validation_summary}), catalog_id),
                )
        conn.commit()
    return {
        "id": catalog_id,
        "event_name": detail.event_name,
        "detail_url": detail_url,
        "result_url": detail.result_url,
        "status": detail.discovery_status,
        "registered": bool(register and detail.result_url),
        "registry_status": registry_status,
        "validation_status": validation_status,
    }


def resolve_catalog(limit: int | None = None, register: bool = False, refresh: bool = False) -> dict[str, Any]:
    query = """
      SELECT id, event_name, metadata, result_url
      FROM ingest.event_discovery_catalog
      WHERE source_name = 'isu_official_events'
        AND (metadata ? 'detail_url')
        AND (%s OR result_url IS NULL)
      ORDER BY id
    """
    if limit:
        query += " LIMIT %s"

    resolved: list[dict[str, Any]] = []
    with psycopg.connect(database_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (refresh, limit) if limit else (refresh,))
            rows = cur.fetchall()
            for catalog_id, event_name, metadata, current_result_url in rows:
                detail_url = (metadata or {}).get("detail_url")
                if not detail_url:
                    continue
                try:
                    detail = discover_detail_from_html(fetch_text(detail_url), detail_url)
                except Exception as exc:  # noqa: BLE001 - resolver should preserve failures and continue.
                    cur.execute(
                        """
                        UPDATE ingest.event_discovery_catalog
                        SET discovery_status = 'manual_review',
                            metadata = metadata || %s,
                            last_seen_at = now()
                        WHERE id = %s
                        """,
                        (Jsonb({"resolve_error": str(exc)}), catalog_id),
                    )
                    resolved.append({"id": catalog_id, "event_name": event_name, "status": "manual_review", "error": str(exc)})
                    continue

                result_url = detail.result_url or current_result_url
                status = "candidate_result_url" if result_url else "manual_review"
                extra_metadata = {
                    "detail_url": detail_url,
                    "resolved_by": "scripts/resolve_isu_event_results.py",
                    "detail_event_name": detail.event_name,
                }
                validation_status = "not_checked"
                registry_status = "pending"
                if result_url and register:
                    registry_status, validation_status, _, validation_summary = upsert_registry_url(
                        cur,
                        result_url=result_url,
                        catalog_id=catalog_id,
                        event_name=detail.event_name or event_name,
                        detail_url=detail_url,
                    )
                    extra_metadata["result_validation"] = validation_summary

                cur.execute(
                    """
                    UPDATE ingest.event_discovery_catalog
                    SET result_url = %s,
                        discovery_status = %s,
                        metadata = metadata || %s,
                        last_seen_at = now()
                    WHERE id = %s
                    """,
                    (result_url, status, Jsonb(extra_metadata), catalog_id),
                )
                resolved.append(
                    {
                        "id": catalog_id,
                        "event_name": detail.event_name or event_name,
                        "detail_url": detail_url,
                        "result_url": result_url,
                        "status": status,
                        "registered": bool(register and result_url),
                        "registry_status": registry_status if register and result_url else None,
                        "validation_status": validation_status if register and result_url else None,
                    }
                )
        conn.commit()
    return {"resolved": resolved, "count": len(resolved)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Resolve ISU event detail pages to external Entries & Results URLs.")
    parser.add_argument("--detail-url", help="Resolve one ISU event detail page directly.")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--register", action="store_true", help="Validate and upsert discovered result URLs into source_url_registry.")
    parser.add_argument("--refresh", action="store_true", help="Refresh rows that already have result_url.")
    args = parser.parse_args()
    if args.detail_url:
        result = {"resolved": [register_detail(args.detail_url, register=args.register)], "count": 1}
    else:
        result = resolve_catalog(limit=args.limit, register=args.register, refresh=args.refresh)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
