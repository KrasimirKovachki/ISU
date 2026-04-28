#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from discover_isu_events import EVENTS_URL, discover_from_html, fetch_text
from isu_parser.local_config import database_dsn


def load_catalog(url: str) -> dict[str, int | str]:
    events = discover_from_html(fetch_text(url), source_page_url=url)
    with psycopg.connect(database_dsn()) as conn:
        with conn.cursor() as cur:
            for event in events:
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
                      metadata,
                      last_seen_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
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
                      discovery_status = CASE
                        WHEN ingest.event_discovery_catalog.discovery_status IN ('registered', 'skipped')
                          THEN ingest.event_discovery_catalog.discovery_status
                        ELSE EXCLUDED.discovery_status
                      END,
                      metadata = ingest.event_discovery_catalog.metadata || EXCLUDED.metadata,
                      last_seen_at = now()
                    """,
                    (
                        "isu_official_events",
                        event.source_page_url,
                        event.source_kind,
                        event.event_name,
                        event.date_range,
                        event.city,
                        event.country_code,
                        event.discipline,
                        event.discovery_status,
                        Jsonb({"discovered_by": "scripts/load_isu_events_catalog.py"}),
                    ),
                )
        conn.commit()
    return {"source_page_url": url, "events_loaded": len(events)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Load official ISU event catalog rows into PostgreSQL.")
    parser.add_argument("--url", default=EVENTS_URL)
    args = parser.parse_args()
    print(json.dumps(load_catalog(args.url), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
