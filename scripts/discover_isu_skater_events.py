#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from isu_parser.isu_api import extract_page, post_form
from isu_parser.local_config import database_dsn


def parse_date(value: Any) -> date | None:
    if not value:
        return None
    text = str(value).strip()
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        pass
    for fmt in ("%d %b %Y", "%d %B %Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def parse_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def none_if_blank(value: Any) -> Any:
    return None if value == "" else value


def fetch_history_page(skaters_id: int, page: int) -> tuple[list[dict[str, Any]], int | None, int | None]:
    api_page = extract_page(
        post_form(
            "skater/get-competition-results",
            {
                "skaters_id": skaters_id,
                "page": page,
            },
        ),
        page=page,
    )
    return api_page.items, api_page.total_pages, api_page.total_items


def upsert_history_rows(cur: psycopg.Cursor, rows: list[dict[str, Any]], skaters_id: int) -> int:
    loaded = 0
    for row in rows:
        result_id = row.get("skaters_competition_results_id")
        event_name = row.get("event_name")
        if not result_id or not event_name:
            continue
        cur.execute(
            """
            INSERT INTO ingest.isu_skater_event_history (
              skaters_competition_results_id, skaters_id, api_id, event_api_id,
              event_name, event_date, event_to_date, year, season, points, rank,
              category, country, city, event_type_name, sport_code, sport_name,
              raw, last_seen_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (skaters_competition_results_id) DO UPDATE SET
              skaters_id = EXCLUDED.skaters_id,
              api_id = EXCLUDED.api_id,
              event_api_id = EXCLUDED.event_api_id,
              event_name = EXCLUDED.event_name,
              event_date = EXCLUDED.event_date,
              event_to_date = EXCLUDED.event_to_date,
              year = EXCLUDED.year,
              season = EXCLUDED.season,
              points = EXCLUDED.points,
              rank = EXCLUDED.rank,
              category = EXCLUDED.category,
              country = EXCLUDED.country,
              city = EXCLUDED.city,
              event_type_name = EXCLUDED.event_type_name,
              sport_code = EXCLUDED.sport_code,
              sport_name = EXCLUDED.sport_name,
              raw = EXCLUDED.raw,
              last_seen_at = now()
            """,
            (
                result_id,
                row.get("skaters_id") or skaters_id,
                none_if_blank(row.get("api_id")),
                none_if_blank(row.get("event_id")),
                event_name,
                parse_date(row.get("date")),
                parse_date(row.get("to_date")),
                parse_int(row.get("year")),
                row.get("season"),
                parse_decimal(row.get("points")),
                str(row.get("rank")) if row.get("rank") is not None else None,
                row.get("category"),
                row.get("country"),
                row.get("city"),
                row.get("event_type_name"),
                row.get("sport_code"),
                row.get("sport_name"),
                Jsonb(row),
            ),
        )
        loaded += 1
    return loaded


def skaters_from_directory(limit: int | None) -> list[int]:
    query = """
      SELECT DISTINCT skaters_id
      FROM ingest.isu_skater_person
      WHERE skaters_id IS NOT NULL
      ORDER BY skaters_id
    """
    if limit:
        query += " LIMIT %s"
    with psycopg.connect(database_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (limit,) if limit else None)
            return [int(row[0]) for row in cur.fetchall()]


def load_histories(skaters_ids: list[int], page_limit: int | None, dry_run: bool) -> dict[str, Any]:
    result: dict[str, Any] = {"skaters": []}
    with psycopg.connect(database_dsn()) if not dry_run else nullcontext() as conn:
        cur = conn.cursor() if conn else None
        for skaters_id in skaters_ids:
            page_number = 1
            loaded = 0
            seen = 0
            samples = []
            total_pages = None
            total_items = None
            while True:
                rows, total_pages, total_items = fetch_history_page(skaters_id, page_number)
                seen += len(rows)
                if dry_run:
                    samples.extend(rows[: max(0, 3 - len(samples))])
                else:
                    assert cur is not None
                    loaded += upsert_history_rows(cur, rows, skaters_id)
                if page_limit and page_number >= page_limit:
                    break
                if total_pages is None or page_number >= total_pages:
                    break
                page_number += 1
            result["skaters"].append(
                {
                    "skaters_id": skaters_id,
                    "events_seen": seen,
                    "events_loaded": loaded,
                    "api_total_pages": total_pages,
                    "api_total_items": total_items,
                    **({"sample": samples} if dry_run else {}),
                }
            )
        if conn:
            conn.commit()
    result["count"] = len(result["skaters"])
    return result


class nullcontext:
    def __enter__(self) -> None:
        return None

    def __exit__(self, *exc_info: object) -> bool:
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Load ISU API per-skater competition histories.")
    parser.add_argument("--skaters-id", type=int, action="append", help="ISU numeric skaters_id. May be repeated.")
    parser.add_argument("--from-directory", action="store_true", help="Walk skaters_id values from ingest.isu_skater_person.")
    parser.add_argument("--limit-skaters", type=int)
    parser.add_argument("--page-limit", type=int)
    parser.add_argument("--dry-run", action="store_true", help="Fetch and print without touching PostgreSQL.")
    args = parser.parse_args()

    skaters_ids = args.skaters_id or []
    if args.from_directory:
        skaters_ids.extend(skaters_from_directory(args.limit_skaters))
    if not skaters_ids:
        parser.error("provide --skaters-id or --from-directory")

    print(json.dumps(load_histories(list(dict.fromkeys(skaters_ids)), args.page_limit, args.dry_run), indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
