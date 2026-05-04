#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from isu_parser.isu_api import extract_page, first_present, post_form, truthy
from isu_parser.local_config import database_dsn


ENTRY_TYPES = ("single", "team_pair", "team_dance")


def parse_date(value: Any) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def endpoint_for(entry_type: str) -> tuple[str, dict[str, Any]]:
    if entry_type == "single":
        return "skater/list", {}
    if entry_type == "team_pair":
        return "skater/team-list", {"is_couple": 1}
    if entry_type == "team_dance":
        return "skater/team-list", {"is_ice_dance": 1}
    raise ValueError(f"unsupported entry type: {entry_type}")


def fetch_page(
    *,
    entry_type: str,
    page: int,
    discipline_id: int,
    gender: str | None,
    country_name: str | None,
    search: str | None,
) -> tuple[list[dict[str, Any]], int | None, int | None]:
    endpoint, extra = endpoint_for(entry_type)
    fields = {
        "discipline_id": discipline_id,
        "page": page,
        "gender": gender if entry_type == "single" else None,
        "country_name": country_name,
        "search": search,
        **extra,
    }
    api_page = extract_page(post_form(endpoint, fields), page=page)
    return api_page.items, api_page.total_pages, api_page.total_items


def person_values(row: dict[str, Any], discipline_id: int) -> tuple[Any, ...] | None:
    api_id = row.get("api_id")
    if not api_id:
        return None
    return (
        api_id,
        row.get("skaters_id"),
        first_present(row, "isu_id", "isu_number"),
        discipline_id,
        row.get("first_name"),
        row.get("last_name"),
        first_present(row, "full_name", "name"),
        row.get("gender"),
        first_present(row, "nationality_code", "nf_organization_code"),
        first_present(row, "organization_code", "nf_organization_code"),
        parse_date(first_present(row, "date_of_birth", "dob")),
        row.get("image"),
        row.get("thumbnail_image"),
        first_present(row, "status", "career_state"),
        row.get("created_at"),
        row.get("updated_at"),
        Jsonb(row),
    )


def upsert_person(cur: psycopg.Cursor, row: dict[str, Any], discipline_id: int) -> bool:
    values = person_values(row, discipline_id)
    if values is None:
        return False
    cur.execute(
        """
        INSERT INTO ingest.isu_skater_person (
          api_id, skaters_id, isu_id, discipline_id, first_name, last_name,
          full_name, gender, nationality_code, organization_code, date_of_birth,
          image_url, thumbnail_url, source_status, source_created_at,
          source_updated_at, raw, last_seen_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (api_id) DO UPDATE SET
          skaters_id = COALESCE(EXCLUDED.skaters_id, ingest.isu_skater_person.skaters_id),
          isu_id = COALESCE(EXCLUDED.isu_id, ingest.isu_skater_person.isu_id),
          discipline_id = EXCLUDED.discipline_id,
          first_name = EXCLUDED.first_name,
          last_name = EXCLUDED.last_name,
          full_name = EXCLUDED.full_name,
          gender = EXCLUDED.gender,
          nationality_code = EXCLUDED.nationality_code,
          organization_code = EXCLUDED.organization_code,
          date_of_birth = COALESCE(EXCLUDED.date_of_birth, ingest.isu_skater_person.date_of_birth),
          image_url = EXCLUDED.image_url,
          thumbnail_url = EXCLUDED.thumbnail_url,
          source_status = EXCLUDED.source_status,
          source_created_at = EXCLUDED.source_created_at,
          source_updated_at = EXCLUDED.source_updated_at,
          raw = EXCLUDED.raw,
          last_seen_at = now()
        """,
        values,
    )
    return True


def upsert_directory_row(cur: psycopg.Cursor, row: dict[str, Any], entry_type: str, discipline_id: int) -> str | None:
    slug = row.get("slug") or row.get("api_id")
    name = first_present(row, "name", "full_name", "couple_name")
    if not slug or not name:
        return None
    cur.execute(
        """
        INSERT INTO ingest.isu_skater_directory (
          slug, entry_type, discipline_id, discipline_title, skaters_team_id, name,
          couple_name, gender, nationality_code, organization_code, image_url,
          thumbnail_url, is_top_display, source_status, source_created_at,
          source_updated_at, raw, last_seen_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (slug) DO UPDATE SET
          entry_type = EXCLUDED.entry_type,
          discipline_id = EXCLUDED.discipline_id,
          discipline_title = EXCLUDED.discipline_title,
          skaters_team_id = EXCLUDED.skaters_team_id,
          name = EXCLUDED.name,
          couple_name = EXCLUDED.couple_name,
          gender = EXCLUDED.gender,
          nationality_code = EXCLUDED.nationality_code,
          organization_code = EXCLUDED.organization_code,
          image_url = EXCLUDED.image_url,
          thumbnail_url = EXCLUDED.thumbnail_url,
          is_top_display = EXCLUDED.is_top_display,
          source_status = EXCLUDED.source_status,
          source_created_at = EXCLUDED.source_created_at,
          source_updated_at = EXCLUDED.source_updated_at,
          raw = EXCLUDED.raw,
          last_seen_at = now()
        """,
        (
            slug,
            entry_type,
            discipline_id,
            row.get("discipline_title"),
            row.get("skaters_team_id"),
            name,
            row.get("couple_name"),
            row.get("gender"),
            first_present(row, "nationality_code", "nf_organization_code"),
            first_present(row, "organization_code", "nf_organization_code"),
            row.get("image"),
            row.get("thumbnail_image"),
            truthy(row.get("is_top_display")),
            first_present(row, "status", "career_state"),
            row.get("created_at"),
            row.get("updated_at"),
            Jsonb(row),
        ),
    )
    return str(slug)


def upsert_member(cur: psycopg.Cursor, slug: str, api_id: str, position: int) -> None:
    cur.execute(
        """
        INSERT INTO ingest.isu_skater_directory_member (slug, api_id, position, last_seen_at)
        VALUES (%s, %s, %s, now())
        ON CONFLICT (slug, api_id) DO UPDATE SET
          position = EXCLUDED.position,
          last_seen_at = now()
        """,
        (slug, api_id, position),
    )


def load_rows(cur: psycopg.Cursor, rows: list[dict[str, Any]], entry_type: str, discipline_id: int) -> dict[str, int]:
    stats = {"directory_rows": 0, "people": 0, "members": 0}
    for row in rows:
        slug = upsert_directory_row(cur, row, entry_type, discipline_id)
        if not slug:
            continue
        stats["directory_rows"] += 1
        if entry_type == "single":
            if upsert_person(cur, row, discipline_id):
                stats["people"] += 1
                upsert_member(cur, slug, row["api_id"], 1)
                stats["members"] += 1
            continue
        members = row.get("skaters_team_member") or []
        for position, member in enumerate(members, start=1):
            if isinstance(member, dict) and upsert_person(cur, member, discipline_id):
                stats["people"] += 1
                upsert_member(cur, slug, member["api_id"], position)
                stats["members"] += 1
    return stats


def load_directory(
    *,
    entry_types: list[str],
    discipline_id: int,
    gender: str | None,
    country_name: str | None,
    search: str | None,
    page_limit: int | None,
    dry_run: bool,
) -> dict[str, Any]:
    result: dict[str, Any] = {"entry_types": entry_types, "loaded": {}}
    with psycopg.connect(database_dsn()) if not dry_run else nullcontext() as conn:
        cur = conn.cursor() if conn else None
        for entry_type in entry_types:
            page_number = 1
            totals = {"directory_rows": 0, "people": 0, "members": 0, "pages": 0}
            samples = []
            total_pages = None
            total_items = None
            while True:
                rows, total_pages, total_items = fetch_page(
                    entry_type=entry_type,
                    page=page_number,
                    discipline_id=discipline_id,
                    gender=gender,
                    country_name=country_name,
                    search=search,
                )
                if dry_run:
                    samples.extend(rows[: max(0, 3 - len(samples))])
                    totals["directory_rows"] += len(rows)
                else:
                    assert cur is not None
                    stats = load_rows(cur, rows, entry_type, discipline_id)
                    for key, value in stats.items():
                        totals[key] += value
                totals["pages"] += 1
                if page_limit and totals["pages"] >= page_limit:
                    break
                if total_pages is None or page_number >= total_pages:
                    break
                page_number += 1
            result["loaded"][entry_type] = {
                **totals,
                "api_total_pages": total_pages,
                "api_total_items": total_items,
                **({"sample": samples} if dry_run else {}),
            }
        if conn:
            conn.commit()
    return result


class nullcontext:
    def __enter__(self) -> None:
        return None

    def __exit__(self, *exc_info: object) -> bool:
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Load ISU API skater/team directory pages.")
    parser.add_argument("--entry-type", choices=(*ENTRY_TYPES, "all"), default="single")
    parser.add_argument("--discipline-id", type=int, default=1)
    parser.add_argument("--gender", choices=("M", "F"))
    parser.add_argument("--country-name")
    parser.add_argument("--search")
    parser.add_argument("--page-limit", type=int, help="Limit pages per entry type for incremental refreshes.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and print without touching PostgreSQL.")
    args = parser.parse_args()

    entry_types = list(ENTRY_TYPES) if args.entry_type == "all" else [args.entry_type]
    print(
        json.dumps(
            load_directory(
                entry_types=entry_types,
                discipline_id=args.discipline_id,
                gender=args.gender,
                country_name=args.country_name,
                search=args.search,
                page_limit=args.page_limit,
                dry_run=args.dry_run,
            ),
            indent=2,
            sort_keys=True,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
