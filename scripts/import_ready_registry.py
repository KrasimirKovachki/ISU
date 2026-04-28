#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from isu_parser.local_config import database_dsn
from scripts.import_event import import_event


def ready_urls(conn: psycopg.Connection, family: str | None, limit: int | None) -> list[str]:
    params: list[object] = []
    family_filter = ""
    if family:
        family_filter = "AND COALESCE(r.summary->>'source', '') LIKE %s"
        params.append(f"%{family}%")
    limit_clause = ""
    if limit:
        limit_clause = "LIMIT %s"
        params.append(limit)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT r.url
            FROM ingest.source_url_registry r
            WHERE r.status = 'ready_for_import'
              AND r.validation_status = 'passed'
              AND NOT EXISTS (
                SELECT 1
                FROM ingest.import_runs ir
                WHERE ir.root_url = r.url
                  AND ir.status = 'completed'
              )
              {family_filter}
            ORDER BY
              CASE r.parser_profile WHEN 'fs_manager' THEN 0 ELSE 1 END,
              r.url
            {limit_clause}
            """,
            params,
        )
        return [row[0] for row in cur.fetchall()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Import URLs currently marked ready_for_import in the registry.")
    parser.add_argument("--family", default="bsf.bg/figure-skating/national-championships", help="Substring filter against registry summary source.")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    results = []
    with psycopg.connect(database_dsn()) as conn:
        urls = ready_urls(conn, args.family, args.limit)
        for url in urls:
            try:
                import_run_id = import_event(conn, url)
            except Exception as exc:  # noqa: BLE001 - keep importing independent registry rows.
                conn.rollback()
                results.append({"url": url, "status": "failed", "error": str(exc)})
            else:
                conn.commit()
                results.append({"url": url, "status": "imported", "import_run_id": import_run_id})
    print(json.dumps({"count": len(results), "results": results}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
