#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from isu_parser.local_config import DEFAULT_LOCAL_CONFIG, database_dsn


VALIDATION_SQL = {
    "import_run": """
        SELECT id, status, root_url, stats, error_message
        FROM ingest.import_runs
        WHERE id = %s
    """,
    "counts": """
        SELECT 'events' AS name, count(*) FROM ingest.events WHERE import_run_id = %s
        UNION ALL SELECT 'source_documents', count(*) FROM ingest.source_documents WHERE import_run_id = %s
        UNION ALL SELECT 'parse_issues', count(*) FROM ingest.parse_issues WHERE import_run_id = %s
        UNION ALL SELECT 'categories', count(*)
          FROM ingest.categories c JOIN ingest.events e ON e.id = c.event_id WHERE e.import_run_id = %s
        UNION ALL SELECT 'segments', count(*)
          FROM ingest.segments s JOIN ingest.categories c ON c.id = s.category_id JOIN ingest.events e ON e.id = c.event_id WHERE e.import_run_id = %s
        UNION ALL SELECT 'source_skater_appearances', count(*) FROM ingest.source_skater_appearances WHERE import_run_id = %s
        UNION ALL SELECT 'category_results', count(*)
          FROM ingest.category_results cr JOIN ingest.categories c ON c.id = cr.category_id JOIN ingest.events e ON e.id = c.event_id WHERE e.import_run_id = %s
        UNION ALL SELECT 'segment_results', count(*)
          FROM ingest.segment_results sr JOIN ingest.segments s ON s.id = sr.segment_id JOIN ingest.categories c ON c.id = s.category_id JOIN ingest.events e ON e.id = c.event_id WHERE e.import_run_id = %s
        UNION ALL SELECT 'official_assignments', count(*)
          FROM ingest.official_assignments oa JOIN ingest.segments s ON s.id = oa.segment_id JOIN ingest.categories c ON c.id = s.category_id JOIN ingest.events e ON e.id = c.event_id WHERE e.import_run_id = %s
        UNION ALL SELECT 'pdf_score_summaries', count(*)
          FROM ingest.pdf_score_summaries ps JOIN ingest.segments s ON s.id = ps.segment_id JOIN ingest.categories c ON c.id = s.category_id JOIN ingest.events e ON e.id = c.event_id WHERE e.import_run_id = %s
        ORDER BY name
    """,
    "issues": """
        SELECT level, code, count(*) AS count
        FROM ingest.parse_issues
        WHERE import_run_id = %s
        GROUP BY level, code
        ORDER BY level, code
    """,
    "missing_segment_officials": """
        SELECT c.name AS category, s.name AS segment
        FROM ingest.segments s
        JOIN ingest.categories c ON c.id = s.category_id
        JOIN ingest.events e ON e.id = c.event_id
        LEFT JOIN ingest.official_assignments oa ON oa.segment_id = s.id
        WHERE e.import_run_id = %s
        GROUP BY c.name, s.name
        HAVING count(oa.id) = 0
        ORDER BY c.name, s.name
    """,
    "pdf_without_elements": """
        SELECT c.name AS category, s.name AS segment, count(ps.id) AS summaries
        FROM ingest.pdf_score_summaries ps
        JOIN ingest.segments s ON s.id = ps.segment_id
        JOIN ingest.categories c ON c.id = s.category_id
        JOIN ingest.events e ON e.id = c.event_id
        LEFT JOIN ingest.pdf_elements pe ON pe.pdf_score_summary_id = ps.id
        WHERE e.import_run_id = %s
        GROUP BY c.name, s.name
        HAVING count(pe.id) = 0
        ORDER BY c.name, s.name
    """,
}


def rows_as_dicts(cur: psycopg.Cursor, columns: list[str]) -> list[dict[str, Any]]:
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def validate(conn: psycopg.Connection, import_run_id: int) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(VALIDATION_SQL["import_run"], (import_run_id,))
        run = cur.fetchone()
        if not run:
            raise SystemExit(f"Import run not found: {import_run_id}")

        cur.execute(VALIDATION_SQL["counts"], (import_run_id,) * 10)
        counts = {name: count for name, count in cur.fetchall()}

        cur.execute(VALIDATION_SQL["issues"], (import_run_id,))
        issues = rows_as_dicts(cur, ["level", "code", "count"])

        cur.execute(VALIDATION_SQL["missing_segment_officials"], (import_run_id,))
        missing_officials = rows_as_dicts(cur, ["category", "segment"])

        cur.execute(VALIDATION_SQL["pdf_without_elements"], (import_run_id,))
        pdf_without_elements = rows_as_dicts(cur, ["category", "segment", "summaries"])

    return {
        "import_run": {
            "id": run[0],
            "status": run[1],
            "root_url": run[2],
            "stats": run[3],
            "error_message": run[4],
        },
        "counts": counts,
        "parse_issues": issues,
        "missing_segment_officials": missing_officials,
        "pdf_without_elements": pdf_without_elements,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate an import run.")
    parser.add_argument("import_run_id", type=int)
    parser.add_argument("--config", default=str(DEFAULT_LOCAL_CONFIG), help="Local env file with database settings.")
    parser.add_argument("--dsn", help="Database DSN. Overrides --config and SKATING_* environment variables.")
    args = parser.parse_args()
    dsn = args.dsn or database_dsn(args.config)
    with psycopg.connect(dsn) as conn:
        print(json.dumps(validate(conn, args.import_run_id), indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
