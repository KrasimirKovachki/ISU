#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from isu_parser.local_config import database_dsn


DEFAULT_MANIFEST = Path("data/source_archive/manifest.csv")


def blank_to_none(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    return value or None


def load_manifest(path: Path) -> dict[str, Any]:
    rows_read = 0
    rows_loaded = 0
    with path.open(newline="", encoding="utf-8") as manifest_file, psycopg.connect(database_dsn()) as conn:
        with conn.cursor() as cur:
            for row in csv.DictReader(manifest_file):
                rows_read += 1
                source_document_id = blank_to_none(row.get("source_document_id"))
                if not source_document_id:
                    continue
                cur.execute(
                    """
                    INSERT INTO ingest.source_archive_files (
                      source_document_id,
                      import_run_id,
                      root_url,
                      url,
                      document_type,
                      parser_profile,
                      content_hash,
                      archive_path,
                      archive_sha256,
                      archive_status,
                      archive_reason,
                      parse_status,
                      fetched_at,
                      loaded_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                    ON CONFLICT (source_document_id) DO UPDATE SET
                      import_run_id = EXCLUDED.import_run_id,
                      root_url = EXCLUDED.root_url,
                      url = EXCLUDED.url,
                      document_type = EXCLUDED.document_type,
                      parser_profile = EXCLUDED.parser_profile,
                      content_hash = EXCLUDED.content_hash,
                      archive_path = EXCLUDED.archive_path,
                      archive_sha256 = EXCLUDED.archive_sha256,
                      archive_status = EXCLUDED.archive_status,
                      archive_reason = EXCLUDED.archive_reason,
                      parse_status = EXCLUDED.parse_status,
                      fetched_at = EXCLUDED.fetched_at,
                      loaded_at = now()
                    """,
                    (
                        int(source_document_id),
                        int(row["import_run_id"]),
                        row["root_url"],
                        row["url"],
                        blank_to_none(row.get("document_type")),
                        blank_to_none(row.get("parser_profile")),
                        blank_to_none(row.get("content_hash")),
                        blank_to_none(row.get("archive_path")),
                        blank_to_none(row.get("archive_sha256")),
                        blank_to_none(row.get("archive_status")) or "unknown",
                        blank_to_none(row.get("archive_reason")),
                        blank_to_none(row.get("parse_status")),
                        blank_to_none(row.get("fetched_at")),
                    ),
                )
                rows_loaded += 1
        conn.commit()
    return {"manifest": str(path), "rows_read": rows_read, "rows_loaded": rows_loaded}


def main() -> int:
    parser = argparse.ArgumentParser(description="Load data/source_archive manifest into PostgreSQL.")
    parser.add_argument("manifest", nargs="?", default=str(DEFAULT_MANIFEST))
    args = parser.parse_args()
    print(json.dumps(load_manifest(Path(args.manifest)), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
