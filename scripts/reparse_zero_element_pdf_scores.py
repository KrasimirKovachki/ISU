#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from isu_parser.local_config import database_dsn
from isu_parser.pdf_scores import parse_judges_scores_pdf


def resolve_archive_path(archive_path: str) -> Path:
    path = Path(archive_path)
    if path.exists():
        return path
    repo_relative = Path("data/source_archive") / path
    if repo_relative.exists():
        return repo_relative
    return path


def fetch_zero_element_targets(cur: psycopg.Cursor) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
          pss.id AS pdf_score_summary_id,
          pss.source_document_id,
          pss.rank,
          pss.starting_number,
          saf.archive_path
        FROM ingest.pdf_score_summaries pss
        JOIN ingest.source_archive_files saf ON saf.source_document_id = pss.source_document_id
        LEFT JOIN ingest.pdf_elements pe ON pe.pdf_score_summary_id = pss.id
        WHERE saf.document_type = 'judges_scores_pdf'
          AND saf.archive_status IN ('written', 'archived')
          AND saf.archive_path IS NOT NULL
        GROUP BY pss.id, pss.source_document_id, pss.rank, pss.starting_number, saf.archive_path
        HAVING count(pe.id) = 0
        ORDER BY pss.source_document_id, pss.rank, pss.starting_number
        """
    )
    return list(cur.fetchall())


def delete_existing_children(cur: psycopg.Cursor, summary_id: int) -> None:
    cur.execute(
        """
        DELETE FROM ingest.pdf_component_judge_scores
        WHERE pdf_program_component_id IN (
          SELECT id FROM ingest.pdf_program_components WHERE pdf_score_summary_id = %s
        )
        """,
        (summary_id,),
    )
    cur.execute("DELETE FROM ingest.pdf_program_components WHERE pdf_score_summary_id = %s", (summary_id,))
    cur.execute(
        """
        DELETE FROM ingest.pdf_element_judge_scores
        WHERE pdf_element_id IN (
          SELECT id FROM ingest.pdf_elements WHERE pdf_score_summary_id = %s
        )
        """,
        (summary_id,),
    )
    cur.execute("DELETE FROM ingest.pdf_elements WHERE pdf_score_summary_id = %s", (summary_id,))


def update_summary(cur: psycopg.Cursor, summary_id: int, skater: dict[str, Any]) -> None:
    cur.execute(
        """
        UPDATE ingest.pdf_score_summaries
        SET
          rank = %s,
          starting_number = %s,
          total_segment_score = %s,
          total_element_score = %s,
          total_program_component_score = %s,
          total_deductions = %s,
          printed_at = %s,
          judge_count = %s,
          base_value_total = %s,
          element_score_total = %s,
          deductions_detail = %s,
          raw = %s
        WHERE id = %s
        """,
        (
            skater.get("rank"),
            skater.get("starting_number"),
            skater.get("total_segment_score"),
            skater.get("total_element_score"),
            skater.get("total_program_component_score"),
            skater.get("total_deductions"),
            skater.get("printed_at"),
            skater.get("judge_count"),
            skater.get("base_value_total"),
            skater.get("element_score_total"),
            Jsonb(skater.get("deductions_detail", {})),
            Jsonb(skater),
            summary_id,
        ),
    )


def insert_pdf_details(cur: psycopg.Cursor, summary_id: int, skater: dict[str, Any]) -> None:
    for element in skater.get("elements", []):
        cur.execute(
            """
            INSERT INTO ingest.pdf_elements (
              pdf_score_summary_id, element_no, element_code, base_element_code,
              raw_element, info, markers, base_value, goe, bonus, panel_score, raw
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                summary_id,
                element.get("element_no"),
                element.get("element_code"),
                element.get("base_element_code"),
                element.get("raw_element"),
                element.get("info"),
                element.get("markers", []),
                element.get("base_value"),
                element.get("goe"),
                element.get("bonus"),
                element.get("panel_score"),
                Jsonb(element),
            ),
        )
        element_row = cur.fetchone()
        element_id = int(element_row["id"] if isinstance(element_row, dict) else element_row[0])
        for judge_no, score in enumerate(element.get("judge_scores", []), start=1):
            cur.execute(
                "INSERT INTO ingest.pdf_element_judge_scores (pdf_element_id, judge_number, score) VALUES (%s, %s, %s)",
                (element_id, judge_no, score),
            )

    for component in skater.get("program_components", []):
        cur.execute(
            """
            INSERT INTO ingest.pdf_program_components (
              pdf_score_summary_id, component_name, factor, score, raw
            )
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                summary_id,
                component.get("component"),
                component.get("factor"),
                component.get("score"),
                Jsonb(component),
            ),
        )
        component_row = cur.fetchone()
        component_id = int(component_row["id"] if isinstance(component_row, dict) else component_row[0])
        for judge_no, score in enumerate(component.get("judge_scores", []), start=1):
            cur.execute(
                """
                INSERT INTO ingest.pdf_component_judge_scores (
                  pdf_program_component_id, judge_number, score
                )
                VALUES (%s, %s, %s)
                """,
                (component_id, judge_no, score),
            )


def repair_zero_element_summaries() -> dict[str, Any]:
    repaired = 0
    skipped_missing = 0
    skipped_unmatched = 0
    processed_documents = 0
    with psycopg.connect(database_dsn()) as conn:
        conn.execute("SET statement_timeout = '0'")
        with conn.cursor(row_factory=dict_row) as cur:
            targets = fetch_zero_element_targets(cur)
            grouped: dict[int, list[dict[str, Any]]] = {}
            for row in targets:
                grouped.setdefault(int(row["source_document_id"]), []).append(row)

            for source_document_id, rows in grouped.items():
                archive_path = resolve_archive_path(rows[0]["archive_path"])
                if not archive_path.exists():
                    skipped_missing += len(rows)
                    continue
                parsed = parse_judges_scores_pdf(archive_path)
                skaters_by_key = {
                    (skater.get("rank"), skater.get("starting_number")): skater for skater in parsed.get("skaters", [])
                }

                cur.execute(
                    "UPDATE ingest.source_documents SET metadata = coalesce(metadata, '{}'::jsonb) || %s WHERE id = %s",
                    (Jsonb({"parsed": parsed}), source_document_id),
                )

                for row in rows:
                    summary_id = int(row["pdf_score_summary_id"])
                    summary_key = (row["rank"], row["starting_number"])
                    skater = skaters_by_key.get(summary_key)
                    if not skater:
                        skipped_unmatched += 1
                        continue
                    delete_existing_children(cur, summary_id)
                    update_summary(cur, summary_id, skater)
                    insert_pdf_details(cur, summary_id, skater)
                    repaired += 1
                processed_documents += 1
        conn.commit()

    return {
        "processed_documents": processed_documents,
        "repaired_summaries": repaired,
        "skipped_missing_archives": skipped_missing,
        "skipped_unmatched_summaries": skipped_unmatched,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Reparse archived PDF score summaries that currently have zero parsed elements.")
    args = parser.parse_args()
    print(json.dumps(repair_zero_element_summaries(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
