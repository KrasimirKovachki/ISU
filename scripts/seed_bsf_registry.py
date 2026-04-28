#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.parse import urljoin

import psycopg
from psycopg.types.json import Jsonb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from isu_parser import fs_manager, old_isucalcfs
from isu_parser.local_config import database_dsn
from isu_parser.source_check import preflight_result_url
from scripts.discover_bsf_national_championships import PAGE_URL, discover, extract_bundle_url, fetch_text


USER_AGENT = "isu-skating-data-parser/0.1"


def event_prefix(url: str) -> str:
    path = urlparse(url).path.strip("/")
    for suffix in ("/index.htm", "/index.html"):
        if path.lower().endswith(suffix):
            return path[: -len(suffix)]
    if path.lower().endswith(".pdf"):
        return path.rsplit("/", 1)[0]
    return path.strip("/")


def profile_key_for(prefix: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", prefix.lower()).strip("_")
    digest = hashlib.sha1(prefix.encode("utf-8")).hexdigest()[:8]
    return f"bsf_discovered_{cleaned[:48]}_{digest}"


def stream_for(source_kind: str, url: str) -> str:
    if source_kind == "result_ISU":
        return "ISU"
    if source_kind == "result_NonISU":
        return "NonISU"
    if source_kind == "result_DP":
        return "DP"
    if "/test-mf/" in url:
        return "club_test"
    return "club"


def representation_for(source_kind: str, url: str) -> str:
    if source_kind in {"result_ISU", "result_NonISU"}:
        return "nation"
    return "club"


def detect_parser(url: str) -> tuple[str, dict[str, Any], str]:
    source_check = preflight_result_url(url)
    if not source_check.ok:
        return "unknown", source_check.summary(), "failed"
    if source_check.content_kind == "pdf":
        return "pdf_result", {**source_check.summary(), "document_type": "pdf_result", "reason": "direct PDF result URL"}, "warning"

    html = source_check.body.decode("windows-1252", errors="replace")
    url = source_check.parse_url
    lower = html.lower()

    candidates: list[tuple[str, int, dict[str, Any]]] = []
    if "isucalcfs" in lower:
        parsed = old_isucalcfs.parse_index(html, url)
        candidates.append(
            (
                "old_isucalcfs",
                len(parsed["categories"]),
                {
                    "event": parsed["event"],
                    "categories": len(parsed["categories"]),
                    "segments": sum(len(category["segments"]) for category in parsed["categories"]),
                    "platform": "ISUCalcFS",
                    **source_check.summary(),
                },
            )
        )
    if "fs manager" in lower or "judgesdetailsperskater" in lower:
        parsed = fs_manager.parse_index(html, url)
        candidates.append(
            (
                "fs_manager",
                len(parsed["categories"]),
                {
                    "event": parsed["event"],
                    "categories": len(parsed["categories"]),
                    "segments": sum(len(category["segments"]) for category in parsed["categories"]),
                    "platform": "FS Manager",
                    **source_check.summary(),
                },
            )
        )

    if not candidates:
        for parser_name, parser_mod in (("fs_manager", fs_manager), ("old_isucalcfs", old_isucalcfs)):
            try:
                parsed = parser_mod.parse_index(html, url)
            except Exception as exc:  # noqa: BLE001 - diagnostic for source triage.
                candidates.append((parser_name, 0, {"parse_error": str(exc)}))
            else:
                candidates.append(
                    (
                        parser_name,
                        len(parsed["categories"]),
                        {
                            "event": parsed["event"],
                            "categories": len(parsed["categories"]),
                            "segments": sum(len(category["segments"]) for category in parsed["categories"]),
                            **source_check.summary(),
                        },
                    )
                )

    parser_name, category_count, summary = max(candidates, key=lambda item: item[1])
    if category_count <= 0:
        summary.setdefault("reason", "no categories parsed from result index")
        return parser_name, summary, "failed"
    return parser_name, summary, "passed"


def meta_refresh_url(html: str, base_url: str) -> str | None:
    match = re.search(
        r'<meta[^>]+http-equiv=["\']?refresh["\']?[^>]+content=["\'][^"\']*url=([^"\']+)["\']',
        html,
        flags=re.IGNORECASE,
    )
    if not match:
        match = re.search(
            r'<meta[^>]+content=["\'][^"\']*url=([^"\']+)["\'][^>]+http-equiv=["\']?refresh["\']?',
            html,
            flags=re.IGNORECASE,
        )
    return urljoin(base_url, match.group(1).strip()) if match else None


def old_isucalcfs_main_candidates(url: str) -> list[str]:
    candidates = [urljoin(url, "pages/main.html"), urljoin(url, "pages/main.htm")]
    unique: list[str] = []
    for candidate in candidates:
        if candidate not in unique:
            unique.append(candidate)
    return unique


def completed_import_urls(conn: psycopg.Connection) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT root_url
            FROM ingest.import_runs
            WHERE status = 'completed'
            """
        )
        return {row[0] for row in cur.fetchall()}


def upsert_profile(
    cur: psycopg.Cursor,
    profile_key: str,
    parser_profile: str,
    prefix: str,
    competition_stream: str,
    representation_primary: str,
) -> int:
    settings = {
        "representation": {
            "primary": representation_primary,
            "store_nation": True,
            "store_club": True,
        },
        "source_family": "bsf_discovered",
    }
    cur.execute(
        """
        INSERT INTO ingest.source_profiles (
          profile_key, parser_profile, host, event_path_prefix,
          competition_stream, representation_primary, settings
        )
        VALUES (%s, %s, 'www.bsf.bg', %s, %s, %s, %s)
        ON CONFLICT (profile_key) DO UPDATE SET
          parser_profile = EXCLUDED.parser_profile,
          host = EXCLUDED.host,
          event_path_prefix = EXCLUDED.event_path_prefix,
          competition_stream = EXCLUDED.competition_stream,
          representation_primary = EXCLUDED.representation_primary,
          settings = EXCLUDED.settings,
          updated_at = now()
        RETURNING id
        """,
        (profile_key, parser_profile, prefix, competition_stream, representation_primary, Jsonb(settings)),
    )
    return int(cur.fetchone()[0])


def upsert_registry(
    cur: psycopg.Cursor,
    url: str,
    resolved_url: str | None,
    profile_id: int | None,
    parser_profile: str,
    competition_stream: str,
    status: str,
    validation_status: str,
    summary: dict[str, Any],
    notes: str,
) -> None:
    cur.execute(
        """
        INSERT INTO ingest.source_url_registry (
          url, resolved_url, source_profile_id, parser_profile, competition_stream,
          status, validation_status, last_validated_at, summary, notes
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, now(), %s, %s)
        ON CONFLICT (url) DO UPDATE SET
          resolved_url = EXCLUDED.resolved_url,
          source_profile_id = COALESCE(ingest.source_url_registry.source_profile_id, EXCLUDED.source_profile_id),
          parser_profile = EXCLUDED.parser_profile,
          competition_stream = EXCLUDED.competition_stream,
          status = CASE
            WHEN ingest.source_url_registry.status = 'imported' THEN ingest.source_url_registry.status
            ELSE EXCLUDED.status
          END,
          validation_status = CASE
            WHEN ingest.source_url_registry.status = 'imported' THEN ingest.source_url_registry.validation_status
            ELSE EXCLUDED.validation_status
          END,
          last_validated_at = now(),
          summary = ingest.source_url_registry.summary || EXCLUDED.summary,
          notes = concat_ws(E'\n', ingest.source_url_registry.notes, EXCLUDED.notes),
          updated_at = now()
        """
        ,
        (url, resolved_url, profile_id, parser_profile, competition_stream, status, validation_status, Jsonb(summary), notes),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed DB registry from BSF figure-skating national championships discovery.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    rows = [row for row in discover(fetch_text(extract_bundle_url(fetch_text(PAGE_URL)))) if row.source_kind != "competition"]
    if args.limit:
        rows = rows[: args.limit]

    report: list[dict[str, Any]] = []
    with psycopg.connect(database_dsn()) as conn:
        imported_urls = completed_import_urls(conn)
        with conn.transaction(), conn.cursor() as cur:
            for row in rows:
                duplicate_exact = row.url in imported_urls
                prefix = event_prefix(row.url)
                parser_profile, validation_summary, validation_status = detect_parser(row.url)
                competition_stream = stream_for(row.source_kind, row.url)
                representation_primary = representation_for(row.source_kind, row.url)
                status = "ready_for_import" if validation_status == "passed" and not duplicate_exact else "analyzed"
                if validation_status == "failed":
                    status = "skipped"
                if parser_profile == "pdf_result":
                    status = "analyzed"

                profile_id = None
                profile_key = profile_key_for(prefix)
                if not args.dry_run:
                    if parser_profile != "unknown":
                        profile_id = upsert_profile(
                            cur,
                            profile_key,
                            parser_profile,
                            prefix,
                            competition_stream,
                            representation_primary,
                        )
                    upsert_registry(
                        cur,
                        row.url,
                        validation_summary.get("resolved_url"),
                        profile_id,
                        parser_profile,
                        competition_stream,
                        status,
                        validation_status,
                        {
                            **asdict(row),
                            **validation_summary,
                            "duplicate_exact": duplicate_exact,
                            "source": PAGE_URL,
                        },
                        "Discovered from BSF national championships page.",
                    )

                report.append(
                    {
                        **asdict(row),
                        "parser_profile": parser_profile,
                        "competition_stream": competition_stream,
                        "representation_primary": representation_primary,
                        "status": status,
                        "validation_status": validation_status,
                        "duplicate_exact": duplicate_exact,
                        "summary": validation_summary,
                    }
                )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
