#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import psycopg
from psycopg.types.json import Jsonb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from isu_parser.local_config import database_dsn
from isu_parser.video_pipeline import (
    NameCandidate,
    VideoAccessBlocked,
    VideoPipelineError,
    download_media_asset,
    match_name_hits,
    resolve_playback_url,
    transcript_segments_from_result,
    transcript_text_from_segments,
    transcribe_with_whisper,
)


DEFAULT_CACHE_DIR = Path("data/video_cache")
DEFAULT_MODEL = "small"


def normalize_slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def company_slug_from_watch_url(watch_url: str) -> str:
    match = re.search(r"https?://[^/]+/([^/]+)/watch/", watch_url)
    if match:
        return match.group(1)
    return "dscup"


def media_slug_from_watch_url(watch_url: str) -> str:
    path = urlparse(watch_url).path.rstrip("/")
    return path.rsplit("/", 1)[-1]


def load_event_video_sources(
    cur: psycopg.Cursor,
    *,
    event_name: str | None,
    video_source_id: int | None,
    limit: int | None,
) -> list[dict[str, Any]]:
    clauses = []
    params: list[Any] = []
    if event_name:
        clauses.append("evs.event_name = %s")
        params.append(event_name)
    if video_source_id is not None:
        clauses.append("evs.id = %s")
        params.append(video_source_id)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    limit_sql = f"LIMIT {int(limit)}" if limit is not None else ""
    cur.execute(
        f"""
        SELECT
          evs.id,
          evs.event_id,
          evs.import_run_id,
          evs.event_name,
          evs.competition_date,
          evs.stream_day,
          evs.region_scope,
          evs.source_platform,
          evs.source_page_url,
          evs.posted_url_text,
          evs.video_url,
          evs.root_result_url,
          evs.url_status,
          evs.access_model,
          evs.download_status,
          evs.local_video_path,
          evs.video_metadata,
          evs.notes,
          evs.transcript_status
        FROM ingest.event_video_sources evs
        {where}
        ORDER BY evs.competition_date, evs.stream_day, evs.region_scope, evs.id
        {limit_sql}
        """,
        params,
    )
    columns = [desc[0] for desc in cur.description or []]
    return [dict(zip(columns, row, strict=False)) for row in cur.fetchall()]


def load_candidate_names(cur: psycopg.Cursor, event_id: int) -> list[NameCandidate]:
    cur.execute(
        """
        SELECT
          id,
          raw_name,
          normalized_name,
          segment_id,
          category_id,
          appearance_type,
          source_row_order
        FROM ingest.source_skater_appearances
        WHERE event_id = %s
        ORDER BY normalized_name, appearance_type, source_row_order NULLS LAST, id
        """,
        (event_id,),
    )
    rows = cur.fetchall()
    best_by_name: dict[str, NameCandidate] = {}
    for row in rows:
        candidate = NameCandidate(
            skater_appearance_id=int(row[0]),
            raw_name=row[1],
            normalized_name=row[2],
            segment_id=int(row[3]) if row[3] is not None else None,
            category_id=int(row[4]) if row[4] is not None else None,
        )
        key = candidate.normalized_name.upper()
        priority = 0 if row[5] == "segment_result" else 1 if row[5] == "pdf_score" else 2 if row[5] == "category_result" else 3
        existing = best_by_name.get(key)
        if existing is None:
            best_by_name[key] = candidate
            continue
        existing_priority = 0 if existing.segment_id is not None else 1
        if priority < existing_priority:
            best_by_name[key] = candidate
            continue
        if existing.segment_id is None and candidate.segment_id is not None:
            best_by_name[key] = candidate
    return sorted(best_by_name.values(), key=lambda item: item.normalized_name)


def upsert_transcript(
    cur: psycopg.Cursor,
    *,
    video_source_id: int,
    transcript_text: str,
    language_code: str | None,
    generated_by: str,
    raw: dict[str, Any],
) -> int:
    cur.execute(
        """
        INSERT INTO ingest.event_video_transcripts (
          video_source_id, language_code, transcript_format, transcript_text,
          generated_by, started_at, completed_at, raw
        )
        VALUES (%s, %s, 'plain_text', %s, %s, now(), now(), %s)
        ON CONFLICT (video_source_id, coalesce(language_code, ''), transcript_format)
        DO UPDATE SET
          transcript_text = EXCLUDED.transcript_text,
          generated_by = EXCLUDED.generated_by,
          completed_at = now(),
          raw = EXCLUDED.raw
        RETURNING id
        """,
        (
            video_source_id,
            language_code,
            transcript_text,
            generated_by,
            Jsonb(raw),
        ),
    )
    return int(cur.fetchone()[0])


def replace_timecodes(
    cur: psycopg.Cursor,
    *,
    video_source_id: int,
    transcript_id: int,
    hits: list[dict[str, Any]],
) -> int:
    cur.execute(
        "DELETE FROM ingest.event_video_skater_timecodes WHERE video_source_id = %s AND transcript_id = %s",
        (video_source_id, transcript_id),
    )
    inserted = 0
    for hit in hits:
        cur.execute(
            """
            INSERT INTO ingest.event_video_skater_timecodes (
              video_source_id, transcript_id, segment_id, skater_appearance_id,
              raw_name, normalized_name, start_time_seconds, end_time_seconds,
              confidence, evidence_kind, transcript_excerpt, raw
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                video_source_id,
                transcript_id,
                hit.get("segment_id"),
                hit.get("skater_appearance_id"),
                hit.get("raw_name") or "",
                hit.get("normalized_name") or "",
                hit.get("start_time_seconds"),
                hit.get("end_time_seconds"),
                hit.get("confidence"),
                hit.get("evidence_kind") or "transcript_match",
                hit.get("transcript_excerpt"),
                Jsonb(hit),
            ),
        )
        inserted += 1
    return inserted


def update_video_source(
    cur: psycopg.Cursor,
    *,
    video_source_id: int,
    download_status: str | None = None,
    transcript_status: str | None = None,
    local_video_path: str | None = None,
    video_metadata: dict[str, Any] | None = None,
    notes: str | None = None,
) -> None:
    parts = []
    params: list[Any] = []
    if download_status is not None:
        parts.append("download_status = %s")
        params.append(download_status)
    if transcript_status is not None:
        parts.append("transcript_status = %s")
        params.append(transcript_status)
    if local_video_path is not None:
        parts.append("local_video_path = %s")
        params.append(local_video_path)
    if video_metadata is not None:
        parts.append("video_metadata = video_metadata || %s")
        params.append(Jsonb(video_metadata))
    if notes is not None:
        parts.append("notes = %s")
        params.append(notes)
    if not parts:
        return
    parts.append("updated_at = now()")
    sql = f"UPDATE ingest.event_video_sources SET {', '.join(parts)} WHERE id = %s"
    params.append(video_source_id)
    cur.execute(sql, params)


def process_video_source(
    conn: psycopg.Connection,
    *,
    source: dict[str, Any],
    cache_dir: Path,
    model_name: str,
    language: str | None,
    admin_access: bool,
    overwrite: bool,
    dry_run: bool,
) -> dict[str, Any]:
    event_name = source["event_name"]
    competition_date = source["competition_date"]
    stream_day = source["stream_day"]
    region_scope = source["region_scope"]
    watch_url = source["video_url"] or source["source_page_url"] or source["posted_url_text"]
    if not watch_url:
        raise VideoPipelineError(f"missing watch URL for video source {source['id']}")
    parsed_slug = media_slug_from_watch_url(watch_url)
    company = company_slug_from_watch_url(watch_url)
    resolution = resolve_playback_url(
        company=company,
        media_object_slug=parsed_slug,
        description_url=watch_url,
        admin_access=admin_access,
    )

    report: dict[str, Any] = {
        "video_source_id": source["id"],
        "event_name": event_name,
        "competition_date": str(competition_date),
        "stream_day": stream_day,
        "region_scope": region_scope,
        "watch_url": watch_url,
        "media_object_ident": resolution["media_object_ident"],
        "allow_download": resolution["allow_download"],
        "access_restriction": resolution["access_restriction"],
        "download_status": resolution["download_status"],
        "stream_status": resolution["stream_status"],
    }

    with conn.cursor() as cur:
        update_video_source(
            cur,
            video_source_id=source["id"],
            video_metadata={
                "resolved_media_object": resolution["media_object"],
                "resolved_media_object_ident": resolution["media_object_ident"],
                "download_response": resolution["download_payload"],
                "stream_response": resolution["stream_payload"],
                "allow_download": resolution["allow_download"],
                "access_restriction": resolution["access_restriction"],
                "resolved_at": datetime.now(timezone.utc).isoformat(),
            },
        )

        if resolution["blocked"]:
            update_video_source(
                cur,
                video_source_id=source["id"],
                download_status="blocked",
                transcript_status="skipped",
                notes="; ".join(
                    [
                        source["notes"] if source.get("notes") else "",
                        resolution["blocked"]["reason"],
                    ]
                ).strip("; "),
            )
            if not dry_run:
                conn.commit()
            report["status"] = "blocked"
            report["reason"] = resolution["blocked"]["reason"]
            return report

        download_url = resolution["download_url"] or resolution["stream_url"]
        if not download_url:
            update_video_source(
                cur,
                video_source_id=source["id"],
                download_status="failed",
                transcript_status="failed",
                notes="No downloadable or streamable URL was returned by the API.",
            )
            if not dry_run:
                conn.commit()
            report["status"] = "failed"
            report["reason"] = "missing_download_url"
            return report

        if dry_run:
            conn.rollback()
            report["status"] = "would_download"
            report["download_url"] = download_url
            report["planned_path"] = str(
                cache_dir
                / normalize_slug(f"{event_name}-day-{stream_day}-{region_scope}")
                / f"{resolution['media_object_ident']}.mp4"
            )
            return report

        cache_slug = normalize_slug(f"{event_name}-day-{stream_day}-{region_scope}")
        destination = cache_dir / cache_slug / f"{resolution['media_object_ident']}.mp4"
        if destination.exists() and not overwrite:
            local_path = destination
        else:
            local_path = download_media_asset(
                download_url,
                destination,
                headers={"Referer": watch_url, "Origin": "https://ice-stars.tv"},
            )
        update_video_source(
            cur,
            video_source_id=source["id"],
            download_status="downloaded",
            local_video_path=str(local_path),
            notes=source.get("notes"),
        )
        candidates = load_candidate_names(cur, source["event_id"])
        transcript_result = transcribe_with_whisper(local_path, model_name=model_name, language=language)
        transcript_segments = transcript_segments_from_result(transcript_result)
        transcript_text = transcript_text_from_segments(transcript_segments)
        transcript_id = upsert_transcript(
            cur,
            video_source_id=source["id"],
            transcript_text=transcript_text,
            language_code=transcript_result.get("language") if isinstance(transcript_result, dict) else language,
            generated_by=f"whisper:{model_name}",
            raw=transcript_result,
        )
        hits = match_name_hits(transcript_segments=transcript_segments, candidates=candidates)
        inserted_hits = replace_timecodes(cur, video_source_id=source["id"], transcript_id=transcript_id, hits=hits)
        update_video_source(
            cur,
            video_source_id=source["id"],
            transcript_status="completed",
        )
        conn.commit()
        report.update(
            {
                "status": "completed",
                "download_path": str(local_path),
                "transcript_id": transcript_id,
                "timecode_hits": inserted_hits,
                "matched_names": len({hit["normalized_name"] for hit in hits}),
                "transcript_language": transcript_result.get("language"),
            }
        )
        return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Download and transcribe event video sources.")
    parser.add_argument("--config", help="Local env file with database settings.")
    parser.add_argument("--event-name", help="Process only a single event name.")
    parser.add_argument("--video-source-id", type=int, help="Process only one video source row.")
    parser.add_argument("--limit", type=int, help="Limit the number of rows processed.")
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--language")
    parser.add_argument("--admin-access", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    report: list[dict[str, Any]] = []
    with psycopg.connect(database_dsn(args.config)) as conn:
        with conn.cursor() as cur:
            sources = load_event_video_sources(
                cur,
                event_name=args.event_name,
                video_source_id=args.video_source_id,
                limit=args.limit,
            )
        for source in sources:
            try:
                item = process_video_source(
                    conn,
                    source=source,
                    cache_dir=args.cache_dir,
                    model_name=args.model,
                    language=args.language,
                    admin_access=args.admin_access,
                    overwrite=args.overwrite,
                    dry_run=args.dry_run,
                )
            except VideoAccessBlocked as exc:
                with conn.cursor() as cur:
                    update_video_source(
                        cur,
                        video_source_id=source["id"],
                        download_status="blocked",
                        transcript_status="skipped",
                        notes=str(exc),
                    )
                    if not args.dry_run:
                        conn.commit()
                item = {
                    "video_source_id": source["id"],
                    "event_name": source["event_name"],
                    "status": "blocked",
                    "reason": str(exc),
                }
            except VideoPipelineError as exc:
                with conn.cursor() as cur:
                    update_video_source(
                        cur,
                        video_source_id=source["id"],
                        download_status="failed",
                        transcript_status="failed",
                        notes=str(exc),
                    )
                    if not args.dry_run:
                        conn.commit()
                item = {
                    "video_source_id": source["id"],
                    "event_name": source["event_name"],
                    "status": "failed",
                    "reason": str(exc),
                }
            report.append(item)
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
