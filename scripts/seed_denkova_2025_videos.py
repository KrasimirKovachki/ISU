#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from isu_parser.local_config import database_dsn


EVENT_NAME = "Denkova-Staviski Cup 2025"
SOURCE_PLATFORM = "ice-stars.tv"
DAY_1_VIDEO_URL = "https://ice-stars.tv/dscup/watch/denkova-staviski-cup-2025-day-1?fbclid=IwY2xjawRi3kZleHRuA2FlbQIxMABicmlkETBWM3JTU0pyZURxSVJXNWJwc3J0YwZhcHBfaWQQMjIyMDM5MTc4ODIwMDg5MgABHny2WOXnSC4kCJmVQnrE3ltgaInqnW-UP7iKmKvkrWyhtYUmXsgwRuRagGYd_aem_oRC6KTmHmalfu5Q9InHUkg"
DAY_2_VIDEO_URL = "https://ice-stars.tv/dscup/watch/denkova-staviski-cup-2025-day-2?fbclid=IwZXh0bgNhZW0CMTAAYnJpZBEwVjNyU1NKcmVEcUlSVzVicHNydGMGYXBwX2lkEDIyMjAzOTE3ODgyMDA4OTIAAR4Kc3nZ7gAF540NSKl4ury4xRXrddWIUlRWl4wkIxYILEH2p8s52OiXkAizVA_aem__7F8qkNtV8Sm4A5grZsA4Q"
DAY_3_RESULT_URL = "https://cup.clubdenkovastaviski.com/2025/ISU/index.htm"
DAY_3_SOLO_RESULT_URL = "https://cup.clubdenkovastaviski.com/2025/SoloIceDance/index.htm"
DAY_3_VIDEO_URL = "https://ice-stars.tv/dscup/watch/denkova-staviski-cup-2025-day-3?fbclid=IwZXh0bgNhZW0CMTAAYnJpZBEwVjNyU1NKcmVEcUlSVzVicHNydGMGYXBwX2lkEDIyMjAzOTE3ODgyMDA4OTIAAR7jchrwtJtHz_0g5blbyWTHZ1J6iB_jZV0udew-SoS8dkobMoZOXuPUL0JUtg_aem_i0vXxfI_UoQXzGXhcuvLsw"
DAY_4_RESULT_URL = "https://cup.clubdenkovastaviski.com/2025/SoloIceDance/index.htm"
DAY_4_ALT_RESULT_URL = "https://cup.clubdenkovastaviski.com/2025/ISU/index.htm"
DAY_4_VIDEO_URL = "https://ice-stars.tv/dscup/watch/denkova-staviski-cup-2025-day-4?fbclid=IwZXh0bgNhZW0CMTAAYnJpZBEwVjNyU1NKcmVEcUlSVzVicHNydGMGYXBwX2lkEDIyMjAzOTE3ODgyMDA4OTIAAR7jchrwtJtHz_0g5blbyWTHZ1J6iB_jZV0udew-SoS8dkobMoZOXuPUL0JUtg_aem_i0vXxfI_UoQXzGXhcuvLsw"
DAY_5_RESULT_URL = "https://cup.clubdenkovastaviski.com/2025/ISU/index.htm"
DAY_5_VIDEO_URL = "https://ice-stars.tv/dscup/watch/denkova-staviski-cup-2025-day-5?fbclid=IwZXh0bgNhZW0CMTAAYnJpZBEwVjNyU1NKcmVEcUlSVzVicHNydGMGYXBwX2lkEDIyMjAzOTE3ODgyMDA4OTIAAR52YIAwrxkY5fj7YO265W6Wxtp6bpFC8SeWlhNjkpE-PeRrRxeHx915-v1NHA_aem_OBe8HqYxEpSq6rVCN_GM_w"

RESULT_URLS_BY_DAY = {
    1: ["https://cup.clubdenkovastaviski.com/2025/NonISU/index.htm"],
    2: ["https://cup.clubdenkovastaviski.com/2025/NonISU/index.htm"],
    3: [
        DAY_3_RESULT_URL,
        DAY_3_SOLO_RESULT_URL,
    ],
    4: [
        DAY_4_RESULT_URL,
        DAY_4_ALT_RESULT_URL,
    ],
    5: [DAY_5_RESULT_URL],
}

STREAMS = [
    {
        "competition_date": date(2025, 11, 5),
        "stream_day": 1,
        "scheduled_start_local": "10:00",
        "region_scope": "bulgaria",
        "posted_url_text": DAY_1_VIDEO_URL,
        "video_url": DAY_1_VIDEO_URL,
        "url_status": "resolved",
        "access_model": "unknown",
    },
    {
        "competition_date": date(2025, 11, 5),
        "stream_day": 1,
        "scheduled_start_local": "10:00",
        "region_scope": "international",
        "posted_url_text": DAY_1_VIDEO_URL,
        "video_url": DAY_1_VIDEO_URL,
        "url_status": "resolved",
        "access_model": "unknown",
    },
    {
        "competition_date": date(2025, 11, 6),
        "stream_day": 2,
        "scheduled_start_local": "10:00",
        "region_scope": "bulgaria",
        "posted_url_text": DAY_2_VIDEO_URL,
        "video_url": DAY_2_VIDEO_URL,
        "url_status": "resolved",
        "access_model": "unknown",
    },
    {
        "competition_date": date(2025, 11, 6),
        "stream_day": 2,
        "scheduled_start_local": "10:00",
        "region_scope": "international",
        "posted_url_text": DAY_2_VIDEO_URL,
        "video_url": DAY_2_VIDEO_URL,
        "url_status": "resolved",
        "access_model": "unknown",
    },
    {
        "competition_date": date(2025, 11, 7),
        "stream_day": 3,
        "scheduled_start_local": "08:15",
        "region_scope": "bulgaria",
        "posted_url_text": DAY_3_VIDEO_URL,
        "video_url": DAY_3_VIDEO_URL,
        "url_status": "resolved",
        "access_model": "unknown",
    },
    {
        "competition_date": date(2025, 11, 7),
        "stream_day": 3,
        "scheduled_start_local": "08:15",
        "region_scope": "international",
        "posted_url_text": DAY_3_VIDEO_URL,
        "video_url": DAY_3_VIDEO_URL,
        "url_status": "resolved",
        "access_model": "unknown",
    },
    {
        "competition_date": date(2025, 11, 8),
        "stream_day": 4,
        "scheduled_start_local": "07:45",
        "region_scope": "bulgaria",
        "posted_url_text": DAY_4_VIDEO_URL,
        "video_url": DAY_4_VIDEO_URL,
        "url_status": "resolved",
        "access_model": "unknown",
    },
    {
        "competition_date": date(2025, 11, 8),
        "stream_day": 4,
        "scheduled_start_local": "07:45",
        "region_scope": "international",
        "posted_url_text": DAY_4_VIDEO_URL,
        "video_url": DAY_4_VIDEO_URL,
        "url_status": "resolved",
        "access_model": "unknown",
    },
    {
        "competition_date": date(2025, 11, 9),
        "stream_day": 5,
        "scheduled_start_local": "08:15",
        "region_scope": "bulgaria",
        "posted_url_text": DAY_5_VIDEO_URL,
        "video_url": DAY_5_VIDEO_URL,
        "url_status": "resolved",
        "access_model": "unknown",
    },
    {
        "competition_date": date(2025, 11, 9),
        "stream_day": 5,
        "scheduled_start_local": "08:15",
        "region_scope": "international",
        "posted_url_text": DAY_5_VIDEO_URL,
        "video_url": DAY_5_VIDEO_URL,
        "url_status": "resolved",
        "access_model": "unknown",
    },
]


def resolve_import_rows(cur: psycopg.Cursor, result_urls: list[str]) -> dict[str, tuple[int | None, int | None]]:
    cur.execute(
        """
        SELECT DISTINCT ON (ir.root_url)
          ir.root_url,
          ir.id AS import_run_id,
          e.id AS event_id
        FROM ingest.import_runs ir
        LEFT JOIN ingest.events e ON e.import_run_id = ir.id
        WHERE ir.root_url = ANY(%s)
          AND ir.status = 'completed'
        ORDER BY ir.root_url, ir.id DESC
        """,
        (result_urls,),
    )
    return {
        row[0]: (int(row[1]), int(row[2]) if row[2] is not None else None)
        for row in cur.fetchall()
    }


def upsert_stream(cur: psycopg.Cursor, row: dict[str, Any]) -> int:
    result_urls = RESULT_URLS_BY_DAY[row["stream_day"]]
    import_rows = resolve_import_rows(cur, result_urls)
    primary_url = result_urls[0] if result_urls else None
    import_run_id, event_id = import_rows.get(primary_url, (None, None))
    metadata = {
        "result_urls": result_urls,
        "scheduled_start_local": row["scheduled_start_local"],
        "source_text_language": "bg",
        "needs_full_url": True,
    }
    cur.execute(
        """
        INSERT INTO ingest.event_video_sources (
          event_id, import_run_id, root_result_url, event_name,
          competition_date, stream_day, region_scope, source_platform,
          posted_url_text, video_url, url_status, access_model, video_metadata, notes
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (
          event_name,
          competition_date,
          stream_day,
          region_scope
        )
        DO UPDATE SET
          event_id = COALESCE(EXCLUDED.event_id, ingest.event_video_sources.event_id),
          import_run_id = COALESCE(EXCLUDED.import_run_id, ingest.event_video_sources.import_run_id),
          root_result_url = COALESCE(EXCLUDED.root_result_url, ingest.event_video_sources.root_result_url),
          video_url = COALESCE(EXCLUDED.video_url, ingest.event_video_sources.video_url),
          url_status = EXCLUDED.url_status,
          access_model = EXCLUDED.access_model,
          video_metadata = ingest.event_video_sources.video_metadata || EXCLUDED.video_metadata,
          notes = EXCLUDED.notes,
          updated_at = now()
        RETURNING id
        """,
        (
            event_id,
            import_run_id,
            primary_url,
            EVENT_NAME,
            row["competition_date"],
            row["stream_day"],
            row["region_scope"],
            SOURCE_PLATFORM,
            row["posted_url_text"],
            row.get("video_url"),
            row.get("url_status", "partial"),
            row["access_model"],
            Jsonb(metadata),
            "Seeded from Facebook post text supplied by user; URL was truncated with ellipses.",
        ),
    )
    video_source_id = int(cur.fetchone()[0])
    for result_url in result_urls:
        linked_import_run_id, linked_event_id = import_rows.get(result_url, (None, None))
        cur.execute(
            """
            INSERT INTO ingest.event_video_result_links (
              video_source_id, import_run_id, event_id, root_result_url, link_role
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (video_source_id, root_result_url) DO UPDATE SET
              import_run_id = COALESCE(EXCLUDED.import_run_id, ingest.event_video_result_links.import_run_id),
              event_id = COALESCE(EXCLUDED.event_id, ingest.event_video_result_links.event_id),
              link_role = EXCLUDED.link_role
            """,
            (
                video_source_id,
                linked_import_run_id,
                linked_event_id,
                result_url,
                "primary_result_stream" if result_url == primary_url else "broadcast_covers_result_stream",
            ),
        )
    return video_source_id


def main() -> int:
    seeded = []
    with psycopg.connect(database_dsn()) as conn:
        with conn.cursor() as cur:
            for stream in STREAMS:
                seeded.append(upsert_stream(cur, stream))
        conn.commit()
    print(json.dumps({"count": len(seeded), "video_source_ids": seeded}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
