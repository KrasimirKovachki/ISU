#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import ssl
import sys
import tempfile
import re
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse
from urllib.error import URLError
from urllib.request import Request, urlopen

import psycopg
from psycopg.types.json import Jsonb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from isu_parser import fs_manager, old_isucalcfs
from isu_parser.local_config import DEFAULT_LOCAL_CONFIG, database_dsn
from isu_parser.pdf_scores import parse_judges_scores_pdf, validate_judges_scores
from isu_parser.source_check import preflight_result_url


USER_AGENT = "isu-skating-data-importer/0.1"


def fetch_bytes(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(request, timeout=60) as response:
            return response.read()
    except URLError as exc:
        if "CERTIFICATE_VERIFY_FAILED" not in str(exc.reason):
            raise
        with urlopen(request, timeout=60, context=ssl._create_unverified_context()) as response:  # noqa: S323
            return response.read()


def decode_html(data: bytes) -> str:
    return data.decode("windows-1252", errors="replace")


def normalize_name(value: str) -> str:
    return " ".join(value.upper().split())


def document_type(url: str) -> str:
    if urlparse(url).path.lower().endswith(".pdf"):
        return "judges_scores_pdf"
    guessed, _ = mimetypes.guess_type(url)
    return "html" if guessed in {None, "text/html"} else guessed


def choose_parser(profile: dict[str, Any] | None, url: str):
    parser_profile = profile.get("parser_profile") if profile else None
    if parser_profile == "old_isucalcfs" or "/2013/" in url:
        return old_isucalcfs
    return fs_manager


def is_old_isucalcfs_wrapper(html: str) -> bool:
    lower = html.lower()
    return "scripts/results" in lower and "pages/main" not in lower and "<body></body>" in lower.replace(" ", "")


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


def old_isucalcfs_main_url(url: str) -> str:
    path = urlparse(url).path.lower()
    main_name = "main.html" if path.endswith(".html") else "main.htm"
    return urljoin(url, f"pages/{main_name}")


def old_isucalcfs_main_candidates(url: str) -> list[str]:
    preferred = old_isucalcfs_main_url(url)
    candidates = [preferred, urljoin(url, "pages/main.html"), urljoin(url, "pages/main.htm")]
    unique: list[str] = []
    for candidate in candidates:
        if candidate not in unique:
            unique.append(candidate)
    return unique


def find_source_profile(conn: psycopg.Connection, url: str) -> dict[str, Any] | None:
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, profile_key, parser_profile, host, event_path_prefix, competition_stream, settings
            FROM ingest.source_profiles
            WHERE host = %s
              AND %s LIKE event_path_prefix || '%%'
            ORDER BY length(event_path_prefix) DESC
            LIMIT 1
            """,
            (parsed.netloc, path),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "profile_key": row[1],
        "parser_profile": row[2],
        "host": row[3],
        "event_path_prefix": row[4],
        "competition_stream": row[5],
        "settings": row[6] or {},
    }


def normalize_representation(record: dict[str, Any], profile: dict[str, Any] | None) -> dict[str, Any]:
    settings = (profile or {}).get("settings") or {}
    representation = settings.get("representation", {})
    normalized = dict(record)
    nation = normalized.get("nation")
    club = normalized.get("club")
    primary = representation.get("primary", (profile or {}).get("representation_primary", "nation"))
    nation_column = representation.get("nation_column")

    nation_is_iso = isinstance(nation, str) and bool(re.fullmatch(r"[A-Z]{3}", nation))

    if primary == "club" and nation and not club and nation_column == "club" and not nation_is_iso:
        normalized["club"] = nation
        normalized["nation"] = None
        normalized["representation_type"] = "club"
        normalized["representation_value"] = nation or None
        normalized["representation_note"] = "source nation column interpreted as club by source profile"
        return normalized

    if primary == "club" and nation and not nation_is_iso:
        normalized["nation"] = None
        normalized["representation_note"] = "ignored non-ISO nation value for club-primary source"

    if primary == "club":
        normalized["representation_type"] = "club"
        normalized["representation_value"] = club or None
    elif primary == "nation":
        normalized["representation_type"] = "nation"
        normalized["representation_value"] = nation or None
    return normalized


def insert_source_document(
    cur: psycopg.Cursor,
    import_run_id: int,
    source_profile_id: int | None,
    url: str,
    parser_profile: str,
    body: bytes,
    parse_status: str = "parsed",
    metadata: dict[str, Any] | None = None,
) -> int:
    raw_text = None if urlparse(url).path.lower().endswith(".pdf") else decode_html(body)
    cur.execute(
        """
        INSERT INTO ingest.source_documents (
          import_run_id, source_profile_id, url, document_type, parser_profile,
          content_hash, parse_status, raw_text, metadata
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (import_run_id, url) DO UPDATE SET
          content_hash = EXCLUDED.content_hash,
          parse_status = EXCLUDED.parse_status,
          raw_text = EXCLUDED.raw_text,
          metadata = EXCLUDED.metadata
        RETURNING id
        """,
        (
            import_run_id,
            source_profile_id,
            url,
            document_type(url),
            parser_profile,
            hashlib.sha256(body).hexdigest(),
            parse_status,
            raw_text,
            Jsonb(metadata or {}),
        ),
    )
    return int(cur.fetchone()[0])


def insert_parse_issues(
    cur: psycopg.Cursor,
    import_run_id: int,
    source_document_id: int | None,
    issues: list[dict[str, Any]],
) -> None:
    for issue in issues:
        cur.execute(
            """
            INSERT INTO ingest.parse_issues (
              import_run_id, source_document_id, level, code, message, context
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                import_run_id,
                source_document_id,
                issue.get("level", "warning"),
                issue.get("code", "unknown"),
                issue.get("message", ""),
                Jsonb({k: v for k, v in issue.items() if k not in {"level", "code", "message"}}),
            ),
        )


def insert_event(cur: psycopg.Cursor, import_run_id: int, source_profile_id: int | None, index: dict[str, Any]) -> int:
    event = index["event"]
    cur.execute(
        """
        INSERT INTO ingest.events (
          import_run_id, source_profile_id, source_url, source_context, name,
          location, venue, date_range, event_protocol_url, raw
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            import_run_id,
            source_profile_id,
            index.get("source_url"),
            Jsonb(index.get("source_context", {})),
            event.get("name") or "",
            event.get("location"),
            event.get("venue"),
            event.get("date_range"),
            (index.get("event_protocol_pdf") or {}).get("url"),
            Jsonb(index),
        ),
    )
    return int(cur.fetchone()[0])


def insert_category(cur: psycopg.Cursor, event_id: int, category: dict[str, Any], source_order: int) -> int:
    cur.execute(
        """
        INSERT INTO ingest.categories (event_id, name, entries_url, result_url, source_order, raw)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            event_id,
            category["name"],
            (category.get("entries") or {}).get("url"),
            (category.get("result") or {}).get("url"),
            source_order,
            Jsonb(category),
        ),
    )
    return int(cur.fetchone()[0])


def insert_segment(cur: psycopg.Cursor, category_id: int, segment: dict[str, Any], source_order: int) -> int:
    cur.execute(
        """
        INSERT INTO ingest.segments (
          category_id, name, officials_url, details_url, judges_scores_pdf_url, source_order, raw
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            category_id,
            segment["name"],
            (segment.get("officials") or {}).get("url"),
            (segment.get("details") or {}).get("url"),
            (segment.get("judges_scores_pdf") or {}).get("url"),
            source_order,
            Jsonb(segment),
        ),
    )
    return int(cur.fetchone()[0])


def insert_appearance(
    cur: psycopg.Cursor,
    import_run_id: int,
    event_id: int,
    category_id: int | None,
    segment_id: int | None,
    source_document_id: int | None,
    source_order: int,
    record: dict[str, Any],
    appearance_type: str,
    profile: dict[str, Any] | None = None,
) -> int:
    record = normalize_representation(record, profile)
    name = record.get("name") or record.get("raw_name") or ""
    cur.execute(
        """
        INSERT INTO ingest.source_skater_appearances (
          import_run_id, event_id, category_id, segment_id, source_document_id,
          source_row_order, raw_name, normalized_name, nation_code, club_name,
          representation_type, representation_value, bio_url, source_skater_id,
          appearance_type, raw
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            import_run_id,
            event_id,
            category_id,
            segment_id,
            source_document_id,
            source_order,
            name,
            normalize_name(name),
            record.get("nation"),
            record.get("club"),
            record.get("representation_type"),
            record.get("representation_value"),
            record.get("bio_url"),
            record.get("source_skater_id"),
            appearance_type,
            Jsonb(record),
        ),
    )
    return int(cur.fetchone()[0])


def import_entries(cur, import_run_id, profile_id, event_id, category_id, parser_mod, link, profile=None):
    if not link:
        return 0
    body = fetch_bytes(link["url"])
    doc_id = insert_source_document(cur, import_run_id, profile_id, link["url"], parser_mod.parse_index.__module__.split(".")[-1], body)
    try:
        parsed = parser_mod.parse_entries(decode_html(body), link["url"])
    except Exception as exc:
        insert_parse_issues(
            cur,
            import_run_id,
            doc_id,
            [{"level": "warning", "code": "entry_page_parse_failed", "message": str(exc), "url": link["url"]}],
        )
        return 0
    for idx, entry in enumerate(parsed.get("entries", []), start=1):
        insert_appearance(cur, import_run_id, event_id, category_id, None, doc_id, idx, entry, "entry", profile)
    return len(parsed.get("entries", []))


def import_category_result(cur, import_run_id, profile_id, event_id, category_id, parser_mod, link, profile=None):
    if not link:
        return 0
    body = fetch_bytes(link["url"])
    doc_id = insert_source_document(cur, import_run_id, profile_id, link["url"], parser_mod.parse_index.__module__.split(".")[-1], body)
    try:
        parsed = parser_mod.parse_category_result(decode_html(body), link["url"])
    except Exception as exc:
        insert_parse_issues(
            cur,
            import_run_id,
            doc_id,
            [{"level": "warning", "code": "category_result_page_parse_failed", "message": str(exc), "url": link["url"]}],
        )
        return 0
    for idx, result in enumerate(parsed.get("results", []), start=1):
        appearance_id = insert_appearance(cur, import_run_id, event_id, category_id, None, doc_id, idx, result, "category_result", profile)
        cur.execute(
            """
            INSERT INTO ingest.category_results (
              category_id, skater_appearance_id, final_place, points, segment_places, source_order, raw
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                category_id,
                appearance_id,
                result.get("final_place"),
                result.get("points"),
                Jsonb(result.get("segment_places", {})),
                idx,
                Jsonb(result),
            ),
        )
    return len(parsed.get("results", []))


def import_segment_details(cur, import_run_id, profile_id, event_id, category_id, segment_id, parser_mod, link, profile=None):
    if not link:
        return 0
    body = fetch_bytes(link["url"])
    doc_id = insert_source_document(cur, import_run_id, profile_id, link["url"], parser_mod.parse_index.__module__.split(".")[-1], body)
    try:
        parsed = parser_mod.parse_segment_result(decode_html(body), link["url"])
    except Exception as exc:
        insert_parse_issues(
            cur,
            import_run_id,
            doc_id,
            [{"level": "warning", "code": "segment_result_page_parse_failed", "message": str(exc), "url": link["url"]}],
        )
        return 0
    for idx, result in enumerate(parsed.get("results", []), start=1):
        appearance_id = insert_appearance(cur, import_run_id, event_id, category_id, segment_id, doc_id, idx, result, "segment_result", profile)
        cur.execute(
            """
            INSERT INTO ingest.segment_results (
              segment_id, skater_appearance_id, place, tss, tes, pcs, deduction,
              starting_number, components, source_order, raw
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                segment_id,
                appearance_id,
                result.get("place"),
                result.get("tss"),
                result.get("tes"),
                result.get("pcs"),
                result.get("deduction"),
                result.get("starting_number"),
                Jsonb(result.get("components", {})),
                idx,
                Jsonb(result),
            ),
        )
    return len(parsed.get("results", []))


def import_officials(cur, import_run_id, profile_id, segment_id, parser_mod, link):
    if not link:
        return 0
    body = fetch_bytes(link["url"])
    doc_id = insert_source_document(cur, import_run_id, profile_id, link["url"], parser_mod.parse_index.__module__.split(".")[-1], body)
    parsed = parser_mod.parse_officials(decode_html(body), link["url"])
    for idx, official in enumerate(parsed.get("officials", []), start=1):
        cur.execute(
            """
            INSERT INTO ingest.official_assignments (
              segment_id, source_document_id, function_text, role_group,
              judge_number, name, nation_code, source_order, raw
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                segment_id,
                doc_id,
                official.get("function"),
                official.get("role_group"),
                official.get("judge_number"),
                official.get("name"),
                official.get("nation"),
                idx,
                Jsonb(official),
            ),
        )
    return len(parsed.get("officials", []))


def import_pdf(cur, import_run_id, profile_id, event_id, category_id, segment_id, parser_profile, link, category_name=None, segment_name=None, profile=None):
    if not link:
        return 0
    body = fetch_bytes(link["url"])
    doc_id = insert_source_document(
        cur,
        import_run_id,
        profile_id,
        link["url"],
        parser_profile,
        body,
        metadata={"link": link},
    )
    if "judges scores" not in (link.get("text") or "").lower():
        insert_parse_issues(
            cur,
            import_run_id,
            doc_id,
            [
                {
                    "level": "info",
                    "code": "skipped_non_judges_pdf_report",
                    "message": f"Skipped non-judges report link: {link.get('text') or link['url']}",
                }
            ],
        )
        return 0
    if not body.startswith(b"%PDF"):
        insert_parse_issues(
            cur,
            import_run_id,
            doc_id,
            [
                {
                    "level": "warning",
                    "code": "invalid_pdf_response",
                    "message": f"Expected PDF bytes but source returned {body[:40]!r}.",
                }
            ],
        )
        return 0
    with tempfile.NamedTemporaryFile(suffix=".pdf") as tmp:
        tmp.write(body)
        tmp.flush()
        try:
            parsed = parse_judges_scores_pdf(tmp.name)
        except Exception as exc:
            insert_parse_issues(
                cur,
                import_run_id,
                doc_id,
                [
                    {
                        "level": "warning",
                        "code": "pdf_parse_failed",
                        "message": str(exc),
                    }
                ],
            )
            return 0
    if parsed.get("report_type") != "MLAD_FIGURIST_TEST_RESULT":
        if category_name and not parsed.get("category"):
            parsed["category"] = category_name
        if segment_name and not parsed.get("segment"):
            parsed["segment"] = segment_name
    cur.execute(
        "UPDATE ingest.source_documents SET metadata = metadata || %s WHERE id = %s",
        (Jsonb({"parsed": parsed}), doc_id),
    )
    insert_parse_issues(cur, import_run_id, doc_id, validate_judges_scores(parsed))
    if parsed.get("report_type") == "MLAD_FIGURIST_TEST_RESULT":
        for idx, row in enumerate(parsed.get("test_results", []), start=1):
            cur.execute(
                """
                INSERT INTO ingest.pdf_mlad_figurist_results (
                  segment_id, source_document_id, source_order, rank, skater_name,
                  normalized_name, club_name, crossings, judge_votes_over_75,
                  average_percent, average_percent_text, result_text, passed, raw
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    segment_id,
                    doc_id,
                    idx,
                    row.get("rank"),
                    row.get("name"),
                    normalize_name(row.get("name", "")),
                    row.get("club"),
                    row.get("crossings"),
                    row.get("judge_votes_over_75"),
                    row.get("average_percent"),
                    row.get("average_percent_text"),
                    row.get("result"),
                    row.get("passed"),
                    Jsonb(row),
                ),
            )
        return len(parsed.get("test_results", []))
    count = 0
    for idx, skater in enumerate(parsed.get("skaters", []), start=1):
        appearance_id = insert_appearance(cur, import_run_id, event_id, category_id, segment_id, doc_id, idx, skater, "pdf_score", profile)
        cur.execute(
            """
            INSERT INTO ingest.pdf_score_summaries (
              segment_id, source_document_id, skater_appearance_id, rank, starting_number,
              total_segment_score, total_element_score, total_program_component_score,
              total_deductions, printed_at, judge_count, base_value_total,
              element_score_total, deductions_detail, raw
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                segment_id,
                doc_id,
                appearance_id,
                skater.get("rank"),
                skater.get("starting_number"),
                skater.get("total_segment_score"),
                skater.get("total_element_score"),
                skater.get("total_program_component_score"),
                skater.get("total_deductions"),
                parsed.get("printed_at"),
                skater.get("judge_count"),
                skater.get("base_value_total"),
                skater.get("element_score_total"),
                Jsonb(skater.get("deductions_detail", {})),
                Jsonb(skater),
            ),
        )
        summary_id = int(cur.fetchone()[0])
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
            element_id = int(cur.fetchone()[0])
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
            component_id = int(cur.fetchone()[0])
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
        count += 1
    return count


def import_event(conn: psycopg.Connection, url: str) -> int:
    profile = find_source_profile(conn, url)
    parser_mod = choose_parser(profile, url)
    parser_profile = parser_mod.parse_index.__module__.split(".")[-1]
    profile_id = profile["id"] if profile else None
    source_check = preflight_result_url(url)
    if not source_check.ok:
        raise ValueError(f"URL preflight failed for {url}: {source_check.reason or source_check.content_kind}")
    if source_check.content_kind == "pdf":
        raise ValueError(f"URL preflight passed but direct PDF root imports need a dedicated importer: {url}")

    checked_lower = decode_html(source_check.body).lower()
    if "isucalcfs" in checked_lower:
        parser_mod = old_isucalcfs
        parser_profile = "old_isucalcfs"
    elif "fs manager" in checked_lower or "judgesdetailsperskater" in checked_lower:
        parser_mod = fs_manager
        parser_profile = "fs_manager"

    stats: dict[str, int] = {
        "categories": 0,
        "segments": 0,
        "entries": 0,
        "category_results": 0,
        "segment_results": 0,
        "officials": 0,
        "pdf_score_summaries": 0,
        "pdf_test_results": 0,
    }
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingest.import_runs (source_profile_id, root_url) VALUES (%s, %s) RETURNING id",
            (profile_id, url),
        )
        import_run_id = int(cur.fetchone()[0])
        try:
            parse_url = source_check.parse_url
            parse_body = source_check.body
            metadata: dict[str, Any] = {"url_preflight": source_check.summary()}
            if source_check.resolution == "meta_refresh":
                metadata["meta_refresh_from"] = url
            if source_check.resolution == "scripts_results_wrapper":
                metadata["wrapper_url"] = url
            index_doc_id = insert_source_document(
                cur,
                import_run_id,
                profile_id,
                parse_url,
                parser_profile,
                parse_body,
                metadata=metadata,
            )
            index = parser_mod.parse_index(decode_html(parse_body), parse_url)
            index["source_url"] = url
            if parse_url != url:
                index["source_context"] = {**index.get("source_context", {}), "resolved_index_url": parse_url}
            if hasattr(parser_mod, "validate_event_index"):
                insert_parse_issues(cur, import_run_id, index_doc_id, parser_mod.validate_event_index(index))
            event_id = insert_event(cur, import_run_id, profile_id, index)
            for idx, item in enumerate(index.get("schedule", []), start=1):
                cur.execute(
                    """
                    INSERT INTO ingest.schedule_items (
                      event_id, category_name, segment_name, source_date, source_time,
                      segment_details_url, source_order, raw
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        event_id,
                        item.get("category", ""),
                        item.get("segment", ""),
                        item.get("date"),
                        item.get("time"),
                        (item.get("segment_details") or {}).get("url"),
                        idx,
                        Jsonb(item),
                    ),
                )
            for category_order, category in enumerate(index.get("categories", []), start=1):
                category_id = insert_category(cur, event_id, category, category_order)
                stats["categories"] += 1
                stats["entries"] += import_entries(cur, import_run_id, profile_id, event_id, category_id, parser_mod, category.get("entries"), profile)
                stats["category_results"] += import_category_result(cur, import_run_id, profile_id, event_id, category_id, parser_mod, category.get("result"), profile)
                for segment_order, segment in enumerate(category.get("segments", []), start=1):
                    segment_id = insert_segment(cur, category_id, segment, segment_order)
                    stats["segments"] += 1
                    stats["officials"] += import_officials(cur, import_run_id, profile_id, segment_id, parser_mod, segment.get("officials"))
                    stats["segment_results"] += import_segment_details(cur, import_run_id, profile_id, event_id, category_id, segment_id, parser_mod, segment.get("details"), profile)
                    pdf_count = import_pdf(
                        cur,
                        import_run_id,
                        profile_id,
                        event_id,
                        category_id,
                        segment_id,
                        parser_profile,
                        segment.get("judges_scores_pdf"),
                        category.get("name"),
                        segment.get("name"),
                        profile,
                    )
                    if category.get("name") == "Mlad figurist":
                        stats["pdf_test_results"] += pdf_count
                    else:
                        stats["pdf_score_summaries"] += pdf_count
            cur.execute(
                "UPDATE ingest.import_runs SET status = 'completed', finished_at = now(), stats = %s WHERE id = %s",
                (Jsonb(stats), import_run_id),
            )
            cur.execute(
                """
                UPDATE ingest.source_url_registry
                SET status = 'imported',
                    last_import_run_id = %s,
                    updated_at = now()
                WHERE url = %s
                """,
                (import_run_id, url),
            )
            return import_run_id
        except Exception as exc:
            cur.execute(
                "UPDATE ingest.import_runs SET status = 'failed', finished_at = now(), error_message = %s, stats = %s WHERE id = %s",
                (str(exc), Jsonb(stats), import_run_id),
            )
            cur.execute(
                """
                UPDATE ingest.source_url_registry
                SET status = 'failed',
                    last_import_run_id = %s,
                    updated_at = now()
                WHERE url = %s
                """,
                (import_run_id, url),
            )
            raise


def main() -> int:
    parser = argparse.ArgumentParser(description="Import a skating result source into PostgreSQL.")
    parser.add_argument("url")
    parser.add_argument("--config", default=str(DEFAULT_LOCAL_CONFIG), help="Local env file with database settings.")
    parser.add_argument("--dsn", help="Database DSN. Overrides --config and SKATING_* environment variables.")
    args = parser.parse_args()
    dsn = args.dsn or database_dsn(args.config)
    with psycopg.connect(dsn) as conn:
        import_run_id = import_event(conn, args.url)
    print(json.dumps({"import_run_id": import_run_id}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
