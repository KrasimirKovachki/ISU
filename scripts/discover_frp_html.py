#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urljoin, urlparse
from urllib.request import Request, urlopen

import psycopg
from bs4 import BeautifulSoup
from psycopg.types.json import Jsonb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from isu_parser import fs_manager
from isu_parser.local_config import database_dsn
from isu_parser.source_check import preflight_result_url


FRP_HTML_URL = "https://www.frponline.ro/HTML/"
USER_AGENT = "isu-skating-frp-discovery/0.1"


@dataclass(frozen=True)
class FrpResultFolder:
    name: str
    url: str
    index_url: str
    last_modified: str | None = None


def fetch_text(url: str) -> str:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=60) as response:
        encoding = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(encoding, errors="replace")


def discover_from_html(html: str, source_url: str = FRP_HTML_URL) -> list[FrpResultFolder]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[FrpResultFolder] = []
    for link in soup.select('td a[href^="/HTML/"], td a[href^="./"], td a[href]'):
        href = link.get("href") or ""
        name = link.get_text(" ", strip=True)
        if not href or not name or name == "Parent Directory":
            continue
        absolute = urljoin(source_url, href)
        parsed = urlparse(absolute)
        if not parsed.path.startswith("/HTML/") or not parsed.path.endswith("/"):
            continue
        if unquote(parsed.path.rstrip("/")).rsplit("/", 1)[-1].lower() in {"flags", "protocol"}:
            continue
        cells = link.find_parent("tr").find_all("td") if link.find_parent("tr") else []
        last_modified = cells[1].get_text(" ", strip=True) if len(cells) > 1 else None
        rows.append(
            FrpResultFolder(
                name=name,
                url=absolute,
                index_url=urljoin(absolute, "index.htm"),
                last_modified=last_modified or None,
            )
        )
    return rows


def event_prefix(url: str) -> str:
    path = urlparse(url).path.strip("/")
    for suffix in ("/index.htm", "/index.html"):
        if path.lower().endswith(suffix):
            return path[: -len(suffix)]
    return path


def profile_key_for(prefix: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", prefix.lower()).strip("_")
    digest = hashlib.sha1(prefix.encode("utf-8")).hexdigest()[:8]
    return f"frp_html_{cleaned[:54]}_{digest}"


def validate_result_index(url: str) -> tuple[str, dict[str, Any], str]:
    source_check = preflight_result_url(url)
    if not source_check.ok:
        return "fs_manager", source_check.summary(), "failed"
    html = source_check.body.decode("windows-1252", errors="replace")
    try:
        parsed = fs_manager.parse_index(html, source_check.parse_url)
    except Exception as exc:  # noqa: BLE001 - discovery diagnostics should keep parser errors.
        return "fs_manager", {**source_check.summary(), "parse_error": str(exc)}, "failed"

    categories = len(parsed.get("categories", []))
    segments = sum(len(category.get("segments", [])) for category in parsed.get("categories", []))
    summary = {
        **source_check.summary(),
        "platform": "FS Manager",
        "event": parsed.get("event", {}),
        "categories": categories,
        "segments": segments,
        "schedule_items": len(parsed.get("schedule", [])),
    }
    if categories <= 0:
        summary["reason"] = "no categories parsed from result index"
        return "fs_manager", summary, "failed"
    return "fs_manager", summary, "passed"


def completed_import_urls(conn: psycopg.Connection) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT root_url FROM ingest.import_runs WHERE status = 'completed'")
        return {row[0] for row in cur.fetchall()}


def upsert_profile(cur: psycopg.Cursor, profile_key: str, prefix: str) -> int:
    settings = {
        "representation": {
            "primary": "nation",
            "store_nation": True,
            "store_club": True,
        },
        "source_family": "frp_html_autoindex",
        "source_system": "fs_manager",
    }
    cur.execute(
        """
        INSERT INTO ingest.source_profiles (
          profile_key, parser_profile, host, event_path_prefix,
          competition_stream, representation_primary, settings
        )
        VALUES (%s, 'fs_manager', 'www.frponline.ro', %s, 'FRP', 'nation', %s)
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
        (profile_key, prefix, Jsonb(settings)),
    )
    return int(cur.fetchone()[0])


def upsert_registry(
    cur: psycopg.Cursor,
    row: FrpResultFolder,
    profile_id: int | None,
    status: str,
    validation_status: str,
    summary: dict[str, Any],
    source_url: str,
) -> None:
    cur.execute(
        """
        INSERT INTO ingest.source_url_registry (
          url, resolved_url, source_profile_id, parser_profile, competition_stream,
          status, validation_status, last_validated_at, summary, notes
        )
        VALUES (%s, %s, %s, 'fs_manager', 'FRP', %s, %s, now(), %s, %s)
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
        """,
        (
            row.index_url,
            summary.get("resolved_url"),
            profile_id,
            status,
            validation_status,
            Jsonb({**asdict(row), **summary, "source": source_url}),
            "Discovered from FRP /HTML/ auto-index.",
        ),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Discover FRP FS Manager result folders from /HTML/.")
    parser.add_argument("--url", default=FRP_HTML_URL)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--config", help="Local env file with database settings.")
    args = parser.parse_args()

    rows = discover_from_html(fetch_text(args.url), args.url)
    if args.limit:
        rows = rows[: args.limit]

    report: list[dict[str, Any]] = []
    with psycopg.connect(database_dsn(args.config)) as conn:
        imported_urls = completed_import_urls(conn)
        with conn.transaction(), conn.cursor() as cur:
            for row in rows:
                parser_profile, summary, validation_status = validate_result_index(row.index_url)
                duplicate_exact = row.index_url in imported_urls
                status = "ready_for_import" if validation_status == "passed" and not duplicate_exact else "analyzed"
                if validation_status == "failed":
                    status = "skipped"
                profile_id = None
                if not args.dry_run and validation_status == "passed":
                    profile_id = upsert_profile(cur, profile_key_for(event_prefix(row.index_url)), event_prefix(row.index_url))
                    upsert_registry(cur, row, profile_id, status, validation_status, summary, args.url)
                report.append(
                    {
                        **asdict(row),
                        "parser_profile": parser_profile,
                        "competition_stream": "FRP",
                        "representation_primary": "nation",
                        "status": status,
                        "validation_status": validation_status,
                        "duplicate_exact": duplicate_exact,
                        "summary": summary,
                    }
                )

    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
