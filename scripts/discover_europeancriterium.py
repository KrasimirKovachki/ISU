#!/usr/bin/env python3
# Edited 2026-10-12 by Claude (AI assistant): European Criterium discovery. www.europeancriterium.eu is only an
# ASP.NET index; each finished competition's "Entries/Results - ISU" and "Entries/Results" (CC = Criterium,
# recreational) buttons are postbacks that redirect to ordinary FS Manager result sites (hosted there or on
# dev.mbit.si, clubdenkovastaviski.com, frponline.ro, ...). This script follows those buttons, resolves the
# final index.htm, checks it with the FS Manager parser and registers it in ingest.source_url_registry.
from __future__ import annotations

import argparse
import hashlib
import http.cookiejar
import json
import re
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode, urljoin, urlparse
from urllib.request import HTTPCookieProcessor, HTTPRedirectHandler, Request, build_opener

import psycopg
from bs4 import BeautifulSoup
from psycopg.types.json import Jsonb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from isu_parser import fs_manager
from isu_parser.local_config import database_dsn
from isu_parser.source_check import decode_html, preflight_result_url


EC_LIST_URL = "http://www.europeancriterium.eu/competitions_list"
USER_AGENT = "isu-skating-ec-discovery/0.1"
GRID_TARGET = "ctl00$MainContent$GridView2"
# Button label on the site → competition_stream in the registry.
BUTTONS = {"Entries/Results - ISU": "ISU", "Entries/Results": "NonISU"}


@dataclass(frozen=True)
class EcResultSite:
    competition: str
    season: str
    row: int
    part: str  # "ISU" or "CC"
    stream: str
    redirect_url: str
    index_url: str


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):  # noqa: ANN002, ANN003 - keep the 302 Location for us
        return None


def _opener():
    jar = http.cookiejar.CookieJar()
    return build_opener(HTTPCookieProcessor(jar), _NoRedirect())


def _get(opener, url: str) -> str:
    with opener.open(Request(url, headers={"User-Agent": USER_AGENT}), timeout=60) as response:
        return decode_html(response.read())


def _form_state(html: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    return {
        field["name"]: field.get("value", "")
        for field in soup.select("input[type=hidden][name]")
        if field["name"].startswith("__")
    }


def list_competitions(html: str) -> list[dict[str, Any]]:
    """Rows of the competitions grid: name, season and which result buttons exist (with their postback argument)."""
    soup = BeautifulSoup(html, "html.parser")
    grid = soup.find(id=re.compile(r"GridView2$"))
    rows: list[dict[str, Any]] = []
    for tr in grid.find_all("tr") if grid else []:
        buttons: dict[str, str] = {}
        for element in tr.select("input[type=submit], input[type=button], a[href*='__doPostBack']"):
            label = (element.get("value") or element.get_text(" ", strip=True) or "").strip()
            script = element.get("onclick") or element.get("href") or ""
            match = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", script)
            if label in BUTTONS and match:
                buttons[label] = match.group(2)
        if not buttons:
            continue
        cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        season = next((c for c in cells if c.lower().startswith("season ")), "")
        name = next((c for c in cells if c and not c.lower().startswith("season ")), "")
        rows.append({"name": name, "season": season, "buttons": buttons})
    return rows


def follow_button(opener, argument: str) -> str | None:
    """Press one result button (fresh view state each time) and return where the site sends us."""
    state = _form_state(_get(opener, EC_LIST_URL))
    data = urlencode({**state, "__EVENTTARGET": GRID_TARGET, "__EVENTARGUMENT": argument}).encode()
    request = Request(EC_LIST_URL, data=data, headers={"User-Agent": USER_AGENT})
    try:
        with opener.open(request, timeout=60) as response:
            return None if response.status == 200 else response.headers.get("Location")
    except HTTPError as exc:
        if exc.code in (301, 302, 303, 307):
            location = exc.headers.get("Location")
            return urljoin(EC_LIST_URL, location) if location else None
        return None


def index_url_for(url: str) -> str:
    """Folder links ("…/EC/tt2026/") point at the FS Manager index.htm inside them."""
    path = urlparse(url).path
    if path.lower().endswith((".htm", ".html")):
        return url
    return url.rstrip("/") + "/index.htm"


def validate_result_index(url: str) -> tuple[dict[str, Any], str, str]:
    source_check = preflight_result_url(url)
    final_url = source_check.parse_url
    if not source_check.ok:
        return source_check.summary(), "failed", final_url
    try:
        parsed = fs_manager.parse_index(decode_html(source_check.body), final_url)
    except Exception as exc:  # noqa: BLE001 - discovery diagnostics should keep parser errors.
        return {**source_check.summary(), "parse_error": str(exc)}, "failed", final_url
    categories = parsed.get("categories", [])
    summary = {
        **source_check.summary(),
        "platform": "FS Manager",
        "event": parsed.get("event", {}),
        "categories": len(categories),
        "segments": sum(len(c.get("segments", [])) for c in categories),
        "schedule_items": len(parsed.get("schedule", [])),
    }
    if not categories:
        summary["reason"] = "no categories parsed from result index"
        return summary, "failed", final_url
    return summary, "passed", final_url


def event_prefix(url: str) -> str:
    path = urlparse(url).path.strip("/")
    return path.rsplit("/", 1)[0] if "/" in path and path.lower().endswith((".htm", ".html")) else path


def profile_key_for(host: str, prefix: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", f"{host}_{prefix}".lower()).strip("_")
    digest = hashlib.sha1(f"{host}/{prefix}".encode("utf-8")).hexdigest()[:8]
    return f"ec_{cleaned[:56]}_{digest}"


def upsert_profile(cur: psycopg.Cursor, url: str, stream: str) -> int:
    host, prefix = urlparse(url).netloc, event_prefix(url)
    settings = {
        "representation": {"primary": "nation", "store_nation": True, "store_club": True},
        "source_family": "europeancriterium",
        "source_system": "fs_manager",
    }
    cur.execute(
        """
        INSERT INTO ingest.source_profiles (
          profile_key, parser_profile, host, event_path_prefix,
          competition_stream, representation_primary, settings
        )
        VALUES (%s, 'fs_manager', %s, %s, %s, 'nation', %s)
        ON CONFLICT (profile_key) DO UPDATE SET
          parser_profile = EXCLUDED.parser_profile,
          competition_stream = EXCLUDED.competition_stream,
          settings = EXCLUDED.settings,
          updated_at = now()
        RETURNING id
        """,
        (profile_key_for(host, prefix), host, prefix, stream, Jsonb(settings)),
    )
    return int(cur.fetchone()[0])


def upsert_registry(cur: psycopg.Cursor, site: EcResultSite, url: str, profile_id: int | None,
                    status: str, validation_status: str, summary: dict[str, Any]) -> None:
    cur.execute(
        """
        INSERT INTO ingest.source_url_registry (
          url, resolved_url, source_profile_id, parser_profile, competition_stream,
          status, validation_status, last_validated_at, summary, notes
        )
        VALUES (%s, %s, %s, 'fs_manager', %s, %s, %s, now(), %s, %s)
        ON CONFLICT (url) DO UPDATE SET
          resolved_url = EXCLUDED.resolved_url,
          source_profile_id = COALESCE(ingest.source_url_registry.source_profile_id, EXCLUDED.source_profile_id),
          status = CASE WHEN ingest.source_url_registry.status = 'imported'
                        THEN ingest.source_url_registry.status ELSE EXCLUDED.status END,
          validation_status = CASE WHEN ingest.source_url_registry.status = 'imported'
                        THEN ingest.source_url_registry.validation_status ELSE EXCLUDED.validation_status END,
          last_validated_at = now(),
          summary = ingest.source_url_registry.summary || EXCLUDED.summary,
          updated_at = now()
        """,
        (
            url, summary.get("resolved_url"), profile_id, site.stream, status, validation_status,
            Jsonb({**asdict(site), **summary, "source": EC_LIST_URL}),
            f"Discovered from europeancriterium.eu ({site.part} part).",
        ),
    )


def completed_import_urls(conn: psycopg.Connection) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT root_url FROM ingest.import_runs WHERE status = 'completed'")
        return {row[0] for row in cur.fetchall()}


def main() -> int:
    parser = argparse.ArgumentParser(description="Discover FS Manager result sites listed on europeancriterium.eu.")
    parser.add_argument("--dry-run", action="store_true", help="Only print what was found; write nothing.")
    parser.add_argument("--limit", type=int, help="Only the first N competitions (newest first).")
    parser.add_argument("--delay", type=float, default=1.0, help="Seconds between requests to the site.")
    parser.add_argument("--config", help="Local env file with database settings.")
    args = parser.parse_args()

    opener = _opener()
    competitions = list_competitions(_get(opener, EC_LIST_URL))
    if args.limit:
        competitions = competitions[: args.limit]

    sites: list[EcResultSite] = []
    for row, competition in enumerate(competitions):
        for label, argument in competition["buttons"].items():
            time.sleep(args.delay)
            location = follow_button(opener, argument)
            if not location or location.rstrip("/").endswith("404"):
                continue
            stream = BUTTONS[label]
            sites.append(EcResultSite(
                competition=competition["name"], season=competition["season"], row=row,
                part="ISU" if stream == "ISU" else "CC", stream=stream,
                redirect_url=location, index_url=index_url_for(location),
            ))

    report: list[dict[str, Any]] = []
    with psycopg.connect(database_dsn(args.config)) as conn:
        imported = completed_import_urls(conn)
        with conn.transaction(), conn.cursor() as cur:
            for site in sites:
                time.sleep(args.delay)
                summary, validation_status, final_url = validate_result_index(site.index_url)
                url = final_url or site.index_url
                duplicate = url in imported or site.index_url in imported
                status = "skipped" if validation_status == "failed" else (
                    "analyzed" if duplicate else "ready_for_import")
                if not args.dry_run:
                    profile_id = upsert_profile(cur, url, site.stream) if validation_status == "passed" else None
                    upsert_registry(cur, site, url, profile_id, status, validation_status, summary)
                report.append({**asdict(site), "url": url, "status": status,
                               "validation_status": validation_status, "duplicate_exact": duplicate,
                               "event": summary.get("event"), "categories": summary.get("categories"),
                               "reason": summary.get("reason") or summary.get("parse_error")})

    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
