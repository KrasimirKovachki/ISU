"""Probe www.isuresults.com archive paths for known ISU event codes.

isuresults.com is the canonical ISU result archive but does not expose a
directory listing (the root and per-season paths return Azure 404s). The
URL convention is well known though:

    https://www.isuresults.com/results/season{YY}{YY}/{eventcode}{YYYY}/

For each season in the requested range we enumerate a curated list of
ISU event codes (Worlds, Continentals, Grand Prix venues, Challenger
Series venues, Junior Grand Prix venues) and probe the constructed URL.
URLs that respond 200 with a result-index shape are routed through the
existing source-check preflight and registered into
``ingest.source_url_registry`` as ``ready_for_import``.

Run ``scripts/import_ready_registry.py`` afterwards to ingest.
"""

from __future__ import annotations

import argparse
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Iterable
from dataclasses import dataclass

import psycopg

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))

from isu_parser import local_config
from scripts.seed_bsf_registry import detect_parser


ARCHIVE_HOST = "https://www.isuresults.com"

# Event codes that live under the same path convention regardless of season.
# Lists are intentionally curated — adding a code once probes it for every
# season in the requested range.
SENIOR_EVENT_CODES = [
    # Senior Worlds, Continental, Final
    "wc",       # ISU World Figure Skating Championships
    "wjc",      # ISU World Junior Figure Skating Championships
    "ec",       # European Championships
    "4cc",      # Four Continents Championships
    "gpf",      # Grand Prix Final
    "jgpf",     # Junior Grand Prix Final
    "olg",      # Olympic Winter Games
    "wtt",      # World Team Trophy
    "wug",      # FISU World University Games
    # Senior Grand Prix venues
    "sa",       # Skate America
    "sc",       # Skate Canada International
    "coc",      # Cup of China
    "nhk",      # NHK Trophy
    "gpfra",    # ISU Grand Prix de France / Internationaux de France
    "gpgermany",
    "gpfin",
    "gpita",
    "gprostelecom",
    "rost",
    "rostelecom",
    "espoo",
]

# Challenger Series venues (rotate across seasons; probing all is cheap)
CS_CODES = [
    "csger",     # Nebelhorn Trophy
    "csaut",     # Cup of Austria / Vienna Cup
    "cstai",     # Asian Open Trophy
    "csfin",     # Finlandia Trophy
    "csita",     # Lombardia Trophy
    "csfra",     # Master's de Patinage
    "cscan",     # Autumn Classic International
    "cspol",     # Warsaw Cup
    "csgbr",     # GB Open
    "csswe",     # Skate Helsinki / Tallinn
    "csswz",     # Sui equivalent
    "cscze",     # Memorial Janka Stastka
    "csusa",     # US International Classic
    "cshun",     # Budapest Trophy
    "csest",     # Tallinn Trophy
    "csgeo",     # Tbilisi Cup
    "csbel",     # Belgium / Brussels
    "csrus",     # Russian variants
    "cstur",     # Turkey
    "csgrc",     # Greece
    "csisr",     # Israel
    "cskor",     # Korea
    "csrom",     # Romania
    "csroumania",
]

# ISU Junior Grand Prix venues (the city/country can rotate)
JGP_CODES = [
    "jgppol",    # Poland
    "jgpaut",    # Austria
    "jgpcze",    # Czech Republic
    "jgpfra",    # France
    "jgpita",    # Italy
    "jgpsui",    # Switzerland
    "jgpcan",    # Canada
    "jgpest",    # Estonia
    "jgpgeo",    # Georgia
    "jgpkaz",    # Kazakhstan
    "jgptur",    # Turkey
    "jgplat",    # Latvia
    "jgpbel",    # Belarus
    "jgphun",    # Hungary
    "jgpslv",    # Slovenia / Ljubljana
    "jgpslo",
    "jgpgbr",    # Great Britain
    "jgpfin",    # Finland
    "jgpswe",    # Sweden
    "jgpkor",    # South Korea
    "jgpjpn",    # Japan
    "jgptha",    # Thailand / Bangkok
    "jgpchn",    # China / Xi'an
    "jgpgrc",    # Greece
    "jgpcro",    # Croatia
    "jgpmex",    # Mexico
    "jgpusa",    # USA
    "jgparmn",   # Armenia
    "jgpaze",    # Azerbaijan
    "jgpchile", # Chile
    "jgpita",    # Italy
    "jgprus",    # Russia
    "jgpbra",    # Brazil
]

ALL_CODES = sorted(set(SENIOR_EVENT_CODES + CS_CODES + JGP_CODES))


@dataclass
class ProbeResult:
    code: str
    season_short: str
    year: int
    url: str
    http_status: int | None
    accepted: bool
    parser_profile: str | None
    failure_reason: str | None


def season_short(start_year: int) -> str:
    """2024/2025 -> '2425'"""
    end = (start_year + 1) % 100
    return f"{start_year % 100:02d}{end:02d}"


def candidate_urls(start_year: int) -> list[tuple[str, int, str]]:
    """Return list of (code, year_used, full_url) for all curated codes.

    ISU events are held both in autumn (year = start_year, e.g. JGP Poland
    in Sept) and late winter / spring (year = start_year + 1, e.g. World
    Championships in March). Probe both variants per code.
    """
    season = season_short(start_year)
    end_year = start_year + 1
    candidates: list[tuple[str, int, str]] = []
    for code in ALL_CODES:
        for year in (start_year, end_year):
            candidates.append((code, year, f"{ARCHIVE_HOST}/results/season{season}/{code}{year}/"))
    return candidates


def fast_head_check(url: str, timeout: int = 8) -> int | None:
    """Quick HEAD-style probe. Returns HTTP status or None on network error."""
    request = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status
    except urllib.error.HTTPError as exc:
        return exc.code
    except (urllib.error.URLError, TimeoutError, OSError):
        return None


def probe_season(start_year: int, *, sleep_seconds: float = 0.05, verbose: bool = False) -> Iterable[ProbeResult]:
    season = season_short(start_year)
    for code, year, url in candidate_urls(start_year):
        status = fast_head_check(url)
        if status != 200:
            if verbose:
                print(f"  {url}  HTTP {status}")
            yield ProbeResult(code, season, year, url, status, False, None, None)
            continue
        try:
            parser_name, summary, vstatus = detect_parser(url)
        except Exception as exc:
            yield ProbeResult(code, season, year, url, 200, False, None, f"detect_error: {exc}")
            continue
        accepted = vstatus == "passed" and parser_name in {"fs_manager", "old_isucalcfs"}
        resolved = summary.get("resolved_url") or url
        if accepted:
            print(f"  ✓ {resolved}  -> {parser_name}")
            yield ProbeResult(code, season, year, resolved, 200, True, parser_name, None)
        else:
            if verbose:
                print(f"  · {url}  parser={parser_name} status={vstatus}")
            yield ProbeResult(code, season, year, url, 200, False, parser_name, f"status={vstatus}")
        time.sleep(sleep_seconds)


def register_passes(conn: psycopg.Connection, results: list[ProbeResult]) -> tuple[int, int, int]:
    """Insert/update ingest.source_url_registry rows for accepted URLs.

    Returns (inserted, updated, skipped) counts.
    """
    from psycopg.types.json import Jsonb

    inserted = updated = skipped = 0
    with conn.cursor() as cur:
        for r in results:
            if not r.accepted:
                continue
            note = f"isuresults_archive_probe season{r.season_short}/{r.code}{r.year}"
            cur.execute(
                "SELECT id, status FROM ingest.source_url_registry WHERE url = %s",
                (r.url,),
            )
            existing = cur.fetchone()
            if existing:
                rid, status = existing
                if status == "imported":
                    skipped += 1
                    continue
                cur.execute(
                    """
                    UPDATE ingest.source_url_registry
                    SET status = 'ready_for_import',
                        validation_status = 'passed',
                        parser_profile = COALESCE(%s, parser_profile),
                        notes = COALESCE(NULLIF(notes, ''), '') ||
                                CASE WHEN COALESCE(notes, '') = '' THEN '' ELSE E'\n' END || %s,
                        updated_at = now()
                    WHERE id = %s
                    """,
                    (r.parser_profile, note, rid),
                )
                updated += 1
            else:
                cur.execute(
                    """
                    INSERT INTO ingest.source_url_registry (
                        url, parser_profile, competition_stream,
                        status, validation_status, summary, notes
                    ) VALUES (%s, %s, %s, 'ready_for_import', 'passed', %s, %s)
                    """,
                    (
                        r.url,
                        r.parser_profile,
                        "ISU",
                        Jsonb({"discovery": "isuresults_archive_probe", "season": r.season_short, "code": r.code, "year": r.year}),
                        note,
                    ),
                )
                inserted += 1
    conn.commit()
    return inserted, updated, skipped


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-year", type=int, default=2018, help="Earliest season start year (inclusive)")
    parser.add_argument("--to-year", type=int, default=2025, help="Latest season start year (inclusive)")
    parser.add_argument("--register", action="store_true", help="Register accepted URLs into ingest.source_url_registry")
    parser.add_argument("--verbose", action="store_true", help="Print every probe outcome")
    args = parser.parse_args()

    print(f"Probing isuresults.com for seasons {args.from_year}/{args.from_year+1} .. {args.to_year}/{args.to_year+1}")
    print(f"Codes per season: {len(ALL_CODES)}")
    all_results: list[ProbeResult] = []
    for year in range(args.from_year, args.to_year + 1):
        print(f"\n=== Season {year}/{year + 1} (path: season{season_short(year)}/) ===")
        season_results = list(probe_season(year, verbose=args.verbose))
        accepted = [r for r in season_results if r.accepted]
        print(f"  {len(accepted)} accepted of {len(season_results)} probed")
        all_results.extend(season_results)

    accepted_total = [r for r in all_results if r.accepted]
    print(f"\n=== Totals: {len(accepted_total)} accepted of {len(all_results)} probed ===")
    for r in accepted_total:
        print(f"  {r.url}  ({r.parser_profile})")

    if args.register and accepted_total:
        with psycopg.connect(local_config.database_dsn()) as conn:
            ins, upd, skip = register_passes(conn, accepted_total)
        print(f"\nRegistry: inserted={ins}, updated={upd}, skipped (already imported)={skip}")


if __name__ == "__main__":
    main()
