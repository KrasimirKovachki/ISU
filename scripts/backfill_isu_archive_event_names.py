"""Backfill event names for isuresults.com runs whose result-index page
omits the title (Swiss Timing pages from older seasons frequently drop
the <title> + header). The URL is the only reliable source of the event
identity in those cases.

Resolves the (code, year) embedded in the URL to a human-readable ISU
event name. Updates ``ingest.events.name`` in place — ``mart.dim_event``
and ``api.mv_*`` (which are live views over ingest.events for the name
column) reflect the change automatically.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from isu_parser.local_config import database_dsn


SENIOR_EVENT_NAMES = {
    "wc":           "ISU World Figure Skating Championships",
    "wjc":          "ISU World Junior Figure Skating Championships",
    "ec":           "ISU European Figure Skating Championships",
    "4cc":          "ISU Four Continents Figure Skating Championships",
    "gpf":          "ISU Grand Prix of Figure Skating Final",
    "jgpf":         "ISU Junior Grand Prix of Figure Skating Final",
    "wtt":          "ISU World Team Trophy in Figure Skating",
    "wug":          "FISU World University Games — Figure Skating",
    "olg":          "Olympic Winter Games — Figure Skating",
    "sa":           "ISU Grand Prix of Figure Skating — Skate America",
    "sc":           "ISU Grand Prix of Figure Skating — Skate Canada International",
    "coc":          "ISU Grand Prix of Figure Skating — Cup of China",
    "nhk":          "ISU Grand Prix of Figure Skating — NHK Trophy",
    "gpfra":        "ISU Grand Prix of Figure Skating — Internationaux de France",
    "gpgermany":    "ISU Grand Prix of Figure Skating — Germany",
    "gpfin":        "ISU Grand Prix of Figure Skating — Finlandia Trophy",
    "gpita":        "ISU Grand Prix of Figure Skating — Gran Premio d'Italia",
    "gprostelecom": "ISU Grand Prix of Figure Skating — Rostelecom Cup",
    "rost":         "ISU Grand Prix of Figure Skating — Rostelecom Cup",
    "rostelecom":   "ISU Grand Prix of Figure Skating — Rostelecom Cup",
    "espoo":        "ISU Grand Prix of Figure Skating — Espoo",
}

# JGP and CS venues use a country code suffix.
COUNTRY_NAMES = {
    "pol": "Poland",   "aut": "Austria",  "cze": "Czech Republic", "fra": "France",
    "ita": "Italy",    "sui": "Switzerland", "can": "Canada",      "est": "Estonia",
    "geo": "Georgia",  "kaz": "Kazakhstan",  "tur": "Turkey",      "lat": "Latvia",
    "bel": "Belarus",  "hun": "Hungary",     "slo": "Slovenia",    "slv": "Slovenia",
    "gbr": "Great Britain", "fin": "Finland", "swe": "Sweden",     "kor": "South Korea",
    "jpn": "Japan",    "tha": "Thailand",    "chn": "China",       "grc": "Greece",
    "cro": "Croatia",  "mex": "Mexico",      "usa": "USA",         "aze": "Azerbaijan",
    "rus": "Russia",   "bra": "Brazil",      "ger": "Germany",     "tai": "Taipei",
    "swz": "Switzerland", "isr": "Israel",   "rom": "Romania",     "roumania": "Romania",
    "armn": "Armenia", "chile": "Chile",
}

# Specific CS event branding when the country alone isn't the canonical name.
CS_SPECIFIC = {
    "csger":   "ISU Challenger Series — Nebelhorn Trophy",
    "csaut":   "ISU Challenger Series — Cup of Austria",
    "cstai":   "ISU Challenger Series — Asian Open Trophy",
    "csfin":   "ISU Challenger Series — Finlandia Trophy",
    "csita":   "ISU Challenger Series — Lombardia Trophy",
    "csfra":   "ISU Challenger Series — Master's de Patinage",
    "cscan":   "ISU Challenger Series — Autumn Classic International",
    "cspol":   "ISU Challenger Series — Warsaw Cup",
    "csgbr":   "ISU Challenger Series — UK / Skate Helsinki",
    "csswe":   "ISU Challenger Series — Skate Helsinki",
    "cscze":   "ISU Challenger Series — Memorial Janka Stastka",
    "csusa":   "ISU Challenger Series — US International Classic",
    "cshun":   "ISU Challenger Series — Budapest Trophy",
    "csest":   "ISU Challenger Series — Tallinn Trophy",
    "csgeo":   "ISU Challenger Series — Tbilisi Cup",
    "csbel":   "ISU Challenger Series — Belgium Open",
    "csrus":   "ISU Challenger Series — Russia",
    "cstur":   "ISU Challenger Series — Turkey",
    "csgrc":   "ISU Challenger Series — Greece",
    "csisr":   "ISU Challenger Series — Israel",
    "cskor":   "ISU Challenger Series — Korea",
    "csrom":   "ISU Challenger Series — Romania",
    "csroumania": "ISU Challenger Series — Romania",
    "csswz":   "ISU Challenger Series — Switzerland",
}


CODE_RE = re.compile(r"^https?://www\.isuresults\.com/results/season\d{4}/([a-z0-9]+?)(\d{4})/?")


def derive_name(url: str) -> str | None:
    match = CODE_RE.match(url)
    if not match:
        return None
    code, year = match.group(1), match.group(2)

    if code in CS_SPECIFIC:
        return f"{CS_SPECIFIC[code]} {year}"
    if code in SENIOR_EVENT_NAMES:
        return f"{SENIOR_EVENT_NAMES[code]} {year}"
    if code.startswith("jgp"):
        country_code = code[3:]
        country = COUNTRY_NAMES.get(country_code, country_code.upper())
        return f"ISU Junior Grand Prix of Figure Skating — {country} {year}"
    if code.startswith("cs"):
        country_code = code[2:]
        country = COUNTRY_NAMES.get(country_code, country_code.upper())
        return f"ISU Challenger Series — {country} {year}"
    if code.startswith("gp"):
        country_code = code[2:]
        country = COUNTRY_NAMES.get(country_code, country_code.upper())
        return f"ISU Grand Prix of Figure Skating — {country} {year}"
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Show planned updates, do not write")
    parser.add_argument("--all", action="store_true", help="Backfill all isuresults.com rows (not just empty names)")
    args = parser.parse_args()

    where = "AND COALESCE(trim(e.name), '') = ''" if not args.all else ""

    with psycopg.connect(database_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT e.id, e.source_url, e.name
                FROM ingest.events e
                JOIN ingest.import_runs ir ON ir.id = e.import_run_id
                WHERE ir.status = 'completed'
                  AND e.source_url ILIKE '%isuresults.com%'
                  {where}
                ORDER BY e.source_url
                """
            )
            rows = cur.fetchall()

        updates: list[tuple[int, str, str, str]] = []
        for event_id, url, current_name in rows:
            derived = derive_name(url)
            if not derived:
                print(f"  ! could not derive name for {url}")
                continue
            updates.append((event_id, url, current_name or "(empty)", derived))

        for _, url, old, new in updates:
            print(f"  {url}")
            print(f"    {old}  ->  {new}")
        print(f"\n{len(updates)} rows to update")

        if args.dry_run or not updates:
            return

        with conn.cursor() as cur:
            for event_id, _, _, derived in updates:
                cur.execute(
                    "UPDATE ingest.events SET name = %s WHERE id = %s",
                    (derived, event_id),
                )
        conn.commit()
        print(f"\nupdated {len(updates)} ingest.events rows")


if __name__ == "__main__":
    main()
