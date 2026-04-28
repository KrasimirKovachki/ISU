#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import asdict, dataclass
from html.parser import HTMLParser
from typing import Iterable
from urllib.request import Request, urlopen


EVENTS_URL = "https://www.isu-skating.com/figure-skating/events/"
USER_AGENT = "isu-skating-event-discovery/0.1"
DATE_RE = re.compile(r"^\d{1,2}\s+[A-Z][a-z]{2}\s+-\s+\d{1,2}\s+[A-Z][a-z]{2},\s+\d{4}$")
COUNTRY_RE = re.compile(r"^(?P<city>.+?)\s*/\s*(?P<country>[A-Z]{3})$")


@dataclass(frozen=True)
class IsuEvent:
    source_page_url: str
    date_range: str
    event_name: str
    city: str | None
    country_code: str | None
    discipline: str | None
    source_kind: str
    discovery_status: str


class TextHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        text = " ".join(data.split())
        if text:
            self.parts.append(text)


def fetch_text(url: str) -> str:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=45) as response:
        encoding = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(encoding, errors="replace")


def page_text_parts(html: str) -> list[str]:
    parser = TextHTMLParser()
    parser.feed(html)
    return parser.parts


def discover_from_html(html: str, source_page_url: str = EVENTS_URL) -> list[IsuEvent]:
    parts = page_text_parts(html)
    events: list[IsuEvent] = []
    index = 0
    while index < len(parts):
        if not DATE_RE.match(parts[index]):
            index += 1
            continue

        date_range = parts[index]
        event_name = parts[index + 1] if index + 1 < len(parts) else ""
        location = parts[index + 2] if index + 2 < len(parts) else ""
        discipline = parts[index + 3] if index + 3 < len(parts) else None
        country_match = COUNTRY_RE.match(location)
        city = country_match.group("city").strip() if country_match else None
        country_code = country_match.group("country") if country_match else None
        source_kind = "isu_official_catalog"
        if "Adult Competition" in event_name:
            source_kind = "isu_adult_catalog"
        elif "International" in event_name or "Challenger Series" in event_name:
            source_kind = "isu_international_catalog"

        events.append(
            IsuEvent(
                source_page_url=source_page_url,
                date_range=date_range,
                event_name=event_name,
                city=city,
                country_code=country_code,
                discipline=discipline,
                source_kind=source_kind,
                discovery_status="catalog_only_needs_result_url",
            )
        )
        index += 4
    return events


def write_csv(rows: Iterable[IsuEvent]) -> None:
    fieldnames = [field for field in IsuEvent.__dataclass_fields__]
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(asdict(row))


def main() -> int:
    parser = argparse.ArgumentParser(description="Discover official ISU figure-skating event catalog rows.")
    parser.add_argument("--url", default=EVENTS_URL, help="ISU events page URL.")
    parser.add_argument("--format", choices=("json", "csv"), default="json")
    args = parser.parse_args()

    rows = discover_from_html(fetch_text(args.url), source_page_url=args.url)
    if args.format == "csv":
        write_csv(rows)
    else:
        print(json.dumps([asdict(row) for row in rows], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
