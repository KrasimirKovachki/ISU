#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import html as html_lib
import json
import re
import sys
from dataclasses import asdict, dataclass
from html.parser import HTMLParser
from typing import Iterable
from urllib.parse import urljoin
from urllib.request import Request, urlopen


FILTERED_EVENTS_URL = "https://www.isu-skating.com/events/?month=All&discipline=FIGURE+SKATING&season=2025%2F2026&event_type=All+ISU+Events"
EVENTS_URL = FILTERED_EVENTS_URL
BASE_URL = "https://www.isu-skating.com"
USER_AGENT = "isu-skating-event-discovery/0.1"
DATE_RE = re.compile(r"^\d{1,2}\s+[A-Z][a-z]{2}\s+-\s+\d{1,2}\s+[A-Z][a-z]{2},\s+\d{4}$")
COUNTRY_RE = re.compile(r"^(?P<city>.+?)\s*/\s*(?P<country>[A-Z]{3})$")
FIGURE_DETAIL_RE = re.compile(r'href="(/figure-skating/events/eventdetail/[^"]+/)"')


@dataclass(frozen=True)
class IsuEvent:
    source_page_url: str
    detail_url: str | None
    result_url: str | None
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
    detail_urls = [
        urljoin(BASE_URL, value)
        for value in dict.fromkeys(FIGURE_DETAIL_RE.findall(html))
    ]
    events: list[IsuEvent] = []
    seen_events: set[tuple[str, str, str, str]] = set()
    figure_index = 0
    index = 0
    while index < len(parts):
        if not DATE_RE.match(parts[index]):
            index += 1
            continue

        date_range = parts[index]
        event_name = parts[index + 1] if index + 1 < len(parts) else ""
        location = parts[index + 2] if index + 2 < len(parts) else ""
        discipline = parts[index + 3] if index + 3 < len(parts) else None
        if discipline != "FIGURE SKATING":
            index += 4
            continue
        country_match = COUNTRY_RE.match(location)
        city = country_match.group("city").strip() if country_match else None
        country_code = country_match.group("country") if country_match else None
        source_kind = "isu_official_catalog"
        if "Adult Competition" in event_name:
            source_kind = "isu_adult_catalog"
        elif "International" in event_name or "Challenger Series" in event_name:
            source_kind = "isu_international_catalog"
        event_key = (date_range, event_name, city or "", country_code or "")
        if event_key in seen_events:
            index += 4
            continue
        seen_events.add(event_key)

        events.append(
            IsuEvent(
                source_page_url=source_page_url,
                detail_url=detail_urls[figure_index] if figure_index < len(detail_urls) else None,
                result_url=None,
                date_range=date_range,
                event_name=event_name,
                city=city,
                country_code=country_code,
                discipline=discipline,
                source_kind=source_kind,
                discovery_status="catalog_only_needs_result_url",
            )
        )
        figure_index += 1
        index += 4
    return events


def unescape_next_value(value: str) -> str:
    return html_lib.unescape(value).replace("\\u0026", "&").replace("\\/", "/")


def first_json_string_field(html: str, field_name: str) -> str | None:
    patterns = [
        rf'"{re.escape(field_name)}":"([^"]*)"',
        rf'\\"{re.escape(field_name)}\\":\\"([^"]*)\\"',
    ]
    for pattern in patterns:
        match = re.search(pattern, html)
        if match:
            value = match.group(1)
            return unescape_next_value(value)
    return None


def pageinfos_html_slice(html: str) -> str:
    starts = [pos for marker in ('"pageinfos"', r'\"pageinfos\"') if (pos := html.find(marker)) >= 0]
    if not starts:
        return html
    start = min(starts)
    ends = [
        pos
        for marker in ('"eventdatas"', r'\"eventdatas\"')
        if (pos := html.find(marker, start)) >= 0
    ]
    end = min(ends) if ends else start + 50000
    return html[start:end]


def discover_detail_from_html(html: str, detail_url: str) -> IsuEvent:
    event_html = pageinfos_html_slice(html)
    name = first_json_string_field(event_html, "name") or ""
    display_date = first_json_string_field(event_html, "display_date") or ""
    city = first_json_string_field(event_html, "city")
    country_code = first_json_string_field(event_html, "country_code")
    discipline = first_json_string_field(event_html, "discipline_title") or first_json_string_field(event_html, "api_sport")
    result_url = first_json_string_field(event_html, "detail_result_url")
    if not result_url:
        match = re.search(r'href="(https?://[^"]+)"><span[^>]+data-hover="Entries\s*&amp;\s*Results"', html)
        result_url = html_lib.unescape(match.group(1)) if match else None
    source_kind = "isu_official_catalog"
    if "Adult Competition" in name:
        source_kind = "isu_adult_catalog"
    elif "International" in name or "Challenger Series" in name:
        source_kind = "isu_international_catalog"
    return IsuEvent(
        source_page_url=detail_url,
        detail_url=detail_url,
        result_url=result_url or None,
        date_range=display_date,
        event_name=name,
        city=city,
        country_code=country_code,
        discipline=discipline,
        source_kind=source_kind,
        discovery_status="candidate_result_url" if result_url else "catalog_only_needs_result_url",
    )


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
