#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import urljoin
from urllib.request import Request, urlopen


BASE_URL = "https://www.bsf.bg"
PAGE_URL = f"{BASE_URL}/figure-skating/national-championships"

BASE_PATHS = {
    "Ke": "/figure-skating/national-championships",
    "Wa": "/figure-skating/priz-victoria",
    "wd": "/figure-skating/black-sea-ice-cup",
    "en": "/figure-skating/ice-peak-trophy",
    "ii": "/figure-skating/kontrolno-sastezanie",
    "wc": "/figure-skating/test-mf",
    "ej": "/figure-skating/spring-cup",
    "tj": "/figure-skating/ice-blade-adult-open-cup",
}


@dataclass(frozen=True)
class DiscoveredUrl:
    season: str
    competition_name: str
    source_kind: str
    url: str


def fetch_text(url: str) -> str:
    request = Request(url, headers={"User-Agent": "isu-skating-data-parser/0.1"})
    with urlopen(request, timeout=30) as response:
        encoding = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(encoding, errors="replace")


def extract_bundle_url(page_html: str) -> str:
    match = re.search(r'<script[^>]+src="([^"]*?/assets/index-[^"]+\.js)"', page_html)
    if not match:
        raise ValueError("Could not find BSF application bundle URL.")
    return urljoin(BASE_URL, match.group(1))


def find_matching(text: str, start: int, open_char: str, close_char: str) -> int:
    depth = 0
    quote: str | None = None
    escaped = False
    for index in range(start, len(text)):
        char = text[index]
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char in ("'", '"', "`"):
            quote = char
        elif char == open_char:
            depth += 1
        elif char == close_char:
            depth -= 1
            if depth == 0:
                return index
    raise ValueError(f"Could not find matching {close_char!r}.")


def split_top_level_array(array_text: str) -> list[str]:
    inner = array_text.strip()
    if inner.startswith("[") and inner.endswith("]"):
        inner = inner[1:-1]
    items: list[str] = []
    start = 0
    depth = 0
    quote: str | None = None
    escaped = False
    for index, char in enumerate(inner):
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char in ("'", '"', "`"):
            quote = char
        elif char in "[{(":
            depth += 1
        elif char in "]})":
            depth -= 1
        elif char == "," and depth == 0:
            items.append(inner[start:index].strip())
            start = index + 1
    tail = inner[start:].strip()
    if tail:
        items.append(tail)
    return items


def parse_js_string(text: str, start: int) -> tuple[str, int]:
    quote = text[start]
    if quote not in ("'", '"'):
        raise ValueError("Expected a JavaScript string.")
    chars: list[str] = []
    escaped = False
    for index in range(start + 1, len(text)):
        char = text[index]
        if escaped:
            chars.append(char)
            escaped = False
        elif char == "\\":
            escaped = True
        elif char == quote:
            return "".join(chars), index + 1
        else:
            chars.append(char)
    raise ValueError("Unterminated JavaScript string.")


def prop_expr(object_text: str, prop: str) -> str | None:
    match = re.search(rf"(?:^|[{{,]){re.escape(prop)}:", object_text)
    if not match:
        return None
    start = match.end()
    depth = 0
    quote: str | None = None
    escaped = False
    for index in range(start, len(object_text)):
        char = object_text[index]
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char in ("'", '"', "`"):
            quote = char
        elif char in "[{(":
            depth += 1
        elif char in "]})":
            if depth == 0:
                return object_text[start:index].strip()
            depth -= 1
        elif char == "," and depth == 0:
            return object_text[start:index].strip()
    return object_text[start:].strip()


def string_prop(object_text: str, prop: str) -> str | None:
    expr = prop_expr(object_text, prop)
    if not expr or expr[0] not in ("'", '"'):
        return None
    value, _ = parse_js_string(expr, 0)
    return value


def resolve_path_expr(expr: str | None) -> str | None:
    if not expr:
        return None
    expr = expr.strip()
    if expr.startswith(("'", '"')):
        value, _ = parse_js_string(expr, 0)
        return urljoin(BASE_URL, value.strip())
    match = re.fullmatch(r'de\((\w+),\s*(["\'])(.*?)\2\)', expr)
    if match:
        base_key = match.group(1)
        suffix = match.group(3)
        base_path = BASE_PATHS.get(base_key)
        if not base_path:
            return None
        return urljoin(BASE_URL, f"{base_path}/{suffix}")
    return None


def extract_lj_array(bundle_text: str) -> str:
    marker = "lj=["
    start = bundle_text.find(marker)
    if start == -1:
        raise ValueError("Could not find figure-skating national championships data array.")
    array_start = start + len("lj=")
    array_end = find_matching(bundle_text, array_start, "[", "]")
    return bundle_text[array_start : array_end + 1]


def discover(bundle_text: str) -> list[DiscoveredUrl]:
    array_text = extract_lj_array(bundle_text)
    rows: list[DiscoveredUrl] = []
    for season_object in split_top_level_array(array_text):
        season = string_prop(season_object, "title") or ""
        data_start = season_object.find("data:[")
        if data_start == -1:
            continue
        items_start = data_start + len("data:")
        items_end = find_matching(season_object, items_start, "[", "]")
        for item in split_top_level_array(season_object[items_start : items_end + 1]):
            name = string_prop(item, "title") or ""
            competition_url = resolve_path_expr(prop_expr(item, "competitionPath"))
            if not competition_url:
                continue
            rows.append(DiscoveredUrl(season, name, "competition", competition_url))

            result = string_prop(item, "result")
            if result:
                rows.append(DiscoveredUrl(season, name, "result", urljoin(competition_url.rstrip("/") + "/", result)))
                continue

            results_expr = prop_expr(item, "results")
            if results_expr:
                for label, _, folder in re.findall(r'(\w+):(["\'])(.*?)\2', results_expr):
                    rows.append(
                        DiscoveredUrl(
                            season,
                            name,
                            f"result_{label}",
                            f"{competition_url.rstrip('/')}/{folder}/index.htm",
                        )
                    )
                continue

            rows.append(DiscoveredUrl(season, name, "result_default", f"{competition_url.rstrip('/')}/index.htm"))
    return rows


def write_csv(rows: Iterable[DiscoveredUrl]) -> None:
    writer = csv.DictWriter(sys.stdout, fieldnames=["season", "competition_name", "source_kind", "url"])
    writer.writeheader()
    for row in rows:
        writer.writerow(row.__dict__)


def main() -> int:
    parser = argparse.ArgumentParser(description="Discover figure-skating result URLs from the BSF national championships page.")
    parser.add_argument("--format", choices=("json", "csv"), default="json")
    args = parser.parse_args()

    page_html = fetch_text(PAGE_URL)
    bundle_url = extract_bundle_url(page_html)
    rows = discover(fetch_text(bundle_url))

    if args.format == "csv":
        write_csv(rows)
    else:
        print(json.dumps([row.__dict__ for row in rows], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
