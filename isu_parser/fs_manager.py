from __future__ import annotations

from typing import Any
from urllib.parse import urljoin, urlparse
import re

from bs4 import BeautifulSoup, Tag

from .old_isucalcfs import classify_official_function, clean_text, to_float, to_int


_BIO_ID_RE = re.compile(r"/bios/(isufs\d+)\.htm", re.IGNORECASE)


def parse_index(document: str, base_url: str = "") -> dict[str, Any]:
    soup = BeautifulSoup(document, "html.parser")
    title = clean_text(soup.title.get_text(" ")) if soup.title else ""
    event_name = title
    location = ""
    venue = ""
    date_range = ""
    protocol_pdf = None

    event_meta = soup.select_one("td.caption3")
    if event_meta:
        meta_row = event_meta.find_parent("tr")
        if meta_row:
            cells = [clean_text(cell.get_text(" ")) for cell in meta_row.find_all("td", recursive=False)]
            if len(cells) >= 2:
                location = cells[0]
                venue = cells[1]
    for cell in soup.select("tr.caption3 td"):
        text = clean_text(cell.get_text(" "))
        if re.match(r"\d{2}\.\d{2}\.\d{4}\s+-\s+\d{2}\.\d{2}\.\d{4}", text):
            date_range = text
        form = cell.find("form")
        if form and form.get("action"):
            protocol_pdf = _link_dict("Download Event protocol", form["action"], base_url)

    categories: list[dict[str, Any]] = []
    current_category: dict[str, Any] | None = None
    report_table = _find_table_by_headers(soup, ["Category", "Segment", "Reports"])
    if report_table:
        for row in _data_rows(report_table):
            cells = _direct_cells(row)
            if len(cells) < 5:
                continue
            category_name = _cell_text(cells[0])
            segment_name = _cell_text(cells[1])
            if category_name:
                current_category = {
                    "name": category_name,
                    "entries": _first_link(cells[2], base_url),
                    "result": _first_link(cells[3], base_url),
                    "segments": [],
                }
                categories.append(current_category)
            elif current_category and segment_name:
                current_category["segments"].append(
                    {
                        "name": segment_name,
                        "officials": _first_link(cells[2], base_url),
                        "details": _first_link(cells[3], base_url),
                        "judges_scores_pdf": _first_link(cells[4], base_url),
                    }
                )

    schedule: list[dict[str, Any]] = []
    schedule_table = _find_table_by_headers(soup, ["Date", "Time", "Category", "Segment"])
    current_date = ""
    if schedule_table:
        for row in _data_rows(schedule_table):
            cells = _direct_cells(row)
            if len(cells) < 4:
                continue
            values = [_cell_text(cell) for cell in cells]
            if values[0] and not values[1] and not values[2] and not values[3]:
                current_date = values[0]
                continue
            if values[1] and values[2] and values[3]:
                schedule.append(
                    {
                        "date": current_date,
                        "time": values[1],
                        "category": values[2],
                        "segment": values[3],
                        "segment_details": _first_link(cells[3], base_url),
                    }
                )

    return {
        "source_profile": "fs_manager",
        "source_context": classify_source_context(base_url),
        "source_url": base_url,
        "event": {"name": event_name, "location": location, "date_range": date_range, "venue": venue},
        "event_protocol_pdf": protocol_pdf,
        "categories": categories,
        "schedule": schedule,
    }


def parse_entries(document: str, base_url: str = "") -> dict[str, Any]:
    from .source_profiles import representation_settings_for_url

    soup = BeautifulSoup(document, "html.parser")
    category = _caption(soup, "caption2")
    table = _find_table_by_headers(soup, ["No.", "Name", "Nation"])
    settings = representation_settings_for_url(base_url)
    entries = []
    if table:
        header_map = _header_map(table)
        for row in _data_rows(table):
            cells = _direct_cells(row)
            if len(cells) < 2:
                continue
            name_cell = _cell_by_header(cells, header_map, "Name")
            record = {
                "entry_no": to_int(_cell_text(_cell_by_header(cells, header_map, "No."))),
                "name": _cell_text(name_cell),
                "club": _optional_cell_text(cells, header_map, "Club"),
                "nation": _nation_code(_optional_cell_text(cells, header_map, "Nation")),
                **_bio_fields(name_cell, base_url),
            }
            entries.append(
                {
                    **record,
                    "representation_type": settings.get("primary", "nation"),
                    "representation_value": _representation_value(record, settings),
                }
            )
    return {"category": category, "entries": entries}


def parse_category_result(document: str, base_url: str = "") -> dict[str, Any]:
    from .source_profiles import representation_settings_for_url

    soup = BeautifulSoup(document, "html.parser")
    category = _caption(soup, "caption2")
    table = _find_table_by_headers(soup, ["FPl.", "Name", "Nation", "Points"])
    settings = representation_settings_for_url(base_url)
    results = []
    if table:
        headers = [_cell_text(cell) for cell in _direct_cells(_header_row(table))]
        header_map = _header_map(table)
        points_index = header_map.get("Points", 3)
        segment_headers = headers[points_index + 1 :]
        for row in _data_rows(table):
            cells = _direct_cells(row)
            if len(cells) <= points_index:
                continue
            name_cell = _cell_by_header(cells, header_map, "Name")
            record = {
                "final_place": to_int(_cell_text(_cell_by_header(cells, header_map, "FPl."))),
                "name": _cell_text(name_cell),
                "club": _optional_cell_text(cells, header_map, "Club"),
                "nation": _nation_code(_optional_cell_text(cells, header_map, "Nation")),
                "points": to_float(_cell_text(_cell_by_header(cells, header_map, "Points"))),
                "segment_places": {
                    segment_headers[idx]: to_int(_cell_text(cell))
                    for idx, cell in enumerate(cells[points_index + 1 :])
                    if idx < len(segment_headers)
                },
                **_bio_fields(name_cell, base_url),
            }
            results.append(
                {
                    **record,
                    "representation_type": settings.get("primary", "nation"),
                    "representation_value": _representation_value(record, settings),
                }
            )
    return {"category": category, "results": results}


def parse_segment_result(document: str, base_url: str = "") -> dict[str, Any]:
    from .source_profiles import representation_settings_for_url

    soup = BeautifulSoup(document, "html.parser")
    category, segment = _split_category_segment(_caption(soup, "caption2"))
    table = _find_table_by_headers(soup, ["Pl.", "Name", "Nation", "TSS"])
    settings = representation_settings_for_url(base_url)
    results = []
    if table:
        headers = [_normalize_header(_cell_text(cell)) for cell in _direct_cells(_header_row(table))]
        header_map = {header: index for index, header in enumerate(headers) if header}
        place_idx = header_map.get("Pl.")
        name_idx = header_map.get("Name")
        nation_idx = header_map.get("Nation")
        club_idx = header_map.get("Club")
        tss_idx = header_map.get("TSS")
        tes_idx = header_map.get("TES")
        pcs_idx = header_map.get("PCS")
        deduction_idx = header_map.get("Ded.")
        starting_number_idx = header_map.get("StN.")
        qualification_idx = header_map.get("Qual.")
        for row in _data_rows(table):
            cells = _direct_cells(row)
            values = [_cell_text(cell) for cell in cells]
            required_indexes = [place_idx, name_idx, nation_idx, tss_idx, tes_idx, pcs_idx, deduction_idx, starting_number_idx]
            if any(index is None or index >= len(values) for index in required_indexes):
                continue
            assert place_idx is not None
            assert name_idx is not None
            assert nation_idx is not None
            assert tss_idx is not None
            assert tes_idx is not None
            assert pcs_idx is not None
            assert deduction_idx is not None
            assert starting_number_idx is not None
            name = values[name_idx]
            if not name:
                continue
            record = {
                "place": to_int(values[place_idx]),
                "qualification": values[qualification_idx] if qualification_idx is not None and qualification_idx < len(values) else None,
                "name": name,
                "club": values[club_idx] if club_idx is not None and club_idx < len(values) else None,
                "nation": _nation_code(values[nation_idx]),
                "tss": to_float(values[tss_idx]),
                "tes": to_float(values[tes_idx]),
                "pcs": to_float(values[pcs_idx]),
                "components": {
                    headers[idx].lower(): to_float(values[idx])
                    for idx in range(pcs_idx + 1, min(deduction_idx, len(headers), len(values)))
                    if headers[idx]
                },
                "deduction": to_float(values[deduction_idx]),
                "starting_number": to_int(values[starting_number_idx]),
                **_bio_fields(cells[name_idx], base_url),
            }
            results.append(
                {
                    **record,
                    "representation_type": settings.get("primary", "nation"),
                    "representation_value": _representation_value(record, settings),
                }
            )
    return {"category": category, "segment": segment, "results": results}


def parse_officials(document: str, base_url: str = "") -> dict[str, Any]:
    soup = BeautifulSoup(document, "html.parser")
    category, segment = _split_category_segment(_caption(soup, "caption2"))
    table = _find_table_by_headers(soup, ["Function", "Name", "Nation"])
    officials = []
    if table:
        for row in _data_rows(table):
            cells = _direct_cells(row)
            if len(cells) < 3:
                continue
            function = _cell_text(cells[0])
            name = _cell_text(cells[1])
            if not function or not name:
                continue
            role = classify_official_function(function)
            officials.append(
                {
                    "function": function,
                    "role_group": role["role_group"],
                    "judge_number": role["judge_number"],
                    "name": name,
                    "nation": _nation_code(_cell_text(cells[2])),
                }
            )
    return {"category": category, "segment": segment, "officials": officials}


def classify_source_context(url: str) -> dict[str, str | None]:
    parsed = urlparse(url)
    path_parts = [part for part in parsed.path.split("/") if part]
    stream = None
    for part in path_parts:
        if part.lower() in {"isu", "nonisu"}:
            stream = part
            break
    return {
        "host": parsed.netloc or None,
        "competition_stream": stream,
        "event_path": "/".join(path_parts[:-1]) if path_parts and path_parts[-1].lower().endswith((".htm", ".html")) else "/".join(path_parts),
    }


def _find_table_by_headers(soup: BeautifulSoup, expected: list[str]) -> Tag | None:
    for row in soup.find_all("tr"):
        headers = [_normalize_header(_cell_text(cell)) for cell in _direct_cells(row)]
        if all(any(header == _normalize_header(item) for header in headers) for item in expected):
            return row.find_parent("table")
    return None


def _header_row(table: Tag) -> Tag:
    return table.find("tr")  # type: ignore[return-value]


def _header_map(table: Tag) -> dict[str, int]:
    return {_cell_text(cell): index for index, cell in enumerate(_direct_cells(_header_row(table)))}


def _data_rows(table: Tag) -> list[Tag]:
    rows = table.find_all("tr", recursive=False)
    if not rows:
        rows = table.find_all("tr")
    return [row for row in rows[1:] if _direct_cells(row)]


def _direct_cells(row: Tag) -> list[Tag]:
    return row.find_all(["td", "th"], recursive=False)


def _cell_text(cell: Tag) -> str:
    return clean_text(cell.get_text(" "))


def _cell_by_header(cells: list[Tag], header_map: dict[str, int], header: str) -> Tag:
    return cells[header_map[header]]


def _optional_cell_text(cells: list[Tag], header_map: dict[str, int], header: str) -> str:
    index = header_map.get(header)
    if index is None or index >= len(cells):
        return ""
    return _cell_text(cells[index])


def _first_link(cell: Tag, base_url: str) -> dict[str, str] | None:
    link = cell.find("a")
    if not link or not link.get("href"):
        return None
    return _link_dict(_cell_text(link), link["href"], base_url)


def _link_dict(text: str, href: str, base_url: str) -> dict[str, str]:
    return {"text": clean_text(text), "href": href, "url": urljoin(base_url, href)}


def _bio_fields(cell: Tag, base_url: str) -> dict[str, str | None]:
    link = cell.find("a")
    href = link.get("href") if link else None
    source_skater_id = None
    if href:
        match = _BIO_ID_RE.search(href)
        source_skater_id = match.group(1) if match else None
    return {
        "bio_url": urljoin(base_url, href) if href else None,
        "source_skater_id": source_skater_id,
    }


def _caption(soup: BeautifulSoup, class_name: str) -> str:
    node = soup.select_one(f"tr.{class_name} td")
    return clean_text(node.get_text(" ")) if node else ""


def _normalize_header(value: str) -> str:
    return clean_text(value).replace(" =", "").replace(" +", "").replace(" -", "").replace("-", "").strip()


def _nation_code(value: str | None) -> str | None:
    if not value:
        return None
    text = clean_text(value)
    if re.fullmatch(r"[A-Z]{3}", text):
        return text
    match = re.search(r"\b([A-Z]{3})\b$", text)
    return match.group(1) if match else None


def _split_category_segment(value: str) -> tuple[str, str]:
    parts = [part.strip() for part in clean_text(value).split(" - ", 1)]
    if len(parts) == 2:
        return parts[0], parts[1]
    return value, ""


def _representation_value(record: dict[str, Any], settings: dict[str, Any]) -> str | None:
    primary = settings.get("primary", "nation")
    if primary == "club":
        return record.get("club") or None
    return record.get("nation") or record.get("club")
