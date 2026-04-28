from __future__ import annotations

from dataclasses import asdict, dataclass, field
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin
import html
import re


_SPACE_RE = re.compile(r"\s+")
_DATE_SUFFIX_RE = re.compile(r"\s*[�.]+$")


def clean_text(value: str) -> str:
    value = html.unescape(value).replace("\xa0", " ")
    value = value.replace("\ufffd", " ")
    value = _SPACE_RE.sub(" ", value)
    return value.strip()


def clean_date(value: str) -> str:
    value = clean_text(value).replace("�.", "").replace("�", "")
    value = re.sub(r"(?<=\d)\s+[^\d\s-]+\.?(?=\s*(?:-|$))", "", value)
    value = re.sub(r"\s+\.\s+", " ", value)
    return _DATE_SUFFIX_RE.sub("", value).strip()


def to_int(value: str) -> int | None:
    value = clean_text(value).lstrip("#")
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def to_float(value: str) -> float | None:
    value = clean_text(value)
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


@dataclass
class Link:
    text: str
    href: str
    absolute_url: str


@dataclass
class Cell:
    text: str
    links: list[Link] = field(default_factory=list)


@dataclass
class Table:
    caption: list[str]
    rows: list[list[Cell]]


class _TableHTMLParser(HTMLParser):
    def __init__(self, base_url: str = "") -> None:
        super().__init__(convert_charrefs=False)
        self.base_url = base_url
        self.tables: list[Table] = []
        self.headings: list[tuple[str, str]] = []
        self.title = ""
        self._in_title = False
        self._heading_tag: str | None = None
        self._heading_parts: list[str] = []
        self._table: Table | None = None
        self._in_caption = False
        self._caption_heading_tag: str | None = None
        self._caption_parts: list[str] = []
        self._row: list[Cell] | None = None
        self._cell_parts: list[str] | None = None
        self._cell_links: list[Link] = []
        self._link_href: str | None = None
        self._link_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_dict = {name.lower(): value or "" for name, value in attrs}
        if tag == "title":
            self._in_title = True
            return
        if tag in {"h1", "h2", "h3", "h4"}:
            if self._in_caption:
                self._caption_heading_tag = tag
                self._caption_parts = []
            else:
                self._heading_tag = tag
                self._heading_parts = []
            return
        if tag == "table":
            self._table = Table(caption=[], rows=[])
            return
        if tag == "caption":
            self._in_caption = True
            return
        if tag == "tr" and self._table is not None:
            self._row = []
            return
        if tag in {"td", "th"} and self._row is not None:
            self._cell_parts = []
            self._cell_links = []
            return
        if tag == "a" and self._cell_parts is not None:
            if self._link_href is not None:
                self._append_current_link()
            self._link_href = attrs_dict.get("href", "")
            self._link_parts = []
            return
        if tag == "br" and self._cell_parts is not None:
            self._cell_parts.append(" ")

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self._in_title = False
            self.title = clean_text(self.title)
            return
        if tag in {"h1", "h2", "h3", "h4"}:
            if self._caption_heading_tag == tag and self._table is not None:
                text = clean_text("".join(self._caption_parts))
                if text:
                    self._table.caption.append(text)
                self._caption_heading_tag = None
            elif self._heading_tag == tag:
                text = clean_text("".join(self._heading_parts))
                if text:
                    self.headings.append((tag, text))
                self._heading_tag = None
            return
        if tag == "caption":
            self._in_caption = False
            return
        if tag == "table" and self._table is not None:
            self.tables.append(self._table)
            self._table = None
            return
        if tag == "tr" and self._table is not None and self._row is not None:
            if any(cell.text or cell.links for cell in self._row):
                self._table.rows.append(self._row)
            self._row = None
            return
        if tag in {"td", "th"} and self._row is not None and self._cell_parts is not None:
            if self._link_href is not None:
                self._append_current_link()
            self._row.append(Cell(clean_text("".join(self._cell_parts)), self._cell_links))
            self._cell_parts = None
            self._cell_links = []
            return
        if tag == "a" and self._cell_parts is not None and self._link_href is not None:
            self._append_current_link()

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title += data
        if self._heading_tag is not None:
            self._heading_parts.append(data)
        if self._caption_heading_tag is not None:
            self._caption_parts.append(data)
        if self._cell_parts is not None:
            self._cell_parts.append(data)
        if self._link_href is not None:
            self._link_parts.append(data)

    def handle_entityref(self, name: str) -> None:
        self.handle_data(f"&{name};")

    def handle_charref(self, name: str) -> None:
        self.handle_data(f"&#{name};")

    def _append_current_link(self) -> None:
        if self._link_href is None:
            return
        text = clean_text("".join(self._link_parts))
        href = clean_text(self._link_href)
        if text:
            self._cell_links.append(Link(text=text, href=href, absolute_url=urljoin(self.base_url, href) if href else ""))
        self._link_href = None
        self._link_parts = []


def parse_tables(document: str, base_url: str = "") -> _TableHTMLParser:
    parser = _TableHTMLParser(base_url=base_url)
    parser.feed(document)
    return parser


def _cell_link(cell: Cell, label: str | None = None) -> dict[str, str] | None:
    for link in cell.links:
        if not link.href:
            continue
        if label is None or clean_text(link.text).lower() == label.lower():
            return {"text": link.text, "href": link.href, "url": link.absolute_url}
    if cell.links:
        link = next((item for item in cell.links if item.href), None)
        if link is None:
            return None
        return {"text": link.text, "href": link.href, "url": link.absolute_url}
    return None


def _row_link_matching(row: list[Cell], text_contains: str) -> dict[str, str] | None:
    needle = text_contains.lower()
    for cell in row:
        for link in cell.links:
            if not link.href:
                continue
            if needle in clean_text(link.text).lower():
                return {"text": link.text, "href": link.href, "url": link.absolute_url}
    return None


def parse_index(document: str, base_url: str = "") -> dict[str, Any]:
    parsed = parse_tables(document, base_url)
    event_name = ""
    location = ""
    date_range = ""
    venue = ""
    for tag, text in parsed.headings:
        if tag == "h2" and not event_name:
            event_name = text
        elif tag == "h3" and not location:
            location = text
        elif tag == "h3" and not date_range:
            date_range = clean_date(text)
        elif tag == "h3" and not venue:
            venue = text

    categories: list[dict[str, Any]] = []
    current_category: dict[str, Any] | None = None
    if parsed.tables:
        for row in parsed.tables[0].rows[1:]:
            cells = [cell.text for cell in row]
            if len(cells) == 2 and cells[0]:
                current_category = {
                    "name": cells[0],
                    "entries": None,
                    "result": _cell_link(row[1]),
                    "segments": [],
                    "source_shape": "category_result_pdf_only",
                }
                categories.append(current_category)
                continue
            if len(cells) < 5:
                continue
            category_name, segment_name = cells[0], cells[1]
            if category_name:
                current_category = {
                    "name": category_name,
                    "entries": _cell_link(row[2]),
                    "result": _cell_link(row[3]),
                    "segments": [],
                }
                categories.append(current_category)
            elif current_category is not None and segment_name:
                current_category["segments"].append(
                    {
                        "name": segment_name,
                        "officials": _cell_link(row[2]),
                        "details": _cell_link(row[3]),
                        "judges_scores_pdf": _row_link_matching(row[4:], "judges scores") or _cell_link(row[4]),
                    }
                )

    schedule: list[dict[str, Any]] = []
    current_date = ""
    for table in parsed.tables[1:]:
        if "Time Schedule" not in table.caption:
            continue
        for row in table.rows[1:]:
            values = [cell.text for cell in row]
            if len(values) == 1 and values[0]:
                current_date = clean_date(values[0])
                continue
            if len(row) >= 4:
                schedule.append(
                    {
                        "date": current_date,
                        "time": row[1].text,
                        "category": row[2].text,
                        "segment": row[3].text,
                        "segment_details": _cell_link(row[3]),
                    }
                )

    return {
        "source_profile": "old_isucalcfs",
        "source_url": base_url,
        "event": {
            "name": event_name,
            "location": location,
            "date_range": date_range,
            "venue": venue,
        },
        "categories": categories,
        "schedule": schedule,
    }


def parse_entries(document: str, base_url: str = "") -> dict[str, Any]:
    parsed = parse_tables(document, base_url)
    table = parsed.tables[0]
    event, category = table.caption[:2]
    rows = table.rows[1:]
    return {
        "event_name": event,
        "category": category,
        "entries": [
            {"entry_no": to_int(row[0].text), "name": row[1].text, "nation": row[2].text}
            for row in rows
            if len(row) >= 3
        ],
    }


def parse_category_result(document: str, base_url: str = "") -> dict[str, Any]:
    parsed = parse_tables(document, base_url)
    table = parsed.tables[0]
    event, category = table.caption[:2]
    headers = [clean_text(cell.text) for cell in table.rows[0]]
    header_map = {_normalize_result_header(header): idx for idx, header in enumerate(headers) if header}
    final_place_idx = header_map.get("fpl", 0)
    name_idx = header_map.get("name", 1)
    club_idx = header_map.get("club")
    nation_idx = header_map.get("nation")
    points_idx = header_map.get("points")
    first_segment_idx = (points_idx + 1) if points_idx is not None else 4
    segment_headers = headers[first_segment_idx:]
    results = []
    for row in table.rows[1:]:
        if len(row) <= max(final_place_idx, name_idx):
            continue
        points = to_float(row[points_idx].text) if points_idx is not None and points_idx < len(row) else None
        nation = row[nation_idx].text if nation_idx is not None and nation_idx < len(row) else None
        club = row[club_idx].text if club_idx is not None and club_idx < len(row) else None
        segment_places = {
            segment_headers[idx]: to_int(cell.text)
            for idx, cell in enumerate(row[first_segment_idx:])
            if idx < len(segment_headers)
        }
        result = {
            "final_place": to_int(row[final_place_idx].text),
            "name": row[name_idx].text,
            "nation": nation,
            "points": points,
            "segment_places": segment_places,
        }
        if club_idx is not None:
            result["club"] = club
        results.append(result)
    return {"event_name": event, "category": category, "results": results}


def _cell_name_and_embedded_club(cell: Cell) -> tuple[str, str | None]:
    link = next((item for item in cell.links if item.text), None)
    if link is None:
        return cell.text, None
    name = clean_text(link.text)
    remainder = clean_text(cell.text.removeprefix(name))
    return name, remainder or None


def _normalize_result_header(value: str) -> str:
    normalized = clean_text(value).lower().replace(".", "")
    if normalized in {"nat", "nation"}:
        return "nation"
    if normalized == "fpl":
        return "fpl"
    if normalized == "pl":
        return "pl"
    if normalized == "stn":
        return "stn"
    if normalized.startswith("deduction"):
        return "deduction"
    return normalized


def parse_segment_result(document: str, base_url: str = "") -> dict[str, Any]:
    parsed = parse_tables(document, base_url)
    title_table = parsed.tables[0]
    result_table = parsed.tables[1]
    event = title_table.caption[0]
    category_segment = title_table.caption[1]
    category, segment = _split_category_segment(category_segment)
    if not result_table.rows:
        return {"event_name": event, "category": category, "segment": segment, "results": []}
    headers = [cell.text.replace(" =", "").replace(" +", "").replace(" -", "") for cell in result_table.rows[0]]
    header_map = {_normalize_result_header(header): idx for idx, header in enumerate(headers) if header}
    place_idx = header_map.get("pl", 0)
    name_idx = header_map.get("name", 1)
    club_idx = header_map.get("club")
    nation_idx = header_map.get("nation")
    tss_idx = header_map.get("tss", 3)
    tes_idx = header_map.get("tes", 4)
    pcs_idx = header_map.get("pcs", 6)
    deduction_idx = header_map.get("deduction", len(headers) - 2)
    starting_number_idx = header_map.get("stn", len(headers) - 1)
    component_start_idx = pcs_idx + 1
    component_end_idx = deduction_idx
    results = []
    for row in result_table.rows[1:]:
        values = [cell.text for cell in row]
        if len(values) <= max(place_idx, name_idx, tss_idx, tes_idx, pcs_idx, deduction_idx, starting_number_idx):
            continue
        name, embedded_club = _cell_name_and_embedded_club(row[name_idx])
        club = row[club_idx].text if club_idx is not None and club_idx < len(row) else embedded_club
        results.append(
            {
                "place": to_int(values[place_idx]),
                "name": name,
                "club": club,
                "club_display_name": embedded_club,
                "nation": values[nation_idx] if nation_idx is not None and nation_idx < len(values) else None,
                "tss": to_float(values[tss_idx]),
                "tes": to_float(values[tes_idx]),
                "pcs": to_float(values[pcs_idx]),
                "components": {
                    headers[idx].lower(): to_float(values[idx])
                    for idx in range(component_start_idx, min(component_end_idx, len(headers), len(values)))
                    if headers[idx]
                },
                "deduction": to_float(values[deduction_idx]),
                "starting_number": to_int(values[starting_number_idx]),
            }
        )
    return {"event_name": event, "category": category, "segment": segment, "results": results}


def parse_officials(document: str, base_url: str = "") -> dict[str, Any]:
    parsed = parse_tables(document, base_url)
    table = parsed.tables[0]
    event = table.caption[0]
    category, segment = _split_category_segment(table.caption[1])
    officials = []
    for row in table.rows[1:]:
        if len(row) < 3 or not row[0].text:
            continue
        role = classify_official_function(row[0].text)
        officials.append(
            {
                "function": row[0].text,
                "role_group": role["role_group"],
                "judge_number": role["judge_number"],
                "name": row[1].text,
                "nation": row[2].text,
            }
        )
    return {"event_name": event, "category": category, "segment": segment, "officials": officials}


def classify_official_function(function: str) -> dict[str, Any]:
    normalized = clean_text(function).lower()
    judge_match = re.match(r"judge\s+no\.?\s*(\d+)", normalized)
    if judge_match:
        return {"role_group": "judge", "judge_number": int(judge_match.group(1))}
    if normalized == "referee":
        return {"role_group": "referee", "judge_number": None}
    if normalized in {"technical controller", "technical specialist", "assistant technical specialist"}:
        return {"role_group": "technical_panel", "judge_number": None}
    if normalized in {"data operator", "replay operator"}:
        return {"role_group": "event_operations", "judge_number": None}
    return {"role_group": "other", "judge_number": None}


def validate_event_index(event_index: dict[str, Any]) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    event = event_index.get("event", {})
    for field_name in ["name", "location", "date_range", "venue"]:
        if not event.get(field_name):
            issues.append({"level": "error", "code": f"missing_event_{field_name}", "message": f"Missing event {field_name}."})

    categories = event_index.get("categories", [])
    if not categories:
        issues.append({"level": "error", "code": "missing_categories", "message": "No categories found."})

    seen_links: set[str] = set()
    for category in categories:
        if not category.get("name"):
            issues.append({"level": "error", "code": "missing_category_name", "message": "Category without name found."})
        for link_field in ["entries", "result"]:
            link = category.get(link_field)
            if not link or not link.get("href"):
                level = "warning" if category.get("source_shape") == "category_result_pdf_only" else "error"
                issues.append({"level": level, "code": f"missing_category_{link_field}", "message": f"{category.get('name', '<unknown>')} missing {link_field} link."})
            else:
                seen_links.add(link["href"])
        if not category.get("segments"):
            issues.append({"level": "warning", "code": "category_without_segments", "message": f"{category.get('name', '<unknown>')} has no segments."})
        for segment in category.get("segments", []):
            for link_field in ["officials", "details", "judges_scores_pdf"]:
                link = segment.get(link_field)
                if not link or not link.get("href"):
                    issues.append({"level": "warning", "code": f"missing_segment_{link_field}", "message": f"{category.get('name')} / {segment.get('name')} missing {link_field} link."})
                else:
                    if link["href"] in seen_links:
                        issues.append({"level": "warning", "code": "duplicate_link", "message": f"Duplicate report link {link['href']}."})
                    seen_links.add(link["href"])

    schedule = event_index.get("schedule", [])
    if not schedule:
        issues.append({"level": "warning", "code": "missing_schedule", "message": "No time schedule found."})
    known_pairs = {(category["name"], segment["name"]) for category in categories for segment in category.get("segments", [])}
    for item in schedule:
        if (item.get("category"), item.get("segment")) not in known_pairs:
            issues.append(
                {
                    "level": "warning",
                    "code": "schedule_segment_not_in_reports",
                    "message": f"Schedule item not listed in report table: {item.get('category')} / {item.get('segment')}.",
                }
            )
    return issues


def _split_category_segment(value: str) -> tuple[str, str]:
    parts = [part.strip() for part in clean_text(value).split(" - ", 1)]
    if len(parts) == 2:
        return parts[0], parts[1]
    return value, ""


def to_dict(value: Any) -> Any:
    if hasattr(value, "__dataclass_fields__"):
        return asdict(value)
    return value
