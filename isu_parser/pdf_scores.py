from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import shutil
import subprocess
from typing import Any


_NUMBER_RE = re.compile(r"^-?\d+(?:\.\d+)?$")
_STARTING_NUMBER_RE = re.compile(r"^\d+$")
_SKATER_SUMMARY_RE = re.compile(
    r"^(?P<name>.+?)\s+(?P<nation>[A-Z]{3})\s+"
    r"(?P<tss>-?\d+(?:\.\d+)?)\s+"
    r"(?P<deductions>-?\d+(?:\.\d+)?)\s+"
    r"(?P<pcs>-?\d+(?:\.\d+)?)\s+"
    r"(?P<tes>-?\d+(?:\.\d+)?)\s+"
    r"(?P<rank>\d+)\s+(?P<starting_number>\d+)$"
)


class PDFTextExtractionError(RuntimeError):
    """Raised when no supported PDF text extraction backend is available."""


@dataclass
class PDFTextPage:
    page_number: int
    text: str


def extract_pdf_text(path: str | Path) -> list[PDFTextPage]:
    """Extract PDF text using an installed backend.

    The project should parse ISU judges-score PDFs as source data. This adapter
    intentionally keeps PDF extraction separate from parsing because production
    may use a stronger backend such as pypdf, PyPDF2, or poppler pdftotext.
    """

    pdf_path = Path(path)
    try:
        from pypdf import PdfReader  # type: ignore

        reader = PdfReader(str(pdf_path))
        return [PDFTextPage(index + 1, page.extract_text() or "") for index, page in enumerate(reader.pages)]
    except ModuleNotFoundError:
        pass

    try:
        from PyPDF2 import PdfReader  # type: ignore

        reader = PdfReader(str(pdf_path))
        return [PDFTextPage(index + 1, page.extract_text() or "") for index, page in enumerate(reader.pages)]
    except ModuleNotFoundError:
        pass

    if shutil.which("pdftotext"):
        output = subprocess.run(
            ["pdftotext", "-layout", str(pdf_path), "-"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        pages = output.split("\f")
        return [PDFTextPage(index + 1, page.strip()) for index, page in enumerate(pages) if page.strip()]

    raise PDFTextExtractionError("Install pypdf, PyPDF2, or poppler pdftotext to extract judges-score PDF text.")


def parse_judges_scores_pdf(path: str | Path) -> dict[str, Any]:
    pages = extract_pdf_text(path)
    parsed = parse_judges_scores_text("\n".join(page.text for page in pages))
    mlad_figurist = parse_mlad_figurist_pdf(path)
    if mlad_figurist:
        return mlad_figurist
    table_skaters = _parse_pdfplumber_skater_tables(path)
    if table_skaters:
        parsed["skaters"] = table_skaters
    return parsed


def parse_mlad_figurist_pdf(path: str | Path) -> dict[str, Any] | None:
    try:
        import pdfplumber  # type: ignore
    except ModuleNotFoundError:
        return None

    text_parts: list[str] = []
    tables: list[list[list[str | None]]] = []
    with pdfplumber.open(str(path)) as pdf:
        for page in pdf.pages:
            text_parts.append(page.extract_text() or "")
            tables.extend(page.extract_tables())

    if "ТЕСТ Млад Фигурист" not in "\n".join(text_parts):
        return None

    results: list[dict[str, Any]] = []
    for table in tables:
        for row in table:
            if len(row) < 7 or not (row[0] or "").strip().isdigit():
                continue
            average_percent_text = _clean_line(row[5] or "")
            result_text = _clean_line(row[6] or "")
            results.append(
                {
                    "rank": int((row[0] or "0").strip()),
                    "name": _clean_line(row[1] or ""),
                    "club": _clean_line(row[2] or ""),
                    "crossings": _clean_line(row[3] or ""),
                    "judge_votes_over_75": _to_int(row[4]),
                    "average_percent": _percent_to_float(average_percent_text),
                    "average_percent_text": average_percent_text,
                    "result": result_text,
                    "passed": result_text.startswith("Покрил"),
                    "raw": row,
                }
            )

    return {
        "event_name": "Айс Пик Трофи 18 - 19.04.2026 г.",
        "category": "ТЕСТ Млад Фигурист",
        "segment": "Краен резултат",
        "report_type": "MLAD_FIGURIST_TEST_RESULT",
        "skaters": [],
        "test_results": results,
        "printed_at": "19 април 2026 г.",
    }


def parse_judges_scores_text(text: str) -> dict[str, Any]:
    """Parse text extracted from an ISUCalcFS judges-score PDF.

    This captures stable top-level data from the old Crystal Reports PDF:
    event/category/segment, per-skater score summaries, and printed timestamp.
    Element-level parsing should be added after more PDFs are sampled because
    line wrapping differs between singles, pairs, and dance.
    """

    lines = [_clean_line(line) for line in text.splitlines()]
    lines = [line for line in lines if line]
    title = _first_non_empty(lines)
    category = ""
    segment = ""
    report_type = ""
    skaters: list[dict[str, Any]] = []
    printed_at = ""

    compact_text = "\n".join(lines)
    if _looks_like_fs_manager_pdf(compact_text):
        return _parse_fs_manager_judges_scores_text(compact_text)

    for line in lines:
        if "JUDGES DETAILS PER SKATER" in line:
            prefix = line.replace("JUDGES DETAILS PER SKATER", "").strip()
            category, segment = _split_category_segment(prefix)
            report_type = "JUDGES DETAILS PER SKATER"
            break

    for line in lines:
        match = _SKATER_SUMMARY_RE.match(line)
        if not match:
            continue
        skaters.append(
            {
                "name": _clean_line(match.group("name")),
                "nation": match.group("nation"),
                "total_segment_score": float(match.group("tss")),
                "total_deductions": float(match.group("deductions")),
                "total_program_component_score": float(match.group("pcs")),
                "total_element_score": float(match.group("tes")),
                "starting_number": int(match.group("starting_number")),
                "rank": int(match.group("rank")),
            }
        )

    if not skaters:
        skaters = _parse_legacy_multiline_summaries(lines)

    for line in lines:
        if line.lower().startswith("printed:"):
            printed_at = line.split(":", 1)[1].strip()

    return {
        "event_name": title,
        "category": category,
        "segment": segment,
        "report_type": report_type,
        "skaters": skaters,
        "printed_at": printed_at,
    }


def validate_judges_scores(parsed: dict[str, Any]) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    if parsed.get("report_type") == "MLAD_FIGURIST_TEST_RESULT":
        if not parsed.get("test_results"):
            return [{"level": "error", "code": "missing_mlad_figurist_results", "message": "No Mlad figurist test rows parsed."}]
        return []
    for field_name in ["category", "segment", "report_type"]:
        if not parsed.get(field_name):
            issues.append({"level": "error", "code": f"missing_pdf_{field_name}", "message": f"Missing PDF {field_name}."})
    if not parsed.get("skaters"):
        issues.append({"level": "error", "code": "missing_pdf_skaters", "message": "No skater score summaries parsed from PDF text."})
    for skater in parsed.get("skaters", []):
        combined_score = skater["total_element_score"] + skater["total_program_component_score"] - skater["total_deductions"]
        averaged_score = ((skater["total_element_score"] + skater["total_program_component_score"]) / 2) - skater["total_deductions"]
        if (
            abs(combined_score - skater["total_segment_score"]) > 0.02
            and abs(averaged_score - skater["total_segment_score"]) > 0.02
        ):
            issues.append(
                {
                    "level": "warning",
                    "code": "pdf_score_arithmetic_mismatch",
                    "message": f"Score arithmetic mismatch for {skater.get('name')}.",
                }
            )
    return issues


def _parse_pdfplumber_skater_tables(path: str | Path) -> list[dict[str, Any]]:
    try:
        import pdfplumber  # type: ignore
    except ModuleNotFoundError:
        return []

    skaters: list[dict[str, Any]] = []
    with pdfplumber.open(str(path)) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables():
                skater = _parse_pdfplumber_skater_table(table)
                if skater:
                    skaters.append(skater)
    return skaters


def _parse_pdfplumber_skater_table(table: list[list[str | None]]) -> dict[str, Any] | None:
    if len(table) < 2 or not table[0] or not table[1]:
        return None
    summary = _parse_pdfplumber_summary(table[0][0] or "")
    if not summary:
        return None
    details = _parse_pdfplumber_details(table[1][0] or "")
    deductions_detail = _parse_pdfplumber_deductions(table[2][0] if len(table) > 2 and table[2] else "")
    return {**summary, **details, "deductions_detail": deductions_detail}


def _parse_pdfplumber_summary(value: str) -> dict[str, Any] | None:
    lines = [_clean_line(line) for line in value.splitlines() if _clean_line(line)]
    if not lines:
        return None
    return _parse_fs_manager_summary(lines[-1])


def _parse_pdfplumber_details(value: str) -> dict[str, Any]:
    lines = [_clean_line(line) for line in value.splitlines() if _clean_line(line)]
    elements: list[dict[str, Any]] = []
    components: list[dict[str, Any]] = []
    base_value_total = None
    element_score_total = None
    mode = "elements"
    for line in lines:
        if line.startswith("# ") or line.startswith("Value ") or line == "Program Components Factor":
            if line == "Program Components Factor":
                mode = "components"
            continue
        if line.startswith("Program Components"):
            mode = "components"
            continue
        if line.startswith("Judges Total Program Component Score"):
            continue
        if mode == "elements":
            totals = _parse_two_float_line(line)
            if totals:
                base_value_total, element_score_total = totals
                continue
            element = _parse_element_line(line)
            if element:
                elements.append(element)
        else:
            component = _parse_component_line(line)
            if component:
                components.append(component)
    return {
        "elements": elements,
        "program_components": components,
        "base_value_total": base_value_total,
        "element_score_total": element_score_total,
        "judge_count": _infer_judge_count(elements, components),
    }


def _parse_element_line(line: str) -> dict[str, Any] | None:
    tokens = line.split()
    if len(tokens) < 6 or not tokens[0].isdigit():
        return None
    panel_score = _to_float_or_none(tokens[-1])
    if panel_score is None:
        return None
    idx = len(tokens) - 2
    bonus = None
    if idx > 0 and "." in tokens[idx] and _to_float_or_none(tokens[idx]) is not None and (re.fullmatch(r"-?\d+", tokens[idx - 1]) or tokens[idx - 1] == "-"):
        bonus = float(tokens[idx])
        idx -= 1
    judge_scores: list[int | None] = []
    while idx >= 0 and (re.fullmatch(r"-?\d+", tokens[idx]) or tokens[idx] == "-"):
        judge_scores.insert(0, None if tokens[idx] == "-" else int(tokens[idx]))
        idx -= 1
    if idx < 2:
        return None
    goe = _to_float_or_none(tokens[idx])
    if goe is None:
        return None
    middle = tokens[1:idx]
    base_index = next((i for i, token in enumerate(middle) if _to_float_or_none(token) is not None), None)
    if base_index is None:
        return None
    base_value = float(middle[base_index])
    element_code = middle[0]
    info = " ".join(middle[1:base_index] + middle[base_index + 1 :]) or None
    markers = _extract_element_markers(element_code, info)
    return {
        "element_no": int(tokens[0]),
        "element_code": element_code,
        "base_element_code": _base_element_code(element_code),
        "raw_element": " ".join(middle),
        "info": info,
        "markers": markers,
        "base_value": base_value,
        "goe": goe,
        "judge_scores": judge_scores,
        "bonus": bonus,
        "panel_score": panel_score,
    }


def _parse_component_line(line: str) -> dict[str, Any] | None:
    tokens = line.split()
    factor_index = next((i for i, token in enumerate(tokens) if _to_float_or_none(token) is not None), None)
    if factor_index is None or factor_index == 0 or len(tokens) - factor_index < 3:
        return None
    numbers = [_to_float_or_none(token) for token in tokens[factor_index:]]
    if any(number is None for number in numbers):
        return None
    numeric = [number for number in numbers if number is not None]
    return {
        "component": " ".join(tokens[:factor_index]),
        "factor": numeric[0],
        "judge_scores": numeric[1:-1],
        "score": numeric[-1],
    }


def _parse_two_float_line(line: str) -> tuple[float, float] | None:
    tokens = line.split()
    if len(tokens) != 2:
        return None
    first = _to_float_or_none(tokens[0])
    second = _to_float_or_none(tokens[1])
    if first is None or second is None:
        return None
    return first, second


def _parse_pdfplumber_deductions(value: str | None) -> dict[str, Any]:
    text = _clean_line(value or "")
    if not text.startswith("Deductions:"):
        return {"raw": text, "total": None}
    body = _clean_line(text.split(":", 1)[1])
    amounts = [float(token) for token in re.findall(r"-\d+\.\d{2}", body)]
    return {"raw": body, "total": abs(amounts[-1]) if amounts else 0.0}


def _infer_judge_count(elements: list[dict[str, Any]], components: list[dict[str, Any]]) -> int:
    counts = [len(element.get("judge_scores", [])) for element in elements]
    counts.extend(len(component.get("judge_scores", [])) for component in components)
    return max(counts) if counts else 0


def _extract_element_markers(element_code: str, info: str | None) -> list[str]:
    markers: list[str] = []
    for token in _marker_tokens_from_code(element_code):
        _append_unique(markers, token)
    if info:
        for token in re.split(r"[\s|]+", info):
            if token in {"F", "x", "!", "<", "<<", "q", "e", "*", "b", "REP"}:
                _append_unique(markers, token)
    return markers


def _marker_tokens_from_code(element_code: str) -> list[str]:
    markers: list[str] = []
    for part in element_code.split("+"):
        if "!" in part:
            _append_unique(markers, "!")
        if "<<" in part:
            _append_unique(markers, "<<")
        elif "<" in part:
            _append_unique(markers, "<")
        if part.endswith("q"):
            _append_unique(markers, "q")
        if part.endswith("e"):
            _append_unique(markers, "e")
        if part.endswith("b"):
            _append_unique(markers, "b")
        if "*" in part:
            _append_unique(markers, "*")
    return markers


def _base_element_code(element_code: str) -> str:
    parts = []
    for part in element_code.split("+"):
        if part == "COMBO":
            parts.append(part)
            continue
        clean = part.replace("<<", "").replace("<", "").replace("!", "").replace("*", "")
        clean = re.sub(r"(?<=[A-Za-z0-9])(q|e|b)$", "", clean)
        parts.append(clean)
    return "+".join(parts)


def _append_unique(values: list[str], value: str) -> None:
    if value not in values:
        values.append(value)


def _clean_line(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split()).strip()


def _first_non_empty(lines: list[str]) -> str:
    return next((line for line in lines if line), "")


def _split_category_segment(value: str) -> tuple[str, str]:
    for segment in [
        "PATTERN DANCE 1 (WITHOUT KEY POINTS)",
        "PATTERN DANCE 2 (WITHOUT KEY POINTS)",
        "SHORT PROGRAM",
        "FREE SKATING",
        "RHYTHM DANCE",
        "FREE DANCE",
        "PATTERN DANCE",
    ]:
        suffix = f" {segment}"
        if value.upper().endswith(suffix):
            return value[: -len(suffix)].strip(), segment
    parts = [part.strip() for part in value.split("  ") if part.strip()]
    if len(parts) >= 2:
        return parts[0], parts[1]
    parts = [part.strip() for part in value.rsplit(" ", 2)]
    if len(parts) >= 2:
        return value, ""
    return value, ""


def _looks_like_name(value: str) -> bool:
    if not value or _NUMBER_RE.match(value):
        return False
    if any(token in value for token in ["Program Components", "Deductions", "Executed Elements", "Starting"]):
        return False
    letters = [char for char in value if char.isalpha()]
    return len(letters) >= 5 and any(char.islower() for char in value)


def _looks_like_nation(value: str) -> bool:
    return len(value) == 3 and value.isalpha() and value.isupper()


def _parse_legacy_multiline_summaries(lines: list[str]) -> list[dict[str, Any]]:
    skaters: list[dict[str, Any]] = []
    for index, line in enumerate(lines):
        if not _looks_like_name(line):
            continue
        if index + 7 >= len(lines):
            continue
        nation = lines[index + 1]
        score_line = lines[index + 2 : index + 6]
        rank = lines[index + 6]
        starting_number = lines[index + 7]
        if not _looks_like_nation(nation):
            continue
        numbers = [float(value) for value in score_line if _NUMBER_RE.match(value)]
        if len(numbers) < 4 or not _STARTING_NUMBER_RE.match(starting_number) or not _STARTING_NUMBER_RE.match(rank):
            continue
        skaters.append(
            {
                "name": line,
                "nation": nation,
                "total_segment_score": numbers[0],
                "total_deductions": numbers[1],
                "total_program_component_score": numbers[2],
                "total_element_score": numbers[3],
                "starting_number": int(starting_number),
                "rank": int(rank),
            }
        )
    return skaters


def _looks_like_fs_manager_pdf(text: str) -> bool:
    return "RankName NationStartingNumberTotalSegmentScore" in text


def _parse_fs_manager_judges_scores_text(text: str) -> dict[str, Any]:
    first_line = _clean_line(text.splitlines()[0]) if text.splitlines() else ""
    report_marker = "JUDGES DETAILS PER SKATER"
    event_name = first_line
    category = ""
    segment = ""
    if report_marker in first_line:
        event_name, category_segment = first_line.split(report_marker, 1)
        event_name = _clean_line(event_name)
        category, segment = _split_category_segment(_clean_line(category_segment))

    printed_at = ""
    printed_match = re.search(r"printed:\s*([0-9.:\s]+)", text)
    if printed_match:
        printed_at = _clean_line(printed_match.group(1))

    skaters = []
    block_pattern = re.compile(r"RankName NationStartingNumber.*?Total\s*Deductions(?P<summary>.*?)#Executed Elements", re.DOTALL)
    for match in block_pattern.finditer(text):
        summary = _clean_line(match.group("summary"))
        parsed = _parse_fs_manager_summary(summary)
        if parsed:
            skaters.append(parsed)

    return {
        "event_name": event_name,
        "category": category,
        "segment": segment,
        "report_type": "JUDGES DETAILS PER SKATER",
        "skaters": skaters,
        "printed_at": printed_at,
    }


def _parse_fs_manager_summary(summary: str) -> dict[str, Any] | None:
    match = re.match(
        r"(?P<rank>\d+)(?P<name>.*?)(?P<nation>[A-Z]{3})\s+(?P<starting_number>\d+)\s+(?P<scores>.+)$",
        summary,
    )
    if not match:
        return None
    scores = _parse_fs_manager_score_chunk(match.group("scores"))
    if not scores:
        return None
    return {
        "name": _clean_line(match.group("name")),
        "nation": match.group("nation"),
        "total_segment_score": scores["tss"],
        "total_deductions": scores["deductions"],
        "total_program_component_score": scores["pcs"],
        "total_element_score": scores["tes"],
        "starting_number": int(match.group("starting_number")),
        "rank": int(match.group("rank")),
    }


def _parse_fs_manager_score_chunk(value: str) -> dict[str, float] | None:
    parts = value.split()
    compact = "".join(parts)
    candidates = _split_compact_scores(compact, 4)
    for tss, tes, pcs, deductions in candidates:
        positive_deductions = abs(deductions)
        if abs((tes + pcs - positive_deductions) - tss) <= 0.02:
            return {"tss": tss, "tes": tes, "pcs": pcs, "deductions": positive_deductions}
        if abs((tes + pcs + deductions) - tss) <= 0.02:
            return {"tss": tss, "tes": tes, "pcs": pcs, "deductions": positive_deductions}
        if abs(((tes + pcs) / 2 - positive_deductions) - tss) <= 0.02:
            return {"tss": tss, "tes": tes, "pcs": pcs, "deductions": positive_deductions}
        if abs(((tes + pcs) / 2 + deductions) - tss) <= 0.02:
            return {"tss": tss, "tes": tes, "pcs": pcs, "deductions": positive_deductions}
    artistic_candidates = _split_compact_scores(compact, 3)
    for tss, pcs, deductions in artistic_candidates:
        positive_deductions = abs(deductions)
        if abs((pcs - positive_deductions) - tss) <= 0.02:
            return {"tss": tss, "tes": 0.0, "pcs": pcs, "deductions": positive_deductions}
        if abs((pcs + deductions) - tss) <= 0.02:
            return {"tss": tss, "tes": 0.0, "pcs": pcs, "deductions": positive_deductions}
    return None


def _to_float_or_none(value: str) -> float | None:
    try:
        return float(value)
    except ValueError:
        return None


def _to_int(value: str | None) -> int | None:
    if value is None:
        return None
    cleaned = _clean_line(value)
    return int(cleaned) if cleaned.isdigit() else None


def _percent_to_float(value: str | None) -> float | None:
    if value is None:
        return None
    cleaned = _clean_line(value).replace("%", "")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _split_compact_scores(value: str, count: int) -> list[list[float]]:
    if count == 0:
        return [[]] if not value else []

    results: list[list[float]] = []
    for end in range(4, min(len(value), 8) + 1):
        token = value[:end]
        if not re.fullmatch(r"-?\d+\.\d{2}", token):
            continue
        number = float(token)
        for rest in _split_compact_scores(value[end:], count - 1):
            results.append([number, *rest])
    return results
