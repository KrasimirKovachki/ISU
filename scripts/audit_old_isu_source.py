#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from isu_parser.old_isucalcfs import parse_index, parse_officials, validate_event_index


def fetch_text(url: str) -> str:
    request = Request(url, headers={"User-Agent": "isu-skating-data-parser/0.1"})
    with urlopen(request, timeout=30) as response:
        encoding = response.headers.get_content_charset() or "windows-1252"
        return response.read().decode(encoding, errors="replace")


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit an old ISUCalcFS event source graph.")
    parser.add_argument("url", help="Old ISUCalcFS index URL.")
    args = parser.parse_args()

    index = parse_index(fetch_text(args.url), args.url)
    issues = validate_event_index(index)
    officials_by_segment = []
    person_keys: set[tuple[str, str]] = set()
    role_counts: Counter[str] = Counter()
    max_judges = 0
    judge_counts: dict[str, int] = {}

    for category in index["categories"]:
        for segment in category["segments"]:
            officials_link = segment.get("officials")
            if not officials_link:
                continue
            parsed = parse_officials(fetch_text(officials_link["url"]), officials_link["url"])
            officials = parsed["officials"]
            segment_key = f"{category['name']} / {segment['name']}"
            judges = [official for official in officials if official["role_group"] == "judge"]
            judge_counts[segment_key] = len(judges)
            max_judges = max(max_judges, len(judges))
            for official in officials:
                role_counts[official["role_group"]] += 1
                person_keys.add((official["name"], official["nation"]))
            officials_by_segment.append(
                {
                    "category": category["name"],
                    "segment": segment["name"],
                    "officials_url": officials_link["url"],
                    "official_count": len(officials),
                    "judge_count": len(judges),
                    "officials": officials,
                }
            )

    pdf_links = [
        segment["judges_scores_pdf"]["url"]
        for category in index["categories"]
        for segment in category["segments"]
        if segment.get("judges_scores_pdf")
    ]
    report = {
        "event": index["event"],
        "source_url": index["source_url"],
        "category_count": len(index["categories"]),
        "segment_count": sum(len(category["segments"]) for category in index["categories"]),
        "schedule_item_count": len(index["schedule"]),
        "judges_scores_pdf_count": len(pdf_links),
        "official_assignment_count": sum(item["official_count"] for item in officials_by_segment),
        "unique_official_person_key_count": len(person_keys),
        "role_assignment_counts": dict(sorted(role_counts.items())),
        "max_judges_in_segment": max_judges,
        "judge_counts_by_segment": judge_counts,
        "validation_issues": issues,
        "pdf_links": pdf_links,
        "officials_by_segment": officials_by_segment,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
