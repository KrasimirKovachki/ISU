#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from isu_parser.pdf_scores import parse_judges_scores_pdf, validate_judges_scores


def fetch_pdf(url: str) -> Path:
    request = Request(url, headers={"User-Agent": "isu-skating-data-parser/0.1"})
    with urlopen(request, timeout=30) as response:
        data = response.read()
    output = Path(tempfile.gettempdir()) / Path(url).name
    output.write_bytes(data)
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description="Parse an old ISUCalcFS judges-score PDF.")
    parser.add_argument("source", help="Local PDF path or URL.")
    parser.add_argument("--validate", action="store_true", help="Add validation issues to the output.")
    args = parser.parse_args()

    source = fetch_pdf(args.source) if args.source.startswith(("http://", "https://")) else Path(args.source)
    parsed = parse_judges_scores_pdf(source)
    if args.validate:
        parsed["validation_issues"] = validate_judges_scores(parsed)
    print(json.dumps(parsed, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
