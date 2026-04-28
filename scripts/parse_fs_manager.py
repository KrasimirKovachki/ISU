#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from isu_parser.fs_manager import parse_index
from isu_parser.old_isucalcfs import validate_event_index


def fetch(url: str) -> str:
    request = Request(url, headers={"User-Agent": "isu-skating-data-parser/0.1"})
    with urlopen(request, timeout=30) as response:
        encoding = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(encoding, errors="replace")


def main() -> int:
    parser = argparse.ArgumentParser(description="Parse a Swiss Timing FS Manager event index page.")
    parser.add_argument("url", help="FS Manager index URL.")
    parser.add_argument("--validate", action="store_true", help="Add validation issues to the output.")
    args = parser.parse_args()

    parsed = parse_index(fetch(args.url), args.url)
    if args.validate:
        parsed["validation_issues"] = validate_event_index(parsed)
    print(json.dumps(parsed, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
