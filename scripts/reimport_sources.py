#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


def read_urls(path: Path) -> list[str]:
    return [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Sequentially reimport source URLs with scripts/import_event.py.")
    parser.add_argument("urls_file", help="Text file with one source root URL per line.")
    args = parser.parse_args()

    urls = read_urls(Path(args.urls_file))
    results: list[dict[str, object]] = []
    for index, url in enumerate(urls, start=1):
        print(json.dumps({"status": "started", "index": index, "total": len(urls), "url": url}), flush=True)
        completed = subprocess.run(
            [sys.executable, "scripts/import_event.py", url],
            check=False,
            text=True,
            capture_output=True,
        )
        result: dict[str, object] = {
            "url": url,
            "returncode": completed.returncode,
            "stdout": completed.stdout.strip(),
            "stderr": completed.stderr.strip(),
        }
        results.append(result)
        print(json.dumps({"status": "finished", **result}), flush=True)
        if completed.returncode != 0:
            print(json.dumps({"status": "failed", "results": results}, indent=2), file=sys.stderr)
            return completed.returncode

    print(json.dumps({"status": "completed", "results": results}, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
