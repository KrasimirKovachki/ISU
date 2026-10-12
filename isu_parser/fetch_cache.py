"""Local file cache for HTTP fetches used during import.

Importing an ISU result event downloads the index page, every category
listing, every segment listing, every officials page, and every judges
PDF — easily 50-200 requests per event. Re-imports of the same event
(after a parser fix, after schema changes, after a cleanup) re-fetch
all of those even though the bytes never change.

This module caches each URL's raw response bytes under
``data/source_cache/<aa>/<sha256>.bin`` (sharded by first two hex
chars of the SHA-256 of the URL). On a cache hit the request returns
from disk in microseconds; on a cache miss it fetches normally and
writes the body. Set ``SKATING_FETCH_NOCACHE=1`` in the environment
to bypass the cache for a fresh download.
"""

from __future__ import annotations

import hashlib
import os
import ssl
import urllib.error
from pathlib import Path
from urllib.request import Request, urlopen

CACHE_ROOT = Path(os.environ.get("SKATING_FETCH_CACHE_DIR", "data/source_cache"))
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


def _cache_path(url: str) -> Path:
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
    return CACHE_ROOT / digest[:2] / f"{digest}.bin"


def cached_fetch(url: str, *, timeout: int = 30, force: bool = False) -> bytes:
    """Return raw response bytes, reading from local cache when possible."""
    bypass = force or os.environ.get("SKATING_FETCH_NOCACHE") == "1"
    path = _cache_path(url)
    if not bypass and path.is_file():
        return path.read_bytes()
    # Edited 2026-10-12 by Claude (AI assistant): archive-only imports never touch the network
    if os.environ.get("SKATING_ARCHIVE_ONLY") == "1":
        raise urllib.error.URLError(f"not in the local archive: {url}")

    request = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(request, timeout=timeout) as response:
            body = response.read()
    except urllib.error.URLError as exc:
        if "CERTIFICATE_VERIFY_FAILED" not in str(exc.reason):
            raise
        with urlopen(
            request,
            timeout=timeout,
            context=ssl._create_unverified_context(),  # noqa: S323 - TLS fallback for old hosts.
        ) as response:
            body = response.read()

    store(url, body)
    return body


def store(url: str, body: bytes) -> None:
    """Keep these bytes as the archived copy of ``url`` (atomic write)."""
    path = _cache_path(url)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".bin.tmp")
    tmp.write_bytes(body)
    tmp.replace(path)


def cache_size() -> tuple[int, int]:
    """Return (file_count, total_bytes) for the cache directory."""
    if not CACHE_ROOT.exists():
        return 0, 0
    files = list(CACHE_ROOT.rglob("*.bin"))
    return len(files), sum(f.stat().st_size for f in files)
