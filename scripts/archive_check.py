#!/usr/bin/env python3
"""Check that every imported source document is kept locally, byte for byte.

Created 2026-10-12 by Claude (AI assistant). Owner rule: source websites can change or disappear, so every page
and judges PDF we imported must stay in our own archive, and we must know when the source changed.

Every row of ingest.source_documents has the SHA-256 of the bytes that were imported (content_hash). A document is
"kept" when a local file with exactly those bytes exists in either store:
  data/source_cache/<aa>/<sha256(url)>.bin      (written by the importer, isu_parser/fetch_cache.py)
  data/source_archive/<host>/<path>             (written by scripts/export_source_archive.py)

Modes:
  local            report kept / changed-file / missing per event (no network)
  --recover        for missing ones, download again and keep the file ONLY if it is byte-identical to the import
  --remote N       re-download N documents (oldest checked first) and record in ingest.source_checks whether the
                   source still matches, changed or is gone; a changed source is saved beside the original
                   (data/source_versions/), never over it
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from isu_parser.fetch_cache import CACHE_ROOT, _cache_path, cached_fetch  # noqa: E402
from scripts.export_source_archive import archive_path_for_url  # noqa: E402
from skating_api.db import connect  # noqa: E402

ARCHIVE_ROOT = CACHE_ROOT.parent / "source_archive"
VERSIONS_ROOT = CACHE_ROOT.parent / "source_versions"


def sha256_file(path: Path) -> str | None:
    if not path.is_file():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def local_state(url: str, content_hash: str) -> str:
    hashes = {sha256_file(_cache_path(url)), sha256_file(archive_path_for_url(ARCHIVE_ROOT, url))} - {None}
    if content_hash in hashes:
        return "kept"
    return "changed_file" if hashes else "missing"


def ensure_checks_table(conn) -> None:
    conn.execute("""
      CREATE TABLE IF NOT EXISTS ingest.source_checks (
        url          text PRIMARY KEY,
        checked_at   timestamptz NOT NULL DEFAULT now(),
        status       text NOT NULL,          -- same | changed | gone | error
        detail       text,
        new_sha256   text
      )""")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--recover", action="store_true", help="re-download missing documents (kept only if identical)")
    ap.add_argument("--remote", type=int, default=0, metavar="N", help="re-check N documents against the source")
    ap.add_argument("--list-missing", type=int, default=20, metavar="N", help="print up to N missing URLs")
    args = ap.parse_args()

    conn = connect()
    # latest imported version of each URL (re-imports keep older rows)
    docs = conn.execute("""
      SELECT DISTINCT ON (sd.url) sd.url, sd.content_hash, coalesce(e.event_name, ir.root_url) AS event
      FROM ingest.source_documents sd
      JOIN ingest.import_runs ir ON ir.id = sd.import_run_id
      LEFT JOIN ingest.events e ON e.import_run_id = ir.id
      WHERE ir.status = 'completed' AND sd.content_hash IS NOT NULL
      ORDER BY sd.url, sd.import_run_id DESC""").fetchall()

    totals: Counter[str] = Counter()
    per_event: dict[str, Counter[str]] = defaultdict(Counter)
    missing: list[tuple[str, str]] = []
    for url, content_hash, event in docs:
        state = local_state(url, content_hash)
        totals[state] += 1
        per_event[event][state] += 1
        if state == "missing":
            missing.append((url, content_hash))

    recovered = 0
    if args.recover:
        for url, content_hash in missing:
            try:
                body = cached_fetch(url, force=True)  # writes the cache file
            except Exception as exc:  # source gone: nothing we can do but report it
                print(f"cannot recover {url}: {exc}", file=sys.stderr)
                continue
            if hashlib.sha256(body).hexdigest() == content_hash:
                recovered += 1
            else:  # the source changed since we imported it: keep it apart, not as the original
                _cache_path(url).unlink(missing_ok=True)
                dest = VERSIONS_ROOT / hashlib.sha256(url.encode()).hexdigest()[:2] / f"{hashlib.sha256(body).hexdigest()}.bin"
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(body)

    remote = Counter()
    if args.remote:
        ensure_checks_table(conn)
        order = conn.execute("""
          SELECT d.url FROM unnest(%s::text[]) AS d(url)
          LEFT JOIN ingest.source_checks c ON c.url = d.url
          ORDER BY c.checked_at NULLS FIRST LIMIT %s""", ([u for u, _, _ in docs], args.remote)).fetchall()
        wanted = {u for (u,) in order}
        for url, content_hash, _ in docs:
            if url not in wanted:
                continue
            try:
                body = cached_fetch(url, force=True)
                new = hashlib.sha256(body).hexdigest()
                status = "same" if new == content_hash else "changed"
                if status == "changed":
                    dest = VERSIONS_ROOT / hashlib.sha256(url.encode()).hexdigest()[:2] / f"{new}.bin"
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    dest.write_bytes(body)
                detail = None
            except Exception as exc:
                new, status, detail = None, ("gone" if "404" in str(exc) or "410" in str(exc) else "error"), str(exc)[:300]
            remote[status] += 1
            conn.execute("""
              INSERT INTO ingest.source_checks(url, checked_at, status, detail, new_sha256) VALUES (%s, now(), %s, %s, %s)
              ON CONFLICT (url) DO UPDATE SET checked_at = now(), status = EXCLUDED.status, detail = EXCLUDED.detail,
                                              new_sha256 = EXCLUDED.new_sha256""", (url, status, detail, new))
        conn.commit()

    worst = sorted(((c["missing"] + c["changed_file"], e) for e, c in per_event.items() if c["missing"] + c["changed_file"]),
                   reverse=True)[:15]
    print(json.dumps({
        "documents": len(docs),
        "local": dict(totals),
        "events_with_gaps": len([1 for c in per_event.values() if c["missing"] + c["changed_file"]]),
        "worst_events": [{"event": e, "not_kept": n} for n, e in worst],
        "missing_sample": [u for u, _ in missing[: args.list_missing]],
        "recovered": recovered if args.recover else None,
        "remote": dict(remote) if args.remote else None,
    }, ensure_ascii=False, indent=1))
    return 0 if not totals["missing"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
