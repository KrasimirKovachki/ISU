#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from pathlib import Path
from urllib.parse import parse_qsl, quote, urlparse
from urllib.request import Request, urlopen

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from isu_parser.local_config import database_dsn


DEFAULT_ARCHIVE_ROOT = Path("data/source_archive")
USER_AGENT = "isu-skating-source-archive/0.1"


def archive_path_for_url(root: Path, url: str) -> Path:
    parsed = urlparse(url)
    host = parsed.netloc or "_no_host"
    path = parsed.path.strip("/") or "index"
    last_part = Path(path).name
    if parsed.path.endswith("/") or "." not in last_part:
        path = f"{path.rstrip('/')}/index.html"
    if parsed.query:
        query = "_".join(f"{quote(k, safe='')}-{quote(v, safe='')}" for k, v in parse_qsl(parsed.query, keep_blank_values=True))
        path = f"{path}__query_{query}"
    return root / host / path


def ensure_parent_dir(path: Path, archive_root: Path) -> None:
    parent = path.parent
    current = archive_root
    for part in parent.relative_to(archive_root).parts:
        current = current / part
        if current.exists() and current.is_file():
            current.unlink()
        current.mkdir(exist_ok=True)


def fetch_url_bytes(url: str, timeout: int = 60) -> bytes:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=timeout) as response:
        return response.read()


def is_pdf_document(url: str, document_type: str | None) -> bool:
    return urlparse(url).path.lower().endswith(".pdf") or document_type == "judges_scores_pdf"


def export_archive(archive_root: Path, latest_only: bool, fetch_pdfs: bool) -> dict[str, int]:
    archive_root.mkdir(parents=True, exist_ok=True)
    manifest_jsonl = archive_root / "manifest.jsonl"
    manifest_csv = archive_root / "manifest.csv"

    latest_join = """
      JOIN (
        SELECT DISTINCT ON (root_url) id
        FROM ingest.import_runs
        WHERE status = 'completed'
        ORDER BY root_url, id DESC
      ) latest ON latest.id = sd.import_run_id
    """ if latest_only else ""

    query = f"""
      SELECT
        sd.id,
        sd.import_run_id,
        ir.root_url,
        sd.url,
        sd.document_type,
        sd.parser_profile,
        sd.content_hash,
        sd.fetched_at,
        sd.parse_status,
        sd.raw_text,
        sd.metadata
      FROM ingest.source_documents sd
      JOIN ingest.import_runs ir ON ir.id = sd.import_run_id
      {latest_join}
      WHERE sd.raw_text IS NOT NULL
         OR lower(sd.url) LIKE '%.pdf'
         OR sd.document_type = 'judges_scores_pdf'
      ORDER BY ir.root_url, sd.url, sd.id
    """

    rows_written = 0
    bytes_written = 0
    with psycopg.connect(database_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            records = cur.fetchall()

    with manifest_jsonl.open("w", encoding="utf-8") as jsonl_file, manifest_csv.open("w", newline="", encoding="utf-8") as csv_file:
        csv_writer = csv.DictWriter(
            csv_file,
            fieldnames=[
                "source_document_id",
                "import_run_id",
                "root_url",
                "url",
                "document_type",
                "parser_profile",
                "content_hash",
                "archive_path",
                "archive_sha256",
                "archive_status",
                "archive_reason",
                "parse_status",
                "fetched_at",
            ],
        )
        csv_writer.writeheader()

        for row in records:
            (
                source_document_id,
                import_run_id,
                root_url,
                url,
                document_type,
                parser_profile,
                content_hash,
                fetched_at,
                parse_status,
                raw_text,
                metadata,
            ) = row
            target_path = archive_path_for_url(archive_root / "files", url)
            archive_path: str | None = None
            archive_sha256: str | None = None
            archive_status = "written"
            archive_reason: str | None = None
            payload: bytes | None = None

            if raw_text is not None:
                payload = raw_text.encode("utf-8")
            elif fetch_pdfs and is_pdf_document(url, document_type):
                try:
                    payload = fetch_url_bytes(url)
                except Exception as exc:  # noqa: BLE001 - archive manifest should preserve fetch failures.
                    archive_status = "skipped"
                    archive_reason = f"pdf_fetch_failed: {exc}"
                else:
                    if not payload.startswith(b"%PDF"):
                        archive_status = "skipped"
                        archive_reason = "pdf_url_did_not_return_pdf_bytes"
                        payload = None
            else:
                archive_status = "skipped"
                archive_reason = "no_raw_text_or_pdf_fetch_disabled"

            if payload is not None:
                ensure_parent_dir(target_path, archive_root)
                target_path.write_bytes(payload)
                archive_path = str(target_path.relative_to(archive_root))
                archive_sha256 = hashlib.sha256(payload).hexdigest()
                bytes_written += len(payload)

            item = {
                "source_document_id": source_document_id,
                "import_run_id": import_run_id,
                "root_url": root_url,
                "url": url,
                "document_type": document_type,
                "parser_profile": parser_profile,
                "content_hash": content_hash,
                "archive_path": archive_path,
                "archive_sha256": archive_sha256,
                "archive_status": archive_status,
                "archive_reason": archive_reason,
                "parse_status": parse_status,
                "fetched_at": fetched_at.isoformat() if fetched_at else None,
                "metadata": metadata or {},
            }
            jsonl_file.write(json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n")
            csv_writer.writerow({key: item[key] for key in csv_writer.fieldnames or []})
            if archive_status == "written":
                rows_written += 1

    return {
        "documents_written": rows_written,
        "bytes_written": bytes_written,
        "manifest_jsonl": str(manifest_jsonl),
        "manifest_csv": str(manifest_csv),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Export stored source HTML and original PDFs into a local source archive.")
    parser.add_argument("--archive-root", default=str(DEFAULT_ARCHIVE_ROOT), help="Archive output directory.")
    parser.add_argument("--all-runs", action="store_true", help="Export all stored runs instead of only latest completed run per root URL.")
    parser.add_argument("--no-fetch-pdfs", action="store_true", help="Do not fetch PDF documents that are present in source_documents.")
    args = parser.parse_args()

    summary = export_archive(Path(args.archive_root), latest_only=not args.all_runs, fetch_pdfs=not args.no_fetch_pdfs)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
