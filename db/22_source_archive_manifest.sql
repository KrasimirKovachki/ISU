-- Local source archive manifest.
--
-- scripts/export_source_archive.py writes data/source_archive/manifest.csv.
-- scripts/load_source_archive_manifest.py loads that file here so analytics/API
-- views can link source facts back to local archived HTML/PDF files.

CREATE SCHEMA IF NOT EXISTS ingest;

CREATE TABLE IF NOT EXISTS ingest.source_archive_files (
  source_document_id bigint PRIMARY KEY REFERENCES ingest.source_documents(id) ON DELETE CASCADE,
  import_run_id bigint NOT NULL REFERENCES ingest.import_runs(id) ON DELETE CASCADE,
  root_url text NOT NULL,
  url text NOT NULL,
  document_type text,
  parser_profile text,
  content_hash text,
  archive_path text,
  archive_sha256 text,
  archive_status text NOT NULL,
  archive_reason text,
  parse_status text,
  fetched_at timestamptz,
  loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_source_archive_files_url
  ON ingest.source_archive_files(url);

CREATE INDEX IF NOT EXISTS idx_source_archive_files_status
  ON ingest.source_archive_files(archive_status, document_type);
