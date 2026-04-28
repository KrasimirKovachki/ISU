-- Dedicated staging table for Ice Peak "ТЕСТ Млад Фигурист" PDFs.
-- These are beginner test results, not standard judges-detail score sheets.

CREATE TABLE IF NOT EXISTS ingest.pdf_mlad_figurist_results (
  id bigserial PRIMARY KEY,
  segment_id bigint NOT NULL REFERENCES ingest.segments(id) ON DELETE CASCADE,
  source_document_id bigint REFERENCES ingest.source_documents(id) ON DELETE SET NULL,
  source_order int,
  rank int,
  skater_name text NOT NULL,
  normalized_name text NOT NULL,
  club_name text,
  crossings text,
  judge_votes_over_75 int,
  average_percent numeric(10, 3),
  average_percent_text text,
  result_text text,
  passed boolean,
  raw jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_pdf_mlad_figurist_segment
  ON ingest.pdf_mlad_figurist_results(segment_id);

CREATE INDEX IF NOT EXISTS idx_pdf_mlad_figurist_name_club
  ON ingest.pdf_mlad_figurist_results(normalized_name, club_name);
