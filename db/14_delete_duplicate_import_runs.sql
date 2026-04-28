-- Delete older duplicate completed imports for the same exact root_url.
--
-- Keeps the latest completed import_run.id per root_url and deletes older runs.
-- Child ingest data is removed by FK cascade from ingest.import_runs.

\echo 'Older duplicate runs that will be deleted'
WITH ranked AS (
  SELECT
    id,
    root_url,
    row_number() OVER (PARTITION BY root_url ORDER BY id DESC) AS rn
  FROM ingest.import_runs
  WHERE status = 'completed'
)
SELECT
  r.root_url,
  array_agg(r.id ORDER BY r.id) FILTER (WHERE r.rn > 1) AS delete_import_run_ids,
  max(r.id) FILTER (WHERE r.rn = 1) AS keep_import_run_id
FROM ranked r
GROUP BY r.root_url
HAVING count(*) FILTER (WHERE r.rn > 1) > 0
ORDER BY r.root_url;

BEGIN;

CREATE TEMP TABLE duplicate_import_runs_to_delete ON COMMIT DROP AS
WITH ranked AS (
  SELECT
    id,
    root_url,
    row_number() OVER (PARTITION BY root_url ORDER BY id DESC) AS rn
  FROM ingest.import_runs
  WHERE status = 'completed'
)
SELECT id, root_url
FROM ranked
WHERE rn > 1;

CREATE TEMP TABLE duplicate_events_to_delete ON COMMIT DROP AS
SELECT id
FROM ingest.events
WHERE import_run_id IN (SELECT id FROM duplicate_import_runs_to_delete);

CREATE TEMP TABLE duplicate_categories_to_delete ON COMMIT DROP AS
SELECT id
FROM ingest.categories
WHERE event_id IN (SELECT id FROM duplicate_events_to_delete);

CREATE TEMP TABLE duplicate_segments_to_delete ON COMMIT DROP AS
SELECT id
FROM ingest.segments
WHERE category_id IN (SELECT id FROM duplicate_categories_to_delete);

CREATE TEMP TABLE duplicate_documents_to_delete ON COMMIT DROP AS
SELECT id
FROM ingest.source_documents
WHERE import_run_id IN (SELECT id FROM duplicate_import_runs_to_delete);

CREATE TEMP TABLE duplicate_appearances_to_delete ON COMMIT DROP AS
SELECT id
FROM ingest.source_skater_appearances
WHERE import_run_id IN (SELECT id FROM duplicate_import_runs_to_delete);

DELETE FROM ingest.pdf_score_summaries pss
WHERE pss.segment_id IN (SELECT id FROM duplicate_segments_to_delete)
   OR pss.source_document_id IN (SELECT id FROM duplicate_documents_to_delete)
   OR pss.skater_appearance_id IN (SELECT id FROM duplicate_appearances_to_delete);

DELETE FROM ingest.pdf_mlad_figurist_results pmfr
WHERE pmfr.segment_id IN (SELECT id FROM duplicate_segments_to_delete)
   OR pmfr.source_document_id IN (SELECT id FROM duplicate_documents_to_delete);

DELETE FROM ingest.official_assignments oa
WHERE oa.segment_id IN (SELECT id FROM duplicate_segments_to_delete)
   OR oa.source_document_id IN (SELECT id FROM duplicate_documents_to_delete);

DELETE FROM ingest.segment_results sr
WHERE sr.segment_id IN (SELECT id FROM duplicate_segments_to_delete)
   OR sr.skater_appearance_id IN (SELECT id FROM duplicate_appearances_to_delete);

DELETE FROM ingest.category_results cr
WHERE cr.category_id IN (SELECT id FROM duplicate_categories_to_delete)
   OR cr.skater_appearance_id IN (SELECT id FROM duplicate_appearances_to_delete);

DELETE FROM ingest.source_skater_appearances ssa
WHERE ssa.id IN (SELECT id FROM duplicate_appearances_to_delete);

DELETE FROM ingest.segments s
WHERE s.id IN (SELECT id FROM duplicate_segments_to_delete);

DELETE FROM ingest.categories c
WHERE c.id IN (SELECT id FROM duplicate_categories_to_delete);

DELETE FROM ingest.schedule_items si
WHERE si.event_id IN (SELECT id FROM duplicate_events_to_delete);

DELETE FROM ingest.parse_issues pi
WHERE pi.import_run_id IN (SELECT id FROM duplicate_import_runs_to_delete)
   OR pi.source_document_id IN (SELECT id FROM duplicate_documents_to_delete);

DELETE FROM ingest.source_documents sd
WHERE sd.id IN (SELECT id FROM duplicate_documents_to_delete);

DELETE FROM ingest.events e
WHERE e.id IN (SELECT id FROM duplicate_events_to_delete);

DELETE FROM ingest.import_runs ir
USING duplicate_import_runs_to_delete d
WHERE ir.id = d.id;

UPDATE ingest.source_url_registry sur
SET
  last_import_run_id = latest.id,
  status = 'imported',
  validation_status = CASE
    WHEN sur.validation_status = 'failed' THEN 'passed'
    ELSE sur.validation_status
  END,
  updated_at = now()
FROM (
  SELECT DISTINCT ON (root_url) id, root_url
  FROM ingest.import_runs
  WHERE status = 'completed'
  ORDER BY root_url, id DESC
) latest
WHERE sur.url = latest.root_url;

COMMIT;

\echo 'Remaining exact duplicate completed imports'
SELECT
  root_url,
  count(*) AS completed_runs,
  array_agg(id ORDER BY id) AS import_run_ids
FROM ingest.import_runs
WHERE status = 'completed'
GROUP BY root_url
HAVING count(*) > 1
ORDER BY root_url;
