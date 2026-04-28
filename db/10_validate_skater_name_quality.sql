-- Detect parser corruption in skater appearance names.
-- Example fixed case: FS Manager segment result tables may include a `Qual.`
-- column before `Name`; the `Q` marker must be stored as qualification, not as
-- the skater name.

\echo 'Skater appearance name quality issues in latest completed import of each URL'

WITH latest AS (
  SELECT DISTINCT ON (root_url)
    id,
    root_url
  FROM ingest.import_runs
  WHERE status = 'completed'
  ORDER BY root_url, id DESC
)
SELECT
  l.id AS import_run_id,
  l.root_url,
  c.name AS category_name,
  s.name AS segment_name,
  ssa.appearance_type,
  ssa.source_row_order,
  ssa.raw_name,
  ssa.normalized_name,
  ssa.nation_code,
  ssa.raw->>'qualification' AS qualification
FROM latest l
JOIN ingest.source_skater_appearances ssa ON ssa.import_run_id = l.id
LEFT JOIN ingest.categories c ON c.id = ssa.category_id
LEFT JOIN ingest.segments s ON s.id = ssa.segment_id
WHERE coalesce(nullif(trim(ssa.raw_name), ''), '') = ''
   OR coalesce(nullif(trim(ssa.normalized_name), ''), '') = ''
   OR ssa.raw_name = 'Q'
   OR ssa.normalized_name = 'Q'
ORDER BY l.id, c.name, s.name, ssa.appearance_type, ssa.source_row_order;
