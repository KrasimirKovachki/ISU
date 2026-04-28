-- Classify missing result/PDF data by checking whether the category has entries.
--
-- Rule:
-- - entry_count = 0 means likely empty category.
-- - entry_count > 0 means source has skaters but missing/unparseable result/PDF data.

\echo 'Missing PDF/result classification for latest completed import of each URL'

WITH latest AS (
  SELECT DISTINCT ON (root_url)
    id,
    root_url
  FROM ingest.import_runs
  WHERE status = 'completed'
  ORDER BY root_url, id DESC
),
segment_pdf_counts AS (
  SELECT
    l.id AS import_run_id,
    l.root_url,
    c.id AS category_id,
    c.name AS category_name,
    s.id AS segment_id,
    s.name AS segment_name,
    s.judges_scores_pdf_url,
    count(ps.id) AS pdf_summary_count
  FROM latest l
  JOIN ingest.events e ON e.import_run_id = l.id
  JOIN ingest.categories c ON c.event_id = e.id
  JOIN ingest.segments s ON s.category_id = c.id
  LEFT JOIN ingest.pdf_score_summaries ps ON ps.segment_id = s.id
  GROUP BY l.id, l.root_url, c.id, c.name, s.id, s.name, s.judges_scores_pdf_url
),
entry_counts AS (
  SELECT
    category_id,
    count(*) FILTER (WHERE appearance_type = 'entry') AS entry_count
  FROM ingest.source_skater_appearances
  GROUP BY category_id
)
SELECT
  spc.import_run_id,
  spc.root_url,
  spc.category_name,
  spc.segment_name,
  coalesce(ec.entry_count, 0) AS entry_count,
  spc.pdf_summary_count,
  CASE
    WHEN coalesce(ec.entry_count, 0) = 0 THEN 'empty_category'
    ELSE 'missing_results_with_entries'
  END AS classification,
  spc.judges_scores_pdf_url
FROM segment_pdf_counts spc
LEFT JOIN entry_counts ec ON ec.category_id = spc.category_id
WHERE spc.pdf_summary_count = 0
ORDER BY spc.import_run_id, spc.category_name, spc.segment_name;
