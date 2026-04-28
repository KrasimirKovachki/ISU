-- Audit duplicate imports and event-level collisions.
--
-- Import runs are append-only so parser fixes can be re-run without deleting
-- older raw data. Use this script to find duplicate completed imports and to
-- identify the latest completed run that should be used for validation/export.

\echo 'Exact duplicate completed imports by root_url'
SELECT
  root_url,
  count(*) AS completed_runs,
  max(id) AS latest_import_run_id,
  array_agg(id ORDER BY id) AS import_run_ids
FROM ingest.import_runs
WHERE status = 'completed'
GROUP BY root_url
HAVING count(*) > 1
ORDER BY completed_runs DESC, root_url;

\echo 'Latest completed import per root_url'
WITH ranked AS (
  SELECT
    id,
    root_url,
    status,
    stats,
    row_number() OVER (PARTITION BY root_url ORDER BY id DESC) AS rn
  FROM ingest.import_runs
  WHERE status = 'completed'
)
SELECT id AS latest_import_run_id, root_url, stats
FROM ranked
WHERE rn = 1
ORDER BY root_url;

\echo 'Event identity collisions by normalized event name and date_range'
SELECT
  lower(trim(e.name)) AS event_name,
  lower(trim(coalesce(e.date_range, ''))) AS date_range,
  count(*) AS event_rows,
  count(DISTINCT ir.root_url) AS distinct_root_urls,
  array_agg(e.import_run_id ORDER BY e.import_run_id) AS import_run_ids,
  array_agg(ir.root_url ORDER BY e.import_run_id) AS root_urls
FROM ingest.events e
JOIN ingest.import_runs ir ON ir.id = e.import_run_id
WHERE ir.status = 'completed'
GROUP BY lower(trim(e.name)), lower(trim(coalesce(e.date_range, '')))
HAVING count(*) > 1
ORDER BY event_rows DESC, event_name;

