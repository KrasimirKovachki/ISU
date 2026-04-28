-- Compare skaters that appear in multiple competitions.
--
-- Purpose:
-- - manual validation that imported skater identity/score data is coherent
-- - quick examples of skaters with results across multiple events
-- - comparison of final category points and segment/PDF scores over time
--
-- Identity key is intentionally conservative for now:
--   normalized_name + nation_code
-- This avoids merging same-name skaters from different countries while the
-- future core skater/profile tables are still being designed.

\set min_competitions 2
\set sample_skaters 25

\echo 'Potential representation issues: non-ISO-looking values stored in nation_code'
WITH latest_import_runs AS (
  SELECT DISTINCT ON (root_url)
    id,
    root_url
  FROM ingest.import_runs
  WHERE status = 'completed'
  ORDER BY root_url, id DESC
)
SELECT
  ssa.nation_code,
  count(*) AS appearances,
  count(DISTINCT ssa.normalized_name) AS skaters,
  count(DISTINCT e.id) AS events,
  array_agg(DISTINCT e.name ORDER BY e.name) AS sample_events
FROM ingest.source_skater_appearances ssa
JOIN latest_import_runs lir ON lir.id = ssa.import_run_id
JOIN ingest.events e ON e.id = ssa.event_id
WHERE NULLIF(ssa.nation_code, '') IS NOT NULL
  AND ssa.nation_code !~ '^[A-Z]{3}$'
GROUP BY ssa.nation_code
ORDER BY appearances DESC, ssa.nation_code
LIMIT 100;

\echo 'Skaters with appearances in multiple competitions'
WITH latest_import_runs AS (
  SELECT DISTINCT ON (root_url)
    id,
    root_url
  FROM ingest.import_runs
  WHERE status = 'completed'
  ORDER BY root_url, id DESC
),
appearances AS (
  SELECT
    ssa.normalized_name,
    NULLIF(ssa.nation_code, '') AS nation_code,
    NULLIF(ssa.club_name, '') AS club_name,
    e.id AS event_id,
    e.name AS event_name,
    e.date_range,
    ir.root_url,
    c.name AS category_name,
    s.name AS segment_name,
    ssa.appearance_type
  FROM ingest.source_skater_appearances ssa
  JOIN latest_import_runs lir ON lir.id = ssa.import_run_id
  JOIN ingest.import_runs ir ON ir.id = ssa.import_run_id
  JOIN ingest.events e ON e.id = ssa.event_id
  LEFT JOIN ingest.categories c ON c.id = ssa.category_id
  LEFT JOIN ingest.segments s ON s.id = ssa.segment_id
  WHERE ssa.normalized_name <> ''
),
multi_competition_skaters AS (
  SELECT
    normalized_name,
    nation_code,
    count(DISTINCT event_id) AS competitions,
    count(DISTINCT root_url) AS source_urls,
    count(*) AS appearances,
    count(DISTINCT category_name) FILTER (WHERE category_name IS NOT NULL) AS categories_seen,
    min(date_range) AS first_seen_source_date,
    max(date_range) AS last_seen_source_date,
    array_agg(DISTINCT club_name) FILTER (WHERE club_name IS NOT NULL) AS clubs_seen,
    array_agg(DISTINCT event_name ORDER BY event_name) AS events_seen
  FROM appearances
  GROUP BY normalized_name, nation_code
  HAVING count(DISTINCT event_id) >= :min_competitions
)
SELECT *
FROM multi_competition_skaters
ORDER BY competitions DESC, appearances DESC, normalized_name, nation_code
LIMIT :sample_skaters;

\echo 'Category final-result comparison for multi-competition skaters'
WITH latest_import_runs AS (
  SELECT DISTINCT ON (root_url)
    id,
    root_url
  FROM ingest.import_runs
  WHERE status = 'completed'
  ORDER BY root_url, id DESC
),
category_scores AS (
  SELECT
    ssa.normalized_name,
    NULLIF(ssa.nation_code, '') AS nation_code,
    NULLIF(ssa.club_name, '') AS club_name,
    e.name AS event_name,
    e.date_range,
    ir.root_url,
    c.name AS category_name,
    cr.final_place,
    cr.points,
    cr.segment_places
  FROM ingest.category_results cr
  JOIN ingest.source_skater_appearances ssa ON ssa.id = cr.skater_appearance_id
  JOIN latest_import_runs lir ON lir.id = ssa.import_run_id
  JOIN ingest.import_runs ir ON ir.id = ssa.import_run_id
  JOIN ingest.events e ON e.id = ssa.event_id
  JOIN ingest.categories c ON c.id = cr.category_id
  WHERE ssa.normalized_name <> ''
),
multi_competition_skaters AS (
  SELECT normalized_name, nation_code
  FROM category_scores
  GROUP BY normalized_name, nation_code
  HAVING count(DISTINCT root_url) >= :min_competitions
),
ranked_rows AS (
  SELECT
    cs.*,
    lag(points) OVER (
      PARTITION BY cs.normalized_name, cs.nation_code, cs.category_name
      ORDER BY cs.date_range, cs.event_name, cs.root_url
    ) AS previous_points,
    points - lag(points) OVER (
      PARTITION BY cs.normalized_name, cs.nation_code, cs.category_name
      ORDER BY cs.date_range, cs.event_name, cs.root_url
    ) AS points_delta_same_category
  FROM category_scores cs
  JOIN multi_competition_skaters mcs
    ON mcs.normalized_name = cs.normalized_name
   AND mcs.nation_code IS NOT DISTINCT FROM cs.nation_code
)
SELECT
  normalized_name,
  nation_code,
  club_name,
  event_name,
  date_range,
  category_name,
  final_place,
  points,
  previous_points,
  points_delta_same_category,
  segment_places,
  root_url
FROM ranked_rows
ORDER BY normalized_name, nation_code, date_range, event_name, category_name
LIMIT 250;

\echo 'Segment/PDF score comparison for multi-competition skaters'
WITH latest_import_runs AS (
  SELECT DISTINCT ON (root_url)
    id,
    root_url
  FROM ingest.import_runs
  WHERE status = 'completed'
  ORDER BY root_url, id DESC
),
pdf_segment_scores AS (
  SELECT
    ssa.normalized_name,
    NULLIF(ssa.nation_code, '') AS nation_code,
    NULLIF(ssa.club_name, '') AS club_name,
    e.name AS event_name,
    e.date_range,
    ir.root_url,
    c.name AS category_name,
    s.name AS segment_name,
    pss.rank AS place,
    pss.total_segment_score AS tss,
    pss.total_element_score AS tes,
    pss.total_program_component_score AS pcs,
    pss.total_deductions AS deduction,
    pss.base_value_total,
    pss.element_score_total,
    pss.judge_count,
    'pdf_score_summary'::text AS score_source
  FROM ingest.pdf_score_summaries pss
  JOIN ingest.source_skater_appearances ssa ON ssa.id = pss.skater_appearance_id
  JOIN latest_import_runs lir ON lir.id = ssa.import_run_id
  JOIN ingest.import_runs ir ON ir.id = ssa.import_run_id
  JOIN ingest.segments s ON s.id = pss.segment_id
  JOIN ingest.categories c ON c.id = s.category_id
  JOIN ingest.events e ON e.id = ssa.event_id
  WHERE ssa.normalized_name <> ''
),
html_segment_scores AS (
  SELECT
    ssa.normalized_name,
    NULLIF(ssa.nation_code, '') AS nation_code,
    NULLIF(ssa.club_name, '') AS club_name,
    e.name AS event_name,
    e.date_range,
    ir.root_url,
    c.name AS category_name,
    s.name AS segment_name,
    sr.place,
    sr.tss,
    sr.tes,
    sr.pcs,
    sr.deduction,
    NULL::numeric AS base_value_total,
    NULL::numeric AS element_score_total,
    NULL::integer AS judge_count,
    'html_segment_result'::text AS score_source
  FROM ingest.segment_results sr
  JOIN ingest.source_skater_appearances ssa ON ssa.id = sr.skater_appearance_id
  JOIN latest_import_runs lir ON lir.id = ssa.import_run_id
  JOIN ingest.import_runs ir ON ir.id = ssa.import_run_id
  JOIN ingest.segments s ON s.id = sr.segment_id
  JOIN ingest.categories c ON c.id = s.category_id
  JOIN ingest.events e ON e.id = ssa.event_id
  WHERE ssa.normalized_name <> ''
    AND NOT EXISTS (
      SELECT 1
      FROM ingest.pdf_score_summaries pss
      JOIN ingest.source_skater_appearances pss_ssa ON pss_ssa.id = pss.skater_appearance_id
      WHERE pss.segment_id = sr.segment_id
        AND pss_ssa.normalized_name = ssa.normalized_name
        AND pss_ssa.nation_code IS NOT DISTINCT FROM ssa.nation_code
    )
),
segment_scores AS (
  SELECT * FROM pdf_segment_scores
  UNION ALL
  SELECT * FROM html_segment_scores
),
multi_competition_skaters AS (
  SELECT normalized_name, nation_code
  FROM segment_scores
  GROUP BY normalized_name, nation_code
  HAVING count(DISTINCT root_url) >= :min_competitions
),
ranked_rows AS (
  SELECT
    ss.*,
    lag(tss) OVER (
      PARTITION BY ss.normalized_name, ss.nation_code, ss.segment_name
      ORDER BY ss.date_range, ss.event_name, ss.root_url, ss.category_name
    ) AS previous_tss_same_segment,
    tss - lag(tss) OVER (
      PARTITION BY ss.normalized_name, ss.nation_code, ss.segment_name
      ORDER BY ss.date_range, ss.event_name, ss.root_url, ss.category_name
    ) AS tss_delta_same_segment
  FROM segment_scores ss
  JOIN multi_competition_skaters mcs
    ON mcs.normalized_name = ss.normalized_name
   AND mcs.nation_code IS NOT DISTINCT FROM ss.nation_code
)
SELECT
  normalized_name,
  nation_code,
  club_name,
  event_name,
  date_range,
  category_name,
  segment_name,
  place,
  tss,
  previous_tss_same_segment,
  tss_delta_same_segment,
  tes,
  pcs,
  deduction,
  base_value_total,
  element_score_total,
  judge_count,
  score_source,
  root_url
FROM ranked_rows
ORDER BY normalized_name, nation_code, date_range, event_name, category_name, segment_name
LIMIT 500;
