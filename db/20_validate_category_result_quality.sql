-- Category-result quality checks.
--
-- Use after parser changes/reimports to find rows where final placement,
-- points, or country/club values look incomplete compared to source layout.

-- Category result rows with missing final place or points.
SELECT
  ve.event_start_date,
  ve.event_name,
  ve.root_url,
  ve.parser_profile,
  ve.event_type,
  ve.competition_stream,
  scr.category_name,
  scr.skater_name,
  scr.nation_code,
  scr.club_name,
  scr.final_place,
  scr.points,
  scr.segment_places,
  scr.raw
FROM analytics.v_skater_category_results scr
JOIN analytics.v_events ve ON ve.event_id = scr.event_id
WHERE scr.final_place IS NULL
   OR scr.points IS NULL
ORDER BY ve.event_start_date NULLS LAST, ve.event_name, scr.category_name, scr.skater_name
LIMIT 200;

-- Old ISUCalcFS result documents with Club/Nation/Points header layouts.
-- These are safe after the header-based parser fix, but useful to identify
-- source URLs that should be reimported if they predate that fix.
WITH latest AS (
  SELECT DISTINCT ON (root_url) id, root_url
  FROM ingest.import_runs
  WHERE status = 'completed'
  ORDER BY root_url, id DESC
)
SELECT
  ir.id AS import_run_id,
  ir.root_url,
  sp.profile_key,
  count(*) FILTER (
    WHERE sd.raw_text ~* '<th[^>]*>Club</th>.*<th[^>]*>Nation</th>.*<th[^>]*>Points</th>'
  ) AS club_nation_points_result_docs
FROM latest
JOIN ingest.import_runs ir ON ir.id = latest.id
JOIN ingest.source_profiles sp ON sp.id = ir.source_profile_id
LEFT JOIN ingest.source_documents sd
  ON sd.import_run_id = ir.id
 AND sd.url ~ 'CAT[0-9]+RS\.HTM$'
WHERE sp.parser_profile = 'old_isucalcfs'
GROUP BY ir.id, ir.root_url, sp.profile_key
HAVING count(*) FILTER (
  WHERE sd.raw_text ~* '<th[^>]*>Club</th>.*<th[^>]*>Nation</th>.*<th[^>]*>Points</th>'
) > 0
ORDER BY ir.root_url;

-- Suspicious category-result rows where points were accidentally treated as a
-- segment place key. Expected result after the parser fix/reimports: zero rows.
SELECT
  ve.event_start_date,
  ve.event_name,
  ve.root_url,
  scr.category_name,
  scr.skater_name,
  scr.nation_code,
  scr.club_name,
  scr.final_place,
  scr.points,
  scr.segment_places
FROM analytics.v_skater_category_results scr
JOIN analytics.v_events ve ON ve.event_id = scr.event_id
WHERE scr.segment_places ? 'Points'
ORDER BY ve.event_start_date NULLS LAST, ve.event_name, scr.category_name, scr.skater_name
LIMIT 200;

-- Old ISUCalcFS segment-result documents with Club/Nation/TSS layouts.
-- These require header-based parsing so the club column does not get treated
-- as nation and the TSS/TES/PCS columns do not shift.
WITH latest AS (
  SELECT DISTINCT ON (root_url) id, root_url
  FROM ingest.import_runs
  WHERE status = 'completed'
  ORDER BY root_url, id DESC
)
SELECT
  ir.id AS import_run_id,
  ir.root_url,
  sp.profile_key,
  count(*) AS club_nation_tss_segment_docs
FROM latest
JOIN ingest.import_runs ir ON ir.id = latest.id
JOIN ingest.source_profiles sp ON sp.id = ir.source_profile_id
JOIN ingest.source_documents sd ON sd.import_run_id = ir.id
WHERE sp.parser_profile = 'old_isucalcfs'
  AND sd.raw_text ~* '<th[^>]*>Club</th>.*<th[^>]*>Nation</th>.*<th[^>]*>TSS'
GROUP BY ir.id, ir.root_url, sp.profile_key
ORDER BY ir.root_url;

-- Summary rows whose best scores are null/zero. Null is expected for
-- entry-only/no-score sources; zero can be valid source status for no-score
-- skaters, but should be reviewed before coach/dashboard use.
SELECT
  club_name,
  skater_name,
  nation_code,
  competitions,
  categories_seen,
  first_event_date,
  latest_event_date,
  best_tss,
  best_score_total,
  best_tes,
  best_pcs,
  event_types_seen
FROM analytics.v_club_skater_summary
WHERE best_tss IS NULL
   OR best_tss = 0
   OR best_score_total IS NULL
   OR best_score_total = 0
   OR best_pcs IS NULL
   OR best_pcs = 0
ORDER BY skater_name, club_name, nation_code
LIMIT 200;
