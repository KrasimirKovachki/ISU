-- Manual validation queries for imported skating data.
-- Run with:
-- db/psql_local.sh -f db/validation_queries.sql

\echo '1. Import runs'
SELECT
  ir.id,
  ir.status,
  sp.profile_key,
  ir.root_url,
  ir.stats,
  ir.error_message
FROM ingest.import_runs ir
LEFT JOIN ingest.source_profiles sp ON sp.id = ir.source_profile_id
ORDER BY ir.id;

\echo '2. Scoring marker legend'
SELECT code, label, meaning, validation_note
FROM core.v_scoring_marker_legend;

\echo '3. Imported marker usage by run'
SELECT
  e.import_run_id,
  marker,
  count(*) AS element_count
FROM ingest.events e
JOIN ingest.categories c ON c.event_id = e.id
JOIN ingest.segments s ON s.category_id = c.id
JOIN ingest.pdf_score_summaries pss ON pss.segment_id = s.id
JOIN ingest.pdf_elements pe ON pe.pdf_score_summary_id = pss.id
CROSS JOIN LATERAL unnest(pe.markers) AS marker
GROUP BY e.import_run_id, marker
ORDER BY e.import_run_id, element_count DESC, marker;

\echo '4. Downgraded jump examples (<<)'
SELECT
  e.import_run_id,
  c.name AS category,
  s.name AS segment,
  ssa.raw_name AS skater,
  ssa.nation_code,
  pe.element_no,
  pe.element_code,
  pe.base_element_code,
  pe.markers,
  pe.base_value,
  pe.goe,
  pe.panel_score
FROM ingest.pdf_elements pe
JOIN ingest.pdf_score_summaries pss ON pss.id = pe.pdf_score_summary_id
JOIN ingest.source_skater_appearances ssa ON ssa.id = pss.skater_appearance_id
JOIN ingest.segments s ON s.id = pss.segment_id
JOIN ingest.categories c ON c.id = s.category_id
JOIN ingest.events e ON e.id = c.event_id
WHERE pe.markers @> ARRAY['<<']::text[]
ORDER BY e.import_run_id, c.name, s.name, ssa.raw_name, pe.element_no
LIMIT 50;

\echo '5. Elements with marker in code but missing parsed marker'
SELECT
  e.import_run_id,
  c.name AS category,
  s.name AS segment,
  ssa.raw_name AS skater,
  pe.element_code,
  pe.markers
FROM ingest.pdf_elements pe
JOIN ingest.pdf_score_summaries pss ON pss.id = pe.pdf_score_summary_id
JOIN ingest.source_skater_appearances ssa ON ssa.id = pss.skater_appearance_id
JOIN ingest.segments s ON s.id = pss.segment_id
JOIN ingest.categories c ON c.id = s.category_id
JOIN ingest.events e ON e.id = c.event_id
WHERE (
    pe.element_code LIKE '%<<%'
    AND NOT pe.markers @> ARRAY['<<']::text[]
  )
  OR (
    pe.element_code LIKE '%<%'
    AND pe.element_code NOT LIKE '%<<%'
    AND NOT pe.markers @> ARRAY['<']::text[]
  )
  OR (
    pe.element_code LIKE '%!%'
    AND NOT pe.markers @> ARRAY['!']::text[]
  )
ORDER BY e.import_run_id, c.name, s.name, ssa.raw_name, pe.element_code
LIMIT 50;

\echo '6. Representation validation for club-profile imports'
SELECT
  e.import_run_id,
  sp.profile_key,
  ssa.appearance_type,
  CASE
    WHEN coalesce(ssa.club_name, '') = '' THEN 'country_or_international'
    ELSE 'club'
  END AS source_bucket,
  ssa.representation_type,
  count(*) AS appearances
FROM ingest.source_skater_appearances ssa
JOIN ingest.events e ON e.id = ssa.event_id
LEFT JOIN ingest.source_profiles sp ON sp.id = e.source_profile_id
WHERE sp.representation_primary = 'club'
GROUP BY e.import_run_id, sp.profile_key, ssa.appearance_type, source_bucket, ssa.representation_type
ORDER BY e.import_run_id, ssa.appearance_type, source_bucket, ssa.representation_type;

\echo '7. Club-profile rows with empty Club needing manual review'
SELECT
  e.import_run_id,
  sp.profile_key,
  ssa.appearance_type,
  ssa.raw_name,
  ssa.nation_code,
  ssa.club_name,
  ssa.representation_type,
  ssa.representation_value,
  CASE
    WHEN coalesce(ssa.representation_value, '') <> '' THEN 'review: empty club but representation_value is filled'
    ELSE 'review: empty club, possible individual/unknown/empty source value'
  END AS validation_status
FROM ingest.source_skater_appearances ssa
JOIN ingest.events e ON e.id = ssa.event_id
LEFT JOIN ingest.source_profiles sp ON sp.id = e.source_profile_id
WHERE sp.representation_primary = 'club'
  AND coalesce(ssa.club_name, '') = ''
  AND ssa.appearance_type IN ('entry', 'category_result')
ORDER BY e.import_run_id, ssa.raw_name
LIMIT 100;

\echo '8. Officials by role group'
SELECT
  e.import_run_id,
  oa.role_group,
  count(*) AS assignments,
  count(*) FILTER (WHERE oa.judge_number IS NOT NULL) AS numbered_judges
FROM ingest.official_assignments oa
JOIN ingest.segments s ON s.id = oa.segment_id
JOIN ingest.categories c ON c.id = s.category_id
JOIN ingest.events e ON e.id = c.event_id
GROUP BY e.import_run_id, oa.role_group
ORDER BY e.import_run_id, oa.role_group;

\echo '9. Parse issues'
SELECT
  import_run_id,
  level,
  code,
  count(*) AS count
FROM ingest.parse_issues
GROUP BY import_run_id, level, code
ORDER BY import_run_id, level, code;
