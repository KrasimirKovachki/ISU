-- Manual validation for representation rules.
--
-- Current interpretation:
-- - ISU / NonISU international-style sources usually represent skaters by nation.
-- - Ice Peak Trophy is currently treated as club-primary because it is an
--   amateur/local source with Club in entries/results.
-- - If a club-primary source has an empty Club value, do not auto-correct it
--   yet. Report it for manual review because it may mean:
--     1. individual/no-club participant,
--     2. unknown club,
--     3. intentionally empty source value, or
--     4. missing/incomplete source data.

\echo '1. Representation profile settings'
SELECT
  profile_key,
  parser_profile,
  competition_stream,
  representation_primary,
  settings
FROM ingest.source_profiles
ORDER BY profile_key;

\echo '2. Representation buckets by import run'
SELECT
  e.import_run_id,
  sp.profile_key,
  ssa.appearance_type,
  CASE
    WHEN coalesce(ssa.club_name, '') = '' THEN 'missing_club'
    ELSE 'has_club'
  END AS club_state,
  ssa.representation_type,
  count(*) AS appearances,
  count(DISTINCT ssa.nation_code) AS nations,
  count(DISTINCT nullif(ssa.club_name, '')) AS clubs
FROM ingest.source_skater_appearances ssa
JOIN ingest.events e ON e.id = ssa.event_id
LEFT JOIN ingest.source_profiles sp ON sp.id = e.source_profile_id
WHERE ssa.appearance_type IN ('entry', 'category_result')
GROUP BY e.import_run_id, sp.profile_key, ssa.appearance_type, club_state, ssa.representation_type
ORDER BY e.import_run_id, ssa.appearance_type, club_state, ssa.representation_type;

\echo '3. Club-primary rows needing manual review because Club is empty'
SELECT
  e.import_run_id,
  sp.profile_key,
  c.name AS category,
  ssa.appearance_type,
  ssa.raw_name,
  ssa.nation_code,
  ssa.club_name,
  ssa.representation_type,
  ssa.representation_value,
  CASE
    WHEN coalesce(ssa.representation_value, '') <> '' THEN 'review: empty club but representation_value is filled'
    ELSE 'review: empty club, possible individual/unknown/empty source value'
  END AS validation_status,
  ssa.raw
FROM ingest.source_skater_appearances ssa
JOIN ingest.events e ON e.id = ssa.event_id
LEFT JOIN ingest.source_profiles sp ON sp.id = e.source_profile_id
LEFT JOIN ingest.categories c ON c.id = ssa.category_id
WHERE sp.representation_primary = 'club'
  AND ssa.appearance_type IN ('entry', 'category_result')
  AND coalesce(ssa.club_name, '') = ''
ORDER BY e.import_run_id, c.name, ssa.raw_name, ssa.appearance_type
LIMIT 200;

\echo '3b. Club-primary empty Club rows by nation'
SELECT
  e.import_run_id,
  sp.profile_key,
  ssa.nation_code,
  count(*) AS appearances
FROM ingest.source_skater_appearances ssa
JOIN ingest.events e ON e.id = ssa.event_id
LEFT JOIN ingest.source_profiles sp ON sp.id = e.source_profile_id
WHERE sp.representation_primary = 'club'
  AND ssa.appearance_type IN ('entry', 'category_result')
  AND coalesce(ssa.club_name, '') = ''
GROUP BY e.import_run_id, sp.profile_key, ssa.nation_code
ORDER BY e.import_run_id, appearances DESC, ssa.nation_code;

\echo '4. International-style rows where Club unexpectedly exists'
SELECT
  e.import_run_id,
  sp.profile_key,
  c.name AS category,
  ssa.appearance_type,
  ssa.raw_name,
  ssa.nation_code,
  ssa.club_name,
  ssa.representation_type,
  ssa.representation_value
FROM ingest.source_skater_appearances ssa
JOIN ingest.events e ON e.id = ssa.event_id
LEFT JOIN ingest.source_profiles sp ON sp.id = e.source_profile_id
LEFT JOIN ingest.categories c ON c.id = ssa.category_id
WHERE coalesce(sp.representation_primary, 'nation') = 'nation'
  AND ssa.appearance_type IN ('entry', 'category_result')
  AND coalesce(ssa.club_name, '') <> ''
ORDER BY e.import_run_id, sp.profile_key, c.name, ssa.raw_name
LIMIT 200;
