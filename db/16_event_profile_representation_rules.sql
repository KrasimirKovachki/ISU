-- Event/source profile rules for competition type and representation handling.
--
-- These rules are deliberately data-driven. Import code reads source profile
-- settings so parser behavior does not hardcode each event family.

-- International ISU/NonISU/ISUCS competitions: skaters represent countries.
UPDATE ingest.source_profiles
SET
  representation_primary = 'nation',
  competition_stream = CASE
    WHEN competition_stream IS NULL THEN 'international'
    ELSE competition_stream
  END,
  settings = settings
    || jsonb_build_object(
      'event_type', 'international',
      'competition_level', 'federated',
      'representation', jsonb_build_object(
        'primary', 'nation',
        'nation_column', 'country',
        'store_nation', true,
        'store_club', false,
        'empty_club_policy', 'not_applicable'
      )
    )
WHERE profile_key LIKE 'denkova_staviski_%'
   OR profile_key LIKE 'sofia_trophy_%'
   OR (
     profile_key LIKE 'bsf_discovered_%'
     AND competition_stream IN ('ISU', 'NonISU', 'ISUCS')
     AND NOT (
       event_path_prefix LIKE 'figure-skating/black-sea-ice-cup%'
       AND competition_stream <> 'ISU'
     )
   );

-- Ice Peak Trophy is amateur/local and club-primary. Club may be empty.
UPDATE ingest.source_profiles
SET
  representation_primary = 'club',
  competition_stream = 'club_amateur',
  settings = settings
    || jsonb_build_object(
      'event_type', 'amateur_club',
      'competition_level', 'amateur',
      'representation', jsonb_build_object(
        'primary', 'club',
        'club_column', 'club',
        'nation_column', 'country',
        'store_nation', true,
        'store_club', true,
        'empty_club_policy', 'preserve_empty_review'
      )
    )
WHERE event_path_prefix LIKE 'figure-skating/ice-peak-trophy%';

-- BSF local/national club competitions in old ISUCalcFS often label the club
-- column as Nation. Treat that column as club for import/profile identity.
UPDATE ingest.source_profiles
SET
  representation_primary = 'club',
  competition_stream = CASE
    WHEN event_path_prefix LIKE 'figure-skating/national-championships%' THEN 'national_club'
    WHEN event_path_prefix LIKE 'figure-skating/priz-victoria%' THEN 'national_club'
    WHEN event_path_prefix LIKE 'figure-skating/kontrolno-sastezanie%' THEN 'club_control'
    ELSE COALESCE(competition_stream, 'club')
  END,
  settings = settings
    || jsonb_build_object(
      'event_type', CASE
        WHEN event_path_prefix LIKE 'figure-skating/national-championships%' THEN 'national_club'
        WHEN event_path_prefix LIKE 'figure-skating/priz-victoria%' THEN 'national_club'
        WHEN event_path_prefix LIKE 'figure-skating/kontrolno-sastezanie%' THEN 'club_control'
        ELSE 'club'
      END,
      'competition_level', CASE
        WHEN event_path_prefix LIKE 'figure-skating/national-championships%' THEN 'national'
        ELSE 'local'
      END,
      'representation', jsonb_build_object(
        'primary', 'club',
        'nation_column', 'club',
        'store_nation', false,
        'store_club', true,
        'empty_club_policy', 'preserve_empty_review'
      )
    )
WHERE host = 'www.bsf.bg'
  AND parser_profile = 'old_isucalcfs'
  AND representation_primary = 'club';

-- BSF local FS Manager club sources generally have separate Nation and Club
-- columns. Keep both, but use Club as the primary representation.
UPDATE ingest.source_profiles
SET
  representation_primary = 'club',
  settings = settings
    || jsonb_build_object(
      'event_type', CASE
        WHEN event_path_prefix LIKE 'figure-skating/black-sea-ice-cup%' AND competition_stream <> 'ISU' THEN 'club'
        ELSE COALESCE(settings->>'event_type', 'club')
      END,
      'competition_level', CASE
        WHEN event_path_prefix LIKE 'figure-skating/black-sea-ice-cup%' AND competition_stream <> 'ISU' THEN 'local'
        ELSE COALESCE(settings->>'competition_level', 'local')
      END,
      'representation', jsonb_build_object(
        'primary', 'club',
        'nation_column', 'country',
        'club_column', 'club',
        'store_nation', true,
        'store_club', true,
        'empty_club_policy', 'preserve_empty_review'
      )
    )
WHERE host = 'www.bsf.bg'
  AND parser_profile = 'fs_manager'
  AND (
    representation_primary = 'club'
    AND competition_stream NOT IN ('ISU', 'NonISU', 'ISUCS')
    OR (
      event_path_prefix LIKE 'figure-skating/black-sea-ice-cup%'
      AND competition_stream <> 'ISU'
    )
  );

-- Correct already-imported old BSF club-primary rows where club abbreviations
-- were stored in nation_code by old ISUCalcFS column naming.
WITH target_rows AS (
  SELECT ssa.id, ssa.nation_code
  FROM ingest.source_skater_appearances ssa
  JOIN ingest.events e ON e.id = ssa.event_id
  JOIN ingest.source_profiles sp ON sp.id = e.source_profile_id
  WHERE sp.settings->'representation'->>'nation_column' = 'club'
    AND NULLIF(ssa.nation_code, '') IS NOT NULL
    AND ssa.nation_code !~ '^[A-Z]{3}$'
    AND COALESCE(ssa.club_name, '') = ''
)
UPDATE ingest.source_skater_appearances ssa
SET
  club_name = target_rows.nation_code,
  nation_code = NULL,
  representation_type = 'club',
  representation_value = target_rows.nation_code,
  raw = ssa.raw || jsonb_build_object(
    'representation_migration', jsonb_build_object(
      'rule', 'old_isucalcfs_nation_column_contains_club',
      'original_nation_code', target_rows.nation_code
    )
  )
FROM target_rows
WHERE ssa.id = target_rows.id;

-- General club-primary safeguard: club-primary sources can still have country.
-- Keep valid ISO country codes in nation_code. For FS Manager sources where
-- Nation is configured as country, non-ISO values are parser/source artifacts,
-- not reliable club names, so clear nation_code and preserve the raw value only.
WITH target_rows AS (
  SELECT ssa.id, ssa.nation_code
  FROM ingest.source_skater_appearances ssa
  JOIN ingest.events e ON e.id = ssa.event_id
  JOIN ingest.source_profiles sp ON sp.id = e.source_profile_id
  WHERE sp.representation_primary = 'club'
    AND sp.parser_profile = 'fs_manager'
    AND COALESCE(sp.settings->'representation'->>'nation_column', 'country') = 'country'
    AND NULLIF(ssa.nation_code, '') IS NOT NULL
    AND ssa.nation_code !~ '^[A-Z]{3}$'
)
UPDATE ingest.source_skater_appearances ssa
SET
  nation_code = NULL,
  representation_type = 'club',
  representation_value = NULLIF(ssa.club_name, ''),
  raw = ssa.raw || jsonb_build_object(
    'representation_migration', jsonb_build_object(
      'rule', 'fs_manager_club_primary_non_iso_nation_code_cleared',
      'original_nation_code', target_rows.nation_code
    )
  )
FROM target_rows
WHERE ssa.id = target_rows.id;

-- Clean up rows affected by the earlier over-broad migration that moved
-- truncated FS Manager Nation fragments into club_name.
UPDATE ingest.source_skater_appearances ssa
SET
  club_name = NULL,
  representation_type = 'club',
  representation_value = NULL,
  raw = ssa.raw || jsonb_build_object(
    'representation_migration', jsonb_build_object(
      'rule', 'fs_manager_club_primary_truncated_club_fragment_cleared',
      'original_club_name', ssa.club_name,
      'previous_migration', ssa.raw->'representation_migration'
    )
  )
FROM ingest.events e
JOIN ingest.source_profiles sp ON sp.id = e.source_profile_id
WHERE ssa.event_id = e.id
  AND sp.representation_primary = 'club'
  AND sp.parser_profile = 'fs_manager'
  AND ssa.club_name ~ '^[a-z]{3}$'
  AND ssa.raw->'representation_migration'->>'rule' = 'club_primary_non_iso_nation_code_interpreted_as_club';

-- Make club-primary rows consistent where club_name exists but representation
-- fields were left empty by older imports.
UPDATE ingest.source_skater_appearances ssa
SET
  representation_type = 'club',
  representation_value = NULLIF(ssa.club_name, '')
FROM ingest.events e
JOIN ingest.source_profiles sp ON sp.id = e.source_profile_id
WHERE ssa.event_id = e.id
  AND sp.representation_primary = 'club'
  AND COALESCE(ssa.club_name, '') <> ''
  AND (
    ssa.representation_type IS DISTINCT FROM 'club'
    OR ssa.representation_value IS DISTINCT FROM ssa.club_name
  );
