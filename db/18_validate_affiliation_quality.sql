-- Affiliation quality checks for country/club/display fields.
--
-- This is a read-only validation script. Use it after imports and after source
-- profile rule changes.

-- 1) Country field must contain ISO-like country codes only.
SELECT
  'non_iso_nation_code' AS check_name,
  ve.event_name,
  ve.root_url,
  sac.category_name,
  sac.appearance_type,
  sac.skater_name,
  sac.nation_code,
  sac.club_name
FROM analytics.v_skater_appearances_clean sac
JOIN analytics.v_events ve ON ve.event_id = sac.event_id
WHERE sac.nation_code IS NOT NULL
  AND sac.nation_code !~ '^[A-Z]{3}$'
ORDER BY ve.event_name, sac.category_name, sac.skater_name
LIMIT 100;

-- 2) FS Manager club-primary sources should not invent short lowercase clubs
-- from malformed/truncated Nation values. Real club names can be abbreviations,
-- but lowercase 3-character fragments usually indicate parser/source artifacts.
SELECT
  'suspicious_fs_manager_club_fragment' AS check_name,
  ve.event_name,
  ve.root_url,
  sac.category_name,
  sac.appearance_type,
  sac.skater_name,
  sac.nation_code,
  sac.club_name,
  sac.raw->'representation_migration' AS representation_migration
FROM analytics.v_skater_appearances_clean sac
JOIN analytics.v_events ve ON ve.event_id = sac.event_id
WHERE ve.parser_profile = 'fs_manager'
  AND ve.representation_primary = 'club'
  AND sac.club_name ~ '^[a-z]{3}$'
ORDER BY ve.event_name, sac.category_name, sac.skater_name
LIMIT 100;

-- 3) Club-primary rows where display affiliation is only country should be
-- reviewed. Some rows legitimately have empty/unknown club, but they should be
-- visible to data QA before they become public profile facts.
SELECT
  'club_primary_missing_club' AS check_name,
  ve.event_name,
  ve.root_url,
  sac.category_name,
  sac.appearance_type,
  sac.skater_name,
  sac.nation_code,
  sac.club_name,
  sac.display_affiliation
FROM analytics.v_skater_appearances_clean sac
JOIN analytics.v_events ve ON ve.event_id = sac.event_id
WHERE ve.representation_primary = 'club'
  AND sac.club_name IS NULL
ORDER BY ve.event_name, sac.category_name, sac.skater_name
LIMIT 100;

-- 4) Summary by source profile for manual review.
SELECT
  ve.event_type,
  ve.competition_level,
  ve.competition_stream,
  ve.profile_key,
  ve.root_url,
  count(*) AS appearances,
  count(*) FILTER (WHERE sac.nation_code IS NOT NULL) AS with_country,
  count(*) FILTER (WHERE sac.club_name IS NOT NULL) AS with_club,
  count(*) FILTER (WHERE sac.nation_code IS NOT NULL AND sac.nation_code !~ '^[A-Z]{3}$') AS non_iso_country_values,
  count(*) FILTER (WHERE ve.parser_profile = 'fs_manager' AND sac.club_name ~ '^[a-z]{3}$') AS lowercase_three_char_clubs,
  count(*) FILTER (WHERE ve.representation_primary = 'club' AND sac.club_name IS NULL) AS club_primary_missing_club
FROM analytics.v_skater_appearances_clean sac
JOIN analytics.v_events ve ON ve.event_id = sac.event_id
GROUP BY
  ve.event_type,
  ve.competition_level,
  ve.competition_stream,
  ve.profile_key,
  ve.root_url
ORDER BY lowercase_three_char_clubs DESC, club_primary_missing_club DESC, appearances DESC;
