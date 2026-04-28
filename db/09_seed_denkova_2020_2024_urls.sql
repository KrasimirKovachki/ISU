-- Source profiles and URL registry rows for Denkova-Staviski 2020-2024 batch.

WITH profile_seed(profile_key, event_path_prefix, competition_stream, status_note) AS (
  VALUES
    ('denkova_staviski_2020_isu', '2020/ISU', 'ISU', 'site fallback; no result index found'),
    ('denkova_staviski_2020_nonisu', '2020/NonISU', 'NonISU', 'site fallback; no result index found'),
    ('denkova_staviski_2022_isu', '2022/ISU', 'ISU', NULL),
    ('denkova_staviski_2022_nonisu', '2022/NonISU', 'NonISU', NULL),
    ('denkova_staviski_2023_isu', '2023/ISU', 'ISU', NULL),
    ('denkova_staviski_2023_nonisu', '2023/NonISU', 'NonISU', NULL)
)
INSERT INTO ingest.source_profiles (
  profile_key,
  parser_profile,
  host,
  event_path_prefix,
  competition_stream,
  representation_primary,
  settings
)
SELECT
  profile_key,
  'fs_manager',
  'cup.clubdenkovastaviski.com',
  event_path_prefix,
  competition_stream,
  'nation',
  jsonb_build_object(
    'representation', jsonb_build_object('primary', 'nation', 'store_nation', true, 'store_club', false),
    'status_note', status_note
  )
FROM profile_seed
ON CONFLICT (profile_key) DO UPDATE SET
  parser_profile = EXCLUDED.parser_profile,
  host = EXCLUDED.host,
  event_path_prefix = EXCLUDED.event_path_prefix,
  competition_stream = EXCLUDED.competition_stream,
  representation_primary = EXCLUDED.representation_primary,
  settings = EXCLUDED.settings,
  updated_at = now();

WITH url_seed(url, profile_key, parser_profile, competition_stream, status, validation_status, summary, notes) AS (
  VALUES
    (
      'https://cup.clubdenkovastaviski.com/2020/NonISU/index.htm',
      'denkova_staviski_2020_nonisu',
      'fs_manager',
      'NonISU',
      'skipped',
      'failed',
      '{"source_condition":"site_fallback_html","http_status":200,"content_type":"text/html","content_length":1400,"interpretation":"URL and common result paths return site fallback HTML; no importable result index found."}'::jsonb,
      '2020 NonISU result source unavailable: index/CAT/SEG candidate paths return site fallback HTML.'
    ),
    (
      'https://cup.clubdenkovastaviski.com/2020/ISU/index.htm',
      'denkova_staviski_2020_isu',
      'fs_manager',
      'ISU',
      'skipped',
      'failed',
      '{"source_condition":"site_fallback_html","http_status":200,"content_type":"text/html","content_length":1400,"interpretation":"URL and common result paths return site fallback HTML; no importable result index found."}'::jsonb,
      '2020 ISU result source unavailable: index/CAT/SEG candidate paths return site fallback HTML.'
    ),
    (
      'https://cup.clubdenkovastaviski.com/2022/NonISU/index.htm',
      'denkova_staviski_2022_nonisu',
      'fs_manager',
      'NonISU',
      'ready_for_import',
      'passed',
      '{"categories":12,"segments":14,"parser_profile":"fs_manager"}'::jsonb,
      'Parsed cleanly with FS Manager profile.'
    ),
    (
      'https://cup.clubdenkovastaviski.com/2022/ISU/index.htm',
      'denkova_staviski_2022_isu',
      'fs_manager',
      'ISU',
      'ready_for_import',
      'passed',
      '{"categories":4,"segments":8,"parser_profile":"fs_manager"}'::jsonb,
      'Parsed cleanly with FS Manager profile.'
    ),
    (
      'https://cup.clubdenkovastaviski.com/2023/NonISU/index.htm',
      'denkova_staviski_2023_nonisu',
      'fs_manager',
      'NonISU',
      'ready_for_import',
      'passed',
      '{"categories":13,"segments":15,"parser_profile":"fs_manager"}'::jsonb,
      'Parsed cleanly with FS Manager profile.'
    ),
    (
      'https://cup.clubdenkovastaviski.com/2023/ISU/index.htm',
      'denkova_staviski_2023_isu',
      'fs_manager',
      'ISU',
      'ready_for_import',
      'passed',
      '{"categories":4,"segments":8,"parser_profile":"fs_manager"}'::jsonb,
      'Parsed cleanly with FS Manager profile.'
    ),
    (
      'https://cup.clubdenkovastaviski.com/2024/NonISU/index.htm',
      'denkova_staviski_2024_nonisu',
      'fs_manager',
      'NonISU',
      'imported',
      'passed',
      '{"categories":10,"segments":12,"parser_profile":"fs_manager","existing_import_run_id":3}'::jsonb,
      'Already imported before this batch as run 3.'
    ),
    (
      'https://cup.clubdenkovastaviski.com/2024/ISU/index.htm',
      'denkova_staviski_2024_isu',
      'fs_manager',
      'ISU',
      'imported',
      'passed',
      '{"categories":6,"segments":12,"parser_profile":"fs_manager","existing_import_run_id":2}'::jsonb,
      'Already imported before this batch as run 2.'
    )
)
INSERT INTO ingest.source_url_registry (
  url,
  source_profile_id,
  parser_profile,
  competition_stream,
  status,
  validation_status,
  last_import_run_id,
  summary,
  notes
)
SELECT
  seed.url,
  sp.id,
  seed.parser_profile,
  seed.competition_stream,
  seed.status,
  seed.validation_status,
  CASE
    WHEN seed.url = 'https://cup.clubdenkovastaviski.com/2024/NonISU/index.htm' THEN 3
    WHEN seed.url = 'https://cup.clubdenkovastaviski.com/2024/ISU/index.htm' THEN 2
    ELSE NULL
  END,
  seed.summary,
  seed.notes
FROM url_seed seed
JOIN ingest.source_profiles sp ON sp.profile_key = seed.profile_key
ON CONFLICT (url) DO UPDATE SET
  source_profile_id = EXCLUDED.source_profile_id,
  parser_profile = EXCLUDED.parser_profile,
  competition_stream = EXCLUDED.competition_stream,
  status = EXCLUDED.status,
  validation_status = EXCLUDED.validation_status,
  last_import_run_id = EXCLUDED.last_import_run_id,
  summary = EXCLUDED.summary,
  notes = EXCLUDED.notes,
  updated_at = now();
