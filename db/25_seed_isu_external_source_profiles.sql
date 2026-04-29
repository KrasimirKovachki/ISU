-- Source profiles for result folders discovered from official ISU event detail
-- pages. These are external result systems, but they should still be governed
-- by source-profile settings rather than imported as anonymous URLs.

INSERT INTO ingest.source_profiles (
  profile_key,
  parser_profile,
  host,
  event_path_prefix,
  competition_stream,
  representation_primary,
  settings
) VALUES
  (
    'isu_official_adult_2025_deu_event',
    'fs_manager',
    'www.deu-event.de',
    'results/adult2025',
    'ISU',
    'nation',
    '{"representation":{"primary":"nation","store_nation":true,"store_club":false},"discovered_from":"isu_official_events","event_type":"adult_international","source_system":"fs_manager"}'::jsonb
  )
ON CONFLICT (profile_key) DO UPDATE SET
  parser_profile = EXCLUDED.parser_profile,
  host = EXCLUDED.host,
  event_path_prefix = EXCLUDED.event_path_prefix,
  competition_stream = EXCLUDED.competition_stream,
  representation_primary = EXCLUDED.representation_primary,
  settings = EXCLUDED.settings,
  updated_at = now();

WITH profile AS (
  SELECT id
  FROM ingest.source_profiles
  WHERE profile_key = 'isu_official_adult_2025_deu_event'
)
UPDATE ingest.source_url_registry r
SET
  source_profile_id = profile.id,
  parser_profile = 'fs_manager',
  competition_stream = 'ISU',
  summary = r.summary || '{"source_profile_key":"isu_official_adult_2025_deu_event"}'::jsonb,
  updated_at = now()
FROM profile
WHERE r.url = 'https://www.deu-event.de/results/adult2025/';

WITH profile AS (
  SELECT id
  FROM ingest.source_profiles
  WHERE profile_key = 'isu_official_adult_2025_deu_event'
),
runs AS (
  SELECT id
  FROM ingest.import_runs
  WHERE root_url = 'https://www.deu-event.de/results/adult2025/'
)
UPDATE ingest.import_runs ir
SET source_profile_id = profile.id
FROM profile, runs
WHERE ir.id = runs.id;

WITH profile AS (
  SELECT id
  FROM ingest.source_profiles
  WHERE profile_key = 'isu_official_adult_2025_deu_event'
),
runs AS (
  SELECT id
  FROM ingest.import_runs
  WHERE root_url = 'https://www.deu-event.de/results/adult2025/'
)
UPDATE ingest.events e
SET source_profile_id = profile.id
FROM profile, runs
WHERE e.import_run_id = runs.id;

WITH profile AS (
  SELECT id
  FROM ingest.source_profiles
  WHERE profile_key = 'isu_official_adult_2025_deu_event'
),
runs AS (
  SELECT id
  FROM ingest.import_runs
  WHERE root_url = 'https://www.deu-event.de/results/adult2025/'
)
UPDATE ingest.source_documents sd
SET source_profile_id = profile.id
FROM profile, runs
WHERE sd.import_run_id = runs.id;
