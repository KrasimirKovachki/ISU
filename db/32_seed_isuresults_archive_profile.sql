-- Source profile for the canonical ISU result archive.
--
-- isuresults.com pages from 2019-2022 set <meta name="generator"
-- content="ISUCalcFS X.X.X by Swiss Timing">, which made the body-content
-- auto-detection in scripts/import_event.py route them to old_isucalcfs.
-- Structurally though these pages use FS Manager link conventions
-- (CAT###EN.htm / SEG###.htm / *_JudgesDetailsperSkater.pdf) and
-- old_isucalcfs.parse_index returns 0 categories on them, while
-- fs_manager.parse_index handles them cleanly.
--
-- Pinning the parser profile via configuration (this row) is the supported
-- per-platform mechanism; import_event trusts a matching source_profiles
-- entry over body sniffing.

INSERT INTO ingest.source_profiles (
  profile_key, parser_profile, host, event_path_prefix,
  competition_stream, representation_primary, settings
) VALUES (
  'isuresults_archive',
  'fs_manager',
  'www.isuresults.com',
  'results/',
  'ISU',
  'nation',
  jsonb_build_object(
    'representation', jsonb_build_object('primary', 'nation'),
    'host_archive', 'isuresults.com',
    'parser_lock_reason', 'meta-generator says ISUCalcFS but pages use FS Manager link convention'
  )
)
ON CONFLICT (profile_key) DO UPDATE SET
  parser_profile = EXCLUDED.parser_profile,
  host = EXCLUDED.host,
  event_path_prefix = EXCLUDED.event_path_prefix,
  competition_stream = EXCLUDED.competition_stream,
  representation_primary = EXCLUDED.representation_primary,
  settings = EXCLUDED.settings,
  updated_at = now();
