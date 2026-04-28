-- Generic BSF figure-skating result-source profiles discovered from
-- https://www.bsf.bg/figure-skating/national-championships.
-- These are source settings; parser behavior stays outside event-specific code.

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
    'bsf_national_championships',
    'fs_manager',
    'www.bsf.bg',
    'figure-skating/national-championships',
    'club',
    'club',
    '{"representation":{"primary":"club","store_nation":true,"store_club":true},"source_family":"bsf_discovered"}'::jsonb
  ),
  (
    'bsf_priz_victoria',
    'fs_manager',
    'www.bsf.bg',
    'figure-skating/priz-victoria',
    'club',
    'club',
    '{"representation":{"primary":"club","store_nation":true,"store_club":true},"source_family":"bsf_discovered"}'::jsonb
  ),
  (
    'bsf_black_sea_ice_cup',
    'fs_manager',
    'www.bsf.bg',
    'figure-skating/black-sea-ice-cup',
    'club',
    'club',
    '{"representation":{"primary":"club","store_nation":true,"store_club":true},"source_family":"bsf_discovered"}'::jsonb
  ),
  (
    'bsf_kontrolno_sastezanie',
    'fs_manager',
    'www.bsf.bg',
    'figure-skating/kontrolno-sastezanie',
    'club',
    'club',
    '{"representation":{"primary":"club","store_nation":true,"store_club":true},"source_family":"bsf_discovered"}'::jsonb
  ),
  (
    'bsf_spring_cup',
    'fs_manager',
    'www.bsf.bg',
    'figure-skating/spring-cup',
    'club',
    'club',
    '{"representation":{"primary":"club","store_nation":true,"store_club":true},"source_family":"bsf_discovered"}'::jsonb
  ),
  (
    'bsf_ice_blade_adult_open_cup',
    'fs_manager',
    'www.bsf.bg',
    'figure-skating/ice-blade-adult-open-cup',
    'club',
    'club',
    '{"representation":{"primary":"club","store_nation":true,"store_club":true},"source_family":"bsf_discovered"}'::jsonb
  ),
  (
    'bsf_ice_peak_trophy',
    'fs_manager',
    'www.bsf.bg',
    'figure-skating/ice-peak-trophy',
    'club',
    'club',
    '{"representation":{"primary":"club","store_nation":true,"store_club":true},"source_family":"bsf_discovered"}'::jsonb
  ),
  (
    'bsf_test_mlad_figurist',
    'pdf_result',
    'www.bsf.bg',
    'figure-skating/test-mf',
    'club_test',
    'club',
    '{"representation":{"primary":"club","store_nation":true,"store_club":true},"source_family":"bsf_discovered","result_kind":"mlad_figurist_pdf"}'::jsonb
  )
ON CONFLICT (profile_key) DO UPDATE SET
  parser_profile = EXCLUDED.parser_profile,
  host = EXCLUDED.host,
  event_path_prefix = EXCLUDED.event_path_prefix,
  competition_stream = EXCLUDED.competition_stream,
  representation_primary = EXCLUDED.representation_primary,
  settings = EXCLUDED.settings,
  updated_at = now();
