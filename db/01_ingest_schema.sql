-- Run as the application owner after db/00_create_database_and_user.sql:
-- db/psql_local.sh -f db/01_ingest_schema.sql

CREATE SCHEMA IF NOT EXISTS ingest;
CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS ingest.source_profiles (
  id bigserial PRIMARY KEY,
  profile_key text NOT NULL UNIQUE,
  parser_profile text NOT NULL,
  host text,
  event_path_prefix text,
  competition_stream text,
  representation_primary text NOT NULL DEFAULT 'nation',
  settings jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ingest.import_runs (
  id bigserial PRIMARY KEY,
  source_profile_id bigint REFERENCES ingest.source_profiles(id),
  root_url text NOT NULL,
  status text NOT NULL DEFAULT 'started',
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  error_message text,
  stats jsonb NOT NULL DEFAULT '{}'::jsonb,
  CHECK (status IN ('started', 'completed', 'failed'))
);

CREATE TABLE IF NOT EXISTS ingest.source_documents (
  id bigserial PRIMARY KEY,
  import_run_id bigint NOT NULL REFERENCES ingest.import_runs(id) ON DELETE CASCADE,
  source_profile_id bigint REFERENCES ingest.source_profiles(id),
  url text NOT NULL,
  document_type text NOT NULL,
  parser_profile text NOT NULL,
  content_hash text,
  fetched_at timestamptz NOT NULL DEFAULT now(),
  parse_status text NOT NULL DEFAULT 'pending',
  raw_text text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (import_run_id, url),
  CHECK (parse_status IN ('pending', 'parsed', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_source_documents_url ON ingest.source_documents(url);
CREATE INDEX IF NOT EXISTS idx_source_documents_hash ON ingest.source_documents(content_hash);

CREATE TABLE IF NOT EXISTS ingest.parse_issues (
  id bigserial PRIMARY KEY,
  import_run_id bigint NOT NULL REFERENCES ingest.import_runs(id) ON DELETE CASCADE,
  source_document_id bigint REFERENCES ingest.source_documents(id) ON DELETE CASCADE,
  level text NOT NULL,
  code text NOT NULL,
  message text NOT NULL,
  context jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (level IN ('info', 'warning', 'error'))
);

CREATE TABLE IF NOT EXISTS ingest.events (
  id bigserial PRIMARY KEY,
  import_run_id bigint NOT NULL REFERENCES ingest.import_runs(id) ON DELETE CASCADE,
  source_profile_id bigint REFERENCES ingest.source_profiles(id),
  source_url text NOT NULL,
  source_context jsonb NOT NULL DEFAULT '{}'::jsonb,
  name text NOT NULL,
  location text,
  venue text,
  date_range text,
  event_protocol_url text,
  raw jsonb NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (import_run_id, source_url)
);

CREATE TABLE IF NOT EXISTS ingest.categories (
  id bigserial PRIMARY KEY,
  event_id bigint NOT NULL REFERENCES ingest.events(id) ON DELETE CASCADE,
  name text NOT NULL,
  entries_url text,
  result_url text,
  source_order int,
  raw jsonb NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (event_id, name)
);

CREATE TABLE IF NOT EXISTS ingest.segments (
  id bigserial PRIMARY KEY,
  category_id bigint NOT NULL REFERENCES ingest.categories(id) ON DELETE CASCADE,
  name text NOT NULL,
  officials_url text,
  details_url text,
  judges_scores_pdf_url text,
  source_order int,
  raw jsonb NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (category_id, name)
);

CREATE TABLE IF NOT EXISTS ingest.schedule_items (
  id bigserial PRIMARY KEY,
  event_id bigint NOT NULL REFERENCES ingest.events(id) ON DELETE CASCADE,
  category_name text NOT NULL,
  segment_name text NOT NULL,
  source_date text,
  source_time text,
  segment_details_url text,
  source_order int,
  raw jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS ingest.source_skater_appearances (
  id bigserial PRIMARY KEY,
  import_run_id bigint NOT NULL REFERENCES ingest.import_runs(id) ON DELETE CASCADE,
  event_id bigint NOT NULL REFERENCES ingest.events(id) ON DELETE CASCADE,
  category_id bigint REFERENCES ingest.categories(id) ON DELETE CASCADE,
  segment_id bigint REFERENCES ingest.segments(id) ON DELETE CASCADE,
  source_document_id bigint REFERENCES ingest.source_documents(id) ON DELETE SET NULL,
  source_row_order int,
  raw_name text NOT NULL,
  normalized_name text NOT NULL,
  nation_code text,
  club_name text,
  representation_type text,
  representation_value text,
  bio_url text,
  source_skater_id text,
  appearance_type text NOT NULL,
  raw jsonb NOT NULL DEFAULT '{}'::jsonb,
  CHECK (appearance_type IN ('entry', 'category_result', 'segment_result', 'pdf_score'))
);

CREATE INDEX IF NOT EXISTS idx_skater_appearances_name_nation
  ON ingest.source_skater_appearances(normalized_name, nation_code);
CREATE INDEX IF NOT EXISTS idx_skater_appearances_name_club
  ON ingest.source_skater_appearances(normalized_name, club_name);
CREATE INDEX IF NOT EXISTS idx_skater_appearances_source_id
  ON ingest.source_skater_appearances(source_skater_id)
  WHERE source_skater_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS ingest.category_results (
  id bigserial PRIMARY KEY,
  category_id bigint NOT NULL REFERENCES ingest.categories(id) ON DELETE CASCADE,
  skater_appearance_id bigint REFERENCES ingest.source_skater_appearances(id) ON DELETE SET NULL,
  final_place int,
  points numeric(10, 3),
  segment_places jsonb NOT NULL DEFAULT '{}'::jsonb,
  source_order int,
  raw jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS ingest.segment_results (
  id bigserial PRIMARY KEY,
  segment_id bigint NOT NULL REFERENCES ingest.segments(id) ON DELETE CASCADE,
  skater_appearance_id bigint REFERENCES ingest.source_skater_appearances(id) ON DELETE SET NULL,
  place int,
  tss numeric(10, 3),
  tes numeric(10, 3),
  pcs numeric(10, 3),
  deduction numeric(10, 3),
  starting_number int,
  components jsonb NOT NULL DEFAULT '{}'::jsonb,
  source_order int,
  raw jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS ingest.official_assignments (
  id bigserial PRIMARY KEY,
  segment_id bigint NOT NULL REFERENCES ingest.segments(id) ON DELETE CASCADE,
  source_document_id bigint REFERENCES ingest.source_documents(id) ON DELETE SET NULL,
  function_text text NOT NULL,
  role_group text NOT NULL,
  judge_number int,
  name text NOT NULL,
  nation_code text,
  source_order int,
  raw jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_official_assignments_source_identity
  ON ingest.official_assignments(segment_id, function_text, name, coalesce(nation_code, ''));

CREATE INDEX IF NOT EXISTS idx_official_assignments_segment_judge
  ON ingest.official_assignments(segment_id, judge_number)
  WHERE judge_number IS NOT NULL;

CREATE TABLE IF NOT EXISTS ingest.pdf_score_summaries (
  id bigserial PRIMARY KEY,
  segment_id bigint NOT NULL REFERENCES ingest.segments(id) ON DELETE CASCADE,
  source_document_id bigint REFERENCES ingest.source_documents(id) ON DELETE SET NULL,
  skater_appearance_id bigint REFERENCES ingest.source_skater_appearances(id) ON DELETE SET NULL,
  rank int,
  starting_number int,
  total_segment_score numeric(10, 3),
  total_element_score numeric(10, 3),
  total_program_component_score numeric(10, 3),
  total_deductions numeric(10, 3),
  printed_at text,
  judge_count int,
  base_value_total numeric(10, 3),
  element_score_total numeric(10, 3),
  deductions_detail jsonb NOT NULL DEFAULT '{}'::jsonb,
  raw jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS ingest.pdf_elements (
  id bigserial PRIMARY KEY,
  pdf_score_summary_id bigint NOT NULL REFERENCES ingest.pdf_score_summaries(id) ON DELETE CASCADE,
  element_no int NOT NULL,
  element_code text NOT NULL,
  base_element_code text,
  raw_element text NOT NULL,
  info text,
  markers text[] NOT NULL DEFAULT ARRAY[]::text[],
  base_value numeric(10, 3),
  goe numeric(10, 3),
  bonus numeric(10, 3),
  panel_score numeric(10, 3),
  raw jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS ingest.pdf_element_judge_scores (
  id bigserial PRIMARY KEY,
  pdf_element_id bigint NOT NULL REFERENCES ingest.pdf_elements(id) ON DELETE CASCADE,
  judge_number int NOT NULL,
  official_assignment_id bigint REFERENCES ingest.official_assignments(id) ON DELETE SET NULL,
  score int,
  UNIQUE (pdf_element_id, judge_number)
);

CREATE TABLE IF NOT EXISTS ingest.pdf_program_components (
  id bigserial PRIMARY KEY,
  pdf_score_summary_id bigint NOT NULL REFERENCES ingest.pdf_score_summaries(id) ON DELETE CASCADE,
  component_name text NOT NULL,
  factor numeric(10, 3),
  score numeric(10, 3),
  raw jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS ingest.pdf_component_judge_scores (
  id bigserial PRIMARY KEY,
  pdf_program_component_id bigint NOT NULL REFERENCES ingest.pdf_program_components(id) ON DELETE CASCADE,
  judge_number int NOT NULL,
  official_assignment_id bigint REFERENCES ingest.official_assignments(id) ON DELETE SET NULL,
  score numeric(10, 3),
  UNIQUE (pdf_program_component_id, judge_number)
);

CREATE TABLE IF NOT EXISTS core.skaters (
  id bigserial PRIMARY KEY,
  display_name text NOT NULL,
  normalized_name text NOT NULL,
  primary_nation_code text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS core.skater_identity_matches (
  id bigserial PRIMARY KEY,
  skater_id bigint NOT NULL REFERENCES core.skaters(id) ON DELETE CASCADE,
  source_skater_appearance_id bigint NOT NULL REFERENCES ingest.source_skater_appearances(id) ON DELETE CASCADE,
  match_method text NOT NULL,
  confidence numeric(5, 4),
  confirmed_by_user_id bigint,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (source_skater_appearance_id)
);
