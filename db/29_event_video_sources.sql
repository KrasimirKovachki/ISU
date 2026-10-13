-- Video/broadcast evidence layer.
--
-- Broadcast URLs are intentionally modeled separately from competition result
-- URLs. A single event can have several day/region streams, and transcripts
-- can later produce skater-level timecode evidence.

CREATE SCHEMA IF NOT EXISTS ingest;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS ingest.event_video_sources (
  id bigserial PRIMARY KEY,
  event_id bigint REFERENCES ingest.events(id) ON DELETE SET NULL,
  import_run_id bigint REFERENCES ingest.import_runs(id) ON DELETE SET NULL,
  root_result_url text,
  event_name text NOT NULL,
  competition_date date,
  stream_day int,
  region_scope text NOT NULL DEFAULT 'unknown',
  source_platform text NOT NULL,
  source_page_url text,
  posted_url_text text NOT NULL,
  video_url text,
  url_status text NOT NULL DEFAULT 'partial',
  access_model text NOT NULL DEFAULT 'unknown',
  download_status text NOT NULL DEFAULT 'not_started',
  local_video_path text,
  downloaded_at timestamptz,
  video_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  transcript_status text NOT NULL DEFAULT 'not_started',
  notes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (region_scope IN ('bulgaria', 'international', 'unknown')),
  CHECK (url_status IN ('partial', 'resolved', 'unavailable', 'failed')),
  CHECK (access_model IN ('free', 'paid', 'geo_blocked', 'unknown')),
  CHECK (download_status IN ('not_started', 'downloaded', 'blocked', 'failed', 'skipped')),
  CHECK (transcript_status IN ('not_started', 'queued', 'completed', 'failed', 'skipped'))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_event_video_sources_identity_v2
  ON ingest.event_video_sources (
    event_name,
    competition_date,
    stream_day,
    region_scope
  );

CREATE INDEX IF NOT EXISTS idx_event_video_sources_event
  ON ingest.event_video_sources(event_id);

CREATE INDEX IF NOT EXISTS idx_event_video_sources_download
  ON ingest.event_video_sources(download_status, transcript_status);

CREATE TABLE IF NOT EXISTS ingest.event_video_result_links (
  id bigserial PRIMARY KEY,
  video_source_id bigint NOT NULL REFERENCES ingest.event_video_sources(id) ON DELETE CASCADE,
  import_run_id bigint REFERENCES ingest.import_runs(id) ON DELETE SET NULL,
  event_id bigint REFERENCES ingest.events(id) ON DELETE SET NULL,
  root_result_url text NOT NULL,
  link_role text NOT NULL DEFAULT 'broadcast_covers_result_stream',
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (video_source_id, root_result_url),
  CHECK (link_role IN ('broadcast_covers_result_stream', 'primary_result_stream'))
);

CREATE INDEX IF NOT EXISTS idx_event_video_result_links_import
  ON ingest.event_video_result_links(import_run_id);

CREATE TABLE IF NOT EXISTS ingest.event_video_transcripts (
  id bigserial PRIMARY KEY,
  video_source_id bigint NOT NULL REFERENCES ingest.event_video_sources(id) ON DELETE CASCADE,
  language_code text,
  transcript_format text NOT NULL DEFAULT 'plain_text',
  transcript_text text NOT NULL,
  generated_by text,
  started_at timestamptz,
  completed_at timestamptz,
  raw jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_event_video_transcripts_identity
  ON ingest.event_video_transcripts (
    video_source_id,
    coalesce(language_code, ''),
    transcript_format
  );

CREATE TABLE IF NOT EXISTS ingest.event_video_skater_timecodes (
  id bigserial PRIMARY KEY,
  video_source_id bigint NOT NULL REFERENCES ingest.event_video_sources(id) ON DELETE CASCADE,
  transcript_id bigint REFERENCES ingest.event_video_transcripts(id) ON DELETE SET NULL,
  segment_id bigint REFERENCES ingest.segments(id) ON DELETE SET NULL,
  skater_appearance_id bigint REFERENCES ingest.source_skater_appearances(id) ON DELETE SET NULL,
  raw_name text NOT NULL,
  normalized_name text NOT NULL,
  start_time_seconds numeric(12, 3),
  end_time_seconds numeric(12, 3),
  confidence numeric(5, 4),
  evidence_kind text NOT NULL DEFAULT 'transcript_match',
  transcript_excerpt text,
  raw jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (evidence_kind IN ('manual', 'transcript_match', 'schedule_alignment', 'ocr', 'audio_match'))
);

CREATE INDEX IF NOT EXISTS idx_event_video_timecodes_name
  ON ingest.event_video_skater_timecodes(normalized_name);

CREATE INDEX IF NOT EXISTS idx_event_video_timecodes_appearance
  ON ingest.event_video_skater_timecodes(skater_appearance_id)
  WHERE skater_appearance_id IS NOT NULL;

DROP VIEW IF EXISTS analytics.v_skater_video_timecodes;
DROP VIEW IF EXISTS analytics.v_event_video_sources;

CREATE OR REPLACE VIEW analytics.v_event_video_sources AS
SELECT
  evs.id,
  evs.event_name,
  evs.competition_date,
  evs.stream_day,
  evs.region_scope,
  evs.source_platform,
  evs.video_url,
  evs.posted_url_text,
  evs.url_status,
  evs.access_model,
  evs.download_status,
  evs.transcript_status,
  evs.local_video_path,
  evs.root_result_url,
  result_links.result_urls,
  result_links.import_run_ids,
  ir.id AS import_run_id,
  ir.status AS import_run_status,
  e.id AS event_id,
  e.name AS imported_event_name,
  e.date_range AS imported_date_range,
  evs.video_metadata,
  evs.notes,
  evs.created_at,
  evs.updated_at
FROM ingest.event_video_sources evs
LEFT JOIN LATERAL (
  SELECT
    jsonb_agg(evrl.root_result_url ORDER BY evrl.root_result_url) AS result_urls,
    jsonb_agg(evrl.import_run_id ORDER BY evrl.root_result_url) FILTER (WHERE evrl.import_run_id IS NOT NULL) AS import_run_ids
  FROM ingest.event_video_result_links evrl
  WHERE evrl.video_source_id = evs.id
) result_links ON true
LEFT JOIN ingest.import_runs ir ON ir.id = evs.import_run_id
LEFT JOIN ingest.events e ON e.id = evs.event_id;

CREATE OR REPLACE VIEW analytics.v_skater_video_timecodes AS
SELECT
  tc.id,
  tc.video_source_id,
  evs.event_name,
  evs.competition_date,
  evs.region_scope,
  evs.video_url,
  tc.raw_name,
  tc.normalized_name,
  tc.start_time_seconds,
  tc.end_time_seconds,
  tc.confidence,
  tc.evidence_kind,
  tc.transcript_excerpt,
  s.name AS segment_name,
  c.name AS category_name,
  ssa.nation_code,
  ssa.club_name
FROM ingest.event_video_skater_timecodes tc
JOIN ingest.event_video_sources evs ON evs.id = tc.video_source_id
LEFT JOIN ingest.segments s ON s.id = tc.segment_id
LEFT JOIN ingest.categories c ON c.id = s.category_id
LEFT JOIN ingest.source_skater_appearances ssa ON ssa.id = tc.skater_appearance_id;
