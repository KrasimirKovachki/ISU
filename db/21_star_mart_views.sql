-- STAR-style analytics mart.
--
-- This layer is intentionally derived from ingest.* and analytics.*. It gives
-- BI tools, future API endpoints, and coach dashboards stable dimensions/facts
-- without replacing the raw source tables.

CREATE SCHEMA IF NOT EXISTS mart;

-- Keep this lightweight dependency here so mart views can be rebuilt before or
-- after the archive loader has run.
CREATE TABLE IF NOT EXISTS ingest.source_archive_files (
  source_document_id bigint PRIMARY KEY REFERENCES ingest.source_documents(id) ON DELETE CASCADE,
  import_run_id bigint NOT NULL REFERENCES ingest.import_runs(id) ON DELETE CASCADE,
  root_url text NOT NULL,
  url text NOT NULL,
  document_type text,
  parser_profile text,
  content_hash text,
  archive_path text,
  archive_sha256 text,
  archive_status text NOT NULL,
  archive_reason text,
  parse_status text,
  fetched_at timestamptz,
  loaded_at timestamptz NOT NULL DEFAULT now()
);

DROP VIEW IF EXISTS mart.v_skater_source_evidence CASCADE;
DROP VIEW IF EXISTS mart.v_skater_personal_bests CASCADE;
DROP VIEW IF EXISTS mart.fact_component_score CASCADE;
DROP VIEW IF EXISTS mart.fact_element_judge_mark CASCADE;
DROP VIEW IF EXISTS mart.fact_element_score CASCADE;
DROP VIEW IF EXISTS mart.fact_segment_score CASCADE;
DROP VIEW IF EXISTS mart.fact_competition_result CASCADE;
DROP VIEW IF EXISTS mart.dim_source_document CASCADE;
DROP VIEW IF EXISTS mart.dim_official CASCADE;
DROP VIEW IF EXISTS mart.dim_element CASCADE;
DROP VIEW IF EXISTS mart.dim_club CASCADE;
DROP VIEW IF EXISTS mart.dim_country CASCADE;
DROP VIEW IF EXISTS mart.dim_segment CASCADE;
DROP VIEW IF EXISTS mart.dim_category CASCADE;
DROP VIEW IF EXISTS mart.dim_event CASCADE;
DROP VIEW IF EXISTS mart.dim_skater CASCADE;
DROP VIEW IF EXISTS mart.dim_date CASCADE;

CREATE OR REPLACE VIEW mart.dim_date AS
SELECT DISTINCT
  event_start_date AS date_key,
  extract(year FROM event_start_date)::int AS year,
  extract(month FROM event_start_date)::int AS month,
  extract(day FROM event_start_date)::int AS day,
  CASE
    WHEN extract(month FROM event_start_date)::int >= 7
      THEN concat(extract(year FROM event_start_date)::int, '/', extract(year FROM event_start_date)::int + 1)
    ELSE concat(extract(year FROM event_start_date)::int - 1, '/', extract(year FROM event_start_date)::int)
  END AS skating_season
FROM analytics.v_events
WHERE event_start_date IS NOT NULL;

CREATE OR REPLACE VIEW mart.dim_skater AS
SELECT
  row_number() OVER (ORDER BY skater_name, nation_code, club_name) AS skater_key,
  skater_name,
  nation_code,
  club_name,
  count(DISTINCT skater_appearance_id) AS source_appearances,
  min(event_start_date) AS first_seen_date,
  max(event_start_date) AS latest_seen_date
FROM analytics.v_skater_appearances_clean
WHERE skater_name IS NOT NULL
GROUP BY skater_name, nation_code, club_name;

CREATE OR REPLACE VIEW mart.dim_event AS
SELECT
  event_id AS event_key,
  import_run_id,
  root_url,
  profile_key,
  parser_profile,
  competition_stream,
  representation_primary,
  event_type,
  competition_level,
  event_name,
  location,
  venue,
  date_range,
  event_start_date,
  event_protocol_url
FROM analytics.v_events;

CREATE OR REPLACE VIEW mart.dim_category AS
SELECT DISTINCT
  category_id AS category_key,
  event_id AS event_key,
  category_name
FROM analytics.v_skater_appearances_clean
WHERE category_id IS NOT NULL;

CREATE OR REPLACE VIEW mart.dim_segment AS
SELECT DISTINCT
  segment_id AS segment_key,
  category_id AS category_key,
  segment_name
FROM analytics.v_skater_appearances_clean
WHERE segment_id IS NOT NULL;

CREATE OR REPLACE VIEW mart.dim_country AS
SELECT DISTINCT
  nation_code AS country_key,
  nation_code
FROM analytics.v_skater_appearances_clean
WHERE nation_code IS NOT NULL;

CREATE OR REPLACE VIEW mart.dim_club AS
SELECT
  club_id AS club_key,
  canonical_name,
  country_code,
  status,
  array_agg(alias_name ORDER BY alias_name) AS aliases,
  sum(imported_appearance_count) AS imported_appearance_count,
  sum(event_count) AS event_count
FROM core.v_club_alias_usage
GROUP BY club_id, canonical_name, country_code, status;

CREATE OR REPLACE VIEW mart.dim_element AS
SELECT
  row_number() OVER (ORDER BY base_element_code, element_code) AS element_key,
  base_element_code,
  element_code,
  count(*) AS observed_count
FROM ingest.pdf_elements
GROUP BY base_element_code, element_code;

CREATE OR REPLACE VIEW mart.dim_official AS
SELECT
  row_number() OVER (ORDER BY name, nation_code, role_group) AS official_key,
  name,
  nation_code,
  role_group,
  count(*) AS assignments
FROM ingest.official_assignments
GROUP BY name, nation_code, role_group;

CREATE OR REPLACE VIEW mart.dim_source_document AS
SELECT
  sd.id AS source_document_key,
  sd.import_run_id,
  ir.root_url,
  sd.url,
  sd.document_type,
  sd.parser_profile,
  sd.content_hash,
  sd.fetched_at,
  sd.parse_status,
  sd.metadata,
  saf.archive_path,
  saf.archive_sha256,
  saf.archive_status,
  saf.archive_reason
FROM ingest.source_documents sd
JOIN ingest.import_runs ir ON ir.id = sd.import_run_id
LEFT JOIN ingest.source_archive_files saf ON saf.source_document_id = sd.id;

CREATE OR REPLACE VIEW mart.fact_competition_result AS
SELECT
  scr.skater_appearance_id AS competition_result_key,
  scr.event_id AS event_key,
  scr.category_id AS category_key,
  ds.skater_key,
  scr.event_start_date AS date_key,
  scr.skater_name,
  scr.nation_code,
  scr.club_name,
  scr.final_place,
  scr.points,
  scr.segment_places,
  scr.raw
FROM analytics.v_skater_category_results scr
LEFT JOIN mart.dim_skater ds
  ON ds.skater_name = scr.skater_name
 AND ds.nation_code IS NOT DISTINCT FROM scr.nation_code
 AND ds.club_name IS NOT DISTINCT FROM scr.club_name;

CREATE OR REPLACE VIEW mart.fact_segment_score AS
SELECT
  sss.score_id AS segment_score_key,
  sss.score_source,
  sss.skater_appearance_id,
  sss.event_id AS event_key,
  sss.category_id AS category_key,
  sss.segment_id AS segment_key,
  ds.skater_key,
  sss.event_start_date AS date_key,
  sss.skater_name,
  sss.nation_code,
  sss.club_name,
  sss.place,
  sss.tss,
  sss.score_total,
  sss.tes,
  sss.pcs,
  sss.deduction,
  sss.base_value_total,
  sss.element_score_total,
  sss.judge_count,
  sss.raw
FROM analytics.v_skater_segment_scores sss
LEFT JOIN mart.dim_skater ds
  ON ds.skater_name = sss.skater_name
 AND ds.nation_code IS NOT DISTINCT FROM sss.nation_code
 AND ds.club_name IS NOT DISTINCT FROM sss.club_name;

CREATE OR REPLACE VIEW mart.fact_element_score AS
SELECT
  se.element_id AS element_score_key,
  se.score_id AS segment_score_key,
  se.skater_appearance_id,
  se.event_id AS event_key,
  se.category_name,
  se.segment_name,
  ds.skater_key,
  de.element_key,
  se.event_start_date AS date_key,
  se.skater_name,
  se.nation_code,
  se.club_name,
  se.element_no,
  se.element_code,
  se.base_element_code,
  se.raw_element,
  se.info,
  se.markers,
  se.base_value,
  se.goe,
  se.bonus,
  se.panel_score,
  se.raw
FROM analytics.v_skater_elements se
LEFT JOIN mart.dim_skater ds
  ON ds.skater_name = se.skater_name
 AND ds.nation_code IS NOT DISTINCT FROM se.nation_code
 AND ds.club_name IS NOT DISTINCT FROM se.club_name
LEFT JOIN mart.dim_element de
  ON de.base_element_code IS NOT DISTINCT FROM se.base_element_code
 AND de.element_code = se.element_code;

CREATE OR REPLACE VIEW mart.fact_element_judge_mark AS
SELECT
  pejs.id AS element_judge_mark_key,
  pe.id AS element_score_key,
  pss.id AS segment_score_key,
  sac.event_id AS event_key,
  sac.category_id AS category_key,
  sac.segment_id AS segment_key,
  ds.skater_key,
  pejs.official_assignment_id,
  pejs.judge_number,
  pejs.score
FROM ingest.pdf_element_judge_scores pejs
JOIN ingest.pdf_elements pe ON pe.id = pejs.pdf_element_id
JOIN ingest.pdf_score_summaries pss ON pss.id = pe.pdf_score_summary_id
JOIN analytics.v_skater_appearances_clean sac ON sac.skater_appearance_id = pss.skater_appearance_id
LEFT JOIN mart.dim_skater ds
  ON ds.skater_name = sac.skater_name
 AND ds.nation_code IS NOT DISTINCT FROM sac.nation_code
 AND ds.club_name IS NOT DISTINCT FROM sac.club_name;

CREATE OR REPLACE VIEW mart.fact_component_score AS
SELECT
  ppc.id AS component_score_key,
  pss.id AS segment_score_key,
  sac.event_id AS event_key,
  sac.category_id AS category_key,
  sac.segment_id AS segment_key,
  ds.skater_key,
  ppc.component_name,
  ppc.factor,
  ppc.score,
  ppc.raw
FROM ingest.pdf_program_components ppc
JOIN ingest.pdf_score_summaries pss ON pss.id = ppc.pdf_score_summary_id
JOIN analytics.v_skater_appearances_clean sac ON sac.skater_appearance_id = pss.skater_appearance_id
LEFT JOIN mart.dim_skater ds
  ON ds.skater_name = sac.skater_name
 AND ds.nation_code IS NOT DISTINCT FROM sac.nation_code
 AND ds.club_name IS NOT DISTINCT FROM sac.club_name;

CREATE OR REPLACE VIEW mart.v_skater_personal_bests AS
SELECT
  skater_key,
  skater_name,
  nation_code,
  club_name,
  max(tss) AS best_tss,
  max(tes) AS best_tes,
  max(pcs) AS best_pcs,
  max(score_total) AS best_score_total,
  min(date_key) AS first_score_date,
  max(date_key) AS latest_score_date,
  count(DISTINCT event_key) AS competitions
FROM mart.fact_segment_score
GROUP BY skater_key, skater_name, nation_code, club_name;

CREATE OR REPLACE VIEW mart.v_skater_source_evidence AS
SELECT
  sac.skater_name,
  sac.nation_code,
  sac.club_name,
  sac.event_name,
  sac.event_start_date,
  sac.category_name,
  sac.segment_name,
  sac.appearance_type,
  sac.skater_appearance_id,
  sd.id AS source_document_key,
  sd.url AS source_url,
  sd.document_type,
  sd.content_hash,
  saf.archive_path,
  saf.archive_sha256,
  saf.archive_status,
  sd.metadata,
  ir.root_url,
  ir.id AS import_run_id
FROM analytics.v_skater_appearances_clean sac
JOIN ingest.source_skater_appearances ssa ON ssa.id = sac.skater_appearance_id
LEFT JOIN ingest.source_documents sd ON sd.id = ssa.source_document_id
LEFT JOIN ingest.source_archive_files saf ON saf.source_document_id = sd.id
LEFT JOIN ingest.import_runs ir ON ir.id = sac.import_run_id;
