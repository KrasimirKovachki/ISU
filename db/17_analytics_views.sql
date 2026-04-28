-- Reusable BI/analytics layer.
--
-- Keep ingest.* as raw/source-truth tables. Consumers such as reports, API,
-- dashboards, clubs, and coaches should read from analytics.* views.

CREATE SCHEMA IF NOT EXISTS analytics;

DROP VIEW IF EXISTS analytics.v_data_quality_representation CASCADE;
DROP VIEW IF EXISTS analytics.v_club_skater_summary CASCADE;
DROP VIEW IF EXISTS analytics.v_skater_progression CASCADE;
DROP VIEW IF EXISTS analytics.v_skater_elements CASCADE;
DROP VIEW IF EXISTS analytics.v_skater_segment_scores CASCADE;
DROP VIEW IF EXISTS analytics.v_skater_category_results CASCADE;
DROP VIEW IF EXISTS analytics.v_skater_appearances_clean CASCADE;
DROP VIEW IF EXISTS analytics.v_events CASCADE;
DROP VIEW IF EXISTS analytics.v_latest_import_runs CASCADE;

CREATE OR REPLACE VIEW analytics.v_latest_import_runs AS
SELECT DISTINCT ON (ir.root_url)
  ir.id AS import_run_id,
  ir.source_profile_id,
  ir.root_url,
  ir.status,
  ir.started_at,
  ir.finished_at,
  ir.stats,
  sp.profile_key,
  sp.parser_profile,
  sp.competition_stream,
  sp.representation_primary,
  sp.settings->>'event_type' AS event_type,
  sp.settings->>'competition_level' AS competition_level,
  sp.settings AS source_profile_settings
FROM ingest.import_runs ir
LEFT JOIN ingest.source_profiles sp ON sp.id = ir.source_profile_id
WHERE ir.status = 'completed'
ORDER BY ir.root_url, ir.id DESC;

CREATE OR REPLACE VIEW analytics.v_events AS
WITH event_source AS (
  SELECT
    e.id AS event_id,
    lir.import_run_id,
    lir.root_url,
    lir.profile_key,
    lir.parser_profile,
    lir.competition_stream,
    lir.representation_primary,
    lir.event_type,
    lir.competition_level,
    e.name AS event_name,
    e.location,
    e.venue,
    e.date_range,
    COALESCE(
      regexp_match(split_part(e.date_range, ' - ', 1), '^(\d{1,2})[./](\d{1,2})[./](\d{4})$'),
      regexp_match(e.date_range, '^(\d{1,2})[./](\d{1,2})[./](\d{4})'),
      regexp_match(e.date_range, '^(\d{1,2})[./](\d{1,2})\s*-\s*\d{1,2}[./]\d{1,2}[./](\d{4})$')
    ) AS start_date_parts,
    e.event_protocol_url,
    e.raw
  FROM ingest.events e
  JOIN analytics.v_latest_import_runs lir ON lir.import_run_id = e.import_run_id
)
SELECT
  event_id,
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
  CASE
    WHEN start_date_parts IS NULL THEN NULL
    WHEN start_date_parts[1]::integer BETWEEN 1 AND 12
      AND start_date_parts[2]::integer BETWEEN 13 AND 31
      THEN make_date(start_date_parts[3]::integer, start_date_parts[1]::integer, start_date_parts[2]::integer)
    WHEN start_date_parts[1]::integer BETWEEN 1 AND 31
      AND start_date_parts[2]::integer BETWEEN 1 AND 12
      THEN make_date(start_date_parts[3]::integer, start_date_parts[2]::integer, start_date_parts[1]::integer)
    ELSE NULL
  END AS event_start_date,
  event_protocol_url,
  raw
FROM event_source;

CREATE OR REPLACE VIEW analytics.v_skater_appearances_clean AS
SELECT
  ssa.id AS skater_appearance_id,
  ve.import_run_id,
  ve.event_id,
  ve.root_url,
  ve.profile_key,
  ve.parser_profile,
  ve.competition_stream,
  ve.representation_primary,
  ve.event_type,
  ve.competition_level,
  ve.event_name,
  ve.event_start_date,
  ve.date_range,
  c.id AS category_id,
  c.name AS category_name,
  s.id AS segment_id,
  s.name AS segment_name,
  ssa.appearance_type,
  ssa.source_row_order,
  ssa.raw_name,
  ssa.normalized_name AS skater_name,
  NULLIF(ssa.nation_code, '') AS nation_code,
  NULLIF(ssa.club_name, '') AS club_name,
  ssa.representation_type,
  NULLIF(ssa.representation_value, '') AS representation_value,
  COALESCE(NULLIF(ssa.club_name, ''), NULLIF(ssa.nation_code, '')) AS display_affiliation,
  ssa.bio_url,
  ssa.source_skater_id,
  ssa.raw
FROM ingest.source_skater_appearances ssa
JOIN analytics.v_events ve ON ve.event_id = ssa.event_id
LEFT JOIN ingest.categories c ON c.id = ssa.category_id
LEFT JOIN ingest.segments s ON s.id = ssa.segment_id
WHERE ssa.normalized_name <> '';

CREATE OR REPLACE VIEW analytics.v_skater_category_results AS
SELECT
  sac.skater_appearance_id,
  sac.import_run_id,
  sac.event_id,
  sac.root_url,
  sac.event_name,
  sac.event_start_date,
  sac.date_range,
  sac.event_type,
  sac.competition_level,
  sac.competition_stream,
  sac.category_id,
  sac.category_name,
  sac.skater_name,
  sac.nation_code,
  sac.club_name,
  sac.display_affiliation,
  cr.final_place,
  cr.points,
  cr.segment_places,
  cr.raw
FROM ingest.category_results cr
JOIN analytics.v_skater_appearances_clean sac ON sac.skater_appearance_id = cr.skater_appearance_id;

CREATE OR REPLACE VIEW analytics.v_skater_segment_scores AS
SELECT
  pss.id AS score_id,
  'pdf_score_summary'::text AS score_source,
  sac.skater_appearance_id,
  sac.import_run_id,
  sac.event_id,
  sac.root_url,
  sac.event_name,
  sac.event_start_date,
  sac.date_range,
  sac.event_type,
  sac.competition_level,
  sac.competition_stream,
  sac.category_id,
  sac.category_name,
  sac.segment_id,
  sac.segment_name,
  sac.skater_name,
  sac.nation_code,
  sac.club_name,
  sac.display_affiliation,
  pss.rank AS place,
  pss.total_segment_score AS tss,
  pss.total_segment_score AS score_total,
  pss.total_element_score AS tes,
  pss.total_program_component_score AS pcs,
  pss.total_deductions AS deduction,
  pss.base_value_total,
  pss.element_score_total,
  pss.judge_count,
  pss.raw
FROM ingest.pdf_score_summaries pss
JOIN analytics.v_skater_appearances_clean sac ON sac.skater_appearance_id = pss.skater_appearance_id
UNION ALL
SELECT
  sr.id AS score_id,
  'html_segment_result'::text AS score_source,
  sac.skater_appearance_id,
  sac.import_run_id,
  sac.event_id,
  sac.root_url,
  sac.event_name,
  sac.event_start_date,
  sac.date_range,
  sac.event_type,
  sac.competition_level,
  sac.competition_stream,
  sac.category_id,
  sac.category_name,
  sac.segment_id,
  sac.segment_name,
  sac.skater_name,
  sac.nation_code,
  sac.club_name,
  sac.display_affiliation,
  sr.place,
  sr.tss,
  COALESCE(sr.tss, sr.tes) AS score_total,
  sr.tes,
  sr.pcs,
  sr.deduction,
  NULL::numeric AS base_value_total,
  NULL::numeric AS element_score_total,
  NULL::integer AS judge_count,
  sr.raw
FROM ingest.segment_results sr
JOIN analytics.v_skater_appearances_clean sac ON sac.skater_appearance_id = sr.skater_appearance_id
WHERE NOT EXISTS (
  SELECT 1
  FROM ingest.pdf_score_summaries pss
  JOIN analytics.v_skater_appearances_clean pdf_sac ON pdf_sac.skater_appearance_id = pss.skater_appearance_id
  WHERE pss.segment_id = sr.segment_id
    AND pdf_sac.skater_name = sac.skater_name
    AND (
      NULLIF(pdf_sac.source_skater_id, '') IS NULL
      OR NULLIF(sac.source_skater_id, '') IS NULL
      OR pdf_sac.source_skater_id = sac.source_skater_id
    )
);

CREATE OR REPLACE VIEW analytics.v_skater_elements AS
SELECT
  pe.id AS element_id,
  pss.id AS score_id,
  sac.skater_appearance_id,
  sac.import_run_id,
  sac.event_id,
  sac.root_url,
  sac.event_name,
  sac.event_start_date,
  sac.date_range,
  sac.event_type,
  sac.competition_level,
  sac.competition_stream,
  sac.category_name,
  sac.segment_name,
  sac.skater_name,
  sac.nation_code,
  sac.club_name,
  sac.display_affiliation,
  pe.element_no,
  pe.element_code,
  pe.base_element_code,
  pe.raw_element,
  pe.info,
  pe.markers,
  pe.base_value,
  pe.goe,
  pe.bonus,
  pe.panel_score,
  pe.raw
FROM ingest.pdf_elements pe
JOIN ingest.pdf_score_summaries pss ON pss.id = pe.pdf_score_summary_id
JOIN analytics.v_skater_appearances_clean sac ON sac.skater_appearance_id = pss.skater_appearance_id;

CREATE OR REPLACE VIEW analytics.v_skater_progression AS
SELECT
  sss.*,
  lag(sss.tss) OVER (
    PARTITION BY sss.skater_name, sss.nation_code, sss.club_name, sss.segment_name
    ORDER BY sss.event_start_date NULLS LAST, sss.event_name, sss.category_name
  ) AS previous_tss_same_segment,
  sss.tss - lag(sss.tss) OVER (
    PARTITION BY sss.skater_name, sss.nation_code, sss.club_name, sss.segment_name
    ORDER BY sss.event_start_date NULLS LAST, sss.event_name, sss.category_name
  ) AS tss_delta_same_segment,
  lag(sss.tes) OVER (
    PARTITION BY sss.skater_name, sss.nation_code, sss.club_name, sss.segment_name
    ORDER BY sss.event_start_date NULLS LAST, sss.event_name, sss.category_name
  ) AS previous_tes_same_segment,
  sss.tes - lag(sss.tes) OVER (
    PARTITION BY sss.skater_name, sss.nation_code, sss.club_name, sss.segment_name
    ORDER BY sss.event_start_date NULLS LAST, sss.event_name, sss.category_name
  ) AS tes_delta_same_segment,
  lag(sss.pcs) OVER (
    PARTITION BY sss.skater_name, sss.nation_code, sss.club_name, sss.segment_name
    ORDER BY sss.event_start_date NULLS LAST, sss.event_name, sss.category_name
  ) AS previous_pcs_same_segment,
  sss.pcs - lag(sss.pcs) OVER (
    PARTITION BY sss.skater_name, sss.nation_code, sss.club_name, sss.segment_name
    ORDER BY sss.event_start_date NULLS LAST, sss.event_name, sss.category_name
  ) AS pcs_delta_same_segment
FROM analytics.v_skater_segment_scores sss;

CREATE OR REPLACE VIEW analytics.v_club_skater_summary AS
SELECT
  COALESCE(club_name, '(empty club)') AS club_name,
  skater_name,
  nation_code,
  count(DISTINCT event_id) AS competitions,
  count(DISTINCT category_name) AS categories_seen,
  min(event_start_date) AS first_event_date,
  max(event_start_date) AS latest_event_date,
  max(tss) AS best_tss,
  max(score_total) AS best_score_total,
  max(tes) AS best_tes,
  max(pcs) AS best_pcs,
  array_agg(DISTINCT event_type ORDER BY event_type) AS event_types_seen
FROM analytics.v_skater_segment_scores
WHERE skater_name IS NOT NULL
GROUP BY COALESCE(club_name, '(empty club)'), skater_name, nation_code;

CREATE OR REPLACE VIEW analytics.v_data_quality_representation AS
SELECT
  event_type,
  competition_level,
  representation_primary,
  competition_stream,
  count(*) AS appearances,
  count(*) FILTER (WHERE nation_code IS NOT NULL) AS with_country,
  count(*) FILTER (WHERE club_name IS NOT NULL) AS with_club,
  count(*) FILTER (WHERE nation_code IS NOT NULL AND nation_code !~ '^[A-Z]{3}$') AS non_iso_nation_codes,
  count(*) FILTER (WHERE representation_primary = 'club' AND club_name IS NULL) AS club_primary_empty_club
FROM analytics.v_skater_appearances_clean
GROUP BY event_type, competition_level, representation_primary, competition_stream;
