-- Dashboard performance materialized views.
--
-- Two slow paths are addressed here:
--
-- 1. /api/dashboard nationality rollup was 20+ seconds because the CTE
--    re-aggregated 73k rows from analytics.v_skater_appearances_with_club
--    and called api.skater_identity_key() per row. Pre-aggregate it once
--    in api.mv_nationality_dashboard with an indexed lookup by nation.
--
-- 2. /api/scores/{key} was 1-3 seconds because mart.fact_* are deep view
--    stacks (UNION ALL of pdf/html, nation inference, club join). Each
--    sub-query in score_detail re-evaluated the whole stack. Snapshot the
--    fact tables into api.mv_* with indexes on the lookup columns.
--
-- All MVs in this file should be refreshed after a fresh import or after
-- a rebuild of the analytics/mart layer.

CREATE SCHEMA IF NOT EXISTS api;

DROP MATERIALIZED VIEW IF EXISTS api.mv_nationality_dashboard CASCADE;
CREATE MATERIALIZED VIEW api.mv_nationality_dashboard AS
WITH nationality_appearances AS (
  SELECT DISTINCT
    sac.nation_code,
    sac.event_id,
    sac.event_start_date,
    api.skater_identity_key(
      sac.skater_name,
      sac.nation_code,
      COALESCE(sac.canonical_club_name, sac.club_name)
    ) AS identity_key,
    sac.category_name ILIKE 'Mlad Figurist%' AS is_development
  FROM analytics.v_skater_appearances_with_club sac
  WHERE sac.nation_code IS NOT NULL
),
nationality_identity_flags AS (
  SELECT
    nation_code,
    identity_key,
    bool_or(is_development) AS has_development_event,
    bool_or(NOT is_development) AS has_standard_event
  FROM nationality_appearances
  GROUP BY nation_code, identity_key
),
nationality_scores AS (
  SELECT
    nation_code,
    count(*) AS segment_scores,
    max(tss) AS best_tss,
    max(tes) AS best_tes,
    max(pcs) AS best_pcs
  FROM mart.fact_segment_score
  WHERE nation_code IS NOT NULL
  GROUP BY nation_code
),
nationality_events AS (
  SELECT
    nation_code,
    count(DISTINCT event_id) AS events,
    min(event_start_date) AS first_seen_date,
    max(event_start_date) AS latest_seen_date
  FROM nationality_appearances
  GROUP BY nation_code
)
SELECT
  nif.nation_code,
  count(DISTINCT nif.identity_key) AS skaters,
  COALESCE(max(ne.events), 0) AS events,
  COALESCE(max(ns.segment_scores), 0) AS segment_scores,
  count(*) FILTER (WHERE nif.has_development_event) AS development_skaters,
  count(*) FILTER (WHERE nif.has_development_event AND NOT nif.has_standard_event) AS development_only_skaters,
  count(*) FILTER (WHERE nif.has_development_event AND nif.has_standard_event) AS converted_development_skaters,
  max(ns.best_tss) AS best_tss,
  max(ns.best_tes) AS best_tes,
  max(ns.best_pcs) AS best_pcs,
  min(ne.first_seen_date) AS first_seen_date,
  max(ne.latest_seen_date) AS latest_seen_date
FROM nationality_identity_flags nif
LEFT JOIN nationality_events ne ON ne.nation_code = nif.nation_code
LEFT JOIN nationality_scores ns ON ns.nation_code = nif.nation_code
GROUP BY nif.nation_code;

CREATE UNIQUE INDEX idx_mv_nationality_dashboard_code
  ON api.mv_nationality_dashboard(nation_code);
CREATE INDEX idx_mv_nationality_dashboard_segment_scores
  ON api.mv_nationality_dashboard(segment_scores DESC, skaters DESC);


DROP MATERIALIZED VIEW IF EXISTS api.mv_segment_score CASCADE;
CREATE MATERIALIZED VIEW api.mv_segment_score AS
SELECT * FROM mart.fact_segment_score;

CREATE UNIQUE INDEX idx_mv_segment_score_pk
  ON api.mv_segment_score(segment_score_key);
CREATE INDEX idx_mv_segment_score_event
  ON api.mv_segment_score(event_key);
CREATE INDEX idx_mv_segment_score_skater
  ON api.mv_segment_score(skater_key);
CREATE INDEX idx_mv_segment_score_identity
  ON api.mv_segment_score(skater_name, nation_code, club_name);
CREATE INDEX idx_mv_segment_score_date
  ON api.mv_segment_score(date_key);
-- Expression index so JOINs that compute api.skater_identity_key on the fly
-- (e.g. /api/skaters/{key}/scores) get an index seek instead of a sequential
-- scan with a function call per row.
CREATE INDEX idx_mv_segment_score_identity_key
  ON api.mv_segment_score(api.skater_identity_key(skater_name, nation_code, club_name));


DROP MATERIALIZED VIEW IF EXISTS api.mv_element_score CASCADE;
CREATE MATERIALIZED VIEW api.mv_element_score AS
SELECT * FROM mart.fact_element_score;

CREATE INDEX idx_mv_element_score_segment
  ON api.mv_element_score(segment_score_key);
CREATE INDEX idx_mv_element_score_skater
  ON api.mv_element_score(skater_key);
CREATE INDEX idx_mv_element_score_identity
  ON api.mv_element_score(skater_name, nation_code, club_name);
CREATE INDEX idx_mv_element_score_date
  ON api.mv_element_score(date_key);
CREATE INDEX idx_mv_element_score_identity_key
  ON api.mv_element_score(api.skater_identity_key(skater_name, nation_code, club_name));


DROP MATERIALIZED VIEW IF EXISTS api.mv_element_judge_mark CASCADE;
CREATE MATERIALIZED VIEW api.mv_element_judge_mark AS
SELECT * FROM mart.fact_element_judge_mark;

CREATE INDEX idx_mv_element_judge_mark_segment
  ON api.mv_element_judge_mark(segment_score_key);


DROP MATERIALIZED VIEW IF EXISTS api.mv_component_score CASCADE;
CREATE MATERIALIZED VIEW api.mv_component_score AS
SELECT * FROM mart.fact_component_score;

CREATE INDEX idx_mv_component_score_segment
  ON api.mv_component_score(segment_score_key);


DROP MATERIALIZED VIEW IF EXISTS api.mv_competition_result CASCADE;
CREATE MATERIALIZED VIEW api.mv_competition_result AS
SELECT * FROM mart.fact_competition_result;

CREATE INDEX idx_mv_competition_result_event
  ON api.mv_competition_result(event_key);


-- The dim_event / dim_category / dim_segment views are deep view stacks
-- (analytics.v_events → ingest.events ⋈ ingest.import_runs etc., with
-- DISTINCT ON / sort steps). They have only a few hundred rows each but the
-- planner re-executes them per JOIN, so /api/skaters/{key}/scores spent ~1s
-- joining 50 rows × dim_*. Snapshotting them fixes that without changing
-- semantics — refresh after every import sweep.

DROP MATERIALIZED VIEW IF EXISTS api.mv_dim_event CASCADE;
CREATE MATERIALIZED VIEW api.mv_dim_event AS
SELECT * FROM mart.dim_event;

CREATE UNIQUE INDEX idx_mv_dim_event_pk ON api.mv_dim_event(event_key);
CREATE INDEX idx_mv_dim_event_start ON api.mv_dim_event(event_start_date DESC NULLS LAST);


DROP MATERIALIZED VIEW IF EXISTS api.mv_dim_category CASCADE;
CREATE MATERIALIZED VIEW api.mv_dim_category AS
SELECT * FROM mart.dim_category;

CREATE UNIQUE INDEX idx_mv_dim_category_pk ON api.mv_dim_category(category_key);


DROP MATERIALIZED VIEW IF EXISTS api.mv_dim_segment CASCADE;
CREATE MATERIALIZED VIEW api.mv_dim_segment AS
SELECT * FROM mart.dim_segment;

CREATE UNIQUE INDEX idx_mv_dim_segment_pk ON api.mv_dim_segment(segment_key);
