-- Make mart.fact_segment_score.segment_score_key unique across score sources.
--
-- analytics.v_skater_segment_scores unions PDF score summaries (pss.id) with
-- HTML segment-result fallbacks (sr.id). Both base tables have independent
-- serial sequences, so their ids collide, causing /api/scores/{key} to return
-- the wrong skater's elements/judge marks when an HTML row and a PDF row
-- happen to share the same numeric id.
--
-- Encode score_source into the key by negating HTML rows. PDF rows keep their
-- original positive id, which preserves joins from mart.fact_element_score,
-- mart.fact_element_judge_mark, and mart.fact_component_score (those facts
-- only carry PDF data, so a negative key naturally returns zero children for
-- HTML-only fallback rows).

CREATE OR REPLACE VIEW mart.fact_segment_score AS
SELECT
  CASE
    WHEN sss.score_source = 'html_segment_result' THEN -sss.score_id
    ELSE sss.score_id
  END AS segment_score_key,
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

REFRESH MATERIALIZED VIEW api.mv_dashboard_overview;
REFRESH MATERIALIZED VIEW api.mv_competition_mix;
REFRESH MATERIALIZED VIEW api.mv_club_dashboard;
REFRESH MATERIALIZED VIEW api.mv_nationality_dashboard;
