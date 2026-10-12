-- Edited 2026-10-12 by Claude (AI assistant): refresh every api.mv_* view after an import.
-- CONCURRENTLY = readers keep the old data until the new data is ready (no more "site hangs during import").
-- Order follows the dependencies: base copies → identity lookup → appearance/score identity → skater search →
-- club status → club/nationality/mix/overview summaries (db/35: these read the copies, seconds each).
-- Small views (one row / a few rows) are refreshed plainly; it takes milliseconds.
\timing on
REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_dim_event;
REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_dim_category;
REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_dim_segment;
REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_segment_score;
REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_element_score;
REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_element_judge_mark;
REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_component_score;
REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_competition_result;
REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_skater_appearances_with_club;
REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_identity_lookup;
REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_appearance_identity;
REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_score_identity;
REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_skater_search;
REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_club_skater_status;
REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_club_dashboard;
REFRESH MATERIALIZED VIEW CONCURRENTLY api.mv_nationality_dashboard;
REFRESH MATERIALIZED VIEW api.mv_competition_mix;
REFRESH MATERIALIZED VIEW api.mv_dashboard_overview;  -- one row: plain refresh (milliseconds)
ANALYZE api.mv_segment_score;
ANALYZE api.mv_element_score;
ANALYZE api.mv_skater_search;
ANALYZE api.mv_nationality_dashboard;
ANALYZE api.mv_club_dashboard;
ANALYZE api.mv_appearance_identity;
ANALYZE api.mv_score_identity;
-- last: the API uses this table's analyze time as the "data changed" signal for its Insights cache
ANALYZE api.mv_identity_lookup;
