#!/usr/bin/env bash
# Refresh every api.mv_* materialized view + ANALYZE.
#
# Run after a fresh import or after re-importing existing events.
# Forgetting one of these (notably api.mv_element_score) causes the
# dashboard to show "0 elements" on score detail pages even though the
# underlying ingest tables are populated.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_DIR}"

"${REPO_DIR}/db/psql_local.sh" -v ON_ERROR_STOP=1 <<'SQL'
\timing on
REFRESH MATERIALIZED VIEW api.mv_skater_search;
REFRESH MATERIALIZED VIEW api.mv_dashboard_overview;
REFRESH MATERIALIZED VIEW api.mv_competition_mix;
REFRESH MATERIALIZED VIEW api.mv_club_skater_status;
REFRESH MATERIALIZED VIEW api.mv_club_dashboard;
REFRESH MATERIALIZED VIEW api.mv_segment_score;
REFRESH MATERIALIZED VIEW api.mv_element_score;
REFRESH MATERIALIZED VIEW api.mv_element_judge_mark;
REFRESH MATERIALIZED VIEW api.mv_component_score;
REFRESH MATERIALIZED VIEW api.mv_competition_result;
REFRESH MATERIALIZED VIEW api.mv_nationality_dashboard;
REFRESH MATERIALIZED VIEW api.mv_dim_event;
REFRESH MATERIALIZED VIEW api.mv_dim_category;
REFRESH MATERIALIZED VIEW api.mv_dim_segment;
REFRESH MATERIALIZED VIEW api.mv_skater_appearances_with_club;
ANALYZE api.mv_segment_score;
ANALYZE api.mv_element_score;
ANALYZE api.mv_skater_search;
ANALYZE api.mv_nationality_dashboard;
ANALYZE api.mv_club_dashboard;
SQL
