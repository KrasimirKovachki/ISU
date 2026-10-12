#!/usr/bin/env bash
# Refresh every api.mv_* materialized view + ANALYZE (SQL in db/refresh_api_mvs.sql).
#
# Run after a fresh import or after re-importing existing events.
# Forgetting one of these (notably api.mv_element_score) causes the
# dashboard to show "0 elements" on score detail pages even though the
# underlying ingest tables are populated.
#
# Edited 2026-10-12 by Claude (AI assistant): CONCURRENTLY + dependency order + api.mv_identity_lookup.
# On the Skating Database server (130.110.71.83) run it as:
#   docker exec -i skating-db psql -U skating_app -d skating_data -v ON_ERROR_STOP=1 < db/refresh_api_mvs.sql

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_DIR}"

"${REPO_DIR}/db/psql_local.sh" -v ON_ERROR_STOP=1 < "${REPO_DIR}/db/refresh_api_mvs.sql"
