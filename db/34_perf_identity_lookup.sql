-- Edited 2026-10-12 by Claude (AI assistant): performance, stage 1.
--
-- 1. api.mv_identity_lookup: "which skater is this" (api.skater_identity_key) computed ONCE per distinct
--    name + nation + club (about 10k rows) instead of per score row on every request. The year-filtered
--    dashboard queries took 18 s because they called the function for ~57k rows, three times.
--    NULL nation / club are stored as '∅' so the lookup has a plain unique key (needed for REFRESH CONCURRENTLY).
-- 2. Expression indexes on the score views use the same '∅' keys, so skater pages find their rows by index.
--    The old indexes on api.skater_identity_key(...) are dropped: they recomputed the function for every
--    score row (~250k calls) on each refresh and nothing reads through them any more.
-- 3. Unique indexes on the large views so scripts/refresh_api_mvs.sh can refresh them CONCURRENTLY
--    (readers are no longer blocked while results are imported).

DROP MATERIALIZED VIEW IF EXISTS api.mv_identity_lookup;
CREATE MATERIALIZED VIEW api.mv_identity_lookup AS
SELECT
  t.skater_name,
  COALESCE(t.nation_code, '∅') AS nk,
  COALESCE(t.club_name, '∅')   AS ck,
  api.skater_identity_key(t.skater_name, t.nation_code, t.club_name) AS identity_key
FROM (
  SELECT skater_name, nation_code, club_name FROM api.mv_segment_score
  UNION
  SELECT skater_name, nation_code, club_name FROM api.mv_element_score
  UNION
  SELECT skater_name, nation_code, COALESCE(canonical_club_name, club_name) FROM api.mv_skater_appearances_with_club
) t
WHERE t.skater_name IS NOT NULL;

CREATE UNIQUE INDEX idx_mv_identity_lookup_pk ON api.mv_identity_lookup(skater_name, nk, ck);
CREATE INDEX idx_mv_identity_lookup_identity ON api.mv_identity_lookup(identity_key);

CREATE INDEX IF NOT EXISTS idx_mv_segment_score_lookup
  ON api.mv_segment_score(skater_name, COALESCE(nation_code, '∅'), COALESCE(club_name, '∅'));
CREATE INDEX IF NOT EXISTS idx_mv_element_score_lookup
  ON api.mv_element_score(skater_name, COALESCE(nation_code, '∅'), COALESCE(club_name, '∅'));
DROP INDEX IF EXISTS api.idx_mv_segment_score_identity_key;
DROP INDEX IF EXISTS api.idx_mv_element_score_identity_key;

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_element_score_key ON api.mv_element_score(element_score_key);
CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_element_judge_mark_key ON api.mv_element_judge_mark(element_judge_mark_key);
CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_component_score_key ON api.mv_component_score(component_score_key);
CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_competition_result_key ON api.mv_competition_result(competition_result_key);
CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_appearances_key ON api.mv_skater_appearances_with_club(skater_appearance_id);

ANALYZE api.mv_identity_lookup;
