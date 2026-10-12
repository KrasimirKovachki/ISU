-- Edited 2026-10-12 by Claude (AI assistant): performance, stage 2.
--
-- The six summary views (skater search, club status, club dashboard, competition mix, overview, nationalities)
-- took 1–1.5 minutes EACH to refresh because they
--   a) re-read the slow source views (analytics.v_skater_appearances_with_club, mart.*) although fresh copies
--      (api.mv_skater_appearances_with_club, api.mv_segment_score, …) are refreshed just before them, and
--   b) called api.skater_identity_key()/normalized_* for every row (~105k appearances + 22k scores, several times).
-- Now they read the copies and take name → identity from api.mv_identity_lookup (computed once per distinct
-- name + nation + club). Same columns, same results (checked with EXCEPT both ways before the swap), same indexes.
-- Skater keys do not change: skater_key is still derived from identity_key.
--
-- Refresh order (db/refresh_api_mvs.sql): copies → mv_identity_lookup → mv_skater_search → club status →
-- club dashboard / nationality / mix / overview.

-- 1. identity lookup + the cleaned display name and nation used by skater search
DROP MATERIALIZED VIEW IF EXISTS api.mv_identity_lookup CASCADE;
CREATE MATERIALIZED VIEW api.mv_identity_lookup AS
SELECT
  t.skater_name,
  COALESCE(t.nation_code, '∅') AS nk,
  COALESCE(t.club_name, '∅')   AS ck,
  api.skater_identity_key(t.skater_name, t.nation_code, t.club_name) AS identity_key,
  api.normalized_skater_name(t.skater_name, t.club_name)             AS cleaned_name,
  api.normalized_skater_nation_code(t.skater_name, t.nation_code)    AS api_nation
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
ANALYZE api.mv_identity_lookup;

-- every appearance / segment score with its skater identity: later joins are plain id joins that the planner
-- estimates correctly (joining on name + nation + club made it expect 1 row instead of ~22k and pick nested loops)
DROP MATERIALIZED VIEW IF EXISTS api.mv_appearance_identity CASCADE;
CREATE MATERIALIZED VIEW api.mv_appearance_identity AS
SELECT sac.skater_appearance_id, il.identity_key
FROM api.mv_skater_appearances_with_club sac
JOIN api.mv_identity_lookup il
  ON il.skater_name = sac.skater_name AND il.nk = COALESCE(sac.nation_code, '∅')
 AND il.ck = COALESCE(sac.canonical_club_name, sac.club_name, '∅');
CREATE UNIQUE INDEX idx_mv_appearance_identity_pk ON api.mv_appearance_identity(skater_appearance_id);
CREATE INDEX idx_mv_appearance_identity_identity ON api.mv_appearance_identity(identity_key);
ANALYZE api.mv_appearance_identity;

DROP MATERIALIZED VIEW IF EXISTS api.mv_score_identity CASCADE;
CREATE MATERIALIZED VIEW api.mv_score_identity AS
SELECT fss.segment_score_key, fss.event_key, il.identity_key
FROM api.mv_segment_score fss
JOIN api.mv_identity_lookup il
  ON il.skater_name = fss.skater_name AND il.nk = COALESCE(fss.nation_code, '∅') AND il.ck = COALESCE(fss.club_name, '∅');
CREATE UNIQUE INDEX idx_mv_score_identity_pk ON api.mv_score_identity(segment_score_key);
CREATE INDEX idx_mv_score_identity_event ON api.mv_score_identity(event_key, identity_key);
CREATE INDEX idx_mv_score_identity_identity ON api.mv_score_identity(identity_key);
ANALYZE api.mv_score_identity;

-- 2. new versions next to the old ones
DROP MATERIALIZED VIEW IF EXISTS api.mv_skater_search_new CASCADE;
CREATE MATERIALIZED VIEW api.mv_skater_search_new AS
WITH appearances AS (
  SELECT
    sac.event_id,
    sac.event_start_date,
    sac.skater_name,
    COALESCE(sac.canonical_club_name, sac.club_name) AS api_club_name,
    il.api_nation   AS api_nation_code,
    il.cleaned_name AS cleaned_skater_name,
    il.identity_key
  FROM api.mv_skater_appearances_with_club sac
  JOIN api.mv_identity_lookup il
    ON il.skater_name = sac.skater_name AND il.nk = COALESCE(sac.nation_code, '∅')
   AND il.ck = COALESCE(sac.canonical_club_name, sac.club_name, '∅')
  WHERE sac.skater_name IS NOT NULL
),
identity_base AS (
  SELECT
    identity_key,
    (array_agg(cleaned_skater_name ORDER BY event_start_date DESC NULLS LAST, length(cleaned_skater_name), cleaned_skater_name))[1] AS skater_name,
    max(api_nation_code) AS nation_code,
    (array_agg(api_club_name ORDER BY api_club_name) FILTER (WHERE api_club_name IS NOT NULL))[1] AS club_name,
    string_agg(DISTINCT skater_name, ' ') AS raw_names_text,
    count(*) AS source_appearances,
    count(DISTINCT event_id) AS competitions,
    min(event_start_date) AS first_seen_date,
    max(event_start_date) AS latest_seen_date
  FROM appearances
  GROUP BY identity_key
),
score_base AS (
  SELECT
    il.identity_key,
    max(fss.tss) AS best_tss,
    max(fss.tes) AS best_tes,
    max(fss.pcs) AS best_pcs,
    max(fss.score_total) AS best_score_total,
    count(DISTINCT fss.event_key) AS scored_competitions
  FROM api.mv_segment_score fss
  JOIN api.mv_identity_lookup il
    ON il.skater_name = fss.skater_name AND il.nk = COALESCE(fss.nation_code, '∅') AND il.ck = COALESCE(fss.club_name, '∅')
  WHERE fss.skater_name IS NOT NULL
  GROUP BY il.identity_key
)
SELECT
  (mod(abs(hashtextextended(ib.identity_key, 0)::bigint), 1000000000) + 1)::int AS skater_key,
  ib.identity_key,
  ib.skater_name,
  ib.nation_code,
  ib.club_name,
  ib.source_appearances,
  ib.competitions,
  COALESCE(sb.scored_competitions, 0) AS scored_competitions,
  ib.first_seen_date,
  ib.latest_seen_date,
  sb.best_tss,
  sb.best_tes,
  sb.best_pcs,
  sb.best_score_total,
  lower(concat_ws(' ', ib.skater_name, ib.raw_names_text, ib.nation_code, ib.club_name, replace(ib.identity_key, '|', ' '))) AS search_text
FROM identity_base ib
LEFT JOIN score_base sb ON sb.identity_key = ib.identity_key;
CREATE UNIQUE INDEX idx_mv_skater_search_new_identity ON api.mv_skater_search_new(identity_key);
ANALYZE api.mv_skater_search_new;

DROP MATERIALIZED VIEW IF EXISTS api.mv_club_skater_status_new CASCADE;
CREATE MATERIALIZED VIEW api.mv_club_skater_status_new AS
WITH club_identities AS (
  SELECT DISTINCT
    COALESCE(sac.canonical_club_name, sac.club_name) AS club_name,
    mss_1.skater_key,
    mss_1.identity_key,
    CASE
      WHEN sac.category_name ILIKE 'Mlad Figurist%%' THEN 'development'
      WHEN de.event_name ILIKE 'Ice Peak Trophy%%' THEN 'recreational'
      ELSE 'national'
    END AS competition_kind
  FROM api.mv_skater_appearances_with_club sac
  JOIN api.mv_dim_event de ON de.event_key = sac.event_id
  JOIN api.mv_appearance_identity il ON il.skater_appearance_id = sac.skater_appearance_id
  JOIN api.mv_skater_search_new mss_1 ON mss_1.identity_key = il.identity_key
  WHERE sac.club_name IS NOT NULL
),
club_identity_flags AS (
  SELECT
    club_name, skater_key, identity_key,
    bool_or(competition_kind = 'national')     AS has_national_event,
    bool_or(competition_kind = 'recreational') AS has_recreational_event,
    bool_or(competition_kind = 'development')  AS has_development_event
  FROM club_identities
  GROUP BY club_name, skater_key, identity_key
)
SELECT
  cif.club_name,
  mss.skater_key,
  mss.identity_key,
  mss.skater_name,
  mss.nation_code,
  mss.source_appearances,
  mss.first_seen_date,
  mss.latest_seen_date,
  mss.best_tss,
  mss.best_tes,
  mss.best_pcs,
  mss.competitions,
  mss.scored_competitions,
  cif.has_national_event,
  cif.has_recreational_event,
  cif.has_development_event,
  CASE
    WHEN cif.has_national_event AND cif.has_recreational_event THEN 'mixed'
    WHEN cif.has_national_event THEN 'national_competitor'
    WHEN cif.has_recreational_event THEN 'recreational'
    ELSE 'development'
  END AS competitor_status
FROM club_identity_flags cif
JOIN api.mv_skater_search_new mss ON mss.skater_key = cif.skater_key;
ANALYZE api.mv_club_skater_status_new;

DROP MATERIALIZED VIEW IF EXISTS api.mv_club_dashboard_new CASCADE;
CREATE MATERIALIZED VIEW api.mv_club_dashboard_new AS
WITH club_appearances AS (
  SELECT DISTINCT
    COALESCE(sac.canonical_club_name, sac.club_name) AS club_name,
    sac.canonical_club_country_code AS registered_country_code,
    sac.nation_code AS appearance_nation_code,
    sac.event_id,
    sac.event_start_date,
    mss.skater_key,
    mss.identity_key,
    CASE
      WHEN de.competition_stream IN ('ISU', 'ISUCS') THEN 'international'
      WHEN de.competition_stream IN ('FRP', 'BSF', 'DP') OR de.competition_level = 'federated' THEN 'federation'
      WHEN de.competition_stream IN ('club', 'club_amateur', 'NonISU') THEN 'club_or_open'
      ELSE 'unknown'
    END AS scope_hint
  FROM api.mv_skater_appearances_with_club sac
  JOIN api.mv_dim_event de ON de.event_key = sac.event_id
  JOIN api.mv_appearance_identity il ON il.skater_appearance_id = sac.skater_appearance_id
  JOIN api.mv_skater_search_new mss ON mss.identity_key = il.identity_key
  WHERE sac.club_name IS NOT NULL
),
score_identity AS (
  SELECT si0.event_key, si0.identity_key, fss.segment_score_key, fss.tss, fss.tes, fss.pcs
  FROM api.mv_score_identity si0
  JOIN api.mv_segment_score fss ON fss.segment_score_key = si0.segment_score_key
),
club_scores AS (
  SELECT ca_1.club_name, si.segment_score_key, si.tss, si.tes, si.pcs
  FROM club_appearances ca_1
  JOIN score_identity si ON si.event_key = ca_1.event_id AND si.identity_key = ca_1.identity_key
),
club_score_totals AS (
  -- per club first (the old version joined every appearance with every score of the club, then de-duplicated)
  SELECT club_name, count(DISTINCT segment_score_key) AS segment_scores,
         max(tss) AS best_tss, max(tes) AS best_tes, max(pcs) AS best_pcs
  FROM club_scores
  GROUP BY club_name
),
club_alias_text AS (
  SELECT c.canonical_name AS club_name, string_agg(ca_1.alias_name, ' ' ORDER BY ca_1.alias_name) AS aliases_text
  FROM core.clubs c
  JOIN core.club_aliases ca_1 ON ca_1.club_id = c.id
  GROUP BY c.canonical_name
),
club_country_guess AS (
  SELECT ranked.club_name, ranked.nation_code AS inferred_country_code
  FROM (
    SELECT club_name, appearance_nation_code AS nation_code, count(*) AS appearances,
           row_number() OVER (PARTITION BY club_name ORDER BY count(*) DESC, appearance_nation_code) AS rank
    FROM club_appearances
    WHERE appearance_nation_code IS NOT NULL
    GROUP BY club_name, appearance_nation_code
  ) ranked
  WHERE ranked.rank = 1
),
club_status_counts AS (
  SELECT
    club_name,
    count(*) FILTER (WHERE has_national_event AND NOT has_recreational_event) AS national_competitors,
    count(*) FILTER (WHERE NOT has_national_event AND has_recreational_event) AS recreational_only_skaters,
    count(*) FILTER (WHERE has_national_event AND has_recreational_event) AS mixed_competitors,
    count(*) FILTER (WHERE has_development_event) AS development_skaters,
    count(*) FILTER (WHERE has_development_event AND NOT has_national_event AND NOT has_recreational_event) AS development_only_skaters,
    count(*) FILTER (WHERE has_development_event AND (has_national_event OR has_recreational_event)) AS converted_development_skaters
  FROM api.mv_club_skater_status_new
  GROUP BY club_name
)
SELECT
  ca.club_name,
  COALESCE(max(ca.registered_country_code), ccg.inferred_country_code) AS country_code,
  count(DISTINCT ca.skater_key) AS skaters,
  count(DISTINCT ca.event_id) AS events,
  COALESCE(max(cs.segment_scores), 0) AS segment_scores,
  count(DISTINCT ca.event_id) FILTER (WHERE ca.scope_hint = 'international') AS international_events,
  count(DISTINCT ca.event_id) FILTER (WHERE ca.scope_hint = 'federation') AS federation_events,
  count(DISTINCT ca.event_id) FILTER (WHERE ca.scope_hint = 'club_or_open') AS club_or_open_events,
  COALESCE(max(csc.national_competitors), 0) AS national_competitors,
  COALESCE(max(csc.recreational_only_skaters), 0) AS recreational_only_skaters,
  COALESCE(max(csc.mixed_competitors), 0) AS mixed_competitors,
  COALESCE(max(csc.development_skaters), 0) AS development_skaters,
  COALESCE(max(csc.development_only_skaters), 0) AS development_only_skaters,
  COALESCE(max(csc.converted_development_skaters), 0) AS converted_development_skaters,
  max(cs.best_tss) AS best_tss,
  max(cs.best_tes) AS best_tes,
  max(cs.best_pcs) AS best_pcs,
  min(ca.event_start_date) AS first_seen_date,
  max(ca.event_start_date) AS latest_seen_date,
  lower(concat_ws(' ', ca.club_name, cat.aliases_text)) AS search_text
FROM club_appearances ca
LEFT JOIN club_score_totals cs ON cs.club_name = ca.club_name
LEFT JOIN club_alias_text cat ON cat.club_name = ca.club_name
LEFT JOIN club_country_guess ccg ON ccg.club_name = ca.club_name
LEFT JOIN club_status_counts csc ON csc.club_name = ca.club_name
GROUP BY ca.club_name, ccg.inferred_country_code, cat.aliases_text;

DROP MATERIALIZED VIEW IF EXISTS api.mv_competition_mix_new CASCADE;
CREATE MATERIALIZED VIEW api.mv_competition_mix_new AS
WITH events AS (
  SELECT de.*,
    CASE
      WHEN de.competition_stream IN ('ISU', 'ISUCS') THEN 'international'
      WHEN de.competition_stream IN ('FRP', 'BSF', 'DP') OR de.competition_level = 'federated' THEN 'federation'
      WHEN de.competition_stream IN ('club', 'club_amateur', 'NonISU') THEN 'club_or_open'
      ELSE 'unknown'
    END AS scope_hint
  FROM api.mv_dim_event de
),
appearance_event AS (
  SELECT
    sac.event_id,
    count(DISTINCT COALESCE(sac.canonical_club_name, sac.club_name)) FILTER (WHERE sac.club_name IS NOT NULL) AS clubs_with_appearances,
    count(DISTINCT concat_ws('|', il.identity_key)) AS skaters_with_appearances
  FROM api.mv_skater_appearances_with_club sac
  JOIN api.mv_appearance_identity il ON il.skater_appearance_id = sac.skater_appearance_id
  WHERE sac.skater_name IS NOT NULL
  GROUP BY sac.event_id
),
score_event AS (
  SELECT
    fss.event_key,
    count(DISTINCT mss.skater_key) FILTER (WHERE mss.skater_key IS NOT NULL) AS skaters_with_scores,
    count(fss.segment_score_key) AS segment_scores
  FROM api.mv_segment_score fss
  LEFT JOIN api.mv_score_identity il ON il.segment_score_key = fss.segment_score_key
  LEFT JOIN api.mv_skater_search_new mss ON mss.identity_key = il.identity_key
  GROUP BY fss.event_key
)
SELECT
  COALESCE(e.competition_stream, 'unknown') AS competition_stream,
  COALESCE(e.competition_level, 'unknown') AS competition_level,
  COALESCE(e.event_type, 'unknown') AS event_type,
  COALESCE(e.representation_primary, 'unknown') AS representation_primary,
  e.scope_hint,
  count(DISTINCT e.event_key) AS events,
  sum(COALESCE(se.skaters_with_scores, 0)) AS skaters_with_scores,
  sum(COALESCE(ae.clubs_with_appearances, 0)) AS clubs_with_appearances,
  sum(COALESCE(ae.skaters_with_appearances, 0)) AS skaters_with_appearances,
  sum(COALESCE(se.segment_scores, 0)) AS segment_scores,
  min(e.event_start_date) AS first_event_date,
  max(e.event_start_date) AS latest_event_date
FROM events e
LEFT JOIN appearance_event ae ON ae.event_id = e.event_key
LEFT JOIN score_event se ON se.event_key = e.event_key
GROUP BY 1, 2, 3, 4, 5;

DROP MATERIALIZED VIEW IF EXISTS api.mv_dashboard_overview_new CASCADE;
CREATE MATERIALIZED VIEW api.mv_dashboard_overview_new AS
SELECT
  (SELECT count(*) FROM api.mv_dim_event) AS events,
  (SELECT count(*) FROM api.mv_skater_search_new) AS skaters,
  (SELECT count(*) FROM mart.dim_club) AS registered_clubs,
  (SELECT count(DISTINCT COALESCE(canonical_club_name, club_name))
     FROM api.mv_skater_appearances_with_club WHERE club_name IS NOT NULL) AS scoring_clubs,
  (SELECT count(*) FROM api.mv_segment_score) AS segment_scores,
  (SELECT count(*) FROM api.mv_element_score) AS element_scores,
  (SELECT count(DISTINCT il.identity_key)
     FROM api.mv_skater_appearances_with_club sac
     JOIN api.mv_appearance_identity il ON il.skater_appearance_id = sac.skater_appearance_id
     WHERE sac.category_name ILIKE 'Mlad Figurist%%') AS development_skaters,
  (SELECT min(event_start_date) FROM api.mv_dim_event) AS first_event_date,
  (SELECT max(event_start_date) FROM api.mv_dim_event) AS latest_event_date,
  (SELECT count(*) FROM api.mv_dim_event WHERE competition_stream IN ('ISU', 'ISUCS')) AS international_events,
  (SELECT count(*) FROM api.mv_dim_event
     WHERE competition_stream IN ('FRP', 'BSF', 'DP') OR competition_level = 'federated') AS federation_events,
  (SELECT count(*) FROM api.mv_dim_event WHERE competition_stream IN ('club', 'club_amateur', 'NonISU')) AS club_or_open_events;

DROP MATERIALIZED VIEW IF EXISTS api.mv_nationality_dashboard_new CASCADE;
CREATE MATERIALIZED VIEW api.mv_nationality_dashboard_new AS
WITH nationality_appearances AS (
  SELECT DISTINCT
    sac.nation_code,
    sac.event_id,
    sac.event_start_date,
    il.identity_key,
    sac.category_name ILIKE 'Mlad Figurist%' AS is_development
  FROM api.mv_skater_appearances_with_club sac
  JOIN api.mv_appearance_identity il ON il.skater_appearance_id = sac.skater_appearance_id
  WHERE sac.nation_code IS NOT NULL
),
nationality_identity_flags AS (
  SELECT nation_code, identity_key,
    bool_or(is_development) AS has_development_event,
    bool_or(NOT is_development) AS has_standard_event
  FROM nationality_appearances
  GROUP BY nation_code, identity_key
),
nationality_scores AS (
  SELECT nation_code, count(*) AS segment_scores, max(tss) AS best_tss, max(tes) AS best_tes, max(pcs) AS best_pcs
  FROM api.mv_segment_score
  WHERE nation_code IS NOT NULL
  GROUP BY nation_code
),
nationality_events AS (
  SELECT nation_code, count(DISTINCT event_id) AS events,
    min(event_start_date) AS first_seen_date, max(event_start_date) AS latest_seen_date
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

-- @@SWAP@@ (everything above builds the new versions; db/35 is applied in full, the check script stops here)
BEGIN;
DROP MATERIALIZED VIEW api.mv_dashboard_overview;
DROP MATERIALIZED VIEW api.mv_competition_mix;
DROP MATERIALIZED VIEW api.mv_club_dashboard;
DROP MATERIALIZED VIEW api.mv_club_skater_status;
DROP MATERIALIZED VIEW api.mv_nationality_dashboard;
DROP MATERIALIZED VIEW api.mv_skater_search;
ALTER MATERIALIZED VIEW api.mv_skater_search_new RENAME TO mv_skater_search;
ALTER MATERIALIZED VIEW api.mv_club_skater_status_new RENAME TO mv_club_skater_status;
ALTER MATERIALIZED VIEW api.mv_club_dashboard_new RENAME TO mv_club_dashboard;
ALTER MATERIALIZED VIEW api.mv_competition_mix_new RENAME TO mv_competition_mix;
ALTER MATERIALIZED VIEW api.mv_dashboard_overview_new RENAME TO mv_dashboard_overview;
ALTER MATERIALIZED VIEW api.mv_nationality_dashboard_new RENAME TO mv_nationality_dashboard;
ALTER INDEX api.idx_mv_skater_search_new_identity RENAME TO idx_mv_skater_search_identity;
CREATE UNIQUE INDEX idx_mv_skater_search_key ON api.mv_skater_search USING btree (skater_key);
CREATE INDEX idx_mv_skater_search_club ON api.mv_skater_search USING btree (club_name) WHERE (club_name IS NOT NULL);
CREATE INDEX idx_mv_skater_search_latest_name ON api.mv_skater_search USING btree (latest_seen_date DESC NULLS LAST, skater_name);
CREATE INDEX idx_mv_skater_search_nation ON api.mv_skater_search USING btree (nation_code) WHERE (nation_code IS NOT NULL);
CREATE INDEX idx_mv_skater_search_text_trgm ON api.mv_skater_search USING gin (search_text gin_trgm_ops);
CREATE UNIQUE INDEX idx_mv_club_skater_status_key ON api.mv_club_skater_status USING btree (club_name, skater_key);
CREATE INDEX idx_mv_club_skater_status_activity ON api.mv_club_skater_status USING btree (club_name, competitor_status, best_tss DESC NULLS LAST, latest_seen_date DESC NULLS LAST, skater_name);
CREATE UNIQUE INDEX idx_mv_club_dashboard_name ON api.mv_club_dashboard USING btree (club_name);
CREATE INDEX idx_mv_club_dashboard_activity ON api.mv_club_dashboard USING btree (events DESC, skaters DESC, latest_seen_date DESC NULLS LAST);
CREATE INDEX idx_mv_club_dashboard_search_trgm ON api.mv_club_dashboard USING gin (search_text gin_trgm_ops);
CREATE INDEX idx_mv_competition_mix_scope ON api.mv_competition_mix USING btree (scope_hint, competition_stream, competition_level);
CREATE UNIQUE INDEX idx_mv_dashboard_overview_singleton ON api.mv_dashboard_overview USING btree ((true));
CREATE UNIQUE INDEX idx_mv_nationality_dashboard_code ON api.mv_nationality_dashboard USING btree (nation_code);
CREATE INDEX idx_mv_nationality_dashboard_segment_scores ON api.mv_nationality_dashboard USING btree (segment_scores DESC, skaters DESC);
COMMIT;
ANALYZE api.mv_skater_search;
ANALYZE api.mv_club_skater_status;
ANALYZE api.mv_club_dashboard;
ANALYZE api.mv_nationality_dashboard;
ANALYZE api.mv_appearance_identity;
ANALYZE api.mv_score_identity;
ANALYZE api.mv_identity_lookup;
