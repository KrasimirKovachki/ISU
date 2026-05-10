-- API-facing materialized views.
--
-- These views keep interactive website/API requests away from expensive mart
-- aggregations. Re-run this file after rebuilding analytics/mart views or after
-- a large import batch.

CREATE SCHEMA IF NOT EXISTS api;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

DROP MATERIALIZED VIEW IF EXISTS api.mv_club_dashboard CASCADE;
DROP MATERIALIZED VIEW IF EXISTS api.mv_club_skater_status CASCADE;
DROP MATERIALIZED VIEW IF EXISTS api.mv_competition_mix CASCADE;
DROP MATERIALIZED VIEW IF EXISTS api.mv_dashboard_overview CASCADE;
DROP MATERIALIZED VIEW IF EXISTS api.mv_skater_search CASCADE;

CREATE OR REPLACE FUNCTION api.normalized_skater_name(raw_name text, club_name text DEFAULT NULL)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  normalized text;
  normalized_club text;
BEGIN
  normalized := upper(regexp_replace(trim(raw_name), '\s+', ' ', 'g'));
  IF normalized IS NULL OR normalized = '' THEN
    RETURN NULL;
  END IF;

  normalized_club := upper(regexp_replace(trim(club_name), '\s+', ' ', 'g'));
  IF normalized_club IS NOT NULL
     AND normalized_club <> ''
     AND normalized <> normalized_club
     AND normalized LIKE '% ' || normalized_club THEN
    normalized := trim(left(normalized, length(normalized) - length(normalized_club) - 1));
  END IF;

  IF normalized = 'ALEXANDRA MINTSIDU' THEN
    normalized := 'ALEKSANDRA MINTSIDU';
  END IF;

  RETURN normalized;
END;
$$;

CREATE OR REPLACE FUNCTION api.normalized_skater_nation_code(raw_name text, nation_code text DEFAULT NULL)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN upper(regexp_replace(trim(raw_name), '\s+', ' ', 'g')) IN ('ALEKSANDRA MINTSIDU', 'ALEXANDRA MINTSIDU') THEN 'BUL'
    ELSE nation_code
  END;
$$;

CREATE OR REPLACE FUNCTION api.skater_identity_key(raw_name text, nation_code text DEFAULT NULL, club_name text DEFAULT NULL)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  WITH cleaned AS (
    SELECT api.normalized_skater_name(raw_name, club_name) AS skater_name
  )
  SELECT concat_ws(
    '|',
    COALESCE(api.normalized_skater_nation_code(raw_name, nation_code), ''),
    regexp_replace(
      regexp_replace(
        split_part(skater_name, ' ', array_length(regexp_split_to_array(skater_name, '\s+'), 1)),
        '[AEIOUY]',
        '',
        'g'
      ),
      '(.)\1+',
      '\1',
      'g'
    ),
    regexp_replace(
      regexp_replace(split_part(skater_name, ' ', 1), '[AEIOUY]', '', 'g'),
      '(.)\1+',
      '\1',
      'g'
    )
  )
  FROM cleaned;
$$;

CREATE MATERIALIZED VIEW api.mv_skater_search AS
WITH appearances AS (
  SELECT
    sac.*,
    COALESCE(sac.canonical_club_name, sac.club_name) AS api_club_name,
    api.normalized_skater_nation_code(sac.skater_name, sac.nation_code) AS api_nation_code,
    api.normalized_skater_name(sac.skater_name, COALESCE(sac.canonical_club_name, sac.club_name)) AS cleaned_skater_name,
    api.skater_identity_key(sac.skater_name, sac.nation_code, COALESCE(sac.canonical_club_name, sac.club_name)) AS identity_key
  FROM analytics.v_skater_appearances_with_club sac
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
    api.skater_identity_key(fss.skater_name, fss.nation_code, fss.club_name) AS identity_key,
    max(fss.tss) AS best_tss,
    max(fss.tes) AS best_tes,
    max(fss.pcs) AS best_pcs,
    max(fss.score_total) AS best_score_total,
    count(DISTINCT fss.event_key) AS scored_competitions
  FROM mart.fact_segment_score fss
  WHERE fss.skater_name IS NOT NULL
  GROUP BY 1
)
SELECT
  -- Stable key derived deterministically from identity_key. Same identity =>
  -- same skater_key forever, even after MV refresh (vs. the prior row_number
  -- approach where adding/removing any skater shifted everyone after them
  -- and broke shared/bookmarked URLs). 9-digit positive int; collision space
  -- ~10^9 vs current ~7k skaters means collision probability is negligible.
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

CREATE UNIQUE INDEX idx_mv_skater_search_key
  ON api.mv_skater_search(skater_key);
CREATE UNIQUE INDEX idx_mv_skater_search_identity
  ON api.mv_skater_search(identity_key);
CREATE INDEX idx_mv_skater_search_text_trgm
  ON api.mv_skater_search USING gin (search_text gin_trgm_ops);
CREATE INDEX idx_mv_skater_search_latest_name
  ON api.mv_skater_search(latest_seen_date DESC NULLS LAST, skater_name);
CREATE INDEX idx_mv_skater_search_club
  ON api.mv_skater_search(club_name)
  WHERE club_name IS NOT NULL;
CREATE INDEX idx_mv_skater_search_nation
  ON api.mv_skater_search(nation_code)
  WHERE nation_code IS NOT NULL;

CREATE MATERIALIZED VIEW api.mv_dashboard_overview AS
SELECT
  (SELECT count(*) FROM mart.dim_event) AS events,
  (SELECT count(*) FROM api.mv_skater_search) AS skaters,
  (SELECT count(*) FROM mart.dim_club) AS registered_clubs,
  (SELECT count(DISTINCT COALESCE(canonical_club_name, club_name)) FROM analytics.v_skater_appearances_with_club WHERE club_name IS NOT NULL) AS scoring_clubs,
  (SELECT count(*) FROM mart.fact_segment_score) AS segment_scores,
  (SELECT count(*) FROM mart.fact_element_score) AS element_scores,
  (
    SELECT count(DISTINCT api.skater_identity_key(skater_name, nation_code, COALESCE(canonical_club_name, club_name)))
    FROM analytics.v_skater_appearances_with_club
    WHERE category_name ILIKE 'Mlad Figurist%%'
  ) AS development_skaters,
  (SELECT min(event_start_date) FROM mart.dim_event) AS first_event_date,
  (SELECT max(event_start_date) FROM mart.dim_event) AS latest_event_date,
  (SELECT count(*) FROM mart.dim_event WHERE competition_stream IN ('ISU', 'ISUCS')) AS international_events,
  (SELECT count(*) FROM mart.dim_event WHERE competition_stream IN ('FRP', 'BSF', 'DP') OR competition_level = 'federated') AS federation_events,
  (SELECT count(*) FROM mart.dim_event WHERE competition_stream IN ('club', 'club_amateur', 'NonISU')) AS club_or_open_events;

CREATE UNIQUE INDEX idx_mv_dashboard_overview_singleton
  ON api.mv_dashboard_overview((true));

CREATE MATERIALIZED VIEW api.mv_competition_mix AS
WITH events AS (
  SELECT
    de.*,
    CASE
      WHEN de.competition_stream IN ('ISU', 'ISUCS') THEN 'international'
      WHEN de.competition_stream IN ('FRP', 'BSF', 'DP') OR de.competition_level = 'federated' THEN 'federation'
      WHEN de.competition_stream IN ('club', 'club_amateur', 'NonISU') THEN 'club_or_open'
      ELSE 'unknown'
    END AS scope_hint
  FROM mart.dim_event de
),
appearance_event AS (
  SELECT
    event_id,
    count(DISTINCT COALESCE(canonical_club_name, club_name)) FILTER (WHERE club_name IS NOT NULL) AS clubs_with_appearances,
    count(DISTINCT concat_ws(
      '|',
      api.skater_identity_key(skater_name, nation_code, COALESCE(canonical_club_name, club_name))
    )) AS skaters_with_appearances
  FROM analytics.v_skater_appearances_with_club
  WHERE skater_name IS NOT NULL
  GROUP BY event_id
),
score_event AS (
  SELECT
    event_key,
    count(DISTINCT mss.skater_key) FILTER (WHERE mss.skater_key IS NOT NULL) AS skaters_with_scores,
    count(segment_score_key) AS segment_scores
  FROM mart.fact_segment_score fss
  LEFT JOIN api.mv_skater_search mss
    ON mss.identity_key = api.skater_identity_key(fss.skater_name, fss.nation_code, fss.club_name)
  GROUP BY event_key
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
GROUP BY
  COALESCE(e.competition_stream, 'unknown'),
  COALESCE(e.competition_level, 'unknown'),
  COALESCE(e.event_type, 'unknown'),
  COALESCE(e.representation_primary, 'unknown'),
  e.scope_hint;

CREATE INDEX idx_mv_competition_mix_scope
  ON api.mv_competition_mix(scope_hint, competition_stream, competition_level);

CREATE MATERIALIZED VIEW api.mv_club_skater_status AS
WITH club_identities AS (
  SELECT DISTINCT
    COALESCE(sac.canonical_club_name, sac.club_name) AS club_name,
    mss.skater_key,
    mss.identity_key,
    CASE
      WHEN sac.category_name ILIKE 'Mlad Figurist%%' THEN 'development'
      WHEN de.event_name ILIKE 'Ice Peak Trophy%%' THEN 'recreational'
      ELSE 'national'
    END AS competition_kind
  FROM analytics.v_skater_appearances_with_club sac
  JOIN mart.dim_event de ON de.event_key = sac.event_id
  JOIN api.mv_skater_search mss
    ON mss.identity_key = api.skater_identity_key(sac.skater_name, sac.nation_code, COALESCE(sac.canonical_club_name, sac.club_name))
  WHERE sac.club_name IS NOT NULL
),
club_identity_flags AS (
  SELECT
    club_name,
    skater_key,
    identity_key,
    bool_or(competition_kind = 'national') AS has_national_event,
    bool_or(competition_kind = 'recreational') AS has_recreational_event,
    bool_or(competition_kind = 'development') AS has_development_event
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
JOIN api.mv_skater_search mss ON mss.skater_key = cif.skater_key;

CREATE UNIQUE INDEX idx_mv_club_skater_status_key
  ON api.mv_club_skater_status(club_name, skater_key);
CREATE INDEX idx_mv_club_skater_status_activity
  ON api.mv_club_skater_status(
    club_name,
    competitor_status,
    best_tss DESC NULLS LAST,
    latest_seen_date DESC NULLS LAST,
    skater_name
  );

CREATE MATERIALIZED VIEW api.mv_club_dashboard AS
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
  FROM analytics.v_skater_appearances_with_club sac
  JOIN mart.dim_event de ON de.event_key = sac.event_id
  JOIN api.mv_skater_search mss
    ON mss.identity_key = api.skater_identity_key(sac.skater_name, sac.nation_code, COALESCE(sac.canonical_club_name, sac.club_name))
  WHERE sac.club_name IS NOT NULL
),
club_scores AS (
  SELECT
    ca.club_name,
    fss.segment_score_key,
    fss.tss,
    fss.tes,
    fss.pcs
  FROM club_appearances ca
  JOIN mart.fact_segment_score fss
    ON fss.event_key = ca.event_id
   AND api.skater_identity_key(fss.skater_name, fss.nation_code, fss.club_name) = ca.identity_key
),
club_alias_text AS (
  SELECT
    c.canonical_name AS club_name,
    string_agg(ca.alias_name, ' ' ORDER BY ca.alias_name) AS aliases_text
  FROM core.clubs c
  JOIN core.club_aliases ca ON ca.club_id = c.id
  GROUP BY c.canonical_name
),
club_country_guess AS (
  SELECT club_name, nation_code AS inferred_country_code
  FROM (
    SELECT
      club_name,
      appearance_nation_code AS nation_code,
      count(*) AS appearances,
      row_number() OVER (
        PARTITION BY club_name
        ORDER BY count(*) DESC, appearance_nation_code
      ) AS rank
    FROM club_appearances
    WHERE appearance_nation_code IS NOT NULL
    GROUP BY club_name, appearance_nation_code
  ) ranked
  WHERE rank = 1
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
  FROM api.mv_club_skater_status
  GROUP BY club_name
)
SELECT
  ca.club_name,
  COALESCE(max(ca.registered_country_code), ccg.inferred_country_code) AS country_code,
  count(DISTINCT ca.skater_key) AS skaters,
  count(DISTINCT ca.event_id) AS events,
  count(DISTINCT cs.segment_score_key) AS segment_scores,
  count(DISTINCT ca.event_id) FILTER (WHERE ca.scope_hint = 'international') AS international_events,
  count(DISTINCT ca.event_id) FILTER (WHERE ca.scope_hint = 'federation') AS federation_events,
  count(DISTINCT ca.event_id) FILTER (WHERE ca.scope_hint = 'club_or_open') AS club_or_open_events,
  COALESCE(max(csc.national_competitors), 0) AS national_competitors,
  COALESCE(max(csc.recreational_only_skaters), 0) AS recreational_only_skaters,
  COALESCE(max(csc.mixed_competitors), 0) AS mixed_competitors,
  COALESCE(max(csc.development_skaters), 0) AS development_skaters,
  COALESCE(max(csc.development_only_skaters), 0) AS development_only_skaters,
  COALESCE(max(csc.converted_development_skaters), 0) AS converted_development_skaters,
  max(cs.tss) AS best_tss,
  max(cs.tes) AS best_tes,
  max(cs.pcs) AS best_pcs,
  min(ca.event_start_date) AS first_seen_date,
  max(ca.event_start_date) AS latest_seen_date,
  lower(concat_ws(' ', ca.club_name, cat.aliases_text)) AS search_text
FROM club_appearances ca
LEFT JOIN club_scores cs ON cs.club_name = ca.club_name
LEFT JOIN club_alias_text cat ON cat.club_name = ca.club_name
LEFT JOIN club_country_guess ccg ON ccg.club_name = ca.club_name
LEFT JOIN club_status_counts csc ON csc.club_name = ca.club_name
GROUP BY ca.club_name, ccg.inferred_country_code, cat.aliases_text;

CREATE UNIQUE INDEX idx_mv_club_dashboard_name
  ON api.mv_club_dashboard(club_name);
CREATE INDEX idx_mv_club_dashboard_search_trgm
  ON api.mv_club_dashboard USING gin (search_text gin_trgm_ops);
CREATE INDEX idx_mv_club_dashboard_activity
  ON api.mv_club_dashboard(events DESC, skaters DESC, latest_seen_date DESC NULLS LAST);
