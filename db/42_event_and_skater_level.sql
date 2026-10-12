-- 2026-10-12 Claude (AI assistant): PROTOTYPE. Event level from the evidence we already have
-- (import profile stream, site address, event name); category level (db/41) wins, event level fills gaps.
CREATE OR REPLACE VIEW api.v_event_level AS
SELECT e.event_key, e.event_name, e.root_url, sp.competition_stream AS profile_stream,
  CASE
    WHEN e.root_url ~* '(recreational|rekreativ|interclub|adult|amateur|hobby)' OR e.event_name ~* '(recreational|interclub|adult|amateur|hobby)'
      OR sp.competition_stream IN ('club_amateur', 'club_test', 'club_control') THEN 'recreational'
    WHEN e.root_url ~* '(/|_)(jgp|gp|cs)(/|_|[0-9])' OR e.event_name ~* '(grand prix|challenger|world|european championships|four continents|olympic)'
      OR sp.competition_stream = 'ISUCS' THEN 'isu_elite'
    WHEN e.root_url ~* '(/|_)(non-?isu|nonisu|non)(/|_|$)|/cc' OR sp.competition_stream IN ('NonISU', 'FRP', 'national_club', 'DP')
      OR e.event_name ~* 'national' THEN 'national'
    WHEN e.root_url ~* '(/|_)isu(/|_|$)' OR sp.competition_stream = 'ISU' OR e.event_name ~* '\misu\M' THEN 'isu'
    WHEN sp.competition_stream = 'club' THEN 'club'
    ELSE 'unknown'
  END AS level
FROM api.mv_dim_event e LEFT JOIN ingest.source_profiles sp ON sp.profile_key = e.profile_key;

CREATE OR REPLACE VIEW api.v_category_level AS
SELECT c.category_key, c.event_key, c.category_name, c.discipline, c.age_group, c.gender, el.level AS event_level,
  CASE
    WHEN c.level = 'recreational' OR el.level = 'recreational' THEN 'recreational'
    WHEN c.level IN ('isu', 'national') AND el.level IN ('isu', 'isu_elite') AND c.level = 'isu' THEN 'isu'
    WHEN c.level = 'national' THEN 'national'
    WHEN c.level = 'isu' AND el.level IN ('national', 'club') THEN 'national'   -- e.g. "Junior Women" at a Non-ISU part
    WHEN c.level = 'isu' THEN 'isu'
    WHEN el.level IN ('isu', 'isu_elite') THEN 'isu'
    WHEN el.level IN ('national', 'club') THEN 'national'
    ELSE 'unknown'
  END AS level
FROM api.v_category_class c JOIN api.v_event_level el USING (event_key);

-- skater: competitive (ISU / national categories) vs recreational, from where they actually skated
CREATE OR REPLACE VIEW api.v_skater_level AS
SELECT ai.identity_key,
  count(DISTINCT s.event_id) FILTER (WHERE cl.level = 'isu')          AS isu_events,
  count(DISTINCT s.event_id) FILTER (WHERE cl.level = 'national')     AS national_events,
  count(DISTINCT s.event_id) FILTER (WHERE cl.level = 'recreational') AS recreational_events,
  CASE WHEN bool_or(cl.level = 'isu') THEN 'competitive_isu'
       WHEN bool_or(cl.level = 'national') AND bool_or(cl.level = 'recreational') THEN 'competitive_and_recreational'
       WHEN bool_or(cl.level = 'national') THEN 'competitive_national'
       WHEN bool_or(cl.level = 'recreational') THEN 'recreational'
       ELSE 'unknown' END AS skater_level
FROM api.mv_skater_appearances_with_club s
JOIN api.mv_appearance_identity ai USING (skater_appearance_id)
JOIN api.v_category_level cl ON cl.category_key = s.category_id
GROUP BY ai.identity_key;
