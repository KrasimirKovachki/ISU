-- Edited 2026-10-13 by Claude (AI assistant): one competition published as several result sets (owner: "one competition
-- with three parts" for Black Sea Ice Cup 2025: ISU / Non-ISU / DP = Държавно първенство, national championship).
-- A Bulgarian skater skates once and is ranked in two lists, so pages show the competition once with its parts and a
-- skater's skate once with every place.
--
-- Rule (only safe cases): same parent folder AND the folder name is a part label AND start dates within 7 days, or the
-- europeancriterium pattern .../html/<n>/ + .../html/<n>/non/. Rebuilt by api.rebuild_event_parts() (refresh_events
-- calls it); manual entries in api.event_part_manual win.

CREATE TABLE IF NOT EXISTS api.event_part_manual (
  event_key  bigint PRIMARY KEY,
  group_key  bigint NOT NULL,
  part       text NOT NULL,
  note       text
);

CREATE TABLE IF NOT EXISTS api.event_part (
  event_key  bigint PRIMARY KEY,
  group_key  bigint NOT NULL,       -- the smallest event_key of the competition
  part       text NOT NULL,          -- shown label: ISU, ISU Challenger, Non-ISU, National championship, Solo Ice Dance
  part_order int NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_event_part_group ON api.event_part(group_key);

CREATE OR REPLACE FUNCTION api.part_label(folder text) RETURNS text LANGUAGE sql IMMUTABLE AS $f$
  SELECT CASE upper(folder)
    WHEN 'ISU' THEN 'ISU' WHEN 'ISUCS' THEN 'ISU Challenger' WHEN 'NONISU' THEN 'Non-ISU' WHEN 'NON' THEN 'Non-ISU'
    WHEN 'DP' THEN 'National championship' WHEN 'SOLOICEDANCE' THEN 'Solo Ice Dance' END
$f$;

CREATE OR REPLACE FUNCTION api.rebuild_event_parts() RETURNS int LANGUAGE plpgsql AS $f$
DECLARE n int;
BEGIN
  TRUNCATE api.event_part;
  WITH e AS (
    SELECT event_key, event_start_date,
           regexp_replace(root_url, '/(index\.html?)?$', '') AS base
    FROM api.mv_dim_event),
  f AS (
    SELECT event_key, event_start_date, base,
           regexp_replace(base, '/[^/]+$', '') AS parent,
           substring(base from '/([^/]+)$') AS folder
    FROM e),
  labelled AS (                                   -- ISU / NonISU / DP ... folders under one parent
    SELECT event_key, event_start_date, parent AS grp_url, api.part_label(folder) AS part FROM f
    WHERE api.part_label(folder) IS NOT NULL
    UNION ALL                                     -- europeancriterium: .../html/<n>/ is the main part of .../html/<n>/non/
    SELECT m.event_key, m.event_start_date, m.base, 'ISU' FROM f m
    WHERE EXISTS (SELECT 1 FROM f x WHERE x.base = m.base || '/non') AND api.part_label(m.folder) IS NULL),
  grouped AS (
    SELECT l.*, min(l2.event_key) AS group_key, count(*) AS parts
    FROM labelled l JOIN labelled l2 ON l2.grp_url = l.grp_url AND abs(l2.event_start_date - l.event_start_date) <= 7
    GROUP BY l.event_key, l.event_start_date, l.grp_url, l.part)
  INSERT INTO api.event_part (event_key, group_key, part, part_order)
  SELECT event_key, group_key, part,
         CASE part WHEN 'ISU' THEN 1 WHEN 'ISU Challenger' THEN 2 WHEN 'Non-ISU' THEN 3 WHEN 'National championship' THEN 4 ELSE 5 END
  FROM grouped WHERE parts > 1
  ON CONFLICT (event_key) DO NOTHING;
  -- manual entries win
  INSERT INTO api.event_part (event_key, group_key, part, part_order)
  SELECT event_key, group_key, part, 9 FROM api.event_part_manual
  ON CONFLICT (event_key) DO UPDATE SET group_key = EXCLUDED.group_key, part = EXCLUDED.part;
  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN (SELECT count(*) FROM api.event_part);
END $f$;

SELECT api.rebuild_event_parts();
