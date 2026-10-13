-- Edited 2026-10-12 by Claude (AI assistant): PROTOTYPE (tested on a copy first).
--
-- Why: every api.mv_* was a materialized view that is rebuilt from scratch after each import (11-50 min on the
-- small server, all 18 at once). The data changes one event at a time, so the copies become normal TABLES with the
-- SAME names (API code unchanged) and api.refresh_events(event_ids) replaces only those events' rows and the
-- summary rows they touch. Each table's old query is kept as a view api.src_<name>, so the logic is unchanged and
-- a full rebuild is still possible (api.rebuild_all()).
--
-- Also: the raw jsonb columns are not copied any more (the API never reads them; the source rows keep them).
--
-- Run once:  psql -v ON_ERROR_STOP=1 -f db/40_tables_instead_of_copies.sql

BEGIN;

CREATE TABLE IF NOT EXISTS api.copy_def (
  name        text PRIMARY KEY,
  ord         int  NOT NULL,           -- build order (dependencies first)
  kind        text NOT NULL,           -- 'event' = rows belong to one event, 'summary' = aggregated
  event_col   text,                    -- column holding the event id for kind='event'
  definition  text NOT NULL,           -- the old materialized view query
  indexes     text[] NOT NULL
);

-- 1. remember every copy's query + indexes (dependency order, from the old refresh script)
WITH ord(name, ord, kind, event_col) AS (VALUES
  ('mv_dim_event',                     1, 'event',   'event_key'),
  ('mv_dim_category',                  2, 'event',   'event_key'),
  ('mv_dim_segment',                   3, 'event',   NULL),         -- by category_key, see refresh_events
  ('mv_skater_appearances_with_club',  4, 'event',   'event_id'),
  ('mv_competition_result',            5, 'event',   'event_key'),
  ('mv_segment_score',                 6, 'event',   'event_key'),
  ('mv_component_score',               7, 'event',   'event_key'),
  ('mv_element_score',                 8, 'event',   'event_key'),
  ('mv_element_judge_mark',            9, 'event',   'event_key'),
  ('mv_identity_lookup',              10, 'summary', NULL),
  ('mv_appearance_identity',          11, 'summary', NULL),
  ('mv_score_identity',               12, 'event',   'event_key'),
  ('mv_skater_search',                13, 'summary', NULL),
  ('mv_club_skater_status',           14, 'summary', NULL),
  ('mv_club_dashboard',               15, 'summary', NULL),
  ('mv_nationality_dashboard',        16, 'summary', NULL),
  ('mv_competition_mix',              17, 'summary', NULL),
  ('mv_dashboard_overview',           18, 'summary', NULL)
)
INSERT INTO api.copy_def(name, ord, kind, event_col, definition, indexes)
SELECT o.name, o.ord, o.kind, o.event_col, pg_get_viewdef(c.oid),
       coalesce((SELECT array_agg(i.indexdef ORDER BY i.indexname) FROM pg_indexes i
                 WHERE i.schemaname = 'api' AND i.tablename = o.name), '{}')
FROM ord o JOIN pg_class c ON c.relname = o.name AND c.relnamespace = 'api'::regnamespace AND c.relkind = 'm'
ON CONFLICT (name) DO NOTHING;

DO $$ BEGIN
  IF (SELECT count(*) FROM api.copy_def) <> 18 THEN RAISE EXCEPTION 'expected 18 copies, got %', (SELECT count(*) FROM api.copy_def); END IF;
END $$;

-- 2. data into plain tables (without raw), drop the materialized views, put the tables in their place
DO $$
DECLARE d record; cols text;
BEGIN
  FOR d IN SELECT * FROM api.copy_def ORDER BY ord LOOP
    SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY a.attnum) INTO cols
    FROM pg_attribute a WHERE a.attrelid = ('api.' || d.name)::regclass AND a.attnum > 0 AND NOT a.attisdropped
      AND a.attname <> 'raw';
    EXECUTE format('CREATE TABLE api.%I AS SELECT %s FROM api.%I', '_t_' || d.name, cols, d.name);
  END LOOP;
  FOR d IN SELECT * FROM api.copy_def ORDER BY ord DESC LOOP
    EXECUTE format('DROP MATERIALIZED VIEW api.%I CASCADE', d.name);
  END LOOP;
  FOR d IN SELECT * FROM api.copy_def ORDER BY ord LOOP
    EXECUTE format('ALTER TABLE api.%I RENAME TO %I', '_t_' || d.name, d.name);
  END LOOP;
END $$;

-- 3. the old queries become source views (they now read the tables of the earlier steps), indexes come back
DO $$
DECLARE d record; ix text;
BEGIN
  FOR d IN SELECT * FROM api.copy_def ORDER BY ord LOOP
    EXECUTE format('CREATE VIEW api.%I AS %s', 'src_' || d.name, d.definition);
    FOREACH ix IN ARRAY d.indexes LOOP EXECUTE ix; END LOOP;
  END LOOP;
END $$;

-- 4. one row = "the data changed" (the API's cache key); bumped by refresh_events / rebuild_all
CREATE TABLE IF NOT EXISTS api.data_version (id int PRIMARY KEY DEFAULT 1 CHECK (id = 1), changed_at timestamptz NOT NULL);
INSERT INTO api.data_version VALUES (1, now()) ON CONFLICT (id) DO UPDATE SET changed_at = now();

-- column list of a table (src views still have raw)
CREATE OR REPLACE FUNCTION api._cols(t text) RETURNS text LANGUAGE sql STABLE AS $f$
  SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum) FROM pg_attribute
  WHERE attrelid = ('api.' || t)::regclass AND attnum > 0 AND NOT attisdropped
$f$;

-- full rebuild (same result as the old refresh script), e.g. after changing identity rules
CREATE OR REPLACE FUNCTION api.rebuild_all() RETURNS void LANGUAGE plpgsql AS $f$
DECLARE d record;
BEGIN
  FOR d IN SELECT * FROM api.copy_def ORDER BY ord LOOP
    EXECUTE format('TRUNCATE api.%I', d.name);
    EXECUTE format('INSERT INTO api.%I (%s) SELECT %s FROM api.%I', d.name, api._cols(d.name), api._cols(d.name), 'src_' || d.name);
    EXECUTE format('ANALYZE api.%I', d.name);
  END LOOP;
  UPDATE api.data_version SET changed_at = now();
END $f$;

-- replace the rows of these events (new import, re-import or deletion) and every summary row they touch
CREATE OR REPLACE FUNCTION api.refresh_events(ev bigint[]) RETURNS jsonb LANGUAGE plpgsql AS $f$
DECLARE
  d record; t0 timestamptz := clock_timestamp(); timing jsonb := '{}';
  names text[]; idents text[]; clubs text[];
BEGIN
  -- identities / clubs touched BEFORE (rows may disappear)
  SELECT array_agg(DISTINCT ai.identity_key) INTO idents
  FROM api.mv_skater_appearances_with_club s JOIN api.mv_appearance_identity ai USING (skater_appearance_id)
  WHERE s.event_id = ANY(ev);
  SELECT array_agg(DISTINCT coalesce(s.canonical_club_name, s.club_name)) INTO clubs
  FROM api.mv_skater_appearances_with_club s WHERE s.event_id = ANY(ev);

  -- event rows: delete + insert from the source views, filtered by event
  DELETE FROM api.mv_appearance_identity a USING api.mv_skater_appearances_with_club s
   WHERE s.skater_appearance_id = a.skater_appearance_id AND s.event_id = ANY(ev);
  DELETE FROM api.mv_dim_segment g USING api.mv_dim_category c WHERE c.category_key = g.category_key AND c.event_key = ANY(ev);
  FOR d IN SELECT * FROM api.copy_def WHERE kind = 'event' AND event_col IS NOT NULL AND name <> 'mv_score_identity' ORDER BY ord LOOP
    EXECUTE format('DELETE FROM api.%I WHERE %I = ANY($1)', d.name, d.event_col) USING ev;
    EXECUTE format('INSERT INTO api.%I (%s) SELECT %s FROM api.%I WHERE %I = ANY($1)',
                   d.name, api._cols(d.name), api._cols(d.name), 'src_' || d.name, d.event_col) USING ev;
    IF d.name = 'mv_dim_category' THEN
      INSERT INTO api.mv_dim_segment SELECT g.* FROM api.src_mv_dim_segment g
      JOIN api.mv_dim_category c ON c.category_key = g.category_key WHERE c.event_key = ANY(ev);
    END IF;
  END LOOP;
  timing := timing || jsonb_build_object('event_rows_ms', round(extract(epoch FROM clock_timestamp() - t0) * 1000));

  -- names of these events -> identity lookup (new names only; existing rows never change)
  SELECT array_agg(DISTINCT n) INTO names FROM (
    SELECT skater_name n FROM api.mv_segment_score WHERE event_key = ANY(ev)
    UNION SELECT skater_name FROM api.mv_element_score WHERE event_key = ANY(ev)
    UNION SELECT skater_name FROM api.mv_skater_appearances_with_club WHERE event_id = ANY(ev)) x WHERE n IS NOT NULL;
  INSERT INTO api.mv_identity_lookup SELECT * FROM api.src_mv_identity_lookup WHERE skater_name = ANY(names)
  ON CONFLICT DO NOTHING;
  INSERT INTO api.mv_appearance_identity
  SELECT * FROM api.src_mv_appearance_identity a
  WHERE a.skater_appearance_id IN (SELECT skater_appearance_id FROM api.mv_skater_appearances_with_club WHERE event_id = ANY(ev));
  DELETE FROM api.mv_score_identity WHERE event_key = ANY(ev);
  INSERT INTO api.mv_score_identity SELECT * FROM api.src_mv_score_identity WHERE event_key = ANY(ev);

  -- identities / clubs touched AFTER
  SELECT array_agg(DISTINCT k) INTO idents FROM (
    SELECT unnest(coalesce(idents, '{}')) k
    UNION SELECT ai.identity_key FROM api.mv_skater_appearances_with_club s
          JOIN api.mv_appearance_identity ai USING (skater_appearance_id) WHERE s.event_id = ANY(ev)) x;
  SELECT array_agg(DISTINCT c) INTO clubs FROM (
    SELECT unnest(coalesce(clubs, '{}')) c
    UNION SELECT coalesce(s.canonical_club_name, s.club_name) FROM api.mv_skater_appearances_with_club s
          WHERE s.event_id = ANY(ev)) x WHERE c IS NOT NULL;
  timing := timing || jsonb_build_object('identity_ms', round(extract(epoch FROM clock_timestamp() - t0) * 1000));

  -- per-skater and per-club summaries: only the touched keys
  DELETE FROM api.mv_skater_search WHERE identity_key = ANY(idents);
  INSERT INTO api.mv_skater_search SELECT * FROM api.src_mv_skater_search WHERE identity_key = ANY(idents);
  timing := timing || jsonb_build_object('skater_search_ms', round(extract(epoch FROM clock_timestamp() - t0) * 1000));
  DELETE FROM api.mv_club_skater_status WHERE club_name = ANY(clubs) OR identity_key = ANY(idents);
  INSERT INTO api.mv_club_skater_status SELECT * FROM api.src_mv_club_skater_status
   WHERE club_name = ANY(clubs) OR identity_key = ANY(idents);
  timing := timing || jsonb_build_object('club_status_ms', round(extract(epoch FROM clock_timestamp() - t0) * 1000));

  -- small whole-database summaries: recompute (each well under a second on tables)
  FOR d IN SELECT * FROM api.copy_def WHERE name IN ('mv_club_dashboard', 'mv_nationality_dashboard',
                                                      'mv_competition_mix', 'mv_dashboard_overview') ORDER BY ord LOOP
    EXECUTE format('TRUNCATE api.%I', d.name);
    EXECUTE format('INSERT INTO api.%I SELECT * FROM api.%I', d.name, 'src_' || d.name);
  END LOOP;
  UPDATE api.data_version SET changed_at = now();
  RETURN timing || jsonb_build_object('total_ms', round(extract(epoch FROM clock_timestamp() - t0) * 1000),
                                      'skaters', coalesce(array_length(idents, 1), 0), 'clubs', coalesce(array_length(clubs, 1), 0));
END $f$;

COMMIT;

ANALYZE;
