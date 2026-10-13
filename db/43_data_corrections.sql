-- Edited 2026-10-13 by Claude (AI assistant): corrections of mistakes in the SOURCE pages (typos by organisers).
-- Every correction is a row with the old value, the new value and the reason. ingest.apply_corrections() changes a
-- value only while it still equals old_value, so a re-import that brings the typo back is corrected again, and a
-- fixed source is left alone. Run scripts/refresh_events.py <event keys> afterwards (the function returns them).

CREATE TABLE IF NOT EXISTS ingest.data_corrections (
  id          serial PRIMARY KEY,
  table_name  text NOT NULL CHECK (table_name IN ('events')),
  row_id      bigint NOT NULL,
  column_name text NOT NULL CHECK (column_name IN ('name', 'date_range', 'location', 'venue')),
  old_value   text NOT NULL,
  new_value   text NOT NULL,
  reason      text NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  applied_at  timestamptz,
  UNIQUE (table_name, row_id, column_name, old_value)
);

CREATE OR REPLACE FUNCTION ingest.apply_corrections() RETURNS bigint[] LANGUAGE plpgsql AS $f$
DECLARE c record; n int; changed bigint[] := '{}';
BEGIN
  FOR c IN SELECT * FROM ingest.data_corrections ORDER BY id LOOP
    EXECUTE format('UPDATE ingest.%I SET %I = $1 WHERE id = $2 AND %I = $3', c.table_name, c.column_name, c.column_name)
      USING c.new_value, c.row_id, c.old_value;
    GET DIAGNOSTICS n = ROW_COUNT;
    IF n > 0 THEN
      UPDATE ingest.data_corrections SET applied_at = now() WHERE id = c.id;
      changed := changed || c.row_id;   -- events.id = event_key
    END IF;
  END LOOP;
  RETURN changed;
END $f$;

INSERT INTO ingest.data_corrections (table_name, row_id, column_name, old_value, new_value, reason) VALUES
 ('events', 499, 'date_range', '14.4.1921 - 18.4.1921', '14.4.2021 - 18.4.2021',
  'Skate Helena 2021: the organiser''s page says 1921 (typo); the event name and season are 2021'),
 ('events', 500, 'date_range', '14.4.1921 - 15.4.1921', '14.4.2021 - 15.4.2021',
  'Skate Helena Mini 2021: same typo as the main event'),
 ('events', 41, 'name', 'Black sea ice cup 2024', 'Black Sea Ice Cup 2025 - Non-ISU',
  'BSF page title says 2024 but the event is 02-06.04.2025 (folder 02-06.04.2025/NonISU); DP part is event 39')
ON CONFLICT DO NOTHING;
