-- Allow repeated imports of the same source URL.
--
-- Events are staging records scoped to an import run. The same source URL must
-- be importable more than once so parser fixes and source changes can be
-- validated without deleting old runs.

ALTER TABLE ingest.events
  DROP CONSTRAINT IF EXISTS events_source_profile_id_source_url_key;

ALTER TABLE ingest.events
  ADD CONSTRAINT events_import_run_id_source_url_key UNIQUE (import_run_id, source_url);
