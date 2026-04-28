-- Discovery catalog for event sources that may not yet expose direct result
-- URLs. This is separate from ingest.source_url_registry, which is for URLs
-- that can be validated/imported as result sources.

CREATE SCHEMA IF NOT EXISTS ingest;

CREATE TABLE IF NOT EXISTS ingest.event_discovery_catalog (
  id bigserial PRIMARY KEY,
  source_name text NOT NULL,
  source_page_url text NOT NULL,
  source_kind text NOT NULL,
  event_name text NOT NULL,
  date_range text,
  city text,
  country_code text,
  discipline text,
  discovery_status text NOT NULL DEFAULT 'catalog_only_needs_result_url',
  result_url text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  first_seen_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at timestamptz NOT NULL DEFAULT now(),
  CHECK (discovery_status IN (
    'catalog_only_needs_result_url',
    'candidate_result_url',
    'ready_for_validation',
    'registered',
    'skipped',
    'manual_review'
  ))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_event_discovery_catalog_identity
  ON ingest.event_discovery_catalog (
    source_name,
    source_page_url,
    event_name,
    coalesce(date_range, ''),
    coalesce(city, ''),
    coalesce(country_code, '')
  );

CREATE INDEX IF NOT EXISTS idx_event_discovery_catalog_status
  ON ingest.event_discovery_catalog(discovery_status);

CREATE INDEX IF NOT EXISTS idx_event_discovery_catalog_country_date
  ON ingest.event_discovery_catalog(country_code, date_range);
