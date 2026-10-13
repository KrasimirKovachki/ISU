-- ISU public API event discovery.
--
-- These tables store source-specific API snapshots. They do not replace
-- ingest.event_discovery_catalog or ingest.source_url_registry; they feed them.

CREATE SCHEMA IF NOT EXISTS ingest;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS ingest.isu_api_event_catalog (
  api_id                  uuid,
  event_id                bigint,
  slug                    text,
  name                    text NOT NULL,
  from_date               date,
  to_date                 date,
  display_date            text,
  city                    text,
  country_code            text,
  country_name            text,
  season                  text,
  sport_code              text,
  sport_name              text,
  discipline_title        text,
  event_type              text,
  event_type_name         text,
  event_sub_type_name     text,
  state                   text,
  is_event_result         boolean,
  is_entry_show           boolean,
  button_1_title          text,
  button_1_url            text,
  button_2_title          text,
  button_2_url            text,
  button_3_title          text,
  button_3_url            text,
  raw                     jsonb NOT NULL,
  first_seen_at           timestamptz NOT NULL DEFAULT now(),
  last_seen_at            timestamptz NOT NULL DEFAULT now(),
  CHECK (api_id IS NOT NULL OR event_id IS NOT NULL OR slug IS NOT NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_isu_api_event_catalog_identity
  ON ingest.isu_api_event_catalog (
    coalesce(api_id::text, ''),
    coalesce(event_id::text, ''),
    coalesce(slug, ''),
    coalesce(season, '')
  );

CREATE INDEX IF NOT EXISTS idx_isu_api_event_catalog_season
  ON ingest.isu_api_event_catalog(season);

CREATE INDEX IF NOT EXISTS idx_isu_api_event_catalog_result_urls
  ON ingest.isu_api_event_catalog(button_1_url, button_2_url, button_3_url);

CREATE TABLE IF NOT EXISTS ingest.isu_skater_event_history (
  skaters_competition_results_id bigint PRIMARY KEY,
  skaters_id                     bigint NOT NULL,
  api_id                         uuid,
  event_api_id                   uuid,
  event_name                     text NOT NULL,
  event_date                     date,
  event_to_date                  date,
  year                           int,
  season                         text,
  points                         numeric,
  rank                           text,
  category                       text,
  country                        text,
  city                           text,
  event_type_name                text,
  sport_code                     text,
  sport_name                     text,
  raw                            jsonb NOT NULL,
  first_seen_at                  timestamptz NOT NULL DEFAULT now(),
  last_seen_at                   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_isu_skater_event_history_skater
  ON ingest.isu_skater_event_history(skaters_id);

CREATE INDEX IF NOT EXISTS idx_isu_skater_event_history_event_api_id
  ON ingest.isu_skater_event_history(event_api_id);

CREATE INDEX IF NOT EXISTS idx_isu_skater_event_history_name_date
  ON ingest.isu_skater_event_history(event_name, event_date);

CREATE OR REPLACE VIEW analytics.v_isu_api_result_url_candidates AS
SELECT
  e.api_id,
  e.event_id,
  e.slug,
  e.name AS event_name,
  e.season,
  e.from_date,
  e.to_date,
  e.city,
  e.country_code,
  e.event_type_name,
  button.button_key,
  button.button_title,
  button.result_url,
  e.last_seen_at
FROM ingest.isu_api_event_catalog e
CROSS JOIN LATERAL (
  VALUES
    ('button_1', e.button_1_title, e.button_1_url),
    ('button_2', e.button_2_title, e.button_2_url),
    ('button_3', e.button_3_title, e.button_3_url)
) AS button(button_key, button_title, result_url)
WHERE button.result_url ~ '^https?://';
