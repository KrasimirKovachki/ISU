-- ISU public API skater directory.
--
-- The api.isu-skating.com endpoints expose a paginated, stable JSON catalog of
-- every skater the ISU promotes (singles + teams). This migration captures that
-- catalog so it can be cross-linked with our import-derived skater data.
--
-- Three tables form the directory:
--   1) isu_skater_directory       - one row per public-facing slug (singles or
--                                   team). Carries display fields, photo URLs,
--                                   and the raw payload.
--   2) isu_skater_person          - one row per real person, keyed on the ISU
--                                   `api_id` UUID. Singles and team-member
--                                   records both flatten into here.
--   3) isu_skater_directory_member- many-to-many link slug <-> person (1:1 for
--                                   singles, 2:1 for pairs/dance).
--
-- Imported `core.skaters` rows are NOT touched. A separate matching layer can
-- map them to `isu_skater_person.api_id` later.

CREATE SCHEMA IF NOT EXISTS ingest;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS ingest.isu_skater_directory (
  slug              text PRIMARY KEY,
  entry_type        text NOT NULL CHECK (entry_type IN ('single', 'team_pair', 'team_dance')),
  discipline_id     int  NOT NULL,
  discipline_title  text,
  skaters_team_id   bigint,
  name              text NOT NULL,
  couple_name       text,
  gender            text,
  nationality_code  text,
  organization_code text,
  image_url         text,
  thumbnail_url     text,
  is_top_display    boolean,
  source_status     text,
  source_created_at timestamptz,
  source_updated_at timestamptz,
  raw               jsonb NOT NULL,
  first_seen_at     timestamptz NOT NULL DEFAULT now(),
  last_seen_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_isu_skater_directory_entry_type
  ON ingest.isu_skater_directory(entry_type);
CREATE INDEX IF NOT EXISTS idx_isu_skater_directory_nation
  ON ingest.isu_skater_directory(nationality_code);
CREATE INDEX IF NOT EXISTS idx_isu_skater_directory_team_id
  ON ingest.isu_skater_directory(skaters_team_id) WHERE skaters_team_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS ingest.isu_skater_person (
  api_id            uuid PRIMARY KEY,
  skaters_id        bigint UNIQUE,
  isu_id            text,
  discipline_id     int,
  first_name        text,
  last_name         text,
  full_name         text,
  gender            text,
  nationality_code  text,
  organization_code text,
  date_of_birth     date,
  image_url         text,
  thumbnail_url     text,
  source_status     text,
  source_created_at timestamptz,
  source_updated_at timestamptz,
  raw               jsonb NOT NULL,
  first_seen_at     timestamptz NOT NULL DEFAULT now(),
  last_seen_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_isu_skater_person_nation
  ON ingest.isu_skater_person(nationality_code);
CREATE INDEX IF NOT EXISTS idx_isu_skater_person_full_name_lower
  ON ingest.isu_skater_person ((lower(full_name)));
CREATE INDEX IF NOT EXISTS idx_isu_skater_person_isu_id
  ON ingest.isu_skater_person(isu_id) WHERE isu_id IS NOT NULL AND isu_id <> '';

CREATE TABLE IF NOT EXISTS ingest.isu_skater_directory_member (
  slug          text NOT NULL REFERENCES ingest.isu_skater_directory(slug) ON DELETE CASCADE,
  api_id        uuid NOT NULL REFERENCES ingest.isu_skater_person(api_id) ON DELETE CASCADE,
  position      smallint NOT NULL DEFAULT 1 CHECK (position BETWEEN 1 AND 4),
  first_seen_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (slug, api_id)
);

CREATE INDEX IF NOT EXISTS idx_isu_skater_directory_member_api_id
  ON ingest.isu_skater_directory_member(api_id);

-- Friendly denormalized view: one row per directory slug with one row of
-- per-person fields for singles and a member-array aggregate for teams.
CREATE OR REPLACE VIEW analytics.v_isu_skater_directory AS
WITH members AS (
  SELECT
    m.slug,
    m.position,
    p.api_id,
    p.skaters_id,
    p.isu_id,
    p.first_name,
    p.last_name,
    p.full_name,
    p.gender,
    p.nationality_code,
    p.organization_code,
    p.date_of_birth
  FROM ingest.isu_skater_directory_member m
  JOIN ingest.isu_skater_person p USING (api_id)
)
SELECT
  d.slug,
  d.entry_type,
  d.name,
  d.couple_name,
  d.discipline_title,
  d.gender,
  d.nationality_code,
  d.image_url,
  d.thumbnail_url,
  d.is_top_display,
  d.skaters_team_id,
  d.source_updated_at,
  COALESCE(
    jsonb_agg(
      jsonb_build_object(
        'position', members.position,
        'api_id', members.api_id,
        'skaters_id', members.skaters_id,
        'isu_id', members.isu_id,
        'full_name', members.full_name,
        'first_name', members.first_name,
        'last_name', members.last_name,
        'gender', members.gender,
        'nationality_code', members.nationality_code,
        'organization_code', members.organization_code,
        'date_of_birth', members.date_of_birth
      )
      ORDER BY members.position
    ) FILTER (WHERE members.api_id IS NOT NULL),
    '[]'::jsonb
  ) AS members,
  d.first_seen_at,
  d.last_seen_at
FROM ingest.isu_skater_directory d
LEFT JOIN members USING (slug)
GROUP BY d.slug;
