-- Canonical club registry and aliases.
--
-- Imported source values remain unchanged in ingest.source_skater_appearances.
-- This layer maps source club strings such as "Ice P" or "ID DS" to reusable
-- club records for BI, API, coach dashboards, and future club management.

CREATE TABLE IF NOT EXISTS core.clubs (
  id bigserial PRIMARY KEY,
  public_id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  canonical_name text NOT NULL UNIQUE,
  country_code text,
  status text NOT NULL DEFAULT 'active',
  confidence text NOT NULL DEFAULT 'review',
  notes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (status IN ('active', 'provisional', 'merged', 'inactive')),
  CHECK (confidence IN ('high', 'medium', 'low', 'review'))
);

CREATE TABLE IF NOT EXISTS core.club_aliases (
  id bigserial PRIMARY KEY,
  club_id bigint NOT NULL REFERENCES core.clubs(id) ON DELETE CASCADE,
  alias_name text NOT NULL,
  alias_normalized text GENERATED ALWAYS AS (
    lower(regexp_replace(trim(alias_name), '\s+', ' ', 'g'))
  ) STORED,
  source text NOT NULL DEFAULT 'imported',
  confidence text NOT NULL DEFAULT 'review',
  notes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (alias_normalized),
  CHECK (source IN ('manual_seed', 'imported', 'inferred')),
  CHECK (confidence IN ('high', 'medium', 'low', 'review'))
);

CREATE INDEX IF NOT EXISTS idx_club_aliases_club_id ON core.club_aliases(club_id);

WITH canonical(canonical_name, country_code, confidence, notes) AS (
  VALUES
    ('Elit', 'BUL', 'high', 'Canonical kept as source spelling; alias Elite appears in some sources.'),
    ('Ice Peak', 'BUL', 'high', 'Includes old abbreviations Ice P and IP.'),
    ('Ice Dance Denkova-Staviski', 'BUL', 'high', 'Includes ID DS, IDS, DS, and Dance/Ice Dance source variants.'),
    ('Ledeni Iskri', 'BUL', 'high', 'Includes L Isk, LI, LEI, and shortened Ledeni/Leden variants.'),
    ('Slavia', 'BUL', 'high', 'Includes Slv and Slavi abbreviations.'),
    ('Odesos Varna', 'BUL', 'high', 'Includes Odes, Odeso, Odesos, ODE, and ODS.'),
    ('Fokstrot', 'BUL', 'high', 'Includes Fokst and Fokstrot na led.'),
    ('Varna', 'BUL', 'high', 'Includes V abbreviation.'),
    ('Iceberg Sulis', 'BUL', 'high', 'Includes Iceberg S and Iceberg-Sulis.'),
    ('Kanki Sofia', 'BUL', 'high', 'Includes Kanki and K Sof.'),
    ('Levski', 'BUL', 'high', 'Includes Levsk and Lvsk.'),
    ('Balkan Botevgrad', 'BUL', 'medium', 'Includes Balka, Balkan, BalkB, and Bolak source variants.'),
    ('Ice Star', 'BUL', 'medium', 'Includes Ice S and IS.'),
    ('Ice Blade', 'BUL', 'medium', 'Includes IB source abbreviation.'),
    ('Avangard', 'BUL', 'high', NULL),
    ('Axel', 'BUL', 'high', NULL),
    ('Artiko-NSA', 'BUL', 'high', NULL),
    ('Sportna Sofia', 'BUL', 'medium', NULL),
    ('Stars of Sofia', 'BUL', 'medium', NULL),
    ('Kredo', 'BUL', 'high', NULL),
    ('Ledena Zvezda', 'BUL', 'high', NULL),
    ('CSU BRASOV', 'ROU', 'high', NULL),
    ('LPS BRASOV', 'ROU', 'high', NULL),
    ('TRIUMF BUCURESTI', 'ROU', 'high', NULL)
)
INSERT INTO core.clubs (canonical_name, country_code, confidence, notes)
SELECT canonical_name, country_code, confidence, notes
FROM canonical
ON CONFLICT (canonical_name) DO UPDATE SET
  country_code = EXCLUDED.country_code,
  confidence = EXCLUDED.confidence,
  notes = EXCLUDED.notes,
  updated_at = now();

WITH alias_seed(canonical_name, alias_name, confidence, notes) AS (
  VALUES
    ('Elit', 'Elit', 'high', NULL),
    ('Elit', 'Elite', 'high', NULL),
    ('Elit', 'ELI', 'medium', 'Short source abbreviation.'),
    ('Ice Peak', 'Ice Peak', 'high', NULL),
    ('Ice Peak', 'Ice P', 'high', 'Old source abbreviation.'),
    ('Ice Peak', 'IP', 'high', 'Short source abbreviation.'),
    ('Ice Dance Denkova-Staviski', 'Ice Dance DS', 'high', NULL),
    ('Ice Dance Denkova-Staviski', 'Dance on Ice DS', 'high', NULL),
    ('Ice Dance Denkova-Staviski', 'Dance on ice DS', 'high', NULL),
    ('Ice Dance Denkova-Staviski', 'ID DS', 'high', 'Old source abbreviation.'),
    ('Ice Dance Denkova-Staviski', 'IDS', 'high', 'Short source abbreviation.'),
    ('Ice Dance Denkova-Staviski', 'DS', 'medium', 'Ambiguous short source abbreviation; mapped to dominant imported usage.'),
    ('Ice Dance Denkova-Staviski', 'ID DS/ID DS', 'high', 'Duplicated source display value.'),
    ('Ice Dance Denkova-Staviski', 'Ice Dance DS / Ice Dance DS', 'high', 'Duplicated source display value.'),
    ('Ice Dance Denkova-Staviski', 'Dance on Ice DS / Dance on Ice DS', 'high', 'Duplicated source display value.'),
    ('Ice Dance Denkova-Staviski', 'Ice D', 'medium', 'Short source abbreviation.'),
    ('Ledeni Iskri', 'Ledeni Iskri', 'high', NULL),
    ('Ledeni Iskri', 'L Isk', 'high', 'Old source abbreviation.'),
    ('Ledeni Iskri', 'LI', 'high', 'Short source abbreviation.'),
    ('Ledeni Iskri', 'LEI', 'high', 'Short source abbreviation.'),
    ('Ledeni Iskri', 'Leden', 'medium', 'Shortened source value.'),
    ('Slavia', 'Slavia', 'high', NULL),
    ('Slavia', 'Slv', 'high', 'Old source abbreviation.'),
    ('Slavia', 'Slavi', 'high', 'Shortened source value.'),
    ('Slavia', 'S', 'low', 'Very short source abbreviation; review if new clubs appear.'),
    ('Odesos Varna', 'Odesos Varna', 'high', NULL),
    ('Odesos Varna', 'Odesos', 'high', NULL),
    ('Odesos Varna', 'Odeso', 'high', 'Shortened source value.'),
    ('Odesos Varna', 'Odes', 'high', 'Old source abbreviation.'),
    ('Odesos Varna', 'ODE', 'high', 'Short source abbreviation.'),
    ('Odesos Varna', 'ODS', 'high', 'Short source abbreviation.'),
    ('Odesos Varna', 'Odesos/Odesos', 'high', 'Duplicated source display value.'),
    ('Fokstrot', 'Fokstrot', 'high', NULL),
    ('Fokstrot', 'Fokst', 'high', 'Shortened source value.'),
    ('Fokstrot', 'Fokstrot na led', 'high', NULL),
    ('Varna', 'Varna', 'high', NULL),
    ('Varna', 'V', 'low', 'Very short source abbreviation; review if new clubs appear.'),
    ('Iceberg Sulis', 'Iceberg Sulis', 'high', NULL),
    ('Iceberg Sulis', 'Iceberg-Sulis', 'high', NULL),
    ('Iceberg Sulis', 'Iceberg S', 'high', NULL),
    ('Iceberg Sulis', 'Icebe', 'medium', 'Shortened source value.'),
    ('Kanki Sofia', 'Kanki Sofia', 'high', NULL),
    ('Kanki Sofia', 'Kanki', 'high', NULL),
    ('Kanki Sofia', 'Kanki Sofia / Kanki Sofia', 'high', 'Duplicated source display value.'),
    ('Kanki Sofia', 'K Sof', 'medium', 'Likely short source value.'),
    ('Levski', 'Levski', 'high', NULL),
    ('Levski', 'Levsk', 'high', 'Shortened source value.'),
    ('Levski', 'Lvsk', 'high', 'Shortened source value.'),
    ('Balkan Botevgrad', 'Balkan Botevgrad', 'high', NULL),
    ('Balkan Botevgrad', 'Balkan', 'medium', NULL),
    ('Balkan Botevgrad', 'Balka', 'medium', 'Shortened source value.'),
    ('Balkan Botevgrad', 'BalkB', 'medium', 'Short source abbreviation.'),
    ('Balkan Botevgrad', 'Bolak', 'low', 'Likely source spelling/abbreviation; review.'),
    ('Ice Star', 'Ice Star', 'high', NULL),
    ('Ice Star', 'Ice S', 'high', 'Short source value.'),
    ('Ice Star', 'IS', 'medium', 'Short source abbreviation.'),
    ('Ice Blade', 'Ice Blade', 'high', NULL),
    ('Ice Blade', 'IB', 'medium', 'Short source abbreviation.'),
    ('Avangard', 'Avangard', 'high', NULL),
    ('Axel', 'Axel', 'high', NULL),
    ('Artiko-NSA', 'Artiko-NSA', 'high', NULL),
    ('Sportna Sofia', 'Sportna Sofia', 'medium', NULL),
    ('Stars of Sofia', 'Stars of Sofia', 'medium', NULL),
    ('Kredo', 'Kredo', 'high', NULL),
    ('Ledena Zvezda', 'Ledena Zvezda', 'high', NULL),
    ('CSU BRASOV', 'CSU BRASOV', 'high', NULL),
    ('LPS BRASOV', 'LPS BRASOV', 'high', NULL),
    ('TRIUMF BUCURESTI', 'TRIUMF BUCURESTI', 'high', NULL)
)
INSERT INTO core.club_aliases (club_id, alias_name, source, confidence, notes)
SELECT c.id, a.alias_name, 'manual_seed', a.confidence, a.notes
FROM (
  SELECT DISTINCT ON (lower(regexp_replace(trim(alias_name), '\s+', ' ', 'g')))
    canonical_name,
    alias_name,
    confidence,
    notes
  FROM alias_seed
  ORDER BY lower(regexp_replace(trim(alias_name), '\s+', ' ', 'g')), confidence
) a
JOIN core.clubs c ON c.canonical_name = a.canonical_name
ON CONFLICT (alias_normalized) DO UPDATE SET
  club_id = EXCLUDED.club_id,
  source = EXCLUDED.source,
  confidence = EXCLUDED.confidence,
  notes = EXCLUDED.notes,
  updated_at = now();

-- Preserve every imported club source value. Unmapped aliases become
-- provisional one-to-one club records so no source club disappears from the
-- canonical layer.
WITH imported_aliases AS (
  SELECT DISTINCT trim(club_name) AS alias_name
  FROM ingest.source_skater_appearances
  WHERE NULLIF(trim(club_name), '') IS NOT NULL
),
missing_aliases AS (
  SELECT ia.alias_name
  FROM imported_aliases ia
  LEFT JOIN core.club_aliases ca
    ON ca.alias_normalized = lower(regexp_replace(trim(ia.alias_name), '\s+', ' ', 'g'))
  WHERE ca.id IS NULL
),
inserted_clubs AS (
  INSERT INTO core.clubs (canonical_name, country_code, status, confidence, notes)
  SELECT alias_name, NULL, 'provisional', 'review', 'Auto-created from imported club_name; needs manual canonical review.'
  FROM missing_aliases
  ON CONFLICT (canonical_name) DO NOTHING
  RETURNING id, canonical_name
)
INSERT INTO core.club_aliases (club_id, alias_name, source, confidence, notes)
SELECT c.id, ma.alias_name, 'imported', 'review', 'Auto-created from imported club_name.'
FROM missing_aliases ma
JOIN core.clubs c ON c.canonical_name = ma.alias_name
ON CONFLICT (alias_normalized) DO NOTHING;

CREATE OR REPLACE VIEW core.v_club_alias_usage AS
SELECT
  c.id AS club_id,
  c.public_id AS club_public_id,
  c.canonical_name,
  c.country_code,
  c.status,
  c.confidence AS club_confidence,
  ca.alias_name,
  ca.source AS alias_source,
  ca.confidence AS alias_confidence,
  count(ssa.id) AS imported_appearance_count,
  count(DISTINCT e.id) AS event_count,
  count(DISTINCT e.source_url) AS source_event_url_count,
  min(e.name) AS sample_event_name,
  min(e.source_url) AS sample_event_url
FROM core.club_aliases ca
JOIN core.clubs c ON c.id = ca.club_id
LEFT JOIN ingest.source_skater_appearances ssa
  ON lower(regexp_replace(trim(ssa.club_name), '\s+', ' ', 'g')) = ca.alias_normalized
LEFT JOIN ingest.events e ON e.id = ssa.event_id
GROUP BY
  c.id,
  c.public_id,
  c.canonical_name,
  c.country_code,
  c.status,
  c.confidence,
  ca.alias_name,
  ca.source,
  ca.confidence;

CREATE OR REPLACE VIEW analytics.v_skater_appearances_with_club AS
SELECT
  sac.*,
  c.id AS canonical_club_id,
  c.public_id AS canonical_club_public_id,
  c.canonical_name AS canonical_club_name,
  c.country_code AS canonical_club_country_code,
  c.status AS canonical_club_status,
  ca.confidence AS club_alias_confidence
FROM analytics.v_skater_appearances_clean sac
LEFT JOIN core.club_aliases ca
  ON ca.alias_normalized = lower(regexp_replace(trim(sac.club_name), '\s+', ' ', 'g'))
LEFT JOIN core.clubs c ON c.id = ca.club_id;

CREATE OR REPLACE VIEW analytics.v_skater_category_results_with_club AS
SELECT
  scr.*,
  c.id AS canonical_club_id,
  c.public_id AS canonical_club_public_id,
  c.canonical_name AS canonical_club_name,
  c.country_code AS canonical_club_country_code,
  c.status AS canonical_club_status,
  ca.confidence AS club_alias_confidence
FROM analytics.v_skater_category_results scr
LEFT JOIN core.club_aliases ca
  ON ca.alias_normalized = lower(regexp_replace(trim(scr.club_name), '\s+', ' ', 'g'))
LEFT JOIN core.clubs c ON c.id = ca.club_id;
