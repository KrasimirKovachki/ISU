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
    ('Ice Dance Denkova Stavinski', 'BUL', 'high', 'Includes ID DS, IDS, DS, Ice, and Dance/Ice Dance source variants.'),
    ('Ledeni Iskri', 'BUL', 'high', 'Includes L Isk, LI, LEI, and shortened Ledeni/Leden variants.'),
    ('Slavia', 'BUL', 'high', 'Includes Slv and Slavi abbreviations.'),
    ('Odesos Varna', 'BUL', 'high', 'Canonical Bulgarian club for Odes/Odeso/Odesos/Odesos Varna/ODE/ODS source values when skater nation is BUL.'),
    ('ODS', 'UKR', 'high', 'Canonical for Odes/Odeso/Odesos/Odesos Varna/ODE/ODS source values when skater nation is UKR.'),
    ('Fokstrot na led', 'BUL', 'high', 'Includes Fokst and Fokstrot.'),
    ('Ledeni', 'BUL', 'high', 'Includes Varna source value.'),
    ('Ledeni Sportove Varna', 'BUL', 'high', 'Kept as its own source club when present.'),
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
    ('Elit', 'E', 'medium', 'Short source abbreviation requested by data owner.'),
    ('Ice Peak', 'Ice Peak', 'high', NULL),
    ('Ice Peak', 'Ice P', 'high', 'Old source abbreviation.'),
    ('Ice Peak', 'IP', 'high', 'Short source abbreviation.'),
    ('Ice Dance Denkova Stavinski', 'Ice Dance Denkova Stavinski', 'high', NULL),
    ('Ice Dance Denkova Stavinski', 'Ice Dance DS', 'high', NULL),
    ('Ice Dance Denkova Stavinski', 'Dance on Ice DS', 'high', NULL),
    ('Ice Dance Denkova Stavinski', 'Dance on ice DS', 'high', NULL),
    ('Ice Dance Denkova Stavinski', 'ID DS', 'high', 'Old source abbreviation.'),
    ('Ice Dance Denkova Stavinski', 'IDS', 'high', 'Short source abbreviation.'),
    ('Ice Dance Denkova Stavinski', 'DS', 'medium', 'Ambiguous short source abbreviation; mapped to dominant imported usage.'),
    ('Ice Dance Denkova Stavinski', 'Ice', 'medium', 'Short source abbreviation requested by data owner.'),
    ('Ice Dance Denkova Stavinski', 'ID DS/ID DS', 'high', 'Duplicated source display value.'),
    ('Ice Dance Denkova Stavinski', 'Ice Dance DS / Ice Dance DS', 'high', 'Duplicated source display value.'),
    ('Ice Dance Denkova Stavinski', 'Dance on Ice DS / Dance on Ice DS', 'high', 'Duplicated source display value.'),
    ('Ice Dance Denkova Stavinski', 'Ice D', 'medium', 'Short source abbreviation.'),
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
    ('Odesos Varna', 'Odes', 'high', 'Shortened source value.'),
    ('Odesos Varna', 'ODE', 'high', 'Short source abbreviation.'),
    ('Odesos Varna', 'Odesos/Odesos', 'high', 'Duplicated source display value.'),
    ('ODS', 'ODS', 'high', NULL),
    ('Fokstrot na led', 'Fokstrot na led', 'high', NULL),
    ('Fokstrot na led', 'Fokstrot', 'high', NULL),
    ('Fokstrot na led', 'Fokst', 'high', 'Shortened source value.'),
    ('Ledeni', 'Ledeni', 'high', NULL),
    ('Ledeni', 'Varna', 'high', 'Source display value requested by data owner.'),
    ('Ledeni Sportove Varna', 'Ledeni Sportove Varna', 'high', NULL),
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
    ('Ice Star', 'Ledena Zvezda', 'high', 'Bulgarian source name mapped to Ice Star.'),
    ('Ice Star', 'IS', 'medium', 'Short source abbreviation.'),
    ('Ice Blade', 'Ice Blade', 'high', NULL),
    ('Ice Blade', 'IB', 'medium', 'Short source abbreviation.'),
    ('Avangard', 'Avangard', 'high', NULL),
    ('Axel', 'Axel', 'high', NULL),
    ('Artiko-NSA', 'Artiko-NSA', 'high', NULL),
    ('Sportna Sofia', 'Sportna Sofia', 'medium', NULL),
    ('Stars of Sofia', 'Stars of Sofia', 'medium', NULL),
    ('Kredo', 'Kredo', 'high', NULL),
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

WITH alias_seed(canonical_name, alias_name) AS (
  VALUES
    ('Elit', 'Elit'),
    ('Elit', 'Elite'),
    ('Elit', 'ELI'),
    ('Elit', 'E'),
    ('Ice Peak', 'Ice Peak'),
    ('Ice Peak', 'Ice P'),
    ('Ice Peak', 'IP'),
    ('Ice Dance Denkova Stavinski', 'Ice Dance Denkova Stavinski'),
    ('Ice Dance Denkova Stavinski', 'Ice Dance DS'),
    ('Ice Dance Denkova Stavinski', 'Dance on Ice DS'),
    ('Ice Dance Denkova Stavinski', 'Dance on ice DS'),
    ('Ice Dance Denkova Stavinski', 'ID DS'),
    ('Ice Dance Denkova Stavinski', 'IDS'),
    ('Ice Dance Denkova Stavinski', 'DS'),
    ('Ice Dance Denkova Stavinski', 'Ice'),
    ('Ice Dance Denkova Stavinski', 'ID DS/ID DS'),
    ('Ice Dance Denkova Stavinski', 'Ice Dance DS / Ice Dance DS'),
    ('Ice Dance Denkova Stavinski', 'Dance on Ice DS / Dance on Ice DS'),
    ('Ice Dance Denkova Stavinski', 'Ice D'),
    ('Ledeni Iskri', 'Ledeni Iskri'),
    ('Ledeni Iskri', 'L Isk'),
    ('Ledeni Iskri', 'LI'),
    ('Ledeni Iskri', 'LEI'),
    ('Ledeni Iskri', 'Leden'),
    ('Slavia', 'Slavia'),
    ('Slavia', 'Slv'),
    ('Slavia', 'Slavi'),
    ('Slavia', 'S'),
    ('Odesos Varna', 'Odesos Varna'),
    ('Odesos Varna', 'Odesos'),
    ('Odesos Varna', 'Odeso'),
    ('Odesos Varna', 'Odes'),
    ('Odesos Varna', 'ODE'),
    ('Odesos Varna', 'Odesos/Odesos'),
    ('ODS', 'ODS'),
    ('Fokstrot na led', 'Fokstrot na led'),
    ('Fokstrot na led', 'Fokstrot'),
    ('Fokstrot na led', 'Fokst'),
    ('Ledeni', 'Ledeni'),
    ('Ledeni', 'Varna'),
    ('Ledeni Sportove Varna', 'Ledeni Sportove Varna'),
    ('Iceberg Sulis', 'Iceberg Sulis'),
    ('Iceberg Sulis', 'Iceberg-Sulis'),
    ('Iceberg Sulis', 'Iceberg S'),
    ('Iceberg Sulis', 'Icebe'),
    ('Kanki Sofia', 'Kanki Sofia'),
    ('Kanki Sofia', 'Kanki'),
    ('Kanki Sofia', 'Kanki Sofia / Kanki Sofia'),
    ('Kanki Sofia', 'K Sof'),
    ('Levski', 'Levski'),
    ('Levski', 'Levsk'),
    ('Levski', 'Lvsk'),
    ('Balkan Botevgrad', 'Balkan Botevgrad'),
    ('Balkan Botevgrad', 'Balkan'),
    ('Balkan Botevgrad', 'Balka'),
    ('Balkan Botevgrad', 'BalkB'),
    ('Balkan Botevgrad', 'Bolak'),
    ('Ice Star', 'Ice Star'),
    ('Ice Star', 'Ice S'),
    ('Ice Star', 'Ledena Zvezda'),
    ('Ice Star', 'IS'),
    ('Ice Blade', 'Ice Blade'),
    ('Ice Blade', 'IB'),
    ('Avangard', 'Avangard'),
    ('Axel', 'Axel'),
    ('Artiko-NSA', 'Artiko-NSA'),
    ('Sportna Sofia', 'Sportna Sofia'),
    ('Stars of Sofia', 'Stars of Sofia'),
    ('Kredo', 'Kredo'),
    ('CSU BRASOV', 'CSU BRASOV'),
    ('LPS BRASOV', 'LPS BRASOV'),
    ('TRIUMF BUCURESTI', 'TRIUMF BUCURESTI')
)
DELETE FROM core.club_aliases ca
WHERE ca.source = 'manual_seed'
  AND NOT EXISTS (
    SELECT 1
    FROM alias_seed seed
    WHERE ca.alias_normalized = lower(regexp_replace(trim(seed.alias_name), '\s+', ' ', 'g'))
  );

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
  CASE
    WHEN ca.alias_normalized IN ('odes', 'odeso', 'odesos', 'odesos varna', 'ode', 'ods', 'odesos/odesos') AND sac.nation_code = 'UKR' THEN ods.id
    WHEN ca.alias_normalized IN ('odes', 'odeso', 'odesos', 'odesos varna', 'ode', 'ods', 'odesos/odesos') AND sac.nation_code = 'BUL' THEN odesos.id
    ELSE base_club.id
  END AS canonical_club_id,
  CASE
    WHEN ca.alias_normalized IN ('odes', 'odeso', 'odesos', 'odesos varna', 'ode', 'ods', 'odesos/odesos') AND sac.nation_code = 'UKR' THEN ods.public_id
    WHEN ca.alias_normalized IN ('odes', 'odeso', 'odesos', 'odesos varna', 'ode', 'ods', 'odesos/odesos') AND sac.nation_code = 'BUL' THEN odesos.public_id
    ELSE base_club.public_id
  END AS canonical_club_public_id,
  CASE
    WHEN ca.alias_normalized IN ('odes', 'odeso', 'odesos', 'odesos varna', 'ode', 'ods', 'odesos/odesos') AND sac.nation_code = 'UKR' THEN ods.canonical_name
    WHEN ca.alias_normalized IN ('odes', 'odeso', 'odesos', 'odesos varna', 'ode', 'ods', 'odesos/odesos') AND sac.nation_code = 'BUL' THEN odesos.canonical_name
    ELSE base_club.canonical_name
  END AS canonical_club_name,
  CASE
    WHEN ca.alias_normalized IN ('odes', 'odeso', 'odesos', 'odesos varna', 'ode', 'ods', 'odesos/odesos') AND sac.nation_code = 'UKR' THEN ods.country_code
    WHEN ca.alias_normalized IN ('odes', 'odeso', 'odesos', 'odesos varna', 'ode', 'ods', 'odesos/odesos') AND sac.nation_code = 'BUL' THEN odesos.country_code
    ELSE base_club.country_code
  END AS canonical_club_country_code,
  CASE
    WHEN ca.alias_normalized IN ('odes', 'odeso', 'odesos', 'odesos varna', 'ode', 'ods', 'odesos/odesos') AND sac.nation_code = 'UKR' THEN ods.status
    WHEN ca.alias_normalized IN ('odes', 'odeso', 'odesos', 'odesos varna', 'ode', 'ods', 'odesos/odesos') AND sac.nation_code = 'BUL' THEN odesos.status
    ELSE base_club.status
  END AS canonical_club_status,
  COALESCE(ca.confidence, suffix_alias.confidence) AS club_alias_confidence
FROM analytics.v_skater_appearances_clean sac
LEFT JOIN core.club_aliases ca
  ON ca.alias_normalized = lower(regexp_replace(trim(sac.club_name), '\s+', ' ', 'g'))
LEFT JOIN core.clubs c ON c.id = ca.club_id
LEFT JOIN LATERAL (
  SELECT ca2.*
  FROM core.club_aliases ca2
  WHERE c.id IS NULL
    AND char_length(ca2.alias_name) >= 4
    AND upper(regexp_replace(trim(sac.skater_name), '\s+', ' ', 'g')) LIKE '% ' || upper(ca2.alias_name)
  ORDER BY char_length(ca2.alias_name) DESC
  LIMIT 1
) suffix_alias ON true
LEFT JOIN core.clubs suffix_club ON suffix_club.id = suffix_alias.club_id
LEFT JOIN core.clubs base_club ON base_club.id = COALESCE(c.id, suffix_club.id)
LEFT JOIN core.clubs ods ON ods.canonical_name = 'ODS'
LEFT JOIN core.clubs odesos ON odesos.canonical_name = 'Odesos Varna';

CREATE OR REPLACE VIEW analytics.v_skater_category_results_with_club AS
SELECT
  scr.*,
  CASE
    WHEN ca.alias_normalized IN ('odes', 'odeso', 'odesos', 'odesos varna', 'ode', 'ods', 'odesos/odesos') AND scr.nation_code = 'UKR' THEN ods.id
    WHEN ca.alias_normalized IN ('odes', 'odeso', 'odesos', 'odesos varna', 'ode', 'ods', 'odesos/odesos') AND scr.nation_code = 'BUL' THEN odesos.id
    ELSE base_club.id
  END AS canonical_club_id,
  CASE
    WHEN ca.alias_normalized IN ('odes', 'odeso', 'odesos', 'odesos varna', 'ode', 'ods', 'odesos/odesos') AND scr.nation_code = 'UKR' THEN ods.public_id
    WHEN ca.alias_normalized IN ('odes', 'odeso', 'odesos', 'odesos varna', 'ode', 'ods', 'odesos/odesos') AND scr.nation_code = 'BUL' THEN odesos.public_id
    ELSE base_club.public_id
  END AS canonical_club_public_id,
  CASE
    WHEN ca.alias_normalized IN ('odes', 'odeso', 'odesos', 'odesos varna', 'ode', 'ods', 'odesos/odesos') AND scr.nation_code = 'UKR' THEN ods.canonical_name
    WHEN ca.alias_normalized IN ('odes', 'odeso', 'odesos', 'odesos varna', 'ode', 'ods', 'odesos/odesos') AND scr.nation_code = 'BUL' THEN odesos.canonical_name
    ELSE base_club.canonical_name
  END AS canonical_club_name,
  CASE
    WHEN ca.alias_normalized IN ('odes', 'odeso', 'odesos', 'odesos varna', 'ode', 'ods', 'odesos/odesos') AND scr.nation_code = 'UKR' THEN ods.country_code
    WHEN ca.alias_normalized IN ('odes', 'odeso', 'odesos', 'odesos varna', 'ode', 'ods', 'odesos/odesos') AND scr.nation_code = 'BUL' THEN odesos.country_code
    ELSE base_club.country_code
  END AS canonical_club_country_code,
  CASE
    WHEN ca.alias_normalized IN ('odes', 'odeso', 'odesos', 'odesos varna', 'ode', 'ods', 'odesos/odesos') AND scr.nation_code = 'UKR' THEN ods.status
    WHEN ca.alias_normalized IN ('odes', 'odeso', 'odesos', 'odesos varna', 'ode', 'ods', 'odesos/odesos') AND scr.nation_code = 'BUL' THEN odesos.status
    ELSE base_club.status
  END AS canonical_club_status,
  COALESCE(ca.confidence, suffix_alias.confidence) AS club_alias_confidence
FROM analytics.v_skater_category_results scr
LEFT JOIN core.club_aliases ca
  ON ca.alias_normalized = lower(regexp_replace(trim(scr.club_name), '\s+', ' ', 'g'))
LEFT JOIN core.clubs c ON c.id = ca.club_id
LEFT JOIN LATERAL (
  SELECT ca2.*
  FROM core.club_aliases ca2
  WHERE c.id IS NULL
    AND char_length(ca2.alias_name) >= 4
    AND upper(regexp_replace(trim(scr.skater_name), '\s+', ' ', 'g')) LIKE '% ' || upper(ca2.alias_name)
  ORDER BY char_length(ca2.alias_name) DESC
  LIMIT 1
) suffix_alias ON true
LEFT JOIN core.clubs suffix_club ON suffix_club.id = suffix_alias.club_id
LEFT JOIN core.clubs base_club ON base_club.id = COALESCE(c.id, suffix_club.id)
LEFT JOIN core.clubs ods ON ods.canonical_name = 'ODS'
LEFT JOIN core.clubs odesos ON odesos.canonical_name = 'Odesos Varna';
