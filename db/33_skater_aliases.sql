-- Canonical skater-name registry for cross-source transliteration variants.
--
-- Different sources spell the same skater differently. Bulgarian federation
-- pages, Russian transliteration outputs, and the ISU site can render the
-- same Cyrillic name as e.g. "ALEKSANDRA FEYGIN" / "ALEXANDRA FEIGIN" /
-- "ALEXANDAR ZLATKOV" / "ALEKSANDAR ZLATKOV" / "BEAT SCHUEMPERLI" /
-- "BEAT SHYUMPERLI". Our identity key (consonants of last name + first
-- name) is too strict to merge those, so the same skater shows up as two
-- entries in the dashboard.
--
-- Pattern: (alias_normalized, canonical_name, nation_code) tuples. The
-- analytics.v_skater_appearances_clean view (db/17) joins this table and
-- substitutes ssa.normalized_name with the canonical_name when an alias
-- matches, so downstream identity-key generation collapses the variants
-- onto a single skater_key.

CREATE TABLE IF NOT EXISTS core.skater_aliases (
  id bigserial PRIMARY KEY,
  alias_normalized text NOT NULL,
  canonical_name text NOT NULL,
  nation_code text,
  notes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_skater_aliases_alias_nation
  ON core.skater_aliases (alias_normalized, COALESCE(nation_code, ''));
CREATE INDEX IF NOT EXISTS idx_skater_aliases_canonical
  ON core.skater_aliases (canonical_name);


-- Helper: a skater_alias_resolution view exposing the (alias -> canonical)
-- mapping with a default-empty-string nation key. The COALESCE-on-LHS pattern
-- lets analytics views do a single LEFT JOIN regardless of whether the alias
-- is nation-scoped.
CREATE OR REPLACE VIEW core.v_skater_aliases AS
SELECT
  alias_normalized,
  canonical_name,
  nation_code,
  notes
FROM core.skater_aliases;


-- Seeds: cases the user reported on 2026-05-09. nation_code intentionally
-- NULL where the canonical is universal across sources for that name.
INSERT INTO core.skater_aliases (alias_normalized, canonical_name, nation_code, notes) VALUES
  ('ALEKSANDRA FEYGIN', 'ALEXANDRA FEIGIN', NULL, 'Cyrillic-transliteration variant; both render the same Bulgarian skater (Ice Dance Denkova-Staviski).'),
  ('ALEKSANDRA FEIGIN', 'ALEXANDRA FEIGIN', NULL, 'Cyrillic-transliteration variant (Aleksandra spelling for the same FEIGIN skater).'),
  ('ALEKSANDAR ZLATKOV', 'ALEXANDAR ZLATKOV', NULL, 'Cyrillic-transliteration variant; same skater across BSF/Elit sources.'),
  ('ALEKSANDER ZLATKOV', 'ALEXANDAR ZLATKOV', NULL, 'Cyrillic-transliteration variant (Aleksander/Aleksandar with -er ending).'),
  ('BEAT SHYUMPERLI', 'BEAT SCHUEMPERLI', NULL, 'Cyrillic-transliteration variant; same skater across Elit sources.')
ON CONFLICT (alias_normalized, COALESCE(nation_code, '')) DO UPDATE SET
  canonical_name = EXCLUDED.canonical_name,
  nation_code = EXCLUDED.nation_code,
  notes = EXCLUDED.notes,
  updated_at = now();
