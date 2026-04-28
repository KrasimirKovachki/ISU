-- Registry for URLs that need parsing validation/import decisions.
-- This keeps future source testing in data instead of hard-coded command lists.

CREATE TABLE IF NOT EXISTS ingest.source_url_registry (
  id bigserial PRIMARY KEY,
  url text NOT NULL UNIQUE,
  resolved_url text,
  source_profile_id bigint REFERENCES ingest.source_profiles(id),
  parser_profile text,
  competition_stream text,
  status text NOT NULL DEFAULT 'pending',
  validation_status text NOT NULL DEFAULT 'not_checked',
  last_validated_at timestamptz,
  last_import_run_id bigint REFERENCES ingest.import_runs(id) ON DELETE SET NULL,
  summary jsonb NOT NULL DEFAULT '{}'::jsonb,
  notes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (status IN ('pending', 'analyzed', 'ready_for_import', 'imported', 'skipped', 'failed')),
  CHECK (validation_status IN ('not_checked', 'passed', 'warning', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_source_url_registry_profile
  ON ingest.source_url_registry(source_profile_id);

CREATE INDEX IF NOT EXISTS idx_source_url_registry_status
  ON ingest.source_url_registry(status, validation_status);

INSERT INTO ingest.source_url_registry (
  url,
  resolved_url,
  source_profile_id,
  parser_profile,
  competition_stream,
  status,
  validation_status,
  summary,
  notes
)
SELECT
  seed.url,
  seed.resolved_url,
  sp.id,
  seed.parser_profile,
  seed.competition_stream,
  seed.status,
  seed.validation_status,
  seed.summary,
  seed.notes
FROM (
  VALUES
    (
      'https://cup.clubdenkovastaviski.com/2015/NonISU/index.htm',
      NULL,
      'denkova_staviski_2015_nonisu',
      'old_isucalcfs',
      'NonISU',
      'analyzed',
      'passed',
      '{"platform":"ISUCalcFS 3.1.2"}'::jsonb,
      'Analyzed only; not imported yet.'
    ),
    (
      'https://cup.clubdenkovastaviski.com/2015/ISU/index.htm',
      NULL,
      'denkova_staviski_2015_isu',
      'old_isucalcfs',
      'ISU',
      'analyzed',
      'passed',
      '{"platform":"ISUCalcFS 3.1.2"}'::jsonb,
      'Analyzed only; not imported yet.'
    ),
    (
      'https://cup.clubdenkovastaviski.com/2015/ISUCS/index.htm',
      NULL,
      'denkova_staviski_2015_isucs',
      'old_isucalcfs',
      'ISUCS',
      'analyzed',
      'passed',
      '{"platform":"ISUCalcFS 3.1.2"}'::jsonb,
      'Analyzed only; not imported yet.'
    ),
    (
      'https://cup.clubdenkovastaviski.com/2016/NonISU/index.htm',
      NULL,
      'denkova_staviski_2016_nonisu',
      'old_isucalcfs',
      'NonISU',
      'analyzed',
      'passed',
      '{"platform":"ISUCalcFS 3.2.5"}'::jsonb,
      'Analyzed only; not imported yet.'
    ),
    (
      'https://cup.clubdenkovastaviski.com/2016/ISU/index.htm',
      NULL,
      'denkova_staviski_2016_isu',
      'old_isucalcfs',
      'ISU',
      'analyzed',
      'passed',
      '{"platform":"ISUCalcFS 3.2.5"}'::jsonb,
      'Analyzed only; not imported yet.'
    ),
    (
      'https://cup.clubdenkovastaviski.com/2017/NonISU/index.htm',
      NULL,
      'denkova_staviski_2017_nonisu',
      'old_isucalcfs',
      'NonISU',
      'analyzed',
      'passed',
      '{"platform":"ISUCalcFS 3.3.3"}'::jsonb,
      'Analyzed only; not imported yet.'
    ),
    (
      'https://cup.clubdenkovastaviski.com/2017/ISU/index.htm',
      NULL,
      'denkova_staviski_2017_isu',
      'old_isucalcfs',
      'ISU',
      'analyzed',
      'passed',
      '{"platform":"ISUCalcFS 3.3.3"}'::jsonb,
      'Analyzed only; not imported yet.'
    ),
    (
      'https://cup.clubdenkovastaviski.com/2018/NonISU/index.htm',
      'https://cup.clubdenkovastaviski.com/2018/NonISU/pages/main.htm',
      'denkova_staviski_2018_nonisu',
      'old_isucalcfs',
      'NonISU',
      'ready_for_import',
      'passed',
      '{"categories":9,"segments":11,"official_assignments":107,"judge_assignments":41,"pdf_link_type":"Time Schedule (pdf)"}'::jsonb,
      'JavaScript wrapper index; parser resolves to pages/main.htm. Index PDFs are time schedules, not judge scores.'
    ),
    (
      'https://cup.clubdenkovastaviski.com/2018/ISU/index.htm',
      'https://cup.clubdenkovastaviski.com/2018/ISU/pages/main.htm',
      'denkova_staviski_2018_isu',
      'old_isucalcfs',
      'ISU',
      'ready_for_import',
      'passed',
      '{"categories":4,"segments":8,"official_assignments":88,"judge_assignments":40,"pdf_link_type":"Time Schedule (pdf)"}'::jsonb,
      'JavaScript wrapper index; parser resolves to pages/main.htm. Index PDFs are time schedules, not judge scores.'
    ),
    (
      'https://cup.clubdenkovastaviski.com/2019/NonISU/index.html',
      'https://cup.clubdenkovastaviski.com/2019/NonISU/pages/main.html',
      'denkova_staviski_2019_nonisu',
      'old_isucalcfs',
      'NonISU',
      'ready_for_import',
      'passed',
      '{"categories":10,"segments":10,"official_assignments":90,"judge_assignments":30,"pdf_link_type":"Judges Scores (pdf)"}'::jsonb,
      'JavaScript wrapper index; parser resolves to pages/main.html. Index PDFs are judge scores.'
    ),
    (
      'https://cup.clubdenkovastaviski.com/2019/ISU/index.html',
      'https://cup.clubdenkovastaviski.com/2019/ISU/pages/main.html',
      'denkova_staviski_2019_isu',
      'old_isucalcfs',
      'ISU',
      'ready_for_import',
      'passed',
      '{"categories":5,"segments":10,"official_assignments":110,"judge_assignments":50,"pdf_link_type":"Time Schedule (pdf)"}'::jsonb,
      'JavaScript wrapper index; parser resolves to pages/main.html. Index PDFs are time schedules, not judge scores.'
    )
) AS seed(url, resolved_url, profile_key, parser_profile, competition_stream, status, validation_status, summary, notes)
JOIN ingest.source_profiles sp ON sp.profile_key = seed.profile_key
ON CONFLICT (url) DO UPDATE SET
  resolved_url = EXCLUDED.resolved_url,
  source_profile_id = EXCLUDED.source_profile_id,
  parser_profile = EXCLUDED.parser_profile,
  competition_stream = EXCLUDED.competition_stream,
  status = EXCLUDED.status,
  validation_status = EXCLUDED.validation_status,
  summary = EXCLUDED.summary,
  notes = EXCLUDED.notes,
  updated_at = now();
