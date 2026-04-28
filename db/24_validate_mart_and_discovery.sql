-- Validation checks for STAR mart, archive evidence, and discovery catalog.

SELECT
  'mart.dim_skater' AS check_name,
  count(*) AS row_count
FROM mart.dim_skater
UNION ALL
SELECT 'mart.fact_competition_result', count(*) FROM mart.fact_competition_result
UNION ALL
SELECT 'mart.fact_segment_score', count(*) FROM mart.fact_segment_score
UNION ALL
SELECT 'mart.fact_element_score', count(*) FROM mart.fact_element_score
UNION ALL
SELECT 'mart.v_skater_personal_bests', count(*) FROM mart.v_skater_personal_bests
UNION ALL
SELECT 'mart.v_skater_source_evidence', count(*) FROM mart.v_skater_source_evidence
ORDER BY check_name;

SELECT
  archive_status,
  document_type,
  count(*) AS source_documents
FROM ingest.source_archive_files
GROUP BY archive_status, document_type
ORDER BY archive_status, document_type;

SELECT
  count(*) FILTER (WHERE archive_path IS NOT NULL) AS evidence_with_archive_path,
  count(*) AS evidence_rows
FROM mart.v_skater_source_evidence;

SELECT
  source_name,
  discovery_status,
  country_code,
  count(*) AS events
FROM ingest.event_discovery_catalog
GROUP BY source_name, discovery_status, country_code
ORDER BY source_name, discovery_status, country_code;

SELECT
  count(*) FILTER (WHERE skater_name ~ '( VARNA| ELIT| LEDENI ISKRI| ICE PEAK| DANCE ON ICE DS)$') AS likely_club_suffix_names,
  count(*) FILTER (WHERE nation_code IS NOT NULL AND nation_code !~ '^[A-Z]{3}$') AS non_iso_nation_codes,
  count(*) FILTER (WHERE club_name ~ '^[A-Z]{3}$' AND nation_code IS NULL) AS iso_code_as_club_no_nation
FROM analytics.v_skater_segment_scores;
