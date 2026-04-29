# Current Project State

This project imports figure skating competition/result data from old ISUCalcFS
and newer Swiss Timing FS Manager result sites into PostgreSQL.

## Database

Current VM target:

- Host: `192.168.1.18`
- Database: `skating_data`
- Application user: `skating_app`
- Local connection config: `config/local.env`

Setup and validation SQLs:

- `db/00_create_database_and_user.sql`
- `db/01_ingest_schema.sql`
- `db/02_seed_source_profiles.sql`
- `db/03_seed_validation_legend.sql`
- `db/04_validate_representation_rules.sql`
- `db/05_allow_event_reimports.sql`
- `db/06_source_url_registry.sql`
- `db/07_validate_missing_results_against_entries.sql`
- `db/08_mlad_figurist_test_results.sql`
- `db/10_validate_skater_name_quality.sql`
- `db/11_seed_bsf_source_profiles.sql`
- `db/12_allow_duplicate_segment_names.sql`
- `db/13_duplicate_import_audit.sql`
- `db/14_delete_duplicate_import_runs.sql`
- `db/15_skater_multi_competition_comparison.sql`
- `db/16_event_profile_representation_rules.sql`
- `db/17_analytics_views.sql`
- `db/18_validate_affiliation_quality.sql`
- `db/19_club_registry.sql`
- `db/validation_queries.sql`

Discovery scripts:

- `scripts/discover_bsf_national_championships.py` extracts figure-skating
  competition rows from the BSF national championships React bundle. It captures
  season, `Име`/competition title, competition URL, and protocol/result URLs
  derived from the `Протокол:` column behavior.
- `scripts/seed_bsf_registry.py` validates discovered BSF result URLs, creates
  per-event source profiles, and upserts rows into `ingest.source_url_registry`.
- `scripts/import_ready_registry.py` imports registry rows marked
  `ready_for_import/passed`. It now commits after each URL and rolls back failed
  URLs independently.

## Imported Sources

Validated imports so far:

- `https://cup.clubdenkovastaviski.com/2013/ISU/index.htm`
- `https://cup.clubdenkovastaviski.com/2014/ISU/index.htm`
- `https://cup.clubdenkovastaviski.com/2014/NonISU/index.htm`
- `https://cup.clubdenkovastaviski.com/2015/ISU/index.htm`
- `https://cup.clubdenkovastaviski.com/2015/NonISU/index.htm`
- `https://cup.clubdenkovastaviski.com/2015/ISUCS/index.htm`
- `https://cup.clubdenkovastaviski.com/2016/ISU/index.htm`
- `https://cup.clubdenkovastaviski.com/2016/NonISU/index.htm`
- `https://cup.clubdenkovastaviski.com/2017/ISU/index.htm`
- `https://cup.clubdenkovastaviski.com/2017/NonISU/index.htm`
- `https://cup.clubdenkovastaviski.com/2018/ISU/index.htm`
- `https://cup.clubdenkovastaviski.com/2018/NonISU/index.htm`
- `https://cup.clubdenkovastaviski.com/2019/ISU/index.html`
- `https://cup.clubdenkovastaviski.com/2019/NonISU/index.html`
- `https://cup.clubdenkovastaviski.com/2022/ISU/index.htm`
- `https://cup.clubdenkovastaviski.com/2022/NonISU/index.htm`
- `https://cup.clubdenkovastaviski.com/2023/ISU/index.htm`
- `https://cup.clubdenkovastaviski.com/2023/NonISU/index.htm`
- `https://cup.clubdenkovastaviski.com/2024/ISU/index.htm`
- `https://cup.clubdenkovastaviski.com/2024/NonISU/index.htm`
- `https://sofia-trophy.clubdenkovastaviski.com/2026/ISU/`
- `https://sofia-trophy.clubdenkovastaviski.com/2026/NonISU/`
- `https://www.bsf.bg/figure-skating/ice-peak-trophy/18-19.04.2026/index.htm`

Skipped/unavailable:

- `https://cup.clubdenkovastaviski.com/2020/ISU/index.htm`
- `https://cup.clubdenkovastaviski.com/2020/NonISU/index.htm`

The 2020 URLs and common candidate paths (`index`, `pages/main`, `CAT001`,
`SEG001`) return the same 1400-byte site fallback HTML rather than a result
index, so they are recorded in the registry as `skipped/failed` source
unavailable.

BSF national-championship discovery:

- Source page:
  `https://www.bsf.bg/figure-skating/national-championships`
- The rendered table is not present in the initial HTML. Data is embedded in the
  loaded `/assets/index-*.js` React bundle.
- The script currently discovers 56 competition rows and 60 result/protocol URLs.
- For ordinary rows, the protocol/result URL is `<competitionPath>/index.htm`.
- For Black Sea Ice Cup rows with multiple result streams, URLs are expanded to
  `/ISU/index.htm`, `/NonISU/index.htm`, and `/DP/index.htm`.
- For `Тест "Млад фигурист"` rows, the result URL is the linked `Result.pdf`.

BSF registry/import status as of 2026-04-27:

- Seeded BSF source profiles with `db/11_seed_bsf_source_profiles.sql`.
- Seeded discovered result URLs with `scripts/seed_bsf_registry.py`.
- Imported all 41 BSF discovered rows that validated as real result indexes:
  import runs 39-60, 62-80, and 82.
- Registry now has 0 `ready_for_import` rows, 61 imported rows, 16 skipped
  failed rows, and 4 analyzed warning rows.
- The 4 analyzed warning rows are direct `Тест "Млад фигурист"` `Result.pdf`
  URLs. They are discovered and retained in the registry, but root-PDF import is
  not yet the normal event-import path.
- The 16 skipped failed rows were discovered links that did not parse as result
  indexes, usually because the page has no categories or returns the BSF React
  fallback shell.
- Backfilled missing registry row for existing Sofia Trophy 2026 ISU import run
  4.
- `Priz Victoria 25-27.03.2016` required `db/12_allow_duplicate_segment_names.sql`
  because the source has duplicate segment display names inside one category
  (`Pattern Dance (With Keypoints)`). Segment identity is now
  `(category_id, source_order)`, with a non-unique `(category_id, name)` index.
- Validation after import: `db/10_validate_skater_name_quality.sql` returns
  zero rows; tests pass (`24 passed`).
These sources are configured locally with `old_isucalcfs` source profiles.
The 2015 sources use ISUCalcFS 3.1.2, 2016 uses ISUCalcFS 3.2.5, and 2017
uses ISUCalcFS 3.3.3.

2018 and 2019 use a JavaScript wrapper index that loads internal result pages
from `pages/main.htm` or `pages/main.html`. Parsing validation should target
those internal pages unless wrapper handling is added to the importer.

Latest parsing-only validation:

- 2018 NonISU internal page: 9 categories, 11 segments, 107 official assignments,
  41 judge assignments, no validation issues. Index PDF links are all
  `Time Schedule (pdf)`.
- 2018 ISU internal page: 4 categories, 8 segments, 88 official assignments,
  40 judge assignments, no validation issues. Index PDF links are all
  `Time Schedule (pdf)`.
- 2019 NonISU internal page: 10 categories, 10 segments, 90 official assignments,
  30 judge assignments, no validation issues. Index PDF links are all
  `Judges Scores (pdf)`.
- 2019 ISU internal page: 5 categories, 10 segments, 110 official assignments,
  50 judge assignments, no validation issues. Index PDF links are all
  `Time Schedule (pdf)`.

Imported 2015/2016/2017 old ISUCalcFS runs:

- Run 32: 2015 NonISU, completed. 4 categories, 4 segments, 41 entries, 41
  category results, 40 segment results, 36 official assignments, 40 old-style
  PDF score summaries, no parse issues.
- Run 33: 2015 ISU, completed. 8 categories, 13 segments, 119 entries, 119
  category results, 175 segment results, 137 official assignments, 175 old-style
  PDF score summaries, no parse issues.
- Run 34: 2015 ISUCS, completed. 3 categories, 6 segments, 32 entries, 32
  category results, 60 segment results, 78 official assignments, 60 old-style
  PDF score summaries, no parse issues.
- Run 35: 2016 NonISU, completed. 4 categories, 4 segments, 40 entries, 40
  category results, 39 segment results, 36 official assignments, 39 old-style
  PDF score summaries, no parse issues.
- Run 36: 2016 ISU, completed. 10 categories, 16 segments, 89 entries, 89
  category results, 134 segment results, 168 official assignments, 134 old-style
  PDF score summaries, no parse issues.
- Run 37: 2017 NonISU, completed. 8 categories, 8 segments, 72 entries, 71
  category results, 69 segment results, 72 official assignments, 69 old-style
  PDF score summaries. Two warnings remain for `Basic Novice B Boys`: category
  result page `CAT001RS.HTM` is blank or malformed, and
  `DSCup2017_BasicNoviceBBoys_FS_Scores.pdf` returns HTML instead of PDF bytes.
  This category has one entry (`Mihai PRISACARU`, ROU) but no published result
  table or judge-score PDF in the source.
- Run 38: 2017 ISU, completed. 6 categories, 12 segments, 76 entries, 76
  category results, 148 segment results, 132 official assignments, 148 old-style
  PDF score summaries, no parse issues.

Imported 2018/2019 runs:

- Run 18: 2018 ISU, completed after correcting old index PDF-link selection. 4
  categories, 8 segments, 39 entries, 39 category results, 72 segment results,
  88 official assignments, 72 old-style PDF score summaries.
- Run 20: 2018 NonISU, completed after correcting old index PDF-link selection.
  9 categories, 11 segments, 120 entries, 119 category results, 104 segment
  results, 107 official assignments, 146 old-style PDF score summaries. Two
  warnings remain: `Intermediate Novice Boys` category result page `CAT003RS.HTM`
  is blank or malformed, and `DS2018_IntermediateNoviceBoys_FS_Scores.pdf`
  returns HTML instead of PDF bytes. This category has one entry
  (`Filip KAYMAKCHIEV`, BUL) but no published result table or judge-score PDF in
  the source, so the warning should be treated as source empty/unpublished
  category output rather than import data loss.
- Run 19: 2019 ISU, completed after correcting old index PDF-link selection. 5
  categories, 10 segments, 54 entries, 54 category results, 108 segment results,
  110 official assignments, 108 old-style PDF score summaries.
- Run 14: 2019 NonISU, completed. 10 categories, 10 segments, 109 entries, 109
  category results, 108 segment results, 90 official assignments, 108 old-style
  PDF score summaries. Old-style PDFs currently import summary rows, not element
  rows.

Imported 2022/2023 FS Manager runs:

- Run 29: 2022 ISU, completed. 4 categories, 8 segments, 48 entries, 48 category
  results, 94 segment results, 88 official assignments, 91 PDF score summaries,
  no parse issues.
- Run 28: 2022 NonISU, completed. 12 categories, 14 segments, 207 entries, 207
  category results, 241 segment results, 124 official assignments, 237 PDF score
  summaries, no parse issues.
- Run 26: 2023 ISU, completed. 4 categories, 8 segments, 78 entries, 78 category
  results, 153 segment results, 88 official assignments, 146 PDF score summaries,
  no parse issues.
- Run 27: 2023 NonISU, completed. 13 categories, 15 segments, 197 entries, 197
  category results, 223 segment results, 139 official assignments, 209 PDF score
  summaries, no parse issues.

## Skater Name Quality

FS Manager segment result pages can include a `Qual.` column between `Pl.` and
`Name`. Earlier fixed-position parsing treated `Q` as the skater name and shifted
surname fragments into `nation_code`. The parser now reads segment results by
header and stores `Qual.` as `raw.qualification`.

Corrected reimports:

- Run 30: 2024 ISU, completed with no `Q`/blank-name rows and no parse issues.
- Run 31: Sofia Trophy 2026 NonISU, completed with no `Q`/blank-name rows and no
  parse issues.

Use `db/10_validate_skater_name_quality.sql` after imports to catch empty names
or accidental `Q` skater names.

## Import Commands

```bash
.venv/bin/python scripts/import_event.py <index-url>
.venv/bin/python scripts/validate_import.py <import_run_id>
```

The scripts read `config/local.env` by default. Use `config/local.env.example`
as the template for another machine. For direct SQL checks, use:

```bash
db/psql_local.sh -f db/validation_queries.sql
```

`ingest.source_url_registry` tracks URLs before import. Current statuses after
the BSF revalidation/import pass:

- `imported/passed`: 34 FS Manager URLs and 39 old ISUCalcFS URLs.
- `imported/warning`: 1 FS Manager URL with accepted source warnings.
- `analyzed/warning`: 4 direct `Mlad figurist` result PDFs from BSF. These are
  retained for a future direct-PDF import profile.
- `skipped/failed`: 3 URLs only. Two are unavailable Denkova-Staviski 2020
  ISU/NonISU links, and one is Black Sea Ice Cup 2026 `DP`, which currently
  returns the BSF React shell instead of a result index.

BSF source profile resolution:

- Black Sea Ice Cup 2026 ISU/NonISU are BSF links that meta-refresh to
  `https://icesportsvarna.com/bsic/...`; importer follows the refresh and uses
  `fs_manager`.
- Many BSF 2019-2022 links are old ISUCalcFS JavaScript wrappers with an empty
  body and `scripts/results...`; importer resolves them to `pages/main.html` or
  `pages/main.htm` and records `resolved_url`.
- National Championships 10-14.02.2016 is old ISUCalcFS but has a special
  category/result-PDF-only table. It imports 15 categories and stores the source
  links, with no entries, segments, officials, or score rows because those links
  are final-result PDFs rather than the normal ISUCalcFS HTML/detail/PDF scope.
  Latest reimport run: 96.

## Duplicate Import Cleanup

Older duplicate completed import runs were removed with
`db/14_delete_duplicate_import_runs.sql`. The cleanup keeps the latest completed
run per exact `root_url` and deletes older run-scoped child data in dependency
order.

Deleted older duplicate run IDs:

- `2`, `6`, `7`, `8`, `13`, `15`, `17`, `22`, `23`, `24`, `25`, `88`

Validation after cleanup:

- `db/13_duplicate_import_audit.sql` reports zero exact duplicate completed
  imports by `root_url`.
- Event-name/date collisions remain only where they represent distinct streams
  such as ISU and NonISU for the same competition.
- `db/10_validate_skater_name_quality.sql` reports zero rows.

## URL Preflight Rule

URL checks must run before parser validation and before any import work. The
shared implementation is `isu_parser/source_check.py`.

Current behavior:

- Fetch the source URL first and classify it as supported result HTML, direct
  PDF, site fallback, HTML fallback, wrapper, or unreachable.
- Follow meta-refresh result links before parser detection/import.
- Resolve old ISUCalcFS `scripts/results...` wrappers to `pages/main.html` or
  `pages/main.htm` before parser validation/import.
- Reject BSF React shell fallback pages before parser validation.
- Reject `.pdf` URLs that return HTML before PDF parsing.

`scripts/seed_bsf_registry.py` uses this preflight before marking any URL
`ready_for_import`. `scripts/import_event.py` also runs the same preflight before
creating an import run, so direct imports cannot accidentally insert failed
parser runs for broken URLs.

## BSF Update Monitoring

`https://www.bsf.bg/figure-skating/national-championships/` changes over time.
Use `scripts/seed_bsf_registry.py` periodically to rediscover the BSF page,
preflight new result links, and add only URLs that are not already completed.
Then run `scripts/import_ready_registry.py` to import only `ready_for_import`
rows. Exact duplicate completed imports are blocked by registry/import checks and
can be audited with `db/13_duplicate_import_audit.sql`.

## Multi-Competition Skater Validation

`db/15_skater_multi_competition_comparison.sql` reports:

- potential representation issues where `nation_code` does not look like an ISO
  country code;
- skaters appearing in multiple competitions using conservative key
  `normalized_name + nation_code`;
- category final-result comparisons with point deltas;
- segment/PDF score comparisons with TSS/TES/PCS/deduction deltas.

Current finding from this report: older BSF club-primary old ISUCalcFS sources
often store club abbreviations in `nation_code` (`Elit`, `ID DS`, `Varna`,
`Ice P`, `L Isk`, etc.). This means those rows need a representation cleanup:
for club-primary BSF sources, these values should be moved/interpreted as club
values instead of country codes before building final skater identity profiles.

Follow-up status: `db/16_event_profile_representation_rules.sql` applies this
cleanup for current imports. Keep this report because it is still useful after
new imports or new source profiles.

## Event Profile Representation Rules

`db/16_event_profile_representation_rules.sql` defines source/event profile
configuration for representation handling:

- `international` / `federated`: ISU, NonISU, ISUCS style sources where skaters
  represent a country. `representation.primary = nation`; club is not expected.
- `amateur_club`: Ice Peak Trophy style amateur/local competitions. Club is the
  primary representation, but country must still be stored when the source has a
  real country code. Club may be present, empty, unknown, or individual; preserve
  empty club for review.
- `national_club`: BSF national/priz-victoria local championship sources where
  the participant belongs to a club. Some old ISUCalcFS pages label the club
  column as `Nation`; profile settings now mark `nation_column = club`.
- `club_control`: control/local club competitions, also club-primary. Store
  country as well when the source provides a valid country value.

The migration corrected existing imported appearances:

- Old club values stored in `nation_code` were moved to `club_name` for
  club-primary profiles.
- Non-ISO `nation_code` fragments from FS Manager segment-result rows are
  treated as parser/source artifacts and cleared, not converted to club names.
  Earlier migrated lowercase fragments such as `eak`, `rot`, `rna`, `kri`, and
  `lit` were removed from `club_name`.
- Valid ISO country codes remain in `nation_code` even for club-primary profiles.
- Current validation: non-ISO-looking `nation_code` values = 0 and lowercase
  three-character FS Manager club fragments = 0.
- BSF-hosted Black Sea Ice Cup needs stream-specific representation handling:
  `ISU` remains international/nation-primary, while local club streams such as
  `NonISU` can be club-primary when the source has real Club values.

## Scoring Marker Rule

Element markers are stored in `ingest.pdf_elements.markers` and explained in
`core.validation_legend`.

Important example:

- `4S<<` is still an executed element.
- `<<` means downgraded jump.
- Keep `element_code`, `base_element_code`, markers, base value, GOE, judge marks,
  and panel score.

Known markers include `<`, `<<`, `q`, `e`, `!`, `F`, `x`, `*`, `b`, and `REP`.

## Representation Rule

ISU and NonISU international-style sources usually represent skaters by nation.

Ice Peak Trophy is club-primary because it includes Club in entries/results.
For Ice Peak:

- If Club exists, store `representation_type=club` and `representation_value=club_name`.
- If Club is empty, keep it empty and report the row for manual review.
- Do not automatically convert empty Club to nation/country.
- Still store `nation_code` separately.

Empty Club can mean individual/no-club participant, unknown club, intentionally
empty source value, or incomplete source data.

## Ice Peak Open Issues

Latest Ice Peak import run has missing result/PDF cases with entries, not empty
categories. Current classification:

- `Ice Dance / Free Dance`: linked document is `StartListwithTimes.pdf`, not a
  judges-score PDF; entry count is 1.
- `Mlad figurist / Free Skating`: score PDF exists and returns PDF bytes, but it
  is not a standard judges-detail sheet. It is a beginner test result table.
  Dedicated parser/table added in `ingest.pdf_mlad_figurist_results`.
  Ice Peak reimport run 21 imported 10 Mlad figurist rows with club, crossings,
  judge votes over 75%, average percent, result text, and pass/fail flag.
- `Solo Ice Dance-2 / Free Dance`: 4 entries are imported. Score PDF exists with
  normal header, but dance score details are outside the current parser scope.
  Keep skater participation entries and mark scores as unavailable/pending
  dance-specific parser.
- `Solo Ice Dance-3 / Free Dance`: 2 entries are imported. Score PDF exists with
  normal header, but dance score details are outside the current parser scope.
  Keep skater participation entries and mark scores as unavailable/pending
  dance-specific parser.
- `Special Olympics Level-1 Boys / Free Skating`: linked score PDF URL returns
  site HTML fallback, not PDF bytes; entry count is 1. Keep the skater entry and
  mark competition scores as unavailable/no score PDF from source.
- `Special Olympics Level-1 Girls / Free Skating`: linked score PDF URL returns
  site HTML fallback, not PDF bytes; entry count is 1. Keep the skater entry and
  mark competition scores as unavailable/no score PDF from source.
- `Special Olympics Level-3 Boys / Free Skating`: linked score PDF URL returns
  site HTML fallback, not PDF bytes; entry count is 2. Keep the skater entries and
  mark competition scores as unavailable/no score PDF from source.
- `Special Olympics Level-4 Girls / Free Skating`: linked score PDF URL returns
  site HTML fallback, not PDF bytes; entry count is 1. Keep the skater entry and
  mark competition scores as unavailable/no score PDF from source.
- `Special Olympics Unified Pair Level-1 / Free Skating`: linked score PDF URL
  returns site HTML fallback, not PDF bytes; entry count is 1. Keep the skater
  entry and mark competition scores as unavailable/no score PDF from source.

Special Olympics no-score handling: if entries exist but no score PDF is
published, preserve the `entry` skater appearances and expose the score state as
unavailable rather than treating it as a failed skater import.

Solo Ice Dance handling: entries are imported as participation data, but detailed
score rows are not imported until a dance-specific PDF parser is added. Treat
the current missing skater summaries as accepted score-scope limitation, not as
missing skater entries.

## Analytics And Future API Layer

`db/17_analytics_views.sql` creates a reusable `analytics` schema for reporting,
BI, future API endpoints, and coach dashboards. The rule is:

- keep `ingest.*` as raw/source-truth import data;
- keep import diagnostics, source URLs, parser issues, and reimport control in
  this project;
- make API/dashboard/report consumers read stable `analytics.*` views instead
  of raw ingest tables.

Current analytics views:

- `analytics.v_latest_import_runs`: latest completed import per exact root URL.
- `analytics.v_events`: event metadata with source profile, stream, event type,
  and normalized start date. Handles both `DD.MM.YYYY` / `DD/MM/YYYY` and old
  `M/D/YYYY` source dates when the second number is clearly the day.
- `analytics.v_skater_appearances_clean`: cleaned appearance rows with category,
  segment, country, club, and display affiliation.
- `analytics.v_skater_category_results`: final category placements/points.
- `analytics.v_skater_segment_scores`: unified PDF score summaries plus HTML
  segment result fallback where no PDF summary exists. Includes `score_total`
  for analysis/reporting while preserving parsed source fields such as `tss`,
  `tes`, and `pcs`.
- `analytics.v_skater_elements`: executed elements, base elements, markers,
  base value, GOE, bonus, and panel score.
- `analytics.v_skater_progression`: segment-score deltas over time per skater.
- `analytics.v_club_skater_summary`: coach/club-friendly summary of skaters,
  competitions, date range, best parsed TSS, best analysis total score, best
  TES/PCS, and event types.
- `analytics.v_data_quality_representation`: profile-level data-quality counts
  for country/club representation.

Recommended future split:

- This repository remains the ingestion, parser, source discovery, database
  migration, validation, and optimization project.
- A separate future API/dashboard project should connect to the same PostgreSQL
  database and read primarily from `analytics.*` views.
- Coach-dashboard MVP can start from `v_club_skater_summary`,
  `v_skater_progression`, and `v_skater_elements`.
- Profile claiming, users, club assignments, training time, calendar, videos,
  and social links should live in product-owned schemas/tables later. Imported
  skater appearances should remain traceable to the source and should not be
  overwritten by profile-claim edits.

Useful BI starter queries:

```sql
select *
from analytics.v_data_quality_representation
order by event_type, competition_level, competition_stream;

select event_start_date, event_name, category_name, segment_name,
       place, tss, tes, pcs, deduction, tss_delta_same_segment
from analytics.v_skater_progression
where skater_name = 'ALEKSANDRA PETKOVA'
order by event_start_date nulls last, event_name, category_name, segment_name;

select event_start_date, event_name, category_name, element_no,
       element_code, base_element_code, markers, base_value, goe, panel_score
from analytics.v_skater_elements
where skater_name = 'ALEKSANDRA PETKOVA'
order by event_start_date nulls last, event_name, category_name, element_no;

select *
from analytics.v_club_skater_summary
where club_name <> '(empty club)'
order by competitions desc, best_tss desc nulls last, skater_name
limit 20;
```

Validation note: older local/club HTML result pages may populate legacy score
columns differently from PDF score summaries. For coach/BI reports, prefer
`score_total` / `best_score_total` when comparing across mixed PDF and HTML-only
sources, and inspect the underlying `score_source` before treating a parsed
field difference as a scoring regression.

`analytics.v_skater_segment_scores` uses PDF score summaries as the preferred
score source. HTML segment-result fallback rows are suppressed when a matching
PDF score exists for the same segment/skater, even if the HTML row has missing
country/club fields. This prevents duplicate summary rows such as
`ALEXANDRA FEIGIN` split into `BUL` and empty-country groups.

Old ISUCalcFS category result parsing now reads result columns by header. This
fixes pages that include both `Club` and `Nation` columns, for example
Denkova-Staviski Cup 2014 ISU `Advanced Novice Girls`, where Alexandra Feigin's
source row has blank Club, `BUL`, `87.03`, `SP=2`, `FS=4`. Reimport run 97
corrected that source.

Use `db/18_validate_affiliation_quality.sql` after imports and source-profile
rule changes. It reports non-ISO country values, suspicious FS Manager club
fragments, club-primary rows missing club values, and source-profile affiliation
summaries.

## Club Registry

`db/19_club_registry.sql` creates a canonical club layer:

- `core.clubs`: canonical club records with stable numeric `id` and public UUID.
- `core.club_aliases`: maps imported source strings to canonical clubs.
- `core.v_club_alias_usage`: usage counts by alias, event count, and sample
  source URL.
- `analytics.v_skater_appearances_with_club`: appearances plus canonical club
  fields.
- `analytics.v_skater_category_results_with_club`: category results plus
  canonical club fields.

Imported `club_name` values are not overwritten. The registry maps source values
to canonical records for reporting and future club management.

Current registry status:

- 118 imported distinct club strings are covered by aliases.
- 74 canonical club records exist.
- 24 active canonical clubs were manually seeded with high/medium confidence.
- 50 provisional review clubs were auto-created from imported club strings.
- Category-result rows with `club_name` are fully mapped: 5445/5445.

Important seeded alias examples:

- `Elit` -> `Elit`
- `Ice P` -> `Ice Peak`
- `ID DS` -> `Ice Dance Denkova-Staviski`
- `L Isk` -> `Ledeni Iskri`
- `Slv` -> `Slavia`
- `Odes` -> `Odesos Varna`

Provisional one-to-one clubs such as `E`, `F`, `KS`, `OLK`, `SCS`, etc. are
preserved but need manual review before they are treated as final public club
names.

## 2026-04-28 Cleanup, Archive, STAR Mart, Discovery

Completed cleanup from the 2026-04-27 import/parser work:

- Reimported the remaining old ISUCalcFS sources affected by `Club`/`Nation`
  result layouts and anchor-name club suffixes.
- Corrected `ANABEL CABEZA`: summary now shows `(empty club)`, `BUL`,
  first/latest date `2021-11-26`, best TSS `41.180`, best TES `15.680`, best
  PCS `26.000`.
- Validation results after rebuild:
  - `analytics.v_skater_category_results where segment_places ? 'Points'`: 0.
  - ISO country codes incorrectly stored as club without nation: 0.
  - non-ISO `nation_code` values: 0.
  - lowercase three-character club fragments: 0.
  - likely club suffixes still attached to skater names in segment scores: 0.
  - events missing parsed `event_start_date`: 0.
- Full tests pass: `33 passed`.

Local source archive:

- `scripts/export_source_archive.py` now exports stored HTML and fetches/stores
  original PDF files as binary files when source documents reference PDFs.
- Latest archive output:
  - `data/source_archive/manifest.csv`
  - `data/source_archive/manifest.jsonl`
  - 6165 written files, about 141 MB.
  - 4891 HTML files written.
  - 1274 PDF files written.
  - 109 PDF source-document records were skipped because the PDF URL did not
    return PDF bytes; keep these as source warnings.
- Added `db/22_source_archive_manifest.sql` and
  `scripts/load_source_archive_manifest.py`.
- Loaded 6274 manifest rows into `ingest.source_archive_files`.

STAR-style analytics mart:

- Added/applied `db/21_star_mart_views.sql`.
- Creates schema `mart` with dimension/fact views for BI/API/dashboard usage:
  `dim_skater`, `dim_event`, `dim_category`, `dim_segment`, `dim_country`,
  `dim_club`, `dim_element`, `dim_official`, `dim_source_document`, `dim_date`,
  `fact_competition_result`, `fact_segment_score`, `fact_element_score`,
  `fact_element_judge_mark`, `fact_component_score`,
  `v_skater_personal_bests`, and `v_skater_source_evidence`.
- Current counts:
  - `mart.dim_skater`: 5804.
  - `mart.fact_segment_score`: 10525.
  - `mart.fact_element_score`: 40569.
  - `mart.fact_competition_result`: 8555.
  - `mart.v_skater_personal_bests`: 2488.
  - `mart.v_skater_source_evidence`: 37476, all with archive paths.

Discovery:

- Added `docs/analytics-and-discovery-strategy.md`.
- Added `docs/european-source-discovery-playbook.md`.
- Added `scripts/discover_isu_events.py` for the official ISU figure-skating
  events catalog. It now targets the filtered official catalog URL, captures
  event detail URLs, ignores non-figure-skating rows from the streamed page
  payload, and de-duplicates repeated rendered rows.
- Added `db/23_event_discovery_catalog.sql` and
  `scripts/load_isu_events_catalog.py`.
- Added `scripts/resolve_isu_event_results.py` to resolve official ISU event
  detail pages into external `Entries & Results` URLs and optionally register
  validated result URLs in `ingest.source_url_registry`.
- The resolver also supports `--dry-run` with `--detail-url` for no-database
  checks while PostgreSQL is offline.
- The ISU flow is now:
  `filtered official catalog` -> `official event detail page` -> external
  result folder such as `https://www.deu-event.de/results/adult2025/`.
- The `deu-event.de` adult competition example is FS Manager / Swiss Timing
  style and may require the SSL verification fallback added to source preflight
  and import fetching.
- Loaded 17 filtered 2025/2026 official ISU figure-skating catalog rows from
  `https://www.isu-skating.com/events/?month=All&discipline=FIGURE+SKATING&season=2025%2F2026&event_type=All+ISU+Events`.
- Those 2026/2027 season rows are mostly future events and currently have empty
  external result URLs, so they remain catalog discoveries for refresh rather
  than importable result URLs.
- Added `db/24_validate_mart_and_discovery.sql` for reusable mart/archive/
  discovery validation.
- Added `docs/next-db-actions.md` with the exact no-database checks and the
  next PostgreSQL command sequence for ISU official catalog refresh, detail
  resolution, registry import, and validation.

2026-04-29 ISU official detail import:

- Cleaned obsolete duplicate official ISU catalog rows from the older
  `https://www.isu-skating.com/figure-skating/events/` source page. Current
  official discovery catalog has 17 filtered future figure-skating rows with
  `catalog_only_needs_result_url` and one resolved Adult Competition detail row
  with `candidate_result_url`.
- Registered and imported the official ISU Adult Competition detail result:
  `https://www.isu-skating.com/figure-skating/events/eventdetail/international-adult-competition/`
  -> `https://www.deu-event.de/results/adult2025/`.
- Added `db/25_seed_isu_external_source_profiles.sql` for the external
  `deu-event.de` FS Manager result folder discovered from the official ISU site.
  Latest import run 219 is assigned to source profile
  `isu_official_adult_2025_deu_event`.
- Final Adult Competition import run 219 completed with zero parse issues:
  86 categories, 91 segments, 738 entries, 738 category results, 759 segment
  results, 808 official assignments, and 742 PDF score summaries.
- Parser improvements from this source:
  - FS Manager artistic free skating PDFs with no executed elements and
    component-only scoring.
  - FS Manager pattern dance PDFs where segment total is based on the average
    of TES and PCS.
  - Pattern dance segment labels such as
    `PATTERN DANCE 1 (WITHOUT KEY POINTS)`.
- Duplicate completed imports were cleaned again with
  `db/14_delete_duplicate_import_runs.sql`; zero exact duplicate completed
  imports remain.
- Source archive was refreshed after adding TLS fallback to
  `scripts/export_source_archive.py`. All mart source-evidence rows now have an
  archive path: 40453/40453.
- Full test suite passes: 38 tests.
