# Analytics And Discovery Strategy

This project should not stop at displaying imported results. The long-term value
is an auditable skating data warehouse that supports coaches, clubs, skaters,
public profiles, APIs, and BI analysis.

## Layered Data Model

Keep these layers separate:

- `ingest`: raw imported source truth, source documents, parse issues, import
  runs, and exact source rows.
- `core`: stable business entities such as skaters, clubs, countries, events,
  source identities, and future user/profile ownership.
- `analytics`: reusable cleaned views for common reporting.
- `mart`: STAR-style facts and dimensions for BI/API/dashboard consumption.
- `api`: future API-specific views or materialized views.

Do not move parser logic directly into the web app. The web/API should consume
cleaned analytics or mart views and link back to source evidence.

## STAR Schema Direction

The first STAR layer is implemented as views in `db/21_star_mart_views.sql`.
Views are safer than physical tables while the source model is still changing.
Later, expensive views can become materialized views.

Initial dimensions:

- `mart.dim_skater`
- `mart.dim_event`
- `mart.dim_category`
- `mart.dim_segment`
- `mart.dim_country`
- `mart.dim_club`
- `mart.dim_element`
- `mart.dim_official`
- `mart.dim_source_document`
- `mart.dim_date`

Initial facts:

- `mart.fact_competition_result`
- `mart.fact_segment_score`
- `mart.fact_element_score`
- `mart.fact_element_judge_mark`
- `mart.fact_component_score`

Initial derived BI views:

- `mart.v_skater_personal_bests`
- `mart.v_skater_source_evidence`

Every fact should retain enough keys to trace back to source import rows and the
original source document. This matters for trust: coach dashboards should be
able to show both an aggregate number and the PDF/HTML evidence behind it.

## Source Evidence

The local source archive is part of the data product, not just debug storage.
For every imported source document:

- keep the original URL.
- keep the local archived file path in `data/source_archive`.
- keep checksum/hash values.
- keep parser profile and import run.
- show validation status and parse issues when applicable.

Future enhancement: load `data/source_archive/manifest.csv` into a DB table such
as `ingest.source_archive_files`, then join it from `mart.dim_source_document`.
For now, the archive manifest is file-based.

## Score Semantics

Null and zero scores must be interpreted carefully:

- `NULL` usually means entry-only, no published result, WD/no score, or source
  shape without that scoring component.
- `0` can be a valid published value for no-score rows, but should be reviewed
  by source/event/category before using it in dashboards.
- BI views should expose both raw values and flags such as `has_score`,
  `score_status`, or `needs_review` before user-facing charts are built.

## Discovery Strategy

Discovery should populate a registry first. Import should only happen after
validation decides which parser profile and representation profile are correct.

Recommended discovery stages:

1. Official catalogs:
   - `https://www.isu-skating.com/events/?month=All&discipline=FIGURE+SKATING&season=2025%2F2026&event_type=All+ISU+Events`
   - useful for international events, Challenger Series, Junior Grand Prix,
     championships, and adult international competitions.
   - this page is a catalog source; it exposes official event detail pages, not
     always direct result URLs.
   - resolve each detail page and use `pageinfos.detail_result_url` / the
     `Entries & Results` button as the external result source when available.
   - future events often have empty result URLs; keep them in
     `catalog_only_needs_result_url` or `manual_review` for later refresh.

2. National federations:
   - Bulgarian federation source is already handled by
     `scripts/discover_bsf_national_championships.py`.
   - add federation-specific discoverers for other European countries.

3. Platform fingerprints:
   - old ISUCalcFS: `Created by ISUCalcFS`, `CAT###RS.HTM`, `SEG###.HTM`,
     `SEG###OF.HTM`.
   - FS Manager: `Starting Order / Detailed Classification`, `Panel of Judges`,
     Swiss Timing style file names.
   - direct PDF-only events: `Result.pdf`, `JudgesDetailsperSkater.pdf`.

4. Search-based discovery:
   - search by country/federation domain and platform terms.
   - examples: `"ISUCalcFS" "Judges Scores" figure skating`, `"SEG001.HTM"
     "Figure Skating"`, `"JudgesDetailsperSkater.pdf" "Figure Skating"`.

5. Import registry:
   - candidate URLs should enter `ingest.source_url_registry`.
   - statuses should move through `pending`, `analyzed`, `ready_for_import`,
     `imported`, `skipped`, or `failed`.
   - validation summary should store parser profile, source shape, number of
     categories/segments/PDFs, and warnings.

## First European Expansion Targets

Prioritize sources likely to contain Bulgarian skaters and comparable European
competition data:

- ISU official event catalog.
- ISU Challenger Series in Europe.
- Junior Grand Prix European stops.
- European national federation result pages.
- Adult/amateur competitions using FS Manager or ISUCalcFS.
- Balkan/regional competitions with club-level entries.

## Product Implication

The future web project should treat this repository as the data warehouse and
import-control project. The web/API can be a separate application that reads:

- public profiles from `core`/`api` views.
- charts from `mart`/`analytics`.
- source evidence from archive/manifest links.
- coach and club dashboards from future assignment tables plus mart facts.
