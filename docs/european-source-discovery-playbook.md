# European Source Discovery Playbook

Goal: discover figure-skating result sources across Europe, validate platform
shape, register importable URLs, and prioritize events where Bulgarian skaters
may appear.

## Source Types

1. Official ISU catalog
   - Use `scripts/discover_isu_events.py`.
   - Load rows with `scripts/load_isu_events_catalog.py`.
   - These rows are event catalog rows with ISU event-detail URLs, not
     guaranteed result URLs.
   - Use `scripts/resolve_isu_event_results.py` to open each detail page and
     extract the external `Entries & Results` URL when ISU exposes it.
   - When PostgreSQL is offline, inspect one detail page without database writes:
     `.venv/bin/python scripts/resolve_isu_event_results.py --detail-url <url> --dry-run`.
   - Store them in `ingest.event_discovery_catalog` with
     `catalog_only_needs_result_url`.

2. National federation result pages
   - Best source for national championships, club events, tests, adult/amateur
     events, and regional competitions.
   - Add one discoverer per federation when the site structure is stable.
   - Store discovered result URLs in `ingest.source_url_registry`.

3. Competition organizer result pages
   - Many clubs host old ISUCalcFS or FS Manager result folders directly.
   - Good examples already imported: Denkova-Staviski Cup, Sofia Trophy,
     Ice Peak Trophy, BSF-hosted events.

4. Direct PDF result sources
   - Mlad Figurist and some local tests may be PDF-only.
   - Keep these in the registry/catalog even when the normal event importer
     cannot fully import them yet.

## Platform Fingerprints

Old ISUCalcFS:

- `Created by ISUCalcFS`
- `CAT###EN.HTM`
- `CAT###RS.HTM`
- `SEG###.HTM`
- `SEG###OF.HTM`
- `Judges Scores (pdf)`

New FS Manager / Swiss Timing style:

- `Panel of Judges`
- `Starting Order / Detailed Classification`
- `JudgesDetailsperSkater.pdf`
- event protocol PDF link
- modern component labels such as `CO`, `PR`, `SK`
- Example from the ISU official event flow:
  `https://www.isu-skating.com/figure-skating/events/eventdetail/international-adult-competition/`
  exposes `https://www.deu-event.de/results/adult2025/`.
  The `deu-event.de` host may require the importer/preflight SSL fallback
  because its certificate chain can fail normal Python verification.

Fallback/unavailable:

- React shell without result tables.
- HTML page returned for a `.pdf` URL.
- Empty category with entries but no result/PDF.

## Discovery Workflow

1. Catalog discovery:
   - Load official or federation event listings into
     `ingest.event_discovery_catalog`.
   - Preserve event name, date range, city/country, and source page.

2. Result URL discovery:
   - Try result links from catalog pages.
   - For the ISU official site, resolve event detail pages first; the external
     result URL is normally in the detail-page `pageinfos.detail_result_url`
     payload and also appears as the `Entries & Results` button when available.
   - Future ISU events may have empty detail result URLs. Keep these as catalog
     rows for refresh, but do not import them.
   - Try common folder forms: `index.htm`, `index.html`, `ISU/index.htm`,
     `NonISU/index.htm`, `pages/main.htm`, `pages/main.html`.
   - Avoid importing until preflight validation identifies the source shape.

3. Registry staging:
   - Insert candidate result URLs into `ingest.source_url_registry`.
   - Store parser profile, representation profile, stream, validation summary,
     and warnings.

4. Validation:
   - Run source preflight before import.
   - Confirm categories, segments, officials, entries, result pages, PDF count,
     and fallback/404 status.
   - Empty categories with entries but no score should be stored as entry-only
     source information, not treated as data loss.

5. Import:
   - Import only registry rows marked `ready_for_import/passed`.
   - Rebuild `analytics` and `mart`.
   - Export source archive and load archive manifest.
   - Run `db/24_validate_mart_and_discovery.sql`.

## Priority Countries / Regions

Prioritize Europe and nearby competitions likely to contain Bulgarian skaters:

- Bulgaria, Romania, Serbia, Greece, Turkey.
- Balkan and regional events.
- ISU Junior Grand Prix European stops.
- Challenger Series and international senior/junior events in Europe.
- Adult/amateur events using club representation.
- National championships and federation-hosted local events.

## Search Patterns

Use these patterns for manual or automated search-based discovery:

- `"ISUCalcFS" "Judges Scores" "Figure Skating"`
- `"SEG001.HTM" "Figure Skating"`
- `"CAT001RS.HTM" "Figure Skating"`
- `"JudgesDetailsperSkater.pdf" "Figure Skating"`
- `"Panel of Judges" "Detailed Classification" "Figure Skating"`
- `"Mlad Figurist" "Result.pdf"`

Country-scoped variants:

- `site:<federation-domain> "Judges Scores" "Figure Skating"`
- `site:<federation-domain> "ISUCalcFS"`
- `site:<federation-domain> "JudgesDetailsperSkater.pdf"`

## Data Product Rule

Discovery must feed the database, not a list in code. The progression should be:

`catalog row` -> `candidate result URL` -> `validated registry row` ->
`imported run` -> `archive evidence` -> `analytics/mart facts`.
