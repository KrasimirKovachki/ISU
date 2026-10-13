# ISU API Investigation

Verified against the live site on 2026-04-30.

## API Base

The public website uses this API base:

```text
https://api.isu-skating.com/api/
```

The website host path `https://www.isu-skating.com/api/...` is not the API; it
returns a Next.js 404 page. The API base is visible in the website JavaScript
bundle and works without authentication for the endpoints below.

## Directory Endpoints

### `POST skater/list`

Purpose: singles directory.

Useful form fields:

- `discipline_id=1`
- `page=1`
- `gender=M` or `gender=F`
- optional `country_name`
- optional `search`

Verified response fields include:

- `skaters_id`
- `api_id` UUID
- `first_name`
- `last_name`
- `full_name`
- `gender`
- `nationality_code`
- `organization_code`
- `image`
- `thumbnail_image`
- `is_top_display`
- `slug`
- `status`
- `created_at`
- `updated_at`

Observed page 1 for men returned `total_items=1667`, `total_page=167`.

### `POST skater/team-list`

Purpose: pairs and ice dance directory.

Useful form fields:

- `discipline_id=1`
- `page=1`
- `is_couple=1` for pairs
- `is_ice_dance=1` for ice dance
- optional `country_name`
- optional `search`

Verified response fields include team-level data:

- `skaters_team_id`
- `api_id` UUID
- `name`
- `couple_name`
- `short_name`
- `career_state`
- `nf_organization_code`
- `image`
- `thumbnail_image`
- `slug`
- `status`
- `skaters_team_member[]`

Each `skaters_team_member[]` carries person-level `skaters_id`, `api_id`,
`first_name`, `last_name`, `full_name`, `gender`, nation/organization codes,
images, slug, and status.

## Profile Images

Profile image and thumbnail URLs are direct Azure Front Door blob URLs. A live
thumbnail HEAD request returned `HTTP/2 200`, image content type, ETag,
last-modified, and content length. So the backend can safely store image URL
metadata now.

Do not copy image bytes into our database. Recommended approach:

- Store `image_url`, `thumbnail_url`, `etag`, `last_modified`,
  `content_length`, `content_type`.
- Treat images as external assets at first.
- Add a later optional media cache/archive only if a UI needs stable offline
  rendering or if ISU URLs prove unstable.

## Per-Skater Competition History

### `POST skater/get-competition-results`

Purpose: skater-driven event discovery.

Useful form fields:

- `skaters_id`
- `page`

Verified with Yuma Kagiyama (`skaters_id=3152`):

- `total_items=48`
- `total_page=5`

Per row:

- `skaters_competition_results_id`
- `skaters_id`
- `api_id`
- `event_id` UUID
- `event_name`
- `date`
- `to_date`
- `year`
- `season`
- `points`
- `rank`
- `category`
- `country`
- `city`
- `event_type_name`
- `sport_code`
- `sport_name`

This endpoint is the strongest discovery surface because walking elite skaters
reveals historical ISU/GP/Championship/National events that are not visible from
one filtered website catalog page.

### `POST skater/get-major-achievements`

Purpose: profile enrichment, not primary ingestion.

Response includes `season[]` and `skaterAchievements[]`. Useful for future UI
summaries, but not needed for result import.

## Event Catalog Endpoint

### `POST event/mobile-list`

Purpose: canonical event catalog and slug/result URL bridge.

Request is JSON, not form-data. Useful fields:

```json
{
  "pagesize": 200,
  "discipline_title": "FIGURE SKATING",
  "season": "2025/2026"
}
```

Verified response fields include:

- numeric `event_id`
- `api_id` UUID when present
- `name`
- `from_date`
- `to_date`
- `city`
- `country_code`
- `country_name`
- `season`
- `event_type`
- `event_type_name`
- `event_sub_type_name`
- `state`
- `slug`
- `button_1_title`
- `button_1_url`
- `button_2_url`
- `button_3_url`
- `is_event_result`
- `is_entry_show`
- `display_date`

Important finding: this endpoint can directly expose result URLs. Example:
`ISU Figure Skating Challenger Series Cranberry Cup International 2025` returned
`button_1_title="Detailed Results"` and a USFS result URL in `button_1_url`.

This should become the first choice for mapping an ISU event to an importable
external result URL. The existing event-detail HTML/RSC parser remains the
fallback when the API row has a slug but no direct button URL.

## Event Detail API

### `POST event/event-type-detail-v2/`

The website calls this with:

- `slug`
- `discipline_id`

It can still return `Data not found` for guessed slugs. Use only canonical
slugs from `event/mobile-list` or the rendered website catalog. If it fails,
fall back to the HTML detail page parser:

```text
https://www.isu-skating.com/figure-skating/events/eventdetail/<slug>/
```

## Backend Strategy

The backend should centralize data in layers, not collapse everything into one
skater or event table immediately.

### 1. Source Snapshot Layer

Store what the ISU API says, with raw JSON preserved:

- `ingest.isu_skater_directory`
- `ingest.isu_skater_person`
- `ingest.isu_skater_directory_member`
- `ingest.isu_api_event_catalog`
- `ingest.isu_skater_event_history`

This layer is refreshable and source-specific. It should not mutate
`core.skaters` automatically.

### 2. Discovery Queue

Use API rows to create candidate result sources:

1. Load `event/mobile-list` by season.
2. Store canonical event slugs and direct button URLs.
3. Walk selected skaters via `get-competition-results`.
4. Link skater-history event UUID/name/date/country to event catalog rows.
5. Resolve missing result URLs via event detail HTML.
6. Validate external result URLs with existing `preflight_result_url`.
7. Upsert validated URLs into `ingest.source_url_registry`.

This keeps API discovery compatible with the current importer.

### 3. Identity Matching Layer

Create a separate reviewable mapping table later:

```text
core.skater_isu_link
```

It should map local/import-derived skater identities to ISU `api_id` UUIDs with
match evidence and review status. Do not auto-merge based only on name/nation.

### 4. Media Metadata Layer

Profile photos are possible now. Store references and HTTP metadata, not image
bytes:

- URL
- content type
- ETag
- last modified
- byte size
- last checked timestamp

This supports a future dashboard without making media archiving a blocker for
competition data ingestion.

## Recommended Build Order

1. Finish/apply `db/26_isu_skater_directory.sql`.
2. Add `db/27_isu_api_event_discovery.sql` for event catalog/history/discovery
   tables.
3. Add `scripts/discover_isu_skaters.py`.
4. Add `scripts/discover_isu_api_events.py` to load `event/mobile-list` by
   season.
5. Add `scripts/discover_isu_skater_events.py` to walk selected skaters and
   populate event history.
6. Add resolver logic that prefers `button_*_url`, then falls back to event
   detail HTML.
7. Register validated URLs into `ingest.source_url_registry`.

This gives the project a reliable growth loop: official API catalog and skater
histories discover events, existing parsers validate/import external result
sites, and identity/media enrichment stays reviewable.
