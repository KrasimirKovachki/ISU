# Integration contract (v1)

This file is the **stable boundary** between the Skating Data platform (this repo) and
external consumers — chiefly the Ice Skating Academy app. The two projects are developed
and versioned **independently**; they only promise to honour this contract.

## Rules

- Consumers call the **`/api/v1/...`** prefix. (`/api/...` without a version still works for
  back-compat but is not guaranteed stable.)
- **v1 changes are additive only**: new endpoints and new response fields are fine; renaming
  or removing an endpoint/field that appears below is a breaking change and must go to a new
  `/api/v2` instead. v1 keeps working until consumers migrate.
- Consumers must treat the API as **best-effort**: handle non-200, timeouts and empty results
  gracefully (the academy proxy already times out in 3s and falls back to "no results").
- Consumers store only the platform's `skater_key` / `identity_key` as a link; they never read
  this database directly.

## Endpoints the academy depends on

| Method & path | Purpose | Key response fields the academy reads |
|---|---|---|
| `GET /api/v1/health` | liveness | `status` = `"ok"` |
| `GET /api/v1/meta` | data freshness/size | `events`, `skaters`, `clubs`, `last_import_at`, `latest_event_date`, `api_version` |
| `GET /api/v1/skaters?q=&limit=` | name search (roster autocomplete) | `items[]`: `skater_key`, `skater_name`, `nation_code`, `club_name`, `identity_key` (also `nation_flag`, counts) |
| `GET /api/v1/skaters/match?name=&nation=&limit=` | link an academy skater to a platform skater | `match` (nullable), `confidence` (0-1), `candidates[]` (same shape as search items) |
| `GET /api/v1/skaters/{skater_key}` | skater profile + history (results panel) | `skater_name`, `nation_code`, `club_name`, `scores[]`, `elements[]`, `component_summary` |
| `GET /api/v1/events/{event_key}/results` | event results | `items[]`: `category_name`, `skater_name`, `nation_code`, `club_name`, `final_place`, `points`, `segment_places` |
| `GET /api/v1/events/{event_key}/schedule` | categories + segments from the entry schedule (seed a competition's config) | `categories[]`: `name`, `segments[]`(`name`,`abbr`); `segments[]`: `name`, `abbr` |

Notes:
- Country codes are **IOC** (e.g. `BUL`, `GER`, `SUI`). Consumers map to ISO-3166-2 themselves
  (the academy has `IOC_TO_ISO2`). Adding an `iso2` field here later would be an additive v1 change.
- Scope: the platform holds events published with ISU-platform software (ISUCalcFS + FS Manager),
  both ISU-sanctioned and local amateur. Sanctioning is a label (`competition_stream`), not a filter.

## Tests that protect the contract

- Platform side: `tests/test_api_contract.py` asserts the v1 endpoints exist and return the fields
  above (skips the data-dependent checks when no DB is reachable). Run before every release.
- Academy side: a proxy test asserts `/trophy/skater-search/` handles the search shape and the
  empty/timeout case.
