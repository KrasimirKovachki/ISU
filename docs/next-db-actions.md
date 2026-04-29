# Next Database Actions

Use this when PostgreSQL is available again. Do not run these while the database
is stopped.

## ISU Official Catalog Refresh

Load the filtered official ISU figure-skating catalog:

```bash
.venv/bin/python scripts/load_isu_events_catalog.py \
  --url 'https://www.isu-skating.com/events/?month=All&discipline=FIGURE+SKATING&season=2025%2F2026&event_type=All+ISU+Events'
```

Resolve official ISU detail pages to external `Entries & Results` URLs and
register only validated result URLs:

```bash
.venv/bin/python scripts/resolve_isu_event_results.py --register --refresh
```

Resolve and register one known detail page:

```bash
.venv/bin/python scripts/resolve_isu_event_results.py \
  --detail-url 'https://www.isu-skating.com/figure-skating/events/eventdetail/international-adult-competition/' \
  --register
```

Import ready registry rows:

```bash
.venv/bin/python scripts/import_ready_registry.py
```

Run validation after import:

```bash
db/psql_local.sh -f db/24_validate_mart_and_discovery.sql
```

After adding new external ISU result hosts, add/apply a source profile SQL so
imports are not anonymous. Current example:

```bash
db/psql_local.sh -v ON_ERROR_STOP=1 -f db/25_seed_isu_external_source_profiles.sql
```

## No-Database Checks

Discover catalog rows without touching PostgreSQL:

```bash
.venv/bin/python scripts/discover_isu_events.py \
  --url 'https://www.isu-skating.com/events/?month=All&discipline=FIGURE+SKATING&season=2025%2F2026&event_type=All+ISU+Events'
```

Resolve one ISU detail page without touching PostgreSQL:

```bash
.venv/bin/python scripts/resolve_isu_event_results.py \
  --detail-url 'https://www.isu-skating.com/figure-skating/events/eventdetail/international-adult-competition/' \
  --dry-run
```

Resolve and validate the external result URL without touching PostgreSQL:

```bash
.venv/bin/python scripts/resolve_isu_event_results.py \
  --detail-url 'https://www.isu-skating.com/figure-skating/events/eventdetail/international-adult-competition/' \
  --dry-run \
  --register
```

In dry-run mode, `--register` means validate the external result URL and print
the parser/registry decision. It does not write to PostgreSQL.
