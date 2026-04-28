# Database Setup

Target VM:

- `192.168.1.18`

Local connection settings are stored in `config/local.env`. Keep that file
local-only; use `config/local.env.example` as the shareable template.

## 1. Create Database And User

Recommended local setup command:

```bash
db/setup_remote_database.sh
```

The script prompts for:

- PostgreSQL host, default `192.168.1.18`
- PostgreSQL superuser, default `postgres`
- application database, default `skating_data`
- application user, default `skating_app`
- superuser password
- application user password

It then runs `00_create_database_and_user.sql` as the superuser and
`01_ingest_schema.sql`, `02_seed_source_profiles.sql`, and
`03_seed_validation_legend.sql` as the application user.

## Manual Commands

Create the database and user as PostgreSQL superuser:

```bash
/usr/local/opt/libpq/bin/psql -h 192.168.1.18 -U postgres \
  -v app_db=skating_data \
  -v app_user=skating_app \
  -v app_password=change_me_strong_password \
  -f db/00_create_database_and_user.sql
```

Change the password before using this outside local testing.

Create import schema as app user:

```bash
db/psql_local.sh -f db/01_ingest_schema.sql
```

Seed known source profiles:

```bash
db/psql_local.sh -f db/02_seed_source_profiles.sql
```

Seed validation/scoring legend:

```bash
db/psql_local.sh -f db/03_seed_validation_legend.sql
```

## Verify

```bash
db/psql_local.sh -c "\dt ingest.*"
```

```bash
db/psql_local.sh -c "select profile_key, parser_profile, representation_primary from ingest.source_profiles order by profile_key"
```

Manual validation report:

```bash
db/psql_local.sh -f db/validation_queries.sql
```

Manual representation validation:

```bash
db/psql_local.sh -f db/04_validate_representation_rules.sql
```

Classify missing result/PDF data against entry counts:

```bash
db/psql_local.sh -f db/07_validate_missing_results_against_entries.sql
```

Create the dedicated Ice Peak `Mlad figurist` test-result table:

```bash
db/psql_local.sh -f db/08_mlad_figurist_test_results.sql
```

Validate skater-name quality:

```bash
db/psql_local.sh -f db/10_validate_skater_name_quality.sql
```

Allow repeated imports of the same source URL:

```bash
db/psql_local.sh -f db/05_allow_event_reimports.sql
```

Create or refresh the source URL registry:

```bash
db/psql_local.sh -f db/06_source_url_registry.sql
```

List URLs waiting for import:

```bash
db/psql_local.sh -c "select status, validation_status, url, resolved_url from ingest.source_url_registry order by url"
```

## Notes

The schema is intentionally staging-first:

- raw source documents and import runs are preserved.
- skater appearances are stored separately from canonical skaters.
- officials are segment assignments.
- PDF element/component judge scores are normalized and can later map `J1`, `J2`, etc. to officials.
