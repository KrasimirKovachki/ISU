# ISU Skating Data

This project centralizes data from figure skating competitions so skater scores,
competition history, source evidence, and analytics can be reviewed in one
place.

Initial parser and validation work for the old ISUCalcFS result format used by:

`https://cup.clubdenkovastaviski.com/2013/ISU/index.htm`

## What Is Covered

- Event index metadata.
- Category entries links.
- Category result links.
- Segment officials links.
- Segment result-detail links.
- Judges-score PDF links.
- Time schedule rows.
- Entries pages.
- Category result pages.
- Segment result-detail pages.
- Officials pages.
- Initial judges-score PDF text parsing.

## Run Tests

```bash
python3 -m unittest discover -s tests
```

## Parse The Old 2013 Index

```bash
python3 scripts/parse_old_isu.py https://cup.clubdenkovastaviski.com/2013/ISU/index.htm --validate
```

## Parse The New 2024 FS Manager Index

```bash
python3 scripts/parse_fs_manager.py https://cup.clubdenkovastaviski.com/2024/ISU/index.htm --validate
```

The 2024 source uses the `fs_manager` source profile. It includes ISU bio links such as `/bios/isufs00107843.htm`; these should be stored as source skater identifiers during import.

Known FS Manager sources currently validated:

```bash
python3 scripts/parse_fs_manager.py https://cup.clubdenkovastaviski.com/2024/ISU/index.htm --validate
python3 scripts/parse_fs_manager.py https://cup.clubdenkovastaviski.com/2024/NonISU/index.htm --validate
python3 scripts/parse_fs_manager.py https://sofia-trophy.clubdenkovastaviski.com/2026/ISU/ --validate
python3 scripts/parse_fs_manager.py https://www.bsf.bg/figure-skating/ice-peak-trophy/18-19.04.2026/index.htm --validate
```

See [docs/platform-version-reuse.md](docs/platform-version-reuse.md) for the reusable source-profile process and tested platform differences.

## PostgreSQL Setup

Database setup scripts are in [db](db):

- [db/00_create_database_and_user.sql](db/00_create_database_and_user.sql)
- [db/01_ingest_schema.sql](db/01_ingest_schema.sql)
- [db/README.md](db/README.md)

Local database connection settings live in `config/local.env`. The import and
validation scripts read that file by default, so normal commands do not need a
password or DSN in the command line:

```bash
python3 scripts/import_event.py https://cup.clubdenkovastaviski.com/2013/ISU/index.htm
python3 scripts/validate_import.py 1
```

Use `config/local.env.example` as the non-secret template for another machine.

### Local PostgreSQL 17 With Docker

For laptop testing, start an isolated PostgreSQL 17 container:

```bash
docker compose up -d postgres
```

The container creates the same local app database/user used by the project:

- database: `skating_data`
- user: `skating_app`
- host port: `55432`

Point project scripts at the container:

```bash
cp config/local.docker.env.example config/local.env
```

Then apply the schema and seed scripts you need for the current task:

```bash
db/psql_local.sh -v ON_ERROR_STOP=1 -f db/01_ingest_schema.sql
db/psql_local.sh -v ON_ERROR_STOP=1 -f db/02_seed_source_profiles.sql
db/psql_local.sh -v ON_ERROR_STOP=1 -f db/03_seed_validation_legend.sql
db/psql_local.sh -v ON_ERROR_STOP=1 -f db/06_source_url_registry.sql
```

To override local credentials or the exposed port, create a repo-local `.env`
file. It is ignored by git:

```env
SKATING_POSTGRES_PORT=55432
POSTGRES_PASSWORD=postgres
SKATING_APP_DB=skating_data
SKATING_APP_USER=skating_app
SKATING_APP_PASSWORD=change_me_strong_password
```

## PDF Parsing

Judges-score PDFs are source data and should be imported, not treated only as attachments.

Example:

`https://cup.clubdenkovastaviski.com/2013/ISU/AdvancedNoviceBoys_SP_Scores.pdf`

The current PDF adapter can use one of these text-extraction backends:

- `pypdf`
- `PyPDF2`
- `pdftotext` from poppler

After one backend is installed:

```bash
python3 scripts/parse_judges_pdf.py https://cup.clubdenkovastaviski.com/2013/ISU/AdvancedNoviceBoys_SP_Scores.pdf --validate
```

The PDF parser captures report metadata and per-skater score summaries. For FS Manager PDFs it also captures executed elements, per-judge GOE marks, program component judge marks, deductions, bonus values where present, and inferred judge count.

PDF summary parsing has been checked against:

```bash
python3 scripts/parse_judges_pdf.py https://cup.clubdenkovastaviski.com/2013/ISU/AdvancedNoviceBoys_SP_Scores.pdf --validate
python3 scripts/parse_judges_pdf.py https://cup.clubdenkovastaviski.com/2024/ISU/FSKMSINGLES-----------QUAL000100--_JudgesDetailsperSkater.pdf --validate
python3 scripts/parse_judges_pdf.py https://cup.clubdenkovastaviski.com/2024/NonISU/FSKMSINGLES-ADVNOV----QUAL000100--_JudgesDetailsperSkater.pdf --validate
python3 scripts/parse_judges_pdf.py https://sofia-trophy.clubdenkovastaviski.com/2026/ISU/FSKMSINGLES-----------QUAL000100--_JudgesDetailsperSkater.pdf --validate
python3 scripts/parse_judges_pdf.py https://www.bsf.bg/figure-skating/ice-peak-trophy/18-19.04.2026/FSKWSINGLES-BASNOV----FNL-000100--_JudgesDetailsperSkater.pdf --validate
```

## Web API And Dashboard

The app reads from the analytics/mart views and a small API-facing cache layer.
Build or refresh the API materialized views after imports and mart rebuilds:

```bash
db/psql_local.sh -v ON_ERROR_STOP=1 -f db/28_api_dashboard_views.sql
```

Run the local dashboard:

```bash
.venv/bin/python -m uvicorn skating_api.app:app --host 127.0.0.1 --port 8000
```

Open `http://127.0.0.1:8000`. The first screen shows the dashboard; the side
rail supports club, skater, and event search.
