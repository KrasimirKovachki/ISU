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
