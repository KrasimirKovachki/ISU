# ISU Source Import Model

## Import Principles

- Treat every URL as source data: HTML pages and PDF files both matter.
- Store raw source identity for every parsed row: source profile, URL, document type, import run, and source row/order where available.
- Preserve raw labels from the source even when normalized fields are added.
- Design reimport as an upsert process, not a one-time scrape.
- Separate old ISUCalcFS parsing from the newer platform parser.

## Old ISUCalcFS Documents

- `index.htm`: event metadata, category links, segment links, judges-score PDF links, schedule.
- `CAT###EN.HTM`: category entries.
- `CAT###RS.HTM`: final category result.
- `SEG###.HTM`: segment result details.
- `SEG###OF.HTM`: officials and technical people for a category segment.
- `*_Scores.pdf`: judges details per skater. This is the source for detailed per-skater scoring and judge columns.

## Swiss Timing FS Manager Documents

The 2024 ISU source uses the newer Swiss Timing FS Manager format.

Example:

- `https://cup.clubdenkovastaviski.com/2024/ISU/index.htm`

Additional FS Manager examples now in scope:

- `https://cup.clubdenkovastaviski.com/2024/NonISU/index.htm`
- `https://sofia-trophy.clubdenkovastaviski.com/2026/ISU/`
- `https://www.bsf.bg/figure-skating/ice-peak-trophy/18-19.04.2026/index.htm`

Differences from the old ISUCalcFS format:

- generator metadata is `FS Manager by Swiss Timing, Ltd.`
- links use lowercase `.htm`
- officials pages are labeled `Panel of Judges`
- result-detail links are labeled `Starting Order / Detailed Classification`
- event protocol PDF can be linked from the index
- entries/results include ISU bio links such as `/bios/isufs00107843.htm`
- categories include modern names such as `Men`, `Women`, `Ice Dance`, `Junior Men`, `Junior Women`, `Junior Ice Dance`
- segment components use modern component names such as `CO`, `PR`, `SK`

The ISU bio id should be stored as a source skater identifier when present, but source appearances should still be retained separately.

The same `fs_manager` parser can be used for ISU, NonISU, Sofia Trophy, and federation-hosted pages, but the importer must store source context:

- host/domain.
- event path.
- competition stream when present in URL, such as `ISU` or `NonISU`.
- root URL.

Do not merge source profiles only because the generator is the same. The generator/parser profile is `fs_manager`; the source context identifies event family and stream.

## Representation Settings

Some FS Manager events represent skaters by country/nation, while local or federation-hosted events can also include club representation.

Example:

- Ice Peak Trophy 04.2026 entries/results include both `Club` and `Nation`.
- International ISU sources normally include `Nation` only.

This should be controlled by source-profile settings, not hardcoded import logic.

Current config file:

- `config/source_profiles.json`

Recommended import behavior:

- Always store raw `nation` when present.
- Always store raw `club` when present.
- Use profile setting `representation.primary` to decide the primary representation shown/imported for that source.
- For Ice Peak Trophy, `representation.primary = club`.
- For ISU/international events, `representation.primary = nation`.

Database implication:

- `source_skater_appearances` should include nullable `nation_code`, nullable `club_name`, `representation_type`, and `representation_value`.
- Canonical skater matching should still use name + nation/club + source context carefully, not overwrite canonical identity with a club-only assumption.

## PDF Score Data

Judges-score PDFs are source documents and must be imported alongside HTML.

Current parser coverage:

- old ISUCalcFS Crystal Reports PDF summary rows.
- Swiss Timing FS Manager PDF summary rows.
- Swiss Timing FS Manager executed element rows.
- Swiss Timing FS Manager per-judge GOE marks.
- Swiss Timing FS Manager program component judge marks.
- Swiss Timing FS Manager deductions detail.
- Swiss Timing FS Manager bonus column when present.
- Structured element markers from both element code and info columns, including `!`, `<`, `<<`, `q`, `e`, `F`, `x`, `b`, `*`, and `REP`.
- event name.
- category.
- segment.
- printed timestamp.
- skater rank.
- skater name.
- nation.
- starting number.
- total segment score.
- total element score.
- total program component score.
- total deductions.

FS Manager PDFs can compress values together, for example `82.9745.24` for TSS/TES and `12.810.00` for PCS/deductions. The parser splits these values by validating score arithmetic.

For element rows, keep both:

- `raw_element`: original source element text.
- `element_code`: source element code as printed.
- `base_element_code`: marker-stripped element code for grouping/search.
- `markers`: normalized list of detected markers.

Remaining PDF import work:

- Map PDF judge columns back to officials from `SEG###OF.htm` using judge number.
- Add detailed element/component parsing for old ISUCalcFS Crystal Reports PDFs if required.
- Store raw extracted PDF text and/or raw PDF file reference for audit.

Validation rule:

- PDF skater summary count should match the corresponding segment result HTML row count when both documents are available.
- PDF judge columns should not be treated as a fixed count. Use the officials page for active judge count and map by `Judge No.N`.

## Officials And Judges

Officials must be stored as segment assignments, not only as global people.

Each official row should keep:

- raw function, for example `Judge No.1`, `Technical Controller`, `Replay Operator`.
- normalized role group: `judge`, `referee`, `technical_panel`, `event_operations`, or `other`.
- judge number when present.
- person name.
- nation.
- category.
- segment.
- source URL.
- import run.

Judge count is variable. Do not hardcode 3, 5, 7, or 9 judges. Use the officials HTML for the active judges in a segment, and map PDF `J1`, `J2`, etc. columns to `Judge No.1`, `Judge No.2`, etc.

Technical people are first-class data:

- Referee.
- Technical Controller.
- Technical Specialist.
- Assistant Technical Specialist.
- Data Operator.
- Replay Operator.

## Skater Uniqueness

The early dedupe key should be conservative:

- normalized full name.
- parsed given/family name when reliable.
- nation/country.
- source profile.

Do not permanently merge skaters only by name. Names can repeat, nations can change, spellings can vary, and pairs/dance may include partner/team contexts. Store source appearances separately, then link them to a canonical skater profile through a match table.

Recommended tables:

- `skaters`: canonical person/profile record.
- `source_skater_appearances`: one row per source occurrence, with raw name, normalized name, nation, source URL, category, segment, import run.
- `skater_identity_matches`: maps source appearances to canonical skaters with match method and confidence.

## Reimport Safety

Use stable source keys for upsert:

- source profile id.
- event source URL.
- document URL.
- category name.
- segment name.
- source row number or source-local order.
- raw name and nation where no source id exists.

Keep import runs:

- `import_runs`: started/finished status, source profile, root URL.
- `source_documents`: URL, content hash, fetched at, document type, parse status.
- `parse_issues`: structured warnings/errors.

This lets the system re-fetch the same event, detect changed documents, update parsed rows, and keep an audit trail.
