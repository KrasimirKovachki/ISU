# Zero-Element PDF Audit

This audit checks `ingest.pdf_score_summaries` rows that currently have no parsed
`ingest.pdf_elements` rows and verifies whether the source PDF evidence is
present in the local archive.

## Summary

- Zero-element PDF score summaries: `4816`
- With archived PDF evidence: `4788`
- With archive path present: `4788`
- Without archived evidence metadata: `28`

## What this means

- The PDFs exist for the overwhelming majority of the zero-element rows.
- The gap is primarily parser coverage, not missing source documents.
- The UI can still show the score summary and evidence links, but element rows
  remain unavailable until the PDF parser is extended for these report formats.

## Daniela Stoeva

Daniela is a clear example of the issue:

- `Denkova-Staviski Cup 2017 · Senior Ladies · 2017-10-31`
- `DANIELA STOEVA · Free Skating`
- `DANIELA STOEVA · Short Program`
- the archived PDFs are present for both segments
- the PDF text contains executed element rows, but the current parser does not
  materialize them into `ingest.pdf_elements`

## Evidence-backed zero-element summaries

Examples of skaters with archived PDFs and no parsed elements:

- `DANIELA STOEVA`
- `ALEXANDRA FEIGIN`
- `PRESIYANA DIMITROVA`
- `KRISTINA GRIGOROVA`
- `IVELINA BAYCHEVA`
- `DANIELA NIKOLOVA`
- `DANIELA STANIMIROVA`

The same pattern repeats across many skaters and events, especially older
FS Manager / judges-score PDFs.

## Non-archived zero-element summaries

There are `28` zero-element summaries where the archive metadata is missing or
not marked `written`. These are concentrated in a small set of current
fsresults/frponline URLs and should be treated as a separate archive-loader
follow-up.
