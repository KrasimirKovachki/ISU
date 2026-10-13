# Skating Database on 130.61.157.150 (shared with the staging academy) — since 2026-10-13

Owner: "we cannot create new server" — the Skating Database moved here from 130.110.71.83 (1 GB micro).
Host: Oracle A1.Flex, 2 OCPU / 12 GB, ARM. Login: `ssh -i ora_test ubuntu@130.61.157.150`.

- `/opt/skating` — compose project `skating` (this folder's `compose.yaml`): skating-db (PostgreSQL 17, 2 GB cache),
  skating-api, skating-dash. Secrets in `/opt/skating/.env` (mode 600, never printed).
- Web: the staging Caddy (`/opt/northstar-staging/Caddyfile.staging`) serves data.icepeaktrophy.com with the block
  in `Caddyfile.data-block` (members-only via academy1 forward_auth; API open to the academy servers).
- Archive: `/opt/skating/archive/data` (source_cache, source_archive, source_versions) mounted at /app/data.
- Database: db/40 per-event tables, db/41-42 levels, db/43 source corrections, api.ec_club (755 clubs).
- After imports: `docker exec -w /app skating-api python scripts/refresh_events.py` (only changed events).
- Nightly (cron 03:30 UTC): `tools/nightly.sh` -> `/opt/skating/reports/nightly-YYYYMMDD.log`; backups in
  `/opt/skating/backups/nightly-*.dump` (14 kept).
- Build on the server itself (ARM, ~1 min): `docker compose build app dash && docker compose up -d app dash`.
