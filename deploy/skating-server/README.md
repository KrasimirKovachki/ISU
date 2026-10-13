# Skating Database server (130.110.71.83)

Edited 2026-10-12 by Claude (AI assistant). Since 2026-10-12 the Skating Database runs on its own server instead of
the academy's live host (137.184.50.26, 1 CPU / 1 GB — a restore there slowed the academy sites).

- `/opt/skating/compose.yaml` (this folder): skating-db (Postgres 17), skating-api, skating-dash, skating-caddy.
- `/opt/skating/Caddyfile`: data.icepeaktrophy.com — same rules as before: public assets; `/api/*` open to the academy
  servers (live 137.184.50.26, staging 130.61.157.150); everything else needs a logged-in Ice Peak member
  (forward_auth to https://academy1.icepeaktrophy.com/member/authz/, cookie domain .icepeaktrophy.com).
- `/opt/skating/.env` holds the passwords (created on the server, never in git).
- Build contexts: `/opt/skating/app` (API: Dockerfile, pyproject.toml, isu_parser, skating_api) and `/opt/skating/dashboard`.
- Tools (db/, scripts/, tests/) in `/opt/skating/tools`.
- Oracle cloud firewall: only 22/80/443 are open.

Deploy API code: rsync the files above to /opt/skating/app, then `docker compose build app && docker compose up -d app`.
After an import: `docker exec -i skating-db psql -U skating_app -d skating_data -v ON_ERROR_STOP=1 < tools/db/refresh_api_mvs.sql`
(non-blocking, ~8 min on this server).
Check answers before/after a change: `sh snap.sh <dir>` and compare the folders (`cmp`).
