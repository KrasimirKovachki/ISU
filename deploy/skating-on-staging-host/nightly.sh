#!/bin/sh
# Edited 2026-10-13 by Claude (AI assistant): nightly Skating Database job on 130.61.157.150 (cron 03:30 UTC).
# Owner rule: keep every source file, notice when sources change, and pick up new events by itself.
#   1 backup  2 discover new European Criterium result sites  3 import what is ready (archive first)
#   4 source-typo corrections  5 update only the changed events  6 flag possible duplicates
#   7 re-check 200 sources against the archive (rotating)  8 report -> /opt/skating/reports/
set -u
cd /opt/skating
exec 9>/tmp/skating-nightly.lock; flock -n 9 || { echo "already running"; exit 0; }
D=$(date -u +%Y%m%d); R=reports/nightly-$D.log
X="docker exec -w /app skating-api python"
P="docker exec skating-db psql -U skating_app -d skating_data -At"
{
echo "== $(date -u +%FT%TZ) start"
docker exec skating-db pg_dump -U postgres -d skating_data -Fc > backups/nightly-$D.dump && echo "backup $(du -h backups/nightly-$D.dump | cut -f1)"
ls -1t backups/nightly-*.dump 2>/dev/null | tail -n +15 | xargs -r rm --
echo "== discover"; $X scripts/discover_europeancriterium.py --delay 1 --profiles-json /app/config/source_profiles.json 2>&1 | tail -3
$X scripts/sync_source_profiles.py 2>&1 | tail -2
echo "== import"; BEFORE=$($P -c "select coalesce(max(id),0) from ingest.import_runs")
$X scripts/import_ready_registry.py 2>&1 | python3 -c "import json,sys; d=json.load(sys.stdin); print(d[\"count\"], \"tried;\", sum(x[\"status\"]==\"imported\" for x in d[\"results\"]), \"imported\")" 2>&1
echo "== corrections"; $P -c "select ingest.apply_corrections()"
echo "== refresh"; $X scripts/refresh_events.py 2>&1 | tail -1
EV=$($P -c "select array_to_string(ingest.apply_corrections(), chr(32))"); [ -n "$EV" ] && $X scripts/refresh_events.py $EV | tail -1
echo "== possible duplicates among new events (same week, >=50% same skaters)"
$P -F" | " -c "
WITH new AS (SELECT e.event_key, e.event_name, e.event_start_date FROM api.mv_dim_event e WHERE e.import_run_id > $BEFORE),
sk AS (SELECT s.event_id, ai.identity_key FROM api.mv_skater_appearances_with_club s JOIN api.mv_appearance_identity ai USING (skater_appearance_id))
SELECT n.event_key, n.event_name, o.event_key, o.event_name,
       round(100.0 * count(DISTINCT b.identity_key) / NULLIF((SELECT count(DISTINCT identity_key) FROM sk WHERE event_id = n.event_key), 0)) AS pct_same
FROM new n JOIN api.mv_dim_event o ON o.event_key <> n.event_key AND abs(o.event_start_date - n.event_start_date) <= 7
JOIN sk a ON a.event_id = n.event_key JOIN sk b ON b.event_id = o.event_key AND b.identity_key = a.identity_key
GROUP BY 1, 2, 3, 4 HAVING count(DISTINCT b.identity_key) >= 0.5 * (SELECT count(DISTINCT identity_key) FROM sk WHERE event_id = n.event_key)"
echo "== sources vs archive"; $X scripts/archive_check.py --remote 200 --list-missing 0 2>/dev/null | python3 -c "import json,sys; d=json.load(sys.stdin); print(\"local\", d[\"local\"], \"remote\", d[\"remote\"])"
$P -F" | " -c "select status, count(*) from ingest.source_checks where checked_at > now() - interval '1 day' group by 1"
echo "== $(date -u +%FT%TZ) done"
} > "$R" 2>&1
