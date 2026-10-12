#!/bin/sh
# usage: snap.sh <dir> — save API answers for comparison (runs on the skating server)
set -e
D=$1; mkdir -p $D
SK=$(docker exec skating-caddy wget -qO- "http://skating-api:8000/api/skaters?q=krasteva" | python3 -c 'import sys,json; print(" ".join(str(i["skater_key"]) for i in json.load(sys.stdin)["items"][:3]))')
SK="$SK $(docker exec skating-caddy wget -qO- "http://skating-api:8000/api/skaters?q=kris&limit=3" | python3 -c 'import sys,json; print(" ".join(str(i["skater_key"]) for i in json.load(sys.stdin)["items"][:3]))')"
n=0
for p in "/api/dashboard" "/api/dashboard?from_year=2024&to_year=2026" "/api/dashboard?from_year=2013&to_year=2019" "/api/dashboard?from_year=2025&to_year=2025" "/api/meta" "/api/events?limit=100" "/api/events/21/results" "/api/clubs?limit=100" "/api/v1/events/21/schedule" "/api/skaters?q=kris&limit=50" $(for s in $SK; do echo /api/skaters/$s /api/skaters/$s/scores /api/skaters/$s/elements; done); do
  n=$((n+1)); s=$(date +%s%N)
  docker exec skating-caddy wget -qO- "http://skating-api:8000$p" > $D/$n.json
  e=$(date +%s%N); echo "$n $(( (e-s)/1000000 ))ms $(wc -c < $D/$n.json)B $p" >> $D/index.txt
done
cat $D/index.txt
