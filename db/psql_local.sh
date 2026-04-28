#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_FILE="${SKATING_CONFIG_FILE:-$ROOT_DIR/config/local.env}"
PSQL_BIN="${PSQL_BIN:-/usr/local/opt/libpq/bin/psql}"

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "Missing local config: $CONFIG_FILE" >&2
  echo "Copy config/local.env.example to config/local.env and fill local values." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
. "$CONFIG_FILE"
set +a

: "${SKATING_DB_HOST:?Missing SKATING_DB_HOST}"
: "${SKATING_DB_PORT:?Missing SKATING_DB_PORT}"
: "${SKATING_DB_NAME:?Missing SKATING_DB_NAME}"
: "${SKATING_DB_USER:?Missing SKATING_DB_USER}"
: "${SKATING_DB_PASSWORD:?Missing SKATING_DB_PASSWORD}"

PGPASSWORD="$SKATING_DB_PASSWORD" "$PSQL_BIN" \
  -h "$SKATING_DB_HOST" \
  -p "$SKATING_DB_PORT" \
  -U "$SKATING_DB_USER" \
  -d "$SKATING_DB_NAME" \
  "$@"
