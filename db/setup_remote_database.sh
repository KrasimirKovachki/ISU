#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PSQL_BIN="${PSQL_BIN:-psql}"
if ! command -v "$PSQL_BIN" >/dev/null 2>&1; then
  if [ -x /usr/local/opt/libpq/bin/psql ]; then
    PSQL_BIN=/usr/local/opt/libpq/bin/psql
  elif [ -x /opt/homebrew/opt/libpq/bin/psql ]; then
    PSQL_BIN=/opt/homebrew/opt/libpq/bin/psql
  else
    echo "psql client not found. Install libpq/postgresql-client first." >&2
    exit 1
  fi
fi

DB_HOST="${DB_HOST:-192.168.1.18}"
DB_PORT="${DB_PORT:-5432}"
SUPERUSER="${SUPERUSER:-postgres}"
APP_DB="${APP_DB:-skating_data}"
APP_USER="${APP_USER:-skating_app}"

read -r -p "PostgreSQL host [$DB_HOST]: " input
DB_HOST="${input:-$DB_HOST}"

read -r -p "PostgreSQL port [$DB_PORT]: " input
DB_PORT="${input:-$DB_PORT}"

read -r -p "PostgreSQL superuser [$SUPERUSER]: " input
SUPERUSER="${input:-$SUPERUSER}"

read -r -p "Application database [$APP_DB]: " input
APP_DB="${input:-$APP_DB}"

read -r -p "Application user [$APP_USER]: " input
APP_USER="${input:-$APP_USER}"

read -r -s -p "Password for PostgreSQL superuser $SUPERUSER: " SUPERPASS
echo
read -r -s -p "Password to set for application user $APP_USER: " APP_PASSWORD
echo

echo "Creating database and application role..."
PGPASSWORD="$SUPERPASS" "$PSQL_BIN" \
  -h "$DB_HOST" \
  -p "$DB_PORT" \
  -U "$SUPERUSER" \
  -d postgres \
  -v ON_ERROR_STOP=1 \
  -v app_db="$APP_DB" \
  -v app_user="$APP_USER" \
  -v app_password="$APP_PASSWORD" \
  -f "$SCRIPT_DIR/00_create_database_and_user.sql"

echo "Creating ingest/core schema..."
PGPASSWORD="$APP_PASSWORD" "$PSQL_BIN" \
  -h "$DB_HOST" \
  -p "$DB_PORT" \
  -U "$APP_USER" \
  -d "$APP_DB" \
  -v ON_ERROR_STOP=1 \
  -f "$SCRIPT_DIR/01_ingest_schema.sql"

echo "Seeding source profiles..."
PGPASSWORD="$APP_PASSWORD" "$PSQL_BIN" \
  -h "$DB_HOST" \
  -p "$DB_PORT" \
  -U "$APP_USER" \
  -d "$APP_DB" \
  -v ON_ERROR_STOP=1 \
  -f "$SCRIPT_DIR/02_seed_source_profiles.sql"

echo "Seeding validation legend..."
PGPASSWORD="$APP_PASSWORD" "$PSQL_BIN" \
  -h "$DB_HOST" \
  -p "$DB_PORT" \
  -U "$APP_USER" \
  -d "$APP_DB" \
  -v ON_ERROR_STOP=1 \
  -f "$SCRIPT_DIR/03_seed_validation_legend.sql"

echo "Database setup complete: $APP_DB on $DB_HOST:$DB_PORT as $APP_USER"
