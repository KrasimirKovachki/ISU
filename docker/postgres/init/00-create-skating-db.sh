#!/usr/bin/env bash
set -euo pipefail

psql \
  -v ON_ERROR_STOP=1 \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  -v app_db="$SKATING_APP_DB" \
  -v app_user="$SKATING_APP_USER" \
  -v app_password="$SKATING_APP_PASSWORD" \
  -f /workspace/db/00_create_database_and_user.sql
