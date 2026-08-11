#!/bin/sh
set -eu

PROJECT_DIR=${PROJECT_DIR:-/opt/salary-prod/app}
ENV_FILE=${SALARY_ENV_FILE:-/opt/salary-prod/config/production.env}
BACKUP_DIR=${BACKUP_DIR:-/var/backups/salary-prod}
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
FINAL="$BACKUP_DIR/salary-$STAMP.dump"
TEMP="$FINAL.tmp"

umask 077
mkdir -p "$BACKUP_DIR"
cd "$PROJECT_DIR"

cleanup() { rm -f "$TEMP"; }
trap cleanup EXIT INT TERM

docker compose --env-file "$ENV_FILE" exec -T postgres sh -c \
  'exec pg_dump --format=custom --compress=9 --no-owner --no-privileges --dbname="$POSTGRES_DB" --username="$POSTGRES_USER"' \
  > "$TEMP"

test -s "$TEMP"
mv "$TEMP" "$FINAL"
sha256sum "$FINAL" > "$FINAL.sha256"
chmod 600 "$FINAL" "$FINAL.sha256"
printf 'BACKUP_FILE=%s\n' "$FINAL"
printf 'BACKUP_SHA256=%s\n' "$(cut -d ' ' -f 1 "$FINAL.sha256")"
