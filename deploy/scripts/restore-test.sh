#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 /absolute/path/to/backup.dump" >&2
  exit 2
fi

BACKUP=$1
PROJECT_DIR=${PROJECT_DIR:-/opt/salary-prod/app}
ENV_FILE=${SALARY_ENV_FILE:-/opt/salary-prod/config/production.env}

case "$BACKUP" in
  /*) ;;
  *) echo "Backup path must be absolute" >&2; exit 2 ;;
esac
test -r "$BACKUP"

cd "$PROJECT_DIR"
docker compose --env-file "$ENV_FILE" --profile restore up -d postgres-restore

attempt=0
until docker compose --env-file "$ENV_FILE" --profile restore exec -T postgres-restore \
  sh -c 'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"' >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  [ "$attempt" -lt 30 ] || { echo "Restore database did not become ready" >&2; exit 1; }
  sleep 2
done

docker compose --env-file "$ENV_FILE" --profile restore exec -T postgres-restore \
  sh -c 'exec pg_restore --clean --if-exists --no-owner --no-privileges --exit-on-error --dbname="$POSTGRES_DB" --username="$POSTGRES_USER"' \
  < "$BACKUP"

docker compose --env-file "$ENV_FILE" --profile restore exec -T postgres-restore \
  sh -c 'psql --no-psqlrc --set=ON_ERROR_STOP=1 --username="$POSTGRES_USER" --dbname="$POSTGRES_DB" --tuples-only --command="SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '\''public'\'';"'

echo "TEST_RESTORE_OK"
