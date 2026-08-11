# Salary production VPS runbook

This stack is prepared for `sverka-mm.ru` and is intentionally bound to
`127.0.0.1:8080` until the final cutover. It must be reached through an SSH
port forward during candidate validation. Render and DNS remain unchanged.

## Files and secrets

- Application checkout: `/opt/salary-prod/app`
- Secret environment: `/opt/salary-prod/config/production.env` (`0600`)
- PostgreSQL backups: `/var/backups/salary-prod` (`0750`, dumps `0600`)
- Compose project: `salary-prod`
- PostgreSQL volume: `salary_pgdata`

Copy `deploy/env/production.env.example` to the secret environment path and
replace every placeholder. Never print that file in logs. Preserve the exact
Render values for `SECRET_SALT`, `SESSION_SECRET`, `ADMIN_LOGIN`, and
`ADMIN_PASSWORD`. Keep `AUTO_SYNC_PRODUCTION_CALENDAR=0`; the approved calendar
is restored with the database.

## Immutable build and hidden start

From a clean checkout of the intended commit:

```sh
export SALARY_ENV_FILE=/opt/salary-prod/config/production.env
export APP_GIT_SHA=$(git rev-parse HEAD)
test -z "$(git status --porcelain)"
docker compose --env-file "$SALARY_ENV_FILE" build --pull app
docker image inspect "salary-app:$APP_GIT_SHA" --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}'
docker compose --env-file "$SALARY_ENV_FILE" up -d postgres app nginx
BASE_URL=http://127.0.0.1:8080 deploy/scripts/verify-stack.sh
```

From Windows, validate through a tunnel without exposing the candidate:

```powershell
ssh -N -L 18080:127.0.0.1:8080 -o IdentitiesOnly=yes -i "C:\Users\eugen\.ssh\sverka_vps_ed25519" sverka-deploy@188.120.237.160
```

Then open `http://127.0.0.1:18080`. Stop the tunnel after validation.

## Backup and tested restore

Create an atomic custom-format backup:

```sh
/opt/salary-prod/app/deploy/scripts/backup-postgres.sh
```

Verify the emitted SHA-256 and run a restore into the isolated
`salary_pgdata_restore` volume only:

```sh
sha256sum --check /var/backups/salary-prod/salary-YYYYMMDDTHHMMSSZ.dump.sha256
/opt/salary-prod/app/deploy/scripts/restore-test.sh /var/backups/salary-prod/salary-YYYYMMDDTHHMMSSZ.dump
```

`restore-test.sh` cannot target the production PostgreSQL service. Remove the
test stack only after validation with `docker compose --profile restore down`
without `--volumes`; volume deletion is a separate destructive operation.

## Logs and rollback

```sh
docker compose ps
docker compose logs --since 30m app nginx postgres
docker image ls salary-app
```

Application rollback means setting `APP_GIT_SHA` to a previously built and
verified image tag and recreating only `app` and `nginx`. Do not use destructive
database SQL for an application rollback. If schema compatibility is uncertain,
stop and restore into an isolated database first.

## Read-only maintenance mode

Set `MAINTENANCE_MODE=1` only in the target environment and recreate the app
container. GET/read routes and `/db-check` remain available; POST, PUT, PATCH,
and DELETE return a styled HTTP 503 without reaching route handlers. Startup
schema writes, lazy table creation, and calendar synchronization are disabled.
Set it back to `0` and recreate the app to leave maintenance mode. Do not enable
this setting on Render before the separately authorized final cutover.

## Final HTTPS cutover (not yet authorized)

Before cutover: create and test a fresh backup, verify candidate smoke tests,
obtain the certificate, bind ports 80/443 publicly, enable the reviewed TLS
configuration, and only then change DNS. The example TLS configuration is
`deploy/nginx/production-tls.conf.example`; it is not active by default and
must not be enabled before certificate files exist.
