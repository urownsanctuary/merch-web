# FirstVDS production runbook

Last read-only infrastructure check: **2026-09-27**. Production is
`https://sverka-mm.ru` on FirstVDS `188.120.237.160`, not Render.
This document does not authorize deployment, migration, restore or DNS changes.
Recheck actual state before an approved operation; do not replay the old cutover.

## Infrastructure and secrets

| Component | Location / identity |
|---|---|
| Compose project | `salary-prod` |
| Application checkout | `/opt/salary-prod/app` |
| App | `salary-app` |
| PostgreSQL | `salary-postgres`, PostgreSQL 18 |
| Production DB volume | `salary_pgdata`, mounted at `/var/lib/postgresql` |
| Nginx | `salary-nginx` |
| Secret environment | `/opt/salary-prod/config/production.env`, mode `0600` |
| Backups | `/var/backups/salary-prod`; restrict directory and dump access |
| SSH account | `sverka-deploy`, separate project key |

Observed app revision: `6e5e49d77d221a566a899b0e360d23fed52dc650`.
All three containers were healthy with restart count 0. This is a dated baseline,
not an instruction to deploy that SHA or a fresh login/business-smoke result.

Never print full `docker inspect`, `docker compose config`, environment files,
connection strings or unredacted exceptions into shared transcripts. They can
contain credentials or personal data. Do not copy the example environment over
an existing installation. New isolated environments need separate secrets;
changing the existing salt can break merchant authentication.

For Windows SSH, use your assigned key path, not another developer's home folder:

```powershell
$SverkaKey = 'C:\REPLACE_WITH_YOUR_KEY_DIRECTORY\sverka_vps_ed25519'
ssh -o BatchMode=yes -o IdentitiesOnly=yes -o ConnectTimeout=10 -i $SverkaKey sverka-deploy@188.120.237.160
```

Verify the host key through the agreed trusted channel. Do not disable host-key
checking or enable root/password SSH to work around a missing deployment key.

## Read-only preflight

On FirstVDS, these commands display selected non-secret metadata:

```sh
docker ps --filter name=salary- --format '{{.Names}} {{.Status}}'
docker inspect salary-app --format '{{.Config.Image}} {{index .Config.Labels "org.opencontainers.image.revision"}}'
docker inspect salary-app salary-postgres salary-nginx --format '{{.Name}} {{.Id}} {{.State.StartedAt}} {{.RestartCount}}'
docker inspect salary-postgres --format '{{range .Mounts}}{{.Name}} {{.Destination}}{{println}}{{end}}'
docker image ls salary-app
```

Record revision/image ID, rollback image availability, database/nginx container
IDs, start times and mounts. For database diagnosis enforce `READ ONLY` at
connection and transaction level, with query timeouts. Do not call application
helpers that lazily create tables during a read-only check.

## Application deployment and rollback

The live installation has operational files outside this repository:

- `/opt/salary-prod/bin/deploy-app.sh`;
- `/opt/salary-prod/bin/salary_app_release.py`;
- `/opt/salary-prod/app/compose.ingress.yaml`.

Their presence and hashes were checked; they were not executed during the handoff
audit. Obtain the reviewed operational package and its dependencies from the
maintainer before taking over releases. Git alone is **not** a complete production
deployment package. Do not replace these files with an older local copy or invent
the wrapper's arguments.

Required release contract:

1. Explicit user approval and a fresh verified backup before a production release.
2. Choose explicit full `TARGET_SHA` and rollback SHA. Verify the commit and
   immutable image revision/image ID; retain the previous image.
3. The target must override stale `APP_GIT_SHA` in the env and be written to runtime
   configuration by the reviewed deploy mechanism.
4. Use the existing Compose files, ingress override and secret env. Recreate
   **only app**, with `--no-deps --no-build`; do not recreate PostgreSQL or nginx,
   remove volumes or restore an old env file.
5. Compare the image, revision label and runtime SHA against `TARGET_SHA`, then
   health, `/db-check`, logins and release-specific smoke. Healthy with the wrong
   SHA is a failed deployment.
6. Compare database/nginx IDs and mounts before/after. Investigate nginx upstream
   issues separately; reload only when needed and approved.
7. Rollback uses the same explicit-target app-only mechanism and verified previous
   image. Never roll back production data to fix an application error.

The repository's `compose.yaml` alone is a loopback candidate configuration, not
the live TLS ingress. Do not run a generic `up postgres app nginx` or project-wide
`down` as an application release procedure.

## Backups and isolated restore

`deploy/scripts/backup-postgres.sh` creates a custom-format dump and SHA-256
sidecar. Retention defaults to 14 days; running it can delete expired matching
backup files. Inspect its target and the existing scheduler first; do not install
duplicate cron entries. Historical scheduling used 02:15 UTC. The scheduler,
latest backup freshness, off-host copy and current restore drill were **not
revalidated** by the 2026-09-27 read-only schema check.

Before relying on a backup: record path/time/size/hash; verify SHA-256 and
`pg_restore --list`; perform a separately approved test restore into an isolated
database/volume. Listing a dump does not prove successful restore.

`deploy/scripts/restore-test.sh` targets service `postgres-restore` and uses
`pg_restore --clean --if-exists`: it is destructive to its **test target**.
Before running it, inspect resolved mounts and confirm:

- container: `salary-postgres-restore`;
- Compose project/service labels: `salary-prod` / `postgres-restore`;
- volume: `salary_pgdata_restore`, never `salary_pgdata`;
- no production connection string or production bind mount is used.

After validation, stop only the verified test container. First inspect:

```sh
docker inspect salary-postgres-restore --format '{{index .Config.Labels "com.docker.compose.project"}} {{index .Config.Labels "com.docker.compose.service"}}'
docker inspect salary-postgres-restore --format '{{range .Mounts}}{{.Name}} {{.Destination}}{{println}}{{end}}'
```

Only if those identities/mounts match the isolated target and it is no longer
needed, use `docker stop --time 30 salary-postgres-restore`. This preserves the
test container and volume; removal requires a separate scoped decision. Recheck
that app/postgres/nginx remain unchanged.

**Do not use `docker compose --profile restore down` for this cleanup.** A profile
does not make a project-wide teardown safe for other production services.
Do not use `--volumes`, Docker prune, or remove the production volume.

## Legacy storage: do not enable migration during handoff

On 2026-09-27 production had `point_adjustments` and `receipt_files`, but none of
`point_notes`, `point_reimbursements`, `reimbursement_receipts`. The matching app
therefore reads and writes legacy adjustments consistently.

Creating all three optional tables switches reads to normalized storage while
the UI writer still writes `point_adjustments`. Even empty new tables can hide
existing amounts. Do not run `legacy_migration --apply`, add these tables for
completeness, or restore a normalized test schema into production. Resolve and
test the read/write transition first in isolated PostgreSQL. The snapshot exporter
is separate; see [LEGACY_SNAPSHOT.md](LEGACY_SNAPSHOT.md).

## Maintenance, TLS and logs

`MAINTENANCE_MODE=1` blocks HTTP mutations and skips startup schema/calendar writes.
It is an application control, not a PostgreSQL read-only guarantee. Changing it
and recreating app requires approval; do not toggle it for documentation work.

FirstVDS has live ingress and certificate mounts. Do not copy the TLS example over
them or repeat DNS cutover/certificate issuance. Transfer ownership of DNS,
certificate renewal and backup monitoring through the secure operations channel.
Their current success cannot be inferred from example files.

Review bounded log intervals locally and redact before sharing. Record failures,
timestamps and test scope; do not claim business PASS from health alone.
See [HANDOFF_STATUS.md](HANDOFF_STATUS.md) for remaining handoff gates.
