# Anonymized legacy migration snapshot

Current production is FirstVDS. After explicit approval for a read-only export,
run inside the existing `salary-app` container. The command uses its configured
`DATABASE_URL`; never paste or print that value. It creates only a snapshot file,
not source database rows. Do not run a migration as part of the export.

```sh
docker exec salary-app python -B -m app.legacy_snapshot --output /tmp/merch-web-legacy-snapshot.sql
```

The PostgreSQL source transaction is `REPEATABLE READ, READ ONLY`. Source SQL is
limited to `SHOW` and `SELECT`:

- all migration fields from `point_adjustments`;
- the migration fields and `legacy_key` values from `point_notes`,
  `point_reimbursements`, and `reimbursement_receipts`, when those tables exist;
- `receipt_files.file_id`, `content_type`, `merchant_id`, `created_at`, and
  `octet_length(data)`. The `data` value itself and `original_filename` are not
  selected.
- relevant column names from `information_schema.columns`.

The exporter does not query `merchants`, so it does not read directory names,
phone suffixes or password hashes from that table. Legacy free-text fields can
still contain personal data; their content is sanitized before serialization.
Merchant IDs, point codes, row IDs, normalized IDs, receipt IDs, and legacy
keys are deterministically remapped. Free text is reduced to placeholders while
line breaks and migration delimiters are retained. Receipt paths are replaced
with test-only paths; only extension, content type, byte count, ownership link,
and timestamp metadata are retained.

The output report prints row counts, excluded fields, privacy scan counts, the
absolute path, and SHA-256. The output file and its atomic temporary file are
created with owner-only permissions. Snapshot patterns are ignored by Git.

## Secure download

The file is inside the app container, not the SSH user's `/tmp`. In an authorized
FirstVDS SSH session, copy it to a newly created private staging directory:

```sh
umask 077
SNAPSHOT_STAGE=$(mktemp -d /tmp/sverka-snapshot.XXXXXX)
docker cp salary-app:/tmp/merch-web-legacy-snapshot.sql "$SNAPSHOT_STAGE/merch-web-legacy-snapshot.sql"
chmod 600 "$SNAPSHOT_STAGE/merch-web-legacy-snapshot.sql"
sha256sum "$SNAPSHOT_STAGE/merch-web-legacy-snapshot.sql"
printf 'STAGING_DIRECTORY=%s\n' "$SNAPSHOT_STAGE"
```

On Windows, replace both marked placeholders with your assigned key and the
printed directory's final name, for example `sverka-snapshot.ABC123`
(not the full `/tmp/...` path and not the literal placeholder):

```powershell
$SverkaKey = 'C:\REPLACE_WITH_YOUR_KEY_DIRECTORY\sverka_vps_ed25519'
scp -o BatchMode=yes -o IdentitiesOnly=yes -o ConnectTimeout=10 -i $SverkaKey 'sverka-deploy@188.120.237.160:/tmp/REPLACE_WITH_PRINTED_STAGING_DIRECTORY/merch-web-legacy-snapshot.sql' .
Get-FileHash -Algorithm SHA256 .\merch-web-legacy-snapshot.sql
```

Compare exporter, staging and downloaded SHA-256. Review the privacy scan and
intended recipient before sharing. Never expose the snapshot via HTTP, logs or Git.
After verified download, remove only the exact snapshot file inside the container
and the exact staged file; remove the now-empty staging directory with `rmdir`.
Do not use recursive cleanup or wildcards. Snapshot generation/cleanup is not part
of a read-only schema audit unless explicitly requested.

## Loading into an isolated test PostgreSQL

Use a blank, disposable database on a separate test PostgreSQL instance. The SQL refuses to load without its explicit
test-only psql gate:

```sh
psql -v LEGACY_SNAPSHOT_TEST_ONLY=on "$TEST_DATABASE_URL" \
  -f /secure/path/merch-web-legacy-snapshot.sql
```

After loading, point the application at that disposable database and run:

```sh
python -m app.legacy_migration --dry-run
python -m app.legacy_migration --apply
python -m app.legacy_migration --apply
```

Never use production `DATABASE_URL` for loading or migration. Test-only psql flags
do not prove that a database is disposable: independently verify host and volume.
Do not copy the resulting normalized tables into production. Their existence
changes application reads; see the [legacy storage warning](VPS_RUNBOOK.md#legacy-storage-do-not-enable-migration-during-handoff).
