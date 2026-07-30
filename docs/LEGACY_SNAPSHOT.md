# Anonymized legacy migration snapshot

Run this only from the existing production service's Render Shell. The command
uses the already configured `DATABASE_URL`; do not paste or print that value.

```sh
python -m app.legacy_snapshot --output /tmp/merch-web-legacy-snapshot.sql
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

The exporter does not query `merchants`, so names, phone suffixes, password
hashes, email addresses, and TU values never enter the snapshot process.
Merchant IDs, point codes, row IDs, normalized IDs, receipt IDs, and legacy
keys are deterministically remapped. Free text is reduced to placeholders while
line breaks and migration delimiters are retained. Receipt paths are replaced
with test-only paths; only extension, content type, byte count, ownership link,
and timestamp metadata are retained.

The output report prints row counts, excluded fields, privacy scan counts, the
absolute path, and SHA-256. The output file and its atomic temporary file are
created with owner-only permissions. Snapshot patterns are ignored by Git.

## Secure download

Use the SSH destination shown in the production service's Render
**Connect → SSH** panel. From a trusted local machine, copy the file over
SFTP-backed SCP:

```sh
scp -s YOUR_SERVICE@ssh.YOUR_REGION.render.com:/tmp/merch-web-legacy-snapshot.sql ./
```

For a service with multiple instances, use the same instance-specific hostname
for both the Render Shell run and the download. Verify the local SHA-256 against
the exporter report before sharing the file. Do not expose it through an HTTP
route, paste it into logs, or commit it.

## Loading into an isolated test PostgreSQL

Use a blank, disposable database. The SQL refuses to load without its explicit
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

Never use `DATABASE_URL` from the production service for loading or migration.
