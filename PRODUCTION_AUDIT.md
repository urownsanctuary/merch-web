# Production audit status

## Baseline verification

The first audit pass used stale `main@c1444fd`. A full remote fetch on
2026-07-29 discovered the actual production line at `origin/main@87fd984`
(`Add receipt file handling and database storage`) with 53 intervening commits.
That version contains the established imports, admin report, three Excel
exports, special inventory dates, notes, reimbursements, and receipt handling.

The audit branch explicitly reverted the stale implementation commits and then
merged `origin/main@87fd984`. Current work is therefore based on the latest code
available in every remote branch/tag/ref. No other remote production branch or
tag exists.

## Implemented on the current production baseline

- Central slots: `MORNING`, `EVENING`, legacy `DAY`, and `FULL_INVENT`.
  Existing rows remain `DAY`; new user selections are morning/evening.
- Intersections require identical point/date/MORNING-or-EVENING and different
  merchants. SQL and the overlap export use one canonical A/B ordering.
- PostgreSQL-backed production calendar with explicit working-day overrides,
  administrative XLSX import, and weekend fallback when a date is absent.
- Full-file validation before writes for rates and merchants, one commit after
  all rows, rollback on errors, and retention of existing data on invalid input.
- Controlled `python -m app.legacy_migration --dry-run|--apply`; application
  startup never performs financial backfill. Legacy fields are never deleted.
- Signed merchant session binding prevents one FIO URL from resolving another
  merchant. Converted mutation forms use POST and CSRF; compatibility GET
  routes only redirect.
- Receipt bytes remain in PostgreSQL across restarts. New files receive an
  owner, are private to that owner/admin, and are checked for size, extension,
  MIME, and magic bytes. HTML, SVG, and executable disguises are rejected.

## Test deployment plan

1. Fetch the exact audit HEAD and build an isolated Render test service.
2. Clone a recent production PostgreSQL snapshot into an isolated staging DB.
3. Set `DATABASE_URL`, unchanged `SECRET_SALT`, a new random
   `SESSION_SECRET`, `ADMIN_LOGIN`, `ADMIN_PASSWORD`, and
   `ENVIRONMENT=production`.
4. Run `python -m app.legacy_migration --dry-run`; archive its JSON report.
5. Review every ambiguous record. Run `--apply` only after counts and ownership
   are approved, then repeat `--apply` and confirm zero new rows.
6. Import representative supplies, rates, merchants, and a production calendar.
7. Exercise owner/other/admin receipt access, morning/evening overlaps, all
   three XLSX exports, submit/reopen locking, and every compatibility route.
8. Keep the test deployment for acceptance; do not point production traffic at
   it until review approval.

## Rollback plan

1. Redeploy production commit `87fd984`.
2. Do not drop new tables or columns; they are additive and the old code ignores
   them.
3. Preserve receipt rows and all legacy `point_adjustments`.
4. If an import was rejected, no data rollback is needed because validation
   precedes writes and the transaction is rolled back.
5. Restore the pre-deploy snapshot only after a separate integrity comparison
   proves existing production rows were corrupted.

## Required before merge

- Run the legacy command against an anonymized production snapshot and resolve
  every ambiguous row.
- Execute the full HTTP/receipt/import/export acceptance sequence against
  PostgreSQL (SQLite is used only for local route smoke tests).
- Review the precise Russian production-calendar workbook for the target year.
- Confirm Render environment variables and perform a test deployment.
- Obtain human review of the large production merge and migration report.
