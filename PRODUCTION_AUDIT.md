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
- PostgreSQL-backed production calendar with complete validated official
  2025/2026 datasets based on Government resolutions № 1335 and № 1466.
  Startup and administrative background synchronization are idempotent and
  transactional. Manual date overrides survive later official synchronization
  and can be reset to the stored official value. XLSX remains an emergency
  fallback; missing years use a weekend fallback without failing page render.
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

## Test deployment

The exact requested revision `b8cdf8fc0770a66dafaf7620776c18164710bdf0`
was deployed first to the isolated Frankfurt Render service
`merch-web-audit-b8cdf8f-eu`. It used an existing non-production Render
PostgreSQL 18 database through the isolated schema
`merch_audit_b8cdf8f`; neither the production service nor production database
was used. The final audit revision is redeployed to the same test service only
after all local checks pass.

The sanitized deployment, fixture, migration, and smoke evidence is recorded in
`TEST_DEPLOYMENT_REPORT.md`.

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

One external action remains: provide an anonymized production PostgreSQL
snapshot and review the ambiguous-record report from a dry run on that snapshot.
The synthetic production-like fixture cannot prove that every real legacy text
format has been represented.
