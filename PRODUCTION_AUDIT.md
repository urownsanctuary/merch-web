# Production audit

## Baseline

The repository contained a single FastAPI module with a merchant calendar. It
did not contain an admin login, imports, exports, reconciliation submission,
notes, reimbursements, durable receipts, schema migrations, or tests.

Critical findings in the baseline:

- `fio` in the URL was treated as authorization, so any merchant name could be
  substituted to read and mutate another merchant's reconciliation.
- state changes used GET requests and had no CSRF protection;
- user-controlled values were interpolated into HTML and URLs without escaping;
- the database schema debug endpoint was public;
- uploaded receipts had no durable implementation;
- missing point rates silently used hard-coded amounts;
- no normalized adjustment schema or immutable submitted state existed;
- no automated or startup smoke tests existed.

## Implemented safeguards

- Signed, expiring, HttpOnly merchant sessions and separate administrator
  sessions. Cookies are `Secure` by default and may be made local-only with
  `ENVIRONMENT=development`.
- CSRF validation on every newly added or converted state-changing form.
- HTML escaping, URL encoding, security headers, strict point-code validation,
  exact four-digit credential validation, and constant-time hash comparisons.
- Additive PostgreSQL tables for notes, reimbursements, receipt bytes, monthly
  submission locks, and special inventory dates.
- Idempotent migration guarded by a PostgreSQL advisory transaction lock. The
  legacy `point_adjustments` data remains in place; compatible JSON rows are
  conservatively backfilled without deleting legacy data.
- Stable record IDs, independent deletion, aggregate totals for points with no
  visits, multi-receipt reimbursements, private owner/admin receipt downloads,
  file size/MIME/extension/signature checks, and PostgreSQL-backed receipt data.
- Transactional batched supply import with complete pre-validation, duplicate
  rejection, `has_supply` handling, and row/point counts.
- Separate administrator authentication, one-query filtered reporting, and a
  complete filtered XLSX export.
- Submitted reconciliations lock visits, notes, and reimbursements. Existing
  records and receipts are retained.

## Required production configuration

- `DATABASE_URL`: PostgreSQL connection string.
- `SECRET_SALT`: existing credential salt (must not change while existing
  `pass_hash` values are in use).
- `SESSION_SECRET`: independent random secret of at least 24 characters.
- `ADMIN_PASSWORD`: strong administrator password.
- `ENVIRONMENT=production`.
- Optional: `MAX_RECEIPT_BYTES` (default 5 MiB per file) and
  `MAX_IMPORT_BYTES` (default 20 MiB).

## Deployment plan

1. Snapshot the PostgreSQL database.
2. Restore the snapshot into staging and start this branch once. Startup applies
   additive DDL and conservative backfill.
3. Compare legacy adjustment counts and sums against normalized rows. Exercise
   merchant/admin sessions, private receipt access, import, report, XLSX export,
   and reconciliation locking in staging.
4. Deploy the same commit to a test Render service with production-like
   environment variables.
5. Only after acceptance, schedule a normal production rollout. Keep the legacy
   adjustment storage throughout the observation period.

## Rollback plan

Redeploy the previous application commit. Do not drop the new tables: they are
additive and the previous code ignores them. Restore the database snapshot only
if an independent integrity check proves that a write corrupted existing data.
Receipt content created by this version remains recoverable in PostgreSQL.

## Known limits not claimed as verified

- No production database or anonymized production snapshot was available, so
  the exact legacy `point_adjustments` column shape and production backfill
  counts were not verified.
- The repository does not define morning/evening visit slots or the legacy
  payroll/admin data model needed for a complete intersection workbook and all
  requested payroll columns. The pure intersection rule is implemented and
  tested, but no speculative destructive schema was introduced.
- Rates and merchant imports are not implemented because their workbook schemas
  are absent from the repository and guessing financial column mappings would
  be unsafe. Supply import is implemented for documented canonical/Russian
  columns.
- Official Russian holiday data is not bundled in the repository; Fridays,
  Saturdays, and administrator-managed special inventory dates are supported.
