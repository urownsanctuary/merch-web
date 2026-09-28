# Production logic comparison

> Архивный технический отчёт. Описывает состояние на момент проверки,
> не подтверждает состояние текущего production и не является инструкцией выпуска.

Baseline: `origin/main@87fd984`

## Preserved without behavioral removal

- Merchant and administrator login surfaces, active-period handling, point
  selection, calendar, summary, monthly submission, and administrator report.
- Rates import, merchant import, manual merchant creation, supplies import,
  special inventory dates, and all three Excel exports.
- The existing `/0.87` reimbursement calculation and point/month totals.
- Supply payment threshold below five boxes and the `pay_lt5` point override.
- Notes and reimbursements, including multiple independent normalized records,
  item deletion, and points with adjustments but no visits.
- Submitted-month editing lock.
- Legacy `DAY` visit rows remain readable and payable.

## Intentionally changed

- New visit selection is explicit MORNING/EVENING; full inventory remains a
  distinct slot. Legacy `DAY` is accepted only for existing data.
- Intersections now require equal point, date, and MORNING-or-EVENING slot,
  different merchants, and canonical merchant ordering. MORNING+EVENING,
  different points/dates, full inventory, and one merchant are not overlaps.
- Merchant identity is bound to a signed session instead of trusting an FIO
  query parameter.
- Mutating day/inventory/reopen actions use protected POST; old GET paths remain
  non-mutating compatibility redirects.
- Receipts are stored durably in PostgreSQL, validated by size/extension/MIME/
  magic bytes, and restricted to owner or administrator.
- Imports validate all rows before a single transaction commits.
- Production calendar exceptions are administrator-imported into PostgreSQL;
  absent dates retain the ordinary weekend fallback.
- No-supply corrections validate a real paid supply date and reject a second
  correction for the same date.
- Same-origin metadata checks supplement signed SameSite cookies and explicit
  CSRF tokens on sensitive mutation forms.

## Replaced

- The migration-only view of aggregate legacy note/reimbursement text is
  supplemented by normalized item rows. The compatible UI continues updating
  `point_adjustments` and preserves multiple entries as independently removable
  lines; the controlled migration copies those values without deleting or
  rewriting the aggregate fields.
- Startup financial backfill is replaced by the explicit
  `python -m app.legacy_migration --dry-run|--apply` command.
- Volatile/static receipt serving is replaced by database-backed private
  receipt retrieval.

## Added

- Managed production-calendar XLSX import with title, source, and comment.
- Ambiguity reporting and idempotent legacy migration.
- Explicit owner/admin receipt authorization and upload hardening.
- Test coverage for route inventory, imports, migration, session/CSRF,
  calendar/supply/slot rules, intersections, receipts, and exports.

## Removed

No working production route or business capability from `87fd984` was removed.
Three legacy mutation-by-GET handlers no longer mutate, but the paths remain as
compatibility redirects and equivalent protected POST handlers are present.
[route compatibility](ROUTE_COMPATIBILITY.md) contains the route-by-route evidence.
