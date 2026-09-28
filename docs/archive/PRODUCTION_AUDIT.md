# Production audit: archived technical report

> Архивный технический отчёт. Описывает состояние на момент проверки,
> не подтверждает состояние текущего production и не является инструкцией выпуска.

- Central slots: `MORNING`, `EVENING`, legacy `DAY`, and `FULL_INVENT`.
  Existing rows remain `DAY`; new user selections are morning/evening.
- Intersections require identical point/date/MORNING-or-EVENING and different
  merchants. SQL and the overlap export use one canonical A/B ordering.
- PostgreSQL-backed production calendar with complete validated official
  2025/2026 datasets based on Government resolutions № 1335 and № 1466.
  Startup and administrative background synchronization are idempotent and
  transactional. Manual date overrides survive later official synchronization
  and can be reset to the stored official value. XLSX remains an emergency
  fallback; missing years use a weekend fallback without failing page generation.
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


## Validation limitation

The original audit required an anonymized PostgreSQL snapshot and review of
ambiguous legacy records. Synthetic fixtures did not establish coverage of
all real legacy text formats. This archive does not establish closure.
