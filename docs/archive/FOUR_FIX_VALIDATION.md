# Four targeted fixes: archived validation checkpoint

> Архивный технический отчёт. Описывает состояние на момент проверки,
> не подтверждает состояние текущего production и не является инструкцией выпуска.

## Changes

- Receipt logs showed an uncaught `ValueError` from the empty/over-5-MiB check.
  The old exception did not distinguish those two conditions. Metadata checks
  also rejected Android `image/jpg`, octet-stream and missing extensions.
  Supported signatures now determine stored MIME and generated filename; file
  IDs remain random and bytes remain in PostgreSQL. HEIC/HEIF return the requested
  unsupported-format message (no decoder dependency added). Default limit is
  15 MiB/file, at most 10 files and 24 MiB per batch (below nginx's 25 MiB cap).
  Explicit deployment environment overrides still take precedence.
- Receipt schema setup runs at startup. The upload loop never creates schema or
  commits. Every file is validated before inserts. Receipt POSTs use caller-owned
  adjustment writes (`commit=False`), one final commit, rollback on rejection or
  error, CSRF and styled errors. Existing non-upload adjustment callers retain
  their previous default commit behavior.
- `coffee_bonus.days_count` is nullable; NULL preserves the old eligible-visit
  count and does not backfill/recalculate historical rows. Explicit changes are
  bounded, audited in `coffee_days_audit`, and blocked after submission. Reports
  read the same value in their existing query; Excel structure is unchanged.
- Notes/reimbursements without visits already contributed to totals. Regression
  tests cover point/overall/monthly page/admin/payroll/check export, including
  multiple legacy note lines. Their financial selection logic is unchanged.
- Roster reset snapshots historical FIO/TU, clears live credentials and Telegram
  association, deactivates the row and keeps its ID and financial links. Only
  directory audit records are removed. Repetition returns zero. New imports get
  new live rows; archived IDs, reports and receipt bytes remain intact. Archived
  rows cannot be reactivated/edited through admin controls. Old sessions are
  invalidated after reset, including when the same FIO is reimported.

## Verified locally

- 148 tests passed, with 22 subtests, using an isolated venv installed from
  requirements.txt (FastAPI 0.136.0, SQLAlchemy 2.0.49, Uvicorn 0.44.0).
- `python -m compileall -q app`: PASS.
- `python -m pip check`: PASS.
- `git diff --check`: PASS.
- New transaction tests use a real SQLite session and multiple-file multipart
  requests; inject failures after a receipt insert and during adjustment write.
  Both leave zero partial records. A database trigger failure verifies reset
  rollback also restores deleted directory audit records.
- Local browser on synthetic in-memory data: merchant login, calendar and coffee
  decrement/recalculation on desktop and increment at 393x851 passed.
- Mobile receipt form selected two synthetic files. Browser submission was not
  completed, so browser upload PASS is not claimed. Multipart upload scenarios
  passed automated tests.
- Local Uvicorn was bounded to 600 seconds and stopped; no production writes.

## Validation limitations at the time of the report

- PostgreSQL migration/integration and complete desktop/mobile business smoke
  remain required in an isolated test environment. SQLite tests are not a
  substitute for PostgreSQL validation.
- No production deployment, schema application, roster reset, backup or merge
  was performed. Before production promotion, require a verified backup and
  successful isolated PostgreSQL/browser smoke. The previous application image
  must remain available for rollback; do not roll back data destructively.
