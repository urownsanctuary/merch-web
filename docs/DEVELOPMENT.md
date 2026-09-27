# Local development and regression checks

Use Python 3.12, matching `Dockerfile`. Runtime dependencies are pinned in
`requirements.txt`; `requirements-dev.txt` adds the tested HTTP client and runner
without changing the production Docker dependency set.

## Windows: isolated test setup

Open a new PowerShell in the repository root. Do not load production `.env` files.
Create a new virtual environment, or use an existing dedicated project environment:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
```

Set the environment **explicitly**. Several tests use `setdefault`, so inherited
production values would otherwise take precedence. These are synthetic test values:

```powershell
$env:DATABASE_URL = 'sqlite+pysqlite:///:memory:'
$env:ENVIRONMENT = 'test'
$env:SECRET_SALT = 'test-secret-salt-at-least-24-characters'
$env:SESSION_SECRET = 'test-session-secret-at-least-24-characters'
$env:ADMIN_LOGIN = 'admin'
$env:ADMIN_PASSWORD = 'strong-password'
$env:AUTO_SYNC_PRODUCTION_CALENDAR = '0'
$env:MAINTENANCE_MODE = '0'
$env:MAX_RECEIPT_BYTES = '15728640'
.\.venv\Scripts\python.exe -B -m pytest -q -p no:cacheprovider
.\.venv\Scripts\python.exe -m compileall -q app
.\.venv\Scripts\python.exe -m pip check
git diff --check
```

Run each check only after the previous succeeds (`$LASTEXITCODE -eq 0`). Close the
dedicated shell afterwards to discard test variables. Linux/macOS use the same
variables/commands with `.venv/bin/python`. Never connect tests to a shared DB.

The suite builds synthetic in-memory SQLite fixtures, including destructive test
DDL, and tests legacy migration against those fixtures. Snapshot tests briefly
write an ignored synthetic SQL file in the working directory and remove it.
`compileall` writes ignored bytecode; neither operation is a production migration.

## PostgreSQL and browser validation

SQLite success is not PostgreSQL DDL/concurrency or desktop/mobile browser PASS.
Use a separate disposable PostgreSQL 18 instance/volume, separate secrets and
synthetic data or an approved anonymized snapshot. Never reuse production volumes,
endpoints or a schema on the production DB.

No reviewed complete fresh-database bootstrap is packaged in this repository.
Startup creates/extends some tables but does not establish that every legacy base
table exists. Obtain and review the synthetic schema/bootstrap from the maintainer;
do not substitute a production dump or automatically apply legacy migration.
The anonymized snapshot covers migration fields only, not a full app bootstrap.

After preparing the test DB, start Uvicorn on loopback only. Bound startup and HTTP
waits, run intended desktop/mobile checks, and stop/wait for the specific process
in a `finally` block. Do not leave Uvicorn or browser smoke waiting indefinitely.
No server startup is required for documentation-only changes.

Future code releases must also check changed paths, imports/exports, transaction
rollback, submitted-month locks and receipt access/persistence, then review the
final diff. Migration dry-run/apply/reapply belongs only to the explicitly isolated
migration test, never the production handoff procedure.

## Source map

- `app/main.py`: routes and server-rendered interface.
- `app/services.py`: queries, totals, imports and legacy adjustment access.
- `app/coffee_days.py`: coffee count and manual adjustments.
- `app/merchant_admin.py`: merchant administration and credential reset.
- `app/security.py`: sessions and CSRF helpers.
- `app/production_calendar.py`: approved calendar synchronization.
- `app/legacy_migration.py`: explicit command, not routine startup.
- `tests/`: synthetic regression coverage; record results per run.

## Safe working files

Store local receipts, screenshots, traces and logs under ignored `artifacts/` or
`test-results/`; keep uploads/backups out of Git. Reviewed synthetic fixtures under
`tests/` are not blanket-ignored. Ignore rules are not a secret scanner: they do
not remove tracked files or protect against `git add -f`. Review status and staged
diff before committing. Never put a real DSN, private key, production export,
personal data or receipt into a test fixture.
