# merch-web

Merchant reconciliation service: FastAPI, PostgreSQL, server-rendered HTML,
and Excel imports/exports. Python 3.12 is used by the production image.

## Start here

- [Local setup and regression checks](docs/DEVELOPMENT.md)
- [Current FirstVDS operations and safety gates](docs/VPS_RUNBOOK.md)
- [Handoff status and known limitations](docs/HANDOFF_STATUS.md)
- [Read-only anonymized snapshot](docs/LEGACY_SNAPSHOT.md)
- [Repository safety rules](AGENTS.md)

Production runs on FirstVDS, not Render. The root-level audit, logic comparison,
route compatibility, and test deployment reports describe historical releases;
do not use their commands, deployment targets, or PASS results as current state.
The current runbook records a dated observation, not a permanent release pin.

Do not run the application or tests with production credentials on a development
machine. Startup can perform additive schema writes. Never enable legacy
financial migration automatically: see the storage warning in the runbook.

## Russian production calendar

When explicitly approved for the selected database,
`python -m app.production_calendar --year 2026` synchronizes a complete,
validated and officially approved year into PostgreSQL. The currently bundled
manifests use:

- Government Resolution № 1335 of 4 October 2024 for 2025;
- Government Resolution № 1466 of 24 September 2025 for 2026.

The application starts the same idempotent synchronization in a daemon
background task for the current and next approved year. Set
`AUTO_SYNC_PRODUCTION_CALENDAR=0` to disable it. An administrator can also
start it from “Управление данными”. Unapproved future years are never
generated. Manual date overrides have priority over later syncs and are
audited; the original official value is retained for reset.
