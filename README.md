# merch-web

Production-audit branch for the merchant reconciliation service.

## Russian production calendar

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
Web version of merch salary bot
