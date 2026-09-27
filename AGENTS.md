# merch-web production rules

- Work only on `codex/full-production-audit`, except `codex/hotfix-four-production-fixes` is permitted for the four fixes explicitly authorized by the user and the documentation, example configuration, Git ignore rules, and local test setup cleanup authorized on 2026-09-27. This cleanup does not authorize application or live deployment-script changes. Production merge or deployment is allowed only after the user explicitly confirms it in the current conversation; otherwise, do not change production.
- Treat `origin/main` as production baseline only after fetching every remote ref.
- The user also authorized the targeted import-error disclosure fix on `codex/hotfix-four-production-fixes` on 2026-09-27, with production rollout only after its checks pass. This does not authorize other deferred audit fixes.
- Preserve existing routes, imports, exports, reports, and user data.
- Schema changes are additive and idempotent. Never delete legacy fields during migration.
- Legacy financial backfill runs only from an explicit administrative command with `--dry-run` or `--apply`.
- State-changing HTTP operations use POST and CSRF protection. Compatibility GET routes may redirect but must not mutate.
- Merchant identity comes from a signed session, never from `fio` in a URL.
- Receipts are durable, size-limited, content-validated, and private to their owner and authenticated administrators.
- Visit slots are centralized constants. Intersections require the same point, date, explicit work slot, and different merchants.
- Production-calendar data is stored in PostgreSQL and updated administratively, not fetched during page rendering.
- Excel imports validate the complete workbook before one transaction; never commit inside a row loop.
- Run compile, unit/integration tests, Uvicorn smoke checks, migration dry-run/apply/reapply, route inventory, and final diff review.
