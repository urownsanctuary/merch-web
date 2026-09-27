# Test deployment and acceptance report

> Historical Render test evidence from July 2026. These URLs, credentials templates,
> limits, test counts and rollback instructions are not current production settings.
> Do not provision Render resources or repeat these mutations during handoff.
> See [FirstVDS operations](docs/VPS_RUNBOOK.md) and [local checks](docs/DEVELOPMENT.md).

Date: 2026-07-29

## Isolation and deployed revisions

- Test URL: `https://merch-web-audit-b8cdf8f-eu.onrender.com`
- Render service: `merch-web-audit-b8cdf8f-eu`, Frankfurt, free test service
- Source branch: `codex/full-production-audit`
- Auto-deploy: disabled
- Requested revision deployed and tested first:
  `b8cdf8fc0770a66dafaf7620776c18164710bdf0`
- Database: an existing non-production Render PostgreSQL 18 resource
- Isolation: dedicated schema `merch_audit_b8cdf8f` selected with `PGOPTIONS`
- Production service and production database: not used or modified

The final audit commit is deployed to this same test service after the complete
local gate. The Render deploy page is the authoritative source for its full SHA.

## Safe environment configuration

Required names:

| Variable | Safe test value / rule |
|---|---|
| `DATABASE_URL` | Internal URL of the non-production database only |
| `PGOPTIONS` | `-c search_path=merch_audit_b8cdf8f,public` |
| `SECRET_SALT` | New random test-only value, at least 32 characters |
| `SESSION_SECRET` | Different new random test-only value, at least 32 characters |
| `ADMIN_LOGIN` | Audit-only login, not a production account |
| `ADMIN_PASSWORD` | Random test-only password, at least 20 characters |
| `ENVIRONMENT` | `production` so secure-cookie behavior is exercised |
| `MAX_RECEIPT_BYTES` | `5242880` |
| `PORT` | Managed by Render; do not hard-code |

Never commit database URLs/passwords, `SECRET_SALT`, `SESSION_SECRET`,
administrator credentials, or Render deploy hooks to Git.

## Production-like anonymized fixture

The bootstrap inserted synthetic data only: 3 merchants, multiple territories
and points, 3 rates, 4 supplies (including 1, 4, and 5 boxes), a
`pay_lt5=true` point, 7 visits with morning/evening/full-inventory coverage,
1 submitted monthly reconciliation, 3 legacy adjustment rows, and 2 receipt
files. It contains both real overlap shapes and non-overlap controls. Names,
phone suffixes, comments, and receipt contents are synthetic.

## Legacy migration evidence

Commands executed inside the isolated test service:

```text
python -m app.legacy_migration --dry-run
python -m app.legacy_migration --apply
python -m app.legacy_migration --apply
```

Sanitized results:

| Run | Legacy found | Notes | Reimbursements | Receipts | Ambiguous | Existing skipped |
|---|---:|---:|---:|---:|---:|---:|
| dry-run | 3 | 3 | 2 | 2 | 1 | 0 |
| apply | 3 | 3 | 2 | 2 | 1 | 0 |
| repeated apply | 3 | 0 | 0 | 0 | 1 | 7 |

Verification after both applies: all 3 legacy rows and their old columns
remained; normalized tables contained exactly 3 notes, 2 reimbursements, and
2 receipts. Legacy financial totals matched normalized totals (notes 250,
reimbursements 425). The deliberately damaged/ambiguous row was reported and
did not crash the migration. Re-apply created no duplicates.

After browser smoke added compatible aggregate UI data, a redeploy against the
same schema found 4 aggregate rows. Its dry run identified only the newly added
material; apply migrated 2 notes, 1 reimbursement, and 3 receipts while skipping
7 existing normalized items. The immediate re-apply migrated zero items and
skipped 13. Final counts were 5 normalized notes, 3 reimbursements, and 5
receipts. This both confirms PostgreSQL durability across deployment and
idempotency after incremental UI changes.

## Manual browser smoke on PostgreSQL

The following was exercised through the deployed UI:

- Merchant login by FIO and last four digits; FIO case, extra spaces, and
  `ё/е` normalization.
- Point selection and calendar; MORNING and EVENING on the same date; full
  inventory; 1/4/5-box supplies and `pay_lt5=true`.
- Two notes, deletion of one, and preservation of the second.
- A no-supply correction and, after the fix, suppression/rejection of a second
  correction for the same date.
- Two reimbursements, multiple PDF receipts, deletion of one reimbursement,
  preservation of the other and correct totals.
- A point with no visits but with note/reimbursement data.
- Monthly submission and read-only lock after submission, including direct
  mutation endpoints.
- Receipt opening by owner and administrator; HTTP 403 for another merchant
  and for an anonymous request.
- Administrator login, filters, report, data-management page, special inventory
  dates, production-calendar upload, and all three XLSX downloads.
- No tested UI mutation returned 405, an unexpected 403/CSRF error, or a server
  error. Deliberate cross-site mutation requests are rejected with 403.

Receipt rejection for HTML, SVG, executable disguises, size limits, MIME/magic
byte mismatch, and owner/admin/other/anonymous authorization is additionally
covered by automated tests. Receipt durability is checked again after the final
test-service redeploy.

## Automated and runtime gate

The final gate consists of:

```text
python -m unittest discover -s tests -v
python -m compileall app
python -m pip check
git diff --check
uvicorn app.main:app
HTTP GET /db-check and login/admin smoke
```

The 66-test suite covers imports and rollback, legacy dry/apply/reapply,
authorization, file validation, every original route, CSRF/session behavior,
calendar overrides, supply policy, explicit slots, overlap rules, canonical
A/B ordering, and all three exports.

## Rollback

The test service can be rolled back to the previously verified requested SHA
from Render. The database changes are additive; do not drop new tables or
columns. Preserve all legacy rows and receipts. Production rollback remains
redeploying `87fd984`, but no production deployment is part of this audit.
