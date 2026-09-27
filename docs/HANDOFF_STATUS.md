# Handoff status — 2026-09-27

This is a documentation/tooling cleanup, not a release or declaration that every
audit finding is fixed. No application, schema, production configuration or
deployment-script change is included. No production operation is authorized by
this document. Owner-approved scope: audit items 6–9 and non-runtime cleanup.

## Addressed in documentation/tooling

- **6:** replace unsafe project-wide restore cleanup with explicit inspection and
  stopping of only the isolated restore container; retain volumes.
- **7:** mark old audit/checkpoint reports as historical, distinguish incomplete
  checks from PASS, remove a tool-specific explanation without changing its result.
  AGENTS keeps its safety rules and only adds the approved documentation exception.
- **8:** document observed FirstVDS layout, explicit-target app-only release
  contract, legacy-storage guard and missing operations-package handoff.
- **9:** remove personal SSH paths and Render-specific secret placeholders;
  align the example receipt limit with the observed 15 MiB setting. Existing
  production environment files are not modified.
- **14:** ignore environment variants, private keys, local uploads/backups,
  IDE state and designated smoke artifacts; keep sanitized examples and synthetic
  source fixtures trackable. Ignore rules do not scan secrets or untrack files.
- **19 (partial):** add a developer entry point, test-only environment instructions
  and pinned local test dependencies. Runtime requirements and Dockerfile unchanged.

## Production storage observation (read-only)

On 2026-09-27 at 15:15 UTC (18:15 Moscow), the app revision was
`6e5e49d77d221a566a899b0e360d23fed52dc650`. App/postgres/nginx were healthy with
restart count 0. `point_adjustments` and `receipt_files` existed; `point_notes`,
`point_reimbursements`, `reimbursement_receipts` did not. The deployed services
source matched that revision after line-ending normalization.

Thus audit item **4** is a latent migration risk, not a demonstrated current
read/write split. Do not enable normalized tables or legacy migration on production
before a separately reviewed fix and PostgreSQL validation. No financial rows or
receipts were changed, and no migration was executed in this check.

## Deferred by owner

- **1:** sessions use normalized FIO rather than unique merchant ID. Owner reports
  no employees with identical full names. Same-name access risk remains if they appear.
- **2:** old valid merchant sessions can still read their own receipts after
  deactivation. Access-revocation fix deferred; medium priority for this scenario.
- **3:** public legacy `/uploads` mount remains. Production contents/exposure were
  not checked; do not describe this as either a proven leak or a resolved finding.

## Not changed: separate review and regression required

- **5 (remaining scope):** exception details in other administrative redirects,
  including special-inventory-date errors. A separately authorized follow-up fixes
  only the three import handlers described below; this is not a global error rewrite.
- **10–11:** technical/contradictory UI wording and plain technical error responses.
  Changing form confirmation tokens, response formats or redirects can affect clients.
- **12:** incomplete explicit CSRF-token coverage on some mutation routes.
- **13:** spreadsheet formula interpretation in exported text cells.
- **15–16:** potentially unused helpers, debug/API exposure and query credentials.
- **17:** administrator session lifecycle and authentication rate limiting.
- **18:** clear-month coverage of newer storage tables.

These are not all equivalent in severity; they remain open, not dismissed as
harmless because the current business smoke passed. Do not bulk-fix them during
documentation handoff. Review each scope and test affected paths separately.

## Remaining operational handoff gates

- Collect/review the actual FirstVDS release wrapper, Python release tool, ingress
  override and referenced smoke dependencies. Transfer through an approved channel,
  scan secrets, then version only suitable non-secret sources. Presence was checked;
  deploy/rollback was not re-executed in this audit.
- Establish named owners/access for VPS, Git, DNS, TLS renewal, backups and alerts.
  Transfer secrets separately; never put them in Tracker, docs or Git.
- Verify backup freshness, retention, off-host copy and isolated restore; do not
  infer those PASS results from a script or old checkpoint.
- Review/package a synthetic PostgreSQL bootstrap and integration/browser smoke
  harness. Current SQLite fixtures are not a full production bootstrap.
- Decide on isolated CI in a separate task. No workflow, external service,
  paid resource, or deployment automation is added by this cleanup.

See [DEVELOPMENT.md](DEVELOPMENT.md) for local commands and
[VPS_RUNBOOK.md](VPS_RUNBOOK.md) for operations constraints. Historical counts and
PASS statements belong to their original revisions; record new test results for
each release rather than treating old reports as perpetual acceptance.

## Documentation cleanup validation

Local checks on 2026-09-27, with an explicit synthetic SQLite test environment:

- 167 tests and 22 subtests passed. The runner reported 5,554 warnings;
  dependency/API warning cleanup is not part of this non-runtime change.
- `python -m compileall -q app`, `python -m pip check`, `git diff --check`: PASS.
- Ignore-rule matrix: 23 sensitive/artifact paths ignored; 8 intended example,
  fixture and source paths remain trackable (including the env example directory).
- 18 relative document links resolve; fenced blocks are balanced.
- App, tests, runtime requirements, Dockerfile, Compose and deployment scripts
  have no diff against the starting revision.

No SSH, production DB query/write, server restart, browser smoke, migration,
commit, push or deployment was performed for this documentation edit. The earlier
read-only production observation is identified separately above. No browser PASS
or fresh PostgreSQL integration PASS is claimed by this local gate.

## Separately authorized follow-up: import error disclosure

After the documentation-only checkpoint, the owner authorized an import-error fix
and production rollout conditional on passing checks. Only failure branches of
supplies, rates and production-calendar uploads change: rollback is retained,
redirects use fixed user-facing messages, and the existing redacted logger records
error type/trace frames without exception values. Import functions, successful
responses and merchant-import validation are unchanged. Separate regression tests
cover safe redirects/rendering, rollback, auth/CSRF, malformed files and success.
Release results must be recorded separately; this paragraph is not a deployment PASS.
