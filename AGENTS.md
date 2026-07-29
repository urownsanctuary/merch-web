# merch-web production rules

- Work only on a dedicated feature branch. Never push directly to `main` and never deploy production.
- Preserve all existing routes and production data. Schema changes must be additive, idempotent, and backward compatible.
- Never clear production data or make irreversible migrations.
- Financial records and receipts must live in PostgreSQL or durable object storage, never only in Render's ephemeral filesystem.
- Authenticate merchant access with a server-side validated session; a `fio` URL parameter is not authorization.
- Authenticate administrators separately. Protect every state-changing request against CSRF.
- Escape user-controlled HTML and validate uploads by filename, MIME type, content signature, and size.
- Keep receipt access private to the reconciliation owner and authenticated administrators.
- A missing configured rate is an explicit data-quality condition; it must not crash or silently invent financial data.
- Notes and reimbursements are independent records with stable IDs. Deleting one record must not alter its siblings.
- Submitted monthly reconciliations are immutable until explicitly reopened by an administrator.
- Supply imports are transactional and batched; never commit inside the row loop and never leave a partial import.
- Excel exports must apply the selected filters to the complete result set, not only a visible page.
- Run compile, application smoke tests, route checks, migration tests, automated tests, forbidden-symbol searches, and a final diff review before claiming completion.
- Do not merge the pull request.
