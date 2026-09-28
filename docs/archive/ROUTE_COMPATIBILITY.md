# Route compatibility against production `87fd984`

> Архивный технический отчёт. Описывает состояние на момент проверки,
> не подтверждает состояние текущего production и не является инструкцией выпуска.

No production route was removed. `GET /toggle-day`, `GET /toggle-inventory`,
and `GET /reopen-monthly-submission` remain as non-mutating compatibility
redirects; the same paths now also accept protected POST forms.

| Production route | Result | Replacement / reason | Compatible redirect | Test |
|---|---|---|---|---|
| `GET /` | preserved | — | n/a | route inventory |
| `GET /db-check` | preserved | — | n/a | route inventory |
| `GET /receipts/{file_id}/{filename}` | secured | owner/admin authorization added | n/a | validation/security tests |
| `GET /active-period` | preserved | — | n/a | route inventory |
| `GET /debug/merchants-columns` | preserved | — | n/a | route inventory |
| `POST /login` | preserved | — | n/a | authentication rules |
| `GET, POST /login-page` | preserved | signed merchant session added | n/a | login tests |
| `GET /merchant-logout` | new | clears the signed merchant cookie | n/a | logout test |
| `GET /menu-page` | preserved | session-bound FIO | n/a | identity test |
| `GET, POST /point-page` | preserved | session-bound FIO | n/a | route inventory |
| `GET /calendar-page` | preserved | explicit slots and DB calendar | n/a | calendar/slot tests |
| `GET /point-note-page` | preserved | — | n/a | route inventory |
| `POST /save-point-note-normal` | preserved | — | n/a | legacy migration tests |
| `POST /save-point-note-no-supply` | preserved | — | n/a | route inventory |
| `GET /point-reimbursement-page` | preserved | — | n/a | route inventory |
| `POST /save-point-reimbursement` | preserved | durable validated receipts | n/a | receipt tests |
| `POST /save-point-adjustment` | preserved | durable validated receipts | n/a | route inventory |
| `POST /delete-point-note` | preserved | — | n/a | migration/item tests |
| `POST /delete-point-reimbursement` | preserved | — | n/a | migration/item tests |
| `GET /monthly-submit-page` | preserved | CSRF forms added | n/a | route inventory |
| `POST /submit-monthly-submission` | preserved | CSRF required | n/a | route inventory |
| `GET /reopen-monthly-submission` | replaced safely | no longer mutates | redirects to submit page | route inventory |
| `POST /reopen-monthly-submission` | new | protected replacement | n/a | route inventory |
| `GET /day-action-page` | preserved | slot selector added | n/a | slot tests |
| `GET /toggle-day` | replaced safely | GET must not mutate | redirects to day action | compatibility test |
| `POST /toggle-day` | new | CSRF + explicit slot | n/a | CSRF/slot tests |
| `GET /toggle-inventory` | replaced safely | GET must not mutate | redirects to day action | route inventory |
| `POST /toggle-inventory` | new | CSRF protected | n/a | route inventory |
| `GET /summary-page` | preserved | — | n/a | route inventory |
| `GET, POST /admin-login` | preserved | Secure cookie in production | n/a | admin login test |
| `GET /admin-logout` | preserved | — | n/a | route inventory |
| `GET /admin-report` | preserved | slot-aware intersections | n/a | slot tests |
| `GET /admin-data` | preserved | calendar upload UI added | n/a | route inventory |
| `POST /admin-upload-supplies` | preserved | rollback on error | required | 900-point import test |
| `POST /admin-upload-rates` | preserved | validate-all / one transaction | required | rate import tests |
| `POST /admin-upload-merchants` | preserved | validate-all / one transaction | n/a | merchant import tests |
| `POST /admin-add-merchant` | preserved | — | n/a | import/auth tests |
| `POST /admin-clear-month` | preserved | rollback and friendly error | required | route inventory |
| `POST /admin-clear-merchants` | preserved | — | n/a | route inventory |
| `POST /admin-add-special-inventory-day` | preserved | independent from calendar | required | route inventory |
| `POST /admin-delete-special-inventory-day` | preserved | independent from calendar | required | route inventory |
| `POST /admin-upload-production-calendar` | new | emergency manual calendar updates | required | calendar import test |
| `POST /admin-sync-production-calendar` | new | background approved official sync | required | calendar route tests |
| `POST /admin-calendar-override` | new | manual date override | required | calendar sync tests |
| `POST /admin-calendar-reset` | new | restore official date value | required | calendar sync tests |
| `GET /admin-export-check` | preserved | complete filtered dataset | n/a | export inventory |
| `GET /admin-export-payroll` | preserved | complete filtered dataset | n/a | export inventory |
| `GET /admin-export-overlaps` | preserved | MORNING/EVENING only, no A/B duplicates | n/a | intersection tests |
