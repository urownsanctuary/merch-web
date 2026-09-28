# Merch Web: обезличенный снимок старых данных

Снимок используется для проверки `app.legacy_migration`, а не для полного
восстановления приложения. Экспорт читает исходную БД и записывает файл; запуск
на production выполняется только по согласованной задаче эксплуатации.

На FirstVDS из checkout и с действующей конфигурацией площадки, описанной в
[VPS runbook](VPS_RUNBOOK.md):

```sh
export SALARY_ENV_FILE=/opt/salary-prod/config/production.env
docker compose --env-file "$SALARY_ENV_FILE" exec -T app \
  python -m app.legacy_snapshot --output /tmp/merch-web-legacy-snapshot.sql
```

Контейнер использует своё настроенное подключение. Не печатайте и не копируйте
его секреты в команды или журналы.

## Состав и обезличивание

The PostgreSQL source transaction is `REPEATABLE READ, READ ONLY`. Source SQL is
limited to `SHOW` and `SELECT`:

- all migration fields from `point_adjustments`;
- the migration fields and `legacy_key` values from `point_notes`,
  `point_reimbursements`, and `reimbursement_receipts`, when those tables exist;
- `receipt_files.file_id`, `content_type`, `merchant_id`, `created_at`, and
  `octet_length(data)`. The `data` value itself and `original_filename` are not
  selected.
- relevant column names from `information_schema.columns`.

The exporter does not query `merchants`, so names, phone suffixes, password
hashes, email addresses, and TU values never enter the snapshot process.
Merchant IDs, point codes, row IDs, normalized IDs, receipt IDs, and legacy
keys are deterministically remapped. Free text is reduced to placeholders while
line breaks and migration delimiters are retained. Receipt paths are replaced
with test-only paths; only extension, content type, byte count, ownership link,
and timestamp metadata are retained.

The output report prints row counts, excluded fields, privacy scan counts, the
absolute path, and SHA-256. The output file and its atomic temporary file are
created with owner-only permissions. Snapshot patterns are ignored by Git.


## Получение файла

На VPS скопируйте файл из контейнера в закрытый каталог учётной записи оператора:

```sh
umask 077
install -d -m 700 ./legacy-snapshots
docker compose --env-file "$SALARY_ENV_FILE" cp \
  app:/tmp/merch-web-legacy-snapshot.sql ./legacy-snapshots/merch-web-legacy-snapshot.sql
chmod 600 ./legacy-snapshots/merch-web-legacy-snapshot.sql
sha256sum ./legacy-snapshots/merch-web-legacy-snapshot.sql
```

Передайте файл по утверждённому SSH/SFTP-каналу или через корпоративное хранилище
с ограниченным доступом. Сверьте SHA-256 с отчётом экспортера. Не публикуйте файл
через HTTP, не добавляйте его в Git и удалите временные копии по политике хранения.

## Проверка в изолированной PostgreSQL

Используйте пустую одноразовую БД. `TEST_DATABASE_URL` — переменная команды проверки,
содержащая libpq-совместимое подключение к этой БД, без префикса драйвера SQLAlchemy.
SQL отказывается загружаться без явного тестового флага:

```sh
psql -v LEGACY_SNAPSHOT_TEST_ONLY=on "$TEST_DATABASE_URL" \
  -f ./legacy-snapshots/merch-web-legacy-snapshot.sql
```

Затем настройте `DATABASE_URL` приложения на ту же тестовую БД и выполните:

```sh
python -m app.legacy_migration --dry-run
python -m app.legacy_migration --apply
python -m app.legacy_migration --apply
```

Проверьте отчёт неоднозначных записей, сохранность исходных строк и отсутствие
дубликатов при повторном применении. Для загрузки и проверки миграции production
подключение не используется. Эти команды не являются частью обычного deploy.
