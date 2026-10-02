# Merch Web: эксплуатация на FirstVDS

Production: FirstVDS, Docker Compose, PostgreSQL и nginx;
пользовательский адрес — [https://sverka-mm.ru](https://sverka-mm.ru).
Команды ниже выполняются оператором в POSIX shell на VPS при согласованном выпуске.

## Конфигурация площадки

| Объект | Путь / имя |
|---|---|
| Checkout | `/opt/salary-prod/app` |
| Закрытый env-файл | `/opt/salary-prod/config/production.env`, права `0600` |
| Резервные копии | `/var/backups/salary-prod`, каталог `0750`, дампы `0600` |
| Compose project | `salary-prod` |
| Сервисы | `app`, `postgres`, `nginx` |
| Основной volume PostgreSQL | `salary_pgdata` |
| Изолированное восстановление | `postgres-restore`, volume `salary_pgdata_restore` |

Это пути штатных скриптов. Если площадка использует другие пути, передайте
`PROJECT_DIR`, `SALARY_ENV_FILE`, `BACKUP_DIR` явно. Секреты получают из корпоративного
хранилища; не выводите env-файл и полный результат `docker compose config` в журналы.
Значения существующих секретов сохраняются при обычном deploy.

В Git находятся HTTP-шаблон nginx и пример TLS-конфигурации; штатный Compose сам
не подключает сертификаты и порт 443. Перед первым выпуском принимающая команда
должна сверить фактический TLS-контур и действующие Compose overrides с площадкой.
Если используется `COMPOSE_FILE`, сохраните его действующее значение (абсолютные
пути) в shell и окружении backup-задачи. Все команды должны адресовать один и тот
же Compose project. При отсутствии конфигурации площадки выпуск не выполняется.
Не заменяйте действующий nginx примером из Git при обновлении приложения.

## Подготовка и выбор TARGET_SHA

Предпосылки: Docker Compose v2 с `--wait`, чистый checkout, проверенный PR,
полный SHA согласованного коммита, доступный предыдущий образ и успешная проверка
восстановления backup. Сохраните предыдущий SHA в записи выпуска.

```sh
set -eu
cd /opt/salary-prod/app
export PROJECT_DIR="$PWD"
export SALARY_ENV_FILE=/opt/salary-prod/config/production.env
test -r "$SALARY_ENV_FILE"
test -z "$(git status --porcelain)"

PREVIOUS_IMAGE_ID=$(docker inspect salary-app --format '{{.Image}}')
PREVIOUS_SHA=$(docker image inspect "$PREVIOUS_IMAGE_ID" \
  --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}')
test "${#PREVIOUS_SHA}" -eq 40
test "$(docker image inspect "salary-app:$PREVIOUS_SHA" --format '{{.Id}}')" = "$PREVIOUS_IMAGE_ID"
export APP_GIT_SHA="$PREVIOUS_SHA"
docker compose --env-file "$SALARY_ENV_FILE" ps

git fetch --all --tags --prune
printf 'TARGET_SHA (full approved commit): '
read -r TARGET_SHA
test "${#TARGET_SHA}" -eq 40
test "$(git rev-parse --verify "$TARGET_SHA^{commit}")" = "$TARGET_SHA"
git merge-base --is-ancestor "$TARGET_SHA" origin/main
git diff --exit-code "$PREVIOUS_SHA" "$TARGET_SHA" -- compose.yaml deploy/nginx
```

Если проверка инфраструктурного diff не проходит, требуется отдельный план
изменения инфраструктуры. Обычный app-only deploy эту разницу не применяет.
Образ предыдущего релиза не удаляйте сборщиком мусора до завершения окна отката.

## Backup перед выпуском

До смены checkout, с экспортированными выше переменными:

```sh
deploy/scripts/backup-postgres.sh
```

Скрипт атомарно создаёт custom-format dump и файл SHA-256 с закрытыми правами.
По умолчанию он удаляет свои копии старше 14 дней; `BACKUP_RETENTION_DAYS`
задаёт согласованный срок. Сохраните путь к новому дампу в записи выпуска.
Проверка восстановления описана ниже и не затрагивает production-БД.

## Сборка и deploy только приложения

```sh
git switch --detach "$TARGET_SHA"
test "$(git rev-parse HEAD)" = "$TARGET_SHA"
test -z "$(git status --porcelain)"
export APP_GIT_SHA="$TARGET_SHA"
docker compose --env-file "$SALARY_ENV_FILE" build --pull app
test "$(docker image inspect "salary-app:$APP_GIT_SHA" \
  --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}')" = "$TARGET_SHA"

POSTGRES_ID=$(docker compose --env-file "$SALARY_ENV_FILE" ps -q postgres)
NGINX_ID=$(docker compose --env-file "$SALARY_ENV_FILE" ps -q nginx)
test -n "$POSTGRES_ID"
test -n "$NGINX_ID"
docker compose --env-file "$SALARY_ENV_FILE" up -d --no-deps --no-build --wait --wait-timeout 120 app
test "$(docker compose --env-file "$SALARY_ENV_FILE" ps -q postgres)" = "$POSTGRES_ID"
test "$(docker compose --env-file "$SALARY_ENV_FILE" ps -q nginx)" = "$NGINX_ID"
```

`--no-deps` сохраняет работающие PostgreSQL и nginx. Не используйте `down`,
`up` без списка сервисов или удаление volumes для обычного выпуска.
Обычный старт новой версии не выполняет schema setup, merchant backfill или
фоновую синхронизацию календаря, независимо от legacy-переменной
`AUTO_SYNC_PRODUCTION_CALENDAR`. Схема проверяется read-only до выпуска.
Если дополнения действительно нужны, они выполняются отдельно, только по
явному разрешению: `python -m app.schema_setup --apply` в окружении проверенной
целевой БД. Команда сохраняет прежние schema helpers, включая merchant metadata
backfill и замену старых уникальных ограничений; это не read-only проверка.
Helpers сохраняют прежние промежуточные commit: не считать весь setup одной
атомарной миграцией. При ошибке проверьте состояние до повторного запуска.
`legacy_migration --apply` эта команда не запускает.

Синхронизация календаря остаётся отдельным явным действием администратора либо
CLI `python -m app.production_calendar --year YEAR`; её нельзя выполнять в рамках
выкладки с запретом изменения БД. Периодическую синхронизацию, если она нужна,
согласуйте отдельно; остановка автоматической записи при startup её не заменяет.
Ленивые schema helpers в некоторых обычных маршрутах этим исправлением не
перерабатываются: при требовании строгого read-only smoke проверяйте его маршруты.

На read-only preflight 02.10.2026 действующий wrapper
`/opt/salary-prod/bin/salary_app_release.py` переписывал закрытый env-файл при
переключении SHA. Не запускайте его при запрете изменения `.env` без отдельного
согласования способа deployment. Эта проверка не изменяла серверные скрипты.

## Проверка фактического SHA и healthcheck

```sh
RUNNING_IMAGE_ID=$(docker inspect salary-app --format '{{.Image}}')
test "$RUNNING_IMAGE_ID" = "$(docker image inspect "salary-app:$TARGET_SHA" --format '{{.Id}}')"
test "$(docker image inspect "$RUNNING_IMAGE_ID" \
  --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}')" = "$TARGET_SHA"
test "$(docker compose --env-file "$SALARY_ENV_FILE" exec -T app \
  python -c 'import os; print(os.environ["APP_GIT_SHA"])')" = "$TARGET_SHA"
test "$(docker inspect salary-app --format '{{.State.Health.Status}}')" = healthy
docker compose --env-file "$SALARY_ENV_FILE" ps
BASE_URL=https://sverka-mm.ru deploy/scripts/verify-stack.sh
curl --fail --silent --show-error --max-time 10 https://sverka-mm.ru/healthz
```

`verify-stack.sh` ожидает HTTP 200 от `/db-check`, `/login-page`, `/admin-login`.
`/db-check` проверяет запрос к БД; это не полная проверка бизнес-сценариев.
В журнале выпуска сохраните SHA, image ID, результат healthcheck и ссылку на PR.
После успешной проверки обновите только `APP_GIT_SHA` в закрытом env-файле через
принятый на площадке способ управления конфигурацией. Иначе новая shell-сессия
или cron могут использовать устаревший тег. Секретные значения не меняются.

## App-only rollback

При неуспешном healthcheck используйте предыдущий проверенный образ. Этот вариант
применим при неизменных Compose/nginx и совместимой схеме БД:

```sh
ROLLBACK_SHA="$PREVIOUS_SHA"
test "${#ROLLBACK_SHA}" -eq 40
test "$(docker image inspect "salary-app:$ROLLBACK_SHA" \
  --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}')" = "$ROLLBACK_SHA"
export APP_GIT_SHA="$ROLLBACK_SHA"
docker compose --env-file "$SALARY_ENV_FILE" up -d --no-deps --no-build --pull never --wait --wait-timeout 120 app
TARGET_SHA="$ROLLBACK_SHA"
```

Повторите проверку фактического SHA и healthcheck из предыдущего раздела, а также
проверку неизменности контейнеров PostgreSQL и nginx. Зафиксируйте возвращённый SHA
в закрытом env-файле и журнале. Checkout можно вернуть на этот SHA после проверки
его чистоты; rollback выше переключает образ без сборки.
При неизвестной совместимости схемы сначала проверьте восстановление в изоляции.
Откат приложения не откатывает данные и не выполняет обратные миграции.

## Backup и проверка восстановления

Запускайте команды из checkout с заданными `SALARY_ENV_FILE`, `APP_GIT_SHA`
и конфигурацией Compose площадки. Подставьте путь из вывода backup-скрипта:

```sh
printf 'Absolute backup path: '
read -r BACKUP_FILE
test -r "$BACKUP_FILE"
sha256sum --check "$BACKUP_FILE.sha256"
deploy/scripts/restore-test.sh "$BACKUP_FILE"
docker compose --env-file "$SALARY_ENV_FILE" --profile restore stop postgres-restore
docker compose --env-file "$SALARY_ENV_FILE" --profile restore rm -f postgres-restore
```

Восстановление перезаписывает только тестовую БД `postgres-restore`. Скрипт
проверяет загрузку дампа и число таблиц; дополнительно сверяйте контрольные
показатели и доступность чеков на изолированном стенде. Его volume сохраняется.
Не применяйте `docker compose --profile restore down`: команда может остановить
весь проект, включая основные сервисы.

Расписание backup, перенос копий вне VPS, срок хранения, RPO/RTO и уведомления
об ошибках закрепляются за командой эксплуатации. Штатный скрипт сам расписание
не устанавливает и внешнюю копию не создаёт. Cron должен получать тот же env-файл
и Compose-конфигурацию, что и ручной запуск. Пример ежедневного задания:

```cron
15 2 * * * /opt/salary-prod/app/deploy/scripts/backup-postgres.sh >>/var/backups/salary-prod/backup.log 2>&1
```

Время задания определяется timezone cron на VPS; проверьте её перед установкой.
Для восстановления production-БД требуется отдельный согласованный план:
остановка записи, сохранение текущего состояния, проверенный дамп, изолированная
репетиция, контроль целостности и разрешение владельца данных. Это не шаг
обычного deploy или app-only rollback.

## Диагностика и обслуживание

```sh
docker compose --env-file "$SALARY_ENV_FILE" ps
docker compose --env-file "$SALARY_ENV_FILE" logs --since 30m app nginx postgres
docker image ls salary-app
```

Просматривайте журналы только в доверенной среде; не переносите персональные данные
и секреты в Git или публичные отчёты.

`MAINTENANCE_MODE=1` в env-файле и пересоздание только `app` блокируют POST/PUT/PATCH/
DELETE с HTTP 503, startup-записи и синхронизацию календаря. GET и `/db-check`
остаются доступны. Вход через POST также недоступен: режим не равен обычной
работе сервиса. Для возврата установите `MAINTENANCE_MODE=0` и обновите только `app`
тем же `up -d --no-deps --no-build` с проверенным `APP_GIT_SHA`.
