# Merch Web

Внутренний сервис сверки работы мерчендайзеров микромаркетов.

## Назначение

- Отметка выходов и учёт поставок.
- Полный инвент и учёт дней работы с кофемашиной.
- Примечания, возмещения и прикрепление чеков.
- Месячная сверка и фиксация отправленных результатов.
- Административные отчёты и Excel-выгрузки.

## Архитектура

Приложение на FastAPI использует SQLAlchemy и PostgreSQL. HTML-страницы
формируются приложением; файлы чеков хранятся в PostgreSQL.
Production размещён на FirstVDS: Docker Compose управляет приложением,
PostgreSQL и nginx, внешний адрес — [https://sverka-mm.ru](https://sverka-mm.ru).
Доступ пользователей осуществляется по HTTPS.

Поставляемый Compose содержит HTTP-конфигурацию nginx; параметры действующего
TLS-контура и сертификаты передаются командой эксплуатации отдельно.
Порядок проверки конфигурации описан в [VPS runbook](docs/VPS_RUNBOOK.md).

## Структура проекта

| Путь | Назначение |
|---|---|
| `app/main.py` | FastAPI, обработчики запросов и HTML-страницы |
| `app/services.py`, `app/coffee_days.py` | Прикладные расчёты и операции |
| `app/db.py`, `app/schemas.py` | Подключение к БД и модели входных данных |
| `app/security.py`, `app/runtime.py` | Сессии и режим обслуживания |
| `app/merchant_admin.py` | Управление справочником мерчендайзеров |
| `app/production_calendar.py` | Производственный календарь |
| `app/legacy_migration.py`, `app/legacy_snapshot.py` | Контролируемый перенос старых данных и обезличенный снимок |
| `app/static/` | Статические ресурсы |
| `tests/` | Автоматизированные проверки |
| `deploy/`, `compose.yaml`, `Dockerfile` | Конфигурация контейнеров и эксплуатационные скрипты |
| `docs/` | Эксплуатационная документация |
| `requirements.txt` | Зависимости приложения |

## Локальный запуск

Требуются Python 3.12 и отдельная PostgreSQL с подготовленной схемой и тестовыми
данными. Полного bootstrap базовых таблиц в репозитории нет: пустая БД не является
готовым окружением. Получите у команды поддерживаемый обезличенный набор данных
и порядок его восстановления. Production БД для локального запуска не используется.

Из корня проекта создайте окружение:

```sh
python -m venv .venv
```

Активируйте его: `source .venv/bin/activate` в POSIX shell или
`.\.venv\Scripts\Activate.ps1` в PowerShell. Затем:

```sh
python -m pip install -r requirements.txt
```

Задайте обязательные переменные из следующего раздела в окружении процесса,
используя отдельные локальные секреты. Для локального HTTP установите
`ENVIRONMENT=development`, `AUTO_SYNC_PRODUCTION_CALENDAR=0` и `MAINTENANCE_MODE=0`.
Приложение само не загружает `.env`.

```sh
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload
```

Откройте `http://127.0.0.1:8000/login-page`; проверка соединения с БД — `/db-check`.
При старте приложение выполняет предусмотренные кодом дополнения схемы;
учётная запись локальной БД должна иметь необходимые права.

## Environment variables

Значения секретов хранятся вне Git. Имена и назначение переменных:

| Переменная | Назначение |
|---|---|
| `DATABASE_URL` | Обязательная строка подключения к PostgreSQL |
| `ADMIN_LOGIN`, `ADMIN_PASSWORD` | Обязательные учётные данные администратора |
| `SECRET_SALT` | Обязательный секрет для существующих хешей и административной сессии |
| `SESSION_SECRET` | Отдельный секрет подписи сессий, не короче 24 символов; обязателен в Compose. Код допускает fallback на `SECRET_SALT` |
| `ENVIRONMENT` | Режим secure-cookie: `production` для HTTPS, `development` для локального HTTP, `test` для тестов |
| `AUTO_SYNC_PRODUCTION_CALENDAR` | Автосинхронизация календаря при старте; Compose фиксирует её выключенной |
| `MAINTENANCE_MODE` | Режим обслуживания с блокировкой запросов изменения данных |
| `MAX_RECEIPT_BYTES` | Лимит одного чека в байтах; default кода — 15 MiB, пример env задаёт 5 MiB |

Compose также использует обязательные `POSTGRES_DB`, `POSTGRES_USER`,
`POSTGRES_PASSWORD`, `APP_GIT_SHA` и необязательные `SERVER_NAME`, `HTTP_BIND_IP`,
`HTTP_PORT`. В Compose `ENVIRONMENT` фиксирован как `production`.
Образ задаёт `PYTHONPATH`, `PYTHONDONTWRITEBYTECODE`, `PYTHONUNBUFFERED`.

Скрипты эксплуатации используют `SALARY_ENV_FILE`, `PROJECT_DIR`, `BACKUP_DIR`,
`BACKUP_RETENTION_DAYS`, `BASE_URL`; Docker Compose учитывает `COMPOSE_FILE`, если
он задан для конфигурации площадки. Переменные shell `TARGET_SHA`, `PREVIOUS_SHA`
и `ROLLBACK_SHA` используются в runbook для выбора релиза.

Формат конфигурации: [production.env.example](deploy/env/production.env.example).
Передача поддержки не требует смены действующих секретов: их замена проводится
отдельно с учётом влияния на хеши и сессии.

## Тесты

В активированном изолированном окружении:

```sh
python -m pip install pytest httpx
python -m pytest -q
python -m compileall -q app tests
python -m pip check
```

Запускайте тесты без production-переменных окружения: тесты задают SQLite in-memory
и синтетические секреты через `setdefault`. Набор содержит unit- и HTTP-проверки;
он не заменяет проверку интеграции с PostgreSQL. Поддерживается также
`python -m unittest discover -s tests -v`.

## Production deployment

Порядок выпуска по `TARGET_SHA`, проверка фактического SHA и healthcheck:
[docs/VPS_RUNBOOK.md](docs/VPS_RUNBOOK.md). Обычный deploy обновляет только `app`.

## Backup / rollback

Дамп PostgreSQL создаётся штатным скриптом, проверяется по SHA-256 и восстановлением
в отдельный сервис. Откат приложения использует ранее проверенный образ;
восстановление production-данных является отдельной операцией.
Команды и ограничения приведены в [VPS runbook](docs/VPS_RUNBOOK.md).

## Документация

- [Передача поддержки](docs/HANDOFF_STATUS.md).
- [Эксплуатация, deploy, backup и rollback](docs/VPS_RUNBOOK.md).
- [Обезличенный снимок и проверка переноса старых данных](docs/LEGACY_SNAPSHOT.md).
- [Обслуживание производственного календаря](docs/PRODUCTION_CALENDAR.md).

Основное название проекта — **Merch Web**. Имена существующих Compose-сервисов,
контейнеров и volumes сохранены для совместимости с площадкой.
