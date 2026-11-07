# Billing

Сервис биллинга подписок онлайн-кинотеатра (`services/billing`). Обеспечивает покупку подписки «Премиум» (30 дней, 299 ₽) через ЮKassa API v3, открытие и автоматическое закрытие доступа к новинкам в `Auth`, отправку и отмену транзакционных писем в `Notifications`, а также операционный контур поддержки и возвратов для `Django Admin`.

Техническое задание, архитектурные решения и план работ: [`docs/billing/tz.md`](../../docs/billing/tz.md), [`docs/billing/plan.md`](../../docs/billing/plan.md).

## Компоненты

- `billing-api` — HTTP-сервер FastAPI:
  - пользовательские HTML-экраны (`Э1` вход `/billing/login`, `Э2` «Моя подписка» `/billing/`, `Э3` «Результат оплаты» `/billing/payments/{id}`, `Э4` экран ошибки);
  - публичное JSON API покупки и статуса подписки (`POST /api/v1/billing/payments`, `GET /api/v1/billing/me/summary`) и приём вебхуков ЮKassa (`POST /api/v1/billing/webhooks/yookassa`);
  - внутреннее API для `Django Admin` (`/api/v1/internal/billing/*`, защищено заголовком `X-Service-Api-Key`).
- `billing-worker` — фоновый процесс с корректным завершением по `SIGTERM`/`SIGINT`:
  - активная сверка незавершённых платежей (`pending`) и возвратов (`processing`) через `GET` в ЮKassa (`SELECT ... FOR UPDATE SKIP LOCKED`);
  - доставка задач из транзакционного outbox (`outbox_tasks`) в `Auth` и `Notifications` с экспоненциальным backoff и переводом в `requires_intervention` при постоянных ошибках;
  - мониторинг зависших операций и проблемных задач доставки (`OperationalMonitor`, сигналы в `GlitchTip` и `ERROR`-лог).
- `billing-migrate` — одноразовый контейнер применения миграций Alembic к `billing-db` (`PostgreSQL 16`, `synchronous_commit = on`).

## Запуск в едином проекте (`GW-FR-02`)

Из корня репозитория:

```bash
cp .env.example .env
# Укажите тестовые ключи магазина ЮKassa в .env:
# YOOKASSA_SHOP_ID=<shop_id>
# YOOKASSA_SECRET_KEY=<test_...>
docker compose up -d --build
docker compose ps
```

Команда поднимает `billing-db`, выполняет миграции `billing-migrate` до старта `billing-api` и `billing-worker`, а также автоматически запускает `notifications-campaign-api` для приёма и отмены писем биллинга.

Внешние маршруты через шлюз `nginx` (и внутренний маршрут проверки здоровья контейнера):

| Маршрут | Назначение |
| --- | --- |
| `GET /billing/` | Экран пользователя «Моя подписка» (`Э2`, `BIL-API-02`) |
| `GET /billing/login`, `POST /billing/login`, `POST /billing/logout` | Вход и выход из биллинга (`Э1`, `BIL-API-01`) |
| `POST /billing/subscribe` | Старт или продолжение покупки подписки из браузера с редиректом на ЮKassa (`BIL-API-03`) |
| `GET /billing/payments/{payment_id}` | Экран «Результат оплаты» (`Э3`, читает только локальную БД, `BIL-API-04`) |
| `GET /billing/payments/{payment_id}/continue` | Продолжение незавершённой оплаты с проверкой статуса в ЮKassa не чаще 1 раза в 5 с (`BIL-API-04`) |
| `POST /api/v1/billing/payments` | Создание или возврат существующего `pending`-платежа в JSON (`BIL-API-03`) |
| `GET /api/v1/billing/me/summary` | JSON-сводка статуса подписки (по 7 правилам `US-U-05`) и истории платежей (`BIL-API-02`) |
| `POST /api/v1/billing/webhooks/yookassa` | Приём уведомлений ЮKassa (из тела берётся только `object.id`, `BIL-API-05`) |
| `GET /billing/health` (внутри контейнера `GET /api/v1/health/`) | Проверка здоровья `billing-api` и `billing-db` (`GW-API-01`, `BIL-API-15`) |
| `GET /billing/docs`, `GET /billing/openapi.json` | Swagger UI и OpenAPI JSON сервиса Billing |

Внутренние маршруты `/api/v1/internal/billing/*` закрыты на внешнем шлюзе `nginx` (`404`) и доступны только внутри сети Docker Compose по сервисному ключу `X-Service-Api-Key`:
- `GET /api/v1/internal/billing/subscriptions` — список подписок с фильтрацией (`BIL-API-06`);
- `GET /api/v1/internal/billing/payments` — список платежей и фильтр «Зависшие операции» (`?stuck=true`, `BIL-API-07`);
- `GET /api/v1/internal/billing/payments/{payment_id}` — карточка платежа с возвратами, историей переходов `operation_events` и задачами `outbox_tasks` (`BIL-API-08`);
- `POST /api/v1/internal/billing/payments/{payment_id}/refunds` — оформление полного возврата с обязательной причиной (`BIL-API-09`);
- `POST /api/v1/internal/billing/operations/{operation_type}/{operation_id}/reconcile` — принудительная сверка с ЮKassa без 5-секундного лимита (`BIL-API-10`);
- `GET /api/v1/internal/billing/outbox-tasks` — проблемные задачи доставки (`requires_intervention` и `pending` старше порога, `BIL-API-11`);
- `POST /api/v1/internal/billing/outbox-tasks/{task_id}/retry` и `/close` — ручной повтор и закрытие задачи с причиной (`BIL-API-12`, `BIL-API-13`);
- `GET /api/v1/internal/billing/admin-actions` — неизменяемый журнал действий администраторов (`BIL-API-14`);
- `GET /api/v1/internal/billing/metrics` — операционные метрики биллинга (`6.4`).

## Локальный (отдельный) запуск

Для отдельного запуска сервиса вне Docker Compose требуются Python 3.12+ и PostgreSQL 16. Из каталога `services/billing`:

```bash
cp .env.example .env
uv sync --locked --all-groups
uv run alembic upgrade head
uv run uvicorn main:app --app-dir src --host 127.0.0.1 --port 8000
```

Запуск фонового процесса `billing-worker` в отдельном терминале:

```bash
cd services/billing
PYTHONPATH=src uv run python -m worker
```

## Локальная эмуляция вебхука ЮKassa

При локальной разработке без публичного домена воркер `billing-worker` автоматически сверяет платежи через `GET` с отступом `RECONCILE_MIN_AGE_SECONDS` (по умолчанию `5` с в `.env.example`). Для мгновенной подачи сигнала вебхука по реальному `provider_payment_id` предусмотрена CLI-утилита:

```bash
docker compose exec billing-api python -m emulate_webhook --id <provider_payment_id> --event payment.succeeded --base-url http://127.0.0.1:8000
```

## Проверка и автотесты

Модульные, контрактные и стаб-тесты матрицы деградации:

```bash
uv run --directory services/billing ruff check .
uv run --directory services/billing ruff format --check .
uv run --directory services/billing mypy src tests
uv run --directory services/billing pytest -m "not integration" -v
```

Интеграционные тесты на реальном PostgreSQL и песочнице ЮKassa:

```bash
docker compose -f tests/notifications/compose.yaml up -d --wait
uv run --directory tests/notifications python run_service_checks.py billing
docker compose -f tests/notifications/compose.yaml down -v
```

Полный сквозной прогон в живом Docker Compose (`T-06`–`T-37`):

```bash
./tests/integration/run_billing_integration.sh
```
