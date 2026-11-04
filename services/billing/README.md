# Billing Service

Сервис биллинга подписок онлайн-кинотеатра (`services/billing`).

## Компоненты

- `billing-api` — HTTP-сервер (пользовательские экраны, публичное API, вебхуки ЮKassa, внутреннее API для Django-админки).
- `billing-worker` — фоновый процесс (сверка незавершённых операций с ЮKassa и доставка задач из `outbox_tasks` в `Auth` и `Notifications`).
- `billing-migrate` — применение миграций Alembic к `billing-db`.
