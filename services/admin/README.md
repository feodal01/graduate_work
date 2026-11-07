# Admin

Django Admin управляет каталогом фильмов в PostgreSQL `theatre-db`. Вход и административные права проверяет сервис Auth. Movies ETL читает таблицы `content.*` и обновляет Elasticsearch.

## Запуск в едином проекте

Выполните команды из корня репозитория:

```bash
cp .env.example .env
docker compose up -d --build
```

Корневой Compose запускает миграции и сборку статических файлов параллельно. Контейнер Admin запускается после успешного завершения обоих процессов. Контейнер `auth-bootstrap` создаёт начального суперпользователя в Auth.

| Ресурс | Внешний URL |
| --- | --- |
| Django Admin | `http://localhost/admin/` |
| Роли Auth | `http://localhost/admin/auth/roles/` |
| Подписки пользователей | `http://localhost/admin/billing/subscriptions/` |
| Платежи и проблемные операции | `http://localhost/admin/billing/payments/` |
| Зависшие операции | `http://localhost/admin/billing/payments/?tab=stuck` |
| Не доставлено в Auth или Notifications | `http://localhost/admin/billing/payments/?tab=undelivered` |
| Карточка платежа | `http://localhost/admin/billing/payments/<payment_id>/` |
| Подтверждение и оформление возврата | `http://localhost/admin/billing/payments/<payment_id>/refund/` |
| Ручной повтор и закрытие задачи outbox | `http://localhost/admin/billing/outbox-tasks/<task_id>/retry/`, `/close/` |
| Принудительная сверка с ЮKassa | `http://localhost/admin/billing/operations/<type>/<id>/reconcile/` |
| Журнал действий администраторов | `http://localhost/admin/billing/admin-actions/` |
| Health | `http://localhost/admin/health` |
| Пример статического файла | `http://localhost/static/admin/css/base.css` |

Данные для входа:

- логин: `admin`;
- пароль: `Cinema-Admin-2026!`.

Логин и пароль хранятся в Auth. Значения задают корневые переменные `AUTH_ADMIN_LOGIN` и `AUTH_ADMIN_PASSWORD`. Начальный каталог загружается из `services/admin/database_dump.sql` при первом создании `theatre-db`.

## Авторизация

Admin отправляет логин и пароль в `POST /api/v1/auth/login`. Затем Admin читает профиль и роли пользователя через Auth API. Доступ разрешён только пользователю с административным правом.

При первом успешном входе Admin создаёт в `theatre-db` стандартного Django `User` с непригодным паролем и запись `AuthIdentity`. `AuthIdentity` связывает локальный primary key с UUID пользователя Auth. Локальная запись нужна для Django-сессии, `request.user` и внешнего ключа `django_admin_log.user_id`. Повторный вход обновляет эту запись и не создаёт дубликат.

Пароль, роли и решение о доступе остаются в Auth. Страница ролей вызывает Auth API и не записывает роли в `theatre-db`. Если Auth недоступен, вход и защищённые страницы возвращают HTTP 503. Если Auth возвращает HTTP `429`, Admin показывает отдельную страницу HTTP `429` и передаёт корректный `Retry-After`. Выход всегда удаляет локальную Django-сессию, в том числе после HTTP `429` или отказа Auth.

Admin повторяет ошибку установления соединения для любого запроса. Admin также повторяет безопасный `GET` после timeout чтения, сетевой ошибки чтения, ошибки удалённого протокола и HTTP `502`, `503`, `504`. Admin не повторяет HTTP `429`. Admin не повторяет `POST`, `PATCH` или `DELETE` после ошибки ответа, потому что Auth мог уже выполнить операцию. Значения `AUTH_CLIENT_BACKOFF_MAX_TRIES` и `AUTH_CLIENT_BACKOFF_MAX_TIME_SECONDS` ограничивают число попыток и общее время ожидания.

Admin передаёт `X-Request-Id` во все запросы к Auth. Запрос через gateway сохраняет идентификатор Nginx во всей цепочке. При отдельном запуске Admin создаёт один UUID и повторно использует его во всех вызовах Auth в рамках текущего HTTP-запроса.

## Локальный запуск

Для отдельного запуска нужны Python 3.10, PostgreSQL и запущенный Auth по адресу из `AUTH_SERVICE_URL`. Выполните команды из `services/admin`:

```bash
cp .env.example .env
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app/manage.py migrate
python app/manage.py runserver 127.0.0.1:8001
```

Django получает конфигурацию через Pydantic Settings из `services/admin/.env`. Переменные окружения имеют приоритет над файлом; чтение файла не изменяет окружение процесса. Шаблон направляет запросы в Auth по адресу `http://127.0.0.1:8000/api/v1/auth`. Локальный адрес Admin: `http://127.0.0.1:8001/admin/`.

Имена переменных и значения по умолчанию сохранены. `SQL_OPTIONS` необязателен. `SQL_PORT` должен быть целым числом от 1 до 65535, таймауты и общее время повторов — конечными положительными числами, `AUTH_CLIENT_BACKOFF_MAX_TRIES` — целым числом от 1. Пустые значения ключа Django, заданного пароля БД, имени БД, пользователя, хоста и адреса Auth отклоняются. `SECRET_KEY` и `POSTGRES_PASSWORD` хранятся в модели как `SecretStr` и преобразуются в строки при передаче Django; ошибки валидации скрывают входные значения. Подключение раздела биллинга настраивается переменными `BILLING_INTERNAL_URL`, `AUTH_INTERNAL_URL`, `SERVICE_API_KEY`, `BILLING_CLIENT_CONNECT_TIMEOUT_SECONDS` и `BILLING_CLIENT_TIMEOUT_SECONDS`.

## Проверка

Выполните команды из корня репозитория:

```bash
docker compose run --rm admin python manage.py check
docker compose run --rm admin python manage.py test example.test_config example.test_health users.tests billing.tests
```

Маршрут `/health/` внутри контейнера проверяет соединение с `theatre-db`. Gateway публикует этот маршрут как `/admin/health`.

## Раздел «Биллинг и подписки»

Приложение `services/admin/app/billing` не создаёт собственных таблиц в `theatre-db`: все данные подписок, платежей, возвратов, задач доставки и журнала действий читаются и изменяются по HTTP через внутренний API сервиса `Billing` (`BIL-API-06`–`BIL-API-14`), а логины и часовые пояса пользователей запрашиваются пакетом через внутренний API `Auth` (`POST /api/v1/internal/users/lookup`).

- Все списки и карточка платежа открыты только для чтения; даты выводятся в двух колонках (`UTC` и часовой пояс пользователя).
- При временной недоступности `Auth` (`ADM-NFR-R-01`) уже открытая сессия администратора продолжает работать со страницами `/admin/billing/*`: вместо логина отображается UUID пользователя, время выводится в `UTC` (с пометкой о недоступности часового пояса), при поиске по логину выводится подсказка искать по UUID, а кнопки «Вернуть», «Проверить в ЮKassa», «Повторить» и «Закрыть с причиной» остаются полностью рабочими.
- При временной недоступности, тайм-ауте или ответе `5xx` от `Billing` (`ADM-NFR-R-02`) админка возвращает штатную страницу `503` без необработанного исключения; повторная отправка формы возврата или нажатие кнопок управления после восстановления безопасны.
