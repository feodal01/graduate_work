# Структура единого проекта онлайн-кинотеатра

Репозиторий содержит семь независимых сервисов и общую инфраструктуру. Каждый сервис имеет собственный код, зависимости и Dockerfile. Корневой `compose.yaml` является единой точкой запуска проекта.

## Структура репозитория

```text
.
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── python-service.yml
├── compose.yaml
├── .env.example
├── README.md
├── docs/
│   ├── project-structure.md
│   ├── rate-limiting.design.md
│   ├── yandex-social-authentication.design.md
│   └── diagrams/
│       ├── admin/
│       ├── auth/
│       ├── movies-api/
│       ├── ugc-events/
│       └── ugc-etl/
├── infra/
│   ├── clickhouse/
│   │   ├── config.d/
│   │   │   └── cluster.xml
│   │   ├── init.sql
│   │   ├── node1.xml
│   │   ├── node2.xml
│   │   ├── node3.xml
│   │   └── node4.xml
│   ├── elasticsearch/
│   │   └── snapshots/
│   ├── filebeat/
│   │   └── filebeat.yml
│   ├── glitchtip/
│   │   └── bootstrap.py
│   ├── kibana/
│   │   └── bootstrap.js
│   ├── logrotate/
│   │   ├── Dockerfile
│   │   └── nginx.conf
│   ├── logstash/
│   │   └── logstash.conf
│   ├── mongo/
│   │   └── init-cluster.sh
│   └── nginx/
│       ├── nginx.conf
│       └── conf.d/
│           └── gateway.conf
├── research/
│   └── storage/
│       ├── compose.yaml
│       ├── bench/
│       ├── results/
│       └── README.md
└── services/
    ├── admin/
    │   ├── .env.example
    │   ├── app/
    │   │   ├── example/
    │   │   ├── movies/
    │   │   └── users/
    │   ├── database_dump.sql
    │   ├── Dockerfile
    │   ├── README.md
    │   └── requirements.txt
    ├── auth/
    │   ├── .env.example
    │   ├── docs/
    │   │   └── c4/
    │   ├── src/
    │   ├── Dockerfile
    │   ├── README.md
    │   ├── alembic.ini
    │   ├── pyproject.toml
    │   ├── uv.lock
    │   └── tests/
    ├── movies-api/
    │   ├── .env.example
    │   ├── src/
    │   ├── Dockerfile
    │   ├── README.md
    │   ├── pyproject.toml
    │   ├── uv.lock
    │   └── tests/
    ├── movies-etl/
    │   ├── .env.example
    │   ├── postgres_to_es/
    │   ├── Dockerfile
    │   ├── README.md
    │   ├── pyproject.toml
    │   ├── uv.lock
    │   └── tests/
    ├── ugc-events/
    │   ├── .env.example
    │   ├── src/
    │   ├── Dockerfile
    │   ├── README.md
    │   ├── pyproject.toml
    │   ├── uv.lock
    │   └── tests/
    ├── ugc-etl/
    │   ├── .env.example
    │   ├── src/
    │   ├── Dockerfile
    │   ├── README.md
    │   ├── pyproject.toml
    │   ├── uv.lock
    │   └── tests/
    └── ugc-api/
        ├── .env.example
        ├── src/
        ├── Dockerfile
        ├── README.md
        ├── pyproject.toml
        ├── uv.lock
        └── tests/
```

## Состав Docker Compose

| Compose-сервис | Источник образа | Постоянные данные | Назначение |
| --- | --- | --- | --- |
| `gateway` | официальный образ Nginx | `nginx_logs` | Единая внешняя точка входа; пишет access- и error-лог в `nginx_logs` для filebeat |
| `nginx-logrotate` | сборка из `infra/logrotate` | `nginx_logs` | Ротация `access-log.json` и `error-log.log` (`logrotate`, `copytruncate`, 50 МБ × 2) |
| `admin` | `services/admin/Dockerfile` | — | Django Admin |
| `admin-migrate` | `services/admin/Dockerfile` | — | Миграции Django |
| `admin-collectstatic` | `services/admin/Dockerfile` | `admin_static` | Сборка статики Django |
| `theatre-db` | `postgres:16` | `theatre_db_data` | База каталога фильмов и административной панели |
| `movies-etl` | `services/movies-etl/Dockerfile` | `movies_etl_state` | Перенос данных из PostgreSQL в Elasticsearch |
| `elasticsearch` | `elasticsearch:8.6.2` | `elasticsearch_data` | Поисковый индекс фильмов |
| `elasticsearch-init` | `elasticsearch:8.6.2` | — | Восстановление индекса `movies` и проверка mapping `creation_date` |
| `movies-api` | `services/movies-api/Dockerfile` | — | Публичный API каталога фильмов |
| `movies-redis` | `redis:7-alpine` | — | Кэш Movies API |
| `auth` | `services/auth/Dockerfile` | — | API аутентификации и авторизации, включая вход через Яндекс ID |
| `auth-migrate` | `services/auth/Dockerfile` | — | Миграции Auth через Alembic |
| `auth-bootstrap` | `services/auth/Dockerfile` | — | Создание начального суперпользователя Auth или повышение существующего пользователя |
| `auth-db` | `postgres:16` | `auth_db_data` | База пользователей, внешних учётных записей, ролей и истории входов |
| `auth-redis` | `redis:7-alpine` | `auth_redis_data` | Сессии, denylist, OAuth state, кэш разрешений и Token Bucket Auth |
| `jaeger` | `cr.jaegertracing.io/jaegertracing/jaeger:2.20.0` | — | Приём и просмотр трасс Auth |
| `elk-elasticsearch` | `elasticsearch:8.10.2` | `elk_elasticsearch_data` | Хранилище логов ELK, отдельное от поискового `elasticsearch` |
| `logstash` | `logstash:8.10.2` | — | Приём логов приложений (UDP `5044`) и filebeat (Beats `5045`), разбор error-лога Nginx (`grok`), запись в индексы `app-*` и `nginx-*` |
| `kibana` | `kibana:8.10.2` | — | Просмотр логов |
| `kibana-bootstrap` | `kibana:8.10.2` | — | Одноразовое создание data views `app-*` и `nginx-*` скриптом `infra/kibana/bootstrap.js` (Node из образа Kibana); существующие представления не меняет |
| `filebeat` | `elastic/filebeat:8.10.2` | `filebeat_data`, `nginx_logs` (чтение) | Отправка access-лога (JSON) и error-лога Nginx в Logstash |
| `glitchtip` | `glitchtip/glitchtip:6.2.6` | — | Приём ошибок по протоколу Sentry; миграции и воркер задач в одном процессе |
| `glitchtip-bootstrap` | `glitchtip/glitchtip:6.2.6` | — | Одноразово создаёт администратора и проект с id и ключом из `SENTRY_DSN` (`infra/glitchtip/bootstrap.py`) |
| `sentry-db` | `postgres:17` | `sentry_db_data` | База GlitchTip |
| `sentry-redis` | `redis:7-alpine` | — | Очередь и кэш GlitchTip |
| `kafka-0`, `kafka-1`, `kafka-2` | `bitnamilegacy/kafka:3.4` | `kafka_0_data`, `kafka_1_data`, `kafka_2_data` | Кластер Kafka в режиме KRaft |
| `kafka-init` | `bitnamilegacy/kafka:3.4` | — | Создание топиков `ugc.events` и `ugc.events.dlq` (6 партиций, replication-factor 3) |
| `kafka-ui` | `provectuslabs/kafka-ui:v0.7.0` | — | Просмотр топиков Kafka |
| `zookeeper` | `zookeeper:3.8` | — | Координация реплик ClickHouse, порт `2181` |
| `clickhouse-node1`…`clickhouse-node4` | `clickhouse/clickhouse-server:24.8` | `clickhouse_node1_data`…`clickhouse_node4_data` | Кластер `company_cluster`: два шарда по две реплики |
| `ugc-events` | `services/ugc-events/Dockerfile` | — | Приём пакетов действий зрителя в Kafka |
| `ugc-etl` | `services/ugc-etl/Dockerfile` | — | Чтение Kafka и запись в ClickHouse |
| `mongors1n1`…`mongors1n3`, `mongors2n1`…`mongors2n3` | `mongo:8.0` | `mongors1n1_data`…`mongors2n3_data` | Два шарда MongoDB по три реплики (`mongors1`, `mongors2`) |
| `mongocfg1`…`mongocfg3` | `mongo:8.0` | `mongocfg1_data`…`mongocfg3_data` | Replica set конфиг-серверов `mongors1conf` |
| `mongos1`, `mongos2` | `mongo:8.0` | — | Роутеры MongoDB, `127.0.0.1:27019` и `127.0.0.1:27020` |
| `mongo-init` | `mongo:8.0` | — | Инициализация replica set и регистрация шардов (`infra/mongo/init-cluster.sh`) |
| `ugc-api` | `services/ugc-api/Dockerfile` | — | Закладки, лайки и рецензии в MongoDB |

Контейнеры приложений собираются через `build` из сервисных Dockerfile. Nginx, PostgreSQL, Redis, Elasticsearch, Kafka, ZooKeeper, ClickHouse, MongoDB, Kafka UI и Jaeger запускаются через `image` из корневого `compose.yaml`.

## Конфигурация окружения

Корневой `.env` задаёт значения для Docker Compose. Compose передаёт сервисам переменные через блоки `environment`.

Каждый сервис имеет собственный `.env.example` для отдельного запуска. Auth, Admin, Movies API, UGC Events, UGC ETL и UGC API загружают `.env` из своего каталога. Movies ETL загружает `.env` из текущего рабочего каталога, поэтому его запускают из `services/movies-etl`.

Корневые префиксы разделяют одноимённые настройки сервисов. `THEATRE_POSTGRES_*` и `THEATRE_SQL_OPTIONS` относятся к общей базе Admin и Movies ETL. `AUTH_POSTGRES_*` относится к базе Auth. `AUTH_ADMIN_*` задаёт начального суперпользователя Auth. `AUTH_CLIENT_*` задаёт таймауты и безопасные повторы внутренних запросов Admin к Auth. `AUTH_TRACING_*` включает трассировку Auth, задаёт имя сервиса и долю сэмплинга. `AUTH_RATE_LIMIT_*` задаёт Token Bucket Auth. `AUTH_YANDEX_OAUTH_*` задаёт client ID, client secret, Redirect URI, TTL OAuth state, таймауты и повторы клиента Яндекса. `JAEGER_UI_PORT` и `JAEGER_OTLP_HTTP_PORT` публикуют локальные порты Jaeger. `CLICKHOUSE_USER` и `CLICKHOUSE_PASSWORD` задают одного пользователя на четырёх узлах ClickHouse и в UGC ETL; пароль также задаёт `secret` кластера и учётные данные реплик в `infra/clickhouse/config.d/cluster.xml`. Compose удаляет служебные части из имён `ADMIN_*`, `AUTH_BACKOFF_*`, `AUTH_REDIS_*`, `AUTH_RATE_LIMIT_*`, `AUTH_YANDEX_OAUTH_*`, `MOVIES_BACKOFF_*`, `MOVIES_REDIS_*`, `MOVIES_MAX_*`, `MOVIES_AUTH_*` и `ETL_*`. Compose передаёт `JWT_SECRET` и `JWT_ALGORITHM` в Auth, Movies API, UGC Events и UGC API. Movies API дополнительно получает `SERVICE_API_KEY`. `GATEWAY_PORT` задаёт внешний порт Nginx. `KIBANA_PORT` и `SENTRY_PORT` публикуют Kibana и GlitchTip на `127.0.0.1`. `SENTRY_DB_*` и `SENTRY_SECRET_KEY` настраивают GlitchTip. `SENTRY_ADMIN_*` задаёт администратора, которого создаёт `glitchtip-bootstrap`. `SENTRY_DSN`, `SENTRY_ENVIRONMENT` и `SENTRY_TRACES_SAMPLE_RATE` передаются без изменений в Auth, Movies API, UGC Events, UGC API и UGC ETL; эти же сервисы получают `LOGSTASH_HOST=logstash` и `LOGSTASH_PORT=5044`.

Конфигурация Elasticsearch задаётся в `compose.yaml` через `environment`: кластер работает в одновузловом режиме, встроенная авторизация Elasticsearch отключена, память JVM ограничена, каталог `infra/elasticsearch/snapshots` подключён как snapshot repository. `elasticsearch-init` восстанавливает индекс `movies`, если индекс отсутствует, и добавляет mapping поля `creation_date`. Полный mapping для последующей загрузки Movies ETL находится в `services/movies-etl/postgres_to_es/pipelines/loader/mapping.py`.

UGC Events в Compose получает `KAFKA_BOOTSTRAP_SERVERS` и `KAFKA_TOPIC=ugc.events`. UGC ETL получает те же брокеры, `CLICKHOUSE_HOST=clickhouse-node1` и монтирует `infra/clickhouse/init.sql` в `/app/init.sql`. Схему `ON CLUSTER company_cluster` выполняет ETL при старте, а не initdb узлов.

UGC API в Compose получает `MONGO_URI=mongodb://mongos1:27017,mongos2:27017` и `MONGO_DB=ugc` и стартует после успешного `mongo-init`. `MONGO_SHARD_CACHE_SIZE_GB` ограничивает кэш WiredTiger шардовых `mongod`.

## Владение данными

| Хранилище | Владелец | Доступ |
| --- | --- | --- |
| `theatre-db` | Admin | Admin записывает данные и управляет схемой; Movies ETL читает таблицы `content.*` |
| `auth-db` | Auth | Auth записывает пользователей, внешние связи, роли и историю входов; Auth управляет схемой через Alembic |
| `elasticsearch` | Общая поисковая инфраструктура | Elasticsearch Init восстанавливает индекс; Movies ETL обновляет документы; Movies API читает документы |
| `movies-redis` | Movies API | Movies API читает и записывает кэш |
| `auth-redis` | Auth | Auth читает и записывает сессии, denylist, одноразовый OAuth state, кэш разрешений и состояние Token Bucket |
| Kafka `ugc.events` | UGC Events | UGC Events пишет конверты; UGC ETL читает group `ugc-etl` |
| Kafka `ugc.events.dlq` | UGC ETL | UGC ETL пишет исходные bytes невалидных сообщений |
| MongoDB `ugc` | UGC API | UGC API читает и пишет `film_ratings`, `bookmarks`, `reviews`, `review_votes` через `mongos1`, `mongos2`; индексы создаёт `init_beanie` при старте |
| ClickHouse `ugc` | UGC ETL | ETL вставляет в Distributed `ugc.events`; materialized view наполняют `film_watches_local` и `page_events_local`; аналитик читает Distributed `ugc.events`, `ugc.film_watches`, `ugc.page_events` |

Начальное состояние `theatre-db` хранится в `services/admin/database_dump.sql`. Снапшоты Elasticsearch хранятся в `infra/elasticsearch/snapshots`.

Jaeger хранит трассы в памяти контейнера. Он не владеет бизнес-данными и не использует постоянный том. ZooKeeper не имеет именованного тома.

Кластер ClickHouse: шард 1 — `clickhouse-node1` и `clickhouse-node2`; шард 2 — `clickhouse-node3` и `clickhouse-node4`. Ключ шардирования — `cityHash64(toString(user_id))`. `internal_replication` включён. HTTP с хоста публикуется только у `clickhouse-node1` на `127.0.0.1:8123`.

## Поток данных

```text
Пользователь
    │
    ▼
Nginx: внешний Leaky Bucket только для /api/v1/auth/*
    ├── /admin/* ──────────────────────► Admin
    ├── /api/v1/auth/* ────────────────► Auth
    ├── /api/v1/films* ────────────────► Movies API
    ├── /api/v1/ugc/events ────────────► UGC Events
    ├── /ugc/health, /ugc/docs ────────► UGC Events
    ├── /api/v1/ugc/* ─────────────────► UGC API
    ├── /ugc-api/health, /ugc-api/docs ► UGC API
    └── /ugc-etl/health ───────────────► UGC ETL

Admin ── запись ──► theatre-db
                         │
                         │ чтение
                         ▼
                    Movies ETL
                         │
                         │ индексирование
                         ▼
                   Elasticsearch ◄── чтение ── Movies API

Снапшоты Elasticsearch ──► Elasticsearch Init ── восстановление ──► Elasticsearch

Auth ──────────────► auth-db
Auth ──────────────► auth-redis
Auth ── OTLP/HTTP ─► Jaeger
Auth ── token и profile API ──► Яндекс ID
Admin ─────────────► Auth: вход, профиль и роли
Admin ─────────────► theatre-db: Django-сессии, AuthIdentity и django_admin_log
Movies API ────────► movies-redis
Movies API ────────► Auth: POST /api/v1/auth/permissions/check

UGC Events ── локальная проверка access JWT ──► Kafka ugc.events
UGC ETL ──── group ugc-etl ──► Kafka ugc.events
UGC ETL ──── INSERT ──► ClickHouse Distributed ugc.events
UGC ETL ──── Produce ──► Kafka ugc.events.dlq (яд)
UGC API ── локальная проверка access JWT ──► mongos1, mongos2 ──► шарды mongors1, mongors2
```

Внешний HTTP-трафик проходит через Nginx. Nginx создаёт `X-Request-Id`, передаёт его приложению и записывает в access-log. Nginx применяет Leaky Bucket только к `/api/v1/auth/*`. Лимит равен `20` запросам в секунду на IP, burst равен `20`. Admin и Movies API обращаются в Auth напрямую по внутреннему DNS-имени `auth` и не проходят через этот внешний лимит. `POST /api/v1/ugc/events` не входит в `/api/v1/auth/*` и не использует внешний лимит Nginx.

UGC Events проверяет access JWT локально теми же `JWT_SECRET` и `JWT_ALGORITHM`, что и Auth. `user_id` берётся из claim `sub`. Auth на каждый клик не вызывается. HTTP `202` и `{"accepted": N}` означают запись в Kafka, а не вставку в ClickHouse.

UGC ETL читает `ugc.events` клиентом `aiochclient` и пишет в Distributed `ugc.events` на `clickhouse-node1:8123`. Commit офсета выполняется только после успешного INSERT. Невалидные сообщения уходят в `ugc.events.dlq`. Локальные таблицы — `ReplicatedReplacingMergeTree` с версией `ingested_at`.

Auth применяет Token Bucket к локальной аутентификации, профилю, RBAC и внутренней проверке прав. Маршруты Яндекс OAuth не используют внутренний Token Bucket. Их ограничивает внешний лимит Nginx, одноразовый state и TTL. Состояние Token Bucket принадлежит Auth и хранится в `auth-redis`. Один зарегистрированный Lua-скрипт атомарно использует время Redis, пополняет bucket и расходует токен. Healthcheck, Swagger UI и OpenAPI также не используют Token Bucket. Ошибка соединения или timeout Redis включает fail-open без backoff.

При входе через Яндекс браузер сначала получает redirect от Auth. Яндекс возвращает браузер на callback через Nginx. Auth атомарно получает OAuth state из `auth-redis`, обменивает одноразовый code на токен Яндекса и получает профиль. Auth сохраняет только стабильную связь Яндекс ID с локальным UUID. Токены Яндекса не сохраняются.

Admin и Movies API сохраняют `X-Request-Id` во внутренних запросах. Admin проверяет вход, профиль и административное право через Auth. Локальная запись Admin связывает UUID Auth с primary key Django для сессии, `request.user` и `django_admin_log`. Она не хранит пароль или роли Auth.

Auth требует `X-Request-Id` для каждого HTTP-запроса. OpenTelemetry записывает 5% новых корневых трасс, наследует решение родительской трассы и добавляет `http.request_id` в серверный спан. Auth отправляет выбранные спаны в Jaeger по OTLP/HTTP. Отказ Jaeger не меняет HTTP-ответ Auth.

Admin повторяет ошибку установления соединения для любого внутреннего запроса. Admin также повторяет безопасный `GET` после временной ошибки чтения или HTTP `502`, `503`, `504`. Admin не повторяет HTTP `429` или изменяющий запрос после ошибки ответа. Такой запрос мог завершиться в Auth до разрыва соединения. После технического отказа Admin возвращает HTTP `503`. После ограничения Auth Admin возвращает HTTP `429` и сохраняет `Retry-After`.

Список фильмов, поиск и healthcheck Movies API не обращаются в Auth. Полная карточка фильма обращается в Auth после чтения фильма. Movies API один раз повторяет timeout, ошибку соединения или HTTP 502, 503, 504. Movies API распознаёт HTTP `429` как отдельное ограничение, сохраняет `Retry-After` и не задерживает запрос пользователя. После повторного технического отказа или HTTP `429` Movies API применяет локальную политику деградации.

UGC ETL публикует только `GET /api/v1/health/`. Swagger и OpenAPI у ETL выключены. Снаружи gateway даёт `GET /ugc-etl/health`. HTTP 200, если доступны Kafka, ClickHouse на `clickhouse-node1` и RSS процесса в пределах лимита. HTTP 503 не перезапускает контейнер: `restart: unless-stopped` реагирует на падение процесса, а не на unhealthy.

## Внешние маршруты

| Маршрут | Получатель |
| --- | --- |
| `/admin/*` | Django Admin |
| `/static/*` | Статические файлы Django |
| `/api/v1/auth/*` | Auth API |
| `/api/v1/auth/social/{provider_name}/login` | Начало входа через зарегистрированного OAuth-провайдера |
| `/api/v1/auth/social/{provider_name}/callback` | Callback зарегистрированного OAuth-провайдера |
| `/api/v1/films*` | Movies API |
| `/api/v1/ugc/events` | UGC Events, пакет действий зрителя |
| `/admin/health` | Healthcheck Admin |
| `/auth/health` | Healthcheck Auth |
| `/movies/health` | Healthcheck Movies API |
| `/ugc/health` | Healthcheck UGC Events |
| `/ugc-etl/health` | Healthcheck UGC ETL |
| `/auth/docs` | OpenAPI Auth |
| `/movies/docs` | OpenAPI Movies API |
| `/ugc/docs` | OpenAPI UGC Events |
| `/auth/openapi.json` | OpenAPI JSON Auth |
| `/movies/openapi.json` | OpenAPI JSON Movies API |
| `/ugc/openapi.json` | OpenAPI JSON UGC Events |
| `http://127.0.0.1:16686/` | Jaeger UI |
| `http://localhost:8080` | Kafka UI |
| `http://127.0.0.1:8123` | ClickHouse HTTP узла `clickhouse-node1` |

## Порядок запуска

### Основной HTTP-контур

1. `theatre-db`, `auth-db`, `auth-redis`, `movies-redis` и `elasticsearch` проходят healthcheck.
2. `admin-migrate` применяет миграции после готовности `theatre-db`. `admin-collectstatic` независимо собирает статику Django.
3. `elasticsearch-init` восстанавливает индекс `movies` после готовности `elasticsearch`, если индекс отсутствует. Затем он добавляет mapping `creation_date`, если это поле ещё не описано.
4. `auth-migrate` применяет миграции Alembic после готовности `auth-db`.
5. `admin` запускается после готовности `theatre-db` и успешного завершения `admin-migrate` и `admin-collectstatic`.
6. `auth-bootstrap` создаёт начального суперпользователя после успешного завершения `auth-migrate` и готовности `auth-redis`.
7. `auth` запускается после успешного завершения `auth-bootstrap` и готовности `auth-redis`.
8. `movies-api` запускается после готовности `elasticsearch`, готовности `movies-redis` и успешного завершения `elasticsearch-init`.
9. `kafka-0`, `kafka-1` и `kafka-2` проходят healthcheck. `kafka-init` создаёт топики и завершается с кодом 0.
10. `ugc-events` запускается после готовности трёх брокеров и успешного завершения `kafka-init`.
11. `gateway` запускается после готовности `admin`, `movies-api` и `ugc-events`. Готовность Auth и UGC ETL не блокирует запуск gateway.

`jaeger` запускается независимо. Его готовность не блокирует Auth или gateway.

Сервисы ELK и GlitchTip входят в профиль `observability` и стартуют только с `--profile observability`. `logstash` и `kibana` запускаются после готовности `elk-elasticsearch`, `kibana-bootstrap` — после готовности `kibana` (healthcheck по `/api/status`), `glitchtip` — после `sentry-db` и `sentry-redis`, `glitchtip-bootstrap` — после готовности `glitchtip`, `filebeat` — после старта `logstash` и `gateway`. `nginx-logrotate` запускается после `gateway` вне профиля. Все сервисы подключают якорь `x-logging` с ротацией `json-file` (3 × 10 МБ). Приложения не ждут ни ELK, ни GlitchTip: логи уходят по UDP, а SDK Sentry отправляет события в фоне.

### Фоновый ETL-контур

`movies-etl` запускается после готовности `theatre-db`, готовности `elasticsearch` и успешного завершения `elasticsearch-init`. Он обновляет индекс `movies` данными из `theatre-db` в фоновом режиме. Запуск `movies-api` и `gateway` не зависит от состояния `movies-etl`.

`zookeeper` проходит healthcheck. Четыре узла ClickHouse стартуют после ZooKeeper. `ugc-etl` запускается после готовности Kafka, `kafka-init` и всех четырёх узлов ClickHouse. Он выполняет `ensure_schema()` из `infra/clickhouse/init.sql` и читает `ugc.events` в фоне. Запуск `gateway` не зависит от состояния `ugc-etl`.
