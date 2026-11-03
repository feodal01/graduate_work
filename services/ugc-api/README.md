# UGC API

Закладки, лайки (оценки 0…10) и рецензии фильмов в MongoDB. FastAPI + Beanie поверх асинхронного клиента PyMongo (`pymongo.AsyncMongoClient`).

Motor, который встречается в примерах Beanie, устарел, и Beanie 2 его не использует. Поэтому клиент берётся из PyMongo: `lifespan` создаёт `AsyncMongoClient`, вызывает `init_beanie(...)` и при остановке закрывает клиент.

Почему это отдельный сервис, объяснено в [корневом README](../../README.md#ugc-api).

## Данные

| Коллекция | Документ | Уникальный ключ |
| --- | --- | --- |
| `film_ratings` | оценка фильма пользователем, `score` 0…10 (10 — лайк, 0 — дизлайк) | `(user_id, film_id)` |
| `bookmarks` | фильм в закладках пользователя | `(user_id, film_id)` |
| `reviews` | текст, оценка автора, счётчики `likes` / `dislikes` | `(film_id, user_id)` — одна рецензия на фильм |
| `review_votes` | лайк/дизлайк рецензии | `(review_id, user_id)` |
| `ugc_outbox` | итоговый счётчик лайков, `likes_version`, получатель и отметка публикации | `event_id`; для загрузки — `(load_id, review_id)` |
| `review_write_gate` | 256 постоянных документов барьера записи для исходного снимка | целый `_id` от 0 до 255 |

Индексы создаёт `init_beanie` при старте. Оценки и закладки пишутся атомарным `find_one_and_update(upsert=True)`. Beanie `upsert` делает find и insert отдельными запросами, поэтому здесь не подходит. Счётчики рецензии меняются через `$inc` на разницу голосов. Оценка автора в рецензии синхронизируется при изменении его оценки фильма.

Для записи рецензий нужен MongoDB replica set или sharded cluster с поддержкой транзакций. Изменение голоса, счётчики и событие `review.likes_updated` сохраняются одной транзакцией. Событие содержит итоговое число лайков и `user_id` автора рецензии. `likes_version` начинается с `0` и увеличивается при изменении числа лайков. Повтор того же голоса и изменение только дизлайков событий не создают. Ошибка записи outbox откатывает все изменения; конфликт транзакций повторяет чтение и запись.

Барьер записи позволяет исходной загрузке остановить изменения рецензий и голосов, включая уже начатые транзакции. Каждая такая операция пишет один документ, выбранный по `int.from_bytes(review_id.binary, 'big') % 256`. Создание заранее выделяет `review_id`. Остановка должна одной транзакцией установить `frozen=True` во всех 256 документах и завершить её перед чтением снимка. При остановке API отвечает `503`. Запуск API создаёт только отсутствующие документы и сохраняет установленную остановку. Процедура исходной загрузки и relay запускаются отдельными CLI ниже.

## API

Все пути начинаются с `/api/v1/ugc`. Эндпоинты с 🔒 требуют access-токен Auth (`Authorization: Bearer …`, HS256).

| Метод и путь | Что делает |
| --- | --- |
| `PUT /films/{film_id}/rating` 🔒 | поставить или изменить оценку `{"score": 0..10}` |
| `GET /films/{film_id}/rating` 🔒 | моя оценка |
| `DELETE /films/{film_id}/rating` 🔒 | удалить оценку |
| `GET /films/{film_id}/rating/summary` | лайки, дизлайки, число оценок, средняя оценка |
| `PUT /films/{film_id}/bookmark` 🔒 | добавить в закладки (идемпотентно) |
| `DELETE /films/{film_id}/bookmark` 🔒 | удалить из закладок |
| `POST /films/{film_id}/reviews` 🔒 | написать рецензию `{"text": "..."}`; вторая на тот же фильм — 409 |
| `GET /films/{film_id}/reviews?sort=-likes` | рецензии фильма; сортировка `created_at`, `likes`, `dislikes`, `score`, с `-` — по убыванию |
| `GET /reviews/{review_id}` | рецензия |
| `PATCH /reviews/{review_id}` 🔒 | изменить текст (только автор) |
| `DELETE /reviews/{review_id}` 🔒 | удалить (только автор), вместе с голосами |
| `PUT /reviews/{review_id}/vote` 🔒 | `{"value": "like" \| "dislike"}` |
| `DELETE /reviews/{review_id}/vote` 🔒 | отозвать голос |
| `GET /users/me/ratings?score=10` 🔒 | мои оценки; `score=10` — понравившиеся фильмы |
| `GET /users/me/bookmarks` 🔒 | мои закладки, новые первыми |

Сервис проверяет access JWT локально, без запроса в Auth. Проверяются подпись HMAC общим секретом, алгоритм (только `HS256`, `alg: none` отклоняется), срок `exp`, обязательные claims `sub`, `jti`, `type`, `iat`, `exp` и `type == access`. `user_id` берётся из `sub` только после успешной проверки. Иначе ответ 401. Отозванный в Auth токен (logout, смена пароля) действует до истечения `exp`: denylist Auth здесь не проверяется, как и в UGC Events.

Списки принимают `page_number` (от 1) и `page_size` (до `MAX_PAGE_SIZE`). Health: `GET /api/v1/health/` пингует MongoDB, при недоступности отвечает 503. Swagger: `/ugc-api/docs`.

## Настройки

| Переменная | По умолчанию | Назначение |
| --- | --- | --- |
| `AUTH_JWT_SECRET` | — | общий с Auth секрет, не короче 32 символов |
| `AUTH_JWT_ALGORITHM` | `HS256` | алгоритм подписи |
| `MONGO_URI` | — | адреса mongos, например `mongodb://mongos1:27017,mongos2:27017` |
| `MONGO_DB` | `ugc` | база данных |
| `MONGO_SERVER_SELECTION_TIMEOUT_MS` | `5000` | таймаут выбора сервера |
| `MAX_PAGE_SIZE` | `100` | максимальный `page_size` |

## Локальный запуск и тесты

Python 3.10–3.13: Beanie пока не поддерживает 3.14, поэтому образ собирается на 3.12.

```bash
cd services/ugc-api
cp .env.example .env            # задать AUTH_JWT_SECRET
uv sync --all-groups
cd src && uv run uvicorn main:app --reload
```

Тесты идут против настоящего MongoDB replica set: mongomock не поддерживает асинхронный PyMongo. Каждый прогон создаёт временную базу `ugc_test_*` и удаляет её после тестов. Недоступный MongoDB или отсутствие replica set завершают проверку ошибкой.

```bash
docker compose up -d mongo-init   # из корня репозитория
cd services/ugc-api
uv run --group lint ruff check . && uv run --group lint mypy
uv run --group test pytest        # MONGO_URI по умолчанию mongodb://127.0.0.1:27019
```


## Исходный снимок и relay Notifications

Снимок — постоянные строки `likes_bootstrap_rows` с исходными `event_id`, числом лайков и версией. Загрузка — запись `likes_bootstrap_loads` с постоянным `load_id`, состоянием и прогрессом. Обе коллекции сохраняются после очистки outbox.

Из каталога `services/ugc-api/src`:

```bash
uv run python -m bootstrap_notifications capture --load-id likes-initial-2026
uv run python -m bootstrap_notifications enqueue --load-id likes-initial-2026 --limit 500
uv run python -m bootstrap_notifications status --load-id likes-initial-2026 > likes-manifest.json
uv run python -m relay
```

`capture` одной короткой транзакцией закрывает все 256 элементов барьера. Конфликт с начатой записью ждёт её завершения через повтор транзакции. Затем процесс сохраняет снимок пачками по `BOOTSTRAP_BATCH_SIZE` (по умолчанию 500), не увеличивая версии. Пока снимок не закончен, API отклоняет изменения рецензий и голосов с `503`. Сбой сохраняет запрет записи: нужно повторить **capture с тем же load_id**, чтобы продолжить. Завершение снимка и открытие записи происходят одной транзакцией. Повтор capture завершённого снимка запрещён.

`enqueue` проходит сохранённые строки; outbox, отметка строки и прогресс загрузки сохраняются одной транзакцией на пачку. Повторный запуск безопасен даже после удаления опубликованных записей outbox. Завершённая загрузка имеет состояние `enqueued`; это означает только добавление событий в outbox.

`status` печатает только JSON manifest `{load_id, expected_count, manifest_hash}`. Хеш — SHA-256 строк UTF-8 `review_id,event_id\n`, отсортированных по review_id. Во время незаконченного capture manifest недоступен, команда завершится ошибкой. Manifest передаётся CLI Notifications для проверки реально обработанных событий; статус контейнера или Kafka ACK не заменяет эту проверку.

Relay работает без HTTP, отправляет JSON в `KAFKA_LIKES_TOPIC` (по умолчанию `ugc.reviews.v1`), ключ — `review_id`. `KAFKA_BOOTSTRAP_SERVERS` обязателен для relay. Сообщения пачки отправляются асинхронно; после ACK Kafka заполняется `published_at` только у подтверждённых строк. `--once` обрабатывает одну пачку. Размер задаёт `RELAY_BATCH_SIZE=500`, паузу при пустом результате/ошибке — `RELAY_POLL_INTERVAL_SECONDS=1`.

Отдельный документ `ugc_relay_lock` сериализует пачки relay в Mongo-транзакции и не блокирует барьер голосов. `RELAY_BATCH_TIMEOUT_SECONDS=20` ограничивает чтение/отправку/ACK; `RELAY_COMMIT_TIMEOUT_MS=5000` ограничивает commit. Эти значения вместе с запасом 5 секунд должны быть меньше `RELAY_MONGO_TRANSACTION_LIFETIME_SECONDS=60`. Для replica set запуск проверяет фактический `transactionLifetimeLimitSeconds` через `getParameter` (нужен доступ к этой команде). При подключении через mongos оператор обязан проверить, что **на каждом shard** лимит не ниже указанного значения; процесс явно пишет это требование в лог, потому что mongos не предоставляет лимит shard.

Сбой или таймаут освобождает транзакционную блокировку и оставляет неподтверждённые строки для повтора. Уже переданное Kafka сообщение невозможно отозвать отменой asyncio: повтор после сбоя сохраняет исходные event_id и payload, поэтому downstream должен устранять дубли. Долгое завершение `producer.stop()` при недоступной Kafka также не удерживает Mongo-блокировку; менеджер процессов может завершить процесс принудительно.
