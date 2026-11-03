# Movies API

FastAPI-сервис читает каталог фильмов из Elasticsearch. Redis хранит кэш ответов. Auth проверяет доступ к полной карточке фильма.

## Запуск в едином проекте

Выполните команды из корня репозитория:

```bash
cp .env.example .env
docker compose up -d --build
```

Контейнер `elasticsearch-init` восстанавливает индекс `movies` и проверяет mapping поля `creation_date` до запуска Movies API. Запуск Movies API не зависит от фонового Movies ETL.

| Ресурс | Внешний URL |
| --- | --- |
| Swagger UI | `http://localhost/movies/docs` |
| OpenAPI JSON | `http://localhost/movies/openapi.json` |
| Health | `http://localhost/movies/health` |
| Фильмы | `http://localhost/api/v1/films` |

## API

| Метод | Путь | Назначение |
| --- | --- | --- |
| `GET` | `/api/v1/films` | Список фильмов с сортировкой, фильтром и пагинацией |
| `GET` | `/api/v1/films/search/` | Поиск по названию |
| `GET` | `/api/v1/films/{film_id}/` | Карточка фильма с проверкой доступа через Auth |
| `GET` | `/api/v1/health/` | Состояние Elasticsearch и Redis |

Примеры:

```bash
curl 'http://localhost/api/v1/films?sort=-imdb_rating&page_size=5'
curl 'http://localhost/api/v1/films/search/?query=star&page_number=1&page_size=5'
```

### Доступ к полной карточке

Movies API допускает анонимный запрос. Переданный access JWT проходит локальную проверку подписи, срока и типа. После чтения фильма Movies API вызывает `POST /api/v1/auth/permissions/check` для фильма с датой и без даты.

Auth возвращает `allowed=true` или `allowed=false`. Movies API преобразует эти значения в HTTP 200 или HTTP 403. Отозванный access-токен возвращает HTTP 401.

После timeout, ошибки соединения или HTTP `502`, `503`, `504` Movies API один раз повторяет запрос. Повтор использует exponential backoff с full jitter. Максимальное ожидание повтора равно 0,5 секунды. Movies API распознаёт HTTP `429` как отдельное ограничение и сохраняет положительный `Retry-After`. HTTP `429`, HTTP `500`, ошибки контракта, ошибки токена и `allowed=false` не повторяются.

Movies API передаёт `X-Request-Id` в `POST /permissions/check`. Запрос через gateway сохраняет идентификатор Nginx. При отдельном вызове Movies API без этого заголовка сервис создаёт UUID один раз и использует его для всех попыток запроса к Auth.

Если Auth остаётся недоступен или возвращает HTTP `429`, Movies API немедленно применяет локальную деградацию. Сервис не удерживает запрос пользователя до окончания `Retry-After`. Фильм возрастом не меньше трёх календарных лет доступен. Фильм без `creation_date` и фильм младше трёх лет недоступны. Список, поиск и healthcheck продолжают работать без Auth.

## Локальная разработка

Для локального запуска подготовьте Elasticsearch и Redis, которые доступны с хоста. Затем выполните команды из `services/movies-api`:

```bash
cp .env.example .env
uv sync --group test
uv run uvicorn main:app --app-dir src --reload --host 127.0.0.1 --port 8001
```

Локальный шаблон ожидает Auth по адресу `http://127.0.0.1:8000`. Значения `AUTH_JWT_SECRET` и `AUTH_SERVICE_API_KEY` должны совпадать со значениями Auth. `AUTH_BACKOFF_MAX_TRIES=2` задаёт одну повторную попытку. `AUTH_BACKOFF_MAX_TIME=0.5` ограничивает ожидание повтора. Movies API не запускается без обязательных адресов и секретов.

Корневой Compose не публикует внутренний порт `8000` Movies API. Для доступа к документации единого проекта используйте gateway-адрес из таблицы выше.

## Проверка

Модульные тесты не требуют запущенных Elasticsearch и Redis:

```bash
set -a
source .env.example
set +a
uv lock --check
uv run --group test pytest tests -q --ignore=tests/functional
```

Функциональные тесты запускают изолированные Movies API, Elasticsearch и Redis:

```bash
cd tests/functional
docker compose up --build --abort-on-container-exit --exit-code-from tests
docker compose down -v
```

Функциональные тесты используют порты из `tests/functional/.env`. Они не используют корневой Compose.

## Структура

```text
src/
  api/v1/          HTTP-маршруты фильмов и health
  clients/         клиент внутреннего Auth API
  security/        локальная проверка access JWT
  services/        бизнес-логика
  db/storage/      доступ к Elasticsearch
  db/cache/        доступ к Redis
  models/          модели ответов
  main.py          FastAPI-приложение
tests/
  functional/      изолированные HTTP-тесты и Compose
```
