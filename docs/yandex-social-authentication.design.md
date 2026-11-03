# Вход через Яндекс в Auth

## Область решения

Auth поддерживает реальный вход через Яндекс ID. Auth работает как потребитель OAuth 2.0. Auth не реализует собственный OAuth-сервер.

Решение использует Authorization Code и Proof Key for Code Exchange (PKCE) с методом `S256`. Фронтенд не добавляется. После успешного callback Auth возвращает текущую пару access и refresh JSON Web Token (JWT).

Первая версия поддерживает только Яндекс. Она не добавляет VK, Google и автоматическое связывание аккаунтов по email.

## Публичные маршруты

| Метод и путь | Назначение | Успешный ответ |
| --- | --- | --- |
| `GET /api/v1/auth/social/yandex/login?tz=Europe/Moscow` | Создать безопасное OAuth-состояние и перенаправить браузер в Яндекс | HTTP `307` |
| `GET /api/v1/auth/social/yandex/callback` | Проверить callback, получить профиль и выпустить JWT Auth | HTTP `200` и `TokenPairResponse` |

Параметр `tz` обязателен и должен содержать именованный часовой пояс IANA пользователя, например `Europe/Moscow`. Auth сохраняет его в OAuth-состоянии и использует его при создании нового пользователя; при повторном входе существующий tz не изменяется.

Текущий `POST /api/v1/auth/login` сохраняет свой контракт. Он возвращает HTTP `401`, если у пользователя нет локального пароля.

## Поток входа

1. Пользователь открывает `GET /api/v1/auth/social/yandex/login?tz=Europe/Moscow` через Nginx gateway.
2. Auth создаёт случайные `state` и `code_verifier`.
3. Auth вычисляет `code_challenge` методом `S256`.
4. Auth сохраняет `code_verifier` и проверенный `tz` в `auth-redis` на 600 секунд.
5. Auth возвращает redirect на `https://oauth.yandex.ru/authorize`.
6. Яндекс проверяет учётную запись и согласие пользователя.
7. Яндекс возвращает браузер на настроенный callback с `code` и `state`.
8. Auth атомарно извлекает и удаляет состояние из Redis.
9. Auth обменивает `code` на токен Яндекса.
10. Auth получает профиль через `GET https://login.yandex.ru/info`.
11. Auth находит или создаёт локального пользователя.
12. Auth записывает успешный вход.
13. Auth выпускает текущие access и refresh JWT.

Потоки описаны в диаграммах:

- `docs/diagrams/auth/auth-yandex-login-start-sequence-as-is.puml` и соседний PNG;
- `docs/diagrams/auth/auth-yandex-callback-sequence-as-is.puml` и соседний PNG;
- `docs/diagrams/auth/sequence-authentication-as-is.puml` и соседний PNG для локального входа.

## OAuth-состояние

Auth хранит OAuth-состояние в существующем `auth-redis`. Отдельный Redis и отдельный сервис не добавляются.

Ключ имеет формат:

```text
oauth-state:yandex:<sha256(state)>
```

Значение содержит только `code_verifier`. TTL равен 600 секундам. Auth использует `SET NX EX` для создания и `GETDEL` для одноразового получения.

Auth не выполняет backoff для `GETDEL`. Первый запрос мог удалить значение до ошибки соединения. Повторный запрос дал бы ложный результат об отсутствующем состоянии.

Недоступность Redis завершает OAuth-вход с HTTP `503`. Auth не продолжает вход без проверки `state`.

## Клиент Яндекса

Auth использует один асинхронный `httpx.AsyncClient` на время жизни процесса.

Клиент выполняет три операции:

```python
def build_authorization_url(
    *,
    state: str,
    code_challenge: str,
) -> str:
    ...

async def exchange_code(
    *,
    code: str,
    code_verifier: str,
) -> str:
    ...

async def get_identity(access_token: str) -> SocialIdentity:
    ...
```

`exchange_code` не повторяет запрос. Код Яндекса является одноразовым. `get_identity` повторяет только безопасный GET при connection error, timeout, HTTP `429` и HTTP `5xx`. Число попыток, таймаут HTTP-запроса и максимальная задержка ограничены настройками.

Для HTTP `429` клиент использует положительный целочисленный `Retry-After`. При отсутствующем или неверном заголовке клиент использует ограниченную экспоненциальную задержку.

Клиент проверяет ответы Яндекса через Pydantic. Он проверяет `token_type`, наличие токена, идентификатор пользователя и `client_id` профиля.

Auth запрашивает только права `login:info` и `login:email`.

## Локальный пользователь и внешняя связь

Таблица `users` остаётся источником локального UUID, ролей, разрешений, истории и JWT. Поле `users.password` становится nullable.

Новая таблица `social_accounts` содержит:

| Поле | Тип | Назначение |
| --- | --- | --- |
| `id` | UUID | Первичный ключ |
| `user_id` | UUID | Ссылка на `users.id` |
| `provider` | String | Значение `yandex` |
| `provider_user_id` | String | Стабильный идентификатор пользователя Яндекса |
| `email` | String или NULL | Последний email из Яндекс ID |
| `created_at` | DateTime | Время создания связи |
| `updated_at` | DateTime | Время последнего обновления связи |

Ограничения:

```text
UNIQUE(provider, provider_user_id)
UNIQUE(user_id, provider)
```

Auth не хранит access и refresh токены Яндекса.

## Создание и обновление пользователя

Auth ищет связь по `provider=yandex` и `provider_user_id`.

Если связь существует, Auth сохраняет локальный `user_id`. Auth обновляет имя, фамилию и email внешней связи. Auth не меняет локальный login.

Auth ограничивает имя и фамилию длиной полей локальной модели. Длинное имя Яндекса не должно блокировать вход.

Если связи нет, Auth создаёт пользователя и внешнюю связь в одной транзакции. Новый пользователь не получает роли и `is_superuser`.

Auth выбирает login в следующем порядке:

1. Использовать `default_email`, если значение есть и login свободен.
2. Использовать логин Яндекса, если email отсутствует и login свободен.
3. Использовать `yandex_<provider_user_id>`, если предпочтительный login занят.
4. Добавить случайный безопасный суффикс, если технический login также занят.

Auth не связывает аккаунты автоматически по email или login. Совпадение не доказывает, что текущий пользователь владеет существующим локальным аккаунтом.

## Локальный пароль

Пользователь Яндекса создаётся с `password=NULL`. `POST /api/v1/auth/login` возвращает тот же ответ HTTP `401`, который используется для неизвестного login и неверного пароля.

Авторизованный пользователь может установить пароль через текущий `PATCH /api/v1/auth/profile`. После этого доступны оба способа входа.

## Конфигурация

Локальный env Auth использует параметры:

| Параметр | Значение или правило |
| --- | --- |
| `YANDEX_OAUTH_ENABLED` | `false` по умолчанию; для реального входа задать `true` |
| `YANDEX_OAUTH_CLIENT_ID` | Идентификатор приложения из Яндекс OAuth; обязателен при включении |
| `YANDEX_OAUTH_CLIENT_SECRET` | Секрет приложения из Яндекс OAuth; обязателен при включении |
| `YANDEX_OAUTH_REDIRECT_URI` | `http://localhost/api/v1/auth/social/yandex/callback` для локального окружения |
| `YANDEX_OAUTH_STATE_TTL_SECONDS` | `600` |
| `YANDEX_OAUTH_CONNECT_TIMEOUT_SECONDS` | `1.0` |
| `YANDEX_OAUTH_REQUEST_TIMEOUT_SECONDS` | `3.0` |
| `YANDEX_OAUTH_USERINFO_MAX_TRIES` | `3` |
| `YANDEX_OAUTH_USERINFO_BACKOFF_MAX_SECONDS` | `2.0` |

Корневой env добавляет префикс `AUTH_`. Compose удаляет этот префикс при передаче значений контейнеру Auth.

`YANDEX_OAUTH_CLIENT_SECRET` является секретом. Репозиторий содержит только пустое значение или пример. Для рабочего входа пользователь записывает настоящий секрет в `.env` и включает `YANDEX_OAUTH_ENABLED`.

Redirect URI является явной настройкой окружения. Код не формирует его из HTTP-заголовка `Host`. Локальное окружение использует `http://localhost`. Развёрнутое окружение использует свой HTTPS-домен. Каждый адрес должен быть зарегистрирован в приложении Яндекс OAuth.

## Gateway, rate limiting и трассировка

Новый блок Nginx не нужен. Текущий `location /api/v1/auth/` передаёт оба маршрута в Auth, добавляет `X-Request-Id` и применяет внешний лимит по IP.

Внутренний Token Bucket Auth не применяется до определения локального пользователя. Одноразовый `state`, TTL и внешний лимит Nginx защищают новые публичные маршруты.

FastAPI instrumentation создаёт серверные спаны. Клиент добавляет дочерние спаны `yandex.oauth.token` и `yandex.oauth.userinfo`. Спаны не содержат `code`, `state`, `code_verifier`, OAuth-токены и client secret.

## Ошибки

| Ситуация | HTTP | Detail |
| --- | ---: | --- |
| Яндекс OAuth отключён или не настроен | `503` | `Yandex OAuth is not configured` |
| Auth не смог сохранить уникальный `state` после трёх попыток | `503` | `OAuth state could not be created` |
| Пользователь отменил вход | `400` | `Yandex authentication was cancelled` |
| Callback не содержит ровно один из параметров `code` и `error` | `400` | `OAuth callback is invalid` |
| `state` отсутствует, истёк или уже использован | `400` | `OAuth state is invalid or expired` |
| Код неверен или истёк | `400` | `OAuth code is invalid or expired` |
| Яндекс недоступен | `503` | `Yandex OAuth is unavailable` |
| Яндекс вернул неверный контракт | `502` | `Yandex OAuth returned an invalid response` |
| Redis состояния недоступен | `503` | `OAuth state store is unavailable` |

Auth не возвращает клиенту `error_description` Яндекса.

## Журналирование

Auth не записывает в лог:

- client secret;
- `code`;
- `state`;
- `code_verifier`;
- токены Яндекса;
- access и refresh JWT Auth.

Auth записывает `request_id` для HTTP-запроса. Дочерние спаны клиента содержат только имя OAuth-операции и имя провайдера.

## Критерии готовности

- Браузер проходит реальный вход через зарегистрированное приложение Яндекс OAuth.
- Callback возвращает текущий `TokenPairResponse` Auth.
- Первый вход создаёт одного пользователя и одну внешнюю связь.
- Повторный вход возвращает тот же локальный UUID.
- Параллельные callback не создают дубликаты.
- Совпавший email не связывает существующие аккаунты.
- Пользователь Яндекса без локального пароля получает HTTP `401` от локального login.
- Пользователь может установить локальный пароль через профиль.
- Успешный социальный вход появляется в текущей истории входов.
- Роли и разрешения продолжают использовать локальный UUID.
- Токены Яндекса не сохраняются в PostgreSQL, Redis и логах.
- Unit, integration и текущие межсервисные тесты проходят.
