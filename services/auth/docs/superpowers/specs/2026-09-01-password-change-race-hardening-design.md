# Защита смены пароля от отложенных запросов и медленного PostgreSQL

## Цель

Сохранить Redis единственным online-хранилищем `session_epoch`, но закрыть две
гонки: повторную смену пароля уже отозванным access-токеном и истечение marker
до завершения PostgreSQL update.

## Инварианты

- Access и refresh по-прежнему проверяются только через Redis; чтение
  PostgreSQL на каждом запросе не добавляется.
- Password change начинается только если epoch access-токена совпадает с
  текущим Redis epoch пользователя.
- Проверка ожидаемого epoch, установка marker и публикация промежуточного epoch
  выполняются одним Lua-скриптом.
- Максимальное время PostgreSQL до commit или rollback строго меньше TTL marker
  с запасом не менее 30 секунд.
- Ошибка успешной PostgreSQL-операции на этапе Redis finish не подавляется:
  HTTP-запрос завершается `503`, а marker продолжает блокировать сессии до TTL.
- Ошибка PostgreSQL сохраняет исходную `DatabaseUnavailableError`; cleanup Redis
  не должен подменять первичную ошибку.
- Смена только логина, social login и обычный logout сохраняют текущую семантику.
- PostgreSQL-миграция и новый HTTP endpoint не требуются.

## Временные границы

Настройки по умолчанию:

```text
POSTGRES_CONNECT_TIMEOUT_SECONDS=5
POSTGRES_POOL_TIMEOUT_SECONDS=10
POSTGRES_COMMAND_TIMEOUT_SECONDS=15
POSTGRES_STATEMENT_TIMEOUT_MS=15000
SESSION_PASSWORD_CHANGE_MARKER_TTL_SECONDS=120
```

Конфигурация должна отклоняться на старте, если не выполняется условие:

```text
marker TTL >= pool timeout + connect timeout
              + 2 * command timeout + 30 seconds
```

Два command timeout покрывают flush/update и commit. Операции после commit не
создают сессию со старым паролем, поэтому не входят в критическое окно.
`statement_timeout` задаётся на стороне PostgreSQL через asyncpg
`server_settings`; driver timeout и pool timeout ограничивают клиентские
ожидания.

## Начало password change

`CurrentUser` сохраняет подписанный `session_epoch` access-токена. Begin Lua
получает `expected_epoch` и возвращает разные исходы:

```text
current epoch != expected epoch -> stale, HTTP 401 Token revoked
marker уже существует          -> busy, HTTP 503
epoch совпал и marker свободен  -> marker + промежуточный epoch
Redis error                     -> HTTP 503
```

Ни marker, ни epoch не меняются при stale/busy результате.

## Завершение password change

- При PostgreSQL success finish публикует новый финальный epoch и удаляет marker
  только своего владельца.
- Если finish не выполнился, ошибка доходит до фасада как
  `CacheUnavailableError`; marker остаётся до TTL.
- К моменту истечения TTL PostgreSQL уже обязан иметь однозначный результат.
  Если commit состоялся, работать может только новый пароль; если rollback —
  старый. Сессии со старым epoch остаются отозванными в обоих случаях.
- При ошибке тела context manager cleanup остаётся best effort и не маскирует
  исходную PostgreSQL-ошибку.

## Проверка

Нужны отдельные тесты на stale expected epoch, busy marker, неизменность Redis
при отказе begin, проброс finish error, сохранение PostgreSQL error, wiring
таймаутов и конфигурационный инвариант. Полный Docker и HTTP lifecycle остаются
финальными gate.
