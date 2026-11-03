# Password Change Race Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Закрыть две найденные гонки Redis `session_epoch` без PostgreSQL-проверки каждой сессии и без миграции.

**Architecture:** Begin password mutation выполняет CAS ожидаемого epoch access-токена. PostgreSQL engine получает server-side и client-side timeouts, а TTL marker конфигурационно обязан превышать полное критическое окно с запасом; Redis finish errors больше не подавляются после успешного commit.

**Tech Stack:** Python 3.14, FastAPI, SQLAlchemy asyncio, asyncpg, Redis Lua, pytest, Docker Compose.

**Spec:** `services/auth/docs/superpowers/specs/2026-09-01-password-change-race-hardening-design.md`

## Global Constraints

- Access и refresh не читают PostgreSQL для проверки `session_epoch`.
- Defaults: connect `5s`, pool `10s`, command `15s`, statement `15000ms`, marker `120s`.
- Marker TTL имеет минимум 30 секунд запаса после вычисленной верхней границы PostgreSQL.
- Stale password-changing access возвращает `401 Token revoked`; busy marker и Redis failure возвращают `503`.
- Login-only update не меняет epoch; logout остаётся per-session; social login сохраняет общий `issue_session`.
- Не добавлять PostgreSQL-миграцию, новый endpoint, CI или Compose-сервис.
- Работать тест-first; commit/push выполнять только после merged-tree verification.

---

### Task 1: Конфигурационно ограничить PostgreSQL раньше marker TTL

**Files:**
- Modify: `services/auth/src/core/config.py`
- Modify: `services/auth/src/db/sql/postgres.py`
- Modify: `services/auth/src/main.py`
- Modify: `services/auth/src/cli.py`
- Modify: `services/auth/.env.example`
- Test: `services/auth/tests/test_config.py`
- Test: `services/auth/tests/test_sql_database.py`
- Test: `services/auth/tests/test_lifespan.py`

**Interfaces:**
- Produces: `PostgresSettings.connect_timeout_seconds: float`.
- Produces: `PostgresSettings.pool_timeout_seconds: float`.
- Produces: `PostgresSettings.command_timeout_seconds: float`.
- Produces: `PostgresSettings.statement_timeout_ms: int`.
- Produces: `SessionSettings.password_change_marker_ttl_seconds: int`.
- Changes: `PostgresDatabase(dsn, *, connect_timeout_seconds, pool_timeout_seconds, command_timeout_seconds, statement_timeout_ms)`.

- [ ] **Step 1: Написать падающие config tests**

Проверить defaults `5/10/15/15000/120` и отклонение конфигурации, где marker
меньше `pool + connect + 2 * command + 30`.

- [ ] **Step 2: Запустить config tests и подтвердить RED**

Run: `uv --directory services/auth run pytest tests/test_config.py -q`

Expected: FAIL, потому что timeout/session settings ещё отсутствуют.

- [ ] **Step 3: Добавить settings и cross-field validator**

Добавить `SessionSettings` с env prefix `SESSION_`; в `Settings` валидировать
временное неравенство из spec. Значения должны быть положительными.

- [ ] **Step 4: Написать падающий engine wiring test**

Ожидать вызов:

```python
create_async_engine(
    dsn,
    echo=False,
    future=True,
    pool_timeout=10.0,
    connect_args={
        'timeout': 5.0,
        'command_timeout': 15.0,
        'server_settings': {'statement_timeout': '15000'},
    },
)
```

- [ ] **Step 5: Реализовать wiring в lifespan и CLI**

Передать настройки в `PostgresDatabase` и
`RedisTokenSessionStore(..., marker_ttl_seconds=120)`; defaults конструктора
оставить безопасными для прямых тестовых вызовов.

- [ ] **Step 6: Обновить env example и focused tests**

Run:

```bash
uv --directory services/auth run pytest \
  tests/test_config.py \
  tests/test_sql_database.py \
  tests/test_lifespan.py -q
```

Expected: PASS.

---

### Task 2: Привязать password change к epoch access-токена

**Files:**
- Modify: `services/auth/src/services/types.py`
- Modify: `services/auth/src/services/authentication.py`
- Modify: `services/auth/src/db/cache/session.py`
- Modify: `services/auth/src/db/cache/session_state.py`
- Modify: `services/auth/tests/fakes.py`
- Test: `services/auth/tests/test_session_store.py`
- Test: `services/auth/tests/test_authentication_service.py`
- Test: `services/auth/tests/test_authentication_api.py`
- Test: `services/auth/tests/test_authorization_service.py`

**Interfaces:**
- Produces: `CurrentUser.session_epoch: UUID`.
- Changes: `TokenSessionStore.password_change(user_id, expected_epoch)`.
- Produces: internal `SessionEpochMismatchError` for stale CAS.

- [ ] **Step 1: Написать падающий Redis CAS test**

Создать E0, изменить Redis epoch на E2 и вызвать password context с E0. Ожидать
stale error и проверить, что marker/epoch не изменились.

- [ ] **Step 2: Написать падающий service race test**

Получить два `CurrentUser` с E0; первой сменой пароля получить E2; вторая смена
с устаревшим principal должна дать `AccessTokenRevokedError`, не вызвать второй
repository update и не изменить Redis state.

- [ ] **Step 3: Запустить focused tests и подтвердить RED**

Run:

```bash
uv --directory services/auth run pytest \
  tests/test_session_store.py \
  tests/test_authentication_service.py -q
```

Expected: FAIL из-за отсутствующих `CurrentUser.session_epoch` и CAS.

- [ ] **Step 4: Реализовать атомарный begin outcome**

Begin Lua сначала сравнивает `GET epoch` с `expected_epoch`, затем пытается
получить marker и только при успехе публикует промежуточный epoch. Разделить
исходы `stale`, `busy`, `acquired`; ни один failure path не меняет Redis.

- [ ] **Step 5: Протянуть epoch через service и fake**

`authenticate_access()` записывает claim в `CurrentUser`; password update
передаёт его в context manager. Stale переводится в `AccessTokenRevokedError`,
busy/Redis error — в `CacheUnavailableError`. Обновить все прямые constructors.

- [ ] **Step 6: Добавить HTTP regression**

PATCH, начатый со stale access principal, возвращает `401` с
`{"detail":"Token revoked"}`; login-only и обычный password change сохраняют
существующие контракты.

- [ ] **Step 7: Повторно запустить focused tests**

Expected: PASS.

---

### Task 3: Сделать Redis finish fail-closed и не маскировать DB error

**Files:**
- Modify: `services/auth/src/db/cache/session_state.py`
- Modify: `services/auth/src/db/cache/session.py`
- Test: `services/auth/tests/test_session_store.py`
- Test: `services/auth/tests/test_authentication_service.py`

**Interfaces:**
- Preserves: body error is re-raised unchanged after best-effort cleanup.
- Produces: finish failure after successful body becomes `CacheUnavailableError`.
- Produces: internal `SessionMutationFinalizationError` for lost ownership.

- [ ] **Step 1: Изменить finish-failure test и подтвердить RED**

После успешного тела context manager ожидать `CacheUnavailableError`; marker
должен оставаться с положительным TTL, а старый epoch не должен возвращаться.

- [ ] **Step 2: Добавить DB-error precedence test**

Одновременно вызвать body `DatabaseUnavailableError` и cleanup Redis error.
Ожидать исходную DB error, сохранённый новый epoch и отсутствие восстановления
старых токенов.

- [ ] **Step 3: Реализовать раздельные success/error ветки context manager**

На success `_finish(..., succeeded=True)` обязан завершиться успешно; Redis
failure/потеря ownership преобразуются фасадом в `CacheUnavailableError`. На
body error выполнить best-effort `_finish(..., succeeded=False)`, залогировать
cleanup failure без raw token/owner и повторно поднять исходную ошибку.

- [ ] **Step 4: Запустить session/service tests**

Run:

```bash
uv --directory services/auth run pytest \
  tests/test_session_store.py \
  tests/test_authentication_service.py \
  tests/test_authentication_api.py -q
```

Expected: PASS.

---

### Task 4: Документация, merge и финальная проверка

**Files:**
- Modify: `services/auth/README.md`
- Modify: `services/auth/functional_tests/README.md`
- Verify: все изменённые production/tests files

**Interfaces:**
- Documents: временной инвариант PostgreSQL/marker и HTTP 401/503 semantics.
- Verifies: итоговое дерево после объединения с актуальным `origin/main`.

- [ ] **Step 1: Обновить документацию**

Зафиксировать значения timeout/TTL, CAS expected epoch и то, что access/refresh
не обращаются в PostgreSQL.

- [ ] **Step 2: Объединить актуальный origin/main без force/rebase**

Сохранить OAuth-изменения `origin/main` и session-epoch изменения, разрешив
пересечения в `services/errors.py` и social authentication tests.

- [ ] **Step 3: Запустить полный Docker pytest**

Run:

```bash
docker compose \
  --env-file services/auth/functional_tests/functional.env \
  -f services/auth/functional_tests/docker-compose.yaml \
  run --build --rm auth-tests
```

Expected: PASS всех tests.

- [ ] **Step 4: Запустить HTTP functional gate**

Run: `cd services/auth && ./functional_tests/run.sh`

Expected: PASS всех functional scenarios.

- [ ] **Step 5: Запустить статические проверки**

Run:

```bash
uv --directory services/auth run ruff check src tests functional_tests
uv --directory services/auth run python -m compileall -q src tests functional_tests
uv --directory services/auth run --with pyright pyright src
uv --directory services/auth lock --check
git diff --check
```

Expected: PASS.

- [ ] **Step 6: Финальный security review**

Проверить stale E0 race, marker/DB timeout inequality, finish failure, DB error,
login-only, social login, refresh replay и per-session logout. Только после
зелёного merged-tree review выполнить commit/push в `main` по явному разрешению
пользователя.
