# RBAC Cache Readability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Сделать чтение RBAC-кэша понятным за счёт предметных имён, именованного результата `MGET` и коротких русских комментариев без изменения поведения.

**Architecture:** `AuthorizationService` продолжает использовать cache-aside через единый `AuthorizationCache`. Redis-хранилище по-прежнему читает ревизию, два marker-ключа и JSON-снимок одним `MGET`, но позиционный результат сразу преобразуется в именованное состояние. Lua-скрипты, Redis-ключи и правила инвалидации не меняются.

**Tech Stack:** Python 3.12, Redis/redis-py asyncio, Pydantic 2, pytest, Pyright, Pylint.

## Global Constraints

- Работать в текущей ветке `main`.
- Сохранить Redis-ключи, JSON-поля, TTL и поведение Lua-скриптов.
- Все новые комментарии и docstring по RBAC-кэшу писать простым русским языком.
- Не комментировать очевидный синтаксис построчно.
- Не включать в коммиты посторонние изменения из грязного рабочего дерева.

---

### Task 1: Предметные имена публичного контракта снимков

**Files:**
- Modify: `services/auth/src/db/cache/authorization_snapshot.py`
- Modify: `services/auth/src/db/cache/authorization.py`
- Modify: `services/auth/src/services/authorization.py`
- Modify: `services/auth/src/services/superusers.py`
- Modify: `services/auth/tests/fakes.py`
- Modify: `services/auth/tests/test_authorization_cache.py`

**Interfaces:**
- Produces: `AuthorizationSnapshotReadResult(cached_snapshot: AuthorizationSnapshot | None, roles_revision_at_read: str)`.
- Produces: `AuthorizationCache.read_snapshot(user_id: UUID) -> AuthorizationSnapshotReadResult`.
- Produces: `AuthorizationCache.store_snapshot(user_id: UUID, snapshot: AuthorizationSnapshot, expected_roles_revision: str) -> None`.

- [x] **Step 1: Перевести тесты и fake-кэш на новые имена**

Заменить `get`, `set`, `AuthorizationCacheLookup`, `snapshot` и
`roles_revision` в контракте чтения на новые предметные имена. Проверки должны
выглядеть явно:

```python
read_result = await store.read_snapshot(user_id)
assert read_result.cached_snapshot is None
await store.store_snapshot(
    user_id,
    snapshot,
    read_result.roles_revision_at_read,
)
```

- [x] **Step 2: Запустить целевые тесты и подтвердить ожидаемое падение**

Run:

```bash
UV_CACHE_DIR=/tmp/auth-sprint-uv-cache uv run pytest \
  tests/test_authorization_cache.py tests/test_authorization_service.py -q
```

Expected: FAIL из-за отсутствующих новых имён в production-коде.

- [x] **Step 3: Переименовать production-контракт и все вызовы**

Новый результат чтения:

```python
@dataclass(frozen=True, slots=True)
class AuthorizationSnapshotReadResult:
    cached_snapshot: AuthorizationSnapshot | None
    roles_revision_at_read: str
```

В `AuthorizationService` использовать `cache_read`, `cached_snapshot` и
`roles_revision_at_read`, чтобы cache-aside поток читался без расшифровки
общих слов `lookup` и `revision`.

- [x] **Step 4: Запустить целевые тесты**

Run: команда из Step 2.

Expected: PASS.

### Task 2: Именованное состояние одного Redis MGET

**Files:**
- Modify: `services/auth/src/db/cache/authorization_snapshot.py`
- Test: `services/auth/tests/test_authorization_cache.py`

**Interfaces:**
- Produces: private `_RedisAuthorizationState` with named raw Redis values.
- Produces: private `_StoredAuthorizationSnapshot` Pydantic model.

- [x] **Step 1: Добавить именованную структуру результата Redis**

```python
@dataclass(frozen=True, slots=True)
class _RedisAuthorizationState:
    roles_revision: bytes | str | None
    roles_mutation_marker: bytes | str | None
    user_mutation_marker: bytes | str | None
    snapshot_payload: bytes | str | None

    @property
    def mutation_in_progress(self) -> bool:
        return (
            self.roles_mutation_marker is not None
            or self.user_mutation_marker is not None
        )
```

- [x] **Step 2: Распаковать MGET один раз рядом с порядком ключей**

```python
(
    roles_revision,
    roles_mutation_marker,
    user_mutation_marker,
    snapshot_payload,
) = await self._redis.mget(keys)
```

Вернуть `_RedisAuthorizationState`; при повторном чтении после инициализации
ревизии использовать тот же helper. В логике чтения не должно остаться
`result[0]`—`result[3]`.

- [x] **Step 3: Переименовать сериализуемую модель**

Переименовать `_AuthorizationCacheEnvelope` в
`_StoredAuthorizationSnapshot`. Добавить русский docstring о том, что модель
валидирует JSON из Redis и хранит ревизию, при которой снимок был вычислен.

- [x] **Step 4: Сделать поток read_snapshot последовательным**

Использовать локальные имена `redis_state`, `current_roles_revision`,
`stored_snapshot`. Добавить короткие комментарии перед проверкой marker-ключей
и сравнением ревизий. Каждая ветка промаха возвращает явно заполненный
`AuthorizationSnapshotReadResult`.

- [x] **Step 5: Проверить отсутствие индексного доступа и старого имени**

Run:

```bash
rg -n "result\[[0-3]\]|_AuthorizationCacheEnvelope" \
  src/db/cache/authorization_snapshot.py
```

Expected: no matches.

### Task 3: Русские комментарии по полному механизму RBAC-кэша

**Files:**
- Modify: `services/auth/src/db/cache/authorization.py`
- Modify: `services/auth/src/db/cache/authorization_snapshot.py`
- Modify: `services/auth/src/db/cache/authorization_mutations.py`
- Modify: `services/auth/src/services/authorization.py`
- Modify: `services/auth/src/services/superusers.py`

**Interfaces:**
- Consumes: names introduced in Tasks 1-2.
- Produces: no runtime interface changes.

- [x] **Step 1: Объяснить фасад и keyspace**

Добавить обзорные docstring: фасад переводит ошибки Redis в доменную ошибку;
keyspace использует общий hash tag для выполнения Lua над ключами одного Redis
Cluster slot; снимок хранит итоговые права пользователя.

- [x] **Step 2: Объяснить пользовательскую и ролевую мутации**

Указать перед `yield`, что marker уже установлен и управление передаётся SQL-
операции. В `finally` объяснить локальное удаление снимка пользователя либо
публикацию новой общей ревизии после успешной ролевой мутации. Объяснить token
владения и защитный TTL.

- [x] **Step 3: Объяснить cache-aside в AuthorizationService**

Разделить комментариями четыре фазы: попытка чтения Redis, fallback в
PostgreSQL, построение снимка, условное заполнение Redis той же наблюдавшейся
ревизией.

- [x] **Step 4: Запустить целевые тесты**

Run:

```bash
UV_CACHE_DIR=/tmp/auth-sprint-uv-cache uv run pytest \
  tests/test_authorization_cache.py \
  tests/test_authorization_service.py \
  tests/test_superusers.py -q
```

Expected: PASS.

### Task 4: Полная проверка

**Files:**
- Verify: `services/auth`

**Interfaces:**
- Consumes: completed Tasks 1-3.
- Produces: verified refactor with unchanged behavior.

- [x] **Step 1: Запустить полный pytest**

```bash
UV_CACHE_DIR=/tmp/auth-sprint-uv-cache uv run pytest -q
```

Expected: all tests pass.

- [x] **Step 2: Запустить статические проверки**

```bash
UV_CACHE_DIR=/tmp/auth-sprint-uv-cache uv run python -m compileall -q src tests
UV_CACHE_DIR=/tmp/auth-sprint-uv-cache uv run pyright
UV_CACHE_DIR=/tmp/auth-sprint-uv-cache uv run pylint \
  src/db/cache/authorization.py \
  src/db/cache/authorization_snapshot.py \
  src/db/cache/authorization_mutations.py \
  src/services/authorization.py \
  src/services/superusers.py
git diff --check
```

Expected: exit code 0, Pyright reports 0 errors, Pylint reports 10.00/10.

- [x] **Step 3: Проверить завершённость переименования**

```bash
rg -n "AuthorizationCacheLookup|_AuthorizationCacheEnvelope|\.get\(user_id\)|\.set\(user_id" \
  src/db/cache/authorization.py \
  src/db/cache/authorization_snapshot.py \
  src/services/authorization.py \
  tests/fakes.py \
  tests/test_authorization_cache.py
```

Expected: no stale authorization-cache names or calls.
