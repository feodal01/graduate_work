# План реализации отзыва сессий при смене пароля

> Этот исходный план дополнен race-hardening планом
> `2026-09-01-password-change-race-hardening.md`. Актуальный интерфейс использует
> `password_change(user_id, expected_epoch)` и ограниченное marker TTL окно.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Добавить Redis `session_epoch` и mutation marker, чтобы смена пароля немедленно отзывала все access- и refresh-токены пользователя.

**Architecture:** Auth подписывает текущий Redis epoch пользователя в каждом JWT. Зарегистрированные Lua-скрипты атомарно проверяют epoch и marker при выпуске, access-проверке и refresh-ротации; смена пароля устанавливает промежуточный epoch и marker, обновляет PostgreSQL, а при успехе публикует финальный epoch и снимает marker по идентификатору владельца.

**Tech Stack:** Python 3.14, FastAPI, PyJWT, redis-py asyncio, Redis 7, PostgreSQL, SQLAlchemy, pytest, pytest-asyncio.

**Spec:** `services/auth/docs/superpowers/specs/2026-09-01-password-change-session-revocation-design.md`

## Global Constraints

- `session_epoch` — обязательный подписанный UUID claim для access- и refresh-JWT.
- Отсутствующий epoch, активный marker, несовпадающий epoch и Redis error обрабатываются fail-closed; старые токены не получают fallback.
- Смена только логина не меняет epoch.
- Begin password mutation атомарно устанавливает промежуточный epoch и marker до изменения PostgreSQL; успешный finish ещё раз меняет epoch перед снятием marker.
- Ошибка PostgreSQL не восстанавливает старый epoch; все прежние сессии остаются отозванными.
- Marker имеет UUID владельца и TTL 30 секунд; password validation и hashing выполняются до его установки.
- Все Redis-ключи одной пользовательской сессии используют hash tag `{auth-session:<user_id>}`.
- Redis Lua вызывается через `register_script`; raw `EVAL` не используется.
- Старые JWT без `session_epoch` намеренно перестают работать при выкладке; compatibility fallback не добавляется.
- Старые ключи `refresh:<jti>` и `revoked-access:<jti>` не сканируются и не удаляются — они истекают по TTL.
- HTTP-схемы и TTL токенов не меняются: access 900 секунд, refresh 604800 секунд.
- Работа выполняется в текущем `main`. Существующий незакоммиченный пробел в `services/auth/src/services/authentication.py` не удалять молча и не включать в отдельный рефакторинг.
- Не добавлять CI, Compose-сервисы, новый endpoint управления устройствами или PostgreSQL-миграцию.
- Не коммитить и не отправлять изменения без отдельного запроса пользователя.

---

### Task 1: Обязательный session epoch в JWT

**Files:**
- Modify: `services/auth/src/security/tokens.py:20-118`
- Test: `services/auth/tests/test_tokens.py`

**Interfaces:**
- Produces: `TokenClaims.session_epoch: UUID`.
- Produces: `TokenService.issue_pair(user_id: UUID, session_epoch: UUID) -> IssuedTokenPair`.
- Preserves: строгую проверку `sub`, `jti`, `type`, `iat`, `exp`, подписи, типа токена и TTL.

- [ ] **Step 1: Написать падающий тест round-trip для epoch**

Добавить в `test_issued_pair_contains_required_typed_claims` явный epoch:

```python
session_epoch = uuid4()
pair = token_service.issue_pair(user_id, session_epoch)

access = token_service.decode(pair.access.value, 'access')
refresh = token_service.decode(pair.refresh.value, 'refresh')
assert access.session_epoch == session_epoch
assert refresh.session_epoch == session_epoch
```

- [ ] **Step 2: Добавить падающие тесты строгого claim-контракта**

Подписать вручную токены без `session_epoch`, с не-UUID строкой и с `None` и
проверить `InvalidTokenError`:

```python
@pytest.mark.parametrize('session_epoch', [None, 'not-a-uuid'])
def test_missing_or_invalid_session_epoch_is_rejected(
    token_service: TokenService,
    session_epoch: str | None,
) -> None:
    now = datetime.now(UTC)
    payload: dict[str, str | datetime] = {
        'sub': str(uuid4()),
        'jti': str(uuid4()),
        'type': 'access',
        'iat': now,
        'exp': now + timedelta(minutes=5),
    }
    if session_epoch is not None:
        payload['session_epoch'] = session_epoch
    token = jwt.encode(payload, SECRET, algorithm='HS256')

    with pytest.raises(InvalidTokenError):
        token_service.decode(token, 'access')
```

- [ ] **Step 3: Запустить token tests и зафиксировать ожидаемое падение**

Run:

```bash
uv --directory services/auth run pytest tests/test_tokens.py -q
```

Expected: FAIL из-за отсутствующей сигнатуры `issue_pair(..., session_epoch)` и
поля `TokenClaims.session_epoch`.

- [ ] **Step 4: Расширить TokenClaims и выпуск пары**

Реализовать единое значение epoch для двух токенов пары:

```python
@dataclass(frozen=True, slots=True)
class TokenClaims:
    user_id: UUID
    token_id: UUID
    token_type: TokenType
    session_epoch: UUID
    issued_at: datetime
    expires_at: datetime


def issue_pair(
    self,
    user_id: UUID,
    session_epoch: UUID,
) -> IssuedTokenPair:
    issued_at = datetime.now(UTC).replace(microsecond=0)
    return IssuedTokenPair(
        access=self._issue(
            user_id,
            'access',
            session_epoch,
            issued_at,
            self._access_ttl,
        ),
        refresh=self._issue(
            user_id,
            'refresh',
            session_epoch,
            issued_at,
            self._refresh_ttl,
        ),
    )
```

- [ ] **Step 5: Добавить claim в encode/decode и список обязательных полей**

```python
options = {'require': ['sub', 'jti', 'type', 'session_epoch', 'iat', 'exp']}

session_epoch = UUID(payload['session_epoch'])

{
    'sub': str(claims.user_id),
    'jti': str(claims.token_id),
    'type': claims.token_type,
    'session_epoch': str(claims.session_epoch),
    'iat': claims.issued_at,
    'exp': claims.expires_at,
}
```

- [ ] **Step 6: Обновить все прямые вызовы issue_pair в token tests**

Каждый тест создаёт явный `uuid4()` epoch; не добавлять default, потому что он
скрыл бы выпуск токена без состояния Redis.

- [ ] **Step 7: Повторно запустить token tests**

Run:

```bash
uv --directory services/auth run pytest tests/test_tokens.py -q
```

Expected: PASS.

---

### Task 2: Redis epoch, marker и атомарные token-session операции

**Files:**
- Create: `services/auth/src/db/cache/session_state.py`
- Modify: `services/auth/src/db/cache/session.py:1-126`
- Modify: `services/auth/src/db/cache/refresh_tokens.py:1-67`
- Modify: `services/auth/src/db/cache/access_token_denylist.py:1-27`
- Test: `services/auth/tests/test_session_store.py`

**Interfaces:**
- Produces: `SessionKeyspace(namespace: str = 'auth-session')`.
- Produces: `RedisSessionStateStore.get_or_create_epoch(user_id: UUID) -> UUID | None`.
- Produces: `RedisSessionStateStore.password_change(user_id: UUID) -> AbstractAsyncContextManager[None]`.
- Produces: `TokenSessionStore.get_or_create_epoch(user_id: UUID) -> UUID`.
- Changes: `store_refresh(..., session_epoch: UUID) -> bool`.
- Changes: `rotate_refresh(..., session_epoch: UUID) -> bool`.
- Replaces: `is_access_revoked(token_id)` with `is_access_valid(user_id, token_id, session_epoch) -> bool`.
- Changes: `revoke_session(..., user_id: UUID, ...) -> None`.

Зафиксировать публичный protocol целиком, чтобы сервис, Redis facade и fake не
разошлись в именах и порядке параметров:

```python
class TokenSessionStore(Protocol):
    async def get_or_create_epoch(self, user_id: UUID) -> UUID: ...

    async def store_refresh(
        self,
        token_id: UUID,
        user_id: UUID,
        session_epoch: UUID,
        ttl: int,
    ) -> bool: ...

    async def rotate_refresh(
        self,
        *,
        old_token_id: UUID,
        new_token_id: UUID,
        user_id: UUID,
        session_epoch: UUID,
        ttl: int,
    ) -> bool: ...

    async def is_access_valid(
        self,
        *,
        user_id: UUID,
        token_id: UUID,
        session_epoch: UUID,
    ) -> bool: ...

    def password_change(
        self,
        user_id: UUID,
    ) -> AbstractAsyncContextManager[None]: ...

    async def revoke_session(
        self,
        *,
        user_id: UUID,
        refresh_token_id: UUID | None,
        access_token_id: UUID,
        access_ttl: int,
    ) -> None: ...
```

- [ ] **Step 1: Написать падающие Redis keyspace и epoch tests**

Проверить стабильный epoch, общий hash tag и отсутствие TTL у epoch:

```python
store = RedisTokenSessionStore(redis_client, namespace=namespace)
user_id = uuid4()

first = await store.get_or_create_epoch(user_id)
second = await store.get_or_create_epoch(user_id)

assert first == second
assert await redis_client.ttl(f'{{{namespace}:{user_id}}}:epoch') == -1
```

- [ ] **Step 2: Написать падающий тест password-change context**

Проверить, что epoch меняется до тела context manager, выпуск сессии во время
marker отклоняется, а marker удаляется после выхода:

```python
old_epoch = await store.get_or_create_epoch(user_id)

async with store.password_change(user_id):
    new_epoch = await redis_client.get(epoch_key)
    assert new_epoch.decode() != str(old_epoch)
    assert await redis_client.get(marker_key) is not None
    assert await store.store_refresh(uuid4(), user_id, old_epoch, ttl=30) is False

assert await redis_client.get(marker_key) is None
```

- [ ] **Step 3: Написать падающие тесты отказа и восстановления marker**

Покрыть четыре случая:

```python
with pytest.raises(RuntimeError):
    async with store.password_change(user_id):
        raise RuntimeError('database failed')

assert await store.get_or_create_epoch(user_id) != old_epoch
assert await redis_client.get(marker_key) is None
```

Дополнительно проверить: второй concurrent context получает
`CacheUnavailableError`; чужой owner не удаляет marker; ошибка finish оставляет
marker до TTL, но новый epoch сохраняется; успешный finish отзывает сессию,
успевшую получить промежуточный epoch после истечения marker.

- [ ] **Step 4: Написать падающие тесты условного store/rotate/access**

Проверить следующие матрицы:

- правильный epoch + нет marker → store/rotate/access разрешены;
- неправильный epoch → все три операции отклонены;
- marker существует → все три операции отклонены;
- отсутствует epoch → access и refresh отклонены;
- access JTI в denylist → access отклонён при правильном epoch;
- два конкурентных refresh по-прежнему имеют ровно одного победителя;
- password-change и refresh-race не оставляют пригодный old-epoch refresh.
- logout одной refresh/access-пары не меняет epoch пользователя, удаляет только
  указанный refresh и не затрагивает вторую пару того же пользователя.

- [ ] **Step 5: Запустить session-store tests и подтвердить падение интерфейсов**

Run:

```bash
uv --directory services/auth run pytest tests/test_session_store.py -q
```

Expected: FAIL из-за отсутствующих `SessionKeyspace`, epoch API и новых
параметров `session_epoch`/`user_id`.

- [ ] **Step 6: Создать SessionKeyspace**

В `session_state.py` определить все ключи через один пользовательский hash tag:

```python
@dataclass(frozen=True, slots=True)
class SessionKeyspace:
    namespace: str = 'auth-session'

    def prefix(self, user_id: UUID) -> str:
        return f'{{{self.namespace}:{user_id}}}'

    def epoch(self, user_id: UUID) -> str:
        return f'{self.prefix(user_id)}:epoch'

    def mutation_marker(self, user_id: UUID) -> str:
        return f'{self.prefix(user_id)}:changing'

    def refresh(self, user_id: UUID, token_id: UUID) -> str:
        return f'{self.prefix(user_id)}:refresh:{token_id}'

    def revoked_access(self, user_id: UUID, token_id: UUID) -> str:
        return f'{self.prefix(user_id)}:revoked-access:{token_id}'
```

- [ ] **Step 7: Реализовать get-or-create и password mutation через registered Lua**

Begin-скрипт одной операцией получает marker и меняет epoch:

```lua
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[3]) then
    redis.call('SET', KEYS[2], ARGV[2])
    return 1
end
return 0
```

Finish-скрипт при успешном PostgreSQL UPDATE всегда публикует финальный epoch,
но удаляет только marker своего владельца. Это закрывает окно медленного UPDATE
и не снимает marker более новой конкурентной операции. При ошибке PostgreSQL
промежуточный epoch остаётся, а marker только удаляется:

```lua
if ARGV[3] == '1' then
    redis.call('SET', KEYS[2], ARGV[2])
end
if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('DEL', KEYS[1])
    return 1
end
if redis.call('EXISTS', KEYS[1]) == 0 then
    return 1
end
return 0
```

`get_or_create_epoch` проверяет отсутствие marker, создаёт epoch из UUID
candidate через `SET NX` и возвращает фактически сохранённое значение.

- [ ] **Step 8: Сделать refresh store и rotation epoch-aware**

Изменить ключи на `SessionKeyspace.refresh(user_id, token_id)`. Store-скрипт
проверяет marker и epoch до `SET EX`; rotate-скрипт выполняет те же проверки до
существующей проверки владельца и одноразовой замены JTI:

```lua
if redis.call('EXISTS', KEYS[2]) == 1
    or redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end
```

Сохранить идемпотентный retry: повтор с тем же `new_token_id` возвращает успех,
но replay в другой `new_token_id` возвращает отказ.

- [ ] **Step 9: Сделать access validation и logout атомарными в пользовательском slot**

`is_access_valid` одним Lua-скриптом проверяет epoch, marker и denylist:

```lua
if redis.call('GET', KEYS[1]) ~= ARGV[1]
    or redis.call('EXISTS', KEYS[2]) == 1
    or redis.call('EXISTS', KEYS[3]) == 1 then
    return 0
end
return 1
```

`revoke_session` получает `user_id` и транзакционным pipeline удаляет refresh и
добавляет access JTI в denylist с прежним TTL.

- [ ] **Step 10: Перевести внутренние Redis ошибки в существующий CacheUnavailableError**

Фасад повторяет RBAC-паттерн: ошибка захвата marker и `RedisError` до `yield`
дают `CacheUnavailableError`; finish error логируется и не подменяет уже
выполненную PostgreSQL-операцию. Epoch уже сменён, поэтому старые токены не
восстанавливаются после TTL marker.

- [ ] **Step 11: Повторно запустить Redis session tests**

Run:

```bash
uv --directory services/auth run pytest tests/test_session_store.py -q
```

Expected: PASS, включая retry/backoff, idempotence, concurrency, marker ownership
и запрет raw `EVAL`.

---

### Task 3: Epoch-aware login, social login, refresh, access и logout

**Files:**
- Modify: `services/auth/src/repositories/users.py:14-135`
- Modify: `services/auth/src/services/authentication.py:29-215`
- Modify: `services/auth/src/services/errors.py:64-89`
- Modify: `services/auth/tests/fakes.py:31-89,297-349`
- Modify: `services/auth/tests/test_repositories.py`
- Modify: `services/auth/tests/test_authentication_service.py`
- Modify: `services/auth/tests/test_social_authentication_service.py`

**Interfaces:**
- Produces: `UserRepository.get_id_by_login(login: str) -> UUID | None`.
- Changes: `AuthenticationService.issue_session(..., expected_epoch: UUID | None = None)`.
- Consumes: epoch-aware `TokenSessionStore` from Task 2.
- Preserves: существующие HTTP-ошибки `Invalid credentials`, `Token revoked`, `Invalid refresh token`.

- [ ] **Step 1: Написать падающий repository test для ID-only lookup**

```python
user = await repository.create(
    login='Alice',
    password_hash='hash',
    first_name=None,
    last_name=None,
)

assert await repository.get_id_by_login('Alice') == user.id
assert await repository.get_id_by_login('Missing') is None
```

- [ ] **Step 2: Реализовать get_id_by_login без загрузки password hash**

Добавить метод в protocol, SQLAlchemy repository и fake:

```python
async def _get_id_by_login(self, login: str) -> UUID | None:
    result = await self._session.execute(select(User.id).where(User.login == login))
    return result.scalar_one_or_none()
```

Использовать тот же PostgreSQL backoff/rollback и `DatabaseUnavailableError`,
что у `get_by_login`.

- [ ] **Step 3: Написать падающие service tests для epoch-aware выпуска**

Проверить:

```python
pair = await service.login(
    login='Alice',
    password='correct horse',
    client=UNKNOWN_CLIENT,
)

assert pair.access.claims.session_epoch == pair.refresh.claims.session_epoch
assert pair.access.claims.session_epoch == token_sessions.epochs[user.id]
```

Дополнительно проверить, что условный store failure не возвращает пару, refresh
с неправильным epoch получает `InvalidRefreshTokenError`, access с неправильным
epoch получает `AccessTokenRevokedError`, а logout записывает denylist в keyspace
нужного пользователя.

- [ ] **Step 4: Защитить password login от конкурентной смены пароля**

Перестроить login в следующем порядке:

```python
user_id = await self._users.get_id_by_login(login)
if user_id is None:
    raise InvalidCredentialsError

session_epoch = await self._token_sessions.get_or_create_epoch(user_id)
user = await self._users.get_by_id(user_id)
if (
    user is None
    or user.login != login
    or user.password_hash is None
    or not self._password_hasher.verify(password, user.password_hash)
):
    raise InvalidCredentialsError

return await self.issue_session(
    user=user,
    client=client,
    expected_epoch=session_epoch,
)
```

Так password hash загружается после epoch. Conditional refresh store из Task 2
не позволяет завершить вход, если marker/epoch изменились после этой проверки.

- [ ] **Step 5: Выпускать JWT только с подтверждённым epoch**

```python
session_epoch = expected_epoch
if session_epoch is None:
    session_epoch = await self._token_sessions.get_or_create_epoch(user.id)

pair = self._token_service.issue_pair(user.id, session_epoch)
stored = await self._token_sessions.store_refresh(
    pair.refresh.claims.token_id,
    user.id,
    session_epoch,
    self._remaining_ttl(pair.refresh.claims.expires_at),
)
if not stored:
    raise CacheUnavailableError('Session state changed; retry authentication')
```

Сохранить запись login history и обновить event assertions социального входа,
не логируя raw JWT или epoch owner marker.

- [ ] **Step 6: Передавать epoch через refresh rotation**

Новая пара наследует epoch старого refresh, а Lua подтверждает, что он всё ещё
текущий:

```python
pair = self._token_service.issue_pair(
    claims.user_id,
    claims.session_epoch,
)
rotated = await self._token_sessions.rotate_refresh(
    old_token_id=claims.token_id,
    new_token_id=pair.refresh.claims.token_id,
    user_id=claims.user_id,
    session_epoch=claims.session_epoch,
    ttl=self._remaining_ttl(pair.refresh.claims.expires_at),
)
```

- [ ] **Step 7: Заменить denylist-only access check общей session validation**

```python
valid = await self._token_sessions.is_access_valid(
    user_id=claims.user_id,
    token_id=claims.token_id,
    session_epoch=claims.session_epoch,
)
if not valid:
    logger.warning(
        'Revoked access token used: user_id=%s token_id=%s ip=%s',
        claims.user_id,
        claims.token_id,
        client.ip_address,
    )
    raise AccessTokenRevokedError
```

Обновить docstring `AccessTokenRevokedError`: отзыв включает denylist,
несовпадающий epoch и активную session mutation.

- [ ] **Step 8: Передать user_id при logout и обновить fake**

`revoke_session` получает `current_user.user_id`. `InMemoryTokenSessionStore`
хранит `epochs`, `markers`, использует `asyncio.Lock` для условного store/rotate
и предоставляет async context manager password mutation с той же семантикой,
что Redis implementation.

Добавить отдельный service test с двумя сессиями одного пользователя:

```python
epoch_before = first_pair.access.claims.session_epoch
await service.logout(first_current, first_pair.refresh.value)

with pytest.raises(AccessTokenRevokedError):
    await service.authenticate_access(
        first_pair.access.value,
        client=UNKNOWN_CLIENT,
    )
with pytest.raises(InvalidRefreshTokenError):
    await service.refresh(first_pair.refresh.value)

second_current = await service.authenticate_access(
    second_pair.access.value,
    client=UNKNOWN_CLIENT,
)
second_rotated = await service.refresh(second_pair.refresh.value)

assert second_current.user_id == first_current.user_id
assert second_rotated.refresh.claims.session_epoch == epoch_before
assert token_sessions.epochs[first_current.user_id] == epoch_before
```

Сохранить существующий тест неверного/чужого refresh: он должен отзывать access
вызывающего пользователя, не удаляя чужой refresh и не меняя epoch ни одного
пользователя.

- [ ] **Step 9: Запустить repository, authentication и social tests**

Run:

```bash
uv --directory services/auth run pytest \
  tests/test_repositories.py \
  tests/test_authentication_service.py \
  tests/test_social_authentication_service.py -q
```

Expected: PASS.

---

### Task 4: Отзыв всех сессий в update_profile

**Files:**
- Modify: `services/auth/src/services/authentication.py:155-181`
- Modify: `services/auth/tests/fakes.py:31-89,297-349`
- Modify: `services/auth/tests/test_authentication_service.py:450-493`
- Modify: `services/auth/tests/test_authentication_api.py:385-411`

**Interfaces:**
- Consumes: `TokenSessionStore.password_change(user_id)` из Task 2.
- Preserves: `PATCH /api/v1/auth/profile` response model и статус `200`.
- Produces: после ответа PATCH все ранее выданные токены пользователя отклоняются.

- [ ] **Step 1: Написать падающий service test с двумя сессиями**

Выпустить две пары до смены пароля, выполнить PATCH workflow первой парой и
проверить обе пары:

```python
await service.update_profile(
    current,
    login=None,
    password='new correct horse',
)

for pair in (first_pair, second_pair):
    with pytest.raises(AccessTokenRevokedError):
        await service.authenticate_access(pair.access.value, client=UNKNOWN_CLIENT)
    with pytest.raises(InvalidRefreshTokenError):
        await service.refresh(pair.refresh.value)
```

- [ ] **Step 2: Написать падающий тест, что login-only update не отзывает пары**

После `update_profile(..., login='AliceUpdated', password=None)` обе старые пары
должны сохранять текущий epoch; access продолжает работать, refresh каждой пары
можно ротировать ровно один раз.

- [ ] **Step 3: Написать тесты failure semantics password mutation**

Проверить:

- Redis marker acquisition failure не вызывает `users.update`;
- repository update failure оставляет новый epoch и отзывает старые токены;
- marker отсутствует после обычной ошибки тела context manager;
- повторный вход после DB failure работает со старым паролем и новым epoch;
- после успешной смены старый пароль отвергается, новый создаёт рабочую пару.

- [ ] **Step 4: Запустить focused service tests и подтвердить уязвимое поведение**

Run:

```bash
uv --directory services/auth run pytest \
  tests/test_authentication_service.py -q
```

Expected: новые тесты FAIL, потому что `update_profile` пока не вызывает
`password_change`.

- [ ] **Step 5: Обернуть только password update в mutation context**

Validation и hashing оставить до marker; login-only сохранить без Redis
mutation:

```python
if password_hash is None:
    return await self._users.update(
        user,
        login=valid_login,
        password_hash=None,
    )

async with self._token_sessions.password_change(user.id):
    return await self._users.update(
        user,
        login=valid_login,
        password_hash=password_hash,
    )
```

- [ ] **Step 6: Расширить HTTP test контракта PATCH**

После успешного PATCH проверить старый access через `GET /profile`, старый
refresh через `POST /refresh`, старый пароль и новый login/password:

```python
revoked_access = await client.get(
    '/api/v1/auth/profile',
    headers=bearer(pair['access_token']),
)
revoked_refresh = await client.post(
    '/api/v1/auth/refresh',
    json={'refresh_token': pair['refresh_token']},
)

assert revoked_access.status_code == 401
assert revoked_access.json() == {'detail': 'Token revoked'}
assert revoked_refresh.status_code == 401
assert revoked_refresh.json() == {'detail': 'Invalid refresh token'}
```

- [ ] **Step 7: Запустить service и API tests**

Run:

```bash
uv --directory services/auth run pytest \
  tests/test_authentication_service.py \
  tests/test_authentication_api.py -q
```

Expected: PASS.

---

### Task 5: Функциональный сценарий, документация и полная проверка

**Files:**
- Modify: `services/auth/functional_tests/test_auth_api.py:25-129`
- Modify: `services/auth/functional_tests/README.md`
- Modify: `services/auth/README.md`
- Verify: `services/auth/src/main.py`
- Verify: `services/auth/src/dependencies.py`

**Interfaces:**
- Documents: Redis epoch, marker, one-time deployment logout и поведение клиента после смены пароля.
- Verifies: реальный HTTP + PostgreSQL + Redis без dependency overrides.

- [ ] **Step 1: Перестроить functional lifecycle после password PATCH**

Сразу после смены пароля проверить отказ старой пары и старого пароля. Затем
сохранить новую пару из `updated_login_response` и продолжить существующие
refresh replay/logout проверки уже с ней:

```python
old_refresh = await http_client.post(
    '/api/v1/auth/refresh',
    json={'refresh_token': first_tokens['refresh_token']},
)
_assert_status(old_refresh, 401)

old_access = await http_client.get(
    '/api/v1/auth/login-history',
    headers=bearer,
)
_assert_status(old_access, 401)

old_password = await http_client.post(
    '/api/v1/auth/login',
    json={'login': updated_login, 'password': password},
)
_assert_status(old_password, 401)

current_tokens = updated_login_response.json()
```

- [ ] **Step 2: Запустить отдельный функциональный authentication test**

Run from `services/auth`:

```bash
./functional_tests/run.sh \
  functional_tests/test_auth_api.py::test_complete_authentication_lifecycle
```

Expected: PASS; контейнеры автоматически очищены скриптом.

- [ ] **Step 3: Обновить русскую документацию Auth**

В `services/auth/README.md` и functional README кратко зафиксировать:

- `session_epoch` находится в JWT и Redis;
- смена пароля отзывает все устройства, включая текущий access;
- клиент после `200 PATCH /profile` с новым паролем должен перейти к login;
- Redis failure не допускает password mutation или token validation;
- выкладка обязательного claim один раз завершает старые сессии.

- [ ] **Step 4: Проверить wiring без добавления новой FastAPI dependency**

Убедиться, что `RedisTokenSessionStore(redis)` в lifespan использует безопасные
defaults `namespace='auth-session'` и `marker_ttl_seconds=30`; отдельный app state
или endpoint не создаётся.

- [ ] **Step 5: Запустить полный набор unit/integration tests Auth**

Run:

```bash
uv --directory services/auth run pytest -q
```

Expected: PASS при доступных test PostgreSQL/Redis. Если host dependencies не
запущены, не смешивать infrastructure failure с регрессией и использовать
изолированный Docker gate следующим шагом.

- [ ] **Step 6: Запустить полный изолированный Docker gate**

Run from `services/auth`:

```bash
./functional_tests/run.sh
```

Expected: PASS всех HTTP-сценариев Auth с миграциями, PostgreSQL и Redis.

- [ ] **Step 7: Запустить статические проверки**

Run:

```bash
uv --directory services/auth run ruff check src tests functional_tests
uv --directory services/auth run python -m compileall -q src tests functional_tests
uv --directory services/auth run --with pyright pyright src tests
uv --directory services/auth lock --check
git diff --check
```

Expected: все команды PASS. Если `git diff --check` указывает только на
существовавший до реализации пробел в `authentication.py`, остановиться и
попросить разрешение на его удаление вместо молчаливого изменения пользовательского
diff.

- [ ] **Step 8: Провести финальную security-проверку поведения**

Проверить по тестам и diff следующие утверждения:

- старый access не переживает password change;
- старый refresh нельзя ротировать ни до, ни после снятия marker;
- DB failure не возвращает старый epoch;
- истечение marker во время медленного UPDATE не оставляет сессию с
  промежуточным epoch активной после успешной смены пароля;
- Redis failure работает fail-closed;
- login-only update не отзывает сессии;
- social login выпускает epoch-aware пару;
- в логах отсутствуют raw JWT, epoch owner token и пароли;
- реализация не содержит `SCAN`, raw `EVAL` и legacy claim fallback.

Описать оставшийся эксплуатационный компромисс: авария процесса после begin
может временно блокировать новый вход до 30 секунд, но не возвращает старые
сессии к жизни.
