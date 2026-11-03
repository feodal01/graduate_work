# Redis Cache Decomposition Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split authorization caching and token sessions into focused domain components while retaining one stable facade per domain.

**Architecture:** `RedisAuthorizationCache` will compose a snapshot store plus separate user and role mutation components. `RedisTokenSessionStore` will compose a refresh-token store and an access-token denylist, while retaining logout transaction coordination in the facade.

**Tech Stack:** Python 3.14, redis-py asyncio, Pydantic v2, pytest, pytest-asyncio.

## Global Constraints

- Preserve `AuthorizationCache` and `TokenSessionStore` public protocols and all existing imports.
- Preserve Redis key names, stored values, TTL rules, backoff, and error semantics.
- Do not change FastAPI endpoints, dependency signatures, PostgreSQL code, or migrations.
- Do not add a universal configurable Redis key repository.
- Use `redis-py` registered scripts; do not introduce raw `EVAL` calls.
- Work in the current dirty `main`; do not stage or commit implementation files.

---

### Task 1: Decompose authorization caching behind its facade

**Files:**
- Create: `src/db/cache/authorization_snapshot.py`
- Create: `src/db/cache/authorization_mutations.py`
- Modify: `src/db/cache/authorization.py`
- Modify: `tests/test_authorization_cache.py`

**Interfaces:**
- Produces: `RedisAuthorizationSnapshotStore.get(user_id: UUID) -> AuthorizationCacheLookup`
- Produces: `RedisAuthorizationSnapshotStore.set(user_id: UUID, snapshot: AuthorizationSnapshot, roles_revision: str) -> None`
- Produces: `RedisUserAuthorizationMutation.mutate(user_id: UUID) -> AbstractAsyncContextManager[None]`
- Produces: `RedisRoleAuthorizationMutation.mutate() -> AbstractAsyncContextManager[None]`
- Preserves: `RedisAuthorizationCache.get`, `set`, `user_mutation`, and `roles_mutation`

- [ ] **Step 1: Add a failing behavior test for the snapshot component**

Add an import and a focused Redis test to `tests/test_authorization_cache.py`:

```python
from db.cache.authorization_snapshot import (
    AuthorizationKeyspace,
    AuthorizationSnapshot,
    RedisAuthorizationSnapshotStore,
)


@pytest.mark.asyncio
async def test_snapshot_component_round_trip(redis_client: Redis) -> None:
    namespace = f'authz-component-test-{uuid4()}'
    store = RedisAuthorizationSnapshotStore(
        redis_client,
        ttl_seconds=30,
        keys=AuthorizationKeyspace.create(namespace),
    )
    user_id = uuid4()
    snapshot = AuthorizationSnapshot(False, frozenset({'roles:manage'}))

    miss = await store.get(user_id)
    assert miss.roles_revision is not None
    await store.set(user_id, snapshot, miss.roles_revision)
    assert (await store.get(user_id)).snapshot == snapshot
```

- [ ] **Step 2: Run the structural test and confirm it fails**

Run:

```bash
env UV_CACHE_DIR=/tmp/auth-sprint-uv-cache uv run pytest \
  tests/test_authorization_cache.py::test_snapshot_component_round_trip -q
```

Expected: import failure because `authorization_snapshot` does not exist.

- [ ] **Step 3: Move snapshot concerns into `authorization_snapshot.py`**

Define and move the following units:

```python
@dataclass(frozen=True, slots=True)
class AuthorizationSnapshot:
    is_superuser: bool
    permissions: frozenset[str]


@dataclass(frozen=True, slots=True)
class AuthorizationCacheLookup:
    snapshot: AuthorizationSnapshot | None
    roles_revision: str | None


@dataclass(frozen=True, slots=True)
class AuthorizationKeyspace:
    prefix: str

    @classmethod
    def create(cls, namespace: str) -> 'AuthorizationKeyspace':
        return cls(prefix=f'{{{namespace}}}')

    @property
    def roles_revision(self) -> str:
        return f'{self.prefix}:roles-revision'

    @property
    def roles_marker(self) -> str:
        return f'{self.prefix}:roles-changing'

    def user_marker(self, user_id: UUID) -> str:
        return f'{self.prefix}:user-changing:{user_id}'

    def snapshot(self, user_id: UUID) -> str:
        return f'{self.prefix}:user:{user_id}'
```

Move `_AuthorizationCacheEnvelope`, the registered conditional-set script,
`MGET` revision bootstrap, strict Pydantic validation, and key construction
into this module. Internal methods keep `@on_redis_exception()` and allow
`RedisError` to reach the facade.

- [ ] **Step 4: Move mutation concerns into `authorization_mutations.py`**

Define one shared marker acquisition base and two separate components. The
shared acquisition logic is:

```python
class AuthorizationMutationInProgressError(Exception):
    """A mutation already owns the requested authorization marker."""


async def _acquire_marker(self, marker_key: str, token: str) -> None:
    acquired = await self._set_marker(marker_key, token)
    if not acquired:
        raise AuthorizationMutationInProgressError


@on_redis_exception()
async def _set_marker(self, marker_key: str, token: str) -> bool:
    return bool(
        await self._redis.set(
            marker_key,
            token,
            ex=self._marker_ttl_seconds,
            nx=True,
        )
    )
```

`RedisUserAuthorizationMutation` registers only the compare-token/delete-user-
snapshot script. `RedisRoleAuthorizationMutation` registers only the
compare-token/update-revision script and changes revision only when its body
completed successfully. Preserve current TTL, retry, logging, idempotence, and
lost-ownership behavior.

- [ ] **Step 5: Reduce `authorization.py` to protocol, exports, and facade**

Keep the existing public names and compose the internal classes:

```python
class RedisAuthorizationCache:
    def __init__(
        self,
        redis: Redis,
        *,
        ttl_seconds: int,
        namespace: str = 'authz',
    ) -> None:
        keys = AuthorizationKeyspace.create(namespace)
        self._snapshots = RedisAuthorizationSnapshotStore(
            redis,
            ttl_seconds=ttl_seconds,
            keys=keys,
        )
        self._user_mutations = RedisUserAuthorizationMutation(
            redis,
            marker_ttl_seconds=ttl_seconds + 30,
            keys=keys,
        )
        self._role_mutations = RedisRoleAuthorizationMutation(
            redis,
            marker_ttl_seconds=ttl_seconds + 30,
            keys=keys,
        )
```

Facade read/write methods catch `RedisError`, log operation context, and raise
`CacheUnavailableError`. Facade mutation context managers translate both
`RedisError` and the internal `AuthorizationMutationInProgressError` to
`CacheUnavailableError`, without changing externally visible behavior.

- [ ] **Step 6: Run focused authorization cache tests**

```bash
env UV_CACHE_DIR=/tmp/auth-sprint-uv-cache uv run pytest \
  tests/test_authorization_cache.py tests/test_authorization_service.py -q
```

Expected: all tests pass, including malformed snapshots, no raw `EVAL`, user
isolation, role revision changes, and failed role mutation behavior.

---

### Task 2: Decompose token sessions behind their facade

**Files:**
- Create: `src/db/cache/refresh_tokens.py`
- Create: `src/db/cache/access_token_denylist.py`
- Modify: `src/db/cache/session.py`
- Modify: `tests/test_session_store.py`

**Interfaces:**
- Produces: `RedisRefreshTokenStore.store(token_id: UUID, user_id: UUID, ttl: int) -> None`
- Produces: `RedisRefreshTokenStore.rotate(old_token_id: UUID, new_token_id: UUID, user_id: UUID, ttl: int) -> bool`
- Produces: `RedisRefreshTokenStore.queue_delete(pipeline: Pipeline, token_id: UUID) -> None`
- Produces: `RedisAccessTokenDenylist.contains(token_id: UUID) -> bool`
- Produces: `RedisAccessTokenDenylist.queue_add(pipeline: Pipeline, token_id: UUID, ttl: int) -> None`
- Preserves: every method of `TokenSessionStore`

- [ ] **Step 1: Add failing behavior tests for token components**

Add to `tests/test_session_store.py`:

```python
from db.cache.refresh_tokens import RedisRefreshTokenStore


@pytest.mark.asyncio
async def test_refresh_component_rotates_session(redis_client: Redis) -> None:
    store = RedisRefreshTokenStore(redis_client)
    old_token_id = uuid4()
    new_token_id = uuid4()
    user_id = uuid4()

    await store.store(old_token_id, user_id, ttl=30)
    assert (
        await store.rotate(
            old_token_id=old_token_id,
            new_token_id=new_token_id,
            user_id=user_id,
            ttl=30,
        )
        is True
    )
    assert await redis_client.get(f'refresh:{old_token_id}') is None
    assert await redis_client.get(f'refresh:{new_token_id}') == str(user_id).encode()
```

- [ ] **Step 2: Run the structural test and confirm it fails**

```bash
env UV_CACHE_DIR=/tmp/auth-sprint-uv-cache uv run pytest \
  tests/test_session_store.py::test_refresh_component_rotates_session -q
```

Expected: import failure because `refresh_tokens` does not exist.

- [ ] **Step 3: Implement `RedisRefreshTokenStore`**

Move refresh key construction, storage, and rotation from `session.py`:

```python
class RedisRefreshTokenStore:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        self._rotate_script = redis.register_script(_ROTATE_REFRESH_SCRIPT)

    @on_redis_exception()
    async def store(self, token_id: UUID, user_id: UUID, ttl: int) -> None:
        await self._redis.set(self._key(token_id), str(user_id), ex=ttl)

    @on_redis_exception()
    async def rotate(
        self,
        *,
        old_token_id: UUID,
        new_token_id: UUID,
        user_id: UUID,
        ttl: int,
    ) -> bool:
        result = await self._rotate_script(
            keys=(self._key(old_token_id), self._key(new_token_id)),
            args=(str(user_id), ttl),
        )
        return bool(result)

    def queue_delete(self, pipeline: Pipeline, token_id: UUID) -> None:
        pipeline.delete(self._key(token_id))

    @staticmethod
    def _key(token_id: UUID) -> str:
        return f'refresh:{token_id}'
```

Call the registered script with `keys=(old_key, new_key)` and
`args=(str(user_id), ttl)`. Preserve concurrent single-winner, idempotence, and
replay rejection.

- [ ] **Step 4: Implement `RedisAccessTokenDenylist`**

```python
class RedisAccessTokenDenylist:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    @on_redis_exception()
    async def contains(self, token_id: UUID) -> bool:
        return await self._redis.get(self._key(token_id)) is not None

    def queue_add(
        self,
        pipeline: Pipeline,
        token_id: UUID,
        ttl: int,
    ) -> None:
        pipeline.set(self._key(token_id), '1', ex=ttl)
```

The denylist remains a simple idempotent presence fact and does not reuse the
authorization marker abstraction.

- [ ] **Step 5: Reduce `session.py` to protocol and facade**

Compose both components in `RedisTokenSessionStore.__init__`. Delegate storage,
rotation, and membership checks while preserving facade-level logging and
`CacheUnavailableError` translation. Keep logout orchestration atomic:

```python
@on_redis_exception()
async def _revoke_session(
    self,
    *,
    refresh_token_id: UUID | None,
    access_token_id: UUID,
    access_ttl: int,
) -> None:
    async with self._redis.pipeline(transaction=True) as pipeline:
        if refresh_token_id is not None:
            self._refresh_tokens.queue_delete(pipeline, refresh_token_id)
        self._access_denylist.queue_add(
            pipeline,
            access_token_id,
            access_ttl,
        )
        await pipeline.execute()
```

- [ ] **Step 6: Update retry tests for registered scripts**

Replace raw `redis.eval` expectations with a registered-script mock:

```python
rotate_script = AsyncMock(side_effect=[RedisConnectionError('response lost'), 1])
redis = MagicMock()
redis.register_script.return_value = rotate_script
store = RedisTokenSessionStore(redis)
```

Assert that the same `keys` and `args` are passed on both retry attempts. Add an
integration assertion that monkeypatching `redis_client.eval` to fail does not
break refresh rotation.

- [ ] **Step 7: Run focused token-session tests**

```bash
env UV_CACHE_DIR=/tmp/auth-sprint-uv-cache uv run pytest \
  tests/test_session_store.py tests/test_authentication_service.py -q
```

Expected: all retry, TTL, concurrent rotation, replay, logout, and denylist
tests pass.

---

### Task 3: Documentation and complete verification

**Files:**
- Modify: `README.md` only if module ownership needs clarification after the split
- Verify: `c4/c4-component.puml`, `c4/c4-container.puml`, `c4/sequence.puml`, `c4/sequence-roles.puml`

**Interfaces:**
- Consumes: the stable facades produced by Tasks 1 and 2
- Produces: verified documentation and repository evidence

- [ ] **Step 1: Check documentation for implementation-specific drift**

```bash
rg -n "raw EVAL|RedisAuthorizationCache|RedisTokenSessionStore|denylist|marker" \
  README.md c4 docs
```

Update only statements contradicted by the refactoring. Do not expose internal
class names in API documentation unless it materially clarifies ownership.

- [ ] **Step 2: Run the full service suite**

```bash
env UV_CACHE_DIR=/tmp/auth-sprint-uv-cache uv run pytest -q
```

Expected: all tests pass.

- [ ] **Step 3: Run compilation and type checks**

```bash
env UV_CACHE_DIR=/tmp/auth-sprint-uv-cache uv run python -m compileall -q src tests
env UV_CACHE_DIR=/tmp/auth-sprint-uv-cache uv run --with pyright pyright src tests
```

Expected: compileall exits zero and Pyright reports zero errors.

- [ ] **Step 4: Run targeted lint**

```bash
env PYLINTHOME=/tmp/auth-sprint-pylint \
  UV_CACHE_DIR=/tmp/auth-sprint-uv-cache \
  uv run --with pylint pylint \
  src/db/cache/authorization.py \
  src/db/cache/authorization_snapshot.py \
  src/db/cache/authorization_mutations.py \
  src/db/cache/session.py \
  src/db/cache/refresh_tokens.py \
  src/db/cache/access_token_denylist.py \
  tests/test_authorization_cache.py \
  tests/test_session_store.py \
  --disable=all --enable=E,F,W0611,W0612
```

Expected: 10.00/10 with no enabled diagnostics.

- [ ] **Step 5: Validate diagrams and diff hygiene**

```bash
plantuml -checkonly c4/*.puml
git diff --check
```

Expected: both commands exit zero.

No implementation commit is created; the dirty `main` worktree remains under
the user's control.
