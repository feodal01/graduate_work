# RBAC Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cache effective user authorization snapshots in Redis while invalidating one user for assignment changes and all snapshots only when role permissions change.

**Architecture:** `AuthorizationService` uses a cache-aside `AuthorizationCache` abstraction. Redis stores one snapshot per user, a global opaque role revision, a global role-mutation marker, and per-user mutation markers; Lua scripts make reads, conditional fills, and mutation finalization atomic. Cache read failures fall back to PostgreSQL, while mutations that cannot establish their marker do not modify PostgreSQL.

**Tech Stack:** Python 3.14, FastAPI, redis-py asyncio, Redis 7, PostgreSQL, pytest, Pydantic Settings.

## Global Constraints

- Role assignment/revocation and superuser promotion must invalidate only the affected user.
- Role permission changes and role deletion must change the global role revision.
- Role creation and name-only updates must not invalidate authorization snapshots.
- A marker must prevent reads and fills while the corresponding mutation is in progress.
- Redis read/write failures during cache-aside checks fall back to PostgreSQL.
- A mutation must not begin when its Redis marker cannot be acquired.
- Finalization failure after a PostgreSQL commit must leave the marker until its TTL expires and must not report the committed DB operation as failed.
- Existing unrelated working-tree changes and the current `main` branch must be preserved.

---

### Task 1: Redis authorization snapshot store

**Files:**
- Create: `services/auth/src/db/cache/authorization.py`
- Create: `services/auth/tests/test_authorization_cache.py`

**Interfaces:**
- Produces: `AuthorizationSnapshot(is_superuser: bool, permissions: frozenset[str])`.
- Produces: `AuthorizationCacheLookup(snapshot: AuthorizationSnapshot | None, roles_revision: str | None)`.
- Produces: `AuthorizationCache.get(user_id)`, `set(user_id, snapshot, roles_revision)`, `user_mutation(user_id)`, and `roles_mutation()`.
- Produces: `RedisAuthorizationCache(redis, ttl_seconds, namespace='authz')`.

- [ ] **Step 1: Write failing Redis integration tests**

Cover a miss/fill/hit, user mutation deleting only one snapshot, role mutation invalidating every old-revision snapshot, a stale fill rejected after revision change, and marker-visible reads bypassing snapshots.

- [ ] **Step 2: Run the focused tests and verify collection or behavior fails because the store is absent**

Run: `uv --directory services/auth run pytest tests/test_authorization_cache.py -q`

- [ ] **Step 3: Implement the protocol, DTOs, Lua scripts, JSON validation, TTLs, and Redis error translation**

Use fixed keys `{namespace}:roles-revision` and `{namespace}:roles-changing`; per-user keys contain the same Redis hash tag. Initialize a missing revision with a UUID candidate, store the revision in each snapshot envelope, and only fill when the observed revision still matches and no marker exists.

- [ ] **Step 4: Run the focused Redis tests until green**

Run: `uv --directory services/auth run pytest tests/test_authorization_cache.py -q`

---

### Task 2: Cache-aside authorization reads and mutation invalidation

**Files:**
- Modify: `services/auth/src/services/authorization.py`
- Modify: `services/auth/tests/fakes.py`
- Modify: `services/auth/tests/test_authorization_service.py`
- Modify: `services/auth/tests/test_authorization_api.py`

**Interfaces:**
- Consumes: `AuthorizationCache` and `AuthorizationSnapshot` from Task 1.
- Produces: `AuthorizationService(..., authorization_cache: AuthorizationCache)`.

- [ ] **Step 1: Write failing service tests**

Verify the second permission check avoids repository reads, Redis read failure falls back to repositories, assignment/revocation deletes only the target snapshot, role permission update/delete changes the global revision, and cache marker acquisition failure prevents the repository mutation.

- [ ] **Step 2: Run the focused service tests and confirm the missing cache dependency/behavior fails**

Run: `uv --directory services/auth run pytest tests/test_authorization_service.py -q`

- [ ] **Step 3: Implement cache-aside reads and wrap mutations**

Load `User` and effective permissions only on cache miss. Wrap assignment/revocation in `user_mutation(user_id)`, permission-bearing role updates and deletions in `roles_mutation()`, and leave role creation/name-only updates unwrapped.

- [ ] **Step 4: Update in-memory fakes and API fixtures, then run authorization tests**

Run: `uv --directory services/auth run pytest tests/test_authorization_service.py tests/test_authorization_api.py -q`

---

### Task 3: Settings, lifespan, and dependency wiring

**Files:**
- Modify: `services/auth/src/core/config.py`
- Modify: `services/auth/src/main.py`
- Modify: `services/auth/src/dependencies.py`
- Modify: `services/auth/.env.example`
- Modify: `services/auth/tests/test_config.py`
- Modify: `services/auth/tests/test_lifespan.py`

**Interfaces:**
- Produces: `AuthorizationCacheSettings(ttl_seconds: int = 300)` with `AUTHORIZATION_CACHE_` environment prefix.
- Produces: `get_authorization_cache(request) -> AuthorizationCache`.

- [ ] **Step 1: Write failing settings and lifespan tests**

Assert environment loading and registration of `app.state.authorization_cache` with the configured TTL.

- [ ] **Step 2: Run the focused tests and verify they fail**

Run: `uv --directory services/auth run pytest tests/test_config.py tests/test_lifespan.py -q`

- [ ] **Step 3: Add settings, instantiate the Redis store once in lifespan, and inject it into `AuthorizationService`**

- [ ] **Step 4: Run focused tests until green**

Run: `uv --directory services/auth run pytest tests/test_config.py tests/test_lifespan.py tests/test_authorization_api.py -q`

---

### Task 4: Superuser promotion invalidation

**Files:**
- Modify: `services/auth/src/services/superusers.py`
- Modify: `services/auth/src/cli.py`
- Modify: `services/auth/tests/test_superusers.py`

**Interfaces:**
- Consumes: `AuthorizationCache.user_mutation(user_id)`.
- Produces: `SuperuserService(..., authorization_cache: AuthorizationCache)`.

- [ ] **Step 1: Write a failing test where an existing non-superuser has a cached snapshot before promotion**

The promotion must delete that snapshot; creating a brand-new superuser and repeating bootstrap must not require invalidation.

- [ ] **Step 2: Run the focused test and verify it fails**

Run: `uv --directory services/auth run pytest tests/test_superusers.py -q`

- [ ] **Step 3: Wrap existing-user promotion and wire a Redis authorization cache into the CLI lifecycle**

- [ ] **Step 4: Run superuser and CLI smoke tests**

Run: `uv --directory services/auth run pytest tests/test_superusers.py -q`

Run: `uv --directory services/auth run python src/cli.py --help`

---

### Task 5: Documentation and complete verification

**Files:**
- Modify: `services/auth/README.md`
- Modify: `services/auth/c4/c4-component.puml`
- Modify: `services/auth/c4/c4-container.puml`

**Interfaces:**
- Documents: cache-aside reads, PostgreSQL fallback, per-user invalidation, global role revision, and Redis mutation markers.

- [ ] **Step 1: Update documentation and architecture sources without changing unrelated scope**

- [ ] **Step 2: Run all service tests**

Run: `uv --directory services/auth run pytest -q`

- [ ] **Step 3: Run static and source checks**

Run: `uv --directory services/auth run python -m compileall -q src tests`

Run: `uv --directory services/auth run --with pyright pyright src tests`

Run: `git diff --check`

- [ ] **Step 4: Inspect the final diff and report any remaining consistency or availability trade-offs**

