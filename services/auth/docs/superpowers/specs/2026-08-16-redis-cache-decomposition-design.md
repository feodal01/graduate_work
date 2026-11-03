# Redis Cache Decomposition Design

## Goal

Reduce the cognitive load of the Redis infrastructure without changing its
public behavior. Authorization cache concerns and token-session concerns will
be split into domain-specific components, while application services continue
to depend on one facade per domain.

## Scope

This is an internal refactoring of `services/auth/src/db/cache`.

The refactoring does not change:

- HTTP endpoints or schemas;
- `AuthorizationCache` and `TokenSessionStore` public protocols;
- FastAPI dependency injection signatures;
- Redis key names, stored values, or TTL rules;
- PostgreSQL behavior or migrations;
- fail-open and fail-closed decisions already made by the service layer.

## Rejected Generalization

The implementation will not introduce a universal configurable Redis key
store. Although access-token denylist entries and authorization mutation
markers both use TTL keys, their invariants differ:

- a denylist entry is an idempotent membership fact that remains until expiry;
- a mutation marker is an exclusive lease with an ownership token and
  conditional release;
- authorization snapshots require revision matching;
- refresh rotation requires an atomic, replay-safe state transition.

Combining these behaviors behind flags such as `exclusive`, `owner_token`, or
`delete_on_finish` would hide domain rules and reduce readability.

## Authorization Cache

`RedisAuthorizationCache` remains the public facade and continues to implement
`AuthorizationCache`. It composes three internal components:

1. `RedisAuthorizationSnapshotStore` reads and conditionally writes effective
   permission snapshots.
2. `RedisUserAuthorizationMutation` owns the per-user marker lifecycle and
   invalidates only that user's snapshot.
3. `RedisRoleAuthorizationMutation` owns the global role marker lifecycle and
   changes the UUID role revision only after a successful role-definition
   mutation.

The two mutation components share only the small marker-acquisition mechanism.
Their completion rules and registered Lua scripts remain separate because
their invariants are different.

The facade delegates `get`, `set`, `user_mutation`, and `roles_mutation` without
adding business logic. `AuthorizationService` and its dependency provider do
not learn about the internal components.

## Token Sessions

`RedisTokenSessionStore` remains the public facade and continues to implement
`TokenSessionStore`. Its internals are separated into:

1. `RedisRefreshTokenStore` for storing and atomically rotating refresh-token
   sessions, and for queuing refresh deletion into a logout transaction.
2. `RedisAccessTokenDenylist` for checking revoked access tokens and queuing a
   revocation entry into a logout transaction.

Logout remains coordinated by the facade because deleting the refresh session
and adding the access token to the denylist must execute in one Redis
transaction. Key construction stays encapsulated in the token-session domain;
it is not exposed to application services.

The refresh rotation script is registered through `redis-py`, matching the
authorization scripts. Direct raw `EVAL` calls are not retained.

## Error Handling and Reliability

Existing behavior is preserved:

- transient Redis connection and timeout failures use the current bounded
  backoff policy;
- Redis errors become `CacheUnavailableError` at the facade boundary;
- authorization reads may fall back to PostgreSQL in `AuthorizationService`;
- token revocation checks remain fail-closed;
- authorization mutations cannot reach PostgreSQL unless their marker was
  acquired;
- failure to finalize a marker after a committed SQL mutation remains logged
  and protected by marker TTL.

Internal components may raise `RedisError`; only public facades translate it to
the domain cache error. This keeps error ownership in one visible place.

## File Layout

The target layout is intentionally small:

- `db/cache/authorization.py`: public types, protocol, and facade;
- `db/cache/authorization_snapshot.py`: snapshot serialization and storage;
- `db/cache/authorization_mutations.py`: shared marker acquisition plus the
  distinct user and role mutation components;
- `db/cache/session.py`: public token-session protocol and facade;
- `db/cache/refresh_tokens.py`: refresh storage and rotation;
- `db/cache/access_token_denylist.py`: access-token denylist checks.

No generic Redis repository layer is added.

## Verification

Existing integration tests remain the primary behavior contract. Focused tests
will additionally prove:

- facade delegation preserves authorization snapshot and invalidation behavior;
- user mutation affects only one user;
- role mutation changes the global revision only on success;
- refresh rotation remains concurrent, atomic, idempotent, and replay-safe;
- logout still deletes refresh and writes denylist atomically;
- registered scripts are used instead of raw `EVAL`;
- Redis failures are translated only at facade boundaries.

The final verification includes the full pytest suite, compileall, Pyright,
targeted Pylint, PlantUML validation for affected diagrams, and
`git diff --check`.
