from hashlib import sha256
from typing import Protocol

from pydantic import BaseModel, ValidationError
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from db.cache.errors import OAuthStateStoreUnavailableError
from validation.timezones import Timezone


class OAuthStateData(BaseModel):
    code_verifier: str
    tz: Timezone


class OAuthStateStore(Protocol):
    async def save(
        self,
        *,
        provider: str,
        state: str,
        code_verifier: str,
        tz: str,
        ttl_seconds: int,
    ) -> bool: ...

    async def consume(
        self,
        *,
        provider: str,
        state: str,
    ) -> OAuthStateData | None: ...


class RedisOAuthStateStore:
    """Store one-time PKCE state without exposing it in Redis keys."""

    def __init__(self, redis: Redis, *, namespace: str = 'oauth-state') -> None:
        if not namespace.strip():
            raise ValueError('OAuth state namespace must not be blank')
        self._redis = redis
        self._namespace = namespace

    async def save(
        self,
        *,
        provider: str,
        state: str,
        code_verifier: str,
        tz: str,
        ttl_seconds: int,
    ) -> bool:
        try:
            created = await self._redis.set(
                self._key(provider, state),
                OAuthStateData(code_verifier=code_verifier, tz=tz).model_dump_json(),
                ex=ttl_seconds,
                nx=True,
            )
        except (RedisConnectionError, RedisTimeoutError) as exc:
            raise OAuthStateStoreUnavailableError from exc
        if not isinstance(created, bool):
            raise ValueError('Malformed OAuth state save result')
        return created

    async def consume(
        self,
        *,
        provider: str,
        state: str,
    ) -> OAuthStateData | None:
        try:
            value = await self._redis.getdel(self._key(provider, state))
        except (RedisConnectionError, RedisTimeoutError) as exc:
            raise OAuthStateStoreUnavailableError from exc
        if value is None:
            return None
        if not isinstance(value, bytes):
            raise ValueError('Malformed OAuth state value')
        try:
            return OAuthStateData.model_validate_json(value)
        except (UnicodeDecodeError, ValidationError) as exc:
            raise ValueError('Malformed OAuth state value') from exc

    def _key(self, provider: str, state: str) -> str:
        digest = sha256(state.encode('utf-8')).hexdigest()
        return f'{self._namespace}:{provider}:{digest}'
