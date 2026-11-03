from typing import Protocol
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError


class RateLimitUnavailableError(Exception):
    """Redis недоступен: проверить лимит пакетов невозможно."""


class EventRateLimiter(Protocol):
    async def check(self, user_id: UUID) -> int:
        """Возвращает 0 для разрешённого пакета, иначе время ожидания в секундах."""
        ...

    async def ping(self) -> bool: ...


_TOKEN_BUCKET_SCRIPT = """
local rate = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local capacity = tonumber(ARGV[3])
local refill_per_second = rate / window
local current_time = redis.call('TIME')
local now = tonumber(current_time[1]) + tonumber(current_time[2]) / 1000000
local state = redis.call('HMGET', KEYS[1], 'tokens', 'last_refill')
local tokens = tonumber(state[1]) or capacity
local last_refill = tonumber(state[2]) or now
tokens = math.min(capacity, tokens + math.max(0, now - last_refill) * refill_per_second)
local retry_after = 0
if tokens >= 1 then
    tokens = tokens - 1
else
    retry_after = math.max(1, math.ceil((1 - tokens) / refill_per_second))
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'last_refill', now)
redis.call('EXPIRE', KEYS[1], math.ceil(capacity / refill_per_second) + 1)
return retry_after
"""


class RedisEventRateLimiter:
    """Один атомарный token bucket на пользователя, общий для всех процессов API."""

    def __init__(
        self,
        redis: Redis,
        *,
        rate: int,
        window_seconds: int,
        capacity: int,
        namespace: str = 'ugc-events:rate-limit',
    ) -> None:
        self._redis = redis
        self._script = redis.register_script(_TOKEN_BUCKET_SCRIPT)
        self._rate = rate
        self._window_seconds = window_seconds
        self._capacity = capacity
        self._namespace = namespace

    async def check(self, user_id: UUID) -> int:
        try:
            result: object = await self._script(
                keys=[f'{self._namespace}:{user_id}'],
                args=[self._rate, self._window_seconds, self._capacity],
            )
        except (RedisConnectionError, RedisTimeoutError) as exc:
            raise RateLimitUnavailableError from exc
        if isinstance(result, bool) or not isinstance(result, int) or result < 0:
            raise ValueError('Malformed rate-limit script result')
        return result

    async def ping(self) -> bool:
        try:
            return bool(await self._redis.ping())
        except (RedisConnectionError, RedisTimeoutError):
            return False
