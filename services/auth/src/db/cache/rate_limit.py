from hashlib import sha256

from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from db.cache.errors import RateLimitStoreUnavailableError
from services.rate_limit import (
    RateLimitDecision,
    RateLimiter,
    RateLimitPolicy,
)

_TOKEN_BUCKET_SCRIPT = """
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local capacity = tonumber(ARGV[3])
local refill_per_second = rate / window

local current_time = redis.call('TIME')
local now = tonumber(current_time[1]) + tonumber(current_time[2]) / 1000000
local state = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(state[1])
local last_refill = tonumber(state[2])

if tokens == nil or last_refill == nil then
    tokens = capacity
    last_refill = now
end

local elapsed = math.max(0, now - last_refill)
tokens = math.min(capacity, tokens + elapsed * refill_per_second)

local allowed = 0
local retry_after = 0
if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
else
    retry_after = math.max(1, math.ceil((1 - tokens) / refill_per_second))
end

local ttl = math.ceil(capacity / refill_per_second) + 1
redis.call('HSET', key, 'tokens', tokens, 'last_refill', now)
redis.call('EXPIRE', key, ttl)

return {allowed, math.floor(tokens), retry_after}
"""


class RedisTokenBucketRateLimiter(RateLimiter):
    def __init__(
        self,
        redis: Redis,
        *,
        namespace: str = 'rate-limit',
    ) -> None:
        if not namespace.strip():
            raise ValueError('Rate-limit namespace must not be blank')
        self._namespace = namespace
        self._script = redis.register_script(_TOKEN_BUCKET_SCRIPT)

    async def check(
        self,
        *,
        policy: RateLimitPolicy,
        identity: str,
    ) -> RateLimitDecision:
        try:
            result: object = await self._script(
                keys=(self._key(self._namespace, policy, identity),),
                args=(policy.rate, policy.window_seconds, policy.capacity),
            )
        except (RedisConnectionError, RedisTimeoutError) as error:
            raise RateLimitStoreUnavailableError from error

        allowed, remaining, retry_after = self._parse_result(result)
        return RateLimitDecision(
            allowed=bool(allowed),
            remaining=remaining,
            retry_after_seconds=retry_after,
        )

    @staticmethod
    def _key(
        namespace: str,
        policy: RateLimitPolicy,
        identity: str,
    ) -> str:
        digest = sha256(identity.encode('utf-8')).hexdigest()
        return f'{namespace}:{policy.name}:{digest}'

    @staticmethod
    def _parse_result(result: object) -> tuple[int, int, int]:
        if not isinstance(result, (list, tuple)) or len(result) != 3:
            raise ValueError('Malformed rate-limit script result')

        allowed, remaining, retry_after = result
        if (
            isinstance(allowed, bool)
            or not isinstance(allowed, int)
            or isinstance(remaining, bool)
            or not isinstance(remaining, int)
            or isinstance(retry_after, bool)
            or not isinstance(retry_after, int)
        ):
            raise ValueError('Malformed rate-limit script result')

        if (
            allowed not in (0, 1)
            or remaining < 0
            or retry_after < 0
            or (allowed == 1 and retry_after != 0)
            or (allowed == 0 and retry_after < 1)
        ):
            raise ValueError('Malformed rate-limit script result')
        return allowed, remaining, retry_after
