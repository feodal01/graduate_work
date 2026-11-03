"""Bounded retries for transient database and cache failures."""

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from functools import wraps
from time import monotonic
from typing import ParamSpec, TypeVar

from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from sqlalchemy.exc import DisconnectionError, OperationalError

from core.config import settings

logger = logging.getLogger(__name__)

POSTGRES_RETRYABLE = (OperationalError, DisconnectionError, OSError)
REDIS_RETRYABLE = (RedisConnectionError, RedisTimeoutError)

_P = ParamSpec('_P')
_R = TypeVar('_R')


def _on_exception(
    exceptions: tuple[type[Exception], ...],
) -> Callable[
    [Callable[_P, Awaitable[_R]]],
    Callable[_P, Awaitable[_R]],
]:
    def decorator(
        function: Callable[_P, Awaitable[_R]],
    ) -> Callable[_P, Awaitable[_R]]:
        @wraps(function)
        async def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            max_tries = settings.backoff.max_tries
            deadline = monotonic() + settings.backoff.max_time
            delay = 1.0

            for attempt in range(1, max_tries + 1):
                try:
                    return await function(*args, **kwargs)
                except exceptions as exc:
                    remaining = deadline - monotonic()
                    if attempt == max_tries or remaining <= 0:
                        raise
                    wait = min(random.uniform(0, delay), remaining)
                    logger.warning(
                        'Transient backend error; retrying '
                        'operation=%s attempt=%s/%s wait=%.3fs error=%s',
                        function.__qualname__,
                        attempt,
                        max_tries,
                        wait,
                        type(exc).__name__,
                    )
                    await asyncio.sleep(wait)
                    delay *= 2

            raise RuntimeError('Backoff loop exhausted unexpectedly')

        return wrapped

    return decorator


def on_postgres_exception() -> Callable[
    [Callable[_P, Awaitable[_R]]],
    Callable[_P, Awaitable[_R]],
]:
    return _on_exception(POSTGRES_RETRYABLE)


def on_redis_exception() -> Callable[
    [Callable[_P, Awaitable[_R]]],
    Callable[_P, Awaitable[_R]],
]:
    return _on_exception(REDIS_RETRYABLE)
