"""Политика повторных запросов к внешним сервисам (exponential backoff)."""

import logging
from collections.abc import Callable
from typing import Any, TypeVar

import backoff
from elastic_transport import ConnectionError as ElasticConnectionError
from elastic_transport import ConnectionTimeout as ElasticConnectionTimeout
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from core.config import settings

logger = logging.getLogger(__name__)

# Только транзиентные сетевые ошибки: NotFound / ResponseError не ретраим.
ELASTIC_RETRYABLE = (ElasticConnectionError, ElasticConnectionTimeout)
REDIS_RETRYABLE = (RedisConnectionError, RedisTimeoutError)

_F = TypeVar('_F', bound=Callable[..., Any])


def on_elastic_exception() -> Callable[[_F], _F]:
    return backoff.on_exception(
        backoff.expo,
        ELASTIC_RETRYABLE,
        max_tries=settings.backoff_max_tries,
        max_time=settings.backoff_max_time,
        logger=logger,
    )


def on_redis_exception() -> Callable[[_F], _F]:
    return backoff.on_exception(
        backoff.expo,
        REDIS_RETRYABLE,
        max_tries=settings.backoff_max_tries,
        max_time=settings.backoff_max_time,
        logger=logger,
    )
