"""Ошибки Elasticsearch → доменные исключения."""

from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any

from elastic_transport import TransportError
from elasticsearch import ApiError, NotFoundError

from db.backoff_policy import ELASTIC_RETRYABLE


class StorageUnavailableError(Exception):
    """Хранилище недоступно или не готово обслуживать запрос."""


def _is_missing_document(exc: NotFoundError) -> bool:
    """404 по документу (get), а не отсутствующий индекс."""
    body: Any = exc.body
    if body is None:
        return True
    return isinstance(body, dict) and body.get('found') is False


async def run_elastic[T](
    awaitable: Awaitable[T],
    *,
    missing_document: bool = False,
) -> T | None:
    """
    Один вызов ES → ответ или StorageUnavailableError.

    missing_document=True (get): документ не найден → None.
    ConnectionError / Timeout пропускаем — их ретраит backoff.
    """
    try:
        return await awaitable
    except NotFoundError as exc:
        if missing_document and _is_missing_document(exc):
            return None
        raise StorageUnavailableError('Storage is unavailable') from exc
    except ELASTIC_RETRYABLE:
        raise
    except (ApiError, TransportError) as exc:
        raise StorageUnavailableError('Storage is unavailable') from exc


def with_elastic_errors[F: Callable[..., Any]](fn: F) -> F:
    """
    Снаружи backoff: после исчерпания ретраев
    ConnectionError/Timeout → StorageUnavailableError.
    """

    @wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return await fn(*args, **kwargs)
        except ELASTIC_RETRYABLE as exc:
            raise StorageUnavailableError('Storage is unavailable') from exc

    return wrapper  # type: ignore[return-value]
