import logging
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from time import gmtime, strftime
from typing import cast

import sentry_sdk
from fastapi import Request, Response
from logstash import LogstashFormatterVersion1, UDPLogstashHandler
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.starlette import StarletteIntegration

REQUEST_ID_HEADER = 'X-Request-Id'
NO_REQUEST_ID = '-'

_request_id: ContextVar[str] = ContextVar('request_id', default=NO_REQUEST_ID)


def get_current_request_id() -> str:
    return _request_id.get()


@contextmanager
def correlation_request_id(fallback_id: str) -> Iterator[str]:
    """Привязывает fallback_id к логам и Sentry вне HTTP-запроса (NFR-S-01)."""
    current = _request_id.get()
    if current != NO_REQUEST_ID and current.strip():
        yield current
        return
    token = _request_id.set(fallback_id)
    sentry_sdk.set_tag('request_id', fallback_id)
    try:
        yield fallback_id
    finally:
        _request_id.reset(token)
        sentry_sdk.set_tag('request_id', _request_id.get())


class RequestIdLogstashFormatter(LogstashFormatterVersion1):
    """Logstash JSON с request_id запроса, в рамках которого пишется лог."""

    def format(self, record: logging.LogRecord) -> bytes:
        record.request_id = _request_id.get()
        return cast(bytes, super().format(record))

    @classmethod
    def format_timestamp(cls, time: float) -> str:
        seconds = strftime('%Y-%m-%dT%H:%M:%S', gmtime(time))
        return f'{seconds}.{int(time % 1 * 1000):03d}Z'


class QuietLogstashHandler(UDPLogstashHandler):
    """UDP-обработчик, который сообщает о недоступности logstash один раз."""

    _error_reported = False

    def handleError(self, record: logging.LogRecord) -> None:  # noqa: N802
        if self._error_reported:
            return
        self._error_reported = True
        super().handleError(record)


async def bind_request_id(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """Делает X-Request-Id от nginx доступным логам и Sentry."""
    state_req_id = getattr(request.state, 'request_id', None)
    header_req_id = request.headers.get(REQUEST_ID_HEADER, '').strip()
    request_id = (
        state_req_id
        if isinstance(state_req_id, str) and state_req_id
        else (header_req_id or NO_REQUEST_ID)
    )
    token = _request_id.set(request_id)
    sentry_sdk.set_tag('request_id', request_id)
    try:
        return await call_next(request)
    finally:
        _request_id.reset(token)
        sentry_sdk.set_tag('request_id', _request_id.get())


def init_sentry(
    *,
    dsn: str,
    environment: str,
    traces_sample_rate: float,
    service: str,
) -> None:
    """Подключает Sentry SDK; без DSN отправка ошибок выключена."""
    if not dsn:
        return
    sentry_sdk.init(
        dsn=dsn,
        environment=environment,
        traces_sample_rate=traces_sample_rate,
        integrations=[StarletteIntegration(), FastApiIntegration()],
    )
    sentry_sdk.get_global_scope().set_tag('service', service)
