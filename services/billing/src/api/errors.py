from collections.abc import Awaitable, Callable
from typing import Any, cast
from urllib.parse import quote

from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from starlette.types import ExceptionHandler

from api.templates import render_error_page
from db.sql.errors import DatabaseUnavailableError
from domain.errors import (
    BillingError,
    DependencyUnavailableError,
    EntityNotFoundError,
    RuleConflictError,
)


class BillingDomainError(Exception):
    """Base application error with an explicit error_code and HTTP status."""

    status_code: int = status.HTTP_400_BAD_REQUEST
    error_code: str = 'billing_error'

    def __init__(
        self,
        detail: str,
        *,
        error_code: str | None = None,
        status_code: int | None = None,
    ) -> None:
        self.detail = detail
        if error_code is not None:
            self.error_code = error_code
        if status_code is not None:
            self.status_code = status_code
        super().__init__(detail)


class InvalidServiceCredentialsError(BillingDomainError):
    status_code = status.HTTP_401_UNAUTHORIZED
    error_code = 'invalid_service_credentials'

    def __init__(self, detail: str = 'Invalid or missing service API key') -> None:
        super().__init__(detail)


class UILoginRedirectError(Exception):
    """Signals that an HTML screen request must redirect to /billing/login."""


class UIErrorScreenException(Exception):
    """Signals that an HTML screen request must render error screen E4."""

    def __init__(
        self,
        *,
        status_code: int,
        message: str,
        title: str = 'Произошла ошибка',
    ) -> None:
        self.status_code = status_code
        self.message = message
        self.title = title
        super().__init__(message)


def error_response(
    status_code: int,
    error_code: str,
    detail: str,
    *,
    errors: list[Any] | None = None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    content: dict[str, Any] = {
        'error_code': error_code,
        'detail': detail,
    }
    if errors is not None:
        content['errors'] = errors
    return JSONResponse(
        status_code=status_code,
        content=content,
        headers=headers,
    )


def _register_handler[ErrorT: Exception](
    app: FastAPI,
    error_type: type[ErrorT],
    handler: Callable[[Request, ErrorT], Awaitable[Response]],
) -> None:
    app.add_exception_handler(error_type, cast(ExceptionHandler, handler))


async def domain_error_handler(
    request: Request,
    exc: BillingDomainError,
) -> Response:
    if request.url.path.startswith('/billing/'):
        return render_error_page(
            request,
            status_code=exc.status_code,
            message=exc.detail,
        )
    return error_response(exc.status_code, exc.error_code, exc.detail)


def http_status_for_rule_error(exc: BillingError) -> int:
    """HTTP-статус для доменной ошибки: решает слой API, а не репозиторий."""
    if isinstance(exc, EntityNotFoundError):
        return status.HTTP_404_NOT_FOUND
    if isinstance(exc, RuleConflictError):
        return status.HTTP_409_CONFLICT
    if isinstance(exc, DependencyUnavailableError):
        return status.HTTP_503_SERVICE_UNAVAILABLE
    return status.HTTP_400_BAD_REQUEST


async def rule_error_handler(
    request: Request,
    exc: BillingError,
) -> Response:
    return await domain_error_handler(
        request,
        BillingDomainError(
            exc.detail,
            error_code=exc.error_code,
            status_code=http_status_for_rule_error(exc),
        ),
    )


async def database_unavailable_handler(
    request: Request,
    exc: DatabaseUnavailableError,
) -> Response:
    if request.url.path.startswith('/billing/'):
        return render_error_page(
            request,
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            message='Сервис биллинга временно недоступен, попробуйте ещё раз',
        )
    return error_response(
        status.HTTP_503_SERVICE_UNAVAILABLE,
        'database_unavailable',
        str(exc) or 'Database is unavailable',
    )


async def validation_error_handler(
    _request: Request,
    exc: RequestValidationError,
) -> JSONResponse:
    return error_response(
        status.HTTP_422_UNPROCESSABLE_CONTENT,
        'validation_error',
        'Request validation failed',
        errors=cast(list[Any], jsonable_encoder(exc.errors())),
    )


async def ui_login_redirect_handler(
    request: Request,
    _exc: UILoginRedirectError,
) -> RedirectResponse:
    target_path = request.url.path
    if request.url.query:
        target_path = f'{target_path}?{request.url.query}'
    if (
        request.method == 'GET'
        and target_path.startswith('/billing')
        and target_path not in ('/billing/', '/billing/login')
    ):
        login_url = f'/billing/login?next={quote(target_path, safe="/")}'
    else:
        login_url = '/billing/login'
    return RedirectResponse(
        url=login_url,
        status_code=status.HTTP_303_SEE_OTHER,
    )


async def ui_error_screen_handler(
    request: Request,
    exc: UIErrorScreenException,
) -> HTMLResponse:
    return render_error_page(
        request,
        status_code=exc.status_code,
        message=exc.message,
        title=exc.title,
    )


def register_exception_handlers(app: FastAPI) -> None:
    _register_handler(app, BillingDomainError, domain_error_handler)
    _register_handler(app, BillingError, rule_error_handler)
    _register_handler(app, DatabaseUnavailableError, database_unavailable_handler)
    _register_handler(app, RequestValidationError, validation_error_handler)
    _register_handler(app, UILoginRedirectError, ui_login_redirect_handler)
    _register_handler(app, UIErrorScreenException, ui_error_screen_handler)
