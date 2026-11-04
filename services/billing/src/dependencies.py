from dataclasses import dataclass
from typing import cast
from uuid import UUID

from fastapi import Depends, Request

from api.errors import BillingDomainError, UIErrorScreenException, UILoginRedirectError
from clients.auth import (
    AuthGateway,
    AuthInvalidCredentialsError,
    AuthServiceUnavailableError,
)
from clients.payment_provider import PaymentProviderGateway
from core.config import settings
from core.observability import get_current_request_id
from db.health import HealthCheck
from db.sql.postgres import PostgresDatabase
from repositories.purchase import SQLAlchemyPurchaseRepository
from security.service_credentials import ServiceCredentials
from services.purchase import PurchaseService
from services.reconciler import PaymentReconciler

ACCESS_TOKEN_COOKIE = 'billing_access_token'
REFRESH_TOKEN_COOKIE = 'billing_refresh_token'


@dataclass(frozen=True, slots=True)
class AuthenticatedUser:
    user_id: UUID
    login: str
    tz: str
    access_token: str


async def get_database_health(request: Request) -> HealthCheck:
    return cast(HealthCheck, request.app.state.database_health)


async def get_service_credentials(request: Request) -> ServiceCredentials:
    return cast(ServiceCredentials, request.app.state.service_credentials)


async def get_auth_gateway(request: Request) -> AuthGateway:
    return cast(AuthGateway, request.app.state.auth_gateway)


async def get_payment_provider(request: Request) -> PaymentProviderGateway:
    return cast(PaymentProviderGateway, request.app.state.payment_provider)


async def get_payment_reconciler(
    request: Request,
    payment_provider: PaymentProviderGateway = Depends(get_payment_provider),
) -> PaymentReconciler:
    database = cast(PostgresDatabase, request.app.state.database)
    repo = SQLAlchemyPurchaseRepository(database.sessionmaker)
    return PaymentReconciler(
        repository=repo,
        payment_provider=payment_provider,
        public_base_url=str(settings.worker.billing_public_base_url),
    )


async def get_purchase_service(
    request: Request,
    payment_provider: PaymentProviderGateway = Depends(get_payment_provider),
    reconciler: PaymentReconciler = Depends(get_payment_reconciler),
) -> PurchaseService:
    database = cast(PostgresDatabase, request.app.state.database)
    repo = SQLAlchemyPurchaseRepository(database.sessionmaker)
    return PurchaseService(
        repository=repo,
        payment_provider=payment_provider,
        public_base_url=str(settings.worker.billing_public_base_url),
        reconciler=reconciler,
    )


def _extract_access_token(request: Request) -> str | None:
    auth_header = request.headers.get('Authorization', '')
    if auth_header.lower().startswith('bearer '):
        token = auth_header[7:].strip()
        if token:
            return token
    cookie_token = request.cookies.get(ACCESS_TOKEN_COOKIE)
    if cookie_token:
        return cookie_token
    return None


async def require_ui_user(
    request: Request,
    auth_gateway: AuthGateway = Depends(get_auth_gateway),
) -> AuthenticatedUser:
    token = _extract_access_token(request)
    if not token:
        raise UILoginRedirectError()
    request_id = get_current_request_id()
    try:
        user_id, login, tz = await auth_gateway.get_current_user(
            token,
            request_id=request_id,
        )
    except AuthInvalidCredentialsError as exc:
        raise UILoginRedirectError() from exc
    except AuthServiceUnavailableError as exc:
        raise UIErrorScreenException(
            status_code=503,
            message='Сервис авторизации временно недоступен',
        ) from exc
    return AuthenticatedUser(
        user_id=user_id,
        login=login,
        tz=tz,
        access_token=token,
    )


async def require_api_user(
    request: Request,
    auth_gateway: AuthGateway = Depends(get_auth_gateway),
) -> AuthenticatedUser:
    token = _extract_access_token(request)
    if not token:
        raise BillingDomainError(
            'Authentication credentials were not provided',
            error_code='unauthenticated',
            status_code=401,
        )
    request_id = get_current_request_id()
    try:
        user_id, login, tz = await auth_gateway.get_current_user(
            token,
            request_id=request_id,
        )
    except AuthInvalidCredentialsError as exc:
        raise BillingDomainError(
            'Access token is invalid or expired',
            error_code='unauthenticated',
            status_code=401,
        ) from exc
    except AuthServiceUnavailableError as exc:
        raise BillingDomainError(
            'Сервис авторизации временно недоступен',
            error_code='auth_unavailable',
            status_code=503,
        ) from exc
    return AuthenticatedUser(
        user_id=user_id,
        login=login,
        tz=tz,
        access_token=token,
    )
