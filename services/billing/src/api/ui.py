import logging
from typing import Annotated
from urllib.parse import urlsplit
from uuid import UUID

from fastapi import APIRouter, Depends, Form, Query, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from api.errors import BillingDomainError, UIErrorScreenException
from api.templates import render_error_page, templates
from clients.auth import (
    AuthGateway,
    AuthInvalidCredentialsError,
    AuthRateLimitExceededError,
    AuthServiceUnavailableError,
)
from core.config import settings
from core.observability import get_current_request_id
from dependencies import (
    ACCESS_TOKEN_COOKIE,
    REFRESH_TOKEN_COOKIE,
    AuthenticatedUser,
    get_auth_gateway,
    get_purchase_service,
    require_ui_user,
)
from services.purchase import PurchaseService

logger = logging.getLogger(__name__)

router = APIRouter(tags=['billing-ui'])


def require_same_site_form(request: Request) -> None:
    """Отклоняет POST экранов Биллинга, отправленный с чужого сайта (CSRF).

    Главный сценарий — login CSRF: чужая страница отправляет форму входа с
    логином атакующего, жертва оказывается в его аккаунте и оплачивает ему
    подписку. SameSite=Lax этого не ловит: ответ навигации верхнего уровня
    ставит cookie. Браузеры шлют Origin на каждый POST (Referer — запасной
    вариант); запрос без обоих заголовков — не браузерный и пропускается.
    Сравниваются имена хостов: шлюз передаёт Host без порта.
    """
    source = request.headers.get('origin') or request.headers.get('referer')
    if source is None:
        return
    source_host = urlsplit(source).hostname
    allowed_hosts = {
        request.url.hostname,
        urlsplit(str(settings.worker.billing_public_base_url)).hostname,
    }
    if source_host is None or source_host not in allowed_hosts:
        logger.warning('Rejected cross-site billing form POST from %s', source)
        raise UIErrorScreenException(
            status_code=status.HTTP_403_FORBIDDEN,
            message='Форма отправлена с другого сайта. Откройте страницу '
            'Биллинга заново и повторите действие.',
        )


def parse_ui_payment_id(payment_id: str) -> UUID:
    """Парсит UUID платежа из пути экрана или выбрасывает 404 BillingDomainError."""
    try:
        return UUID(payment_id)
    except ValueError as exc:
        raise BillingDomainError(
            'Платёж не найден',
            error_code='payment_not_found',
            status_code=status.HTTP_404_NOT_FOUND,
        ) from exc


def sanitize_next_url(raw_next: str | None) -> str:
    """Разрешает редирект только на локальные пути /billing* (без Open Redirect)."""
    if not raw_next:
        return '/billing/'
    candidate = raw_next.strip()
    if (
        not candidate.startswith('/billing')
        or candidate.startswith('//')
        or candidate.startswith('/\\')
        or '\\' in candidate
        or '\r' in candidate
        or '\n' in candidate
    ):
        return '/billing/'
    parsed = urlsplit(candidate)
    if parsed.scheme or parsed.netloc:
        return '/billing/'
    return candidate


def _is_secure_request(request: Request) -> bool:
    forwarded_proto = request.headers.get('X-Forwarded-Proto', '')
    first_proto = forwarded_proto.split(',', 1)[0].strip().lower()
    return request.url.scheme == 'https' or first_proto == 'https'


@router.get('/', response_class=HTMLResponse)
async def get_my_subscription_page(
    request: Request,
    user: Annotated[AuthenticatedUser, Depends(require_ui_user)],
    purchase_service: Annotated[PurchaseService, Depends(get_purchase_service)],
) -> HTMLResponse:
    """Экран «Моя подписка» Э2: статус и история (BIL-FR-09, BIL-API-02)."""
    view = await purchase_service.get_my_subscription_view(
        user_id=user.user_id,
        user_tz=user.tz,
    )
    return templates.TemplateResponse(
        request=request,
        name='my_subscription.html',
        context={'view': view, 'user': user},
    )


@router.get('/login', response_class=HTMLResponse)
async def get_login_page(
    request: Request,
    next_url: Annotated[str | None, Query(alias='next')] = None,
) -> HTMLResponse:
    """Экран входа Э1 (BIL-API-01, BIL-FR-12)."""
    return templates.TemplateResponse(
        request=request,
        name='login.html',
        context={
            'error_message': None,
            'login_value': '',
            'next_url': sanitize_next_url(next_url),
        },
    )


@router.post(
    '/login',
    response_model=None,
    dependencies=[Depends(require_same_site_form)],
)
async def submit_login_form(
    request: Request,
    auth_gateway: Annotated[AuthGateway, Depends(get_auth_gateway)],
    login: str = Form(default=''),
    password: str = Form(default=''),
    next_url: Annotated[str, Form(alias='next')] = '/billing/',
) -> Response:
    """Проверка логина/пароля через Auth и сохранение токенов в HttpOnly-cookie."""
    safe_next = sanitize_next_url(next_url)
    request_id = get_current_request_id()
    try:
        access_token, refresh_token = await auth_gateway.login(
            login,
            password,
            request_id=request_id,
        )
    except AuthInvalidCredentialsError:
        return templates.TemplateResponse(
            request=request,
            name='login.html',
            context={
                'error_message': 'Неверный логин или пароль',
                'login_value': login,
                'next_url': safe_next,
            },
            status_code=status.HTTP_401_UNAUTHORIZED,
        )
    except AuthRateLimitExceededError:
        return templates.TemplateResponse(
            request=request,
            name='login.html',
            context={
                'error_message': (
                    'Слишком много попыток входа, попробуйте через минуту'
                ),
                'login_value': login,
                'next_url': safe_next,
            },
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        )
    except AuthServiceUnavailableError:
        return render_error_page(
            request,
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            message='Сервис авторизации временно недоступен',
        )

    secure_cookie = _is_secure_request(request)
    redirect = RedirectResponse(
        url=safe_next,
        status_code=status.HTTP_303_SEE_OTHER,
    )
    redirect.set_cookie(
        key=ACCESS_TOKEN_COOKIE,
        value=access_token,
        httponly=True,
        secure=secure_cookie,
        samesite='lax',
        path='/',
    )
    redirect.set_cookie(
        key=REFRESH_TOKEN_COOKIE,
        value=refresh_token,
        httponly=True,
        secure=secure_cookie,
        samesite='lax',
        path='/',
    )
    return redirect


@router.post('/logout', dependencies=[Depends(require_same_site_form)])
async def submit_logout(
    request: Request,
    auth_gateway: Annotated[AuthGateway, Depends(get_auth_gateway)],
) -> RedirectResponse:
    """Выход: отзыв сессии в Auth по возможности, затем очистка cookie (BIL-API-01).

    Ответ всегда `303` на экран входа: недоступность Auth не мешает выйти,
    неотозванные токены истекают по TTL (BIL-NFR-SEC-05, матрица 6.3).
    """
    access_token = request.cookies.get(ACCESS_TOKEN_COOKIE, '')
    refresh_token = request.cookies.get(REFRESH_TOKEN_COOKIE, '')
    if access_token or refresh_token:
        try:
            await auth_gateway.logout(
                access_token,
                refresh_token,
                request_id=get_current_request_id(),
            )
        except Exception:
            # Gateway сам глушит ошибки Auth; сюда попадает только дефект кода.
            logger.exception('Unexpected error while revoking Auth session')
    secure_cookie = _is_secure_request(request)
    redirect = RedirectResponse(
        url='/billing/login',
        status_code=status.HTTP_303_SEE_OTHER,
    )
    redirect.delete_cookie(
        key=ACCESS_TOKEN_COOKIE,
        path='/',
        httponly=True,
        secure=secure_cookie,
        samesite='lax',
    )
    redirect.delete_cookie(
        key=REFRESH_TOKEN_COOKIE,
        path='/',
        httponly=True,
        secure=secure_cookie,
        samesite='lax',
    )
    return redirect


@router.post(
    '/subscribe',
    response_model=None,
    dependencies=[Depends(require_same_site_form)],
)
async def subscribe(
    user: Annotated[AuthenticatedUser, Depends(require_ui_user)],
    purchase_service: Annotated[PurchaseService, Depends(get_purchase_service)],
) -> RedirectResponse:
    """Начать покупку подписки или продолжить незавершённую (BIL-API-03)."""
    outcome = await purchase_service.start_or_continue_purchase(user.user_id)
    return RedirectResponse(
        url=outcome.redirect_url,
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.get('/payments/{payment_id}', response_class=HTMLResponse)
async def get_payment_result_page(
    request: Request,
    parsed_payment_id: Annotated[UUID, Depends(parse_ui_payment_id)],
    user: Annotated[AuthenticatedUser, Depends(require_ui_user)],
    purchase_service: Annotated[PurchaseService, Depends(get_purchase_service)],
) -> HTMLResponse:
    """Экран «Результат оплаты» Э3: чтение локальной БД (BIL-FR-04, BIL-API-04)."""
    view = await purchase_service.get_payment_result_view(
        payment_id=parsed_payment_id,
        user_id=user.user_id,
        user_tz=user.tz,
    )
    return templates.TemplateResponse(
        request=request,
        name='payment_result.html',
        context={'view': view, 'user': user},
    )


@router.get('/payments/{payment_id}/continue', response_model=None)
async def continue_payment(
    parsed_payment_id: Annotated[UUID, Depends(parse_ui_payment_id)],
    user: Annotated[AuthenticatedUser, Depends(require_ui_user)],
    purchase_service: Annotated[PurchaseService, Depends(get_purchase_service)],
) -> RedirectResponse:
    """Продолжить оплату: локальный статус + GET в ЮKassa (T-13, BIL-API-04)."""
    outcome = await purchase_service.continue_payment_for_user(
        payment_id=parsed_payment_id,
        user_id=user.user_id,
    )
    return RedirectResponse(
        url=outcome.redirect_url,
        status_code=status.HTTP_303_SEE_OTHER,
    )
