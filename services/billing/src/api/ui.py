from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Form, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from api.errors import BillingDomainError
from api.templates import render_error_page, templates
from clients.auth import (
    AuthGateway,
    AuthInvalidCredentialsError,
    AuthRateLimitExceededError,
    AuthServiceUnavailableError,
)
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

router = APIRouter(tags=['billing-ui'])


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


@router.get('/login', response_class=HTMLResponse)
async def get_login_page(request: Request) -> HTMLResponse:
    """Экран входа Э1 (BIL-API-01, BIL-FR-12)."""
    return templates.TemplateResponse(
        request=request,
        name='login.html',
        context={'error_message': None, 'login_value': ''},
    )


@router.post('/login', response_model=None)
async def submit_login_form(
    request: Request,
    auth_gateway: Annotated[AuthGateway, Depends(get_auth_gateway)],
    login: str = Form(default=''),
    password: str = Form(default=''),
) -> Response:
    """Проверка логина/пароля через Auth и сохранение токенов в HttpOnly-cookie."""
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
            },
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        )
    except AuthServiceUnavailableError:
        return render_error_page(
            request,
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            message='Сервис авторизации временно недоступен',
        )

    redirect = RedirectResponse(
        url='/billing/',
        status_code=status.HTTP_303_SEE_OTHER,
    )
    redirect.set_cookie(
        key=ACCESS_TOKEN_COOKIE,
        value=access_token,
        httponly=True,
        samesite='lax',
        path='/',
    )
    redirect.set_cookie(
        key=REFRESH_TOKEN_COOKIE,
        value=refresh_token,
        httponly=True,
        samesite='lax',
        path='/',
    )
    return redirect


@router.post('/logout')
async def submit_logout(
    request: Request,
    auth_gateway: Annotated[AuthGateway, Depends(get_auth_gateway)],
) -> RedirectResponse:
    """Выход пользователя с очисткой HttpOnly-cookie и редиректом на экран входа."""
    access_token = request.cookies.get(ACCESS_TOKEN_COOKIE, '')
    refresh_token = request.cookies.get(REFRESH_TOKEN_COOKIE, '')
    if access_token and refresh_token:
        await auth_gateway.logout(
            access_token,
            refresh_token,
            request_id=get_current_request_id(),
        )
    redirect = RedirectResponse(
        url='/billing/login',
        status_code=status.HTTP_303_SEE_OTHER,
    )
    redirect.delete_cookie(key=ACCESS_TOKEN_COOKIE, path='/')
    redirect.delete_cookie(key=REFRESH_TOKEN_COOKIE, path='/')
    return redirect


@router.post('/subscribe', response_model=None)
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
