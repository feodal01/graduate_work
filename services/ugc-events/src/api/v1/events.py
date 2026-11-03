import logging
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, status

from dependencies import EventBrokerDep, EventRateLimiterDep
from security.jwt import VerifiedAccessToken, required_jwt_bearer
from services.event_schema import (
    MAX_REQUEST_ID_LENGTH,
    AcceptedResponse,
    EventBatch,
)
from services.ingest import event_time_errors, publish_user_events
from services.rate_limit import RateLimitUnavailableError

logger = logging.getLogger(__name__)
router = APIRouter()

AccessTokenDep = Annotated[VerifiedAccessToken, Depends(required_jwt_bearer)]


@router.post(
    '',
    response_model=AcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary='Принять пакет действий зрителя',
    description=(
        'Проверяет access JWT локально и публикует события в Kafka. '
        'user_id берётся из токена, не из тела.'
    ),
    responses={
        202: {'description': 'Пакет принят и записан в Kafka'},
        401: {'description': 'Нет или неверен access JWT'},
        413: {'description': 'Готовая запись больше лимита Kafka'},
        422: {'description': 'Тело не проходит схему или время события выходит за допустимое окно'},
        429: {
            'description': 'Превышен лимит пакетов пользователя',
            'headers': {
                'Retry-After': {
                    'description': 'Время до следующей попытки в секундах',
                    'schema': {'type': 'integer', 'minimum': 1},
                },
            },
        },
        503: {'description': 'Kafka недоступна'},
    },
)
async def ingest_events(
    token: AccessTokenDep,
    payload: EventBatch,
    request: Request,
    broker: EventBrokerDep,
    limiter: EventRateLimiterDep,
) -> AcceptedResponse:
    received_at = datetime.now(UTC)
    request_id = request.headers.get('X-Request-Id')
    if request_id is not None and len(request_id) > MAX_REQUEST_ID_LENGTH:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail='X-Request-Id is too long',
        )
    errors = event_time_errors(payload.events, received_at)
    if errors:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=errors,
        )
    try:
        retry_after = await limiter.check(token.user_id)
    except RateLimitUnavailableError:
        logger.warning(
            'Rate-limit store is unavailable; request continues request_id=%s',
            request_id,
            extra={'request_id': request_id, 'rate_limit.store_available': False},
        )
        retry_after = 0
    if retry_after:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail='Too many requests',
            headers={'Retry-After': str(retry_after)},
        )
    accepted = await publish_user_events(
        payload.events,
        user_id=token.user_id,
        request_id=request_id,
        broker=broker,
        received_at=received_at,
    )
    return AcceptedResponse(accepted=accepted)
