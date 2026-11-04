from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from dependencies import AuthenticatedUser, get_purchase_service, require_api_user
from services.purchase import PurchaseService

router = APIRouter(tags=['billing-payments'])


class PaymentPurchaseResponse(BaseModel):
    payment_id: UUID
    status: str
    redirect_url: str
    confirmation_url: str | None = None


@router.post(
    '/payments',
    response_model=PaymentPurchaseResponse,
    summary='Начать покупку подписки или продолжить незавершённую (BIL-API-03)',
)
async def create_or_continue_payment(
    user: Annotated[AuthenticatedUser, Depends(require_api_user)],
    purchase_service: Annotated[PurchaseService, Depends(get_purchase_service)],
) -> PaymentPurchaseResponse:
    outcome = await purchase_service.start_or_continue_purchase(user.user_id)
    return PaymentPurchaseResponse(
        payment_id=outcome.payment_id,
        status=outcome.status,
        redirect_url=outcome.redirect_url,
        confirmation_url=outcome.confirmation_url,
    )
