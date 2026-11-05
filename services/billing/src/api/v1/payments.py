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


class MySubscriptionPaymentItemResponse(BaseModel):
    payment_id: UUID
    created_at_display: str
    tariff_title: str
    amount_kopeks: int
    amount_display: str
    status: str
    status_display: str
    refund_status: str | None = None
    refund_badge_display: str | None = None
    result_url: str


class MySubscriptionSummaryResponse(BaseModel):
    rule_number: int
    status_key: str
    status_display: str
    action_type: str
    action_label: str | None = None
    action_url: str | None = None
    can_subscribe: bool
    subscription_ends_display: str | None = None
    payments: list[MySubscriptionPaymentItemResponse]


@router.get(
    '/me/summary',
    response_model=MySubscriptionSummaryResponse,
    summary='Сводный статус подписки и история своих платежей (BIL-API-02)',
)
async def get_my_subscription_summary(
    user: Annotated[AuthenticatedUser, Depends(require_api_user)],
    purchase_service: Annotated[PurchaseService, Depends(get_purchase_service)],
) -> MySubscriptionSummaryResponse:
    view = await purchase_service.get_my_subscription_view(
        user_id=user.user_id,
        user_tz=user.tz,
    )
    return MySubscriptionSummaryResponse(
        rule_number=view.rule_number,
        status_key=view.status_key,
        status_display=view.status_display,
        action_type=view.action_type,
        action_label=view.action_label,
        action_url=view.action_url,
        can_subscribe=view.can_subscribe,
        subscription_ends_display=view.subscription_ends_display,
        payments=[
            MySubscriptionPaymentItemResponse(
                payment_id=row.payment_id,
                created_at_display=row.created_at_display,
                tariff_title=row.tariff_title,
                amount_kopeks=row.amount_kopeks,
                amount_display=row.amount_display,
                status=row.status,
                status_display=row.status_display,
                refund_status=row.refund_status,
                refund_badge_display=row.refund_badge_display,
                result_url=row.result_url,
            )
            for row in view.payments
        ],
    )


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
