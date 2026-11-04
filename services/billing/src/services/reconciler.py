from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from uuid import UUID

from clients.payment_provider import PaymentProviderGateway
from domain.subscription import ReconcileSource
from repositories.purchase import PaymentRecordProtocol

CHECK_THROTTLE_WINDOW = timedelta(seconds=5)


@dataclass(frozen=True, slots=True)
class ReconcileOutcome:
    payment_id: UUID
    user_id: UUID
    status: str
    confirmation_url: str | None
    failure_reason: str | None
    transitioned: bool
    throttled: bool


class ReconcileRepositoryProtocol(Protocol):
    async def get_payment(
        self,
        payment_id: UUID,
    ) -> PaymentRecordProtocol | None: ...

    async def get_payment_by_provider_id(
        self,
        provider_code: str,
        provider_payment_id: str,
    ) -> PaymentRecordProtocol | None: ...

    async def record_payment_checked(
        self,
        payment_id: UUID,
        *,
        confirmation_url: str | None = None,
        provider_details: dict[str, Any] | None = None,
    ) -> PaymentRecordProtocol: ...

    async def finalize_payment_succeeded(
        self,
        payment_id: UUID,
        *,
        source: ReconcileSource,
        provider_details: dict[str, Any] | None = None,
        public_base_url: str = 'http://localhost',
    ) -> tuple[PaymentRecordProtocol, bool]: ...

    async def finalize_payment_failed(
        self,
        payment_id: UUID,
        *,
        failure_reason: str,
        source: ReconcileSource,
        provider_details: dict[str, Any] | None = None,
        public_base_url: str = 'http://localhost',
    ) -> tuple[PaymentRecordProtocol, bool]: ...


class PaymentReconciler:
    """Единый доменный сервис сверки платежей через GET (T-11, инв. 6, 7, 11)."""

    def __init__(
        self,
        *,
        repository: ReconcileRepositoryProtocol,
        payment_provider: PaymentProviderGateway,
        public_base_url: str = 'http://localhost',
    ) -> None:
        self._repo = repository
        self._provider = payment_provider
        self._public_base_url = public_base_url.rstrip('/')

    async def reconcile_payment(
        self,
        payment_id: UUID,
        *,
        source: ReconcileSource,
        respect_throttle: bool = True,
    ) -> ReconcileOutcome:
        payment = await self._repo.get_payment(payment_id)
        if payment is None:
            raise LookupError(f'Payment {payment_id} not found')

        return await self.reconcile_payment_record(
            payment,
            source=source,
            respect_throttle=respect_throttle,
        )

    async def reconcile_by_provider_payment_id(
        self,
        provider_payment_id: str,
        *,
        source: ReconcileSource = 'webhook',
        respect_throttle: bool = False,
    ) -> ReconcileOutcome | None:
        payment = await self._repo.get_payment_by_provider_id(
            self._provider.provider_code,
            provider_payment_id.strip(),
        )
        if payment is None:
            return None

        return await self.reconcile_payment_record(
            payment,
            source=source,
            respect_throttle=respect_throttle,
        )

    async def reconcile_payment_record(
        self,
        payment: PaymentRecordProtocol,
        *,
        source: ReconcileSource,
        respect_throttle: bool,
    ) -> ReconcileOutcome:
        # Инвариант 7: если платёж в итоговом статусе, не вызываем ЮKassa
        if payment.status != 'pending':
            return ReconcileOutcome(
                payment_id=payment.id,
                user_id=payment.user_id,
                status=payment.status,
                confirmation_url=payment.confirmation_url,
                failure_reason=payment.failure_reason,
                transitioned=False,
                throttled=False,
            )

        if not payment.provider_payment_id:
            return ReconcileOutcome(
                payment_id=payment.id,
                user_id=payment.user_id,
                status='pending',
                confirmation_url=payment.confirmation_url,
                failure_reason=None,
                transitioned=False,
                throttled=False,
            )

        now_utc = datetime.now(UTC)
        if (
            respect_throttle
            and payment.last_checked_at is not None
            and (now_utc - payment.last_checked_at) < CHECK_THROTTLE_WINDOW
        ):
            return ReconcileOutcome(
                payment_id=payment.id,
                user_id=payment.user_id,
                status='pending',
                confirmation_url=payment.confirmation_url,
                failure_reason=None,
                transitioned=False,
                throttled=True,
            )

        snapshot = await self._provider.get_payment(
            payment.provider_payment_id,
            expected_payment_id=payment.id,
            expected_amount_kopeks=payment.amount_kopeks,
            expected_currency=payment.currency.strip(),
        )

        if snapshot.domain_status == 'pending':
            confirmation_url = snapshot.confirmation_url or payment.confirmation_url
            updated = await self._repo.record_payment_checked(
                payment.id,
                confirmation_url=confirmation_url,
                provider_details=snapshot.provider_details,
            )
            return ReconcileOutcome(
                payment_id=updated.id,
                user_id=updated.user_id,
                status=updated.status,
                confirmation_url=updated.confirmation_url,
                failure_reason=updated.failure_reason,
                transitioned=False,
                throttled=False,
            )

        if snapshot.domain_status == 'succeeded':
            updated, transitioned = await self._repo.finalize_payment_succeeded(
                payment.id,
                source=source,
                provider_details=snapshot.provider_details,
                public_base_url=self._public_base_url,
            )
            return ReconcileOutcome(
                payment_id=updated.id,
                user_id=updated.user_id,
                status=updated.status,
                confirmation_url=updated.confirmation_url,
                failure_reason=updated.failure_reason,
                transitioned=transitioned,
                throttled=False,
            )

        reason = snapshot.failure_reason or 'canceled_by_provider'
        updated, transitioned = await self._repo.finalize_payment_failed(
            payment.id,
            failure_reason=reason,
            source=source,
            provider_details=snapshot.provider_details,
            public_base_url=self._public_base_url,
        )
        return ReconcileOutcome(
            payment_id=updated.id,
            user_id=updated.user_id,
            status=updated.status,
            confirmation_url=updated.confirmation_url,
            failure_reason=updated.failure_reason,
            transitioned=transitioned,
            throttled=False,
        )
