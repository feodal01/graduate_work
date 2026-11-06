from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch
from uuid import UUID, uuid4

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from users.auth_backend import (
    AUTH_ACCESS_TOKEN_SESSION_KEY,
    AUTH_REFRESH_TOKEN_SESSION_KEY,
)
from users.tests.fakes import FakeAdminAuthClient

User = get_user_model()


class PaymentCardT28Tests(TestCase):
    def setUp(self) -> None:
        self.user = User.objects.create(
            username='card_admin',
            is_active=True,
            is_staff=True,
            is_superuser=True,
        )
        self.user.set_unusable_password()
        self.user.save()
        self.client.force_login(
            self.user,
            backend='users.auth_backend.AuthServiceBackend',
        )
        session = self.client.session
        session[AUTH_ACCESS_TOKEN_SESSION_KEY] = 'access-token'
        session[AUTH_REFRESH_TOKEN_SESSION_KEY] = 'refresh-token'
        session.save()
        self.auth_client = FakeAdminAuthClient()

    def _get(
        self,
        url: str,
        *,
        billing_client: object | None = None,
        lookup_client: object | None = None,
    ):
        from billing.clients import get_auth_lookup_client, get_billing_client

        b_client = billing_client or get_billing_client()
        l_client = lookup_client or get_auth_lookup_client()
        with (
            patch(
                'users.auth_middleware.get_auth_client',
                return_value=self.auth_client,
            ),
            patch(
                'billing.views.get_billing_client',
                return_value=b_client,
            ),
            patch(
                'billing.views.get_auth_lookup_client',
                return_value=l_client,
            ),
        ):
            return self.client.get(
                url,
                headers={'X-Request-Id': 't28-card-req'},
            )

    def test_payment_card_renders_all_blocks_and_conditional_buttons(self) -> None:
        """T-28 (ADM-FR-04, US-A-03, NFR-U-03): карточка платежа, блоки, пояса и кнопки."""
        from billing.clients import (
            BillingOperationEventItem,
            BillingOutboxTaskItem,
            BillingPaymentCardData,
            BillingRefundItem,
            BillingSubscriptionItem,
            UserLookupProfile,
        )

        user_id = UUID('11111111-1111-4111-8111-111111111111')
        admin_id = UUID('22222222-2222-4222-8222-222222222222')
        pay_id = UUID('33333333-3333-4333-8333-333333333333')
        sub_id = UUID('44444444-4444-4444-8444-444444444444')
        ref_id = UUID('55555555-5555-4555-8555-555555555555')
        task_ok_id = UUID('66666666-6666-4666-8666-666666666666')
        task_fail_id = UUID('77777777-7777-4777-8777-777777777777')

        t0 = datetime(2026, 11, 5, 10, 0, tzinfo=timezone.utc)
        t1 = datetime(2026, 12, 5, 10, 0, tzinfo=timezone.utc)

        sample_card = BillingPaymentCardData(
            id=pay_id,
            user_id=user_id,
            tariff_code='premium',
            tariff_name='Подписка «Премиум»',
            provider_code='yookassa',
            provider_payment_id='yk_card_adm_01',
            amount_kopeks=29900,
            currency='RUB',
            status='succeeded',
            failure_reason=None,
            created_at=t0,
            updated_at=t0,
            can_refund=True,
            can_reconcile_payment=False,
            subscription=BillingSubscriptionItem(
                id=sub_id,
                user_id=user_id,
                payment_id=pay_id,
                tariff_code='premium',
                tariff_name='Подписка «Премиум»',
                starts_at=t0,
                ends_at=t1,
                status='active',
                amount_kopeks=29900,
                currency='RUB',
                created_at=t0,
            ),
            refunds=(
                BillingRefundItem(
                    id=ref_id,
                    payment_id=pay_id,
                    subscription_id=sub_id,
                    provider_code='yookassa',
                    provider_refund_id='yk_ref_adm_01',
                    amount_kopeks=29900,
                    currency='RUB',
                    status='rejected',
                    reason='Клиент обратился в поддержку',
                    rejection_reason='rejected_by_provider',
                    admin_user_id=admin_id,
                    created_at=t0,
                    updated_at=t0,
                    can_reconcile=False,
                ),
            ),
            operation_events=(
                BillingOperationEventItem(
                    id=uuid4(),
                    operation_type='payment',
                    operation_id=pay_id,
                    from_status=None,
                    to_status='pending',
                    source='continue',
                    details={'created': True},
                    created_at=t0,
                ),
                BillingOperationEventItem(
                    id=uuid4(),
                    operation_type='payment',
                    operation_id=pay_id,
                    from_status='pending',
                    to_status='succeeded',
                    source='webhook',
                    details={'id': 'yk_card_adm_01'},
                    created_at=t0,
                ),
            ),
            outbox_tasks=(
                BillingOutboxTaskItem(
                    id=task_ok_id,
                    target_service='auth',
                    task_type='sync_access',
                    access_version=1,
                    status='completed',
                    attempts=1,
                    last_error=None,
                    created_at=t0,
                    updated_at=t0,
                    can_retry_or_close=False,
                ),
                BillingOutboxTaskItem(
                    id=task_fail_id,
                    target_service='notifications',
                    task_type='payment_succeeded_email',
                    access_version=None,
                    status='requires_intervention',
                    attempts=20,
                    last_error='Notifications HTTP 500',
                    created_at=t0,
                    updated_at=t0,
                    can_retry_or_close=True,
                ),
            ),
        )

        class CardBilling:
            def __init__(self, card: BillingPaymentCardData) -> None:
                self.card = card

            def get_payment_card(
                self, payment_id: UUID, *, request_id: str
            ) -> BillingPaymentCardData:
                assert payment_id == pay_id
                return self.card

        class CardLookup:
            def __init__(self) -> None:
                self.batch_calls: list[tuple[UUID, ...]] = []

            def get_by_ids(
                self, user_ids: list[UUID], *, request_id: str
            ) -> dict[UUID, UserLookupProfile]:
                self.batch_calls.append(tuple(user_ids))
                return {
                    user_id: UserLookupProfile(
                        user_id=user_id,
                        login='alice_card_user',
                        tz='Asia/Yekaterinburg',
                    ),
                    admin_id: UserLookupProfile(
                        user_id=admin_id,
                        login='support_operator_1',
                        tz='Europe/Moscow',
                    ),
                }

        bil = CardBilling(sample_card)
        lkp = CardLookup()

        res = self._get(
            reverse('admin:billing_payment_detail', args=[pay_id]),
            billing_client=bil,
            lookup_client=lkp,
        )
        self.assertEqual(res.status_code, 200)
        # Ровно 1 пакетный запрос lookup для user_id + admin_user_id (ADM-NFR-P-01)
        self.assertEqual(len(lkp.batch_calls), 1)
        self.assertEqual(set(lkp.batch_calls[0]), {user_id, admin_id})

        # Основные поля, пояса и блоки
        self.assertContains(res, 'alice_card_user')
        self.assertContains(res, 'support_operator_1')
        self.assertContains(res, 'yk_card_adm_01')
        self.assertContains(res, '05.11.2026 10:00 UTC')
        self.assertContains(res, '05.11.2026 15:00 (Asia/Yekaterinburg)')
        self.assertContains(res, 'Клиент обратился в поддержку')
        self.assertContains(res, 'webhook')
        self.assertContains(res, 'continue')
        self.assertContains(res, 'Notifications HTTP 500')
        # Кнопка «Вернуть» видна при can_refund=True
        self.assertContains(res, 'Вернуть')
        # Кнопки «Повторить» и «Закрыть с причиной» видны для requires_intervention
        self.assertContains(res, 'Повторить')
        self.assertContains(res, 'Закрыть с причиной')

    def test_payment_card_unhappy_paths_not_found_auth_down_and_billing_down(
        self,
    ) -> None:
        """T-28 (ADM-NFR-R-01, ADM-NFR-R-02, BIL-API-08): 404, отказ Auth и отказ Billing."""
        from billing.clients import (
            AuthLookupUnavailableError,
            BillingNotFoundError,
            BillingPaymentCardData,
            BillingUnavailableError,
        )

        pay_id = UUID('88888888-8888-4888-8888-888888888888')
        user_id = UUID('99999999-9999-4999-8999-999999999999')
        t0 = datetime(2026, 11, 5, 10, 0, tzinfo=timezone.utc)

        class ModeBilling:
            def __init__(self) -> None:
                self.mode = 'ok'

            def get_payment_card(
                self, _payment_id: UUID, *, request_id: str
            ) -> BillingPaymentCardData:
                if self.mode == 'not_found':
                    raise BillingNotFoundError('payment not found')
                if self.mode == 'down':
                    raise BillingUnavailableError('billing down')
                return BillingPaymentCardData(
                    id=pay_id,
                    user_id=user_id,
                    tariff_code='premium',
                    tariff_name='Подписка «Премиум»',
                    provider_code='yookassa',
                    provider_payment_id='yk_pending_01',
                    amount_kopeks=29900,
                    currency='RUB',
                    status='pending',
                    failure_reason=None,
                    created_at=t0,
                    updated_at=t0,
                    can_refund=False,
                    can_reconcile_payment=True,
                    subscription=None,
                    refunds=(),
                    operation_events=(),
                    outbox_tasks=(),
                )

        class DownLookup:
            def get_by_ids(self, _user_ids: list[UUID], *, request_id: str):
                raise AuthLookupUnavailableError('auth down')

        bil = ModeBilling()
        down_lkp = DownLookup()

        # 1. При недоступном Auth карточка открывается (200 OK), показывает UUID, UTC
        # и кнопку «Проверить в ЮKassa» для pending-платежа, но скрывает «Вернуть»
        res_auth_down = self._get(
            reverse('admin:billing_payment_detail', args=[pay_id]),
            billing_client=bil,
            lookup_client=down_lkp,
        )
        self.assertEqual(res_auth_down.status_code, 200)
        self.assertContains(res_auth_down, str(user_id))
        self.assertContains(res_auth_down, '05.11.2026 10:00 UTC')
        self.assertContains(res_auth_down, 'Проверить в ЮKassa')
        self.assertNotContains(res_auth_down, '>Вернуть<')

        # 2. Несуществующий платёж -> 404
        bil.mode = 'not_found'
        res_404 = self._get(
            reverse('admin:billing_payment_detail', args=[pay_id]),
            billing_client=bil,
            lookup_client=down_lkp,
        )
        self.assertEqual(res_404.status_code, 404)

        # 3. Недоступный Billing -> 503
        bil.mode = 'down'
        res_503 = self._get(
            reverse('admin:billing_payment_detail', args=[pay_id]),
            billing_client=bil,
            lookup_client=down_lkp,
        )
        self.assertEqual(res_503.status_code, 503)
        self.assertContains(
            res_503,
            'Сервис биллинга временно недоступен',
            status_code=503,
        )

    def test_billing_admin_client_get_payment_card_contract(self) -> None:
        """T-28: проверка HTTP-клиента get_payment_card (200, 404, невалидный JSON)."""
        import httpx

        from billing.clients import (
            BillingAdminClient,
            BillingContractError,
            BillingNotFoundError,
        )

        pid = UUID('11111111-1111-4111-8111-111111111111')
        uid = UUID('22222222-2222-4222-8222-222222222222')
        sid = UUID('33333333-3333-4333-8333-333333333333')
        rid = UUID('44444444-4444-4444-8444-444444444444')
        ev_id = UUID('55555555-5555-4555-8555-555555555555')
        tid = UUID('66666666-6666-4666-8666-666666666666')

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith(str(pid)):
                return httpx.Response(
                    200,
                    json={
                        'id': str(pid),
                        'user_id': str(uid),
                        'tariff_code': 'premium',
                        'tariff_name': 'Подписка «Премиум»',
                        'provider_code': 'yookassa',
                        'provider_payment_id': 'yk_card_1',
                        'amount_kopeks': 29900,
                        'currency': 'RUB',
                        'status': 'succeeded',
                        'failure_reason': None,
                        'created_at': '2026-11-05T10:00:00Z',
                        'updated_at': '2026-11-05T10:00:00Z',
                        'can_refund': True,
                        'can_reconcile_payment': False,
                        'subscription': {
                            'id': str(sid),
                            'user_id': str(uid),
                            'payment_id': str(pid),
                            'tariff_code': 'premium',
                            'tariff_name': 'Подписка «Премиум»',
                            'starts_at': '2026-11-05T10:00:00Z',
                            'ends_at': '2026-12-05T10:00:00Z',
                            'status': 'active',
                            'amount_kopeks': 29900,
                            'currency': 'RUB',
                            'created_at': '2026-11-05T10:00:00Z',
                        },
                        'refunds': [
                            {
                                'id': str(rid),
                                'payment_id': str(pid),
                                'subscription_id': str(sid),
                                'provider_code': 'yookassa',
                                'provider_refund_id': None,
                                'amount_kopeks': 29900,
                                'currency': 'RUB',
                                'status': 'rejected',
                                'reason': 'Тест',
                                'rejection_reason': 'err',
                                'admin_user_id': str(uid),
                                'created_at': '2026-11-05T10:00:00Z',
                                'updated_at': '2026-11-05T10:00:00Z',
                                'can_reconcile': False,
                            }
                        ],
                        'operation_events': [
                            {
                                'id': str(ev_id),
                                'operation_type': 'payment',
                                'operation_id': str(pid),
                                'from_status': 'pending',
                                'to_status': 'succeeded',
                                'source': 'webhook',
                                'details': {'ok': True},
                                'created_at': '2026-11-05T10:00:00Z',
                            }
                        ],
                        'outbox_tasks': [
                            {
                                'id': str(tid),
                                'target_service': 'auth',
                                'task_type': 'sync_access',
                                'access_version': 1,
                                'status': 'completed',
                                'attempts': 1,
                                'last_error': None,
                                'created_at': '2026-11-05T10:00:00Z',
                                'updated_at': '2026-11-05T10:00:00Z',
                                'can_retry_or_close': False,
                            }
                        ],
                    },
                )
            return httpx.Response(404, json={'error_code': 'payment_not_found'})

        client = BillingAdminClient(
            base_url='http://billing:8000/api/v1/internal/billing',
            service_api_key='secret',
            http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        )
        card = client.get_payment_card(pid, request_id='req-c')
        self.assertEqual(card.id, pid)
        self.assertTrue(card.can_refund)
        self.assertEqual(len(card.refunds), 1)
        self.assertEqual(len(card.operation_events), 1)
        self.assertEqual(len(card.outbox_tasks), 1)

        with self.assertRaises(BillingNotFoundError):
            client.get_payment_card(uuid4(), request_id='req-404')

        bad_client = BillingAdminClient(
            base_url='http://billing:8000/api/v1/internal/billing',
            service_api_key='secret',
            http_client=httpx.Client(
                transport=httpx.MockTransport(
                    lambda _r: httpx.Response(200, json={'invalid': True})
                )
            ),
        )
        with self.assertRaises(BillingContractError):
            bad_client.get_payment_card(pid, request_id='req-bad')
