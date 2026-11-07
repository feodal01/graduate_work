from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch
from uuid import UUID, uuid4

from django.contrib.auth import get_user_model
from django.test import Client, TestCase
from django.urls import reverse
from users.auth_backend import (
    AUTH_ACCESS_TOKEN_SESSION_KEY,
    AUTH_REFRESH_TOKEN_SESSION_KEY,
)
from users.models import AuthIdentity
from users.tests.fakes import FakeAdminAuthClient

User = get_user_model()


class RefundConfirmationT30Tests(TestCase):
    def setUp(self) -> None:
        self.admin_auth_uuid = UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa')
        self.user = User.objects.create(
            username='refund_admin',
            is_active=True,
            is_staff=True,
            is_superuser=True,
        )
        self.user.set_unusable_password()
        self.user.save()
        AuthIdentity.objects.create(
            user=self.user,
            auth_user_id=self.admin_auth_uuid,
        )
        self.client.force_login(
            self.user,
            backend='users.auth_backend.AuthServiceBackend',
        )
        session = self.client.session
        session[AUTH_ACCESS_TOKEN_SESSION_KEY] = 'access-token'
        session[AUTH_REFRESH_TOKEN_SESSION_KEY] = 'refresh-token'
        session.save()
        self.auth_client = FakeAdminAuthClient()

    def _request(
        self,
        method: str,
        url: str,
        data: dict[str, str] | None = None,
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
            return getattr(self.client, method)(
                url,
                data=data or {},
                headers={'X-Request-Id': 't30-refund-req'},
            )

    def test_unauthenticated_and_csrf_protection_step(self) -> None:
        """T-30 (ADM-NFR-SEC-01): защита входа и CSRF на форме возврата."""
        pay_id = uuid4()
        url = reverse('admin:billing_payment_refund', args=[pay_id])

        anon_client = Client()
        anon_res = anon_client.get(url)
        self.assertEqual(anon_res.status_code, 302)
        self.assertIn('/admin/login/', anon_res['Location'])

        csrf_client = Client(enforce_csrf_checks=True)
        csrf_client.force_login(
            self.user,
            backend='users.auth_backend.AuthServiceBackend',
        )
        sess = csrf_client.session
        sess[AUTH_ACCESS_TOKEN_SESSION_KEY] = 'access-token'
        sess[AUTH_REFRESH_TOKEN_SESSION_KEY] = 'refresh-token'
        sess.save()
        with patch(
            'users.auth_middleware.get_auth_client',
            return_value=self.auth_client,
        ):
            no_csrf = csrf_client.post(url, data={'reason': 'Причина без CSRF'})
        self.assertEqual(no_csrf.status_code, 403)

    def test_refund_confirm_get_and_post_happy_and_empty_reason_unhappy(self) -> None:
        """T-30 (ADM-FR-05, US-A-04): страница подтверждения, пустая причина и отправка возврата."""
        from billing.clients import (
            BillingPaymentCardData,
            BillingRefundItem,
            BillingSubscriptionItem,
            UserLookupProfile,
        )

        user_id = UUID('11111111-1111-4111-8111-111111111111')
        pay_id = UUID('22222222-2222-4222-8222-222222222222')
        sub_id = UUID('33333333-3333-4333-8333-333333333333')
        ref_id = UUID('44444444-4444-4444-8444-444444444444')
        t0 = datetime(2026, 11, 5, 10, 0, tzinfo=timezone.utc)
        t1 = datetime(2026, 12, 5, 10, 0, tzinfo=timezone.utc)

        class RefundBilling:
            def __init__(self) -> None:
                self.create_refund_calls: list[dict[str, object]] = []

            def get_payment_card(
                self, payment_id: UUID, *, request_id: str
            ) -> BillingPaymentCardData:
                return BillingPaymentCardData(
                    id=payment_id,
                    user_id=user_id,
                    tariff_code='premium',
                    tariff_name='Подписка «Премиум»',
                    provider_code='yookassa',
                    provider_payment_id='yk_ref_confirm_01',
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
                        payment_id=payment_id,
                        tariff_code='premium',
                        tariff_name='Подписка «Премиум»',
                        starts_at=t0,
                        ends_at=t1,
                        status='active',
                        amount_kopeks=29900,
                        currency='RUB',
                        created_at=t0,
                    ),
                    refunds=(),
                    operation_events=(),
                    outbox_tasks=(),
                )

            def create_refund(
                self,
                payment_id: UUID,
                *,
                admin_user_id: UUID,
                reason: str,
                request_id: str,
            ) -> BillingRefundItem:
                self.create_refund_calls.append(
                    {
                        'payment_id': payment_id,
                        'admin_user_id': admin_user_id,
                        'reason': reason,
                        'request_id': request_id,
                    }
                )
                return BillingRefundItem(
                    id=ref_id,
                    payment_id=payment_id,
                    subscription_id=sub_id,
                    provider_code='yookassa',
                    provider_refund_id='yk_ref_created_01',
                    amount_kopeks=29900,
                    currency='RUB',
                    status='succeeded',
                    reason=reason,
                    rejection_reason=None,
                    admin_user_id=admin_user_id,
                    created_at=t0,
                    updated_at=t0,
                    can_reconcile=False,
                )

        class RefundLookup:
            def get_by_ids(
                self, user_ids: list[UUID], *, request_id: str
            ) -> dict[UUID, UserLookupProfile]:
                return {
                    user_id: UserLookupProfile(
                        user_id=user_id,
                        login='alice_refund_user',
                        tz='Asia/Yekaterinburg',
                    )
                }

        bil = RefundBilling()
        lkp = RefundLookup()
        url = reverse('admin:billing_payment_refund', args=[pay_id])

        # 1. GET страницы подтверждения
        get_res = self._request('get', url, billing_client=bil, lookup_client=lkp)
        self.assertEqual(get_res.status_code, 200)
        self.assertContains(get_res, 'alice_refund_user')
        self.assertContains(get_res, '299 ₽')
        self.assertContains(get_res, '05.11.2026 10:00 UTC')
        self.assertContains(get_res, '05.11.2026 15:00 (Asia/Yekaterinburg)')
        self.assertContains(get_res, 'доступ к новинкам закроется')

        # 2. Unhappy: POST с пустой причиной -> ошибка формы и 0 вызовов Billing
        for blank in ('', '   '):
            bad_post = self._request(
                'post',
                url,
                data={'reason': blank},
                billing_client=bil,
                lookup_client=lkp,
            )
            self.assertEqual(bad_post.status_code, 400)
            self.assertContains(
                bad_post,
                'Укажите причину возврата',
                status_code=400,
            )
            self.assertEqual(bil.create_refund_calls, [])

        # 3. Happy: POST с непустой причиной -> вызов create_refund и редирект в карточку платежа
        ok_post = self._request(
            'post',
            url,
            data={'reason': '  Ошибочное списание по обращению #104  '},
            billing_client=bil,
            lookup_client=lkp,
        )
        self.assertEqual(ok_post.status_code, 302)
        self.assertEqual(
            ok_post['Location'],
            reverse('admin:billing_payment_detail', args=[pay_id]),
        )
        self.assertEqual(len(bil.create_refund_calls), 1)
        self.assertEqual(
            bil.create_refund_calls[0],
            {
                'payment_id': pay_id,
                'admin_user_id': self.admin_auth_uuid,
                'reason': 'Ошибочное списание по обращению #104',
                'request_id': 't30-refund-req',
            },
        )

    def test_refund_confirm_unhappy_conflict_auth_down_and_billing_down(self) -> None:
        """T-30 (ADM-FR-05, ADM-NFR-R-01, ADM-NFR-R-02): 409, отказ Auth и отказ Billing."""
        from billing.clients import (
            AuthLookupUnavailableError,
            BillingConflictError,
            BillingNotFoundError,
            BillingPaymentCardData,
            BillingRefundItem,
            BillingSubscriptionItem,
            BillingUnavailableError,
        )

        user_id = UUID('55555555-5555-4555-8555-555555555555')
        pay_id = UUID('66666666-6666-4666-8666-666666666666')
        t0 = datetime(2026, 11, 5, 10, 0, tzinfo=timezone.utc)

        class ConflictBilling:
            def __init__(self) -> None:
                self.mode = 'conflict'

            def get_payment_card(
                self, payment_id: UUID, *, request_id: str
            ) -> BillingPaymentCardData:
                if self.mode == 'not_found':
                    raise BillingNotFoundError('not found')
                if self.mode == 'down':
                    raise BillingUnavailableError('billing down')
                return BillingPaymentCardData(
                    id=payment_id,
                    user_id=user_id,
                    tariff_code='premium',
                    tariff_name='Подписка «Премиум»',
                    provider_code='yookassa',
                    provider_payment_id='yk_1',
                    amount_kopeks=29900,
                    currency='RUB',
                    status='succeeded',
                    failure_reason=None,
                    created_at=t0,
                    updated_at=t0,
                    can_refund=(self.mode == 'ok'),
                    can_reconcile_payment=False,
                    subscription=BillingSubscriptionItem(
                        id=UUID('88888888-8888-4888-8888-888888888888'),
                        user_id=user_id,
                        payment_id=payment_id,
                        tariff_code='premium',
                        tariff_name='Подписка «Премиум»',
                        starts_at=t0,
                        ends_at=t0,
                        status='active',
                        amount_kopeks=29900,
                        currency='RUB',
                        created_at=t0,
                    ),
                    refunds=(),
                    operation_events=(),
                    outbox_tasks=(),
                )

            def create_refund(self, payment_id: UUID, **_kwargs: object):
                if self.mode == 'ok':
                    return BillingRefundItem(
                        id=UUID('77777777-7777-4777-8777-777777777777'),
                        payment_id=payment_id,
                        subscription_id=UUID('88888888-8888-4888-8888-888888888888'),
                        provider_code='yookassa',
                        provider_refund_id='yk_ref_ok',
                        amount_kopeks=29900,
                        currency='RUB',
                        status='succeeded',
                        reason='Возврат при недоступном Auth',
                        rejection_reason=None,
                        admin_user_id=user_id,
                        created_at=t0,
                        updated_at=t0,
                        can_reconcile=False,
                    )
                if self.mode == 'conflict':
                    raise BillingConflictError(
                        'Возврат возможен только для платежа текущей активной подписки'
                    )
                if self.mode == 'down':
                    raise BillingUnavailableError('billing down')
                raise BillingNotFoundError('not found')

        class DownLookup:
            def get_by_ids(self, _user_ids: list[UUID], *, request_id: str):
                raise AuthLookupUnavailableError('auth down')

        bil = ConflictBilling()
        down_lkp = DownLookup()
        url = reverse('admin:billing_payment_refund', args=[pay_id])

        # 1. Прямой POST при запрещённом возврате -> 409 с текстом при недоступном Auth
        conflict_post = self._request(
            'post',
            url,
            data={'reason': 'Попытка возврата завершённой подписки'},
            billing_client=bil,
            lookup_client=down_lkp,
        )
        self.assertEqual(conflict_post.status_code, 409)
        self.assertContains(
            conflict_post,
            'Возврат возможен только для платежа текущей активной подписки',
            status_code=409,
        )

        # 2. Несуществующий платёж -> 404
        bil.mode = 'not_found'
        res_404 = self._request('get', url, billing_client=bil, lookup_client=down_lkp)
        self.assertEqual(res_404.status_code, 404)

        # 3. Недоступный Billing на GET и POST -> 503
        bil.mode = 'down'
        res_get_503 = self._request('get', url, billing_client=bil, lookup_client=down_lkp)
        self.assertEqual(res_get_503.status_code, 503)
        res_post_503 = self._request(
            'post',
            url,
            data={'reason': 'Причина при сбое Billing'},
            billing_client=bil,
            lookup_client=down_lkp,
        )
        self.assertEqual(res_post_503.status_code, 503)

        # 4. Строки 12 и 13 матрицы 6.3 (T-37): после восстановления Billing (и при всё ещё
        #    недоступном Auth) страница подтверждения открывается с UUID и временем в UTC,
        #    а повторная отправка формы возврата проходит штатно (302)
        bil.mode = 'ok'
        res_get_auth_down = self._request('get', url, billing_client=bil, lookup_client=down_lkp)
        self.assertEqual(res_get_auth_down.status_code, 200)
        self.assertContains(res_get_auth_down, str(user_id))
        self.assertContains(res_get_auth_down, '05.11.2026 10:00 UTC')

        res_post_auth_down = self._request(
            'post',
            url,
            data={'reason': 'Повторная отправка после восстановления Billing'},
            billing_client=bil,
            lookup_client=down_lkp,
        )
        self.assertEqual(res_post_auth_down.status_code, 302)
        self.assertEqual(
            res_post_auth_down['Location'],
            reverse('admin:billing_payment_detail', args=[pay_id]),
        )

    def test_billing_admin_client_create_refund_contract(self) -> None:
        """T-30: проверка HTTP-клиента create_refund (200, 404, 409, 503)."""
        import httpx

        from billing.clients import (
            BillingAdminClient,
            BillingConflictError,
            BillingNotFoundError,
            BillingUnavailableError,
        )

        pid = UUID('11111111-1111-4111-8111-111111111111')
        sid = UUID('22222222-2222-4222-8222-222222222222')
        rid = UUID('33333333-3333-4333-8333-333333333333')
        aid = UUID('44444444-4444-4444-8444-444444444444')
        mode = 'ok'

        def handler(request: httpx.Request) -> httpx.Response:
            if mode == 'ok':
                return httpx.Response(
                    200,
                    json={
                        'id': str(rid),
                        'payment_id': str(pid),
                        'subscription_id': str(sid),
                        'provider_code': 'yookassa',
                        'provider_refund_id': 'yk_ref_1',
                        'amount_kopeks': 29900,
                        'currency': 'RUB',
                        'status': 'succeeded',
                        'reason': 'Причина',
                        'rejection_reason': None,
                        'admin_user_id': str(aid),
                        'created_at': '2026-11-05T10:00:00Z',
                        'updated_at': '2026-11-05T10:00:00Z',
                        'can_reconcile': False,
                    },
                )
            if mode == '404':
                return httpx.Response(404, json={'error_code': 'payment_not_found'})
            if mode == '409':
                return httpx.Response(
                    409,
                    json={
                        'error_code': 'refund_not_allowed',
                        'detail': 'Возврат не разрешён',
                    },
                )
            return httpx.Response(503, json={'error_code': 'database_unavailable'})

        client = BillingAdminClient(
            base_url='http://billing:8000/api/v1/internal/billing',
            service_api_key='secret',
            http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        )
        ref = client.create_refund(
            pid,
            admin_user_id=aid,
            reason='Причина',
            request_id='req-1',
        )
        self.assertEqual(ref.id, rid)
        self.assertEqual(ref.status, 'succeeded')

        mode = '404'
        with self.assertRaises(BillingNotFoundError):
            client.create_refund(pid, admin_user_id=aid, reason='Причина', request_id='req-2')

        mode = '409'
        with self.assertRaises(BillingConflictError) as ctx:
            client.create_refund(pid, admin_user_id=aid, reason='Причина', request_id='req-3')
        self.assertEqual(ctx.exception.detail, 'Возврат не разрешён')

        mode = '503'
        with self.assertRaises(BillingUnavailableError):
            client.create_refund(pid, admin_user_id=aid, reason='Причина', request_id='req-4')
