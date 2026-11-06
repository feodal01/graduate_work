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


class BillingListsT27Tests(TestCase):
    def setUp(self) -> None:
        self.user = User.objects.create(
            username='support_admin',
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
        params: dict[str, str] | None = None,
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
                data=params or {},
                headers={'X-Request-Id': 't27-admin-req'},
            )

    def test_unauthenticated_access_redirects_to_admin_login_step(self) -> None:
        """T-27 (ADM-NFR-SEC-01): неавторизованный доступ перенаправляется на вход."""
        self.client.logout()
        sub_res = self.client.get('/admin/billing/subscriptions/')
        pay_res = self.client.get('/admin/billing/payments/')
        self.assertEqual(sub_res.status_code, 302)
        self.assertIn('/admin/login/', sub_res['Location'])
        self.assertEqual(pay_res.status_code, 302)
        self.assertIn('/admin/login/', pay_res['Location'])

    def test_subscriptions_search_by_login_and_uuid_timezone_and_batch_lookup(
        self,
    ) -> None:
        """T-27 (ADM-FR-01, ADM-FR-02, ADM-NFR-P-01, NFR-U-03): поиск, пояса и N+1."""
        from billing.clients import (
            BillingSubscriptionItem,
            BillingSubscriptionsPage,
            UserLookupProfile,
        )

        alice_id = UUID('11111111-1111-4111-8111-111111111111')
        pay_id = UUID('22222222-2222-4222-8222-222222222222')
        sub_id = UUID('33333333-3333-4333-8333-333333333333')

        starts_utc = datetime(2026, 11, 5, 10, 0, tzinfo=timezone.utc)
        ends_utc = datetime(2026, 12, 5, 10, 0, tzinfo=timezone.utc)

        class FakeBilling:
            def __init__(self) -> None:
                self.sub_calls: list[dict[str, object]] = []

            def list_subscriptions(self, **kwargs: object) -> BillingSubscriptionsPage:
                self.sub_calls.append(dict(kwargs))
                uid_filter = kwargs.get('user_id')
                if uid_filter is not None and uid_filter != alice_id:
                    return BillingSubscriptionsPage(total=0, items=())
                return BillingSubscriptionsPage(
                    total=1,
                    items=(
                        BillingSubscriptionItem(
                            id=sub_id,
                            user_id=alice_id,
                            payment_id=pay_id,
                            tariff_code='premium',
                            tariff_name='Подписка «Премиум»',
                            starts_at=starts_utc,
                            ends_at=ends_utc,
                            status='active',
                            amount_kopeks=29900,
                            currency='RUB',
                            created_at=starts_utc,
                        ),
                    ),
                )

        class FakeLookup:
            def __init__(self) -> None:
                self.login_calls: list[str] = []
                self.batch_calls: list[tuple[UUID, ...]] = []

            def find_by_login(self, login: str, *, request_id: str) -> UserLookupProfile | None:
                self.login_calls.append(login)
                if login == 'alice_ekb':
                    return UserLookupProfile(
                        user_id=alice_id,
                        login='alice_ekb',
                        tz='Asia/Yekaterinburg',
                    )
                return None

            def get_by_ids(
                self, user_ids: list[UUID], *, request_id: str
            ) -> dict[UUID, UserLookupProfile]:
                self.batch_calls.append(tuple(user_ids))
                return {
                    uid: UserLookupProfile(
                        user_id=uid,
                        login='alice_ekb' if uid == alice_id else f'user_{uid.hex[:6]}',
                        tz='Asia/Yekaterinburg',
                    )
                    for uid in user_ids
                }

        fake_bil = FakeBilling()
        fake_lkp = FakeLookup()

        # 1. Поиск по логину (alice_ekb) и поиск по UUID возвращают одинаковый список
        by_login = self._get(
            reverse('admin:billing_subscriptions'),
            {'q': 'alice_ekb'},
            billing_client=fake_bil,
            lookup_client=fake_lkp,
        )
        self.assertEqual(by_login.status_code, 200)
        self.assertContains(by_login, 'alice_ekb')
        self.assertContains(by_login, str(alice_id))
        # Колонка UTC: 05.11.2026 10:00 UTC -> 05.12.2026 10:00 UTC
        self.assertContains(by_login, '05.11.2026 10:00')
        # Колонка пояса пользователя (Asia/Yekaterinburg = UTC+5): 05.11.2026 15:00
        self.assertContains(by_login, '05.11.2026 15:00')
        self.assertContains(by_login, 'Asia/Yekaterinburg')
        self.assertContains(
            by_login,
            reverse('admin:billing_payment_detail', args=[pay_id]),
        )

        by_uuid = self._get(
            reverse('admin:billing_subscriptions'),
            {'q': str(alice_id)},
            billing_client=fake_bil,
            lookup_client=fake_lkp,
        )
        self.assertEqual(by_uuid.status_code, 200)
        self.assertContains(by_uuid, 'alice_ekb')
        self.assertContains(by_uuid, '05.11.2026 15:00')

        # 2. Unhappy: несуществующий логин -> пустой список без вызова Billing
        before_calls = len(fake_bil.sub_calls)
        not_found = self._get(
            reverse('admin:billing_subscriptions'),
            {'q': 'nonexistent_login'},
            billing_client=fake_bil,
            lookup_client=fake_lkp,
        )
        self.assertEqual(not_found.status_code, 200)
        self.assertEqual(len(fake_bil.sub_calls), before_calls)
        self.assertNotContains(not_found, str(sub_id))

        # 3. ADM-NFR-P-01: 20 подписок разных пользователей -> ровно 1 пакетный запрос lookup
        twenty_items = tuple(
            BillingSubscriptionItem(
                id=uuid4(),
                user_id=uuid4(),
                payment_id=uuid4(),
                tariff_code='premium',
                tariff_name='Подписка «Премиум»',
                starts_at=starts_utc,
                ends_at=ends_utc,
                status='active',
                amount_kopeks=29900,
                currency='RUB',
                created_at=starts_utc,
            )
            for _ in range(20)
        )
        fake_bil.list_subscriptions = lambda **_kw: BillingSubscriptionsPage(  # type: ignore[method-assign]
            total=20,
            items=twenty_items,
        )
        fake_lkp.batch_calls.clear()
        batch_res = self._get(
            reverse('admin:billing_subscriptions'),
            billing_client=fake_bil,
            lookup_client=fake_lkp,
        )
        self.assertEqual(batch_res.status_code, 200)
        self.assertEqual(len(fake_lkp.batch_calls), 1)
        self.assertEqual(len(fake_lkp.batch_calls[0]), 20)

    def test_payments_list_filters_and_auth_or_billing_degradation(self) -> None:
        """T-27 (ADM-FR-01, ADM-NFR-R-01, ADM-NFR-R-02): список платежей и деградация."""
        from billing.clients import (
            AuthLookupUnavailableError,
            BillingPaymentItem,
            BillingPaymentsPage,
            BillingUnavailableError,
        )

        user_id = UUID('44444444-4444-4444-8444-444444444444')
        pay_id = UUID('55555555-5555-4555-8555-555555555555')
        created_utc = datetime(2026, 11, 5, 12, 30, tzinfo=timezone.utc)

        class DegradableBilling:
            def __init__(self) -> None:
                self.fail = False
                self.last_kwargs: dict[str, object] = {}

            def list_subscriptions(self, **_kwargs: object):
                if self.fail:
                    raise BillingUnavailableError('billing down')
                raise AssertionError('unexpected call')

            def list_payments(self, **kwargs: object) -> BillingPaymentsPage:
                if self.fail:
                    raise BillingUnavailableError('billing down')
                self.last_kwargs = dict(kwargs)
                return BillingPaymentsPage(
                    total=1,
                    items=(
                        BillingPaymentItem(
                            id=pay_id,
                            user_id=user_id,
                            tariff_code='premium',
                            tariff_name='Подписка «Премиум»',
                            provider_code='yookassa',
                            provider_payment_id='yk_t27_test_01',
                            amount_kopeks=29900,
                            currency='RUB',
                            status='succeeded',
                            failure_reason=None,
                            has_refund=True,
                            refund_status='processing',
                            active_refund_id=uuid4(),
                            is_stuck=True,
                            created_at=created_utc,
                            updated_at=created_utc,
                        ),
                    ),
                )

        class DownLookup:
            def find_by_login(self, _login: str, *, request_id: str):
                raise AuthLookupUnavailableError('auth lookup down')

            def get_by_ids(self, _user_ids: list[UUID], *, request_id: str):
                raise AuthLookupUnavailableError('auth lookup down')

        bil = DegradableBilling()
        down_lkp = DownLookup()

        # 1. При недоступном Auth список платежей по UUID / вкладкам работает (200 OK),
        # показывает UUID пользователя и время в UTC с предупреждением (ADM-NFR-R-01)
        res_auth_down = self._get(
            reverse('admin:billing_payments'),
            {'tab': 'stuck', 'q': str(user_id)},
            billing_client=bil,
            lookup_client=down_lkp,
        )
        self.assertEqual(res_auth_down.status_code, 200)
        self.assertTrue(bil.last_kwargs.get('stuck'))
        self.assertEqual(bil.last_kwargs.get('user_id'), user_id)
        self.assertContains(res_auth_down, str(user_id))
        self.assertContains(res_auth_down, 'yk_t27_test_01')
        self.assertContains(res_auth_down, '05.11.2026 12:30 UTC')
        self.assertContains(res_auth_down, 'Возврат в обработке')

        # 2. Поиск по логину при недоступном Auth показывает предупреждение использовать UUID
        res_login_auth_down = self._get(
            reverse('admin:billing_subscriptions'),
            {'q': 'some_login'},
            billing_client=bil,
            lookup_client=down_lkp,
        )
        self.assertEqual(res_login_auth_down.status_code, 200)
        self.assertContains(
            res_login_auth_down,
            'Поиск по логину временно недоступен, используйте UUID',
        )

        res_pay_login_auth_down = self._get(
            reverse('admin:billing_payments'),
            {'q': 'some_login'},
            billing_client=bil,
            lookup_client=down_lkp,
        )
        self.assertEqual(res_pay_login_auth_down.status_code, 200)
        self.assertContains(
            res_pay_login_auth_down,
            'Поиск по логину временно недоступен, используйте UUID',
        )

        # 3. Невалидные даты фильтра -> 400 без обращения к Billing (NFR-U-02)
        bad_date_sub = self._get(
            reverse('admin:billing_subscriptions'),
            {'date_from': '2026-12-01', 'date_to': '2026-11-01'},
            billing_client=bil,
            lookup_client=down_lkp,
        )
        self.assertEqual(bad_date_sub.status_code, 400)
        self.assertContains(
            bad_date_sub,
            'Дата начала периода не может быть позже даты окончания',
            status_code=400,
        )

        bad_date_pay = self._get(
            reverse('admin:billing_payments'),
            {'date_from': 'not-a-date'},
            billing_client=bil,
            lookup_client=down_lkp,
        )
        self.assertEqual(bad_date_pay.status_code, 400)

        bad_pid_pay = self._get(
            reverse('admin:billing_payments'),
            {'search_by': 'payment_id', 'q': 'not-a-uuid'},
            billing_client=bil,
            lookup_client=down_lkp,
        )
        self.assertEqual(bad_pid_pay.status_code, 400)

        # 4. При недоступном Billing возвращается штатная страница 503 (ADM-NFR-R-02)
        bil.fail = True
        res_bil_down_sub = self._get(
            reverse('admin:billing_subscriptions'),
            billing_client=bil,
            lookup_client=down_lkp,
        )
        self.assertEqual(res_bil_down_sub.status_code, 503)
        self.assertContains(
            res_bil_down_sub,
            'Сервис биллинга временно недоступен',
            status_code=503,
        )

        res_bil_down_pay = self._get(
            reverse('admin:billing_payments'),
            billing_client=bil,
            lookup_client=down_lkp,
        )
        self.assertEqual(res_bil_down_pay.status_code, 503)
        self.assertContains(
            res_bil_down_pay,
            'Сервис биллинга временно недоступен',
            status_code=503,
        )

    def test_payments_search_by_login_payment_uuid_and_provider_id(self) -> None:
        """T-27 (ADM-FR-01, ADM-FR-02): поиск платежей по логину, UUID платежа и номеру ЮKassa."""
        from billing.clients import (
            BillingPaymentItem,
            BillingPaymentsPage,
            UserLookupProfile,
        )

        user_id = UUID('66666666-6666-4666-8666-666666666666')
        pay_id = UUID('77777777-7777-4777-8777-777777777777')
        created_utc = datetime(2026, 11, 5, 8, 15, tzinfo=timezone.utc)
        sample_item = BillingPaymentItem(
            id=pay_id,
            user_id=user_id,
            tariff_code='premium',
            tariff_name='Подписка «Премиум»',
            provider_code='yookassa',
            provider_payment_id='yk_search_test_99',
            amount_kopeks=29900,
            currency='RUB',
            status='failed',
            failure_reason='insufficient_funds',
            has_refund=False,
            refund_status=None,
            active_refund_id=None,
            is_stuck=False,
            created_at=created_utc,
            updated_at=created_utc,
        )

        class SearchBilling:
            def list_payments(self, **kwargs: object) -> BillingPaymentsPage:
                if kwargs.get('user_id') == user_id:
                    return BillingPaymentsPage(total=1, items=(sample_item,))
                if kwargs.get('payment_id') == pay_id:
                    return BillingPaymentsPage(total=1, items=(sample_item,))
                if kwargs.get('provider_payment_id') == 'yk_search_test_99':
                    return BillingPaymentsPage(total=1, items=(sample_item,))
                return BillingPaymentsPage(total=0, items=())

        class SearchLookup:
            def find_by_login(self, login: str, *, request_id: str) -> UserLookupProfile | None:
                if login == 'payer_login':
                    return UserLookupProfile(
                        user_id=user_id,
                        login='payer_login',
                        tz='Europe/Moscow',
                    )
                return None

            def get_by_ids(
                self, user_ids: list[UUID], *, request_id: str
            ) -> dict[UUID, UserLookupProfile]:
                return {
                    uid: UserLookupProfile(
                        user_id=uid,
                        login='payer_login',
                        tz='Europe/Moscow',
                    )
                    for uid in user_ids
                }

        bil = SearchBilling()
        lkp = SearchLookup()

        for params in (
            {'search_by': 'user', 'q': 'payer_login'},
            {'search_by': 'user', 'q': str(user_id)},
            {'search_by': 'payment_id', 'q': str(pay_id)},
            {'search_by': 'provider_payment_id', 'q': 'yk_search_test_99'},
            {'q': 'yk_search_test_99'},
        ):
            res = self._get(
                reverse('admin:billing_payments'),
                params,
                billing_client=bil,
                lookup_client=lkp,
            )
            self.assertEqual(res.status_code, 200)
            self.assertContains(res, str(pay_id))
            self.assertContains(res, 'yk_search_test_99')
            self.assertContains(res, 'payer_login')
            self.assertContains(res, '05.11.2026 11:15 (Europe/Moscow)')

    def test_http_clients_contract_and_unhappy_paths(self) -> None:
        """T-27: проверка HTTP-клиентов AuthUserLookupClient и BillingAdminClient."""
        import httpx

        from billing.clients import (
            AuthLookupUnavailableError,
            AuthUserLookupClient,
            BillingAdminClient,
            BillingContractError,
            BillingUnavailableError,
        )

        uid = UUID('88888888-8888-4888-8888-888888888888')
        pid = UUID('99999999-9999-4999-8999-999999999999')
        sid = UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa')

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.headers.get('X-Service-Api-Key') == 'svc-secret'
            assert request.headers.get('X-Request-Id') == 'req-1'
            if request.url.path.endswith('/users/lookup'):
                return httpx.Response(
                    200,
                    json={
                        'items': [
                            {
                                'user_id': str(uid),
                                'login': 'lookup_user',
                                'tz': 'Europe/Moscow',
                            }
                        ]
                    },
                )
            if request.url.path.endswith('/subscriptions'):
                return httpx.Response(
                    200,
                    json={
                        'total': 1,
                        'items': [
                            {
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
                            }
                        ],
                    },
                )
            if request.url.path.endswith('/payments'):
                return httpx.Response(
                    200,
                    json={
                        'total': 1,
                        'items': [
                            {
                                'id': str(pid),
                                'user_id': str(uid),
                                'tariff_code': 'premium',
                                'tariff_name': 'Подписка «Премиум»',
                                'provider_code': 'yookassa',
                                'provider_payment_id': 'yk_1',
                                'amount_kopeks': 29900,
                                'currency': 'RUB',
                                'status': 'succeeded',
                                'failure_reason': None,
                                'has_refund': False,
                                'refund_status': None,
                                'active_refund_id': None,
                                'is_stuck': False,
                                'created_at': '2026-11-05T10:00:00Z',
                                'updated_at': '2026-11-05T10:00:00Z',
                            }
                        ],
                    },
                )
            return httpx.Response(503, json={'detail': 'unavailable'})

        http_ok = httpx.Client(transport=httpx.MockTransport(handler))
        lkp = AuthUserLookupClient(
            base_url='http://auth:8000/api/v1/internal',
            service_api_key='svc-secret',
            http_client=http_ok,
        )
        bil = BillingAdminClient(
            base_url='http://billing:8000/api/v1/internal/billing',
            service_api_key='svc-secret',
            http_client=http_ok,
        )
        found = lkp.find_by_login('lookup_user', request_id='req-1')
        self.assertIsNotNone(found)
        assert found is not None
        self.assertEqual(found.user_id, uid)
        self.assertIsNone(lkp.find_by_login('   ', request_id='req-1'))
        self.assertEqual(lkp.get_by_ids([], request_id='req-1'), {})
        batch = lkp.get_by_ids([uid], request_id='req-1')
        self.assertIn(uid, batch)

        subs_page = bil.list_subscriptions(request_id='req-1', user_id=uid)
        self.assertEqual(subs_page.total, 1)
        self.assertEqual(subs_page.items[0].id, sid)

        pays_page = bil.list_payments(request_id='req-1', user_id=uid, stuck=True)
        self.assertEqual(pays_page.total, 1)
        self.assertEqual(pays_page.items[0].id, pid)

        # Unhappy paths: 503 и невалидный JSON/контракт
        http_503 = httpx.Client(transport=httpx.MockTransport(lambda _r: httpx.Response(503)))
        lkp_503 = AuthUserLookupClient(
            base_url='http://auth:8000/api/v1/internal',
            service_api_key='svc-secret',
            http_client=http_503,
        )
        bil_503 = BillingAdminClient(
            base_url='http://billing:8000/api/v1/internal/billing',
            service_api_key='svc-secret',
            http_client=http_503,
        )
        with self.assertRaises(AuthLookupUnavailableError):
            lkp_503.find_by_login('lookup_user', request_id='req-1')
        with self.assertRaises(BillingUnavailableError):
            bil_503.list_subscriptions(request_id='req-1')

        http_bad_json = httpx.Client(
            transport=httpx.MockTransport(lambda _r: httpx.Response(200, json={'unexpected': 1}))
        )
        bil_bad = BillingAdminClient(
            base_url='http://billing:8000/api/v1/internal/billing',
            service_api_key='svc-secret',
            http_client=http_bad_json,
        )
        with self.assertRaises(BillingContractError):
            bil_bad.list_payments(request_id='req-1')
