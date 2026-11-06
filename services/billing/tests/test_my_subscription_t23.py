from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from dependencies import (
    ACCESS_TOKEN_COOKIE,
    get_auth_gateway,
    get_purchase_service,
)
from domain.subscription import compute_my_subscription_summary
from main import create_app
from services.purchase import PurchaseService
from tests.fakes import (
    ConfigurableAuthGateway,
    FakeOutboxTaskRow,
    FakePaymentRow,
    FakeProviderGateway,
    FakeRefundRow,
    FakeSubscriptionRow,
    InMemoryPurchaseRepository,
)


def _build_t23_app(
    repo: InMemoryPurchaseRepository,
    provider: FakeProviderGateway,
    auth: ConfigurableAuthGateway,
) -> FastAPI:
    app = create_app(app_lifespan=None)
    service = PurchaseService(
        repository=repo,
        payment_provider=provider,
        public_base_url='http://localhost',
    )
    app.dependency_overrides[get_purchase_service] = lambda: service
    app.dependency_overrides[get_auth_gateway] = lambda: auth
    return app


def test_compute_my_subscription_summary_all_7_rules_in_priority_order() -> None:
    """T-23 (US-U-05, BIL-FR-09): проверка всех 7 правил таблицы US-U-05."""
    now_utc = datetime(2026, 11, 5, 12, 0, tzinfo=UTC)
    pending_pid = uuid4()
    future_ends = now_utc + timedelta(days=20)
    past_ends = now_utc - timedelta(days=5)

    # Правило 7: Подписок не было
    r7 = compute_my_subscription_summary(
        pending_payment_id=None,
        latest_subscription_ends_at=None,
        latest_subscription_has_succeeded_refund=False,
        latest_subscription_has_processing_refund=False,
        latest_subscription_auth_delivered=False,
        user_tz='Europe/Moscow',
        now_utc=now_utc,
    )
    assert r7.rule_number == 7
    assert r7.status_key == 'no_subscription'
    assert r7.status_display == 'Нет подписки'
    assert r7.action_type == 'subscribe'
    assert r7.can_subscribe is True

    # Правило 6: Последняя подписка возвращена
    r6 = compute_my_subscription_summary(
        pending_payment_id=None,
        latest_subscription_ends_at=future_ends,
        latest_subscription_has_succeeded_refund=True,
        latest_subscription_has_processing_refund=False,
        latest_subscription_auth_delivered=True,
        user_tz='Europe/Moscow',
        now_utc=now_utc,
    )
    assert r6.rule_number == 6
    assert r6.status_key == 'refunded'
    assert r6.status_display == 'Возвращена'
    assert r6.action_type == 'subscribe'
    assert r6.can_subscribe is True

    # Правило 5: Последняя подписка завершена (возврата в обработке нет)
    r5 = compute_my_subscription_summary(
        pending_payment_id=None,
        latest_subscription_ends_at=past_ends,
        latest_subscription_has_succeeded_refund=False,
        latest_subscription_has_processing_refund=False,
        latest_subscription_auth_delivered=True,
        user_tz='Europe/Moscow',
        now_utc=now_utc,
    )
    assert r5.rule_number == 5
    assert r5.status_key == 'expired'
    assert r5.status_display == 'Завершена 31.10.2026 15:00'
    assert r5.action_type == 'subscribe'
    assert r5.can_subscribe is True

    # Правило 4: Последняя подписка завершена, но возврат по ней «В обработке»
    r4 = compute_my_subscription_summary(
        pending_payment_id=None,
        latest_subscription_ends_at=past_ends,
        latest_subscription_has_succeeded_refund=False,
        latest_subscription_has_processing_refund=True,
        latest_subscription_auth_delivered=True,
        user_tz='Europe/Moscow',
        now_utc=now_utc,
    )
    assert r4.rule_number == 4
    assert r4.status_key == 'expired_refund_processing'
    assert r4.status_display == 'Завершена 31.10.2026 15:00'
    assert r4.action_type == 'none'
    assert r4.can_subscribe is False

    # Правило 3: Последняя подписка активна (в т.ч. пока возврат «В обработке»)
    r3 = compute_my_subscription_summary(
        pending_payment_id=None,
        latest_subscription_ends_at=future_ends,
        latest_subscription_has_succeeded_refund=False,
        latest_subscription_has_processing_refund=True,
        latest_subscription_auth_delivered=True,
        user_tz='Europe/Moscow',
        now_utc=now_utc,
    )
    assert r3.rule_number == 3
    assert r3.status_key == 'active'
    assert r3.status_display == 'Активна до 25.11.2026 15:00'
    assert r3.action_type == 'none'
    assert r3.can_subscribe is False

    # Правило 2: Последняя подписка активна, но состояние ещё не доставлено в Auth
    r2 = compute_my_subscription_summary(
        pending_payment_id=None,
        latest_subscription_ends_at=future_ends,
        latest_subscription_has_succeeded_refund=False,
        latest_subscription_has_processing_refund=False,
        latest_subscription_auth_delivered=False,
        user_tz='Europe/Moscow',
        now_utc=now_utc,
    )
    assert r2.rule_number == 2
    assert r2.status_key == 'activating'
    assert r2.status_display == 'Оплачено, доступ активируется'
    assert r2.action_type == 'none'
    assert r2.can_subscribe is False

    # Правило 1 (наивысший приоритет): Есть платёж в состоянии «Ожидает оплаты»
    r1 = compute_my_subscription_summary(
        pending_payment_id=pending_pid,
        latest_subscription_ends_at=past_ends,
        latest_subscription_has_succeeded_refund=False,
        latest_subscription_has_processing_refund=False,
        latest_subscription_auth_delivered=True,
        user_tz='Europe/Moscow',
        now_utc=now_utc,
    )
    assert r1.rule_number == 1
    assert r1.status_key == 'pending_payment'
    assert r1.status_display == 'Ожидает оплаты'
    assert r1.action_type == 'continue_payment'
    assert r1.action_url == f'/billing/payments/{pending_pid}/continue'
    assert r1.can_subscribe is False


def test_my_subscription_html_and_json_summary_all_rules_and_history_isolation() -> (
    None
):
    """T-23 (BIL-FR-09, BIL-FR-12, BIL-API-02): экран Э2, JSON summary и история."""
    user_id = uuid4()
    other_user_id = uuid4()
    tariff_id = UUID('00000000-0000-0000-0000-000000000001')
    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway()
    auth = ConfigurableAuthGateway(user_id=user_id, tz='Asia/Yekaterinburg')
    app = _build_t23_app(repo, provider, auth)
    token = 'valid-user-token'

    # Чужой платёж не должен появляться в истории пользователя (BIL-NFR-SEC-03)
    foreign_pid = uuid4()
    repo.payments[foreign_pid] = FakePaymentRow(
        id=foreign_pid,
        user_id=other_user_id,
        tariff_id=tariff_id,
        provider_code='yookassa',
        amount_kopeks=29_900,
        currency='RUB',
        status='succeeded',
        idempotency_key=foreign_pid,
    )

    with TestClient(app) as client:
        # 1. Без входа: GET /billing/ -> редирект на /billing/login, JSON -> 401
        unauth_ui = client.get('/billing/', follow_redirects=False)
        assert unauth_ui.status_code == 303
        assert unauth_ui.headers['location'].startswith('/billing/login')

        unauth_json = client.get(
            '/api/v1/billing/me/summary',
            headers={'X-Request-Id': 'req-t23-401'},
        )
        assert unauth_json.status_code == 401

        client.cookies.set(ACCESS_TOKEN_COOKIE, token)

        # 2. Правило 7: Подписок нет, но есть неуспешный платёж в истории
        failed_pid = uuid4()
        repo.payments[failed_pid] = FakePaymentRow(
            id=failed_pid,
            user_id=user_id,
            tariff_id=tariff_id,
            provider_code='yookassa',
            amount_kopeks=29_900,
            currency='RUB',
            status='failed',
            idempotency_key=failed_pid,
            failure_reason='insufficient_funds',
            created_at=datetime(2026, 11, 1, 10, 0, tzinfo=UTC),
        )
        r7_html = client.get('/billing/')
        assert r7_html.status_code == 200
        assert 'Нет подписки' in r7_html.text
        assert 'Оформить за 299 ₽' in r7_html.text
        assert 'Не оплачен' in r7_html.text
        assert str(failed_pid) in r7_html.text
        assert str(foreign_pid) not in r7_html.text
        # Даты переведены в часовой пояс пользователя Asia/Yekaterinburg (+5 UTC)
        assert '01.11.2026 15:00' in r7_html.text

        r7_json = client.get(
            '/api/v1/billing/me/summary',
            headers={
                'X-Request-Id': 'req-t23-r7',
                'Authorization': f'Bearer {token}',
            },
        )
        assert r7_json.status_code == 200
        data_r7 = r7_json.json()
        assert data_r7['rule_number'] == 7
        assert data_r7['status_display'] == 'Нет подписки'
        assert data_r7['can_subscribe'] is True
        assert len(data_r7['payments']) == 1
        assert data_r7['payments'][0]['payment_id'] == str(failed_pid)
        assert data_r7['payments'][0]['status_display'] == 'Не оплачен'

        # 3. Правило 1: Есть ожидающий платёж -> «Ожидает оплаты» + ссылка /continue
        pending_pid = uuid4()
        repo.payments[pending_pid] = FakePaymentRow(
            id=pending_pid,
            user_id=user_id,
            tariff_id=tariff_id,
            provider_code='yookassa',
            amount_kopeks=29_900,
            currency='RUB',
            status='pending',
            idempotency_key=pending_pid,
            created_at=datetime(2026, 11, 2, 10, 0, tzinfo=UTC),
        )
        r1_html = client.get('/billing/')
        assert r1_html.status_code == 200
        assert 'Ожидает оплаты' in r1_html.text
        assert f'/billing/payments/{pending_pid}/continue' in r1_html.text
        assert 'Оформить за 299 ₽' not in r1_html.text

        # Переводим платёж в succeeded и создаём подписку (ещё не доставлена в Auth)
        del repo.payments[pending_pid]
        paid_pid = uuid4()
        now_utc = datetime.now(UTC)
        ends_at = now_utc + timedelta(days=30)
        repo.payments[paid_pid] = FakePaymentRow(
            id=paid_pid,
            user_id=user_id,
            tariff_id=tariff_id,
            provider_code='yookassa',
            amount_kopeks=29_900,
            currency='RUB',
            status='succeeded',
            idempotency_key=paid_pid,
            created_at=now_utc,
        )
        sub_id = uuid4()
        repo.subscriptions[paid_pid] = FakeSubscriptionRow(
            id=sub_id,
            user_id=user_id,
            tariff_id=tariff_id,
            payment_id=paid_pid,
            starts_at=now_utc,
            ends_at=ends_at,
        )
        auth_task_key = f'payment_succeeded:{paid_pid}:auth_access'
        repo.outbox_tasks[auth_task_key] = FakeOutboxTaskRow(
            id=uuid4(),
            dedup_key=auth_task_key,
            target_service='auth',
            task_type='sync_access',
            payment_id=paid_pid,
            user_id=user_id,
            access_version=1,
            payload={'version': 1, 'expires_at': ends_at.isoformat()},
            status='pending',
        )

        # 4. Правило 2: Подписка активна, но Auth ещё не получил доступ
        r2_html = client.get('/billing/')
        assert r2_html.status_code == 200
        assert 'Оплачено, доступ активируется' in r2_html.text
        assert 'Оформить за 299 ₽' not in r2_html.text

        # 5. Правило 3: Доступ доставлен в Auth -> «Активна до …»
        repo.outbox_tasks[auth_task_key].status = 'completed'
        r3_html = client.get('/billing/')
        assert r3_html.status_code == 200
        assert 'Активна до ' in r3_html.text
        assert 'Оформить за 299 ₽' not in r3_html.text

        # 6. Правило 4: Подписка завершена, но возврат «В обработке»
        repo.subscriptions[paid_pid].ends_at = now_utc - timedelta(days=1)
        refund_id = uuid4()
        repo.refunds[refund_id] = FakeRefundRow(
            id=refund_id,
            payment_id=paid_pid,
            subscription_id=sub_id,
            provider_code='yookassa',
            amount_kopeks=29_900,
            status='processing',
            reason='Тестовый возврат',
            admin_user_id=uuid4(),
            idempotency_key=refund_id,
        )
        r4_html = client.get('/billing/')
        assert r4_html.status_code == 200
        assert 'Завершена ' in r4_html.text
        assert 'Возврат в обработке' in r4_html.text
        assert 'Оформить за 299 ₽' not in r4_html.text

        # 7. Правило 5: Подписка завершена без активного возврата -> кнопка покупки
        repo.refunds[refund_id].status = 'rejected'
        r5_html = client.get('/billing/')
        assert r5_html.status_code == 200
        assert 'Завершена ' in r5_html.text
        assert 'Оформить за 299 ₽' in r5_html.text

        # 8. Правило 6: Подписка возвращена (возврат succeeded)
        repo.refunds[refund_id].status = 'succeeded'
        r6_html = client.get('/billing/')
        assert r6_html.status_code == 200
        assert 'Возвращена' in r6_html.text
        assert 'Возвращён' in r6_html.text
        assert 'Оформить за 299 ₽' in r6_html.text

        # 9. Сбой Auth при проверке пользователя -> 503 без раскрытия данных
        auth.unavailable = True
        r_503 = client.get('/billing/')
        assert r_503.status_code == 503
        assert 'Сервис авторизации временно недоступен' in r_503.text
        assert str(paid_pid) not in r_503.text

        r_503_json = client.get(
            '/api/v1/billing/me/summary',
            headers={
                'X-Request-Id': 'req-t23-auth-503',
                'Authorization': f'Bearer {token}',
            },
        )
        assert r_503_json.status_code == 503
        assert r_503_json.json()['error_code'] == 'auth_unavailable'
        assert str(paid_pid) not in r_503_json.text


def test_my_subscription_unhappy_paths_per_scenario_step() -> None:
    """T-23 (US-U-05, 03-us-u-05): unhappy paths для каждого шага сценария Э2."""
    user_id = uuid4()
    tariff_id = UUID('00000000-0000-0000-0000-000000000001')
    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway()
    auth = ConfigurableAuthGateway(
        user_id=user_id,
        login='<script>alert(1)</script>',
        tz='Invalid/Timezone_From_Auth',
    )
    app = _build_t23_app(repo, provider, auth)
    token = 'expired-or-revoked-token'

    paid_pid = uuid4()
    now_utc = datetime(2026, 11, 5, 12, 0, tzinfo=UTC)
    repo.payments[paid_pid] = FakePaymentRow(
        id=paid_pid,
        user_id=user_id,
        tariff_id=tariff_id,
        provider_code='yookassa',
        amount_kopeks=29_950,
        currency='RUB',
        status='succeeded',
        idempotency_key=paid_pid,
        created_at=now_utc,
    )

    with TestClient(app) as client:
        client.cookies.set(ACCESS_TOKEN_COOKIE, token)

        # Шаг 1 (проверка токена в Auth): токен протух/отозван -> редирект на вход / 401
        auth.invalid_token = True
        expired_ui = client.get('/billing/', follow_redirects=False)
        assert expired_ui.status_code == 303
        assert expired_ui.headers['location'].startswith('/billing/login')

        expired_json = client.get(
            '/api/v1/billing/me/summary',
            headers={
                'X-Request-Id': 'req-t23-expired',
                'Authorization': f'Bearer {token}',
            },
        )
        assert expired_json.status_code == 401
        assert expired_json.json()['error_code'] == 'unauthenticated'
        auth.invalid_token = False

        # Шаг 1b (JSON API без X-Request-Id): отклоняется с 400
        missing_req_id = client.get(
            '/api/v1/billing/me/summary',
            headers={'Authorization': f'Bearer {token}'},
        )
        assert missing_req_id.status_code == 400

        # Шаг 2 (чтение из БД Биллинга): БД недоступна -> 503 без раскрытия данных
        repo.db_unavailable = True
        db_down_ui = client.get('/billing/')
        assert db_down_ui.status_code == 503
        assert str(paid_pid) not in db_down_ui.text

        db_down_json = client.get(
            '/api/v1/billing/me/summary',
            headers={
                'X-Request-Id': 'req-t23-db-down',
                'Authorization': f'Bearer {token}',
            },
        )
        assert db_down_json.status_code == 503
        assert db_down_json.json()['error_code'] == 'database_unavailable'
        repo.db_unavailable = False

        # Шаг 3 (перевод часового пояса и экранирование HTML):
        # невалидный tz из Auth безопасно деградирует в UTC (12:00),
        # а HTML в логине экранируется шаблоном Jinja2
        fallback_ui = client.get('/billing/')
        assert fallback_ui.status_code == 200
        assert '05.11.2026 12:00' in fallback_ui.text
        assert '299,50 ₽' in fallback_ui.text
        assert '<script>alert(1)</script>' not in fallback_ui.text
        assert '&lt;script&gt;alert(1)&lt;/script&gt;' in fallback_ui.text

        # Шаг 4 (попытка купить подписку POST /billing/subscribe при блокирующих
        # правилах 2/3 и 4): отклоняется с 409 Conflict без вызова ЮKassa
        repo.has_active_subscription = True
        sub_409_active = client.post('/billing/subscribe')
        assert sub_409_active.status_code == 409
        assert 'Подписка уже активна' in sub_409_active.text
        assert provider.create_calls == []

        repo.has_active_subscription = False
        repo.has_processing_refund = True
        sub_409_refund = client.post('/billing/subscribe')
        assert sub_409_refund.status_code == 409
        assert 'возврат находится в обработке' in sub_409_refund.text
        assert provider.create_calls == []
