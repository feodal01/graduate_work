from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx

from clients.auth import SubscriptionAccessSyncResult
from clients.notifications import NotificationsTransientError
from clients.payment_provider_registry import PaymentProviderRegistry
from dependencies import (
    get_auth_gateway,
    get_notifications_gateway,
    get_outbox_dispatcher,
    get_payment_providers,
    get_payment_reconciler,
    get_purchase_service,
    get_repositories,
    get_service_credentials,
)
from main import create_app
from security.service_credentials import ServiceCredentials
from services.outbox_dispatcher import OutboxDispatcher
from services.purchase import PurchaseService
from services.reconciler import PaymentReconciler
from tests.fakes import (
    FakeProviderGateway,
    InMemoryPurchaseRepository,
    in_memory_repositories,
)

SERVICE_KEY = 'billing-demo-walkthrough-t40-service-key-long-enough'


class _DemoAuthAndMoviesSimulator:
    """Эмулирует связку Auth + movies-api для проверки 4 шагов живого демо 9.2."""

    def __init__(self) -> None:
        self.user_id = uuid4()
        self.login_name = 'demo_viewer_9_2'
        self.tz = 'Europe/Moscow'
        self.down = False
        self.current_version = 0
        self.expires_at: datetime | None = None

    def can_watch_new_release_film(self, *, now_utc: datetime | None = None) -> int:
        """Возвращает 200 OK если у пользователя активна роль subscriber, иначе 403."""
        now = now_utc or datetime.now(UTC)
        if self.expires_at is not None and self.expires_at > now:
            return 200
        return 403

    async def login(
        self, login: str, password: str, *, request_id: str
    ) -> tuple[str, str]:
        from clients.auth import (
            AuthInvalidCredentialsError,
            AuthServiceUnavailableError,
        )

        if self.down:
            raise AuthServiceUnavailableError('Auth is stopped')
        if login != self.login_name or password != 'DemoPass-2026!':
            raise AuthInvalidCredentialsError()
        return 'demo-jwt-access', 'demo-jwt-refresh'

    async def logout(
        self, access_token: str, refresh_token: str, *, request_id: str
    ) -> None:
        return None

    async def get_current_user(
        self, access_token: str, *, request_id: str
    ) -> tuple[UUID, str, str]:
        from clients.auth import (
            AuthInvalidCredentialsError,
            AuthServiceUnavailableError,
        )

        if self.down:
            raise AuthServiceUnavailableError('Auth is stopped')
        if access_token != 'demo-jwt-access':
            raise AuthInvalidCredentialsError()
        return self.user_id, self.login_name, self.tz

    async def put_subscription_access(
        self,
        user_id: UUID,
        *,
        version: int,
        expires_at: str | None,
        request_id: str,
    ) -> SubscriptionAccessSyncResult:
        from clients.auth import AuthServiceUnavailableError

        if self.down:
            raise AuthServiceUnavailableError('Auth is stopped')
        if version > self.current_version:
            self.current_version = version
            self.expires_at = (
                datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                if expires_at is not None
                else None
            )
            return SubscriptionAccessSyncResult(
                applied=True, current_version=self.current_version
            )
        return SubscriptionAccessSyncResult(
            applied=False, current_version=self.current_version
        )


class _DemoMailpitAndNotificationsSimulator:
    def __init__(self) -> None:
        self.down = False
        self.mailpit_inbox: list[dict[str, Any]] = []
        self.scheduled_campaigns: dict[UUID, dict[str, Any]] = {}

    async def send_campaign_batch(
        self,
        *,
        campaign_id: UUID,
        batch_id: UUID,
        user_id: UUID,
        template_id: str,
        subject: str,
        text: str,
        available_at: str | None,
        request_id: str,
    ) -> None:
        if self.down:
            raise NotificationsTransientError('Notifications down')
        if available_at is None:
            self.mailpit_inbox.append(
                {'campaign_id': campaign_id, 'subject': subject, 'text': text}
            )
        else:
            self.scheduled_campaigns[campaign_id] = {
                'status': 'scheduled',
                'available_at': available_at,
                'subject': subject,
            }

    async def cancel_campaign(
        self,
        campaign_id: UUID,
        *,
        request_id: str,
    ) -> None:
        if self.down:
            raise NotificationsTransientError('Notifications down')
        if campaign_id in self.scheduled_campaigns:
            self.scheduled_campaigns[campaign_id]['status'] = 'cancelled'
            self.scheduled_campaigns[campaign_id]['cancel_reason'] = (
                'cancelled_by_sender'
            )


async def test_defense_presentation_and_full_demo_walkthrough_section_9_2() -> None:
    """T-40 (раздел 9.2 ТЗ): проверка материалов защиты и всех 4 шагов живого демо."""
    repo_root = Path(__file__).resolve().parents[3]
    defense_doc = repo_root / 'docs' / 'billing' / 'defense.md'
    assert defense_doc.is_file(), 'docs/billing/defense.md must exist for T-40'
    defense_text = defense_doc.read_text(encoding='utf-8')
    for required_heading in (
        'Слайд 1.',
        'Слайд 2.',
        'Слайд 3.',
        'Слайд 4.',
        'Шаг 1. Исходное состояние',
        'Шаг 2. Покупка подписки',
        'Шаг 3. Контроль в Django-админке',
        'Шаг 4. Проверка устойчивости к перебоям',
    ):
        assert required_heading in defense_text

    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway()
    auth_sim = _DemoAuthAndMoviesSimulator()
    ntf_sim = _DemoMailpitAndNotificationsSimulator()
    dispatcher = OutboxDispatcher(
        repository=repo,
        auth_gateway=auth_sim,
        notifications_gateway=ntf_sim,
    )
    reconciler = PaymentReconciler(
        payments=repo,
        refunds=repo,
        payment_providers=PaymentProviderRegistry.single(provider),
        public_base_url='http://localhost',
        outbox_dispatcher=dispatcher,
    )
    purchase_svc = PurchaseService(
        payments=repo,
        user_views=repo,
        payment_providers=PaymentProviderRegistry.single(provider),
        reconciler=reconciler,
        public_base_url='http://localhost',
    )

    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_repositories] = lambda: in_memory_repositories(repo)
    app.dependency_overrides[get_payment_providers] = lambda: (
        PaymentProviderRegistry.single(provider)
    )
    app.dependency_overrides[get_auth_gateway] = lambda: auth_sim
    app.dependency_overrides[get_notifications_gateway] = lambda: ntf_sim
    app.dependency_overrides[get_outbox_dispatcher] = lambda: dispatcher
    app.dependency_overrides[get_payment_reconciler] = lambda: reconciler
    app.dependency_overrides[get_purchase_service] = lambda: purchase_svc
    app.dependency_overrides[get_service_credentials] = lambda: ServiceCredentials(
        SERVICE_KEY
    )

    svc_headers = {'X-Service-Api-Key': SERVICE_KEY, 'X-Request-Id': 'demo-9-2-admin'}

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url='http://test',
    ) as client:
        # === Шаг 1 демо 9.2: Исходное состояние (пользователь без подписки) ===
        r_redir = await client.get('/billing/', follow_redirects=False)
        assert r_redir.status_code == 303
        assert '/billing/login' in r_redir.headers['location']

        r_login = await client.post(
            '/billing/login',
            data={'login': 'demo_viewer_9_2', 'password': 'DemoPass-2026!'},
            follow_redirects=False,
        )
        assert r_login.status_code == 303
        client.cookies.update(r_login.cookies)

        r_step1 = await client.get('/billing/')
        assert r_step1.status_code == 200
        assert 'Нет подписки' in r_step1.text
        assert 'Оформить за 299 ₽' in r_step1.text
        assert auth_sim.can_watch_new_release_film() == 403

        # === Шаг 2 демо 9.2: Покупка, защита от двойного списания и доступ ===
        r_buy_1 = await client.post('/billing/subscribe', follow_redirects=False)
        assert r_buy_1.status_code == 303
        pay_1_id = provider.create_calls[0].payment_id
        yk_1_id = repo.payments[pay_1_id].provider_payment_id
        assert yk_1_id is not None

        # Повторный параллельный запрос возвращает тот же платёж без дубля в ЮKassa
        r_buy_dup = await client.post(
            '/api/v1/billing/payments',
            headers={'X-Request-Id': 'demo-step2-dup'},
        )
        assert r_buy_dup.status_code == 200
        assert r_buy_dup.json()['payment_id'] == str(pay_1_id)
        assert len(provider.create_calls) == 1

        # Пользователь вернулся на экран результата (Э3): «Проверяем оплату…»
        r_res_checking = await client.get(f'/billing/payments/{pay_1_id}')
        assert r_res_checking.status_code == 200
        assert 'Проверяем оплату' in r_res_checking.text

        # Сверка фиксирует оплату -> экран результата показывает «Оплата прошла»,
        # доступ к новинке в movies-api открыт (200), в Mailpit пришло письмо,
        # а письмо об окончании запланировано
        provider.get_status = 'succeeded'
        await client.post(
            '/api/v1/billing/webhooks/yookassa',
            json={'object': {'id': yk_1_id}},
            headers={'X-Request-Id': 'demo-step2-wh'},
        )
        r_res_paid = await client.get(f'/billing/payments/{pay_1_id}')
        assert 'Оплата прошла' in r_res_paid.text
        assert auth_sim.can_watch_new_release_film() == 200
        assert len(ntf_sim.mailpit_inbox) == 1
        assert ntf_sim.mailpit_inbox[0]['subject'] == 'Подписка «Премиум» оплачена'
        assert len(ntf_sim.scheduled_campaigns) == 1

        # Повторная покупка при активной подписке -> 409 Conflict
        r_buy_409 = await client.post(
            '/api/v1/billing/payments',
            headers={'X-Request-Id': 'demo-step2-409'},
        )
        assert r_buy_409.status_code == 409

        # === Шаг 3 демо 9.2: Контроль в админке и оформление возврата ===
        r_adm_subs = await client.get(
            '/api/v1/internal/billing/subscriptions',
            params={'user_id': str(auth_sim.user_id)},
            headers=svc_headers,
        )
        assert r_adm_subs.json()['items'][0]['status'] == 'active'

        r_adm_card = await client.get(
            f'/api/v1/internal/billing/payments/{pay_1_id}',
            headers=svc_headers,
        )
        assert r_adm_card.json()['can_refund'] is True

        admin_id = uuid4()
        # Пустая причина отклоняется (422)
        r_ref_empty = await client.post(
            f'/api/v1/internal/billing/payments/{pay_1_id}/refunds',
            json={'admin_user_id': str(admin_id), 'reason': '  '},
            headers=svc_headers,
        )
        assert r_ref_empty.status_code == 422

        # Подтверждение возврата с причиной «Обращение клиента №101»
        r_ref_ok = await client.post(
            f'/api/v1/internal/billing/payments/{pay_1_id}/refunds',
            json={'admin_user_id': str(admin_id), 'reason': 'Обращение клиента №101'},
            headers=svc_headers,
        )
        assert r_ref_ok.status_code == 200
        assert r_ref_ok.json()['status'] == 'succeeded'

        # Последствия возврата: movies-api -> 403, экран Э2 -> «Возвращена»,
        # в Mailpit письмо о возврате, запланированное письмо об окончании -> cancelled,
        # в журнале admin_actions запись о возврате
        assert auth_sim.can_watch_new_release_film() == 403
        r_sub_after_ref = await client.get('/billing/')
        assert 'Возвращена' in r_sub_after_ref.text
        assert len(ntf_sim.mailpit_inbox) == 2
        assert (
            ntf_sim.mailpit_inbox[1]['subject']
            == 'Возврат средств по подписке «Премиум» выполнен'
        )
        exp_entry = next(iter(ntf_sim.scheduled_campaigns.values()))
        assert exp_entry['status'] == 'cancelled'
        assert exp_entry['cancel_reason'] == 'cancelled_by_sender'

        r_actions = await client.get(
            '/api/v1/internal/billing/admin-actions',
            headers=svc_headers,
        )
        assert r_actions.json()['total'] == 1
        assert r_actions.json()['items'][0]['reason'] == 'Обращение клиента №101'

        # === Шаг 4 демо 9.2: Проверка устойчивости к перебоям (TASK-4) ===
        # 4.1 и 4.2: закрытая вкладка оплаты + остановленный Auth во время сверки
        provider.get_status = 'pending'
        r_buy_2 = await client.post('/billing/subscribe', follow_redirects=False)
        assert r_buy_2.status_code == 303
        pay_2_id = provider.create_calls[1].payment_id

        # Контейнер Auth остановлен; фоновая сверка проводит платёж без возврата на сайт
        auth_sim.down = True
        provider.get_status = 'succeeded'
        repo.payments[pay_2_id].created_at = datetime.now(UTC) - timedelta(seconds=20)
        await reconciler.reconcile_due_pending_payments(min_age_seconds=5.0)
        assert repo.payments[pay_2_id].status == 'succeeded'

        # При остановленном Auth карточка платежа и фильтр «Не доставлено» работают
        r_card_auth_down = await client.get(
            f'/api/v1/internal/billing/payments/{pay_2_id}',
            headers=svc_headers,
        )
        assert r_card_auth_down.status_code == 200

        for t in repo.outbox_tasks.values():
            if t.payment_id == pay_2_id and t.status == 'pending':
                t.created_at = datetime.now(UTC) - timedelta(minutes=15)
        r_undeliv = await client.get(
            '/api/v1/internal/billing/outbox-tasks',
            params={'payment_id': str(pay_2_id)},
            headers=svc_headers,
        )
        assert r_undeliv.status_code == 200
        assert r_undeliv.json()['total'] == 1

        # Auth запускается обратно -> воркер доставляет доступ, задача уходит из
        # «Не доставлено», а просмотр фильма-новинки открывается (200 OK)
        auth_sim.down = False
        for t in repo.outbox_tasks.values():
            if t.status == 'pending':
                t.next_retry_at = datetime.now(UTC)
        await dispatcher.process_due_tasks()
        assert auth_sim.can_watch_new_release_film() == 200

        r_undeliv_after = await client.get(
            '/api/v1/internal/billing/outbox-tasks',
            params={'payment_id': str(pay_2_id)},
            headers=svc_headers,
        )
        assert r_undeliv_after.json()['total'] == 0
