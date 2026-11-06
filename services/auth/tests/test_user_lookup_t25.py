from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import event
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from db.sql.errors import DatabaseUnavailableError
from db.sql.postgres import get_session
from dependencies import get_rate_limiter, get_service_credentials
from main import create_app
from models import User
from security.service_credentials import ServiceCredentials
from services.rate_limit import RateLimitDecision
from tests.fakes import ProgrammableRateLimiter
from tests.test_repositories import session  # noqa: F401

SERVICE_KEY = 'billing-admin-lookup-service-key-long-enough'
PATH = '/api/v1/internal/users/lookup'


class _FakeLookupSession:
    def __init__(self, rows: list[tuple[UUID, str, str]]) -> None:
        self._rows = rows
        self.calls: int = 0
        self.fail_db: bool = False

    async def execute(self, statement: Any) -> list[Any]:
        self.calls += 1
        if self.fail_db:
            raise OperationalError('SELECT 1', {}, OSError('db down'))
        sql_text = str(statement.compile(compile_kwargs={'literal_binds': True}))
        matched: list[Any] = []
        for uid, login, tz in self._rows:
            if (
                f"'{login}'" in sql_text
                or f"'{uid}'" in sql_text
                or uid.hex in sql_text
            ):
                row = type('Row', (), {'id': uid, 'login': login, 'tz': tz})()
                matched.append(row)
        return matched


@pytest.mark.parametrize(
    ('header_name', 'header_value', 'expected_status'),
    [
        ('X-Service-Api-Key', SERVICE_KEY, 200),
        ('X-Service-Key', SERVICE_KEY, 200),
        ('X-Service-Api-Key', 'wrong-service-key', 401),
        ('X-Service-Key', 'wrong-service-key', 401),
        (None, None, 401),
    ],
)
async def test_user_lookup_api_auth_rate_limit_and_validation_unit(
    header_name: str | None,
    header_value: str | None,
    expected_status: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T-25 (AUTH-FR-04, AUTH-API-02, AUTH-NFR-SEC-01): ключи, лимит и схема."""
    alice_id = uuid4()
    bob_id = uuid4()
    fake_session = _FakeLookupSession(
        [
            (alice_id, 'alice_support', 'Asia/Yekaterinburg'),
            (bob_id, 'bob_support', 'Europe/Moscow'),
        ]
    )
    rate_limiter = ProgrammableRateLimiter()

    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_session] = lambda: fake_session
    app.dependency_overrides[get_service_credentials] = lambda: ServiceCredentials(
        SERVICE_KEY
    )
    app.dependency_overrides[get_rate_limiter] = lambda: rate_limiter

    headers: dict[str, str] = {'X-Request-Id': 't25-unit'}
    if header_name is not None and header_value is not None:
        headers[header_name] = header_value

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url='http://test',
        headers=headers,
    ) as client:
        resp = await client.post(PATH, json={'login': 'alice_support'})
        assert resp.status_code == expected_status
        if expected_status == 401:
            assert fake_session.calls == 0
            return

        assert resp.json() == {
            'items': [
                {
                    'user_id': str(alice_id),
                    'login': 'alice_support',
                    'tz': 'Asia/Yekaterinburg',
                }
            ]
        }
        assert fake_session.calls == 1

        # Поиск по несуществующему логину -> 200 {'items': []}
        missing_resp = await client.post(PATH, json={'login': 'unknown_user'})
        assert missing_resp.status_code == 200
        assert missing_resp.json() == {'items': []}

        # Пакетный поиск по user_ids с сохранением порядка и дедупликацией
        batch_resp = await client.post(
            PATH,
            json={
                'user_ids': [
                    str(bob_id),
                    str(uuid4()),
                    str(alice_id),
                    str(bob_id),
                ]
            },
        )
        assert batch_resp.status_code == 200
        assert batch_resp.json() == {
            'items': [
                {
                    'user_id': str(bob_id),
                    'login': 'bob_support',
                    'tz': 'Europe/Moscow',
                },
                {
                    'user_id': str(alice_id),
                    'login': 'alice_support',
                    'tz': 'Asia/Yekaterinburg',
                },
            ]
        }

        # Unhappy paths валидации схемы -> 422
        invalid_payloads: list[dict[str, Any]] = [
            {},
            {'login': None, 'user_ids': None},
            {'login': ''},
            {'login': '   '},
            {'user_ids': []},
            {'login': 'alice_support', 'user_ids': [str(alice_id)]},
            {'user_ids': ['not-a-uuid']},
            {'user_ids': [str(uuid4()) for _ in range(101)]},
            {'login': 'alice_support', 'extra_field': 'forbidden'},
        ]
        for bad_payload in invalid_payloads:
            bad_resp = await client.post(PATH, json=bad_payload)
            assert bad_resp.status_code == 422, f'Expected 422 for {bad_payload}'

        monkeypatch.setattr(settings, 'auth_user_lookup_batch_size', 2, raising=False)
        too_large = await client.post(
            PATH,
            json={'user_ids': [str(uuid4()), str(uuid4()), str(uuid4())]},
        )
        assert too_large.status_code == 422

        # Unhappy path: сбой PostgreSQL -> 503
        fake_session.fail_db = True
        db_down = await client.post(PATH, json={'login': 'alice_support'})
        assert db_down.status_code == 503
        fake_session.fail_db = False

        # Unhappy path: превышение сервисного rate limit -> 429
        rate_limiter.decision = RateLimitDecision(
            allowed=False,
            remaining=0,
            retry_after_seconds=4,
        )
        rate_limited = await client.post(PATH, json={'login': 'alice_support'})
        assert rate_limited.status_code == 429
        assert rate_limited.headers.get('Retry-After') == '4'


async def test_user_lookup_postgres_integration_exact_login_and_batch(
    session: AsyncSession,  # noqa: F811
) -> None:
    """T-25 (PostgreSQL integration): 1 SQL запрос без чтения секретных полей."""
    from repositories.user_lookup import lookup_users

    u1 = User(
        login='pg_lookup_alice',
        email='alice@example.org',
        password_hash='secret-hash-1',
        tz='Asia/Yekaterinburg',
    )
    u2 = User(
        login='pg_lookup_bob',
        email='bob@example.org',
        password_hash='secret-hash-2',
        tz='Europe/Moscow',
    )
    session.add_all([u1, u2])
    await session.commit()

    statements: list[str] = []
    assert session.bind is not None
    engine = session.bind.sync_engine

    def listener(*args: Any) -> None:
        statements.append(args[2])

    event.listen(engine, 'before_cursor_execute', listener)
    try:
        by_login = await lookup_users(session, login='pg_lookup_alice', user_ids=None)
        by_missing = await lookup_users(session, login='missing_user', user_ids=None)
        by_ids = await lookup_users(
            session,
            login=None,
            user_ids=[u2.id, uuid4(), u1.id, u2.id],
        )
    finally:
        event.remove(engine, 'before_cursor_execute', listener)

    assert len(by_login) == 1
    assert (by_login[0].user_id, by_login[0].login, by_login[0].tz) == (
        u1.id,
        'pg_lookup_alice',
        'Asia/Yekaterinburg',
    )
    assert by_missing == []
    assert [(item.user_id, item.login, item.tz) for item in by_ids] == [
        (u2.id, 'pg_lookup_bob', 'Europe/Moscow'),
        (u1.id, 'pg_lookup_alice', 'Asia/Yekaterinburg'),
    ]
    assert len(statements) == 3
    for sql in statements:
        assert 'password' not in sql
        assert 'email' not in sql

    with pytest.raises(DatabaseUnavailableError):
        bad_session = _FakeLookupSession([])
        bad_session.fail_db = True
        await lookup_users(
            bad_session,  # type: ignore[arg-type]
            login='pg_lookup_alice',
            user_ids=None,
        )
