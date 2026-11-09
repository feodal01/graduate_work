from uuid import uuid4

import httpx
import pytest
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from db.sql.postgres import get_session
from dependencies import get_rate_limiter, get_service_credentials
from main import create_app
from models import User
from security.service_credentials import ServiceCredentials
from tests.fakes import ProgrammableRateLimiter
from tests.test_repositories import session  # noqa: F401

SERVICE_KEY = 'notification-test-service-key-long-enough'
PATH = '/api/v1/internal/notification-contacts'


@pytest.fixture
async def client(session: AsyncSession):  # noqa: F811
    """Expose the real isolated PostgreSQL session through HTTP."""
    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_session] = lambda: session
    app.dependency_overrides[get_service_credentials] = lambda: ServiceCredentials(
        SERVICE_KEY
    )
    app.dependency_overrides[get_rate_limiter] = lambda: ProgrammableRateLimiter()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url='http://test',
        headers={'X-Request-Id': 'contacts-test'},
    ) as result:
        yield result


@pytest.mark.parametrize('key', [None, 'wrong'])
async def test_rejects_service_key_before_database_read(client, session, key, caplog):  # noqa: F811
    """Reject anonymous and incorrect service credentials without SQL or PII."""
    statements = []
    engine = session.bind.sync_engine

    def listener(*args):
        statements.append(args[2])

    event.listen(engine, 'before_cursor_execute', listener)
    try:
        response = await client.post(
            PATH,
            json={'user_ids': [str(uuid4())]},
            headers={} if key is None else {'X-Service-Key': key},
        )
    finally:
        event.remove(engine, 'before_cursor_execute', listener)
    assert response.status_code == 401
    assert statements == []
    assert 'email' not in response.text
    assert SERVICE_KEY not in caplog.text


async def test_contacts_batch_uses_one_query_and_preserves_ids(client, session):  # noqa: F811
    """Return nullable contacts only for existing IDs with one projected query."""
    first = User(login='first', email='private@example.org', first_name='First')
    second = User(login='second', email=None, locale='ru-RU')
    session.add_all([first, second])
    await session.commit()
    missing = uuid4()
    statements = []
    engine = session.bind.sync_engine

    def listener(*args):
        statements.append(args[2])

    event.listen(engine, 'before_cursor_execute', listener)
    try:
        response = await client.post(
            PATH,
            headers={'X-Service-Key': SERVICE_KEY},
            json={'user_ids': [str(second.id), str(missing), str(first.id)]},
        )
    finally:
        event.remove(engine, 'before_cursor_execute', listener)
    assert response.status_code == 200
    assert response.json() == {
        'items': [
            {
                'user_id': str(second.id),
                'status': 'active',
                'email': None,
                'first_name': None,
                'last_name': None,
                'locale': 'ru-RU',
            },
            {'user_id': str(missing), 'status': 'unavailable'},
            {
                'user_id': str(first.id),
                'status': 'active',
                'email': 'private@example.org',
                'first_name': 'First',
                'last_name': None,
                'locale': None,
            },
        ]
    }
    assert len(statements) == 1
    assert 'password' not in statements[0]


async def test_batch_limit_duplicates_and_empty_rejected(client, monkeypatch):
    """Enforce the configured maximum and reject ambiguous or empty batches."""
    monkeypatch.setattr(settings, 'auth_notification_contacts_batch_size', 2)
    identifier = str(uuid4())
    for ids in [[], [identifier, identifier], [str(uuid4()) for _ in range(3)]]:
        response = await client.post(
            PATH,
            headers={'X-Service-Key': SERVICE_KEY},
            json={'user_ids': ids},
        )
        assert response.status_code == 422


async def test_database_failure_is_service_unavailable(
    client,
    session,  # noqa: F811
    monkeypatch,
):  # noqa: F811
    """Distinguish an Auth outage from an existing profile without email."""
    from sqlalchemy.exc import OperationalError

    async def fail(*args, **kwargs):
        raise OperationalError('select', {}, Exception('database down'))

    monkeypatch.setattr(session, 'execute', fail)
    response = await client.post(
        PATH,
        headers={'X-Service-Key': SERVICE_KEY},
        json={'user_ids': [str(uuid4())]},
    )
    assert response.status_code == 503
    assert 'items' not in response.json()


def test_setting_reads_exact_environment_variable(monkeypatch):
    """Configure the batch limit with the deployment-facing environment name."""
    monkeypatch.setenv('AUTH_NOTIFICATION_CONTACTS_BATCH_SIZE', '7')
    assert (
        type(settings)(
            **{
                **settings.model_dump(
                    exclude={'auth_notification_contacts_batch_size'}
                ),
            }
        ).auth_notification_contacts_batch_size
        == 7
    )


def test_openapi_discriminates_unavailable_and_has_no_bearer_security():
    """Publish the exact internal contract without a user JWT requirement."""
    document = create_app(app_lifespan=None).openapi()
    operation = document['paths'][PATH]['post']
    assert not operation.get('security')
    schemas = document['components']['schemas']
    assert set(schemas['UnavailableNotificationContact']['properties']) == {
        'user_id',
        'status',
    }
    assert (
        schemas['NotificationContactsResponse']['properties']['items']['items'][
            'discriminator'
        ]['propertyName']
        == 'status'
    )


async def test_service_rate_limit_rejects_before_contact_query(
    client,
    session,  # noqa: F811
    monkeypatch,
):  # noqa: F811
    """Apply the existing service bucket before reading contact data."""
    from services.rate_limit import RateLimitDecision

    calls = []

    async def deny(self, *, policy, identity):
        calls.append((policy.name, identity))
        return RateLimitDecision(allowed=False, remaining=0, retry_after_seconds=12)

    async def unexpected_query(*args, **kwargs):
        pytest.fail('Rate-limited request read contacts')

    monkeypatch.setattr(settings.rate_limit, 'enabled', True)
    monkeypatch.setattr(ProgrammableRateLimiter, 'check', deny)
    monkeypatch.setattr(session, 'execute', unexpected_query)
    response = await client.post(
        PATH,
        headers={'X-Service-Key': SERVICE_KEY},
        json={'user_ids': [str(uuid4())]},
    )
    assert response.status_code == 429
    assert response.headers['Retry-After'] == '12'
    assert calls == [('service', 'internal-service:notification-contacts')]


async def test_deleted_and_unrequested_profiles_never_reveal_contacts(
    client,
    session,  # noqa: F811
):
    """Hide physically deleted contacts and exclude unrelated existing users."""
    deleted = User(login='deleted', email='deleted-private@example.org')
    unrelated = User(login='unrelated', email='unrequested-private@example.org')
    session.add_all([deleted, unrelated])
    await session.commit()
    deleted_id = deleted.id
    await session.delete(deleted)
    await session.commit()

    response = await client.post(
        PATH,
        headers={'X-Service-Key': SERVICE_KEY},
        json={'user_ids': [str(deleted_id)]},
    )

    assert response.status_code == 200
    assert response.json() == {
        'items': [{'user_id': str(deleted_id), 'status': 'unavailable'}],
    }
    assert str(unrelated.id) not in response.text
    assert deleted.email not in response.text
    assert unrelated.email not in response.text
