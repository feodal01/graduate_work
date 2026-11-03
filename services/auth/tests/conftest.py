import os

# Юнит-тесты не ходят во внешние сервисы; дефолты нужны только для валидации
# Settings на чистом checkout (без .env). Выставляем до импорта приложения.
os.environ.setdefault('JWT_SECRET', 'test-jwt-secret-that-is-long-enough-for-hs256')
os.environ.setdefault('SERVICE_API_KEY', 'test-service-key-that-is-long-enough')

import pytest

# Фикстуры, которые подключаются к настоящим Redis и PostgreSQL.
_INTEGRATION_FIXTURES = frozenset({'redis_client', 'session', 'session_factory'})


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Mark tests that need live Redis or PostgreSQL as integration tests."""
    for item in items:
        fixturenames = getattr(item, 'fixturenames', ())
        if _INTEGRATION_FIXTURES.intersection(fixturenames):
            item.add_marker(pytest.mark.integration)
