import os

os.environ.setdefault('JWT_SECRET', 'test-jwt-secret-that-is-long-enough-for-hs256')
os.environ.setdefault('SERVICE_API_KEY', 'test-service-key-that-is-long-enough')
os.environ['SENTRY_DSN'] = ''
os.environ['LOGSTASH_HOST'] = ''
os.environ['TRACING_ENABLED'] = 'false'

import pytest

_INTEGRATION_FIXTURES = frozenset({'migrated_billing_db'})


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Mark tests that need live PostgreSQL as integration tests."""
    for item in items:
        fixturenames = getattr(item, 'fixturenames', ())
        if _INTEGRATION_FIXTURES.intersection(fixturenames):
            item.add_marker(pytest.mark.integration)
