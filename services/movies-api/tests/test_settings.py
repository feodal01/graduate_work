import pytest
from pydantic import SecretStr, ValidationError

from core.config import Settings


def test_settings_rejects_invalid_redis_port() -> None:
    with pytest.raises(ValidationError):
        Settings(
            redis_host='127.0.0.1',
            elasticsearch_host='127.0.0.1',
            elasticsearch_index='movies',
            redis_port='not-a-port',  # type: ignore[arg-type]
        )


def test_settings_rejects_empty_elasticsearch_host() -> None:
    with pytest.raises(ValidationError):
        Settings(
            redis_host='127.0.0.1',
            elasticsearch_host='',
            elasticsearch_index='movies',
        )


def test_settings_requires_hosts(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        'REDIS_HOST',
        'ELASTICSEARCH_HOST',
        'ELASTICSEARCH_INDEX',
    ):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(ValidationError):
        Settings(_env_file=None)  # type: ignore[call-arg]


def test_settings_requires_auth_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        'AUTH_JWT_SECRET',
        'AUTH_SERVICE_URL',
        'AUTH_SERVICE_API_KEY',
    ):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(ValidationError):
        Settings(
            _env_file=None,
            redis_host='127.0.0.1',
            elasticsearch_host='127.0.0.1',
            elasticsearch_index='movies',
        )


def test_settings_accepts_explicit_values() -> None:
    s = Settings(
        project_name='test-movies',
        redis_host='redis',
        elasticsearch_host='elasticsearch',
        elasticsearch_index='movies_test',
        redis_port=6380,
        elasticsearch_request_timeout=1.5,
        backoff_max_tries=3,
        backoff_max_time=2.0,
        auth_jwt_secret='test-secret-that-is-long-enough-for-hs256',
        auth_service_url='http://auth:8000/api/v1/auth',
        auth_service_api_key='test-service-key-that-is-long-enough',
        auth_backoff_max_tries=2,
        auth_backoff_max_time=0.5,
    )
    assert s.project_name == 'test-movies'
    assert s.redis_host == 'redis'
    assert s.elasticsearch_host == 'elasticsearch'
    assert s.elasticsearch_index == 'movies_test'
    assert s.redis_port == 6380
    assert s.elasticsearch_request_timeout == 1.5
    assert s.backoff_max_tries == 3
    assert s.backoff_max_time == 2.0
    assert isinstance(s.auth_jwt_secret, SecretStr)
    assert s.auth_jwt_algorithm == 'HS256'
    assert s.auth_service_url == 'http://auth:8000/api/v1/auth'
    assert isinstance(s.auth_service_api_key, SecretStr)
    assert s.auth_connect_timeout == 0.5
    assert s.auth_request_timeout == 1.0
    assert s.auth_backoff_max_tries == 2
    assert s.auth_backoff_max_time == 0.5
