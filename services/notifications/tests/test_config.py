import pytest
from pydantic import ValidationError


def settings(**values):
    from notifications.config import Settings

    # BaseSettings runtime options and intentionally invalid values are validated here.
    return Settings(_env_file=None, **values)  # pyright: ignore[reportCallIssue]


BASE = {"process": "consumer", "database_url": "postgresql+asyncpg://u:p@localhost/db"}
CONSUMER = {
    **BASE,
    "kafka_bootstrap_servers": "localhost:9092",
    "initial_likes": True,
    "initial_new_films": False,
    "initial_marketing": False,
}


def test_consumer_requires_explicit_flags_but_no_smtp():
    with pytest.raises(ValidationError):
        settings(**BASE)
    assert settings(**CONSUMER).initial_likes is True


@pytest.mark.parametrize("value", [0, -1])
def test_threshold_must_be_positive(value):
    with pytest.raises(ValidationError):
        settings(**CONSUMER, likes_min_delta=value)


@pytest.mark.parametrize("process", ["api", "scheduler", "worker", "publisher"])
def test_process_refuses_missing_required_settings(process):
    with pytest.raises(ValidationError):
        settings(**{**BASE, "process": process})


API = {
    **BASE,
    "process": "api",
    "jwt_secret": "secret",
    "jwt_algorithm": "HS256",
    "auth_introspection_url": "https://auth.test/api/v1/internal/introspect",
    "auth_introspection_api_key": "test-service-key",
    "auth_introspection_timeout_seconds": 1,
}


def test_api_requires_jwt_and_session_verification():
    assert settings(**API).process == "api"
    for required in (
        "jwt_secret",
        "jwt_algorithm",
        "auth_introspection_url",
        "auth_introspection_api_key",
        "auth_introspection_timeout_seconds",
    ):
        with pytest.raises(ValidationError, match=required):
            settings(**{name: value for name, value in API.items() if name != required})


@pytest.mark.parametrize(
    "field,value",
    [
        ("auth_introspection_url", "/introspect"),
        ("auth_introspection_url", "file:///tmp/auth"),
        ("auth_introspection_api_key", " "),
        ("auth_introspection_timeout_seconds", 0),
        ("auth_introspection_timeout_seconds", True),
        ("auth_introspection_timeout_seconds", float("inf")),
    ],
)
def test_api_rejects_invalid_introspection_settings(field, value):
    with pytest.raises(ValidationError):
        settings(**{**API, field: value})


def test_scheduler_requires_explicit_intervals_and_bootstrap_load():
    values = {
        **BASE,
        "process": "scheduler",
        "likes_min_delta": 3,
        "likes_interval_seconds": 86400,
        "films_interval_seconds": 604800,
        "likes_bootstrap_load_id": "initial-likes-v1",
    }
    assert settings(**values).likes_min_delta == 3
    for required in (
        "likes_min_delta",
        "likes_interval_seconds",
        "films_interval_seconds",
        "likes_bootstrap_load_id",
    ):
        with pytest.raises(ValidationError, match=required):
            settings(**{name: value for name, value in values.items() if name != required})
    with pytest.raises(ValidationError):
        settings(**{**values, "likes_interval_seconds": 0})


def test_worker_requires_valid_fallback_timezone():
    values = {
        **BASE,
        "process": "worker",
        "rabbitmq_url": "amqp://localhost/",
        "smtp_host": "localhost",
        "smtp_port": 1025,
        "smtp_from": "mail@example.org",
        "smtp_starttls": False,
        "fallback_locale": "ru",
        "fallback_tz": "UTC",
    }
    assert settings(**values).fallback_tz == "UTC"
    with pytest.raises(ValidationError):
        settings(**{**values, "fallback_tz": "not-a-timezone"})


@pytest.mark.parametrize("value", [True, 1.5])
def test_threshold_rejects_non_integer_input(value):
    with pytest.raises(ValidationError):
        settings(**CONSUMER, likes_min_delta=value)


def test_environment_config_and_empty_secret(monkeypatch):
    from notifications.config import Settings

    monkeypatch.setenv("NOTIFICATIONS_PROCESS", "bootstrap")
    monkeypatch.setenv("NOTIFICATIONS_DATABASE_URL", BASE["database_url"])
    # Required fields are provided by the environment above.
    assert Settings().process == "bootstrap"  # pyright: ignore[reportCallIssue]
    with pytest.raises(ValidationError):
        settings(**{**BASE, "process": "api", "jwt_secret": "", "jwt_algorithm": "HS256"})


def test_campaign_api_needs_database_and_timezone_pause_without_jwt():
    values = {**BASE, "process": "campaign-api"}
    with pytest.raises(ValidationError, match="campaign_api_key"):
        settings(**values)
    configured = settings(**values, campaign_api_key="test-key")
    assert configured.process == "campaign-api"
    assert configured.jwt_secret is None
