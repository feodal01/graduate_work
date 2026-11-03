"""Worker environment and CLI reject invalid input before opening clients."""

import argparse
import os

import pytest
from pydantic import SecretStr, ValidationError

from notifications import config
from notifications.delivery import worker as module
from notifications.delivery.limiter_config import LimiterSettings
from notifications.delivery.rate_limit import RedisRateLimiter

WORKER = {
    "process": "worker",
    "database_url": "postgresql+asyncpg://u:p@localhost/db",
    "rabbitmq_url": "amqp://localhost/",
    "smtp_host": "localhost",
    "smtp_port": 1025,
    "smtp_from": "mail@example.org",
    "smtp_starttls": False,
    "fallback_locale": "ru",
    "fallback_tz": "UTC",
    "auth_contacts_url": "https://auth.test/contacts",
    "auth_contacts_api_key": "test-service-key",
    "auth_contacts_timeout_seconds": 5,
    "auth_contacts_retry_seconds": 60,
    "auth_contacts_batch_size": 100,
    "send_class": "transactional",
    "max_concurrency": 2,
    "retry_seconds": 60,
    "max_attempts": 5,
}


@pytest.fixture
def worker_env(monkeypatch):
    for name in os.environ:
        if name.startswith("NOTIFICATIONS_"):
            monkeypatch.delenv(name)
    for name, value in WORKER.items():
        monkeypatch.setenv("NOTIFICATIONS_" + name.upper(), str(value))
    for name, value in {
        "redis_url": "redis://localhost/0",
        "account_id": "worker-config-test",
        "total_rate": 3,
        "total_capacity": 3,
        "transactional_rate": 1,
        "transactional_capacity": 1,
        "service_rate": 1,
        "service_capacity": 1,
        "bulk_rate": 1,
        "bulk_capacity": 1,
        "unavailable_retry_seconds": 1,
        "redis_timeout_seconds": 1,
    }.items():
        monkeypatch.setenv("NOTIFICATIONS_LIMITER_" + name.upper(), str(value))
    return monkeypatch


@pytest.mark.parametrize("send_class", ["transactional", "service", "bulk"])
def test_worker_settings_environment_classes_and_cli_priority(worker_env, send_class):
    worker_env.setenv("NOTIFICATIONS_SEND_CLASS", send_class)
    worker_env.setenv("NOTIFICATIONS_MAX_CONCURRENCY", "4")
    worker_env.setenv("NOTIFICATIONS_POLL_SECONDS", "2.5")
    # Обязательные поля BaseSettings загружаются из окружения теста.
    settings = config.WorkerSettings(max_concurrency=3)  # pyright: ignore[reportCallIssue]
    assert settings.send_class == send_class
    assert settings.max_concurrency == 3
    assert settings.poll_seconds == 2.5
    assert settings.retry_seconds == 60
    assert settings.max_attempts == 5
    assert settings.lease_seconds == 120
    assert settings.smtp_timeout == 30
    assert settings.shutdown_timeout == 45
    assert settings.prefetch == 10
    assert settings.namespace == ""


@pytest.mark.parametrize(
    "name",
    [
        "send_class",
        "max_concurrency",
        "retry_seconds",
        "max_attempts",
        "auth_contacts_url",
        "auth_contacts_api_key",
        "auth_contacts_timeout_seconds",
        "auth_contacts_retry_seconds",
        "auth_contacts_batch_size",
    ],
)
def test_worker_settings_require_explicit_product_parameters(worker_env, name):
    worker_env.delenv("NOTIFICATIONS_" + name.upper())
    with pytest.raises(ValidationError, match=name):
        # Обязательные поля BaseSettings загружаются из окружения теста.
        config.WorkerSettings()  # pyright: ignore[reportCallIssue]


@pytest.mark.parametrize(
    "name", ["retry_seconds", "lease_seconds", "smtp_timeout", "shutdown_timeout", "poll_seconds"]
)
@pytest.mark.parametrize("value", [0, -1, True, float("nan"), float("inf"), float("-inf")])
def test_worker_settings_reject_invalid_intervals(worker_env, name, value):
    with pytest.raises(ValidationError, match=name):
        config.WorkerSettings(**{name: value})


@pytest.mark.parametrize("name", ["max_concurrency", "max_attempts", "prefetch"])
@pytest.mark.parametrize("value", [0, -1, True, 1.5, 1.0])
def test_worker_settings_reject_nonpositive_or_noninteger_counts(worker_env, name, value):
    with pytest.raises(ValidationError, match=name):
        config.WorkerSettings(**{name: value})


@pytest.mark.parametrize(
    "name,value", [("send_class", "other"), ("max_attempts", "true"), ("retry_seconds", "nan")]
)
def test_worker_environment_is_validated(worker_env, name, value):
    worker_env.setenv("NOTIFICATIONS_" + name.upper(), value)
    with pytest.raises(ValidationError, match=name):
        # Обязательные поля BaseSettings загружаются из окружения теста.
        config.WorkerSettings()  # pyright: ignore[reportCallIssue]


def test_worker_settings_validate_lease_against_smtp_timeout(worker_env):
    with pytest.raises(ValidationError, match="lease"):
        # Обязательные поля BaseSettings загружаются из окружения теста.
        config.WorkerSettings(lease_seconds=30, smtp_timeout=30)  # pyright: ignore[reportCallIssue]


def test_worker_secrets_remain_wrapped_and_absent_from_validation_text(worker_env):
    worker_env.setenv("NOTIFICATIONS_SMTP_USERNAME", "mailer")
    worker_env.setenv("NOTIFICATIONS_SMTP_PASSWORD", "private-smtp-password")
    # Обязательные поля BaseSettings загружаются из окружения теста.
    settings = config.WorkerSettings()  # pyright: ignore[reportCallIssue]
    assert isinstance(settings.smtp_password, SecretStr)
    assert "private-smtp-password" not in repr(settings)
    with pytest.raises(ValidationError) as error:
        # Обязательные поля BaseSettings загружаются из окружения теста.
        config.WorkerSettings(lease_seconds=1, smtp_password="private-cli-password")  # pyright: ignore[reportCallIssue]
    assert "private-smtp-password" not in str(error.value)
    assert "private-cli-password" not in str(error.value)


def refuse_resources(*args, **kwargs):
    pytest.fail("runtime resources opened before validation")


@pytest.mark.parametrize("entrypoint", ["configured", "cli"])
@pytest.mark.parametrize(
    "name,value",
    [
        ("send_class", "unknown"),
        ("max_concurrency", 0),
        ("max_attempts", True),
        ("retry_seconds", float("nan")),
        ("lease_seconds", 10),
        ("smtp_timeout", float("inf")),
        ("shutdown_timeout", 0),
        ("poll_seconds", -1),
        ("prefetch", 0),
    ],
)
async def test_entrypoints_validate_before_resources(worker_env, entrypoint, name, value):
    worker_env.setattr(module, "Database", refuse_resources)
    worker_env.setattr(module, "RedisRateLimiter", refuse_resources)
    if entrypoint == "configured":
        worker_env.setenv("NOTIFICATIONS_" + name.upper(), str(value))
        with pytest.raises(ValidationError):
            module.configured_worker()
    else:
        with pytest.raises(ValidationError):
            await module.run(argparse.Namespace(**{name: value}))


@pytest.mark.parametrize(
    "name",
    [
        "send_class",
        "max_concurrency",
        "retry_seconds",
        "max_attempts",
        "auth_contacts_url",
        "auth_contacts_api_key",
        "auth_contacts_timeout_seconds",
        "auth_contacts_retry_seconds",
        "auth_contacts_batch_size",
    ],
)
async def test_cli_refuses_missing_required_setting_before_resources(worker_env, name):
    worker_env.delenv("NOTIFICATIONS_" + name.upper())
    worker_env.setattr(module, "Database", refuse_resources)
    worker_env.setattr(module, "RedisRateLimiter", refuse_resources)
    with pytest.raises(ValidationError, match=name):
        await module.run(argparse.Namespace())


def test_other_process_settings_do_not_require_worker_fields(worker_env):
    for name in ("send_class", "max_concurrency", "retry_seconds", "max_attempts"):
        worker_env.delenv("NOTIFICATIONS_" + name.upper())
    # Обязательные поля BaseSettings загружаются из окружения теста.
    assert config.Settings(process="bootstrap").process == "bootstrap"  # pyright: ignore[reportCallIssue]


def test_limiter_validation_text_does_not_expose_redis_credentials(worker_env):
    with pytest.raises(ValidationError) as error:
        # Обязательные поля BaseSettings загружаются из окружения теста.
        LimiterSettings(redis_url="http://user:private-redis-password@localhost")  # pyright: ignore[reportCallIssue]
    assert "private-redis-password" not in str(error.value)


def test_main_explicit_cli_overrides_invalid_environment_before_validation(worker_env):
    for name in ("send_class", "max_concurrency", "retry_seconds", "max_attempts"):
        worker_env.setenv("NOTIFICATIONS_" + name.upper(), "invalid")
    worker_env.setattr(module, "Database", refuse_resources)
    worker_env.setattr(module, "RedisRateLimiter", refuse_resources)
    worker_env.setattr(
        "sys.argv",
        [
            "worker",
            "--send-class",
            "bulk",
            "--max-concurrency",
            "3",
            "--retry-seconds",
            "60.5",
            "--max-attempts",
            "5",
            "--poll-seconds",
            "0",
        ],
    )
    with pytest.raises(ValidationError) as error:
        module.main()
    assert [item["loc"] for item in error.value.errors()] == [("poll_seconds",)]


async def test_configured_worker_uses_validated_environment_and_closes_clients(worker_env):
    worker_env.setenv("NOTIFICATIONS_SEND_CLASS", "bulk")
    worker_env.setenv("NOTIFICATIONS_LEASE_SECONDS", "180")
    worker_env.setenv("NOTIFICATIONS_SMTP_TIMEOUT", "40")
    worker = module.configured_worker()
    try:
        assert isinstance(worker.settings, config.WorkerSettings)
        assert worker.send_class == "bulk"
        assert worker.max_concurrency == 2
        assert worker.max_attempts == 5
        assert worker.retry_seconds == 60
        assert worker.lease_seconds == 180
        assert worker.smtp_timeout == 40
    finally:
        assert isinstance(worker.limiter, RedisRateLimiter)
        await worker.limiter.aclose()
        await worker.database.close()


@pytest.mark.parametrize(
    "name,value",
    [
        ("auth_contacts_api_key", " "),
        ("auth_contacts_timeout_seconds", True),
        ("auth_contacts_timeout_seconds", 0),
        ("auth_contacts_retry_seconds", float("inf")),
        ("auth_contacts_batch_size", 0),
        ("auth_contacts_batch_size", True),
        ("auth_contacts_batch_size", 1.5),
    ],
)
def test_worker_rejects_invalid_contact_settings(worker_env, name, value):
    with pytest.raises(ValidationError):
        config.WorkerSettings(**{name: value})


@pytest.mark.parametrize(
    "url", ["relative/path", "ftp://auth.test/contacts", "http:///contacts", "https://"]
)
def test_worker_rejects_nonabsolute_contact_http_url(worker_env, url):
    with pytest.raises(ValidationError, match="auth_contacts_url"):
        config.WorkerSettings(auth_contacts_url=url)  # pyright: ignore[reportCallIssue]


@pytest.mark.parametrize(
    "url",
    [
        "http://auth:8000/api/v1/internal/notification-contacts",
        "https://auth.test/contacts?mode=batch",
    ],
)
def test_worker_preserves_valid_contact_url(worker_env, url):
    settings = config.WorkerSettings(auth_contacts_url=url)  # pyright: ignore[reportCallIssue]
    assert settings.auth_contacts_url == url
