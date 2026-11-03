import pytest
from pydantic import ValidationError

from core import config

# Pyright synthesizes Settings signatures without the runtime _env_file option.


def _build_settings(
    *,
    postgres: config.PostgresSettings,
    session: config.SessionSettings,
) -> config.Settings:
    return config.Settings(
        postgres=postgres,
        redis=config.settings.redis,
        password_policy=config.settings.password_policy,
        backoff=config.settings.backoff,
        authorization_cache=config.settings.authorization_cache,
        rate_limit=config.settings.rate_limit,
        tracing=config.settings.tracing,
        yandex_oauth=config.settings.yandex_oauth,
        session=session,
    )


def test_yandex_oauth_is_disabled_without_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Disabled social login must not require provider credentials."""
    for name in (
        'YANDEX_OAUTH_ENABLED',
        'YANDEX_OAUTH_CLIENT_ID',
        'YANDEX_OAUTH_CLIENT_SECRET',
        'YANDEX_OAUTH_REDIRECT_URI',
    ):
        monkeypatch.delenv(name, raising=False)

    value = config.YandexOAuthSettings(_env_file=None)  # pyright: ignore[reportCallIssue]

    assert value.enabled is False
    assert value.client_id is None
    assert value.client_secret is None
    assert value.redirect_uri is None


def test_yandex_oauth_requires_credentials_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Enabled social login must fail before serving incomplete config."""
    monkeypatch.setenv('YANDEX_OAUTH_ENABLED', 'true')
    for name in (
        'YANDEX_OAUTH_CLIENT_ID',
        'YANDEX_OAUTH_CLIENT_SECRET',
        'YANDEX_OAUTH_REDIRECT_URI',
    ):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(ValidationError):
        config.YandexOAuthSettings(_env_file=None)  # pyright: ignore[reportCallIssue]


def test_yandex_oauth_loads_complete_runtime_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """All provider limits must use their dedicated environment values."""
    monkeypatch.setenv('YANDEX_OAUTH_ENABLED', 'true')
    monkeypatch.setenv('YANDEX_OAUTH_CLIENT_ID', 'test-client-id')
    monkeypatch.setenv('YANDEX_OAUTH_CLIENT_SECRET', 'test-client-secret')
    monkeypatch.setenv(
        'YANDEX_OAUTH_REDIRECT_URI',
        'http://localhost/api/v1/auth/social/yandex/callback',
    )
    monkeypatch.setenv('YANDEX_OAUTH_STATE_TTL_SECONDS', '300')
    monkeypatch.setenv('YANDEX_OAUTH_CONNECT_TIMEOUT_SECONDS', '1.5')
    monkeypatch.setenv('YANDEX_OAUTH_REQUEST_TIMEOUT_SECONDS', '4.5')
    monkeypatch.setenv('YANDEX_OAUTH_USERINFO_MAX_TRIES', '4')
    monkeypatch.setenv('YANDEX_OAUTH_USERINFO_BACKOFF_MAX_SECONDS', '3.5')

    value = config.YandexOAuthSettings(_env_file=None)  # pyright: ignore[reportCallIssue]

    assert value.enabled is True
    assert value.client_id == 'test-client-id'
    assert value.client_secret is not None
    assert value.client_secret.get_secret_value() == 'test-client-secret'
    assert str(value.redirect_uri) == (
        'http://localhost/api/v1/auth/social/yandex/callback'
    )
    assert value.state_ttl_seconds == 300
    assert value.connect_timeout_seconds == 1.5
    assert value.request_timeout_seconds == 4.5
    assert value.userinfo_max_tries == 4
    assert value.userinfo_backoff_max_seconds == 3.5


def test_rate_limit_settings_load_all_policies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each Auth rate-limit policy uses its dedicated environment values."""
    monkeypatch.setenv('RATE_LIMIT_ENABLED', 'true')
    expected = {
        'login': (7, 90, 4),
        'signup': (8, 91, 5),
        'refresh': (9, 92, 6),
        'user': (10, 93, 7),
        'admin': (11, 94, 8),
        'service': (12, 95, 9),
    }
    for policy_name, (rate, window_seconds, capacity) in expected.items():
        prefix = f'RATE_LIMIT_{policy_name.upper()}'
        monkeypatch.setenv(f'{prefix}_RATE', str(rate))
        monkeypatch.setenv(
            f'{prefix}_WINDOW_SECONDS',
            str(window_seconds),
        )
        monkeypatch.setenv(f'{prefix}_CAPACITY', str(capacity))

    value = config.RateLimitSettings(_env_file=None)  # pyright: ignore[reportCallIssue]

    assert value.enabled is True
    for policy_name, (rate, window_seconds, capacity) in expected.items():
        assert getattr(value, f'{policy_name}_rate') == rate
        assert getattr(value, f'{policy_name}_window_seconds') == window_seconds
        assert getattr(value, f'{policy_name}_capacity') == capacity


@pytest.mark.parametrize(
    ('environment_name', 'invalid_value'),
    [
        ('RATE_LIMIT_LOGIN_RATE', '0'),
        ('RATE_LIMIT_SIGNUP_WINDOW_SECONDS', '0'),
        ('RATE_LIMIT_SERVICE_CAPACITY', '-1'),
    ],
)
def test_rate_limit_settings_reject_non_positive_policy_values(
    monkeypatch: pytest.MonkeyPatch,
    environment_name: str,
    invalid_value: str,
) -> None:
    """A policy cannot disable a numeric bound with zero or a negative value."""
    monkeypatch.setenv(environment_name, invalid_value)

    with pytest.raises(ValidationError):
        config.RateLimitSettings(_env_file=None)  # pyright: ignore[reportCallIssue]


def test_tracing_settings_are_loaded_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tracing settings use the dedicated environment namespace."""
    monkeypatch.setenv('TRACING_ENABLED', 'true')
    monkeypatch.setenv('TRACING_SERVICE_NAME', 'auth-test')
    monkeypatch.setenv(
        'TRACING_OTLP_ENDPOINT',
        'http://jaeger:4318/v1/traces',
    )
    monkeypatch.setenv('TRACING_SAMPLE_RATE', '0.05')

    tracing = config.TracingSettings(_env_file=None)  # pyright: ignore[reportCallIssue]

    assert tracing.enabled is True
    assert tracing.service_name == 'auth-test'
    assert tracing.otlp_endpoint == 'http://jaeger:4318/v1/traces'
    assert tracing.sample_rate == 0.05


@pytest.mark.parametrize('sample_rate', ['-0.01', '1.01'])
def test_tracing_sample_rate_must_be_between_zero_and_one(
    monkeypatch: pytest.MonkeyPatch,
    sample_rate: str,
) -> None:
    """Invalid sampling values fail during configuration loading."""
    monkeypatch.setenv('TRACING_SAMPLE_RATE', sample_rate)

    with pytest.raises(ValidationError):
        config.TracingSettings(_env_file=None)  # pyright: ignore[reportCallIssue]


def test_password_policy_settings_are_loaded_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Настройки длины и сложности пароля читаются из окружения."""
    monkeypatch.setenv('PASSWORD_MIN_LENGTH', '12')
    monkeypatch.setenv('PASSWORD_MIN_SCORE', '4')
    settings_type = getattr(config, 'PasswordPolicySettings', None)

    assert settings_type is not None
    password_settings = settings_type(_env_file=None)
    assert password_settings.min_length == 12
    assert password_settings.min_score == 4


def test_backoff_settings_are_loaded_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ограничения повторных попыток читаются из окружения."""
    monkeypatch.setenv('BACKOFF_MAX_TRIES', '4')
    monkeypatch.setenv('BACKOFF_MAX_TIME', '1.5')
    settings_type = getattr(config, 'BackoffSettings', None)

    assert settings_type is not None
    backoff_settings = settings_type(_env_file=None)
    assert backoff_settings.max_tries == 4
    assert backoff_settings.max_time == 1.5


def test_authorization_cache_ttl_is_loaded_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Время жизни RBAC-кэша читается из окружения."""
    monkeypatch.setenv('AUTHORIZATION_CACHE_TTL_SECONDS', '120')
    settings_type = getattr(config, 'AuthorizationCacheSettings', None)

    assert settings_type is not None
    cache_settings = settings_type(_env_file=None)
    assert cache_settings.ttl_seconds == 120


def test_service_api_key_is_required_and_loaded_as_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Service key обязателен и хранится как секретное значение."""
    monkeypatch.delenv('SERVICE_API_KEY', raising=False)
    settings_type = getattr(config, 'ServiceSettings', None)

    assert settings_type is not None
    with pytest.raises(ValidationError):
        settings_type(_env_file=None)

    monkeypatch.setenv(
        'SERVICE_API_KEY',
        'test-internal-service-key-with-32-characters',
    )
    service_settings = settings_type(_env_file=None)
    assert service_settings.api_key.get_secret_value() == (
        'test-internal-service-key-with-32-characters'
    )


def test_password_change_timeouts_have_safe_defaults() -> None:
    """PostgreSQL must settle before the Redis mutation marker can expire."""
    postgres = config.PostgresSettings(_env_file=None)  # pyright: ignore[reportCallIssue]
    session = config.SessionSettings(_env_file=None)  # pyright: ignore[reportCallIssue]

    assert postgres.connect_timeout_seconds == 5.0
    assert postgres.pool_timeout_seconds == 10.0
    assert postgres.command_timeout_seconds == 15.0
    assert postgres.statement_timeout_ms == 15_000
    assert session.password_change_marker_ttl_seconds == 120

    value = _build_settings(postgres=postgres, session=session)
    database_window = (
        value.postgres.pool_timeout_seconds
        + value.postgres.connect_timeout_seconds
        + 2 * value.postgres.command_timeout_seconds
    )
    assert value.session.password_change_marker_ttl_seconds >= (database_window + 30)


def test_password_change_marker_rejects_unsafe_database_window() -> None:
    """An expiring marker cannot expose sessions while DB work is unsettled."""
    postgres = config.PostgresSettings(_env_file=None)  # pyright: ignore[reportCallIssue]
    session = config.SessionSettings(
        password_change_marker_ttl_seconds=74,
        _env_file=None,  # pyright: ignore[reportCallIssue]
    )

    with pytest.raises(
        ValidationError,
        match='password change marker TTL',
    ):
        _build_settings(postgres=postgres, session=session)


def test_postgres_secret_and_dsn_preserve_environment_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Database URLs unwrap and encode the original environment secret."""
    from sqlalchemy.engine import make_url

    secret = 'fake@:/?#%'
    monkeypatch.setenv('POSTGRES_PASSWORD', secret)
    value = config.PostgresSettings(_env_file=None)  # pyright: ignore[reportCallIssue]

    assert secret not in repr(value)
    assert value.password.get_secret_value() == secret
    assert make_url(value.dsn).password == secret


def test_postgres_validation_error_hides_input() -> None:
    """Invalid database settings never format the supplied password."""
    with pytest.raises(ValidationError) as error:
        config.PostgresSettings(
            _env_file=None,  # pyright: ignore[reportCallIssue]
            password='FAKE32',
            port='bad',
        )

    assert 'FAKE32' not in str(error.value)
    assert 'input_value' not in str(error.value)


def test_nested_postgres_validation_error_hides_input() -> None:
    """The outer model also hides raw dictionaries from nested failures."""
    with pytest.raises(ValidationError) as error:
        config.Settings(  # pyright: ignore[reportCallIssue]
            _env_file=None,  # pyright: ignore[reportCallIssue]
            postgres={'password': 'FAKE32', 'port': 'bad'},
        )

    assert 'FAKE32' not in str(error.value)
    assert 'input_value' not in str(error.value)


@pytest.mark.parametrize(
    ('settings_type', 'values'),
    [
        (config.TokenSettings, {'jwt_secret': 'FAKE32'}),
        (config.ServiceSettings, {'api_key': 'FAKE32'}),
        (
            config.YandexOAuthSettings,
            {'enabled': True, 'client_id': 'fake', 'client_secret': 'FAKE32'},
        ),
    ],
)
def test_secret_settings_validation_hides_input(
    settings_type: type[config.BaseSettings],
    values: dict[str, object],
) -> None:
    """Short or incomplete secret configuration stays absent from tracebacks."""
    with pytest.raises(ValidationError) as error:
        # Keep .env disabled while passing intentionally invalid raw values.
        settings_type(_env_file=None, **values)  # pyright: ignore[reportArgumentType]

    assert 'FAKE32' not in str(error.value)
    assert 'FAKE32' not in repr(error.value)
    assert 'input_value' not in str(error.value)
