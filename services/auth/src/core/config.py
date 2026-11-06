from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import AnyHttpUrl, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL

_REPO_ROOT = Path(__file__).resolve().parents[2]
_ENV_FILE = _REPO_ROOT / '.env'


class PostgresSettings(BaseSettings):
    model_config = SettingsConfigDict(
        hide_input_in_errors=True,
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        env_prefix='POSTGRES_',
        extra='ignore',
    )

    user: str = 'app'
    password: SecretStr = SecretStr('123qwe')
    host: str = 'localhost'
    port: int = 5432
    db: str = 'auth'
    connect_timeout_seconds: float = Field(default=5.0, gt=0)
    pool_timeout_seconds: float = Field(default=10.0, gt=0)
    command_timeout_seconds: float = Field(default=15.0, gt=0)
    statement_timeout_ms: int = Field(default=15_000, gt=0)

    @property
    def dsn(self) -> str:
        return URL.create(
            drivername='postgresql+asyncpg',
            username=self.user,
            password=self.password.get_secret_value(),
            host=self.host,
            port=self.port,
            database=self.db,
        ).render_as_string(hide_password=False)


class RedisSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        env_prefix='REDIS_',
        extra='ignore',
    )

    host: str = Field(default='localhost', min_length=1)
    port: int = Field(default=6379, ge=1, le=65535)
    socket_connect_timeout: float = Field(default=1.0, gt=0)
    socket_timeout: float = Field(default=1.0, gt=0)


class TokenSettings(BaseSettings):
    model_config = SettingsConfigDict(
        hide_input_in_errors=True,
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        extra='ignore',
    )

    jwt_secret: Annotated[SecretStr, Field(min_length=32)]
    jwt_algorithm: Literal['HS256'] = 'HS256'
    access_token_ttl_seconds: int = Field(default=900, gt=0)
    refresh_token_ttl_seconds: int = Field(default=604800, gt=0)


class PasswordPolicySettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        env_prefix='PASSWORD_',
        extra='ignore',
    )

    min_length: int = Field(default=8, ge=1, le=72)
    min_score: int = Field(default=3, ge=0, le=4)


class BackoffSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        env_prefix='BACKOFF_',
        extra='ignore',
    )

    max_tries: int = Field(default=3, ge=1)
    max_time: float = Field(default=2.0, gt=0)


class AuthorizationCacheSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        env_prefix='AUTHORIZATION_CACHE_',
        extra='ignore',
    )

    ttl_seconds: int = Field(default=300, gt=0)


class SessionSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        env_prefix='SESSION_',
        extra='ignore',
    )

    password_change_marker_ttl_seconds: int = Field(default=120, gt=0)


class RateLimitSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        env_prefix='RATE_LIMIT_',
        extra='ignore',
    )

    enabled: bool = True
    login_rate: int = Field(default=5, gt=0)
    login_window_seconds: int = Field(default=60, gt=0)
    login_capacity: int = Field(default=5, gt=0)
    signup_rate: int = Field(default=3, gt=0)
    signup_window_seconds: int = Field(default=60, gt=0)
    signup_capacity: int = Field(default=3, gt=0)
    refresh_rate: int = Field(default=10, gt=0)
    refresh_window_seconds: int = Field(default=60, gt=0)
    refresh_capacity: int = Field(default=5, gt=0)
    user_rate: int = Field(default=60, gt=0)
    user_window_seconds: int = Field(default=60, gt=0)
    user_capacity: int = Field(default=10, gt=0)
    admin_rate: int = Field(default=30, gt=0)
    admin_window_seconds: int = Field(default=60, gt=0)
    admin_capacity: int = Field(default=5, gt=0)
    service_rate: int = Field(default=600, gt=0)
    service_window_seconds: int = Field(default=60, gt=0)
    service_capacity: int = Field(default=50, gt=0)


class TracingSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        env_prefix='TRACING_',
        extra='ignore',
    )

    enabled: bool = False
    service_name: str = Field(default='auth', min_length=1)
    otlp_endpoint: str = Field(
        default='http://127.0.0.1:4318/v1/traces',
        min_length=1,
    )
    sample_rate: float = Field(default=0.05, ge=0.0, le=1.0)


class SentrySettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        env_prefix='SENTRY_',
        extra='ignore',
    )

    dsn: str = ''
    environment: str = 'local'
    traces_sample_rate: float = Field(default=0.0, ge=0.0, le=1.0)


class LogstashSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        env_prefix='LOGSTASH_',
        extra='ignore',
    )

    host: str = ''
    port: int = Field(default=5044, ge=1, le=65535)


class YandexOAuthSettings(BaseSettings):
    model_config = SettingsConfigDict(
        hide_input_in_errors=True,
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        env_prefix='YANDEX_OAUTH_',
        extra='ignore',
    )

    enabled: bool = False
    client_id: str | None = Field(default=None, min_length=1)
    client_secret: SecretStr | None = Field(default=None, min_length=1)
    redirect_uri: AnyHttpUrl | None = None
    state_ttl_seconds: int = Field(default=600, ge=60, le=600)
    connect_timeout_seconds: float = Field(default=1.0, gt=0)
    request_timeout_seconds: float = Field(default=3.0, gt=0)
    userinfo_max_tries: int = Field(default=3, ge=1, le=5)
    userinfo_backoff_max_seconds: float = Field(default=2.0, gt=0)

    @model_validator(mode='after')
    def require_enabled_configuration(self) -> Self:
        if not self.enabled:
            return self
        if (
            self.client_id is None
            or self.client_secret is None
            or not self.client_secret.get_secret_value()
            or self.redirect_uri is None
        ):
            raise ValueError('Yandex OAuth credentials and redirect URI are required')
        return self


class ServiceSettings(BaseSettings):
    model_config = SettingsConfigDict(
        hide_input_in_errors=True,
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        env_prefix='SERVICE_',
        extra='ignore',
    )

    api_key: Annotated[SecretStr, Field(min_length=32)]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        hide_input_in_errors=True,
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore',
    )

    project_name: str = 'auth service'
    auth_notification_contacts_batch_size: int = Field(default=100, ge=1)
    auth_user_lookup_batch_size: int = Field(default=100, ge=1)

    postgres: PostgresSettings
    redis: RedisSettings
    password_policy: PasswordPolicySettings
    backoff: BackoffSettings
    authorization_cache: AuthorizationCacheSettings
    rate_limit: RateLimitSettings
    tracing: TracingSettings
    yandex_oauth: YandexOAuthSettings
    session: SessionSettings
    sentry: SentrySettings = Field(default_factory=SentrySettings)
    logstash: LogstashSettings = Field(default_factory=LogstashSettings)

    @model_validator(mode='after')
    def validate_password_change_window(self) -> Self:
        database_window = (
            self.postgres.pool_timeout_seconds
            + self.postgres.connect_timeout_seconds
            + 2 * self.postgres.command_timeout_seconds
        )
        minimum_marker_ttl = database_window + 30
        if self.session.password_change_marker_ttl_seconds < minimum_marker_ttl:
            raise ValueError(
                'password change marker TTL must be at least '
                f'{minimum_marker_ttl:g} seconds for configured PostgreSQL '
                'timeouts'
            )
        return self


settings = Settings(
    postgres=PostgresSettings(),
    redis=RedisSettings(),
    password_policy=PasswordPolicySettings(),
    backoff=BackoffSettings(),
    authorization_cache=AuthorizationCacheSettings(),
    rate_limit=RateLimitSettings(),
    tracing=TracingSettings(),
    yandex_oauth=YandexOAuthSettings(),
    session=SessionSettings(),
)


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        env_prefix='KAFKA_',
        extra='ignore',
    )

    bootstrap_servers: str = Field(default='localhost:9092', min_length=1)


class AuthRelaySettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        env_prefix='AUTH_RELAY_',
        extra='ignore',
    )

    batch_size: int = Field(default=500, ge=1, le=500)
    topic: str = Field(default='auth.users.v1', min_length=1)
    poll_interval_seconds: float = Field(default=1.0, gt=0)
    delivery_timeout_seconds: float = Field(default=30.0, gt=0)
    shutdown_timeout_seconds: float = Field(default=10.0, gt=0)
