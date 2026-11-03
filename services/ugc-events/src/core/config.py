from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

_REPO_ROOT = Path(__file__).resolve().parents[2]
_ENV_FILE = _REPO_ROOT / '.env'


class Settings(BaseSettings):
    """Конфиг приложения из env / .env."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        extra='ignore',
    )

    project_name: str = 'ugc-events'
    sentry_dsn: str = ''
    sentry_environment: str = 'local'
    sentry_traces_sample_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    logstash_host: str = ''
    logstash_port: int = Field(default=5044, ge=1, le=65535)

    auth_jwt_secret: Annotated[
        SecretStr,
        Field(min_length=32),
    ] = SecretStr('')
    auth_jwt_algorithm: Literal['HS256'] = 'HS256'

    kafka_bootstrap_servers: str = Field(min_length=1)
    kafka_topic: str = 'ugc.events'
    kafka_max_request_size: int = Field(default=1_048_576, ge=1)
    events_max_batch_size: int = Field(default=100, ge=1, le=1000)
    events_max_age_seconds: int = Field(default=604800, gt=0)
    events_max_future_skew_seconds: int = Field(default=300, ge=0)
    redis_host: str = 'localhost'
    redis_port: int = Field(default=6379, ge=1, le=65535)
    redis_socket_connect_timeout: float = Field(default=1.0, gt=0)
    redis_socket_timeout: float = Field(default=1.0, gt=0)
    events_rate_limit_rate: int = Field(default=60, ge=1)
    events_rate_limit_window_seconds: int = Field(default=60, ge=1)
    events_rate_limit_capacity: int = Field(default=10, ge=1)


settings = Settings()
