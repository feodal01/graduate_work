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

    project_name: str = 'movies'
    sentry_dsn: str = ''
    sentry_environment: str = 'local'
    sentry_traces_sample_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    logstash_host: str = ''
    logstash_port: int = Field(default=5044, ge=1, le=65535)

    redis_host: str = Field(default='', min_length=1)
    redis_port: int = Field(default=6379, ge=1, le=65535)
    redis_socket_connect_timeout: float = Field(default=1.0, gt=0)
    redis_socket_timeout: float = Field(default=1.0, gt=0)

    elasticsearch_host: str = Field(default='', min_length=1)
    elasticsearch_port: int = Field(default=9200, ge=1, le=65535)
    elasticsearch_index: str = Field(default='', min_length=1)
    elasticsearch_request_timeout: float = Field(default=2.0, gt=0)

    backoff_max_tries: int = Field(default=5, ge=1)
    backoff_max_time: float = Field(default=3.0, gt=0)

    max_elasticsearch_result_window: int = Field(default=10_000, ge=1)
    max_page_size_api_response: int = Field(default=100, ge=1)

    auth_jwt_secret: Annotated[
        SecretStr,
        Field(min_length=32),
    ] = SecretStr('')
    auth_jwt_algorithm: Literal['HS256'] = 'HS256'
    auth_service_url: str = Field(default='', min_length=1)
    auth_service_api_key: Annotated[
        SecretStr,
        Field(min_length=32),
    ] = SecretStr('')
    auth_connect_timeout: float = Field(default=0.5, gt=0)
    auth_request_timeout: float = Field(default=1.0, gt=0)
    auth_backoff_max_tries: int = Field(default=2, ge=1)
    auth_backoff_max_time: float = Field(default=0.5, gt=0)


settings = Settings()
