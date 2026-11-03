from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import Field, SecretStr, field_validator, model_validator
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

    project_name: str = 'ugc-api'
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

    mongo_uri: str = Field(min_length=1)
    mongo_db: str = 'ugc'
    mongo_server_selection_timeout_ms: int = Field(default=5000, ge=100)

    kafka_bootstrap_servers: str = ''
    kafka_likes_topic: str = 'ugc.reviews.v1'
    relay_batch_size: int = Field(default=500, ge=1, le=5000)
    relay_poll_interval_seconds: float = Field(default=1.0, gt=0)
    relay_batch_timeout_seconds: float = Field(default=20.0, gt=0)
    relay_commit_timeout_ms: int = Field(default=5000, ge=100)
    # Must not exceed transactionLifetimeLimitSeconds on any MongoDB shard.
    relay_mongo_transaction_lifetime_seconds: int = Field(default=60, ge=10)

    @model_validator(mode='after')
    def validate_relay_deadline(self) -> 'Settings':
        total = self.relay_batch_timeout_seconds + self.relay_commit_timeout_ms / 1000 + 5
        if total >= self.relay_mongo_transaction_lifetime_seconds:
            raise ValueError('Relay batch and commit deadlines need a 5s Mongo transaction margin')
        return self

    bootstrap_batch_size: int = Field(default=500, ge=1, le=5000)

    @field_validator('relay_batch_size', 'bootstrap_batch_size', mode='before')
    @classmethod
    def integer_batch_size(cls, value: Any) -> Any:
        if isinstance(value, (bool, float)):
            raise ValueError('batch sizes must be integers')
        return value

    max_page_size: int = Field(default=100, ge=1, le=1000)


# Required mongo_uri is supplied by the Pydantic environment source.
settings = Settings()  # pyright: ignore[reportCallIssue]
