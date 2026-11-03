from collections.abc import Callable
from typing import Any, TypeVar, cast

import backoff
import requests
from psycopg2 import (
    DatabaseError,
    DataError,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from pydantic import BaseModel, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_NETWORK = (OSError, ConnectionError, TimeoutError)

POSTGRES_RETRY = (*_NETWORK, InterfaceError, OperationalError)
POSTGRES_GIVEUP = (
    ProgrammingError,
    IntegrityError,
    DataError,
    InternalError,
    NotSupportedError,
)
POSTGRES_AUTH_GIVEUP_CODES = frozenset({"28000", "28P01"})
POSTGRES_EXCEPTIONS = (*POSTGRES_RETRY, *POSTGRES_GIVEUP, DatabaseError)


def _postgres_giveup(exc: Exception) -> bool:
    if isinstance(exc, POSTGRES_GIVEUP):
        return True
    if getattr(exc, "pgcode", None) in POSTGRES_AUTH_GIVEUP_CODES:
        return True
    if isinstance(exc, POSTGRES_RETRY):
        return False
    return True


ES_RETRIABLE = (*_NETWORK, requests.exceptions.RequestException)


def _es_giveup(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    return response is not None and 400 <= response.status_code < 500


F = TypeVar("F", bound=Callable[..., Any])


class BackoffSettings(BaseModel):
    max_tries: int = Field(default=5, gt=0)
    initial_delay: float = Field(default=0.5, gt=0)
    max_delay: float = Field(default=60.0, gt=0)
    multiplier: float = Field(default=2.0, gt=0)

    def decorator(
        self,
        *exceptions: type[Exception],
        giveup: Callable[[Exception], bool] | None = None,
    ) -> Callable[[F], F]:
        return cast(
            Callable[[F], F],
            backoff.on_exception(
                backoff.expo,
                exceptions,
                max_tries=self.max_tries,
                jitter=backoff.full_jitter,
                giveup=giveup or (lambda _exc: False),
                base=self.multiplier,
                factor=self.initial_delay,
                max_value=self.max_delay,
            ),
        )

    def for_postgres(self) -> Callable[[F], F]:
        return self.decorator(*POSTGRES_EXCEPTIONS, giveup=_postgres_giveup)

    def for_elasticsearch(self) -> Callable[[F], F]:
        return self.decorator(*ES_RETRIABLE, giveup=_es_giveup)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        hide_input_in_errors=True,
        env_file=".env",
        env_nested_delimiter="__",
        extra="ignore",
    )

    state_database_file_path: str = "state.sqlite"
    producer_batch_size: int = Field(default=500, gt=0)
    worker_lease_seconds: int = Field(default=300, gt=0)
    job_max_attempts: int = Field(default=5, gt=0)
    worker_poll_interval_seconds: int = Field(default=5, gt=0)
    log_level: str = "INFO"
    # Product cadence/window choices are mandatory only for the active detector process.
    catalog_scan_interval_seconds: float | None = Field(default=None, gt=0)
    catalog_overlap_seconds: float | None = Field(default=None, ge=0)
    catalog_reconcile_interval_seconds: float | None = Field(default=None, gt=0)
    catalog_batch_size: int = Field(default=500, gt=0, le=10000)
    catalog_relay_batch_size: int = Field(default=500, gt=0)
    catalog_legacy_batch_size: int = Field(default=500, gt=0)

    @field_validator(
        "producer_batch_size",
        "catalog_batch_size",
        "catalog_relay_batch_size",
        "catalog_legacy_batch_size",
        mode="before",
    )
    @classmethod
    def integer_batch_size(cls, value: Any) -> Any:
        if isinstance(value, (bool, float)):
            raise ValueError("batch sizes must be integers")
        return value

    catalog_relay_pause_seconds: float = Field(default=1.0, gt=0)
    catalog_relay_delivery_timeout_seconds: float = Field(default=30.0, gt=0)
    kafka_bootstrap_servers: str | None = None
    kafka_movies_topic: str = "movies.films.v1"
    postgres_db: str
    postgres_user: str
    postgres_password: SecretStr
    postgres_host: str = Field(default="127.0.0.1", validation_alias="SQL_HOST")
    postgres_port: int = Field(default=5432, gt=0, validation_alias="SQL_PORT")
    elasticsearch_url: str
    elasticsearch_index: str
    elasticsearch_timeout_seconds: int = Field(default=30, gt=0)
    postgres_backoff: BackoffSettings = Field(
        default_factory=lambda: BackoffSettings(
            max_tries=10,
            initial_delay=1.0,
            max_delay=60.0,
            multiplier=2.0,
        )
    )
    elasticsearch_backoff: BackoffSettings = Field(
        default_factory=lambda: BackoffSettings(
            max_tries=5,
            initial_delay=0.5,
            max_delay=30.0,
            multiplier=2.0,
        )
    )


# BaseSettings читает обязательные поля из окружения; Pyright этого не учитывает.
settings = Settings()  # pyright: ignore[reportCallIssue]
_settings_dump = settings.model_dump()
postgres_backoff_settings = BackoffSettings.model_validate(_settings_dump["postgres_backoff"])
elasticsearch_backoff_settings = BackoffSettings.model_validate(
    _settings_dump["elasticsearch_backoff"]
)
