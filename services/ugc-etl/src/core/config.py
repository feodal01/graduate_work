from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, Optional, TypeVar, cast

import backoff
from aiochclient.exceptions import ChClientError
from aiohttp import ClientError, ClientResponseError, ClientSession, ClientTimeout
from pydantic import BaseModel, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

_SERVICE_ROOT = Path(__file__).resolve().parents[2]
_REPO_ROOT: Optional[Path] = _SERVICE_ROOT.parents[1] if len(_SERVICE_ROOT.parents) > 1 else None
_ENV_FILE = _SERVICE_ROOT / '.env'

_NETWORK = (OSError, ConnectionError, TimeoutError)
CLICKHOUSE_RETRY = (*_NETWORK, ClientError, ChClientError)

F = TypeVar('F', bound=Callable[..., Any])


def _http_status_from_clickhouse_error(exc: Exception) -> Optional[int]:
    if isinstance(exc, ClientResponseError):
        return exc.status
    text = str(exc)
    marker = 'HTTP status '
    start = text.find(marker)
    if start == -1:
        return None
    index = start + len(marker)
    digits: list[str] = []
    while index < len(text) and text[index].isdigit():
        digits.append(text[index])
        index += 1
    if not digits:
        return None
    return int(''.join(digits))


def _clickhouse_giveup(exc: Exception) -> bool:
    if isinstance(exc, ClientError) and not isinstance(exc, ClientResponseError):
        return False
    if isinstance(exc, ChClientError) and _http_status_from_clickhouse_error(exc) is None:
        return False
    status = _http_status_from_clickhouse_error(exc)
    if status is not None:
        return 400 <= status < 500
    return False


class BackoffSettings(BaseModel):
    max_tries: int = Field(default=5, gt=0)
    initial_delay: float = Field(default=0.5, gt=0)
    max_delay: float = Field(default=30.0, gt=0)
    multiplier: float = Field(default=2.0, gt=0)

    def decorator(
        self,
        *exceptions: type[Exception],
        giveup: Optional[Callable[[Exception], bool]] = None,
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

    def for_clickhouse(self) -> Callable[[F], F]:
        return self.decorator(*CLICKHOUSE_RETRY, giveup=_clickhouse_giveup)


class Settings(BaseSettings):
    """Конфиг ETL из env / сервисного .env."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding='utf-8',
        extra='ignore',
        env_nested_delimiter='__',
    )

    project_name: str = 'ugc-etl'
    sentry_dsn: str = ''
    sentry_environment: str = 'local'
    sentry_traces_sample_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    logstash_host: str = ''
    logstash_port: int = Field(default=5044, ge=1, le=65535)
    log_level: str = 'INFO'
    kafka_bootstrap_servers: str = Field(min_length=1)
    kafka_topic: str = 'ugc.events'
    kafka_dlq_topic: str = 'ugc.events.dlq'
    kafka_group_id: str = 'ugc-etl'
    kafka_max_request_size: int = Field(default=1_048_576, ge=1)
    events_max_age_seconds: int = Field(default=604800, gt=0)
    events_max_future_skew_seconds: int = Field(default=300, ge=0)
    clickhouse_host: str = '127.0.0.1'
    clickhouse_port: int = 8123
    clickhouse_database: str = 'ugc'
    clickhouse_user: str = Field(min_length=1)
    clickhouse_password: SecretStr
    clickhouse_timeout_seconds: int = 60
    clickhouse_schema_path: str = '/app/init.sql'
    etl_batch_size: int = 10000
    etl_batch_max_bytes: int = 8388608
    etl_flush_interval_seconds: float = 5
    etl_stall_timeout_seconds: float = Field(default=360, gt=0, allow_inf_nan=False)
    etl_memory_limit_bytes: int = 268435456
    clickhouse_backoff: BackoffSettings = Field(
        default_factory=lambda: BackoffSettings(
            max_tries=5,
            initial_delay=0.5,
            max_delay=30.0,
            multiplier=2.0,
        )
    )

    def schema_file(self) -> Path:
        path = Path(self.clickhouse_schema_path)
        if path.is_absolute():
            return path
        if _REPO_ROOT is not None:
            return _REPO_ROOT / path
        return _SERVICE_ROOT / path

    def clickhouse_url(self) -> str:
        return f'http://{self.clickhouse_host}:{self.clickhouse_port}/'


# BaseSettings читает обязательные поля из env / .env; Pylance этого не учитывает.
settings = Settings()  # pyright: ignore[reportCallIssue]


def split_sql(script: str) -> Iterator[str]:
    for part in script.split(';'):
        statement = part.strip()
        if statement:
            yield statement


def build_http_session() -> ClientSession:
    timeout = ClientTimeout(total=settings.clickhouse_timeout_seconds)
    return ClientSession(timeout=timeout)
