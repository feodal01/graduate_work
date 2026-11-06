from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).resolve().parents[2] / '.env',
        env_file_encoding='utf-8',
        extra='ignore',
        hide_input_in_errors=True,
    )

    SECRET_KEY: SecretStr = Field(default=SecretStr('secret_key'), min_length=1)
    DEBUG: bool = False
    POSTGRES_DB: str | None = Field(default=None, min_length=1)
    POSTGRES_USER: str | None = Field(default=None, min_length=1)
    POSTGRES_PASSWORD: SecretStr | None = Field(default=None, min_length=1)
    SQL_HOST: str = Field(default='127.0.0.1', min_length=1)
    SQL_PORT: int = Field(default=5432, ge=1, le=65535)
    SQL_OPTIONS: str | None = None
    AUTH_SERVICE_URL: str = Field(
        default='http://127.0.0.1:8000/api/v1/auth',
        min_length=1,
    )
    AUTH_CLIENT_CONNECT_TIMEOUT_SECONDS: float = Field(
        default=0.5,
        gt=0,
        allow_inf_nan=False,
    )
    AUTH_CLIENT_REQUEST_TIMEOUT_SECONDS: float = Field(
        default=1.0,
        gt=0,
        allow_inf_nan=False,
    )
    AUTH_CLIENT_BACKOFF_MAX_TRIES: int = Field(default=2, ge=1)
    AUTH_CLIENT_BACKOFF_MAX_TIME_SECONDS: float = Field(
        default=0.5,
        gt=0,
        allow_inf_nan=False,
    )
    SERVICE_API_KEY: SecretStr = Field(
        default=SecretStr('local-service-api-key-that-is-32-chars'),
        min_length=1,
    )
    BILLING_INTERNAL_URL: str = Field(
        default='http://127.0.0.1:8000/api/v1/internal/billing',
        min_length=1,
    )
    AUTH_INTERNAL_URL: str = Field(
        default='http://127.0.0.1:8000/api/v1/internal',
        min_length=1,
    )
    BILLING_CLIENT_CONNECT_TIMEOUT_SECONDS: float = Field(
        default=0.5,
        gt=0,
        allow_inf_nan=False,
    )
    BILLING_CLIENT_TIMEOUT_SECONDS: float = Field(
        default=3.0,
        gt=0,
        allow_inf_nan=False,
    )
