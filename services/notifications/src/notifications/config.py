"""Validated process settings; timing defaults preserve the initial delivery policy."""

import re
from datetime import UTC, datetime, time, timedelta
from typing import Any, Literal, Self
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import (
    Field,
    SecretStr,
    ValidationInfo,
    field_serializer,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict


class BatchSettings(BaseSettings):
    """Operational batch sizes shared by processes and standalone schema validation."""

    model_config = SettingsConfigDict(
        env_prefix="NOTIFICATIONS_", extra="forbid", hide_input_in_errors=True
    )

    scheduler_batch_size: int = Field(default=500, ge=1)
    transactional_batch_size: int = Field(default=100, ge=1)
    service_batch_size: int = Field(default=50, ge=1)
    bulk_batch_size: int = Field(default=25, ge=1)
    recovery_batch_size: int = Field(default=100, ge=1)
    consumer_batch_size: int = Field(default=500, ge=1)
    campaign_batch_size: int = Field(default=100, ge=1)

    @field_validator(
        "scheduler_batch_size",
        "transactional_batch_size",
        "service_batch_size",
        "bulk_batch_size",
        "recovery_batch_size",
        "consumer_batch_size",
        "campaign_batch_size",
        mode="before",
    )
    @classmethod
    def integer_batch_size(cls, value: Any) -> Any:
        if isinstance(value, (bool, float)):
            raise ValueError("batch sizes must be integers")
        return value


class TimingSettings(BatchSettings):
    """Shared window, UTC cadence and digest size policy."""

    likes_schedule_anchor_utc: datetime = datetime(1970, 1, 1, tzinfo=UTC)
    films_schedule_anchor_utc: datetime = datetime(1970, 1, 1, tzinfo=UTC)
    send_window_start: time = time(10)
    send_window_end: time = time(20)
    films_per_digest: int = Field(default=20, ge=1)

    @field_validator("films_per_digest", mode="before")
    @classmethod
    def integer_digest_size(cls, value: Any) -> Any:
        if isinstance(value, (bool, float)):
            raise ValueError("films_per_digest must be an integer")
        return value

    @field_validator("likes_schedule_anchor_utc", "films_schedule_anchor_utc", mode="before")
    @classmethod
    def explicit_anchor(cls, value: Any) -> Any:
        if not isinstance(value, (str, datetime)):
            raise ValueError("schedule anchor must be an explicit UTC datetime")
        return value

    @field_validator("likes_schedule_anchor_utc", "films_schedule_anchor_utc")
    @classmethod
    def utc_anchor(cls, value: datetime) -> datetime:
        if value.utcoffset() != timedelta(0):
            raise ValueError("schedule anchor must be timezone-aware UTC")
        return value.astimezone(UTC)

    @field_validator("send_window_start", "send_window_end", mode="before")
    @classmethod
    def local_minute(cls, value: Any) -> Any:
        if isinstance(value, str):
            if re.fullmatch(r"[0-9]{2}:[0-9]{2}", value) is None:
                raise ValueError("send window must use HH:MM local time")
        elif not isinstance(value, time):
            raise ValueError("send window must use HH:MM local time")
        return value

    @field_validator("send_window_start", "send_window_end")
    @classmethod
    def naive_minute(cls, value: time) -> time:
        if value.tzinfo is not None or value.second or value.microsecond:
            raise ValueError("send window must use local time with minute precision")
        return value

    @field_serializer("send_window_start", "send_window_end", when_used="json")
    def window_json(self, value: time) -> str:
        return value.strftime("%H:%M")

    @model_validator(mode="after")
    def ordered_window(self) -> Self:
        if self.send_window_start >= self.send_window_end:
            raise ValueError("send_window_start must be earlier than send_window_end")
        return self


class Settings(TimingSettings):
    process: Literal[
        "api", "campaign-api", "consumer", "scheduler", "publisher", "worker", "bootstrap"
    ]
    database_url: str = Field(min_length=1)
    kafka_bootstrap_servers: str | None = Field(default=None, min_length=1)
    initial_likes: bool | None = None
    initial_new_films: bool | None = None
    initial_marketing: bool | None = None
    likes_min_delta: int | None = Field(default=None, ge=1)
    likes_interval_seconds: int | None = Field(default=None, ge=1)
    films_interval_seconds: int | None = Field(default=None, ge=1)
    likes_bootstrap_load_id: str | None = Field(default=None, min_length=1)
    campaign_api_key: SecretStr | None = None
    jwt_secret: SecretStr | None = None
    jwt_algorithm: str | None = Field(default=None, min_length=1)
    auth_introspection_url: str | None = Field(default=None, min_length=1)
    auth_introspection_api_key: SecretStr | None = None
    auth_introspection_timeout_seconds: float | None = Field(
        default=None, gt=0, allow_inf_nan=False
    )
    rabbitmq_url: str | None = Field(default=None, min_length=1)
    smtp_host: str | None = Field(default=None, min_length=1)
    smtp_port: int | None = Field(default=None, ge=1, le=65535)
    smtp_from: str | None = Field(default=None, min_length=1)
    smtp_starttls: bool | None = None
    smtp_username: str | None = Field(default=None, min_length=1)
    smtp_password: SecretStr | None = None
    fallback_locale: str | None = Field(default=None, min_length=1)
    fallback_tz: str | None = Field(default=None, min_length=1)
    auth_contacts_url: str | None = Field(default=None, min_length=1)
    auth_contacts_api_key: SecretStr | None = None
    auth_contacts_timeout_seconds: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    auth_contacts_retry_seconds: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    auth_contacts_batch_size: int | None = Field(default=None, ge=1)

    @field_validator("auth_contacts_url", "auth_introspection_url")
    @classmethod
    def absolute_auth_http_url(cls, value: str | None, info: ValidationInfo) -> str | None:
        if value is not None:
            try:
                parsed = urlsplit(value)
                if parsed.scheme not in {"http", "https"} or not parsed.hostname:
                    raise ValueError
            except ValueError:
                raise ValueError(
                    f"{info.field_name} must be an absolute HTTP(S) URL with a host"
                ) from None
        return value

    @field_validator("auth_contacts_batch_size", mode="before")
    @classmethod
    def integer_contact_batch(cls, value: Any) -> Any:
        if isinstance(value, (bool, float)):
            raise ValueError("auth_contacts_batch_size must be an integer")
        return value

    @field_validator(
        "auth_contacts_timeout_seconds",
        "auth_contacts_retry_seconds",
        "auth_introspection_timeout_seconds",
        mode="before",
    )
    @classmethod
    def numeric_auth_interval(cls, value: Any) -> Any:
        if isinstance(value, bool):
            raise ValueError("Auth intervals must be positive numbers")
        return value

    @field_validator("likes_min_delta", mode="before")
    @classmethod
    def integer_threshold(cls, value: Any) -> Any:
        if isinstance(value, (bool, float)):
            raise ValueError("likes_min_delta must be an integer")
        return value

    @field_validator("database_url")
    @classmethod
    def async_postgresql(cls, value: str) -> str:
        if not value.startswith("postgresql+asyncpg://"):
            raise ValueError("database_url must use postgresql+asyncpg://")
        return value

    @field_validator("fallback_tz")
    @classmethod
    def timezone_exists(cls, value: str | None) -> str | None:
        if value is not None:
            try:
                ZoneInfo(value)
            except ZoneInfoNotFoundError as exc:
                raise ValueError("fallback_tz must be an IANA timezone") from exc
        return value

    @model_validator(mode="after")
    def required_for_process(self) -> Self:
        required = {
            "api": (
                "jwt_secret",
                "jwt_algorithm",
                "auth_introspection_url",
                "auth_introspection_api_key",
                "auth_introspection_timeout_seconds",
            ),
            "campaign-api": ("campaign_api_key",),
            "consumer": (
                "kafka_bootstrap_servers",
                "initial_likes",
                "initial_new_films",
                "initial_marketing",
            ),
            "scheduler": (
                "likes_min_delta",
                "likes_interval_seconds",
                "films_interval_seconds",
                "likes_bootstrap_load_id",
            ),
            "publisher": ("rabbitmq_url",),
            "worker": (
                "rabbitmq_url",
                "auth_contacts_url",
                "auth_contacts_api_key",
                "auth_contacts_timeout_seconds",
                "auth_contacts_retry_seconds",
                "auth_contacts_batch_size",
                "smtp_host",
                "smtp_port",
                "smtp_from",
                "smtp_starttls",
                "fallback_locale",
                "fallback_tz",
            ),
            "bootstrap": (),
        }[self.process]
        missing = [name for name in required if getattr(self, name) is None]
        if missing:
            raise ValueError(f"{self.process} requires: {', '.join(missing)}")
        if (self.smtp_username is None) != (self.smtp_password is None):
            raise ValueError("smtp_username and smtp_password must be supplied together")
        if (
            self.campaign_api_key is not None
            and not self.campaign_api_key.get_secret_value().strip()
        ):
            raise ValueError("campaign_api_key must not be empty")
        for secret in (
            self.jwt_secret,
            self.smtp_password,
            self.auth_contacts_api_key,
            self.auth_introspection_api_key,
        ):
            if secret is not None and not secret.get_secret_value().strip():
                raise ValueError("secrets must not be empty")
        return self


class WorkerSettings(Settings):
    """Validate worker environment and explicit CLI overrides before opening clients."""

    # Pydantic specializes the required process discriminator in this input model.
    process: Literal["worker"]  # pyright: ignore[reportIncompatibleVariableOverride]
    send_class: Literal["transactional", "service", "bulk"]
    max_concurrency: int = Field(ge=1)
    retry_seconds: float = Field(gt=0, allow_inf_nan=False)
    max_attempts: int = Field(ge=1)
    lease_seconds: float = Field(default=120, gt=0, allow_inf_nan=False)
    smtp_timeout: float = Field(default=30, gt=0, allow_inf_nan=False)
    shutdown_timeout: float = Field(default=45, gt=0, allow_inf_nan=False)
    poll_seconds: float = Field(default=1, gt=0, allow_inf_nan=False)
    prefetch: int = Field(default=10, ge=1)
    namespace: str = ""

    @field_validator(
        "retry_seconds",
        "lease_seconds",
        "smtp_timeout",
        "shutdown_timeout",
        "poll_seconds",
        mode="before",
    )
    @classmethod
    def numeric_worker_interval(cls, value: Any) -> Any:
        if isinstance(value, bool):
            raise ValueError("worker intervals must be positive numbers")
        return value

    @field_validator("max_concurrency", "max_attempts", "prefetch", mode="before")
    @classmethod
    def integer_worker_count(cls, value: Any) -> Any:
        if isinstance(value, (bool, float)):
            raise ValueError("worker counts must be integers")
        return value

    @model_validator(mode="after")
    def lease_covers_smtp(self) -> Self:
        if self.lease_seconds <= self.smtp_timeout:
            raise ValueError("lease must exceed the entire SMTP transaction timeout")
        return self
