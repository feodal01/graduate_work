"""Explicit shared SMTP quotas; no provider limits are guessed."""

import hashlib
import json
from fractions import Fraction
from typing import Any, Self
from urllib.parse import urlsplit

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from notifications.delivery.routing import SEND_CLASSES

BUCKETS = ("total", *SEND_CLASSES)


class LimiterSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="NOTIFICATIONS_LIMITER_", extra="forbid", frozen=True, hide_input_in_errors=True
    )

    redis_url: SecretStr
    account_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.-]+$")
    total_rate: float = Field(gt=0, allow_inf_nan=False)
    total_capacity: int = Field(ge=1, le=2**53 - 1)
    transactional_rate: float = Field(gt=0, allow_inf_nan=False)
    transactional_capacity: int = Field(ge=1, le=2**53 - 1)
    service_rate: float = Field(gt=0, allow_inf_nan=False)
    service_capacity: int = Field(ge=1, le=2**53 - 1)
    bulk_rate: float = Field(gt=0, allow_inf_nan=False)
    bulk_capacity: int = Field(ge=1, le=2**53 - 1)
    unavailable_retry_seconds: float = Field(gt=0, allow_inf_nan=False)
    redis_timeout_seconds: float = Field(gt=0, allow_inf_nan=False)

    @field_validator("*", mode="before")
    @classmethod
    def reject_bool(cls, value: Any) -> Any:
        if isinstance(value, bool):
            raise ValueError("boolean settings are not numeric limits")
        return value

    @field_validator(
        "total_capacity",
        "transactional_capacity",
        "service_capacity",
        "bulk_capacity",
        mode="before",
    )
    @classmethod
    def integer_capacity(cls, value: Any) -> Any:
        if isinstance(value, float):
            raise ValueError("capacity must be an integer")
        return value

    @field_validator("redis_url")
    @classmethod
    def redis_endpoint(cls, value: SecretStr) -> SecretStr:
        parsed = urlsplit(value.get_secret_value())
        if parsed.scheme not in ("redis", "rediss") or not parsed.hostname:
            raise ValueError("redis_url must be a redis:// or rediss:// endpoint")
        # URL query options override redis-py keyword settings, including timeout/retry.
        if parsed.query or parsed.fragment:
            raise ValueError("redis_url must not contain query options or a fragment")
        return value

    @model_validator(mode="after")
    def reserved_shares(self) -> Self:
        rates = [getattr(self, f"{name}_rate") for name in SEND_CLASSES]
        if sum(Fraction(str(rate)) for rate in rates) > Fraction(str(self.total_rate)):
            raise ValueError("sum of class rates must not exceed total_rate")
        if sum(getattr(self, f"{name}_capacity") for name in SEND_CLASSES) > self.total_capacity:
            raise ValueError("sum of class capacities must not exceed total_capacity")
        return self

    @property
    def key(self) -> str:
        return f"notifications:smtp:{{{self.account_id}}}:limiter"

    @property
    def fingerprint(self) -> str:
        policy = {
            f"{name}_{part}": getattr(self, f"{name}_{part}")
            for name in BUCKETS
            for part in ("rate", "capacity")
        }
        return hashlib.sha256(json.dumps(policy, sort_keys=True).encode()).hexdigest()
