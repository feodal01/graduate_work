"""The registered personal_offer input contract and canonical replay identity."""

import hashlib
import json
import re
import unicodedata
from datetime import UTC, datetime
from typing import Annotated, Any, Literal, cast
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    ValidationInfo,
    field_validator,
)
from pydantic_core import PydanticCustomError

from notifications.config import BatchSettings
from notifications.template_registry import TemplateSpec, get_template_spec

MAX_BODY_BYTES = 1048576


class CampaignItem(BaseModel):
    """All externally registered templates currently use this plain-text contract."""

    model_config = ConfigDict(extra="forbid")

    user_id: UUID
    template_id: Annotated[str, Field(strict=True, min_length=1, max_length=64)]
    subject: Annotated[str, Field(strict=True, min_length=1, max_length=200)]
    text: Annotated[str, Field(strict=True, min_length=1, max_length=20000)]
    type: Literal["email"]
    available_at: AwareDatetime | None = None

    @field_validator("subject", "text")
    @classmethod
    def nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Must contain non-whitespace characters")
        if "\x00" in value or any(unicodedata.category(char) == "Cs" for char in value):
            raise ValueError("Text contains unsupported characters")
        return value

    @field_validator("subject")
    @classmethod
    def single_line_subject(cls, value: str) -> str:
        if any(unicodedata.category(char) in {"Cc", "Zl", "Zp"} for char in value):
            raise ValueError("Subject must be a single line without control characters")
        return value

    @field_validator("available_at", mode="before")
    @classmethod
    def iso_datetime_only(cls, value: Any) -> Any:
        if value is not None and not isinstance(value, datetime):
            if not isinstance(value, str) or not re.match(r"^\d{4}-\d{2}-\d{2}[Tt ]", value):
                raise ValueError("Timestamp must be an ISO datetime with timezone")
        return value

    @field_validator("available_at")
    @classmethod
    def utc_timestamp(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        try:
            return value.astimezone(UTC)
        except (OverflowError, ValueError):
            raise ValueError("Timestamp is outside the supported UTC range") from None


class CampaignBatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    batch_id: UUID
    created_by_user_id: UUID | None = Field(
        default=None,
        description="Required UUID for new batches. Omit only for exact historical replay.",
    )
    items: Annotated[list[CampaignItem], Field(min_length=1)]

    @field_validator("items", mode="before")
    @classmethod
    def configured_batch_size(cls, value: object, info: ValidationInfo) -> object:
        settings = info.context if isinstance(info.context, BatchSettings) else BatchSettings()
        if isinstance(value, list):
            items = cast(list[object], value)
            if len(items) > settings.campaign_batch_size:
                raise PydanticCustomError(
                    "too_long",
                    "Batch must contain at most {max_length} items",
                    {"max_length": settings.campaign_batch_size},
                )
            return items
        return value

    @field_validator("created_by_user_id", mode="before")
    @classmethod
    def explicit_author_is_uuid(cls, value: Any) -> Any:
        if value is None:
            raise ValueError("Author must be a UUID")
        return value

    @field_validator("items")
    @classmethod
    def unique_recipients(cls, items: list[CampaignItem]) -> list[CampaignItem]:
        seen: set[UUID] = set()
        for index, item in enumerate(items):
            if item.user_id in seen:
                raise ValidationError.from_exception_data(
                    cls.__name__,
                    [
                        {
                            "type": "value_error",
                            "loc": (index, "user_id"),
                            "input": None,
                            "ctx": {"error": ValueError("Duplicate recipient")},
                        }
                    ],
                )
            seen.add(item.user_id)
        return items


# An external registry entry alone does not declare its accepted input schema.
_TEMPLATE_INPUT_SCHEMAS = {"personal_offer": CampaignItem}


def item_validation_error(index: int, field: str, message: str) -> ValidationError:
    return ValidationError.from_exception_data(
        CampaignBatchRequest.__name__,
        [
            {
                "type": "value_error",
                "loc": ("items", index, field),
                "input": None,
                "ctx": {"error": ValueError(message)},
            }
        ],
    )


def new_batch_template_specs(body: CampaignBatchRequest) -> list[TemplateSpec]:
    """Admit every new item before persistence; saved replay bypasses this check."""
    specs: list[TemplateSpec] = []
    for index, item in enumerate(body.items):
        try:
            spec = get_template_spec(item.template_id, external_only=True)
        except ValueError as exc:
            raise item_validation_error(index, "template_id", str(exc)) from None
        if _TEMPLATE_INPUT_SCHEMAS.get(item.template_id) is not type(item):
            raise item_validation_error(index, "template_id", "No registered input schema")
        if spec.channel != item.type:
            raise item_validation_error(index, "type", "Template channel does not match")
        specs.append(spec)
    return specs


def canonical_request_hash(body: CampaignBatchRequest) -> str:
    """Hash validated source fields before substituting any server-generated time."""
    encoded = json.dumps(
        body.model_dump(
            mode="json",
            exclude={"created_by_user_id"} if body.created_by_user_id is None else set(),
        ),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


class CloseCampaignRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_batches: Annotated[int, Field(strict=True, gt=0, le=2147483647)]


class BatchAccepted(BaseModel):
    campaign_id: UUID
    batch_id: UUID
    status: Literal["accepted"] = "accepted"
    item_count: int


class CampaignClosed(BaseModel):
    campaign_id: UUID
    intake_status: Literal["closed"] = "closed"


class CampaignStatus(BaseModel):
    campaign_id: UUID
    created_by_user_id: UUID | None
    intake_status: Literal["open", "closed"]
    status: Literal["in_progress", "completed", "partially_completed", "failed", "skipped"]
    accepted_batches: int
    total: int
    sent: int
    failed: int
    skipped: int
    pending: int
    skipped_reasons: dict[str, int]
    failed_reasons: dict[str, int]
