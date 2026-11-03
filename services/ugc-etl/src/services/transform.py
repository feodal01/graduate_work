import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal, Optional, Self, Union
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    computed_field,
    field_validator,
    model_validator,
)

from core.config import settings

MAX_PAGE_LENGTH = 2048
MAX_ELEMENT_TYPE_LENGTH = 64
MAX_ELEMENT_ID_LENGTH = 256
MAX_QUALITY_LENGTH = 32
MAX_FILTER_KEY_LENGTH = 64
MAX_FILTER_VALUE_LENGTH = 256
MAX_FILTER_FIELDS = 20
MAX_FILTER_VALUES = 20
MAX_REQUEST_ID_LENGTH = 256

PagePath = Annotated[str, Field(min_length=1, max_length=MAX_PAGE_LENGTH)]
ElementType = Annotated[str, Field(min_length=1, max_length=MAX_ELEMENT_TYPE_LENGTH)]
ElementId = Annotated[str, Field(min_length=1, max_length=MAX_ELEMENT_ID_LENGTH)]
QualityLabel = Annotated[str, Field(min_length=1, max_length=MAX_QUALITY_LENGTH)]
FilterKey = Annotated[str, Field(min_length=1, max_length=MAX_FILTER_KEY_LENGTH)]
FilterValue = Annotated[str, Field(min_length=1, max_length=MAX_FILTER_VALUE_LENGTH)]
FilterValueList = Annotated[
    list[FilterValue],
    Field(min_length=1, max_length=MAX_FILTER_VALUES),
]


@dataclass(frozen=True)
class EventRow:
    event_id: UUID
    user_id: UUID
    event_type: str
    occurred_at: datetime
    received_at: datetime
    ingested_at: datetime
    request_id: Optional[str]
    film_id: Optional[UUID]
    page: Optional[str]
    element_type: Optional[str]
    element_id: Optional[str]
    position_seconds: Optional[float]
    duration_seconds: Optional[float]
    completion_ratio: Optional[float]
    from_quality: Optional[str]
    to_quality: Optional[str]
    filters: str
    payload: str

    def as_insert_tuple(
        self,
    ) -> tuple[
        UUID,
        UUID,
        str,
        datetime,
        datetime,
        datetime,
        Optional[str],
        Optional[UUID],
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[float],
        Optional[float],
        Optional[float],
        Optional[str],
        Optional[str],
        str,
        str,
    ]:
        return (
            self.event_id,
            self.user_id,
            self.event_type,
            self.occurred_at,
            self.received_at,
            self.ingested_at,
            self.request_id,
            self.film_id,
            self.page,
            self.element_type,
            self.element_id,
            self.position_seconds,
            self.duration_seconds,
            self.completion_ratio,
            self.from_quality,
            self.to_quality,
            self.filters,
            self.payload,
        )


class ClickPayload(BaseModel):
    element_type: ElementType
    element_id: ElementId
    page: PagePath


class PageViewPayload(BaseModel):
    page: PagePath
    duration_seconds: float = Field(ge=0)


class QualityChangedPayload(BaseModel):
    film_id: UUID
    from_quality: QualityLabel
    to_quality: QualityLabel


class WatchCompletedPayload(BaseModel):
    film_id: UUID


class SearchFilterUsedPayload(BaseModel):
    filters: dict[FilterKey, Union[FilterValue, FilterValueList]] = Field(
        min_length=1,
        max_length=MAX_FILTER_FIELDS,
    )


class WatchProgressPayload(BaseModel):
    film_id: UUID
    position_seconds: float = Field(ge=0)
    duration_seconds: float = Field(gt=0)

    @model_validator(mode='after')
    def position_within_duration(self) -> Self:
        if self.position_seconds > self.duration_seconds:
            raise ValueError('position_seconds must be <= duration_seconds')
        return self


class EnvelopeBase(BaseModel):
    model_config = ConfigDict(extra='ignore')
    event_id: UUID
    user_id: UUID
    occurred_at: datetime
    received_at: datetime
    request_id: Optional[Annotated[str, Field(max_length=MAX_REQUEST_ID_LENGTH)]]

    @field_validator('occurred_at', 'received_at')
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError('timestamp must include timezone')
        return value

    @model_validator(mode='after')
    def occurred_at_within_received_window(self) -> Self:
        max_age = timedelta(seconds=settings.events_max_age_seconds)
        max_future_skew = timedelta(seconds=settings.events_max_future_skew_seconds)
        delta = self.occurred_at - self.received_at
        if not -max_age <= delta <= max_future_skew:
            raise ValueError('occurred_at is outside the allowed window relative to received_at')
        return self

    def to_event_row(self, raw_json: str) -> EventRow:
        raise NotImplementedError

    def _row(
        self,
        raw_json: str,
        *,
        event_type: str,
        film_id: Optional[UUID] = None,
        page: Optional[str] = None,
        element_type: Optional[str] = None,
        element_id: Optional[str] = None,
        position_seconds: Optional[float] = None,
        duration_seconds: Optional[float] = None,
        completion_ratio: Optional[float] = None,
        from_quality: Optional[str] = None,
        to_quality: Optional[str] = None,
        filters: str = '',
    ) -> EventRow:
        return EventRow(
            event_id=self.event_id,
            user_id=self.user_id,
            event_type=event_type,
            occurred_at=self.occurred_at,
            received_at=self.received_at,
            ingested_at=datetime.now(UTC),
            request_id=self.request_id,
            film_id=film_id,
            page=page,
            element_type=element_type,
            element_id=element_id,
            position_seconds=position_seconds,
            duration_seconds=duration_seconds,
            completion_ratio=completion_ratio,
            from_quality=from_quality,
            to_quality=to_quality,
            filters=filters,
            payload=raw_json,
        )


class ClickEnvelope(EnvelopeBase):
    event_type: Literal['click']
    payload: ClickPayload

    def to_event_row(self, raw_json: str) -> EventRow:
        return self._row(
            raw_json,
            event_type=self.event_type,
            page=self.payload.page,
            element_type=self.payload.element_type,
            element_id=self.payload.element_id,
        )


class PageViewEnvelope(EnvelopeBase):
    event_type: Literal['page_view']
    payload: PageViewPayload

    def to_event_row(self, raw_json: str) -> EventRow:
        return self._row(
            raw_json,
            event_type=self.event_type,
            page=self.payload.page,
            duration_seconds=self.payload.duration_seconds,
        )


class QualityChangedEnvelope(EnvelopeBase):
    event_type: Literal['quality_changed']
    payload: QualityChangedPayload

    def to_event_row(self, raw_json: str) -> EventRow:
        return self._row(
            raw_json,
            event_type=self.event_type,
            film_id=self.payload.film_id,
            from_quality=self.payload.from_quality,
            to_quality=self.payload.to_quality,
        )


class WatchCompletedEnvelope(EnvelopeBase):
    event_type: Literal['watch_completed']
    payload: WatchCompletedPayload

    def to_event_row(self, raw_json: str) -> EventRow:
        return self._row(
            raw_json,
            event_type=self.event_type,
            film_id=self.payload.film_id,
            completion_ratio=1.0,
        )


class SearchFilterUsedEnvelope(EnvelopeBase):
    event_type: Literal['search_filter_used']
    payload: SearchFilterUsedPayload

    def to_event_row(self, raw_json: str) -> EventRow:
        return self._row(
            raw_json,
            event_type=self.event_type,
            filters=json.dumps(self.payload.filters, ensure_ascii=False),
        )


class WatchProgressEnvelope(EnvelopeBase):
    event_type: Literal['watch_progress']
    payload: WatchProgressPayload

    @computed_field  # type: ignore[prop-decorator]
    @property
    def completion_ratio(self) -> float:
        return self.payload.position_seconds / self.payload.duration_seconds

    def to_event_row(self, raw_json: str) -> EventRow:
        return self._row(
            raw_json,
            event_type=self.event_type,
            film_id=self.payload.film_id,
            position_seconds=self.payload.position_seconds,
            duration_seconds=self.payload.duration_seconds,
            completion_ratio=self.completion_ratio,
        )


KafkaUserEvent = Annotated[
    Union[
        ClickEnvelope,
        PageViewEnvelope,
        QualityChangedEnvelope,
        WatchCompletedEnvelope,
        SearchFilterUsedEnvelope,
        WatchProgressEnvelope,
    ],
    Field(discriminator='event_type'),
]

kafka_event_adapter: TypeAdapter[KafkaUserEvent] = TypeAdapter(KafkaUserEvent)


def parse_envelope(value: bytes) -> EventRow:
    text = value.decode('utf-8')
    return kafka_event_adapter.validate_json(text).to_event_row(text)
