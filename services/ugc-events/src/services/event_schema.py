from datetime import datetime
from typing import Annotated, Literal, Self, Union
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator

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


class EventBase(BaseModel):
    event_id: UUID
    occurred_at: datetime

    @field_validator('occurred_at')
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError('occurred_at must include timezone')
        return value


class ClickEvent(EventBase):
    event_type: Literal['click']
    payload: ClickPayload


class PageViewEvent(EventBase):
    event_type: Literal['page_view']
    payload: PageViewPayload


class QualityChangedEvent(EventBase):
    event_type: Literal['quality_changed']
    payload: QualityChangedPayload


class WatchCompletedEvent(EventBase):
    event_type: Literal['watch_completed']
    payload: WatchCompletedPayload


class SearchFilterUsedEvent(EventBase):
    event_type: Literal['search_filter_used']
    payload: SearchFilterUsedPayload


class WatchProgressEvent(EventBase):
    event_type: Literal['watch_progress']
    payload: WatchProgressPayload


UserEvent = Annotated[
    Union[
        ClickEvent,
        PageViewEvent,
        QualityChangedEvent,
        WatchCompletedEvent,
        SearchFilterUsedEvent,
        WatchProgressEvent,
    ],
    Field(discriminator='event_type'),
]


class EventBatch(BaseModel):
    events: Annotated[
        list[UserEvent],
        Field(min_length=1, max_length=settings.events_max_batch_size),
    ]


class AcceptedResponse(BaseModel):
    accepted: int = Field(ge=1)
