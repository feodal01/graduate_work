from typing import Annotated, cast

from fastapi import Depends, Request

from services.event_broker import EventBroker
from services.rate_limit import EventRateLimiter


async def get_event_broker(request: Request) -> EventBroker:
    return cast(EventBroker, request.app.state.event_broker)


EventBrokerDep = Annotated[EventBroker, Depends(get_event_broker)]


async def get_event_rate_limiter(request: Request) -> EventRateLimiter:
    return cast(EventRateLimiter, request.app.state.event_rate_limiter)


EventRateLimiterDep = Annotated[EventRateLimiter, Depends(get_event_rate_limiter)]
