from typing import Annotated, cast

from fastapi import Depends, Request

from services.contracts import EventConsumer, Warehouse


async def get_event_consumer(request: Request) -> EventConsumer:
    return cast(EventConsumer, request.app.state.consumer)


async def get_warehouse(request: Request) -> Warehouse:
    return cast(Warehouse, request.app.state.warehouse)


EventConsumerDep = Annotated[EventConsumer, Depends(get_event_consumer)]
WarehouseDep = Annotated[Warehouse, Depends(get_warehouse)]
