"""Internal deployment-boundary HTTP routes; no user JWT contract applies here."""

import secrets
from collections.abc import AsyncIterator
from typing import Annotated, cast
from uuid import UUID

from fastapi import APIRouter, Depends, Request
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from notifications.campaigns.schemas import (
    MAX_BODY_BYTES,
    BatchAccepted,
    CampaignBatchRequest,
    CampaignClosed,
    CampaignStatus,
    CloseCampaignRequest,
)
from notifications.campaigns.service import (
    CampaignError,
    accept_batch,
    campaign_status,
    close_campaign,
)
from notifications.db.session import Database


async def require_service_key(request: Request) -> None:
    settings = request.app.state.settings
    supplied = request.headers.get("X-API-Key")
    configured = settings.campaign_api_key.get_secret_value()
    if supplied is None or not secrets.compare_digest(supplied.encode(), configured.encode()):
        raise CampaignError("unauthorized", 401)


router = APIRouter(
    prefix="/api/v1/notifications/campaigns",
    tags=["campaigns"],
    dependencies=[Depends(require_service_key)],
)


async def bounded_body[Body: BaseModel](request: Request, schema: type[Body]) -> Body:
    data = bytearray()
    async for chunk in request.stream():
        if len(data) + len(chunk) > MAX_BODY_BYTES:
            raise CampaignError("request_too_large", 413)
        data.extend(chunk)
    try:
        return schema.model_validate_json(data, context=request.app.state.settings)
    except ValidationError as exc:
        raise RequestValidationError(
            exc.errors(include_input=False, include_context=False)
        ) from None


async def batch_body(request: Request) -> CampaignBatchRequest:
    return await bounded_body(request, CampaignBatchRequest)


async def close_body(request: Request) -> CloseCampaignRequest:
    return await bounded_body(request, CloseCampaignRequest)


async def request_session(request: Request) -> AsyncIterator[AsyncSession]:
    database = cast(Database, request.app.state.database)
    async with database.transaction() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(request_session, scope="function")]


@router.post("/{campaign_id}/batches", status_code=202)
async def post_batch(
    campaign_id: UUID,
    body: Annotated[CampaignBatchRequest, Depends(batch_body)],
    request: Request,
    session: SessionDep,
) -> BatchAccepted:
    try:
        return await accept_batch(
            session,
            campaign_id,
            body,
            timing=request.app.state.settings,
        )
    except ValidationError as exc:
        raise RequestValidationError(
            exc.errors(include_input=False, include_context=False)
        ) from None


@router.post("/{campaign_id}/close")
async def post_close(
    campaign_id: UUID,
    body: Annotated[CloseCampaignRequest, Depends(close_body)],
    session: SessionDep,
) -> CampaignClosed:
    return await close_campaign(session, campaign_id, body.expected_batches)


@router.get("/{campaign_id}")
async def get_campaign(campaign_id: UUID, session: SessionDep) -> CampaignStatus:
    return await campaign_status(session, campaign_id)
