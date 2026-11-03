"""Preferences API; Auth events and preference writes serialize on the profile row."""

from collections.abc import AsyncIterator
from typing import Annotated, cast
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from notifications.db.models import Profile
from notifications.db.session import Database
from notifications.profiles.schemas import Preferences, PreferencesPatch
from notifications.profiles.security import AccessTokenDep

router = APIRouter(prefix="/api/v1/notifications/preferences", tags=["preferences"])


async def request_session(request: Request) -> AsyncIterator[AsyncSession]:
    database = cast(Database, request.app.state.database)
    async with database.transaction() as session:
        yield session


# Commit succeeds before sending a successful HTTP response.
SessionDep = Annotated[AsyncSession, Depends(request_session, scope="function")]


async def find_profile(session: AsyncSession, user_id: UUID, *, lock: bool = False) -> Profile:
    query = select(Profile).where(Profile.user_id == user_id)
    if lock:
        query = query.with_for_update()
    profile = await session.scalar(query)
    if profile is None:
        raise HTTPException(status_code=409, detail="profile_not_ready")
    if profile.deleted_at is not None:
        raise HTTPException(status_code=410, detail="profile_deleted")
    return profile


@router.get("/me")
async def get_preferences(token: AccessTokenDep, session: SessionDep) -> Preferences:
    return Preferences.model_validate(await find_profile(session, token.user_id))


@router.patch("/me")
async def patch_preferences(
    body: PreferencesPatch,
    token: AccessTokenDep,
    session: SessionDep,
) -> Preferences:
    profile = await find_profile(session, token.user_id, lock=True)
    for name, value in body.model_dump(exclude_unset=True).items():
        setattr(profile, name, value)
    await session.flush()
    return Preferences.model_validate(profile)
