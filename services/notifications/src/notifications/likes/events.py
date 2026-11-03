"""Project latest likes and the independent, immutable bootstrap baseline.

Lock order: bootstrap load (when present), user advisory lock, profile row,
review row. Auth takes the same user lock before its upsert, including absent
profiles. All writes belong to the caller's transaction; no handler commits.
"""

from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from notifications.contracts import LikesEvent
from notifications.db.models import (
    BootstrapReceipt,
    LoadState,
    PendingReview,
    Profile,
    ReviewCounter,
)
from notifications.ingest.bootstrap import lock_load


async def lock_user(session: AsyncSession, user_id: UUID) -> None:
    await session.execute(
        select(
            func.pg_advisory_xact_lock(
                func.hashtextextended("notifications:likes-user:" + str(user_id), 0)
            )
        )
    )


async def _receipt(session: AsyncSession, event: LikesEvent) -> bool:
    """Return whether this is a previously unseen immutable source pair."""
    if event.load_id is None:
        return False
    await lock_load(session, event.load_id)
    saved = await session.get(BootstrapReceipt, (event.load_id, event.review_id))
    if saved is not None:
        if saved.event_id != event.event_id:
            raise ValueError("bootstrap receipt event_id conflicts with original source pair")
        return False
    state = await session.get(LoadState, event.load_id)
    if state is not None and state.completed_at is not None:
        raise ValueError("cannot add a new receipt to a completed load")
    session.add(
        BootstrapReceipt(load_id=event.load_id, entity_id=event.review_id, event_id=event.event_id)
    )
    await session.flush()
    return True


async def _mark_applied(session: AsyncSession, review_id: str) -> None:
    await session.execute(
        update(BootstrapReceipt)
        .where(BootstrapReceipt.entity_id == review_id, BootstrapReceipt.applied_at.is_(None))
        .values(applied_at=func.now())
    )


async def _counter(
    session: AsyncSession,
    review_id: str,
    user_id: UUID,
    count: int,
    version: int,
    baseline: int | None,
) -> None:
    counter = await session.scalar(
        select(ReviewCounter).where(ReviewCounter.review_id == review_id).with_for_update()
    )
    if counter is None:
        session.add(
            ReviewCounter(
                review_id=review_id,
                user_id=user_id,
                likes_count=count,
                likes_version=version,
                last_sent_likes_count=baseline or 0,
            )
        )
    else:
        if counter.user_id != user_id:
            raise ValueError("review author cannot change")
        if version > counter.likes_version:
            counter.likes_count = count
            counter.likes_version = version
        if baseline is not None and counter.last_notification_sent_at is None:
            counter.last_sent_likes_count = baseline
    await session.flush()
    await _mark_applied(session, review_id)


async def apply_likes(session: AsyncSession, event: LikesEvent) -> None:
    new_receipt = await _receipt(session, event)
    await lock_user(session, event.user_id)
    profile = await session.scalar(
        select(Profile).where(Profile.user_id == event.user_id).with_for_update()
    )
    baseline = None
    if new_receipt:
        # Receipt history replaces an extra counter field. A second load or replay
        # must never reset the original baseline, even after processed-event cleanup.
        previous = await session.scalar(
            select(BootstrapReceipt.entity_id)
            .where(
                BootstrapReceipt.entity_id == event.review_id,
                BootstrapReceipt.load_id != event.load_id,
            )
            .limit(1)
        )
        if previous is None:
            baseline = event.likes_count
    if profile is not None:
        if profile.deleted_at is not None:
            await _mark_applied(session, event.review_id)
            return
        await _counter(
            session,
            event.review_id,
            event.user_id,
            event.likes_count,
            event.likes_version,
            baseline,
        )
        return
    pending = await session.scalar(
        select(PendingReview).where(PendingReview.review_id == event.review_id).with_for_update()
    )
    if pending is None:
        session.add(
            PendingReview(
                review_id=event.review_id,
                user_id=event.user_id,
                likes_count=event.likes_count,
                likes_version=event.likes_version,
                baseline_likes_count=baseline,
                baseline_load_id=event.load_id if baseline is not None else None,
            )
        )
    else:
        if pending.user_id != event.user_id:
            raise ValueError("review author cannot change")
        if event.likes_version > pending.likes_version:
            pending.likes_count = event.likes_count
            pending.likes_version = event.likes_version
        if baseline is not None and pending.baseline_likes_count is None:
            pending.baseline_likes_count = baseline
            pending.baseline_load_id = event.load_id
    await session.flush()


async def activate_pending(session: AsyncSession, user_id: UUID, *, deleted: bool) -> None:
    """Auth already holds the user and profile locks after an accepted upsert."""
    rows = await session.scalars(
        select(PendingReview)
        .where(PendingReview.user_id == user_id)
        .order_by(PendingReview.review_id)
        .with_for_update()
    )
    for pending in rows:
        if deleted:
            await _mark_applied(session, pending.review_id)
        else:
            await _counter(
                session,
                pending.review_id,
                user_id,
                pending.likes_count,
                pending.likes_version,
                pending.baseline_likes_count,
            )
        await session.delete(pending)
    await session.flush()
