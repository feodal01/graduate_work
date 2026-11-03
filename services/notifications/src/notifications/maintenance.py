"""Retire invalid-timezone work and erase expired message content."""

import argparse
import asyncio
from datetime import timedelta
from uuid import UUID

from pydantic import Field
from sqlalchemy import delete, func, null, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from notifications.db.models import (
    CampaignBatch,
    CampaignRecipient,
    Delivery,
    DeliveryAttempt,
    DeliveryItem,
    Outbox,
    Profile,
    ScheduleItem,
)
from notifications.db.session import Database
from notifications.db.settings import DatabaseSettings
from notifications.delivery.ownership import delivery_ownership
from notifications.delivery.policy import valid_timezone

TERMINAL = ("sent", "cancelled", "skipped", "failed")


class MaintenanceSettings(DatabaseSettings):
    maintenance_batch_size: int = Field(default=100, gt=0)
    maintenance_poll_seconds: float = Field(gt=0, allow_inf_nan=False)
    delivery_payload_retention_seconds: float = Field(gt=0, allow_inf_nan=False)


class Maintenance:
    def __init__(self, database: Database, *, batch_size: int, retention_seconds: float):
        self.database = database
        self.batch_size = batch_size
        self.retention = timedelta(seconds=retention_seconds)

    async def retire(self, did: UUID) -> bool:
        async with delivery_ownership(self.database, did) as conn:
            if conn is None:
                return False
            async with AsyncSession(bind=conn) as session, session.begin():
                delivery = await session.get(Delivery, did, with_for_update=True)
                if delivery is None or delivery.status in TERMINAL:
                    return False
                now = await session.scalar(select(func.clock_timestamp()))
                assert now is not None
                if delivery.lease_until is not None and delivery.lease_until > now:
                    return False
                profile = await session.get(Profile, delivery.user_id, with_for_update=True)
                if profile is None or valid_timezone(profile.tz):
                    return False
                await session.execute(
                    update(DeliveryAttempt)
                    .where(DeliveryAttempt.delivery_id == did, DeliveryAttempt.outcome == "started")
                    .values(outcome="uncertain", duplicate_risk=True, finished_at=now)
                )
                delivery.status = "skipped"
                delivery.terminal_reason = "invalid_timezone"
                delivery.terminal_at = now
                delivery.lease_token = delivery.lease_until = None
                await session.execute(
                    update(DeliveryItem).where(DeliveryItem.delivery_id == did).values(active=False)
                )
                if delivery.run_id is not None:
                    await session.execute(
                        update(ScheduleItem)
                        .where(
                            ScheduleItem.run_id == delivery.run_id,
                            ScheduleItem.user_id == delivery.user_id,
                        )
                        .values(active=False)
                    )
                await session.execute(
                    delete(Outbox).where(Outbox.delivery_id == did, Outbox.published_at.is_(None))
                )
                return True

    async def run_once(self) -> tuple[int, int]:
        """Return retired deliveries and erased messages, including intake skips."""
        retired = cleaned = 0
        cursor = None
        while True:
            async with self.database.transaction() as session:
                query = (
                    select(Delivery.delivery_id, Profile.tz)
                    .join(Profile, Profile.user_id == Delivery.user_id)
                    .where(
                        Delivery.status.not_in(TERMINAL),
                        Delivery.send_class.in_(("service", "bulk")),
                        or_(
                            Delivery.lease_until.is_(None),
                            Delivery.lease_until <= func.clock_timestamp(),
                        ),
                    )
                    .order_by(Delivery.delivery_id)
                    .limit(self.batch_size)
                )
                if cursor is not None:
                    query = query.where(Delivery.delivery_id > cursor)
                rows = (await session.execute(query)).all()
            if not rows:
                break
            cursor = rows[-1].delivery_id
            for did, tz in rows:
                if not valid_timezone(tz):
                    retired += await self.retire(did)
        while True:
            async with self.database.transaction() as session:
                expired = (
                    await session.scalars(
                        select(Delivery)
                        .outerjoin(
                            CampaignRecipient,
                            CampaignRecipient.delivery_id == Delivery.delivery_id,
                        )
                        .where(
                            Delivery.status.in_(TERMINAL),
                            or_(
                                Delivery.payload.is_not(None),
                                CampaignRecipient.subject.is_not(None),
                            ),
                            Delivery.terminal_at + self.retention < func.clock_timestamp(),
                        )
                        .order_by(Delivery.delivery_id)
                        .limit(self.batch_size)
                        .with_for_update(of=Delivery, skip_locked=True)
                    )
                ).all()
                for delivery in expired:
                    delivery.payload = null()
                if expired:
                    await session.execute(
                        update(CampaignRecipient)
                        .where(CampaignRecipient.delivery_id.in_([d.delivery_id for d in expired]))
                        .values(subject=None, text=None)
                    )
                cleaned += len(expired)
            if not expired:
                break
        while True:
            async with self.database.transaction() as session:
                skipped = (
                    await session.scalars(
                        select(CampaignRecipient)
                        .join(CampaignBatch)
                        .where(
                            CampaignRecipient.delivery_id.is_(None),
                            CampaignRecipient.subject.is_not(None),
                            CampaignBatch.accepted_at + self.retention < func.clock_timestamp(),
                        )
                        .order_by(CampaignRecipient.campaign_id, CampaignRecipient.user_id)
                        .limit(self.batch_size)
                        .with_for_update(of=CampaignRecipient, skip_locked=True)
                    )
                ).all()
                for recipient in skipped:
                    recipient.subject = recipient.text = None
                cleaned += len(skipped)
            if not skipped:
                break
        return retired, cleaned


async def run(*, once: bool = False) -> None:
    settings = MaintenanceSettings()  # type: ignore[call-arg]
    database = Database(settings.database_url.get_secret_value())
    maintenance = Maintenance(
        database,
        batch_size=settings.maintenance_batch_size,
        retention_seconds=settings.delivery_payload_retention_seconds,
    )
    try:
        while True:
            await maintenance.run_once()
            if once:
                return
            await asyncio.sleep(settings.maintenance_poll_seconds)
    finally:
        await database.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true")
    asyncio.run(run(once=parser.parse_args().once))


if __name__ == "__main__":
    main()
