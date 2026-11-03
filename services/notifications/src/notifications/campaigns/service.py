"""Campaign-then-profile locking and atomic intake; caller owns the transaction."""

from datetime import UTC, datetime
from uuid import UUID, uuid4

from pydantic import ValidationError
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from notifications.campaigns.schemas import (
    BatchAccepted,
    CampaignBatchRequest,
    CampaignClosed,
    CampaignStatus,
    canonical_request_hash,
    item_validation_error,
    new_batch_template_specs,
)
from notifications.config import TimingSettings
from notifications.db.models import (
    Campaign,
    CampaignBatch,
    CampaignRecipient,
    Delivery,
    Outbox,
    Profile,
)
from notifications.delivery.policy import skip_reason
from notifications.scheduling.windows import next_send_at


class CampaignError(Exception):
    def __init__(
        self, error_code: str, status_code: int = 409, *, user_ids: list[UUID] | None = None
    ):
        super().__init__(error_code)
        self.error_code = error_code
        self.status_code = status_code
        self.user_ids = user_ids


async def locked_campaign(session: AsyncSession, campaign_id: UUID) -> Campaign:
    campaign = await session.scalar(
        select(Campaign).where(Campaign.campaign_id == campaign_id).with_for_update()
    )
    if campaign is None:
        raise CampaignError("campaign_not_found", 404)
    return campaign


def missing_author_error() -> ValidationError:
    return ValidationError.from_exception_data(
        CampaignBatchRequest.__name__,
        [{"type": "missing", "loc": ("created_by_user_id",), "input": None}],
    )


async def accept_batch(
    session: AsyncSession,
    campaign_id: UUID,
    body: CampaignBatchRequest,
    *,
    timing: TimingSettings,
) -> BatchAccepted:
    request_hash = canonical_request_hash(body)
    campaign: Campaign | None
    if body.created_by_user_id is not None:
        await session.execute(
            insert(Campaign)
            .values(campaign_id=campaign_id, created_by_user_id=body.created_by_user_id)
            .on_conflict_do_nothing()
        )
        campaign = await locked_campaign(session, campaign_id)
    else:
        campaign = await session.scalar(
            select(Campaign).where(Campaign.campaign_id == campaign_id).with_for_update()
        )
        if campaign is None:
            raise missing_author_error()
    existing = await session.get(CampaignBatch, (campaign_id, body.batch_id))
    if existing is not None:
        if existing.request_hash != request_hash:
            raise CampaignError("batch_content_conflict")
        return BatchAccepted(
            campaign_id=campaign_id, batch_id=existing.batch_id, item_count=existing.item_count
        )
    if campaign.created_by_user_id is None:
        raise CampaignError("campaign_author_conflict")
    if body.created_by_user_id is None:
        raise missing_author_error()
    if campaign.created_by_user_id != body.created_by_user_id:
        raise CampaignError("campaign_author_conflict")
    specs = new_batch_template_specs(body)
    if campaign.intake_status == "closed":
        raise CampaignError("campaign_closed")
    user_ids = sorted(item.user_id for item in body.items)
    duplicate = await session.scalar(
        select(CampaignRecipient.user_id)
        .where(
            CampaignRecipient.campaign_id == campaign_id, CampaignRecipient.user_id.in_(user_ids)
        )
        .limit(1)
    )
    if duplicate is not None:
        raise CampaignError("recipient_conflict")
    profiles = {
        profile.user_id: profile
        for profile in await session.scalars(
            select(Profile)
            .where(Profile.user_id.in_(user_ids))
            .order_by(Profile.user_id)
            .with_for_update()
        )
    }
    missing = [user_id for user_id in user_ids if user_id not in profiles]
    if missing:
        raise CampaignError("profile_not_ready", user_ids=missing)
    accepted_at = datetime.now(UTC)
    session.add(
        CampaignBatch(
            campaign_id=campaign_id,
            batch_id=body.batch_id,
            request_hash=request_hash,
            item_count=len(body.items),
            accepted_at=accepted_at,
        )
    )
    await session.flush()
    for index, (item, spec) in enumerate(zip(body.items, specs, strict=True)):
        profile = profiles[item.user_id]
        reason = skip_reason(profile, spec.required_subscription, send_class=spec.send_class)
        effective_at = max(item.available_at or accepted_at, accepted_at)
        delivery_id = None
        if reason is None:
            try:
                due = next_send_at(
                    spec.send_class,
                    profile.tz,
                    effective_at,
                    timing=timing,
                )
            except OverflowError:
                # A valid aware datetime can exceed the representable calendar
                # when the recipient window moves it to a later local day.
                raise item_validation_error(
                    index, "available_at", "Timestamp exceeds the scheduling range"
                ) from None
            delivery_id = uuid4()
            session.add(
                Delivery(
                    delivery_id=delivery_id,
                    logical_key=f"campaign:{campaign_id}:{item.user_id}",
                    user_id=item.user_id,
                    kind="external",
                    external_send_class=spec.send_class,
                    required_subscription=spec.required_subscription,
                    next_attempt_at=due,
                )
            )
            await session.flush()
            session.add(Outbox(delivery_id=delivery_id, available_at=due))
        session.add(
            CampaignRecipient(
                campaign_id=campaign_id,
                batch_id=body.batch_id,
                user_id=item.user_id,
                delivery_id=delivery_id,
                template_id=spec.template_id,
                channel=spec.channel,
                subject=item.subject,
                text=item.text,
                source_available_at=item.available_at,
                effective_available_at=effective_at,
                skip_reason=reason,
            )
        )
    await session.flush()
    return BatchAccepted(
        campaign_id=campaign_id, batch_id=body.batch_id, item_count=len(body.items)
    )


async def close_campaign(
    session: AsyncSession, campaign_id: UUID, expected_batches: int
) -> CampaignClosed:
    campaign = await locked_campaign(session, campaign_id)
    count = await session.scalar(
        select(func.count())
        .select_from(CampaignBatch)
        .where(CampaignBatch.campaign_id == campaign_id)
    )
    if count != expected_batches or (
        campaign.intake_status == "closed" and campaign.expected_batches != expected_batches
    ):
        raise CampaignError("batch_count_mismatch")
    if campaign.intake_status != "closed":
        campaign.intake_status = "closed"
        campaign.expected_batches = expected_batches
        campaign.closed_at = datetime.now(UTC)
        await session.flush()
    return CampaignClosed(campaign_id=campaign_id)


# All counters and reasons share one PostgreSQL statement snapshot. Batch counts
# stay independent of recipient counts, so multiple batches cannot multiply totals.
_STATUS_QUERY = text("""
WITH outcomes AS (
    SELECT CASE WHEN r.skip_reason IS NOT NULL OR d.status IN ('skipped', 'cancelled')
                THEN 'skipped'
                WHEN d.status = 'sent' THEN 'sent'
                WHEN d.status = 'failed' THEN 'failed'
                ELSE 'pending' END AS outcome,
           COALESCE(r.skip_reason, d.terminal_reason) AS reason
    FROM campaign_recipients r
    LEFT JOIN deliveries d ON d.delivery_id = r.delivery_id
    WHERE r.campaign_id = :campaign_id
), totals AS (
    SELECT count(*) AS total,
           count(*) FILTER (WHERE outcome = 'sent') AS sent,
           count(*) FILTER (WHERE outcome = 'failed') AS failed,
           count(*) FILTER (WHERE outcome = 'skipped') AS skipped,
           count(*) FILTER (WHERE outcome = 'pending') AS pending
    FROM outcomes
), reasons AS (
    SELECT outcome, reason, count(*) AS quantity FROM outcomes
    WHERE reason IS NOT NULL AND outcome IN ('skipped', 'failed')
    GROUP BY outcome, reason
)
SELECT c.campaign_id, c.created_by_user_id, c.intake_status,
       (SELECT count(*) FROM campaign_batches b WHERE b.campaign_id = c.campaign_id)
           AS accepted_batches,
       totals.*,
       COALESCE((SELECT jsonb_object_agg(reason, quantity) FROM reasons
                 WHERE outcome = 'skipped'), '{}'::jsonb) AS skipped_reasons,
       COALESCE((SELECT jsonb_object_agg(reason, quantity) FROM reasons
                 WHERE outcome = 'failed'), '{}'::jsonb) AS failed_reasons
FROM campaigns c CROSS JOIN totals WHERE c.campaign_id = :campaign_id
""")


async def campaign_status(session: AsyncSession, campaign_id: UUID) -> CampaignStatus:
    row = (
        (await session.execute(_STATUS_QUERY, {"campaign_id": campaign_id}))
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise CampaignError("campaign_not_found", 404)
    if row["intake_status"] == "open" or row["pending"] > 0:
        status = "in_progress"
    elif row["sent"] == row["total"]:
        status = "completed"
    elif row["sent"] > 0:
        status = "partially_completed"
    elif row["failed"] > 0:
        status = "failed"
    else:
        status = "skipped"
    return CampaignStatus.model_validate({**row, "status": status})
