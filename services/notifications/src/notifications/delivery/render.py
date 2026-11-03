"""Render once from local projections; only the saved snapshot is retried."""

from importlib.resources import files
from typing import Any

from jinja2 import Environment, PackageLoader, StrictUndefined, select_autoescape
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from notifications.clients.auth_contacts import ActiveContact
from notifications.config import Settings
from notifications.db.models import (
    CampaignRecipient,
    Delivery,
    DeliveryItem,
    FilmCard,
    Profile,
    ScheduleRun,
)
from notifications.template_registry import get_template_spec

ENVIRONMENT = Environment(
    loader=PackageLoader("notifications", "templates"),
    autoescape=select_autoescape(["html"]),
    undefined=StrictUndefined,
)


async def render(
    session: AsyncSession,
    delivery: Delivery,
    profile: Profile,
    settings: Settings,
    contact: ActiveContact,
) -> dict[str, Any]:
    body_text = None
    items = []
    if delivery.kind == "external":
        recipient = (
            await session.execute(
                select(CampaignRecipient).where(
                    CampaignRecipient.delivery_id == delivery.delivery_id
                )
            )
        ).scalar_one()
        if recipient.subject is None or recipient.text is None:
            raise ValueError("External notification content has been erased")
        spec = get_template_spec(recipient.template_id)
        subject, body_text = recipient.subject, recipient.text
    else:
        spec = get_template_spec(delivery.kind)
        if spec.subject_file is None:
            raise ValueError("Internal notification template requires a subject file")
        subject = (
            files("notifications")
            .joinpath("templates", spec.subject_file)
            .read_text(encoding="utf-8")
            .rstrip("\r\n")
        )
    if delivery.kind == "likes":
        items = list(
            await session.scalars(
                select(DeliveryItem)
                .where(DeliveryItem.delivery_id == delivery.delivery_id)
                .order_by(DeliveryItem.item_id)
            )
        )
    reviews = [
        {"review_id": item.review_id, "likes_count": item.likes_count}
        for item in items
        if item.review_id is not None
    ]
    films: list[dict[str, object]] = []
    if delivery.kind == "new_films":
        # Freeze the cap per issue; legacy unprepared jobs use the current policy.
        frozen_limit = (
            select(ScheduleRun.films_per_digest)
            .where(ScheduleRun.run_id == delivery.run_id)
            .scalar_subquery()
        )
        cards = await session.scalars(
            select(FilmCard)
            .join(DeliveryItem, DeliveryItem.film_id == FilmCard.film_id)
            .where(DeliveryItem.delivery_id == delivery.delivery_id)
            .order_by(FilmCard.catalog_published_at.desc(), FilmCard.film_id)
            .limit(func.coalesce(frozen_limit, settings.films_per_digest))
        )
        films = [
            {
                "film_id": str(card.film_id),
                "film_version": card.film_version,
                "title": card.title,
                "description": card.description,
                "year": card.creation_date.year if card.creation_date else None,
                "rating": card.imdb_rating,
                "genres": card.genres,
            }
            for card in cards
        ]
    data = {
        "first_name": contact.first_name,
        "last_name": contact.last_name,
        "locale": contact.locale or settings.fallback_locale,
        "tz": profile.tz or settings.fallback_tz,
        "reviews": reviews,
        "films": films,
    }
    if body_text is not None:
        data["subject"] = subject
        data["text"] = body_text
    return {
        "to": contact.email,
        "from": settings.smtp_from,
        "subject": subject,
        "html": ENVIRONMENT.get_template(spec.html_file).render(**data),
        **data,
    }
