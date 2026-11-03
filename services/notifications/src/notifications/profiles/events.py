"""Apply Auth state and welcome jobs inside the caller's transaction."""

from dataclasses import dataclass

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from notifications.config import Settings
from notifications.contracts import AuthEvent
from notifications.db.models import Delivery, Outbox, Profile
from notifications.likes.events import activate_pending, lock_user


@dataclass(frozen=True)
class AuthProfileHandler:
    """Bind explicit initial preferences once when constructing the consumer."""

    settings: Settings

    def __post_init__(self) -> None:
        if any(
            value is None
            for value in (
                self.settings.initial_likes,
                self.settings.initial_new_films,
                self.settings.initial_marketing,
            )
        ):
            raise ValueError("Auth profile handler requires all three explicit initial flags")

    async def __call__(self, session: AsyncSession, event: AuthEvent) -> None:
        await lock_user(session, event.user_id)
        deleted = event.event_type == "user.deleted"
        contacts = {"tz": event.tz}
        flags = {
            "likes": False if deleted else self.settings.initial_likes,
            "new_films": False if deleted else self.settings.initial_new_films,
            "marketing": False if deleted else self.settings.initial_marketing,
        }
        statement = insert(Profile).values(
            user_id=event.user_id,
            user_version=event.user_version,
            **contacts,
            **flags,
            deleted_at=func.now() if deleted else None,
        )
        # On deletion, retain contacts; on profile updates, retain user preferences.
        changes = {
            "user_version": event.user_version,
            "deleted_at": func.now() if deleted else None,
            **(flags if deleted else contacts),
        }
        applied = await session.scalar(
            statement.on_conflict_do_update(
                index_elements=[Profile.user_id],
                set_=changes,
                where=Profile.user_version < event.user_version,
            ).returning(Profile.user_id)
        )
        if applied is None:
            return
        await activate_pending(session, event.user_id, deleted=deleted)
        if deleted:
            return
        if event.event_type != "user.registered":
            return
        delivery_id = await session.scalar(
            insert(Delivery)
            .values(
                logical_key=f"welcome:{event.user_id}",
                user_id=event.user_id,
                kind="welcome",
                status="pending",
            )
            .on_conflict_do_nothing()
            .returning(Delivery.delivery_id)
        )
        if delivery_id is not None:
            await session.execute(insert(Outbox).values(delivery_id=delivery_id))


async def apply_auth(session: AsyncSession, event: AuthEvent) -> None:
    """Public two-argument handler; configuration is loaded only at invocation.

    Consumer startup should bind AuthProfileHandler(settings) to reuse its validated
    settings. Neither entry point commits or opens a separate transaction.
    """
    await AuthProfileHandler(Settings())(session, event)  # type: ignore[call-arg]
