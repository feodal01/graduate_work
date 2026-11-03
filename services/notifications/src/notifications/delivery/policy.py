"""Recipient eligibility shared by intake and delivery."""

from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from notifications.db.models import Profile
from notifications.delivery.routing import SendClass


def valid_timezone(tz: str | None) -> bool:
    try:
        if not tz:
            return False
        ZoneInfo(tz)
        return True
    except (ZoneInfoNotFoundError, ValueError):
        return False


def skip_reason(
    profile: Profile | None,
    subscription: str | None,
    *,
    send_class: SendClass = "transactional",
) -> str | None:
    if subscription is not None and subscription not in {"likes", "new_films", "marketing"}:
        raise ValueError("Unknown notification subscription")
    if profile is None or profile.deleted_at is not None:
        return "profile_deleted"
    if subscription is not None and not getattr(profile, subscription):
        return "subscription_disabled"
    if send_class != "transactional" and not valid_timezone(profile.tz):
        return "invalid_timezone"
    return None
