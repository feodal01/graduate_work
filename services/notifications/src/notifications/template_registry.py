"""Registered file templates and their delivery policy."""

from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal


@dataclass(frozen=True)
class TemplateSpec:
    template_id: str
    html_file: str
    channel: Literal["email"]
    send_class: Literal["transactional", "service", "bulk"]
    required_subscription: str | None
    external: bool
    subject_file: str | None = None


_TEMPLATES = MappingProxyType(
    {
        "welcome": TemplateSpec(
            "welcome",
            "welcome.html",
            "email",
            "transactional",
            None,
            False,
            "welcome.subject.txt",
        ),
        "likes": TemplateSpec(
            "likes",
            "likes.html",
            "email",
            "service",
            "likes",
            False,
            "likes.subject.txt",
        ),
        "new_films": TemplateSpec(
            "new_films",
            "new_films.html",
            "email",
            "bulk",
            "new_films",
            False,
            "new_films.subject.txt",
        ),
        "personal_offer": TemplateSpec(
            "personal_offer",
            "personal_offer.html",
            "email",
            "bulk",
            "marketing",
            True,
        ),
        "billing_notification": TemplateSpec(
            "billing_notification",
            "billing_notification.html",
            "email",
            "transactional",
            None,
            True,
        ),
    }
)


def get_template_spec(template_id: str, *, external_only: bool = False) -> TemplateSpec:
    try:
        spec = _TEMPLATES[template_id]
    except KeyError:
        raise ValueError("Unknown notification template") from None
    if external_only and not spec.external:
        raise ValueError("Notification template is not available for external requests")
    return spec
