from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from importlib.resources import files
from uuid import uuid4

import pytest
from contact_fixtures import contact, contact_profile

from notifications.config import Settings
from notifications.db.models import Delivery
from notifications.db.session import Database
from notifications.delivery.render import ENVIRONMENT, render


@pytest.mark.parametrize(
    "template_id,send_class,subscription,subject",
    [
        ("welcome", "transactional", None, "Добро пожаловать!"),
        ("likes", "service", "likes", "Новые лайки ваших комментариев"),
        ("new_films", "bulk", "new_films", "Новинки кино"),
    ],
)
async def test_internal_template_uses_registered_subject_file(
    database_url, template_id, send_class, subscription, subject
):
    from notifications.template_registry import get_template_spec

    spec = get_template_spec(template_id)
    assert (spec.channel, spec.send_class, spec.required_subscription) == (
        "email",
        send_class,
        subscription,
    )
    assert not spec.external
    assert spec.html_file == template_id + ".html"
    assert spec.subject_file is not None
    assert spec.subject_file == template_id + ".subject.txt"
    assert (
        files("notifications").joinpath("templates", spec.subject_file).read_text().strip()
        == subject
    )
    db = Database(database_url)
    try:
        async with db.transaction() as session:
            profile = contact_profile(
                user_id=uuid4(),
                user_version=1,
                email="user@example.org",
                first_name="Анна",
                likes=True,
                new_films=True,
                marketing=True,
            )
            delivery = Delivery(delivery_id=uuid4(), user_id=profile.user_id, kind=template_id)
            payload = await render(
                session,
                delivery,
                profile,
                Settings(process="bootstrap", database_url=database_url),
                contact(profile.user_id),
            )
            assert payload["subject"] == subject
            assert "Анна" in payload["html"]
    finally:
        await db.close()


@pytest.mark.parametrize("template_id", ["welcome", "likes", "new_films"])
def test_internal_template_is_forbidden_for_external_requests(template_id):
    from notifications.template_registry import get_template_spec

    with pytest.raises(ValueError):
        get_template_spec(template_id, external_only=True)


@pytest.mark.parametrize("template_id", ["missing", "../welcome", "welcome.html", ""])
def test_unknown_template_is_rejected(template_id):
    from notifications.template_registry import get_template_spec

    with pytest.raises(ValueError):
        get_template_spec(template_id)


def test_personal_offer_has_external_policy_without_default_subject():
    from notifications.template_registry import get_template_spec

    spec = get_template_spec("personal_offer", external_only=True)
    assert (spec.template_id, spec.html_file, spec.channel, spec.send_class) == (
        "personal_offer",
        "personal_offer.html",
        "email",
        "bulk",
    )
    assert spec.required_subscription == "marketing"
    assert spec.subject_file is None
    with pytest.raises(FrozenInstanceError):
        # Намеренно проверяем запрет изменения frozen dataclass во время выполнения.
        spec.send_class = "transactional"  # pyright: ignore[reportAttributeAccessIssue]
    assert get_template_spec("personal_offer").send_class == "bulk"


def test_billing_notification_has_external_transactional_policy_without_subscription():
    from notifications.template_registry import get_template_spec

    spec = get_template_spec("billing_notification", external_only=True)
    assert (spec.template_id, spec.html_file, spec.channel, spec.send_class) == (
        "billing_notification",
        "billing_notification.html",
        "email",
        "transactional",
    )
    assert spec.required_subscription is None
    assert spec.external is True
    assert spec.subject_file is None


@pytest.mark.parametrize("newline", ["\n", "\r\n", "\r"])
def test_billing_notification_displays_literal_escaped_text_and_line_breaks(newline):
    html = ENVIRONMENT.get_template("billing_notification.html").render(
        first_name='<img src=x onerror="alert(1)">',
        last_name="<script>surname</script>",
        subject="Подписка <b>Премиум</b> {{ 6 * 7 }}",
        text=newline.join(["<script>alert(1)</script>", "", "{{ 6 * 7 }} & МСК", ""]),
    )
    assert "<img src=x" not in html
    assert "<script>" not in html
    assert "<b>Премиум</b>" not in html
    assert "&lt;img src=x onerror=&#34;alert(1)&#34;&gt;" in html
    assert "&lt;script&gt;surname&lt;/script&gt;" in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt;<br><br>{{ 6 * 7 }} &amp; МСК<br>" in html
    assert "42" not in html


@pytest.mark.parametrize("newline", ["\n", "\r\n", "\r"])
def test_personal_offer_displays_literal_escaped_text_and_line_breaks(newline):
    html = ENVIRONMENT.get_template("personal_offer.html").render(
        first_name='<img src=x onerror="alert(1)">',
        last_name="<script>surname</script>",
        subject="{{ 6 * 7 }}",
        text=newline.join(["<script>alert(1)</script>", "", "{{ 6 * 7 }} & конец", ""]),
    )
    assert "<img src=x" not in html
    assert "<script>" not in html
    assert "&lt;img src=x onerror=&#34;alert(1)&#34;&gt;" in html
    assert "&lt;script&gt;surname&lt;/script&gt;" in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt;<br><br>{{ 6 * 7 }} &amp; конец<br>" in html
    assert "42" not in html


@pytest.mark.parametrize(
    "deleted,email,subscription,enabled,expected",
    [
        (True, None, "marketing", False, "profile_deleted"),
        (False, None, "marketing", False, "subscription_disabled"),
        (False, None, "marketing", True, None),
        (False, "", None, False, None),
        (False, "user@example.org", "marketing", True, None),
        (False, "user@example.org", "likes", False, "subscription_disabled"),
        (False, "user@example.org", "new_films", False, "subscription_disabled"),
        (False, "user@example.org", None, False, None),
    ],
)
def test_delivery_policy_applies_subscription_and_reason_priority(
    deleted, email, subscription, enabled, expected
):
    from notifications.delivery.policy import skip_reason

    profile = contact_profile(
        email=email,
        deleted_at=datetime.now(UTC) if deleted else None,
        likes=enabled,
        new_films=enabled,
        marketing=enabled,
    )
    assert skip_reason(profile, subscription) == expected


def test_missing_profile_is_deleted_for_delivery_policy():
    from notifications.delivery.policy import skip_reason

    assert skip_reason(None, "marketing") == "profile_deleted"


@pytest.mark.parametrize(
    "profile", [None, contact_profile(email="user@example.org"), contact_profile()]
)
def test_unknown_subscription_is_an_error_even_for_missing_profile(profile):
    from notifications.delivery.policy import skip_reason

    with pytest.raises(ValueError):
        skip_reason(profile, "unexpected")
