import json
from pathlib import Path
from uuid import uuid4

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from pydantic import ValidationError

CONTRACTS = Path(__file__).resolve().parents[3] / "contracts/notifications"


def auth(kind="user.profile_updated"):
    result = {
        "event_id": str(uuid4()),
        "event_type": kind,
        "user_id": str(uuid4()),
        "user_version": 1,
    }
    if kind != "user.deleted":
        result.update(email=None, first_name=None, last_name=None, locale=None, tz=None)
    return result


def film():
    return {
        "event_id": str(uuid4()),
        "event_type": "film.updated",
        "film_id": str(uuid4()),
        "film_version": 1,
        "catalog_published_at": "2026-10-10T00:00:00Z",
        "catalog_updated_at": "2026-10-10T00:00:00+00:00",
        "title": "Film",
        "description": None,
        "creation_date": "2020-01-02",
        "imdb_rating": None,
        "genres": [{"id": str(uuid4()), "name": "Comedy"}],
    }


@pytest.mark.parametrize("kind", ["user.registered", "user.profile_updated", "user.deleted"])
def test_auth_roundtrip_matches_shared_contract(kind):
    from notifications.contracts import AuthEvent

    payload = AuthEvent.model_validate(auth(kind)).model_dump(mode="json")
    schema = json.loads((CONTRACTS / "auth.schema.json").read_text())
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(payload)


@pytest.mark.parametrize("change", [{"user_version": True}, {"tz": 123}, {"extra": 1}])
def test_auth_rejects_contract_violations(change):
    from notifications.contracts import AuthEvent

    with pytest.raises(ValidationError):
        AuthEvent.model_validate({**auth(), **change})


def test_auth_requires_full_profile_and_forbids_tombstone_contacts():
    from notifications.contracts import AuthEvent

    incomplete = auth()
    incomplete.pop("tz")
    for payload in [incomplete, {**auth("user.deleted"), "tz": None}]:
        with pytest.raises(ValidationError):
            AuthEvent.model_validate(payload)


def test_likes_zero_bootstrap_and_optional_load_id():
    from notifications.contracts import LikesEvent

    payload = {
        "event_id": str(uuid4()),
        "event_type": "review.likes_updated",
        "review_id": "abcdef0123456789abcdef01",
        "user_id": str(uuid4()),
        "likes_count": 0,
        "likes_version": 0,
        "load_id": "initial",
    }
    event = LikesEvent.model_validate(payload)
    schema = json.loads((CONTRACTS / "ugc.schema.json").read_text())
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(
        event.model_dump(mode="json")
    )
    for changes in [{"review_id": str(uuid4())}, {"likes_count": -1}, {"likes_version": True}]:
        with pytest.raises(ValidationError):
            LikesEvent.model_validate({**payload, **changes})


def test_film_roundtrip_nullable_date_and_utc():
    from notifications.contracts import FilmEvent

    schema = json.loads((CONTRACTS / "movies.schema.json").read_text())
    for creation_date in [None, "2020-01-02"]:
        event = FilmEvent.model_validate({**film(), "creation_date": creation_date})
        Draft202012Validator(schema, format_checker=FormatChecker()).validate(
            event.model_dump(mode="json")
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"catalog_updated_at": "2026-10-10T03:00:00+03:00"},
        {"catalog_updated_at": "2026-10-10T00:00:00"},
        {"catalog_updated_at": 1234567890},
        {"creation_date": "2020-01-02T00:00:00Z"},
        {"creation_date": 1234567890},
        {"film_version": True},
        {"imdb_rating": True},
    ],
)
def test_film_rejects_invalid_wire_types(changes):
    from notifications.contracts import FilmEvent

    with pytest.raises(ValidationError):
        FilmEvent.model_validate({**film(), **changes})
