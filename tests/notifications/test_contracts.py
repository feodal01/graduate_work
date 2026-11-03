"""Executable notification boundary contracts and isolated infrastructure checks."""

import copy
import hashlib
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

CONTRACTS = Path(__file__).resolve().parents[2] / "contracts" / "notifications"
REVIEW_AUTHOR_ID = "10000000-0000-4000-8000-000000000002"


def load_contract(name):
    schema = json.loads((CONTRACTS / f"{name}.schema.json").read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FormatChecker())


def examples(name):
    return json.loads((CONTRACTS / "fixtures" / f"{name}.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("name", ["auth", "ugc", "movies", "bootstrap"])
def test_event_contracts(name):
    validator = load_contract(name)
    for event in examples(name):
        validator.validate(event)
    if name == "auth":
        deleted = next(event for event in examples(name) if event["event_type"] == "user.deleted")
        assert deleted["event_type"] == "user.deleted"
        assert not {"email", "first_name", "last_name", "locale", "tz"} & deleted.keys()
        assert {event["event_type"] for event in examples(name)} == {
            "user.registered",
            "user.profile_updated",
            "user.deleted",
        }
    if name == "ugc":
        review_author_id = REVIEW_AUTHOR_ID
        ugc = examples(name)[0]
        assert ugc["user_id"] == review_author_id
        assert any(event.get("load_id") for event in examples(name))
        assert any("load_id" not in event for event in examples(name))
    if name == "movies":
        assert {event["event_type"] for event in examples(name)} == {
            "film.published",
            "film.updated",
        }
        for movie in examples(name):
            assert "poster" not in movie and "url" not in movie


@pytest.mark.parametrize(
    "name,field,value",
    [
        ("auth", "event_id", "bad-id"),
        ("auth", "user_id", "bad-id"),
        ("auth", "user_version", 0),
        ("auth", "user_version", True),
        ("auth", "user_version", 1.5),
        ("auth", "event_type", "user.unknown"),
        *[("auth", field, None) for field in ("email", "first_name", "last_name", "locale")],
        ("ugc", "event_id", "bad-id"),
        ("ugc", "user_id", "bad-id"),
        ("ugc", "review_id", "not-an-object-id"),
        ("ugc", "likes_version", -1),
        ("ugc", "likes_version", True),
        ("ugc", "likes_count", -1),
        ("movies", "film_id", "bad-id"),
        ("movies", "event_id", "bad-id"),
        ("movies", "film_version", 0),
        ("movies", "film_version", 1.5),
        ("movies", "catalog_published_at", "2026-10-10T12:00:00"),
        ("movies", "catalog_updated_at", "2026-10-10T15:00:00+03:00"),
        ("movies", "catalog_updated_at", "2026-13-10T12:00:00Z"),
        ("movies", "genres", [{"id": "bad-id", "name": "Comedy"}]),
        ("movies", "poster", "https://example.org/poster.jpg"),
        ("movies", "url", "https://example.org/film"),
        ("bootstrap", "expected_count", -1),
        ("bootstrap", "expected_count", True),
        ("bootstrap", "manifest_hash", "not-a-sha256"),
    ],
)
def test_invalid_contract_fields_are_rejected(name, field, value):
    event = copy.deepcopy(examples(name)[0])
    event[field] = value
    assert not load_contract(name).is_valid(event)


@pytest.mark.parametrize(
    "name,field",
    [
        *[("auth", field) for field in ("event_id", "event_type", "user_id", "user_version", "tz")],
        *[
            ("movies", field)
            for field in (
                "event_id",
                "event_type",
                "film_id",
                "film_version",
                "catalog_published_at",
                "catalog_updated_at",
                "title",
                "description",
                "creation_date",
                "imdb_rating",
                "genres",
            )
        ],
        *[
            ("ugc", field)
            for field in (
                "event_id",
                "event_type",
                "review_id",
                "user_id",
                "likes_count",
                "likes_version",
            )
        ],
        *[("bootstrap", field) for field in ("load_id", "expected_count", "manifest_hash")],
    ],
)
def test_incomplete_snapshot_is_rejected(name, field):
    event = copy.deepcopy(examples(name)[0])
    del event[field]
    assert not load_contract(name).is_valid(event)


def test_nullable_snapshot_fields_remain_explicit():
    for name, fields in [
        ("auth", ["tz"]),
        ("movies", ["description", "creation_date", "imdb_rating"]),
    ]:
        event = copy.deepcopy(examples(name)[0])
        event.update({field: None for field in fields})
        load_contract(name).validate(event)
    ugc = copy.deepcopy(examples("ugc")[0])
    ugc.update(likes_version=0, likes_count=0, load_id=None)
    load_contract("ugc").validate(ugc)


def test_manifest_hash_does_not_depend_on_input_order():
    rows = [
        ("10000000-0000-4000-8000-000000000002", "20000000-0000-4000-8000-000000000002"),
        ("10000000-0000-4000-8000-000000000001", "20000000-0000-4000-8000-000000000001"),
    ]
    expected = examples("bootstrap")[0]["manifest_hash"]
    for incoming in (rows, list(reversed(rows))):
        canonical = "".join(f"{entity_id},{event_id}\n" for entity_id, event_id in sorted(incoming))
        assert hashlib.sha256(canonical.encode("utf-8")).hexdigest() == expected
    assert examples("bootstrap")[0]["expected_count"] == len(rows)


def test_integration_stack_and_mongo_transactions(notification_stack):
    # The fixture verifies real commit/abort, PostgreSQL, Kafka, ES, RabbitMQ and SMTP.
    assert notification_stack["MONGO_DB"].startswith("notifications_test_")
    assert notification_stack["POSTGRES_DB"].startswith("notifications_test_")
