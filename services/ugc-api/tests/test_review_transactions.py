import asyncio
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from threading import Event
from uuid import uuid4

import pytest
from bson import ObjectId

from tests.conftest import TEST_MONGO_DB, auth_header
from tests.test_reviews import create_review


def events(mongo, review_id):
    return list(
        mongo[TEST_MONGO_DB].ugc_outbox.find({'review_id': review_id}).sort('likes_version', 1)
    )


def test_like_snapshots_target_author_and_count_all_changes(client, mongo, film_id):
    author, voter = uuid4(), uuid4()
    review = create_review(client, film_id, author)
    url = f'/api/v1/ugc/reviews/{review["id"]}/vote'
    assert (
        mongo[TEST_MONGO_DB].reviews.find_one({'_id': ObjectId(review['id'])}).get('likes_version')
        == 0
    )

    for value in ['like', 'like', 'dislike', 'dislike', 'like']:
        response = client.put(url, json={'value': value}, headers=auth_header(voter))
        assert response.status_code == 200, response.text
    response = client.delete(url, headers=auth_header(voter))
    assert response.status_code == 200
    rows = events(mongo, review['id'])
    assert [(row['likes_count'], row['likes_version']) for row in rows] == [
        (1, 1),
        (0, 2),
        (1, 3),
        (0, 4),
    ]
    assert len({row['event_id'] for row in rows}) == 4
    for row in rows:
        assert row['event_type'] == 'review.likes_updated'
        assert row['user_id'] == author
        assert row['user_id'] != voter
        assert row['published_at'] is None
        assert row.get('load_id') is None


def test_dislike_without_likes_change_has_no_event(client, mongo, film_id):
    review = create_review(client, film_id, uuid4())
    voter = uuid4()
    url = f'/api/v1/ugc/reviews/{review["id"]}/vote'
    assert client.put(url, json={'value': 'dislike'}, headers=auth_header(voter)).status_code == 200
    assert client.delete(url, headers=auth_header(voter)).status_code == 200
    assert events(mongo, review['id']) == []
    stored = mongo[TEST_MONGO_DB].reviews.find_one({'_id': ObjectId(review['id'])})
    assert (stored['likes'], stored['dislikes'], stored.get('likes_version')) == (0, 0, 0)


@pytest.mark.parametrize('initial,operation', [(None, 'like'), ('like', 'dislike'), ('like', None)])
def test_outbox_failure_rolls_back_vote_counters_and_version(
    client, mongo, film_id, monkeypatch, initial, operation
):
    from services import reviews

    review = create_review(client, film_id, uuid4())
    voter = uuid4()
    url = f'/api/v1/ugc/reviews/{review["id"]}/vote'
    if initial:
        assert (
            client.put(url, json={'value': initial}, headers=auth_header(voter)).status_code == 200
        )
    db = mongo[TEST_MONGO_DB]
    before = db.reviews.find_one({'_id': ObjectId(review['id'])})
    before_vote = db.review_votes.find_one({'review_id': ObjectId(review['id']), 'user_id': voter})
    before_events = events(mongo, review['id'])
    original = reviews.append_likes_event

    async def fail_after_insert(session, snapshot):
        await original(session, snapshot)
        raise RuntimeError('injected outbox failure')

    monkeypatch.setattr(reviews, 'append_likes_event', fail_after_insert, raising=False)
    with pytest.raises(RuntimeError, match='injected outbox failure'):
        if operation:
            client.put(url, json={'value': operation}, headers=auth_header(voter))
        else:
            client.delete(url, headers=auth_header(voter))
    assert db.reviews.find_one({'_id': ObjectId(review['id'])}) == before
    assert (
        db.review_votes.find_one({'review_id': ObjectId(review['id']), 'user_id': voter})
        == before_vote
    )
    assert events(mongo, review['id']) == before_events


@pytest.mark.parametrize('same_voter', [False, True])
def test_concurrent_votes_have_exact_counts_versions_and_events(client, mongo, film_id, same_voter):
    review = create_review(client, film_id, uuid4())
    url = f'/api/v1/ugc/reviews/{review["id"]}/vote'
    voter = uuid4()
    voters = [voter if same_voter else uuid4() for _ in range(16)]

    def like(user):
        return client.put(url, json={'value': 'like'}, headers=auth_header(user))

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(like, voters))
    assert all(response.status_code == 200 for response in responses)
    expected = 1 if same_voter else 16
    stored = mongo[TEST_MONGO_DB].reviews.find_one({'_id': ObjectId(review['id'])})
    assert (stored['likes'], stored.get('likes_version')) == (expected, expected)
    assert (
        mongo[TEST_MONGO_DB].review_votes.count_documents({'review_id': ObjectId(review['id'])})
        == expected
    )
    assert [(row['likes_count'], row['likes_version']) for row in events(mongo, review['id'])] == [
        (number, number) for number in range(1, expected + 1)
    ]


def test_frozen_gate_blocks_review_membership_and_votes(client, mongo, film_id):
    author, voter = uuid4(), uuid4()
    review = create_review(client, film_id, author)
    base = f'/api/v1/ugc/reviews/{review["id"]}'
    assert (
        client.put(base + '/vote', json={'value': 'like'}, headers=auth_header(voter)).status_code
        == 200
    )
    db = mongo[TEST_MONGO_DB]
    db.review_write_gate.update_many({}, {'$set': {'frozen': True}})
    try:
        responses = [
            client.post(
                f'/api/v1/ugc/films/{uuid4()}/reviews',
                json={'text': 'blocked'},
                headers=auth_header(author),
            ),
            client.patch(base, json={'text': 'blocked'}, headers=auth_header(author)),
            client.delete(base, headers=auth_header(author)),
            client.put(base + '/vote', json={'value': 'dislike'}, headers=auth_header(voter)),
            client.delete(base + '/vote', headers=auth_header(voter)),
        ]
        assert [response.status_code for response in responses] == [503] * 5
        assert client.get(base).json()['likes'] == 1
        assert len(events(mongo, review['id'])) == 1
    finally:
        db.review_write_gate.update_many({}, {'$set': {'frozen': False}})


def test_freeze_fences_in_flight_vote_before_snapshot(client, mongo, film_id, monkeypatch):
    from services import reviews

    review = create_review(client, film_id, uuid4())
    url = f'/api/v1/ugc/reviews/{review["id"]}/vote'
    db = mongo[TEST_MONGO_DB]
    entered, release, freezing = Event(), Event(), Event()
    original = reviews.append_likes_event

    async def hold_before_outbox(session, snapshot):
        entered.set()
        assert await asyncio.to_thread(release.wait, 10), 'writer release timed out'
        await original(session, snapshot)

    monkeypatch.setattr(reviews, 'append_likes_event', hold_before_outbox, raising=False)

    def freeze():
        def transaction(session):
            freezing.set()
            return db.review_write_gate.update_many(
                {}, {'$set': {'frozen': True}}, session=session
            ).matched_count

        with mongo.start_session() as session:
            return session.with_transaction(transaction)

    with ThreadPoolExecutor(max_workers=2) as pool:
        writer = pool.submit(client.put, url, json={'value': 'like'}, headers=auth_header(uuid4()))
        try:
            assert entered.wait(5), 'writer did not reach outbox inside transaction'
            freezer = pool.submit(freeze)
            assert freezing.wait(5)
            with pytest.raises(TimeoutError):
                freezer.result(timeout=0.2)
        finally:
            release.set()
        try:
            assert writer.result(timeout=10).status_code == 200
            assert freezer.result(timeout=10) == 256
            assert client.get(url.removesuffix('/vote')).json()['likes'] == 1
            assert len(events(mongo, review['id'])) == 1
            assert (
                client.put(url, json={'value': 'like'}, headers=auth_header(uuid4())).status_code
                == 503
            )
        finally:
            db.review_write_gate.update_many({}, {'$set': {'frozen': False}})
