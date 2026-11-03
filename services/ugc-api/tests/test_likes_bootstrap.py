import asyncio
import hashlib
from uuid import uuid4

import pytest
from bson import ObjectId

from tests.conftest import TEST_MONGO_DB, auth_header
from tests.test_reviews import create_review


def run(client, coroutine, *args):
    return client.portal.call(coroutine, *args)


def test_snapshot_is_immutable_and_enqueue_survives_cleanup(client, mongo, film_id):
    from events.bootstrap_store import capture_reviews, enqueue_reviews, load_status

    review = create_review(client, film_id, uuid4())
    db = mongo[TEST_MONGO_DB]
    db.reviews.update_one(
        {'_id': ObjectId(review['id'])}, {'$set': {'likes': 12, 'likes_version': 7}}
    )
    load_id = str(uuid4())
    count = run(client, capture_reviews, load_id)
    snapshot = db.likes_bootstrap_rows.find_one({'load_id': load_id, 'review_id': review['id']})
    assert snapshot['payload']['likes_count'] == 12
    assert snapshot['payload']['likes_version'] == 7
    assert (
        client.put(
            f'/api/v1/ugc/reviews/{review["id"]}/vote',
            json={'value': 'like'},
            headers=auth_header(uuid4()),
        ).status_code
        == 200
    )
    assert (
        db.likes_bootstrap_rows.find_one({'_id': snapshot['_id']})['payload']['likes_count'] == 12
    )
    with pytest.raises(ValueError, match='completed'):
        run(client, capture_reviews, load_id)
    total = 0
    while batch := run(client, enqueue_reviews, load_id, 2):
        total += batch
    assert total == count
    event = db.ugc_outbox.find_one({'load_id': load_id, 'review_id': review['id']})
    assert event['event_id'] == snapshot['payload']['event_id']
    assert (event['likes_count'], event['likes_version']) == (12, 7)
    status = run(client, load_status, load_id)
    rows = list(db.likes_bootstrap_rows.find({'load_id': load_id}).sort('review_id', 1))
    expected = hashlib.sha256(
        ''.join(f'{row["review_id"]},{row["payload"]["event_id"]}\n' for row in rows).encode()
    ).hexdigest()
    assert status == {'load_id': load_id, 'expected_count': count, 'manifest_hash': expected}
    assert db.likes_bootstrap_loads.find_one({'_id': load_id})['state'] == 'enqueued'
    db.ugc_outbox.delete_many({'load_id': load_id})
    assert run(client, enqueue_reviews, load_id, 100) == 0
    assert db.ugc_outbox.count_documents({'load_id': load_id}) == 0


def test_capture_crash_keeps_gate_frozen_then_resumes_same_snapshot(client, mongo, monkeypatch):
    from events import bootstrap_store
    from services.review_writes import initialize_review_write_gate

    review = create_review(client, uuid4(), uuid4())
    load_id = str(uuid4())
    original = bootstrap_store.capture_batch
    calls = 0

    async def crash_after_batch(*args):
        nonlocal calls
        result = await original(*args)
        calls += 1
        if calls == 1:
            raise RuntimeError('capture interrupted')
        return result

    monkeypatch.setattr(bootstrap_store, 'capture_batch', crash_after_batch)
    with pytest.raises(RuntimeError, match='capture interrupted'):
        run(client, bootstrap_store.capture_reviews, load_id)
    db = mongo[TEST_MONGO_DB]
    assert db.review_write_gate.count_documents({'frozen': True}) == 256
    rows_before = list(db.likes_bootstrap_rows.find({'load_id': load_id}))
    assert rows_before
    run(client, initialize_review_write_gate)
    assert db.review_write_gate.count_documents({'frozen': True}) == 256
    assert (
        client.put(
            f'/api/v1/ugc/reviews/{review["id"]}/vote',
            json={'value': 'like'},
            headers=auth_header(uuid4()),
        ).status_code
        == 503
    )
    with pytest.raises(ValueError, match='another'):
        run(client, bootstrap_store.capture_reviews, str(uuid4()))
    monkeypatch.setattr(bootstrap_store, 'capture_batch', original)
    run(client, bootstrap_store.capture_reviews, load_id)
    assert db.review_write_gate.count_documents({'frozen': True}) == 0
    for row in rows_before:
        assert db.likes_bootstrap_rows.find_one({'_id': row['_id']}) == row


def test_enqueue_failure_rolls_back_outbox_markers_and_progress(client, mongo, monkeypatch):
    from events import bootstrap_store

    create_review(client, uuid4(), uuid4())
    load_id = str(uuid4())
    run(client, bootstrap_store.capture_reviews, load_id)
    original = bootstrap_store.append_likes_event

    async def fail_after_insert(session, snapshot):
        await original(session, snapshot)
        raise RuntimeError('enqueue interrupted')

    monkeypatch.setattr(bootstrap_store, 'append_likes_event', fail_after_insert)
    with pytest.raises(RuntimeError, match='enqueue interrupted'):
        run(client, bootstrap_store.enqueue_reviews, load_id, 2)
    db = mongo[TEST_MONGO_DB]
    assert db.ugc_outbox.count_documents({'load_id': load_id}) == 0
    assert db.likes_bootstrap_rows.count_documents({'load_id': load_id, 'enqueued': True}) == 0
    assert db.likes_bootstrap_loads.find_one({'_id': load_id})['enqueued_count'] == 0
    monkeypatch.setattr(bootstrap_store, 'append_likes_event', original)
    assert run(client, bootstrap_store.enqueue_reviews, load_id, 2) > 0


def test_concurrent_enqueue_does_not_duplicate(client, mongo):
    from events.bootstrap_store import capture_reviews, enqueue_reviews

    for _ in range(3):
        create_review(client, uuid4(), uuid4())
    load_id = str(uuid4())
    count = run(client, capture_reviews, load_id)
    assert count >= 3

    async def concurrent():
        return await asyncio.gather(*(enqueue_reviews(load_id, 2) for _ in range(count + 2)))

    assert sum(run(client, concurrent)) == count
    assert mongo[TEST_MONGO_DB].ugc_outbox.count_documents({'load_id': load_id}) == count


def test_capture_fences_in_flight_writer(client, mongo, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor, TimeoutError
    from threading import Event

    from events import bootstrap_store
    from services import reviews

    review = create_review(client, uuid4(), uuid4())
    entered, release, capturing = Event(), Event(), Event()
    original = reviews.append_likes_event
    original_begin = bootstrap_store.begin_capture
    load_id = str(uuid4())

    async def hold(session, snapshot):
        entered.set()
        assert await asyncio.to_thread(release.wait, 10)
        await original(session, snapshot)

    async def begin(load):
        capturing.set()
        await original_begin(load)

    monkeypatch.setattr(reviews, 'append_likes_event', hold)
    monkeypatch.setattr(bootstrap_store, 'begin_capture', begin)
    with ThreadPoolExecutor(max_workers=2) as pool:
        writer = pool.submit(
            client.put,
            f'/api/v1/ugc/reviews/{review["id"]}/vote',
            json={'value': 'like'},
            headers=auth_header(uuid4()),
        )
        try:
            assert entered.wait(5)
            capture = pool.submit(run, client, bootstrap_store.capture_reviews, load_id)
            assert capturing.wait(5)
            with pytest.raises(TimeoutError):
                capture.result(timeout=0.1)
        finally:
            release.set()
        assert writer.result(timeout=10).status_code == 200
        assert capture.result(timeout=10) > 0
    row = mongo[TEST_MONGO_DB].likes_bootstrap_rows.find_one(
        {'load_id': load_id, 'review_id': review['id']}
    )
    assert (row['payload']['likes_count'], row['payload']['likes_version']) == (1, 1)


def test_cli_process_crash_resume_and_manifest(client, mongo):
    import json
    import os
    import subprocess
    import sys
    from pathlib import Path

    import jsonschema

    review = create_review(client, uuid4(), uuid4())
    load_id = str(uuid4())
    service = Path(__file__).resolve().parents[1]
    env = {**os.environ, 'PYTHONPATH': str(service / 'src'), 'BOOTSTRAP_BATCH_SIZE': '1'}
    code = """
import asyncio, os
from events import bootstrap_store
from events.runtime import mongo_runtime
original = bootstrap_store.capture_batch
async def crash(*args):
    await original(*args)
    os._exit(23)
bootstrap_store.capture_batch = crash
async def main():
    async with mongo_runtime():
        await bootstrap_store.capture_reviews(os.environ['TEST_LOAD_ID'])
asyncio.run(main())
"""
    stopped = subprocess.run(
        [sys.executable, '-c', code],
        env={**env, 'TEST_LOAD_ID': load_id},
        cwd=service,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert stopped.returncode == 23, stopped.stderr
    db = mongo[TEST_MONGO_DB]
    assert db.review_write_gate.count_documents({'frozen': True}) == 256
    original = db.likes_bootstrap_rows.find_one({'load_id': load_id})
    assert original is not None

    def cli(phase):
        result = subprocess.run(
            [
                sys.executable,
                '-m',
                'bootstrap_notifications',
                phase,
                '--load-id',
                load_id,
                '--limit',
                '2',
            ],
            env=env,
            cwd=service,
            capture_output=True,
            text=True,
            timeout=20,
        )
        assert result.returncode == 0, result.stderr
        return json.loads(result.stdout)

    assert cli('capture')['captured'] > 0
    assert db.review_write_gate.count_documents({'frozen': True}) == 0
    assert db.likes_bootstrap_rows.find_one({'_id': original['_id']}) == original
    assert db.reviews.find_one({'_id': ObjectId(review['id'])})['likes_version'] == 0
    assert cli('enqueue')['enqueued'] > 0
    assert cli('enqueue')['enqueued'] == 0
    manifest = cli('status')
    schema = json.loads(
        (service.parents[1] / 'contracts/notifications/bootstrap.schema.json').read_text()
    )
    jsonschema.validate(manifest, schema)
    assert manifest['load_id'] == load_id
