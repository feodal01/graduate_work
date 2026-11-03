import os
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

TEST_MONGO_DB = f'ugc_test_{uuid4().hex[:12]}'

os.environ.setdefault('AUTH_JWT_SECRET', 'test-secret-that-is-long-enough-for-hs256')
os.environ.setdefault('MONGO_URI', 'mongodb://127.0.0.1:27019')
os.environ['MONGO_DB'] = TEST_MONGO_DB
os.environ.setdefault('MONGO_SERVER_SELECTION_TIMEOUT_MS', '3000')
os.environ.setdefault('PROJECT_NAME', 'ugc-api')

import jwt
import pytest
from fastapi.testclient import TestClient
from pymongo import MongoClient
from pymongo.errors import PyMongoError

TEST_JWT_SECRET = os.environ['AUTH_JWT_SECRET']
TEST_USER_ID = UUID('11111111-1111-4111-8111-111111111111')
OTHER_USER_ID = UUID('22222222-2222-4222-8222-222222222222')


def make_token(
    *,
    secret: str = TEST_JWT_SECRET,
    token_type: str = 'access',
    user_id: UUID = TEST_USER_ID,
) -> str:
    issued_at = datetime.now(timezone.utc).replace(microsecond=0)
    payload = {
        'sub': str(user_id),
        'jti': str(uuid4()),
        'type': token_type,
        'iat': issued_at,
        'exp': issued_at + timedelta(minutes=15),
    }
    return jwt.encode(payload, secret, algorithm='HS256')


def auth_header(user_id: UUID = TEST_USER_ID) -> dict[str, str]:
    return {'Authorization': f'Bearer {make_token(user_id=user_id)}'}


@pytest.fixture(scope='session')
def mongo() -> Iterator[MongoClient]:
    client: MongoClient = MongoClient(
        os.environ['MONGO_URI'],
        serverSelectionTimeoutMS=3000,
        uuidRepresentation='standard',
    )
    try:
        client.admin.command('ping')
        if not client.admin.command('hello').get('setName'):
            client.close()
            pytest.fail('UGC tests require a MongoDB replica set for real transactions')
    except PyMongoError as exc:
        client.close()
        pytest.fail(f'MongoDB replica set is unavailable: {exc}')
    yield client
    client.drop_database(TEST_MONGO_DB)
    client.close()


@pytest.fixture(scope='session')
def client(mongo: MongoClient) -> Iterator[TestClient]:
    from main import app

    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def film_id() -> UUID:
    return uuid4()
