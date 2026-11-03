import os
from collections.abc import Iterator
from typing import Any, Optional
from uuid import UUID

os.environ.setdefault('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
os.environ.setdefault('CLICKHOUSE_USER', 'ugc')
os.environ.setdefault('CLICKHOUSE_PASSWORD', 'test-clickhouse-password')
os.environ.setdefault('PROJECT_NAME', 'ugc-etl')

import pytest
from fastapi.testclient import TestClient

from services.contracts import ConsumedRecord
from services.transform import EventRow

TEST_USER_ID = UUID('11111111-1111-4111-8111-111111111111')
FILM_ID = UUID('3d825f60-9fff-4dfe-b294-1a45fa1e115d')


class MemoryConsumer:
    def __init__(self, records: Optional[list[ConsumedRecord]] = None) -> None:
        self.records = list(records or [])
        self.committed = 0
        self.dlq: list[tuple[bytes, Optional[bytes]]] = []
        self.reachable = True
        self.dlq_fail = False
        self.events: list[str] = []

    async def poll_batch(self) -> list[ConsumedRecord]:
        batch = self.records
        self.records = []
        return batch

    async def commit(self, records: list[ConsumedRecord]) -> None:
        del records
        self.committed += 1
        self.events.append('commit')

    async def send_dlq(self, value: bytes, key: Optional[bytes]) -> None:
        if self.dlq_fail:
            raise RuntimeError('dlq unavailable')
        self.dlq.append((value, key))
        self.events.append('dlq')

    async def ping(self) -> bool:
        return self.reachable


class RunningPipelineTask:
    def done(self) -> bool:
        return False


class StoppedPipelineTask:
    def done(self) -> bool:
        return True


class MemoryWarehouse:
    def __init__(self) -> None:
        self.inserted: list[list[EventRow]] = []
        self.fail = False
        self.reachable = True
        self.insert_calls = 0
        self.schema_calls = 0
        self.events: list[str] = []

    async def ensure_schema(self) -> None:
        self.schema_calls += 1

    async def insert_batch(self, events: list[EventRow]) -> None:
        self.insert_calls += 1
        self.events.append('insert')
        if self.fail:
            raise RuntimeError('clickhouse unavailable')
        self.inserted.append(list(events))

    async def ping(self) -> bool:
        return self.reachable


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.inserts: list[str] = []
        self.commands: list[str] = []

    async def execute(self, query: str, *args: Any, **kwargs: Any) -> None:
        self.commands.append(query)
        if query.upper().startswith('INSERT'):
            self.inserts.append(query.split()[2])

    async def is_alive(self) -> bool:
        return True


@pytest.fixture(autouse=True)
def _reset_pipeline_pending() -> None:
    from services.pipeline import reset_pending, stats

    reset_pending()
    stats.last_success_at = None
    stats.consecutive_errors = 0


@pytest.fixture
def warehouse() -> MemoryWarehouse:
    return MemoryWarehouse()


@pytest.fixture
def consumer() -> MemoryConsumer:
    return MemoryConsumer()


@pytest.fixture
def client(consumer: MemoryConsumer, warehouse: MemoryWarehouse) -> Iterator[TestClient]:
    from main import create_app

    app = create_app(consumer=consumer, warehouse=warehouse)
    app.state.pipeline_task = RunningPipelineTask()
    test_client = TestClient(app)
    yield test_client
