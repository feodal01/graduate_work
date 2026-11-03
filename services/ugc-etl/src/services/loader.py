import logging
from collections.abc import Sequence
from typing import Any, Optional, Protocol, Self, Union

from aiochclient import ChClient
from aiohttp import ClientSession

from core.config import build_http_session, settings, split_sql
from services.transform import EventRow

logger = logging.getLogger(__name__)

EVENT_COLUMNS = (
    'event_id',
    'user_id',
    'event_type',
    'occurred_at',
    'received_at',
    'ingested_at',
    'request_id',
    'film_id',
    'page',
    'element_type',
    'element_id',
    'position_seconds',
    'duration_seconds',
    'completion_ratio',
    'from_quality',
    'to_quality',
    'filters',
    'payload',
)

EVENTS_TABLE = 'ugc.events'
_INSERT_SQL = f'INSERT INTO {EVENTS_TABLE} ({", ".join(EVENT_COLUMNS)}) VALUES'


class ClickHouseClient(Protocol):
    async def execute(self, query: str, *args: Any, **kwargs: Any) -> Any: ...

    async def is_alive(self) -> bool: ...


class ClickHouseLoader:
    def __init__(
        self,
        client: Union[ChClient, ClickHouseClient],
        session: Optional[ClientSession] = None,
    ) -> None:
        self._client = client
        self._session = session
        self._insert = settings.clickhouse_backoff.for_clickhouse()(self._insert_rows)
        self._apply_schema = settings.clickhouse_backoff.for_clickhouse()(self._apply_schema_once)

    @classmethod
    async def from_settings(cls) -> Self:
        session = build_http_session()
        client = ChClient(
            session,
            url=settings.clickhouse_url(),
            user=settings.clickhouse_user,
            password=settings.clickhouse_password.get_secret_value(),
        )
        return cls(client, session)

    async def ensure_schema(self) -> None:
        await self._apply_schema()

    async def insert_batch(self, events: list[EventRow]) -> None:
        if not events:
            return
        rows = [event.as_insert_tuple() for event in events]
        await self._insert(rows)

    async def ping(self) -> bool:
        try:
            return await self._client.is_alive()
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception('ClickHouse ping failed')
            return False

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()

    async def _insert_rows(self, rows: Sequence[Sequence[object]]) -> None:
        await self._client.execute(_INSERT_SQL, *rows)

    async def _apply_schema_once(self) -> None:
        script = settings.schema_file().read_text(encoding='utf-8')
        for statement in split_sql(script):
            await self._client.execute(statement)
