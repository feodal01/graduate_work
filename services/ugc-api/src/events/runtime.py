"""MongoDB для отдельных CLI-процессов, без запуска HTTP приложения."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from beanie import init_beanie
from pymongo import AsyncMongoClient

from core.config import settings
from dependencies import set_mongo_client
from models.documents import DOCUMENT_MODELS


@asynccontextmanager
async def mongo_runtime() -> AsyncIterator[AsyncMongoClient[dict[str, Any]]]:
    client: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(
        settings.mongo_uri,
        uuidRepresentation='standard',
        tz_aware=True,
        serverSelectionTimeoutMS=settings.mongo_server_selection_timeout_ms,
    )
    set_mongo_client(client)
    try:
        hello = await client.admin.command('hello')
        if not hello.get('setName') and hello.get('msg') != 'isdbgrid':
            raise RuntimeError('UGC requires a replica set or sharded MongoDB for transactions')
        await init_beanie(database=client[settings.mongo_db], document_models=DOCUMENT_MODELS)
        yield client
    finally:
        set_mongo_client(None)
        await client.close()
