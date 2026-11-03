"""PostgreSQL health-check implementation."""

import logging

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from db.health import HealthCheck

logger = logging.getLogger(__name__)


class PostgresHealthCheck(HealthCheck):
    """Check PostgreSQL availability without owning its engine."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def ping(self) -> bool:
        try:
            async with self._engine.connect() as connection:
                await connection.execute(text('SELECT 1'))
            return True
        except (SQLAlchemyError, OSError):
            logger.exception('Error pinging database')
            return False
