"""HTTP application factory: uvicorn notifications.main:create_app --factory."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import cast

import httpx
from fastapi import FastAPI
from pydantic import SecretStr

from notifications.clients.auth_introspection import AuthIntrospectionClient
from notifications.config import Settings
from notifications.db.session import Database
from notifications.profiles.api import router


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings if settings is not None else Settings()  # type: ignore[call-arg]
    if settings.process != "api":
        raise ValueError("HTTP application requires process=api")

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        database = Database(settings.database_url)
        app.state.database = database
        try:
            async with httpx.AsyncClient(
                timeout=cast(float, settings.auth_introspection_timeout_seconds),
            ) as http:
                app.state.auth_introspection = AuthIntrospectionClient(
                    http,
                    cast(str, settings.auth_introspection_url),
                    cast(SecretStr, settings.auth_introspection_api_key).get_secret_value(),
                )
                yield
        finally:
            await database.close()

    app = FastAPI(title="Notifications", lifespan=lifespan)
    app.state.settings = settings
    app.include_router(router)
    return app
