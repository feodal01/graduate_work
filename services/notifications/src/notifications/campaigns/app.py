"""Separate campaign intake process: notifications.campaigns.app:create_campaign_app."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from notifications.campaigns.api import router
from notifications.campaigns.service import CampaignError
from notifications.config import Settings
from notifications.db.session import Database


def create_campaign_app(settings: Settings | None = None) -> FastAPI:
    settings = settings if settings is not None else Settings()  # type: ignore[call-arg]
    if settings.process != "campaign-api":
        raise ValueError("Campaign HTTP application requires process=campaign-api")

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        database = Database(settings.database_url)
        app.state.database = database
        try:
            yield
        finally:
            await database.close()

    app = FastAPI(title="Notifications campaigns", lifespan=lifespan)
    app.state.settings = settings
    app.include_router(router)

    @app.exception_handler(CampaignError)
    async def campaign_error(request: Request, exc: CampaignError) -> JSONResponse:
        content: dict[str, object] = {"error_code": exc.error_code, "detail": exc.error_code}
        if exc.user_ids is not None:
            content["user_ids"] = [str(user_id) for user_id in exc.user_ids]
        return JSONResponse(content, status_code=exc.status_code)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        # Neither raw input nor validator context is an API error field. Generic
        # messages also prevent parser/validator diagnostics from echoing content.
        errors = [
            {"loc": error["loc"], "type": error["type"], "msg": "Invalid value"}
            for error in exc.errors()
        ]
        return JSONResponse(
            {"error_code": "validation_error", "detail": "Invalid request", "errors": errors},
            status_code=422,
        )

    return app
