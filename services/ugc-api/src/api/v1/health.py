from typing import Any, Literal, cast

from fastapi import APIRouter, Request, Response
from pydantic import BaseModel, Field
from pymongo import AsyncMongoClient
from pymongo.errors import PyMongoError

router = APIRouter()

ComponentStatus = Literal['ok', 'unavailable']


class HealthStatus(BaseModel):
    status: ComponentStatus = Field(examples=['ok'])
    mongo: ComponentStatus = Field(description='Доступность MongoDB через mongos', examples=['ok'])


@router.get(
    '/',
    response_model=HealthStatus,
    summary='Проверка здоровья',
    responses={200: {'description': 'Сервис готов'}, 503: {'description': 'MongoDB недоступна'}},
)
async def health(request: Request, response: Response) -> HealthStatus:
    client = cast(AsyncMongoClient[dict[str, Any]], request.app.state.mongo_client)
    try:
        await client.admin.command('ping')
        mongo_ok = True
    except PyMongoError:
        mongo_ok = False
    if not mongo_ok:
        response.status_code = 503
    state: ComponentStatus = 'ok' if mongo_ok else 'unavailable'
    return HealthStatus(status=state, mongo=state)
