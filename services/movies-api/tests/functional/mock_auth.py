from datetime import date
from typing import Any

from fastapi import FastAPI, Header, HTTPException

SERVICE_KEY = 'functional-service-key-that-is-long-enough'
UNAVAILABLE_YEARS = {1900, 2999}

app = FastAPI()


@app.get('/health')
async def health() -> dict[str, str]:
    return {'status': 'ok'}


@app.post('/api/v1/auth/permissions/check')
async def check_permission(
    payload: dict[str, Any],
    service_key: str | None = Header(default=None, alias='X-Service-Key'),
    authorization: str | None = Header(default=None),
) -> dict[str, bool]:
    if service_key != SERVICE_KEY:
        raise HTTPException(status_code=401, detail='Invalid service credentials')

    raw_creation_date = payload['attributes']['creation_date']
    creation_date = date.fromisoformat(raw_creation_date) if raw_creation_date is not None else None
    if creation_date is not None and creation_date.year in UNAVAILABLE_YEARS:
        raise HTTPException(status_code=503, detail='Auth unavailable')

    allowed = authorization is not None
    if creation_date is not None and creation_date <= date(2023, 1, 1):
        allowed = True
    return {'allowed': allowed}
