from collections.abc import Awaitable, Callable

from fastapi import Request
from fastapi.responses import JSONResponse, Response

RequestResponseEndpoint = Callable[[Request], Awaitable[Response]]


async def require_request_id(
    request: Request,
    call_next: RequestResponseEndpoint,
) -> Response:
    """Reject a request that has no request identifier."""
    if not request.headers.get('X-Request-Id'):
        return JSONResponse(
            status_code=400,
            content={
                'error_code': 'missing_request_id',
                'detail': 'X-Request-Id is required',
            },
        )
    return await call_next(request)
