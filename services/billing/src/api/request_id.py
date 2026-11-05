from collections.abc import Awaitable, Callable
from uuid import uuid4

from fastapi import Request
from fastapi.responses import JSONResponse, Response

RequestResponseEndpoint = Callable[[Request], Awaitable[Response]]

_DOC_PATHS = frozenset({'/billing/docs', '/billing/openapi.json', '/billing/redoc'})


def _is_browser_ui_path(path: str) -> bool:
    if path in _DOC_PATHS:
        return False
    return path == '/billing' or path.startswith('/billing/')


async def require_request_id(
    request: Request,
    call_next: RequestResponseEndpoint,
) -> Response:
    """Require X-Request-Id on API routes; auto-generate on browser HTML routes."""
    raw_request_id = request.headers.get('X-Request-Id', '').strip()
    if not raw_request_id:
        if _is_browser_ui_path(request.url.path):
            raw_request_id = f'ui-{uuid4().hex}'
        else:
            return JSONResponse(
                status_code=400,
                content={
                    'error_code': 'missing_request_id',
                    'detail': 'X-Request-Id is required',
                },
            )
    request.state.request_id = raw_request_id
    response = await call_next(request)
    response.headers.setdefault('X-Request-Id', raw_request_id)
    return response
