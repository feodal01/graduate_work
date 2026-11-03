from uuid import uuid4

from django.http import HttpRequest

_REQUEST_ID_ATTRIBUTE = '_auth_request_id'


def get_request_id(request: HttpRequest) -> str:
    """Return one request identifier for the current Admin request."""
    incoming = request.headers.get('X-Request-Id')
    if incoming:
        return incoming

    stored = getattr(request, _REQUEST_ID_ATTRIBUTE, None)
    if isinstance(stored, str):
        return stored

    generated = uuid4().hex
    setattr(request, _REQUEST_ID_ATTRIBUTE, generated)
    return generated
