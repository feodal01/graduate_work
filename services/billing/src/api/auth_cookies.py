"""Cookie с токенами Auth для экранов Биллинга (BIL-API-01).

Каждая cookie видна только тем адресам, где её читают: за шлюзом на том же
хосте живут другие сервисы, и с `Path=/` браузер отдавал бы токены им всем.
"""

from fastapi import Request, Response

ACCESS_TOKEN_COOKIE = 'billing_access_token'
REFRESH_TOKEN_COOKIE = 'billing_refresh_token'

# Access нужен экранам `/billing/*`; JSON API принимает только Bearer.
ACCESS_COOKIE_PATH = '/billing'
# Refresh читается только при выходе.
REFRESH_COOKIE_PATH = '/billing/logout'
# Раньше обе cookie ставились на весь хост. Браузер считает cookie с другим
# Path отдельной, поэтому прежние нужно стирать явно.
LEGACY_COOKIE_PATH = '/'


def is_secure_request(request: Request) -> bool:
    forwarded_proto = request.headers.get('X-Forwarded-Proto', '')
    first_proto = forwarded_proto.split(',', 1)[0].strip().lower()
    return request.url.scheme == 'https' or first_proto == 'https'


def _delete(response: Response, key: str, path: str, *, secure: bool) -> None:
    response.delete_cookie(
        key=key,
        path=path,
        httponly=True,
        secure=secure,
        samesite='lax',
    )


def _clear_legacy_cookies(response: Response, *, secure: bool) -> None:
    for key in (ACCESS_TOKEN_COOKIE, REFRESH_TOKEN_COOKIE):
        _delete(response, key, LEGACY_COOKIE_PATH, secure=secure)


def set_auth_cookies(
    response: Response,
    *,
    access_token: str,
    refresh_token: str,
    secure: bool,
) -> None:
    """Сохраняет токены в HttpOnly-cookie и стирает прежние с `Path=/`.

    Прежние стираются уже при входе: иначе браузер пришлёт две cookie с одним
    именем, и сервер прочитает старую.
    """
    _clear_legacy_cookies(response, secure=secure)
    response.set_cookie(
        key=ACCESS_TOKEN_COOKIE,
        value=access_token,
        httponly=True,
        secure=secure,
        samesite='lax',
        path=ACCESS_COOKIE_PATH,
    )
    response.set_cookie(
        key=REFRESH_TOKEN_COOKIE,
        value=refresh_token,
        httponly=True,
        secure=secure,
        samesite='lax',
        path=REFRESH_COOKIE_PATH,
    )


def clear_auth_cookies(response: Response, *, secure: bool) -> None:
    """Стирает cookie с токенами: и текущие, и прежние с `Path=/`."""
    _clear_legacy_cookies(response, secure=secure)
    _delete(response, ACCESS_TOKEN_COOKIE, ACCESS_COOKIE_PATH, secure=secure)
    _delete(response, REFRESH_TOKEN_COOKIE, REFRESH_COOKIE_PATH, secure=secure)
