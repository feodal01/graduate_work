from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast
from uuid import UUID

import jwt
from fastapi import HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt.exceptions import ExpiredSignatureError, PyJWTError

from core.config import settings


@dataclass(frozen=True, slots=True)
class VerifiedAccessToken:
    value: str
    user_id: UUID
    token_id: UUID
    issued_at: datetime
    expires_at: datetime


def decode_access_token(token: str) -> VerifiedAccessToken:
    """Проверяет access JWT и возвращает типизированные claims."""
    try:
        payload = jwt.decode(
            token,
            settings.auth_jwt_secret.get_secret_value(),
            algorithms=[settings.auth_jwt_algorithm],
            options={'require': ['sub', 'jti', 'type', 'iat', 'exp']},
        )
    except ExpiredSignatureError as exc:
        raise _invalid_token_error() from exc
    except PyJWTError as exc:
        raise _invalid_token_error() from exc

    try:
        if payload['type'] != 'access':
            raise ValueError('unexpected token type')
        return VerifiedAccessToken(
            value=token,
            user_id=UUID(payload['sub']),
            token_id=UUID(payload['jti']),
            issued_at=datetime.fromtimestamp(payload['iat'], UTC),
            expires_at=datetime.fromtimestamp(payload['exp'], UTC),
        )
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise _invalid_token_error() from exc


def _invalid_token_error() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail='Invalid or expired token',
        headers={'WWW-Authenticate': 'Bearer'},
    )


class OptionalJWTBearer(HTTPBearer):
    """Допускает анонимный запрос и строго проверяет переданный access JWT."""

    def __init__(self) -> None:
        super().__init__(
            auto_error=False,
            scheme_name='BearerAuth',
            bearerFormat='JWT',
        )

    async def __call__(  # type: ignore[override]
        self,
        request: Request,
    ) -> VerifiedAccessToken | None:
        credentials = cast(
            HTTPAuthorizationCredentials | None,
            await super().__call__(request),
        )
        if credentials is None:
            if request.headers.get('Authorization') is None:
                return None
            raise _invalid_token_error()
        if credentials.scheme.lower() != 'bearer':
            raise _invalid_token_error()
        return decode_access_token(credentials.credentials)


optional_jwt_bearer = OptionalJWTBearer()
