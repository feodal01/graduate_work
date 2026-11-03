"""Validate access JWTs locally, then require current session validity from Auth."""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Annotated, cast
from uuid import UUID, uuid4

import jwt
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt.exceptions import PyJWTError

from notifications.clients.auth_introspection import (
    AuthIntrospectionClient,
    AuthIntrospectionUnavailable,
)
from notifications.config import Settings


@dataclass(frozen=True)
class VerifiedAccessToken:
    value: str
    user_id: UUID
    token_id: UUID
    issued_at: datetime
    expires_at: datetime


def invalid_token() -> HTTPException:
    return HTTPException(
        status_code=401,
        detail="Invalid or expired token",
        headers={"WWW-Authenticate": "Bearer"},
    )


def decode_access_token(token: str, settings: Settings) -> VerifiedAccessToken:
    if settings.jwt_secret is None or settings.jwt_algorithm is None:
        raise ValueError("API JWT settings are required")
    try:
        # PyJWT leaves its optional key default untyped; our key is explicitly a string.
        payload = jwt.decode(  # pyright: ignore[reportUnknownMemberType]
            token,
            settings.jwt_secret.get_secret_value(),
            algorithms=[settings.jwt_algorithm],
            options={"require": ["sub", "jti", "type", "iat", "exp"]},
        )
    except PyJWTError as exc:
        raise invalid_token() from exc
    try:
        if payload["type"] != "access":
            raise ValueError("unexpected token type")
        return VerifiedAccessToken(
            value=token,
            user_id=UUID(payload["sub"]),
            token_id=UUID(payload["jti"]),
            issued_at=datetime.fromtimestamp(payload["iat"], UTC),
            expires_at=datetime.fromtimestamp(payload["exp"], UTC),
        )
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise invalid_token() from exc


bearer = HTTPBearer(auto_error=False, scheme_name="BearerAuth", bearerFormat="JWT")


async def access_token(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
) -> VerifiedAccessToken:
    if credentials is None:
        raise HTTPException(
            status_code=401,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if credentials.scheme.lower() != "bearer":
        raise invalid_token()
    verified = decode_access_token(
        credentials.credentials, cast(Settings, request.app.state.settings)
    )
    auth = cast(AuthIntrospectionClient, request.app.state.auth_introspection)
    try:
        user_id = await auth.check(
            verified.value, request.headers.get("X-Request-Id") or str(uuid4())
        )
        if user_id is not None and user_id != verified.user_id:
            raise AuthIntrospectionUnavailable("subject_mismatch")
    except AuthIntrospectionUnavailable:
        raise HTTPException(status_code=503, detail="Authentication service unavailable") from None
    if user_id is None:
        raise invalid_token()
    return verified


AccessTokenDep = Annotated[VerifiedAccessToken, Depends(access_token)]
