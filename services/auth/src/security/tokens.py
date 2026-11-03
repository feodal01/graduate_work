from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal, cast
from uuid import UUID, uuid4

import jwt
from jwt.exceptions import ExpiredSignatureError, PyJWTError

TokenType = Literal['access', 'refresh']


class TokenExpiredError(Exception):
    """Срок действия JWT истёк."""


class InvalidTokenError(Exception):
    """JWT не прошёл строгую проверку."""


@dataclass(frozen=True, slots=True)
class TokenClaims:
    user_id: UUID
    token_id: UUID
    token_type: TokenType
    session_epoch: UUID
    issued_at: datetime
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class IssuedToken:
    value: str
    claims: TokenClaims


@dataclass(frozen=True, slots=True)
class IssuedTokenPair:
    access: IssuedToken
    refresh: IssuedToken


class TokenService:
    """Выпуск и строгая проверка access/refresh JWT."""

    def __init__(
        self,
        *,
        secret_key: str,
        algorithm: str,
        access_ttl_seconds: int,
        refresh_ttl_seconds: int,
    ) -> None:
        self._secret_key = secret_key
        self._algorithm = algorithm
        self._access_ttl = timedelta(seconds=access_ttl_seconds)
        self._refresh_ttl = timedelta(seconds=refresh_ttl_seconds)

    def issue_pair(
        self,
        user_id: UUID,
        session_epoch: UUID,
    ) -> IssuedTokenPair:
        issued_at = datetime.now(UTC).replace(microsecond=0)
        return IssuedTokenPair(
            access=self._issue(
                user_id,
                'access',
                session_epoch,
                issued_at,
                self._access_ttl,
            ),
            refresh=self._issue(
                user_id,
                'refresh',
                session_epoch,
                issued_at,
                self._refresh_ttl,
            ),
        )

    def decode(self, token: str, expected_type: TokenType) -> TokenClaims:
        try:
            payload = jwt.decode(
                token,
                self._secret_key,
                algorithms=[self._algorithm],
                options={
                    'require': [
                        'sub',
                        'jti',
                        'type',
                        'session_epoch',
                        'iat',
                        'exp',
                    ]
                },
            )
        except ExpiredSignatureError as exc:
            raise TokenExpiredError from exc
        except PyJWTError as exc:
            raise InvalidTokenError from exc

        try:
            token_type = payload['type']
            if token_type not in ('access', 'refresh'):
                raise ValueError('unknown token type')
            if token_type != expected_type:
                raise ValueError('unexpected token type')
            return TokenClaims(
                user_id=UUID(payload['sub']),
                token_id=UUID(payload['jti']),
                token_type=cast(TokenType, token_type),
                session_epoch=UUID(payload['session_epoch']),
                issued_at=datetime.fromtimestamp(payload['iat'], UTC),
                expires_at=datetime.fromtimestamp(payload['exp'], UTC),
            )
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise InvalidTokenError from exc

    def _issue(
        self,
        user_id: UUID,
        token_type: TokenType,
        session_epoch: UUID,
        issued_at: datetime,
        ttl: timedelta,
    ) -> IssuedToken:
        claims = TokenClaims(
            user_id=user_id,
            token_id=uuid4(),
            token_type=token_type,
            session_epoch=session_epoch,
            issued_at=issued_at,
            expires_at=issued_at + ttl,
        )
        value = jwt.encode(
            {
                'sub': str(claims.user_id),
                'jti': str(claims.token_id),
                'type': claims.token_type,
                'session_epoch': str(claims.session_epoch),
                'iat': claims.issued_at,
                'exp': claims.expires_at,
            },
            self._secret_key,
            algorithm=self._algorithm,
        )
        return IssuedToken(value=value, claims=claims)
