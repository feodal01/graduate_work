import logging
from datetime import UTC, datetime
from math import ceil
from uuid import UUID

from db.cache.errors import CacheUnavailableError, SessionEpochMismatchError
from db.cache.session import TokenSessionStore
from models import LoginHistory, User
from repositories.errors import UserNotFoundError
from repositories.login_history import LoginHistoryRepository
from repositories.users import UserRepository
from security.passwords import PasswordHasher
from security.tokens import (
    InvalidTokenError,
    IssuedTokenPair,
    TokenClaims,
    TokenExpiredError,
    TokenService,
)
from services.errors import (
    AccessTokenExpiredError,
    AccessTokenRevokedError,
    InvalidAccessTokenError,
    InvalidCredentialsError,
    InvalidRefreshTokenError,
)
from services.types import ClientInfo, CurrentUser
from validation.credentials import PasswordPolicy, validate_login
from validation.timezones import validate_timezone

logger = logging.getLogger(__name__)


class AuthenticationService:
    """Пользовательские authentication workflows без HTTP-логики."""

    def __init__(
        self,
        *,
        users: UserRepository,
        login_history: LoginHistoryRepository,
        token_sessions: TokenSessionStore,
        password_hasher: PasswordHasher,
        password_policy: PasswordPolicy,
        token_service: TokenService,
    ) -> None:
        self._users = users
        self._login_history = login_history
        self._token_sessions = token_sessions
        self._password_hasher = password_hasher
        self._password_policy = password_policy
        self._token_service = token_service

    async def signup(
        self,
        *,
        login: str,
        password: str,
        first_name: str | None,
        last_name: str | None,
        email: str | None = None,
        locale: str | None = None,
        tz: str,
    ) -> User:
        tz = validate_timezone(tz)
        valid_login = validate_login(login)
        valid_password = self._password_policy.validate(
            password,
            login=valid_login,
            user_inputs=(first_name, last_name),
        )
        return await self._users.create(
            login=valid_login,
            password_hash=self._password_hasher.hash(valid_password),
            first_name=first_name,
            last_name=last_name,
            email=email,
            locale=locale,
            tz=tz,
        )

    async def login(
        self,
        *,
        login: str,
        password: str,
        client: ClientInfo,
    ) -> IssuedTokenPair:
        user_id = await self._users.get_id_by_login(login)
        if user_id is None:
            raise InvalidCredentialsError

        session_epoch = await self._token_sessions.get_or_create_epoch(user_id)
        user = await self._users.get_by_id(user_id)
        if (
            user is None
            or user.login != login
            or user.password_hash is None
            or not self._password_hasher.verify(
                password,
                user.password_hash,
            )
        ):
            raise InvalidCredentialsError

        return await self.issue_session(
            user=user,
            client=client,
            expected_epoch=session_epoch,
        )

    async def issue_session(
        self,
        *,
        user: User,
        client: ClientInfo,
        expected_epoch: UUID | None = None,
    ) -> IssuedTokenPair:
        """Record a successful login and issue the shared Auth token pair."""

        session_epoch = expected_epoch
        if session_epoch is None:
            session_epoch = await self._token_sessions.get_or_create_epoch(user.id)
        await self._login_history.record(
            user_id=user.id,
            ip_address=client.ip_address,
            user_agent=client.user_agent,
        )
        pair = self._token_service.issue_pair(user.id, session_epoch)
        stored = await self._token_sessions.store_refresh(
            pair.refresh.claims.token_id,
            user.id,
            session_epoch,
            self._remaining_ttl(pair.refresh.claims.expires_at),
        )
        if not stored:
            raise CacheUnavailableError('Session state changed; retry authentication')
        return pair

    def verify_refresh_token(self, refresh_token: str) -> TokenClaims:
        try:
            return self._token_service.decode(refresh_token, 'refresh')
        except (TokenExpiredError, InvalidTokenError) as exc:
            raise InvalidRefreshTokenError from exc

    async def rotate_refresh(self, claims: TokenClaims) -> IssuedTokenPair:
        pair = self._token_service.issue_pair(
            claims.user_id,
            claims.session_epoch,
        )
        rotated = await self._token_sessions.rotate_refresh(
            old_token_id=claims.token_id,
            new_token_id=pair.refresh.claims.token_id,
            user_id=claims.user_id,
            session_epoch=claims.session_epoch,
            ttl=self._remaining_ttl(pair.refresh.claims.expires_at),
        )
        if not rotated:
            raise InvalidRefreshTokenError
        return pair

    async def authenticate_access(
        self,
        access_token: str,
        *,
        client: ClientInfo,
    ) -> CurrentUser:
        try:
            claims = self._token_service.decode(access_token, 'access')
        except TokenExpiredError as exc:
            raise AccessTokenExpiredError from exc
        except InvalidTokenError as exc:
            raise InvalidAccessTokenError from exc

        valid = await self._token_sessions.is_access_valid(
            user_id=claims.user_id,
            token_id=claims.token_id,
            session_epoch=claims.session_epoch,
        )
        if not valid:
            logger.warning(
                'Revoked access token used: user_id=%s token_id=%s ip=%s',
                claims.user_id,
                claims.token_id,
                client.ip_address,
            )
            raise AccessTokenRevokedError
        return CurrentUser(
            user_id=claims.user_id,
            token_id=claims.token_id,
            session_epoch=claims.session_epoch,
            expires_at=claims.expires_at,
        )

    async def update_profile(
        self,
        current_user: CurrentUser,
        *,
        login: str | None = None,
        password: str | None = None,
        contacts: dict[str, str | None] | None = None,
    ) -> User:
        if login is None and password is None and not contacts:
            raise ValueError('At least one profile field must be provided')
        user = await self._users.get_by_id(current_user.user_id)
        if user is None:
            raise InvalidAccessTokenError

        valid_login = validate_login(login) if login is not None else None
        password_hash = None
        if password is not None:
            valid_password = self._password_policy.validate(
                password,
                login=valid_login or user.login,
                user_inputs=(user.first_name, user.last_name),
            )
            password_hash = self._password_hasher.hash(valid_password)
        if password_hash is None:
            return await self._users.update(
                user,
                login=valid_login,
                password_hash=None,
                contacts=contacts,
            )

        try:
            async with self._token_sessions.password_change(
                user.id,
                current_user.session_epoch,
            ):
                return await self._users.update(
                    user,
                    login=valid_login,
                    password_hash=password_hash,
                    contacts=contacts,
                )
        except SessionEpochMismatchError as exc:
            raise AccessTokenRevokedError from exc

    async def delete_profile(self, current_user: CurrentUser) -> None:
        """Invalidate all sessions before atomically deleting the SQL profile."""
        try:
            async with self._token_sessions.password_change(
                current_user.user_id,
                current_user.session_epoch,
            ):
                await self._users.delete(current_user.user_id)
        except SessionEpochMismatchError as exc:
            raise AccessTokenRevokedError from exc
        except UserNotFoundError as exc:
            raise InvalidAccessTokenError from exc

    async def get_profile(self, current_user: CurrentUser) -> User:
        user = await self._users.get_by_id(current_user.user_id)
        if user is None:
            raise InvalidAccessTokenError
        return user

    async def get_login_history(
        self,
        current_user: CurrentUser,
    ) -> list[LoginHistory]:
        return await self._login_history.list_for_user(current_user.user_id)

    async def logout(
        self,
        current_user: CurrentUser,
        refresh_token: str,
    ) -> None:
        refresh_token_id = None
        try:
            refresh_claims = self._token_service.decode(
                refresh_token,
                'refresh',
            )
            if refresh_claims.user_id == current_user.user_id:
                refresh_token_id = refresh_claims.token_id
        except (TokenExpiredError, InvalidTokenError):
            pass

        await self._token_sessions.revoke_session(
            user_id=current_user.user_id,
            refresh_token_id=refresh_token_id,
            access_token_id=current_user.token_id,
            access_ttl=self._remaining_ttl(current_user.expires_at),
        )

    @staticmethod
    def _remaining_ttl(expires_at: datetime) -> int:
        return max(1, ceil((expires_at - datetime.now(UTC)).total_seconds()))
