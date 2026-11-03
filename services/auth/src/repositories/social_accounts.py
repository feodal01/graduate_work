import secrets
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from db.sql.errors import DatabaseUnavailableError
from models import SocialAccount, User
from services.profile_events import (
    apply_contact_changes,
    create_profile_event,
    lock_profile,
)
from services.types import SocialIdentity
from validation.timezones import validate_timezone

_MAX_LOGIN_LENGTH = 255
_RANDOM_LOGIN_ATTEMPTS = 3


class SocialAccountRepository(Protocol):
    async def get_or_create_user(
        self, identity: SocialIdentity, *, tz: str
    ) -> User: ...


def social_login_candidates(identity: SocialIdentity) -> tuple[str, ...]:
    preferred = identity.email or identity.provider_login
    technical = _technical_login(identity)
    if preferred == technical:
        return (preferred,)
    return (preferred, technical)


def _technical_login(identity: SocialIdentity) -> str:
    return f'{identity.provider}_{identity.provider_user_id}'[:_MAX_LOGIN_LENGTH]


class SQLAlchemySocialAccountRepository:
    """Store the stable provider identity without linking by email."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_or_create_user(self, identity: SocialIdentity, *, tz: str) -> User:
        tz = validate_timezone(tz)
        try:
            account = await self._find(identity)
            if account is not None:
                return await self._update(account, identity)

            for login in self._login_attempts(identity):
                created = await self._try_create(identity, login, tz)
                if created is not None:
                    return created
        except (SQLAlchemyError, OSError) as exc:
            await self._session.rollback()
            raise DatabaseUnavailableError from exc

        except BaseException:
            await self._session.rollback()
            raise

        raise DatabaseUnavailableError('Could not allocate a social login')

    async def _find(
        self,
        identity: SocialIdentity,
    ) -> SocialAccount | None:
        result = await self._session.execute(
            select(SocialAccount)
            .options(selectinload(SocialAccount.user))
            .where(
                SocialAccount.provider == identity.provider,
                SocialAccount.provider_user_id == identity.provider_user_id,
            )
        )
        return result.scalar_one_or_none()

    async def _update(
        self,
        account: SocialAccount,
        identity: SocialIdentity,
    ) -> User:
        user = await lock_profile(self._session, account.user_id)
        account.email = identity.email
        await apply_contact_changes(
            self._session,
            user,
            {
                'first_name': identity.first_name,
                'last_name': identity.last_name,
            },
        )
        await self._session.commit()
        return user

    async def _try_create(
        self,
        identity: SocialIdentity,
        login: str,
        tz: str,
    ) -> User | None:
        user = User(
            login=login,
            password_hash=None,
            tz=tz,
            email=identity.email,
            first_name=identity.first_name,
            last_name=identity.last_name,
            is_superuser=False,
            roles=[],
        )
        user.social_accounts.append(
            SocialAccount(
                provider=identity.provider,
                provider_user_id=identity.provider_user_id,
                email=identity.email,
            )
        )
        self._session.add(user)
        try:
            await create_profile_event(self._session, user)
            await self._session.commit()
        except IntegrityError as exc:
            await self._session.rollback()
            if getattr(exc.orig, 'sqlstate', None) != '23505':
                raise
            account = await self._find(identity)
            if account is not None:
                return await self._update(account, identity)
            return None
        await self._session.refresh(user)
        return user

    @staticmethod
    def _login_attempts(identity: SocialIdentity) -> tuple[str, ...]:
        candidates = list(social_login_candidates(identity))
        technical = _technical_login(identity)
        for _ in range(_RANDOM_LOGIN_ATTEMPTS):
            suffix = secrets.token_hex(4)
            prefix = technical[: _MAX_LOGIN_LENGTH - len(suffix) - 1]
            candidates.append(f'{prefix}_{suffix}')
        return tuple(candidates)
