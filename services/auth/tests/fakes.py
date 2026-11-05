import asyncio
from collections.abc import AsyncIterator, Collection
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid4

from db.cache.authorization import (
    AuthorizationSnapshot,
    AuthorizationSnapshotReadResult,
)
from db.cache.errors import (
    CacheUnavailableError,
    OAuthStateStoreUnavailableError,
    SessionEpochMismatchError,
)
from db.cache.oauth_state import OAuthStateData
from db.sql.errors import DatabaseUnavailableError
from models import LoginHistory, Permission, Role, User
from repositories.errors import (
    LoginAlreadyTakenError,
    RoleAlreadyAssignedError,
    RoleAlreadyExistsError,
    RoleAssignmentNotFoundError,
    RoleNotFoundError,
    UnknownPermissionError,
    UserNotFoundError,
)
from repositories.social_accounts import social_login_candidates
from services.rate_limit import RateLimitDecision, RateLimitPolicy
from services.types import SocialIdentity


class InMemoryUserRepository:
    def __init__(self) -> None:
        self.items: dict[UUID, User] = {}
        self.get_by_id_calls = 0
        self.update_calls = 0
        self.fail_update = False

    async def create(
        self,
        *,
        login: str,
        password_hash: str | None,
        first_name: str | None,
        last_name: str | None,
        is_superuser: bool = False,
        email: str | None = None,
        locale: str | None = None,
        tz: str | None = None,
    ) -> User:
        if any(user.login == login for user in self.items.values()):
            raise LoginAlreadyTakenError
        user = User(
            id=uuid4(),
            login=login,
            password_hash=password_hash,
            first_name=first_name,
            last_name=last_name,
            is_superuser=is_superuser,
            email=email,
            locale=locale,
            tz=tz,
            user_version=1,
            created_at=datetime.now(UTC),
        )
        self.items[user.id] = user
        return user

    async def get_by_login(self, login: str) -> User | None:
        return next(
            (user for user in self.items.values() if user.login == login),
            None,
        )

    async def get_id_by_login(self, login: str) -> UUID | None:
        user = await self.get_by_login(login)
        return user.id if user is not None else None

    async def get_by_id(self, user_id: UUID) -> User | None:
        self.get_by_id_calls += 1
        return self.items.get(user_id)

    async def promote_to_superuser(self, user: User) -> User:
        user.is_superuser = True
        return user

    async def update(
        self,
        user: User,
        *,
        login: str | None,
        password_hash: str | None,
        contacts: dict[str, str | None] | None = None,
    ) -> User:
        self.update_calls += 1
        if self.fail_update:
            raise DatabaseUnavailableError
        if login is not None and any(
            item.id != user.id and item.login == login for item in self.items.values()
        ):
            raise LoginAlreadyTakenError
        if login is not None:
            user.login = login
        if password_hash is not None:
            user.password_hash = password_hash
        if contacts and any(
            getattr(user, field) != value for field, value in contacts.items()
        ):
            for field, value in contacts.items():
                setattr(user, field, value)
            user.user_version += 1
        return user

    async def delete(self, user_id: UUID) -> None:
        if self.fail_update:
            raise DatabaseUnavailableError
        if self.items.pop(user_id, None) is None:
            raise UserNotFoundError


class InMemoryRoleRepository:
    def __init__(self) -> None:
        self.items: dict[UUID, Role] = {}
        self.assignments: set[tuple[UUID, UUID]] = set()
        self.assignment_expirations: dict[tuple[UUID, UUID], datetime | None] = {}
        self.access_versions: dict[UUID, int] = {}
        self.fail_db: bool = False
        self.users: set[UUID] = set()
        self.permission_catalog = {
            'roles:manage',
            'user_roles:manage',
            'films.early_access:read',
        }
        self.get_permissions_calls = 0

    async def list(self) -> list[Role]:
        return sorted(self.items.values(), key=lambda role: role.name)

    async def create(
        self,
        *,
        name: str,
        permission_codes: Collection[str],
    ) -> Role:
        if any(role.name == name for role in self.items.values()):
            raise RoleAlreadyExistsError
        codes = set(permission_codes)
        unknown = codes - self.permission_catalog
        if unknown:
            raise UnknownPermissionError(', '.join(sorted(unknown)))
        role = Role(
            id=uuid4(),
            name=name,
            created_at=datetime.now(UTC),
            permissions=[Permission(code=code) for code in codes],
        )
        self.items[role.id] = role
        return role

    async def update(
        self,
        role_id: UUID,
        *,
        name: str | None,
        permission_codes: Collection[str] | None,
    ) -> Role:
        role = self.items.get(role_id)
        if role is None:
            raise RoleNotFoundError
        if name is not None and any(
            item.id != role_id and item.name == name for item in self.items.values()
        ):
            raise RoleAlreadyExistsError
        if permission_codes is not None:
            codes = set(permission_codes)
            unknown = codes - self.permission_catalog
            if unknown:
                raise UnknownPermissionError(', '.join(sorted(unknown)))
            role.permissions = [Permission(code=code) for code in codes]
        if name is not None:
            role.name = name
        return role

    async def delete(self, role_id: UUID) -> None:
        if self.items.pop(role_id, None) is None:
            raise RoleNotFoundError
        self.assignments = {
            assignment for assignment in self.assignments if assignment[1] != role_id
        }

    async def assign(self, user_id: UUID, role_id: UUID) -> None:
        if user_id not in self.users:
            raise UserNotFoundError
        if role_id not in self.items:
            raise RoleNotFoundError
        assignment = (user_id, role_id)
        if assignment in self.assignments:
            raise RoleAlreadyAssignedError
        self.assignments.add(assignment)

    async def revoke(self, user_id: UUID, role_id: UUID) -> None:
        if user_id not in self.users:
            raise UserNotFoundError
        if role_id not in self.items:
            raise RoleNotFoundError
        assignment = (user_id, role_id)
        if assignment not in self.assignments:
            raise RoleAssignmentNotFoundError
        self.assignments.remove(assignment)
        self.assignment_expirations.pop(assignment, None)

    async def set_subscription_access(
        self,
        user_id: UUID,
        *,
        version: int,
        expires_at: datetime | None,
    ) -> tuple[bool, int]:
        if self.fail_db:
            raise DatabaseUnavailableError('Database is down')
        if user_id not in self.users:
            raise UserNotFoundError
        subscriber_role = next(
            (role for role in self.items.values() if role.name == 'subscriber'),
            None,
        )
        if subscriber_role is None:
            raise RoleNotFoundError
        current_version = self.access_versions.get(user_id)
        if current_version is not None and version <= current_version:
            return False, current_version

        self.access_versions[user_id] = version
        assignment = (user_id, subscriber_role.id)
        if expires_at is not None:
            self.assignments.add(assignment)
            self.assignment_expirations[assignment] = expires_at
        else:
            self.assignments.discard(assignment)
            self.assignment_expirations.pop(assignment, None)
        return True, version

    async def get_permissions_for_user(self, user_id: UUID) -> set[str]:
        self.get_permissions_calls += 1
        role_ids = {
            role_id
            for assigned_user_id, role_id in self.assignments
            if assigned_user_id == user_id
        }
        return {
            permission.code
            for role_id in role_ids
            for permission in self.items[role_id].permissions
        }


class InMemoryAuthorizationCache:
    def __init__(self) -> None:
        self.items: dict[UUID, tuple[str, AuthorizationSnapshot]] = {}
        self.roles_revision = str(uuid4())
        self.user_markers: set[UUID] = set()
        self.roles_changing = False
        self.fail_reads = False
        self.fail_writes = False
        self.fail_mutations = False
        self.user_mutations: list[UUID] = []
        self.roles_mutations = 0

    async def read_snapshot(
        self,
        user_id: UUID,
    ) -> AuthorizationSnapshotReadResult:
        if self.fail_reads:
            raise CacheUnavailableError
        if self.roles_changing or user_id in self.user_markers:
            return AuthorizationSnapshotReadResult(None, self.roles_revision)
        item = self.items.get(user_id)
        if item is None or item[0] != self.roles_revision:
            return AuthorizationSnapshotReadResult(None, self.roles_revision)
        return AuthorizationSnapshotReadResult(item[1], self.roles_revision)

    async def store_snapshot(
        self,
        user_id: UUID,
        snapshot: AuthorizationSnapshot,
        expected_roles_revision: str,
    ) -> None:
        if self.fail_writes:
            raise CacheUnavailableError
        if (
            not self.roles_changing
            and user_id not in self.user_markers
            and expected_roles_revision == self.roles_revision
        ):
            self.items[user_id] = (expected_roles_revision, snapshot)

    @asynccontextmanager
    async def user_mutation(self, user_id: UUID) -> AsyncIterator[None]:
        if self.fail_mutations or user_id in self.user_markers:
            raise CacheUnavailableError
        self.user_markers.add(user_id)
        self.user_mutations.append(user_id)
        try:
            yield
        finally:
            self.items.pop(user_id, None)
            self.user_markers.remove(user_id)

    @asynccontextmanager
    async def roles_mutation(self) -> AsyncIterator[None]:
        if self.fail_mutations or self.roles_changing:
            raise CacheUnavailableError
        self.roles_changing = True
        self.roles_mutations += 1
        succeeded = False
        try:
            yield
            succeeded = True
        finally:
            if succeeded:
                self.roles_revision = str(uuid4())
            self.roles_changing = False


class InMemoryLoginHistoryRepository:
    def __init__(self, *, events: list[str] | None = None) -> None:
        self.items: list[LoginHistory] = []
        self._events = events

    async def record(
        self,
        *,
        user_id: UUID,
        ip_address: str | None,
        user_agent: str | None,
    ) -> LoginHistory:
        if self._events is not None:
            self._events.append('record_login_history')
        history = LoginHistory(
            id=uuid4(),
            user_id=user_id,
            logged_at=datetime.now(UTC),
            ip_address=ip_address,
            user_agent=user_agent,
        )
        self.items.append(history)
        return history

    async def list_for_user(self, user_id: UUID) -> list[LoginHistory]:
        return sorted(
            (item for item in self.items if item.user_id == user_id),
            key=lambda item: item.logged_at,
            reverse=True,
        )


class InMemoryTokenSessionStore:
    def __init__(self, *, events: list[str] | None = None) -> None:
        self.refresh: dict[UUID, UUID] = {}
        self.refresh_epochs: dict[UUID, UUID] = {}
        self.refresh_ttls: dict[UUID, int] = {}
        self.revoked_access: dict[UUID, int] = {}
        self.epochs: dict[UUID, UUID] = {}
        self.markers: set[UUID] = set()
        self.reject_store = False
        self.fail_password_change = False
        self.fail_password_change_finish = False
        self._lock = asyncio.Lock()
        self._events = events

    async def get_or_create_epoch(self, user_id: UUID) -> UUID:
        if self._events is not None:
            self._events.append('get_or_create_epoch')
        async with self._lock:
            if user_id in self.markers:
                raise CacheUnavailableError('Session state is changing')
            return self.epochs.setdefault(user_id, uuid4())

    async def store_refresh(
        self,
        token_id: UUID,
        user_id: UUID,
        session_epoch: UUID,
        ttl: int,
    ) -> bool:
        if self._events is not None:
            self._events.append('store_refresh')
        async with self._lock:
            if (
                self.reject_store
                or user_id in self.markers
                or self.epochs.get(user_id) != session_epoch
            ):
                return False
            self.refresh[token_id] = user_id
            self.refresh_epochs[token_id] = session_epoch
            self.refresh_ttls[token_id] = ttl
            return True

    async def rotate_refresh(
        self,
        *,
        old_token_id: UUID,
        new_token_id: UUID,
        user_id: UUID,
        session_epoch: UUID,
        ttl: int,
    ) -> bool:
        async with self._lock:
            if user_id in self.markers or self.epochs.get(user_id) != session_epoch:
                return False
            current_user = self.refresh.get(old_token_id)
            if current_user is not None:
                if (
                    current_user != user_id
                    or self.refresh_epochs.get(old_token_id) != session_epoch
                ):
                    return False
                self.refresh.pop(old_token_id)
                self.refresh_epochs.pop(old_token_id, None)
                self.refresh_ttls.pop(old_token_id, None)
                self.refresh[new_token_id] = user_id
                self.refresh_epochs[new_token_id] = session_epoch
                self.refresh_ttls[new_token_id] = ttl
                return True
            return (
                self.refresh.get(new_token_id) == user_id
                and self.refresh_epochs.get(new_token_id) == session_epoch
            )

    async def is_access_valid(
        self,
        *,
        user_id: UUID,
        token_id: UUID,
        session_epoch: UUID,
    ) -> bool:
        return (
            user_id not in self.markers
            and self.epochs.get(user_id) == session_epoch
            and token_id not in self.revoked_access
        )

    @asynccontextmanager
    async def password_change(
        self,
        user_id: UUID,
        expected_epoch: UUID,
    ) -> AsyncIterator[None]:
        async with self._lock:
            if self.fail_password_change:
                raise CacheUnavailableError('Session state is unavailable')
            if self.epochs.get(user_id) != expected_epoch:
                raise SessionEpochMismatchError
            if user_id in self.markers:
                raise CacheUnavailableError('Password change is already in progress')
            self.markers.add(user_id)
            self.epochs[user_id] = uuid4()
        try:
            yield
        except BaseException:
            if not self.fail_password_change_finish:
                self.markers.discard(user_id)
            raise
        else:
            if self.fail_password_change_finish:
                raise CacheUnavailableError(
                    'Password change session finalization failed'
                )
            self.epochs[user_id] = uuid4()
            self.markers.discard(user_id)

    async def revoke_session(
        self,
        *,
        user_id: UUID,
        refresh_token_id: UUID | None,
        access_token_id: UUID,
        access_ttl: int,
    ) -> None:
        if refresh_token_id is not None:
            self.refresh.pop(refresh_token_id, None)
            self.refresh_epochs.pop(refresh_token_id, None)
            self.refresh_ttls.pop(refresh_token_id, None)
        self.revoked_access[access_token_id] = access_ttl


@dataclass(frozen=True, slots=True)
class SavedOAuthState:
    provider: str
    state: str
    code_verifier: str
    tz: str
    ttl_seconds: int


class InMemoryOAuthStateStore:
    def __init__(self, *, events: list[str] | None = None) -> None:
        self.items: dict[tuple[str, str], OAuthStateData] = {}
        self.saved: list[SavedOAuthState] = []
        self.reject_saves = 0
        self.fail_save = False
        self.fail_consume = False
        self._events = events

    async def save(
        self,
        *,
        provider: str,
        state: str,
        code_verifier: str,
        tz: str,
        ttl_seconds: int,
    ) -> bool:
        if self.fail_save:
            raise OAuthStateStoreUnavailableError
        saved = SavedOAuthState(
            provider=provider,
            state=state,
            code_verifier=code_verifier,
            tz=tz,
            ttl_seconds=ttl_seconds,
        )
        self.saved.append(saved)
        if self.reject_saves > 0:
            self.reject_saves -= 1
            return False
        key = (provider, state)
        if key in self.items:
            return False
        self.items[key] = OAuthStateData(code_verifier=code_verifier, tz=tz)
        return True

    async def consume(
        self,
        *,
        provider: str,
        state: str,
    ) -> OAuthStateData | None:
        if self._events is not None:
            self._events.append('consume_state')
        if self.fail_consume:
            raise OAuthStateStoreUnavailableError
        return self.items.pop((provider, state), None)


class FakeSocialProviderClient:
    def __init__(
        self,
        *,
        identity: SocialIdentity,
        events: list[str] | None = None,
    ) -> None:
        self.identity = identity
        self.authorization_calls: list[tuple[str, str]] = []
        self.exchange_calls: list[tuple[str, str]] = []
        self.identity_calls: list[str] = []
        self._events = events

    def build_authorization_url(
        self,
        *,
        state: str,
        code_challenge: str,
    ) -> str:
        self.authorization_calls.append((state, code_challenge))
        return f'https://provider.example/login?state={state}'

    async def exchange_code(
        self,
        *,
        code: str,
        code_verifier: str,
    ) -> str:
        if self._events is not None:
            self._events.append('exchange_code')
        self.exchange_calls.append((code, code_verifier))
        return 'provider-access-token'

    async def get_identity(self, access_token: str) -> SocialIdentity:
        if self._events is not None:
            self._events.append('get_identity')
        self.identity_calls.append(access_token)
        return self.identity


class InMemorySocialAccountRepository:
    def __init__(
        self,
        users: InMemoryUserRepository,
        *,
        events: list[str] | None = None,
    ) -> None:
        self._users = users
        self._links: dict[tuple[str, str], UUID] = {}
        self._events = events
        self.calls: list[SocialIdentity] = []
        self._last_user: User | None = None

    @property
    def last_user(self) -> User:
        if self._last_user is None:
            raise RuntimeError('No social user has been created')
        return self._last_user

    async def get_or_create_user(self, identity: SocialIdentity, *, tz: str) -> User:
        if self._events is not None:
            self._events.append('get_or_create_user')
        self.calls.append(identity)
        key = (identity.provider, identity.provider_user_id)
        user_id = self._links.get(key)
        if user_id is not None:
            user = self._users.items[user_id]
            await self._users.update(
                user,
                login=None,
                password_hash=None,
                contacts={
                    'first_name': identity.first_name,
                    'last_name': identity.last_name,
                },
            )
            self._last_user = user
            return user

        for login in social_login_candidates(identity):
            try:
                user = await self._users.create(
                    login=login,
                    password_hash=None,
                    email=identity.email,
                    first_name=identity.first_name,
                    last_name=identity.last_name,
                    tz=tz,
                )
            except LoginAlreadyTakenError:
                continue
            self._links[key] = user.id
            self._last_user = user
            return user
        raise LoginAlreadyTakenError


class ProgrammableRateLimiter:
    def __init__(
        self,
        *,
        decision: RateLimitDecision | None = None,
        error: Exception | None = None,
    ) -> None:
        self.decision = decision or RateLimitDecision(
            allowed=True,
            remaining=1,
            retry_after_seconds=0,
        )
        self.error = error
        self.calls: list[tuple[RateLimitPolicy, str]] = []

    async def check(
        self,
        *,
        policy: RateLimitPolicy,
        identity: str,
    ) -> RateLimitDecision:
        """Record the input and return the configured backend result."""
        self.calls.append((policy, identity))
        if self.error is not None:
            raise self.error
        return self.decision
