from collections.abc import Collection
from datetime import UTC, date, datetime
from uuid import UUID, uuid4

import pytest

from db.cache.errors import CacheUnavailableError
from services import film_access
from services.authorization import (
    FILM_EARLY_ACCESS_READ,
    ROLES_MANAGE,
    USER_ROLES_MANAGE,
    AuthorizationService,
)
from services.errors import PermissionDeniedError
from services.film_access import (
    FilmAccessContext,
    FilmAccessPolicy,
    FilmAccessRule,
    RecentFilmAccessRule,
)
from services.types import CurrentUser
from tests.fakes import (
    InMemoryAuthorizationCache,
    InMemoryRoleRepository,
    InMemoryUserRepository,
)


def current_user(user_id: UUID) -> CurrentUser:
    """Создаёт контекст текущего пользователя с действующим токеном."""
    return CurrentUser(
        user_id=user_id,
        token_id=uuid4(),
        session_epoch=uuid4(),
        expires_at=datetime(2026, 8, 15, 12, tzinfo=UTC),
    )


def test_film_policy_uses_three_calendar_year_boundary() -> None:
    """Фильм становится публичным ровно через три календарных года."""
    rule = RecentFilmAccessRule(today=lambda: date(2026, 8, 15))

    assert (
        rule.required_permissions(FilmAccessContext(creation_date=date(2023, 8, 14)))
        == frozenset()
    )
    assert (
        rule.required_permissions(FilmAccessContext(creation_date=date(2023, 8, 15)))
        == frozenset()
    )
    assert rule.required_permissions(
        FilmAccessContext(creation_date=date(2023, 8, 16))
    ) == frozenset({FILM_EARLY_ACCESS_READ})
    assert rule.required_permissions(
        FilmAccessContext(creation_date=date(2027, 1, 1))
    ) == frozenset({FILM_EARLY_ACCESS_READ})


def test_film_policy_handles_leap_day_as_calendar_years() -> None:
    """Правило трёх лет корректно обрабатывает високосный день."""
    rule = RecentFilmAccessRule(today=lambda: date(2024, 2, 29))

    assert (
        rule.required_permissions(FilmAccessContext(creation_date=date(2021, 2, 28)))
        == frozenset()
    )
    assert rule.required_permissions(
        FilmAccessContext(creation_date=date(2021, 3, 1))
    ) == frozenset({FILM_EARLY_ACCESS_READ})


def test_film_policy_requires_early_access_for_unknown_date() -> None:
    """An unknown date does not prove that a film is public."""
    rule = RecentFilmAccessRule(today=lambda: date(2026, 8, 15))

    assert rule.required_permissions(
        FilmAccessContext(creation_date=None)
    ) == frozenset({FILM_EARLY_ACCESS_READ})


def test_default_film_access_policy_enables_recent_film_rule() -> None:
    """Политика по умолчанию включает правило раннего доступа."""
    policy = film_access.create_film_access_policy()

    assert policy.required_permissions(
        FilmAccessContext(creation_date=date(9999, 1, 1))
    ) == frozenset({FILM_EARLY_ACCESS_READ})


class StaticPermissionRule:
    def __init__(self, permission: str) -> None:
        self._permission = permission

    def required_permissions(
        self,
        context: FilmAccessContext,
    ) -> frozenset[str]:
        """Возвращает фиксированное разрешение для любого фильма."""
        del context
        return frozenset({self._permission})


def test_film_policy_unions_permissions_from_all_rules() -> None:
    """Политика объединяет разрешения всех сработавших правил."""
    policy = FilmAccessPolicy(
        rules=(
            StaticPermissionRule('films.first:read'),
            StaticPermissionRule('films.second:read'),
        )
    )

    assert policy.required_permissions(
        FilmAccessContext(creation_date=date(2020, 1, 1))
    ) == frozenset({'films.first:read', 'films.second:read'})


@pytest.fixture
def repositories() -> tuple[InMemoryUserRepository, InMemoryRoleRepository]:
    """Создаёт изолированные репозитории пользователей и ролей."""
    return InMemoryUserRepository(), InMemoryRoleRepository()


def service(
    users: InMemoryUserRepository,
    roles: InMemoryRoleRepository,
    *,
    extra_rules: Collection[FilmAccessRule] = (),
    authorization_cache: InMemoryAuthorizationCache | None = None,
) -> AuthorizationService:
    """Собирает сервис авторизации из тестовых зависимостей."""
    return AuthorizationService(
        users=users,
        roles=roles,
        authorization_cache=(authorization_cache or InMemoryAuthorizationCache()),
        film_policy=FilmAccessPolicy(
            rules=(
                RecentFilmAccessRule(today=lambda: date(2026, 8, 15)),
                *extra_rules,
            )
        ),
    )


@pytest.mark.asyncio
async def test_public_film_allows_anonymous_but_early_film_does_not(
    repositories: tuple[InMemoryUserRepository, InMemoryRoleRepository],
) -> None:
    """Анониму доступен публичный, но не новый фильм."""
    users, roles = repositories
    authorization = service(users, roles)

    assert (
        await authorization.can_read_film(
            None,
            FilmAccessContext(creation_date=date(2023, 8, 15)),
        )
        is True
    )
    assert (
        await authorization.can_read_film(
            None,
            FilmAccessContext(creation_date=date(2023, 8, 16)),
        )
        is False
    )


@pytest.mark.asyncio
async def test_subscriber_permission_allows_early_film(
    repositories: tuple[InMemoryUserRepository, InMemoryRoleRepository],
) -> None:
    """Разрешение подписчика открывает новый фильм."""
    users, roles = repositories
    user = await users.create(
        login='Alice',
        password_hash='hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    roles.users.add(user.id)
    subscriber = await roles.create(
        name='subscriber',
        permission_codes={FILM_EARLY_ACCESS_READ},
    )
    await roles.assign(user.id, subscriber.id)

    assert (
        await service(users, roles).can_read_film(
            current_user(user.id),
            FilmAccessContext(creation_date=date(2023, 8, 16)),
        )
        is True
    )


@pytest.mark.asyncio
async def test_unrelated_role_does_not_allow_early_film(
    repositories: tuple[InMemoryUserRepository, InMemoryRoleRepository],
) -> None:
    """Посторонняя роль не даёт ранний доступ к фильму."""
    users, roles = repositories
    user = await users.create(
        login='Alice',
        password_hash='hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    roles.users.add(user.id)
    viewer = await roles.create(name='viewer', permission_codes=set())
    await roles.assign(user.id, viewer.id)

    assert (
        await service(users, roles).can_read_film(
            current_user(user.id),
            FilmAccessContext(creation_date=date(2023, 8, 16)),
        )
        is False
    )


@pytest.mark.asyncio
async def test_superuser_bypasses_film_permission(
    repositories: tuple[InMemoryUserRepository, InMemoryRoleRepository],
) -> None:
    """Суперпользователь проходит проверку без отдельного разрешения."""
    users, roles = repositories
    user = await users.create(
        login='Root',
        password_hash='hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    user.is_superuser = True

    assert (
        await service(users, roles).can_read_film(
            current_user(user.id),
            FilmAccessContext(creation_date=date(2026, 8, 15)),
        )
        is True
    )


@pytest.mark.asyncio
async def test_film_access_requires_every_permission_from_matching_rules(
    repositories: tuple[InMemoryUserRepository, InMemoryRoleRepository],
) -> None:
    """Доступ требует все разрешения, запрошенные правилами."""
    users, roles = repositories
    additional_permission = 'films.country_access:read'
    roles.permission_catalog.add(additional_permission)
    user = await users.create(
        login='Alice',
        password_hash='hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    roles.users.add(user.id)
    early_access = await roles.create(
        name='subscriber',
        permission_codes={FILM_EARLY_ACCESS_READ},
    )
    country_access = await roles.create(
        name='country-access',
        permission_codes={additional_permission},
    )
    await roles.assign(user.id, early_access.id)
    cache = InMemoryAuthorizationCache()
    authorization = service(
        users,
        roles,
        extra_rules=(StaticPermissionRule(additional_permission),),
        authorization_cache=cache,
    )
    context = FilmAccessContext(creation_date=date(2023, 8, 16))

    assert (
        await authorization.can_read_film(
            current_user(user.id),
            context,
        )
        is False
    )

    async with cache.user_mutation(user.id):
        await roles.assign(user.id, country_access.id)

    assert (
        await authorization.can_read_film(
            current_user(user.id),
            context,
        )
        is True
    )


@pytest.mark.asyncio
async def test_role_management_requires_stable_permissions(
    repositories: tuple[InMemoryUserRepository, InMemoryRoleRepository],
) -> None:
    """Управление ролями разрешено только с нужными правами."""
    users, roles = repositories
    user = await users.create(
        login='Admin',
        password_hash='hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    roles.users.add(user.id)
    admin = await roles.create(
        name='admin',
        permission_codes={ROLES_MANAGE, USER_ROLES_MANAGE},
    )
    await roles.assign(user.id, admin.id)
    authorization = service(users, roles)

    created = await authorization.create_role(
        current_user(user.id),
        name='subscriber',
        permission_codes={FILM_EARLY_ACCESS_READ},
    )
    assert [
        role.id for role in await authorization.list_roles(current_user(user.id))
    ] == [admin.id, created.id]


@pytest.mark.asyncio
async def test_user_without_management_permission_is_forbidden(
    repositories: tuple[InMemoryUserRepository, InMemoryRoleRepository],
) -> None:
    """Пользователь без прав управления получает запрет."""
    users, roles = repositories
    user = await users.create(
        login='Alice',
        password_hash='hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )

    with pytest.raises(PermissionDeniedError):
        await service(users, roles).list_roles(current_user(user.id))


@pytest.mark.asyncio
async def test_superuser_can_update_delete_assign_and_revoke_roles(
    repositories: tuple[InMemoryUserRepository, InMemoryRoleRepository],
) -> None:
    """Суперпользователь управляет ролями и их назначениями."""
    users, roles = repositories
    root = await users.create(
        login='Root',
        password_hash='hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    root.is_superuser = True
    target = await users.create(
        login='Alice',
        password_hash='hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    roles.users.update({root.id, target.id})
    authorization = service(users, roles)

    role = await authorization.create_role(
        current_user(root.id),
        name='viewer',
        permission_codes=set(),
    )
    updated = await authorization.update_role(
        current_user(root.id),
        role.id,
        name='subscriber',
        permission_codes={FILM_EARLY_ACCESS_READ},
    )
    assert updated.name == 'subscriber'

    await authorization.assign_role(current_user(root.id), target.id, role.id)
    assert (
        await authorization.can_read_film(
            current_user(target.id),
            FilmAccessContext(creation_date=date(2023, 8, 16)),
        )
        is True
    )
    await authorization.revoke_role(current_user(root.id), target.id, role.id)
    assert (
        await authorization.can_read_film(
            current_user(target.id),
            FilmAccessContext(creation_date=date(2023, 8, 16)),
        )
        is False
    )

    await authorization.delete_role(current_user(root.id), role.id)
    assert await authorization.list_roles(current_user(root.id)) == []


@pytest.mark.asyncio
async def test_role_manager_without_assignment_permission_cannot_assign(
    repositories: tuple[InMemoryUserRepository, InMemoryRoleRepository],
) -> None:
    """Право редактировать роли не разрешает назначать их людям."""
    users, roles = repositories
    manager = await users.create(
        login='Manager',
        password_hash='hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    target = await users.create(
        login='Alice',
        password_hash='hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    roles.users.update({manager.id, target.id})
    manager_role = await roles.create(
        name='role-manager',
        permission_codes={ROLES_MANAGE},
    )
    target_role = await roles.create(name='viewer', permission_codes=set())
    await roles.assign(manager.id, manager_role.id)

    with pytest.raises(PermissionDeniedError):
        await service(users, roles).assign_role(
            current_user(manager.id),
            target.id,
            target_role.id,
        )


@pytest.mark.asyncio
async def test_authorization_cache_hit_avoids_repeated_repository_reads(
    repositories: tuple[InMemoryUserRepository, InMemoryRoleRepository],
) -> None:
    """Повторная проверка берёт права из кэша без чтения PostgreSQL."""
    users, roles = repositories
    cache = InMemoryAuthorizationCache()
    user = await users.create(
        login='Alice',
        password_hash='hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    roles.users.add(user.id)
    subscriber = await roles.create(
        name='subscriber',
        permission_codes={FILM_EARLY_ACCESS_READ},
    )
    await roles.assign(user.id, subscriber.id)
    authorization = service(users, roles, authorization_cache=cache)

    assert (
        await authorization.has_permission(
            current_user(user.id),
            FILM_EARLY_ACCESS_READ,
        )
        is True
    )
    first_user_reads = users.get_by_id_calls
    first_permission_reads = roles.get_permissions_calls

    assert (
        await authorization.has_permission(
            current_user(user.id),
            FILM_EARLY_ACCESS_READ,
        )
        is True
    )
    assert users.get_by_id_calls == first_user_reads
    assert roles.get_permissions_calls == first_permission_reads


@pytest.mark.asyncio
async def test_authorization_read_falls_back_to_repositories_when_cache_fails(
    repositories: tuple[InMemoryUserRepository, InMemoryRoleRepository],
) -> None:
    """Ошибка чтения Redis переводит проверку прав на PostgreSQL."""
    users, roles = repositories
    cache = InMemoryAuthorizationCache()
    cache.fail_reads = True
    user = await users.create(
        login='Root',
        password_hash='hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    user.is_superuser = True

    assert (
        await service(
            users,
            roles,
            authorization_cache=cache,
        ).has_permission(current_user(user.id), 'anything')
        is True
    )
    assert users.get_by_id_calls == 1


@pytest.mark.asyncio
async def test_assign_role_requires_cache_marker_before_repository_mutation(
    repositories: tuple[InMemoryUserRepository, InMemoryRoleRepository],
) -> None:
    """Роль не назначается, если защита инвалидации недоступна."""
    users, roles = repositories
    cache = InMemoryAuthorizationCache()
    root = await users.create(
        login='Root',
        password_hash='hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    root.is_superuser = True
    target = await users.create(
        login='Alice',
        password_hash='hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    roles.users.update({root.id, target.id})
    role = await roles.create(name='viewer', permission_codes=set())
    cache.fail_mutations = True

    with pytest.raises(CacheUnavailableError):
        await service(
            users,
            roles,
            authorization_cache=cache,
        ).assign_role(current_user(root.id), target.id, role.id)

    assert (target.id, role.id) not in roles.assignments


@pytest.mark.asyncio
async def test_role_name_update_does_not_change_global_revision(
    repositories: tuple[InMemoryUserRepository, InMemoryRoleRepository],
) -> None:
    """Переименование роли не меняет ревизию эффективных прав."""
    users, roles = repositories
    cache = InMemoryAuthorizationCache()
    root = await users.create(
        login='Root',
        password_hash='hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    root.is_superuser = True
    role = await roles.create(name='viewer', permission_codes=set())
    authorization = service(users, roles, authorization_cache=cache)

    await authorization.update_role(
        current_user(root.id),
        role.id,
        name='reader',
        permission_codes=None,
    )

    assert cache.roles_mutations == 0


@pytest.mark.asyncio
async def test_role_permissions_update_changes_global_revision(
    repositories: tuple[InMemoryUserRepository, InMemoryRoleRepository],
) -> None:
    """Изменение разрешений роли создаёт новую ревизию."""
    users, roles = repositories
    cache = InMemoryAuthorizationCache()
    root = await users.create(
        login='Root',
        password_hash='hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    root.is_superuser = True
    role = await roles.create(name='viewer', permission_codes=set())
    authorization = service(users, roles, authorization_cache=cache)

    old_revision = cache.roles_revision
    await authorization.update_role(
        current_user(root.id),
        role.id,
        name=None,
        permission_codes={FILM_EARLY_ACCESS_READ},
    )

    assert cache.roles_mutations == 1
    assert cache.roles_revision != old_revision
