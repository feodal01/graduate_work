import pytest

from db.cache.errors import CacheUnavailableError
from security.passwords import PasswordHasher
from services.superusers import SuperuserService
from tests.fakes import InMemoryAuthorizationCache, InMemoryUserRepository
from validation.credentials import PasswordPolicy


@pytest.fixture
def users() -> InMemoryUserRepository:
    """Создаёт изолированный репозиторий пользователей."""
    return InMemoryUserRepository()


def service(
    users: InMemoryUserRepository,
    cache: InMemoryAuthorizationCache | None = None,
) -> SuperuserService:
    """Собирает сервис суперпользователей из тестовых зависимостей."""
    return SuperuserService(
        users=users,
        authorization_cache=cache or InMemoryAuthorizationCache(),
        password_hasher=PasswordHasher(),
        password_policy=PasswordPolicy(min_length=8, min_score=3),
    )


@pytest.mark.asyncio
async def test_create_superuser_hashes_password_and_sets_bypass_flag(
    users: InMemoryUserRepository,
) -> None:
    """Новый суперпользователь получает hash пароля и флаг обхода RBAC."""
    user = await service(users).create_or_promote(
        login='Root', password='correct horse', tz='Europe/Moscow'
    )

    assert user.login == 'Root'
    assert user.is_superuser is True
    assert user.password_hash is not None
    assert user.password_hash != 'correct horse'
    assert PasswordHasher().verify('correct horse', user.password_hash) is True


@pytest.mark.asyncio
async def test_existing_user_is_promoted_without_replacing_password(
    users: InMemoryUserRepository,
) -> None:
    """Повышение пользователя сохраняет его текущий пароль."""
    existing = await users.create(
        login='Alice',
        password_hash='existing-hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    cache = InMemoryAuthorizationCache()

    promoted = await service(users, cache).create_or_promote(
        login='Alice', password='different horse', tz='Europe/Moscow'
    )

    assert promoted.id == existing.id
    assert promoted.is_superuser is True
    assert promoted.password_hash == 'existing-hash'
    assert cache.user_mutations == [existing.id]


@pytest.mark.asyncio
async def test_repeated_superuser_bootstrap_is_idempotent(
    users: InMemoryUserRepository,
) -> None:
    """Повторное создание суперпользователя не создаёт дубликат."""
    first = await service(users).create_or_promote(
        login='Root', password='correct horse', tz='Europe/Moscow'
    )
    second = await service(users).create_or_promote(
        login='Root', password='ignored horse', tz='Asia/Tokyo'
    )

    assert second.id == first.id
    assert second.tz == 'Europe/Moscow'
    assert len(users.items) == 1


@pytest.mark.asyncio
async def test_promotion_requires_cache_marker_before_repository_mutation(
    users: InMemoryUserRepository,
) -> None:
    """Повышение не начинается без защиты инвалидации кэша."""
    existing = await users.create(
        login='Alice',
        password_hash='existing-hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    cache = InMemoryAuthorizationCache()
    cache.fail_mutations = True

    with pytest.raises(CacheUnavailableError):
        await service(users, cache).create_or_promote(
            login='Alice', password='different horse', tz='Europe/Moscow'
        )

    assert existing.is_superuser is False
