from db.cache.authorization import AuthorizationCache
from models import User
from repositories.users import UserRepository
from security.passwords import PasswordHasher
from validation.credentials import PasswordPolicy, validate_login
from validation.timezones import validate_timezone


class SuperuserService:
    """Создаёт суперпользователя или повышает существующего пользователя."""

    def __init__(
        self,
        *,
        users: UserRepository,
        authorization_cache: AuthorizationCache,
        password_hasher: PasswordHasher,
        password_policy: PasswordPolicy,
    ) -> None:
        self._users = users
        self._authorization_cache = authorization_cache
        self._password_hasher = password_hasher
        self._password_policy = password_policy

    async def create_or_promote(self, *, login: str, password: str, tz: str) -> User:
        tz = validate_timezone(tz)
        valid_login = validate_login(login)
        existing = await self._users.get_by_login(valid_login)
        if existing is not None:
            if existing.is_superuser:
                return existing
            # После повышения старый снимок обычного пользователя больше
            # нельзя использовать, поэтому мутация удаляет только его.
            async with self._authorization_cache.user_mutation(existing.id):
                return await self._users.promote_to_superuser(existing)

        valid_password = self._password_policy.validate(
            password,
            login=valid_login,
            user_inputs=(),
        )
        return await self._users.create(
            login=valid_login,
            password_hash=self._password_hasher.hash(valid_password),
            first_name=None,
            last_name=None,
            is_superuser=True,
            tz=tz,
        )
