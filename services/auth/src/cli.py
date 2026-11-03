import argparse
import asyncio
from getpass import getpass

from redis.asyncio import Redis

from core.config import (
    AuthorizationCacheSettings,
    PasswordPolicySettings,
    PostgresSettings,
    RedisSettings,
)
from db.cache.authorization import RedisAuthorizationCache
from db.sql.postgres import PostgresDatabase
from repositories.users import SQLAlchemyUserRepository
from security.passwords import PasswordHasher
from services.superusers import SuperuserService
from validation.credentials import PasswordPolicy
from validation.timezones import validate_timezone


async def _create_superuser(login: str, password: str, tz: str) -> str:
    tz = validate_timezone(tz)
    postgres_settings = PostgresSettings()
    database = PostgresDatabase(
        postgres_settings.dsn,
        connect_timeout_seconds=(postgres_settings.connect_timeout_seconds),
        pool_timeout_seconds=postgres_settings.pool_timeout_seconds,
        command_timeout_seconds=(postgres_settings.command_timeout_seconds),
        statement_timeout_ms=postgres_settings.statement_timeout_ms,
    )
    redis_settings = RedisSettings()
    redis = Redis(
        host=redis_settings.host,
        port=redis_settings.port,
        socket_connect_timeout=redis_settings.socket_connect_timeout,
        socket_timeout=redis_settings.socket_timeout,
    )
    password_settings = PasswordPolicySettings()
    authorization_cache_settings = AuthorizationCacheSettings()
    try:
        async with database.session() as session:
            user = await SuperuserService(
                users=SQLAlchemyUserRepository(session),
                authorization_cache=RedisAuthorizationCache(
                    redis,
                    ttl_seconds=authorization_cache_settings.ttl_seconds,
                ),
                password_hasher=PasswordHasher(),
                password_policy=PasswordPolicy(
                    min_length=password_settings.min_length,
                    min_score=password_settings.min_score,
                ),
            ).create_or_promote(login=login, password=password, tz=tz)
            return str(user.id)
    finally:
        try:
            await database.close()
        finally:
            await redis.aclose()


def main() -> None:
    parser = argparse.ArgumentParser(description='Create or promote superuser')
    parser.add_argument('--login', required=True)
    parser.add_argument('--tz', required=True, type=validate_timezone)
    arguments = parser.parse_args()
    password = getpass('Password: ')
    confirmation = getpass('Confirm password: ')
    if password != confirmation:
        parser.error('passwords do not match')
    user_id = asyncio.run(_create_superuser(arguments.login, password, arguments.tz))
    print(f'Superuser ready: {user_id}')


if __name__ == '__main__':
    main()
