import asyncio

from bootstrap_settings import BootstrapSettings
from cli import _create_superuser


def main() -> None:
    # Required credentials are supplied by the Pydantic environment source.
    settings = BootstrapSettings()  # pyright: ignore[reportCallIssue]
    user_id = asyncio.run(
        _create_superuser(
            settings.login, settings.password.get_secret_value(), settings.tz
        )
    )
    print(f'Initial Auth superuser ready: {user_id}')


if __name__ == '__main__':
    main()
