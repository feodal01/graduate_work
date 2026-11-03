import pytest


def test_bootstrap_reads_credentials_from_environment(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Bootstrap uses environment credentials without printing the password."""
    import bootstrap

    received = {}

    async def create_superuser(login: str, password: str, tz: str) -> str:
        received.update(login=login, password=password, tz=tz)
        return '11111111-1111-4111-8111-111111111111'

    monkeypatch.setenv('AUTH_ADMIN_TZ', 'Europe/Moscow')
    monkeypatch.setenv('AUTH_ADMIN_LOGIN', 'Admin')
    monkeypatch.setenv('AUTH_ADMIN_PASSWORD', 'secret-password')
    monkeypatch.setattr(bootstrap, '_create_superuser', create_superuser)

    bootstrap.main()

    self_output = capsys.readouterr().out
    assert received == {
        'tz': 'Europe/Moscow',
        'login': 'Admin',
        'password': 'secret-password',
    }
    assert '11111111-1111-4111-8111-111111111111' in self_output
    assert 'secret-password' not in self_output


@pytest.mark.parametrize('missing', ['AUTH_ADMIN_LOGIN', 'AUTH_ADMIN_PASSWORD'])
def test_bootstrap_credentials_are_required(
    monkeypatch: pytest.MonkeyPatch, missing: str
) -> None:
    """Bootstrap validates required credentials without exposing password input."""
    from pydantic import ValidationError

    from bootstrap_settings import BootstrapSettings

    monkeypatch.setenv('AUTH_ADMIN_TZ', 'Europe/Moscow')
    monkeypatch.setenv('AUTH_ADMIN_LOGIN', 'Admin')
    monkeypatch.setenv('AUTH_ADMIN_PASSWORD', 'private-bootstrap-password')
    monkeypatch.delenv(missing)
    with pytest.raises(ValidationError) as error:
        # Exercise required credentials through the environment source.
        BootstrapSettings()  # pyright: ignore[reportCallIssue]
    assert 'private-bootstrap-password' not in str(error.value)


@pytest.mark.parametrize('empty', ['AUTH_ADMIN_LOGIN', 'AUTH_ADMIN_PASSWORD'])
def test_bootstrap_credentials_are_nonempty(
    monkeypatch: pytest.MonkeyPatch, empty: str
) -> None:
    """An empty credential is rejected before user creation."""
    from pydantic import ValidationError

    from bootstrap_settings import BootstrapSettings

    monkeypatch.setenv('AUTH_ADMIN_TZ', 'Europe/Moscow')
    monkeypatch.setenv('AUTH_ADMIN_LOGIN', 'Admin')
    monkeypatch.setenv('AUTH_ADMIN_PASSWORD', 'private-bootstrap-password')
    monkeypatch.setenv(empty, '')
    with pytest.raises(ValidationError) as error:
        # Exercise required credentials through the environment source.
        BootstrapSettings()  # pyright: ignore[reportCallIssue]
    assert 'private-bootstrap-password' not in str(error.value)


def test_bootstrap_settings_keep_password_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Settings expose credentials only at the consuming boundary."""
    from pydantic import SecretStr

    from bootstrap_settings import BootstrapSettings

    monkeypatch.setenv('AUTH_ADMIN_TZ', 'Europe/Moscow')
    monkeypatch.setenv('AUTH_ADMIN_LOGIN', 'Admin')
    monkeypatch.setenv('AUTH_ADMIN_PASSWORD', 'private-bootstrap-password')
    # Required credentials come from the environment set above.
    settings = BootstrapSettings()  # pyright: ignore[reportCallIssue]
    assert settings.login == 'Admin'
    assert isinstance(settings.password, SecretStr)
    assert settings.password.get_secret_value() == 'private-bootstrap-password'
    assert 'private-bootstrap-password' not in repr(settings)
    assert 'private-bootstrap-password' not in settings.model_dump_json()


def test_bootstrap_timezone_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing timezone fails before the bootstrap writes a profile."""
    from pydantic import ValidationError

    from bootstrap_settings import BootstrapSettings

    monkeypatch.setenv('AUTH_ADMIN_TZ', 'Europe/Moscow')
    monkeypatch.setenv('AUTH_ADMIN_LOGIN', 'Admin')
    monkeypatch.setenv('AUTH_ADMIN_PASSWORD', 'private-bootstrap-password')
    monkeypatch.delenv('AUTH_ADMIN_TZ', raising=False)
    with pytest.raises(ValidationError):
        BootstrapSettings()  # pyright: ignore[reportCallIssue]
