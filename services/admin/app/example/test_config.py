import importlib
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from pydantic import SecretStr, ValidationError

from example.config import Settings


class SettingsTests(TestCase):
    def test_defaults_keep_optional_database_configuration(self):
        with patch.dict(os.environ, {}, clear=True):
            config = Settings(_env_file=None)

        self.assertEqual(config.SECRET_KEY.get_secret_value(), 'secret_key')
        self.assertFalse(config.DEBUG)
        self.assertIsNone(config.POSTGRES_DB)
        self.assertIsNone(config.POSTGRES_USER)
        self.assertIsNone(config.POSTGRES_PASSWORD)
        self.assertEqual(config.SQL_HOST, '127.0.0.1')
        self.assertEqual(config.SQL_PORT, 5432)
        self.assertIsNone(config.SQL_OPTIONS)
        self.assertEqual(config.AUTH_SERVICE_URL, 'http://127.0.0.1:8000/api/v1/auth')
        self.assertEqual(config.AUTH_CLIENT_CONNECT_TIMEOUT_SECONDS, 0.5)
        self.assertEqual(config.AUTH_CLIENT_REQUEST_TIMEOUT_SECONDS, 1.0)
        self.assertEqual(config.AUTH_CLIENT_BACKOFF_MAX_TRIES, 2)
        self.assertEqual(config.AUTH_CLIENT_BACKOFF_MAX_TIME_SECONDS, 0.5)
        self.assertEqual(
            config.SERVICE_API_KEY.get_secret_value(),
            'local-service-api-key-that-is-32-chars',
        )
        self.assertEqual(
            config.BILLING_INTERNAL_URL,
            'http://127.0.0.1:8000/api/v1/internal/billing',
        )
        self.assertEqual(
            config.AUTH_INTERNAL_URL,
            'http://127.0.0.1:8000/api/v1/internal',
        )
        self.assertEqual(config.BILLING_CLIENT_CONNECT_TIMEOUT_SECONDS, 0.5)
        self.assertEqual(config.BILLING_CLIENT_TIMEOUT_SECONDS, 3.0)

    def test_environment_is_typed_and_secrets_are_masked(self):
        environment = {
            'SECRET_KEY': 'test-admin-key',
            'DEBUG': 'True',
            'POSTGRES_DB': 'catalog',
            'POSTGRES_USER': 'catalog-user',
            'POSTGRES_PASSWORD': 'test-db-password',
            'SQL_HOST': 'theatre-db',
            'SQL_PORT': '6543',
            'SQL_OPTIONS': '-c search_path=public,content',
            'AUTH_SERVICE_URL': 'http://auth:8000/api/v1/auth',
            'AUTH_CLIENT_CONNECT_TIMEOUT_SECONDS': '0.25',
            'AUTH_CLIENT_REQUEST_TIMEOUT_SECONDS': '2.5',
            'AUTH_CLIENT_BACKOFF_MAX_TRIES': '3',
            'AUTH_CLIENT_BACKOFF_MAX_TIME_SECONDS': '1.5',
            'SERVICE_API_KEY': 'test-internal-service-secret-key',
            'BILLING_INTERNAL_URL': 'http://billing-api:8000/api/v1/internal/billing',
            'AUTH_INTERNAL_URL': 'http://auth:8000/api/v1/internal',
            'BILLING_CLIENT_CONNECT_TIMEOUT_SECONDS': '0.4',
            'BILLING_CLIENT_TIMEOUT_SECONDS': '4.5',
        }
        with patch.dict(os.environ, environment, clear=True):
            config = Settings(_env_file=None)
        self.assertTrue(config.DEBUG)
        self.assertEqual(config.POSTGRES_DB, 'catalog')
        self.assertEqual(config.POSTGRES_USER, 'catalog-user')
        self.assertEqual(config.SQL_HOST, 'theatre-db')
        self.assertEqual(config.SQL_PORT, 6543)
        self.assertIsInstance(config.SQL_PORT, int)
        self.assertEqual(config.SQL_OPTIONS, '-c search_path=public,content')
        self.assertEqual(config.AUTH_SERVICE_URL, 'http://auth:8000/api/v1/auth')
        self.assertEqual(config.AUTH_CLIENT_CONNECT_TIMEOUT_SECONDS, 0.25)
        self.assertEqual(config.AUTH_CLIENT_REQUEST_TIMEOUT_SECONDS, 2.5)
        self.assertEqual(config.AUTH_CLIENT_BACKOFF_MAX_TRIES, 3)
        self.assertEqual(config.AUTH_CLIENT_BACKOFF_MAX_TIME_SECONDS, 1.5)
        self.assertIsInstance(config.SERVICE_API_KEY, SecretStr)
        self.assertEqual(
            config.SERVICE_API_KEY.get_secret_value(),
            'test-internal-service-secret-key',
        )
        self.assertEqual(
            config.BILLING_INTERNAL_URL,
            'http://billing-api:8000/api/v1/internal/billing',
        )
        self.assertEqual(
            config.AUTH_INTERNAL_URL,
            'http://auth:8000/api/v1/internal',
        )
        self.assertEqual(config.BILLING_CLIENT_CONNECT_TIMEOUT_SECONDS, 0.4)
        self.assertEqual(config.BILLING_CLIENT_TIMEOUT_SECONDS, 4.5)
        self.assertIsInstance(config.SECRET_KEY, SecretStr)
        self.assertIsInstance(config.POSTGRES_PASSWORD, SecretStr)
        self.assertEqual(config.POSTGRES_PASSWORD.get_secret_value(), 'test-db-password')
        self.assertNotIn('test-admin-key', repr(config))
        self.assertNotIn('test-db-password', repr(config))
        self.assertNotIn('test-internal-service-secret-key', repr(config))

    def test_dotenv_is_read_without_mutating_environment_and_env_wins(self):
        with TemporaryDirectory() as directory:
            dotenv = Path(directory) / '.env'
            dotenv.write_text(
                'POSTGRES_DB=dotenv-catalog\nSQL_PORT=6544\nDEBUG=True\n'
                'POSTGRES_PASSWORD=dotenv-password\nUNRELATED_SETTING=ignored\n',
                encoding='utf-8',
            )
            with patch.dict(os.environ, {'SQL_PORT': '6545'}, clear=True):
                before = dict(os.environ)
                config = Settings(_env_file=dotenv)
                self.assertEqual(dict(os.environ), before)
        self.assertEqual(config.POSTGRES_DB, 'dotenv-catalog')
        self.assertTrue(config.DEBUG)
        self.assertEqual(config.SQL_PORT, 6545)
        self.assertEqual(config.POSTGRES_PASSWORD.get_secret_value(), 'dotenv-password')

    def test_explicit_values_override_environment(self):
        with patch.dict(os.environ, {'SQL_PORT': '6544'}, clear=True):
            config = Settings(SQL_PORT=6545, _env_file=None)
        self.assertEqual(config.SQL_PORT, 6545)

    def test_valid_boundaries_and_empty_optional_options(self):
        with patch.dict(os.environ, {}, clear=True):
            for port in (1, 65535):
                with self.subTest(port=port):
                    config = Settings(SQL_PORT=port, SQL_OPTIONS='', _env_file=None)
                    self.assertEqual(config.SQL_PORT, port)
                    self.assertEqual(config.SQL_OPTIONS, '')
            config = Settings(AUTH_CLIENT_BACKOFF_MAX_TRIES=1, _env_file=None)
            self.assertEqual(config.AUTH_CLIENT_BACKOFF_MAX_TRIES, 1)

    def test_invalid_values_fail_without_showing_input(self):
        cases = {
            'SQL_PORT': ('', 'invalid-private-input', '0', '65536', '1.5'),
            'DEBUG': ('', 'invalid-private-input'),
            'AUTH_CLIENT_BACKOFF_MAX_TRIES': ('', 'invalid-private-input', '0', '-1', '1.5'),
            'AUTH_CLIENT_CONNECT_TIMEOUT_SECONDS': (
                '',
                'invalid-private-input',
                '0',
                '-1',
                'nan',
                'inf',
            ),
            'AUTH_CLIENT_REQUEST_TIMEOUT_SECONDS': (
                '',
                'invalid-private-input',
                '0',
                '-1',
                'nan',
                'inf',
            ),
            'AUTH_CLIENT_BACKOFF_MAX_TIME_SECONDS': (
                '',
                'invalid-private-input',
                '0',
                '-1',
                'nan',
                'inf',
            ),
            'SECRET_KEY': ('',),
            'POSTGRES_PASSWORD': ('',),
            'POSTGRES_DB': ('',),
            'POSTGRES_USER': ('',),
            'SQL_HOST': ('',),
            'AUTH_SERVICE_URL': ('',),
        }
        for name, values in cases.items():
            for value in values:
                with (
                    self.subTest(name=name, value=value),
                    patch.dict(
                        os.environ,
                        {
                            name: value,
                            'SECRET_KEY': 'test-private-key',
                            'POSTGRES_PASSWORD': 'test-private-password',
                        }
                        if name not in ('SECRET_KEY', 'POSTGRES_PASSWORD')
                        else {name: value},
                        clear=True,
                    ),
                ):
                    with self.assertRaises(ValidationError) as raised:
                        Settings(_env_file=None)
                    message = str(raised.exception)
                    self.assertIn(name, message)
                    self.assertNotIn('test-private-key', message)
                    self.assertNotIn('test-private-password', message)
                    self.assertNotIn('invalid-private-input', message)
                    self.assertNotIn('input_value', message)

    def test_django_exports_unwrap_secrets_at_consuming_boundary(self):
        from example import settings

        try:
            with patch.dict(
                os.environ,
                {
                    'SECRET_KEY': 'django-test-key',
                    'POSTGRES_PASSWORD': 'django-test-password',
                    'SQL_PORT': '6543',
                    'AUTH_CLIENT_CONNECT_TIMEOUT_SECONDS': '0.25',
                    'AUTH_CLIENT_BACKOFF_MAX_TRIES': '3',
                },
                clear=True,
            ):
                importlib.reload(settings)
                self.assertEqual(settings.SECRET_KEY, 'django-test-key')
                self.assertEqual(settings.DATABASES['default']['PASSWORD'], 'django-test-password')
                self.assertEqual(settings.DATABASES['default']['PORT'], 6543)
                self.assertIsInstance(settings.DATABASES['default']['PORT'], int)
                self.assertEqual(settings.AUTH_CLIENT_CONNECT_TIMEOUT_SECONDS, 0.25)
                self.assertEqual(settings.AUTH_CLIENT_BACKOFF_MAX_TRIES, 3)
        finally:
            importlib.reload(settings)

    def test_default_dotenv_path_is_service_local(self):
        self.assertEqual(
            Settings.model_config['env_file'],
            Path(__file__).resolve().parents[2] / '.env',
        )
