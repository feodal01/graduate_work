"""Configuration-only tests; importing these does not initialize Django."""

import os
import unittest
from unittest.mock import patch

from pydantic import SecretStr, ValidationError

from bootstrap_settings import BootstrapSettings


class BootstrapSettingsTests(unittest.TestCase):
    def test_environment_and_secret_fields(self) -> None:
        with patch.dict(
            os.environ,
            {
                'SENTRY_ADMIN_EMAIL': 'admin@example.test',
                'SENTRY_ADMIN_PASSWORD': 'private-admin-password',
                'SENTRY_DSN': 'http://private-key@localhost/1',
            },
            clear=True,
        ):
            settings = BootstrapSettings()
        self.assertEqual(settings.admin_email, 'admin@example.test')
        self.assertIsInstance(settings.admin_password, SecretStr)
        self.assertIsInstance(settings.dsn, SecretStr)
        self.assertEqual(settings.admin_password.get_secret_value(), 'private-admin-password')
        self.assertEqual(settings.dsn.get_secret_value(), 'http://private-key@localhost/1')
        self.assertNotIn('private-admin-password', repr(settings))
        self.assertNotIn('private-key', settings.model_dump_json())

    def test_empty_dsn_default_is_preserved(self) -> None:
        with patch.dict(
            os.environ,
            {
                'SENTRY_ADMIN_EMAIL': 'admin@example.test',
                'SENTRY_ADMIN_PASSWORD': 'private-admin-password',
            },
            clear=True,
        ):
            self.assertEqual(BootstrapSettings().dsn.get_secret_value(), '')

    def test_missing_or_empty_credentials_are_rejected_without_secrets(self) -> None:
        for name in ('SENTRY_ADMIN_EMAIL', 'SENTRY_ADMIN_PASSWORD'):
            for value in (None, ''):
                with self.subTest(name=name, value=value):
                    environment = {
                        'SENTRY_ADMIN_EMAIL': 'admin@example.test',
                        'SENTRY_ADMIN_PASSWORD': 'private-admin-password',
                        'SENTRY_DSN': 'http://private-key@localhost/1',
                    }
                    if value is None:
                        environment.pop(name)
                    else:
                        environment[name] = value
                    with patch.dict(os.environ, environment, clear=True):
                        with self.assertRaises(ValidationError) as error:
                            BootstrapSettings()
                    self.assertNotIn('private-admin-password', str(error.exception))
                    self.assertNotIn('private-key', str(error.exception))
