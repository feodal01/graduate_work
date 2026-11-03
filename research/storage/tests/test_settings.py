"""Environment configuration checks without opening database connections."""

import os
import unittest
from unittest.mock import patch

from pydantic import SecretStr

from bench.settings import Settings


class SettingsTests(unittest.TestCase):
    def test_defaults_are_preserved(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            settings = Settings()
        self.assertEqual(
            settings.mongo_uri.get_secret_value(),
            'mongodb://127.0.0.1:27117/?replicaSet=rs0&directConnection=true',
        )
        self.assertEqual(
            settings.postgres_dsn.get_secret_value(),
            'postgresql://research:research@127.0.0.1:55432/ugc',
        )

    def test_environment_values_stay_secret(self) -> None:
        with patch.dict(
            os.environ,
            {
                'MONGO_URI': 'mongodb://user:private-mongo-password@localhost/',
                'POSTGRES_DSN': 'postgresql://user:private-pg-password@localhost/db',
            },
            clear=True,
        ):
            settings = Settings()
        self.assertIsInstance(settings.mongo_uri, SecretStr)
        self.assertEqual(
            settings.mongo_uri.get_secret_value(),
            'mongodb://user:private-mongo-password@localhost/',
        )
        self.assertEqual(
            settings.postgres_dsn.get_secret_value(),
            'postgresql://user:private-pg-password@localhost/db',
        )
        self.assertNotIn('private-mongo-password', repr(settings))
        self.assertNotIn('private-pg-password', settings.model_dump_json())
