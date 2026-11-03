"""Client construction checks with database drivers mocked at their boundary."""

import os
import unittest
from unittest.mock import patch

from bench.stores import MongoStore, PostgresStore


class StoreSettingsTests(unittest.TestCase):
    def test_clients_receive_environment_values(self) -> None:
        with (
            patch.dict(
                os.environ,
                {
                    'MONGO_URI': 'mongodb://user:private-mongo-password@localhost/',
                    'POSTGRES_DSN': 'postgresql://user:private-pg-password@localhost/db',
                },
                clear=True,
            ),
            patch('bench.stores.MongoClient') as mongo,
            patch(
                'bench.stores.psycopg.connect',
            ) as postgres,
        ):
            MongoStore()
            PostgresStore()
        mongo.assert_called_once_with(
            'mongodb://user:private-mongo-password@localhost/',
            uuidRepresentation='standard',
        )
        postgres.assert_called_once_with(
            'postgresql://user:private-pg-password@localhost/db',
            autocommit=True,
            prepare_threshold=0,
        )

    def test_explicit_connection_arguments_override_environment(self) -> None:
        with (
            patch.dict(
                os.environ,
                {
                    'MONGO_URI': 'mongodb://environment/',
                    'POSTGRES_DSN': 'postgresql://environment/db',
                },
                clear=True,
            ),
            patch('bench.stores.MongoClient') as mongo,
            patch(
                'bench.stores.psycopg.connect',
            ) as postgres,
        ):
            MongoStore('mongodb://explicit/')
            PostgresStore('postgresql://explicit/db')
        mongo.assert_called_once_with('mongodb://explicit/', uuidRepresentation='standard')
        postgres.assert_called_once_with(
            'postgresql://explicit/db',
            autocommit=True,
            prepare_threshold=0,
        )
