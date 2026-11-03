from unittest.mock import patch

from django.db import DatabaseError
from django.test import SimpleTestCase


class HealthViewTests(SimpleTestCase):
    @patch('example.health.connection.ensure_connection')
    def test_health_returns_ok_when_database_is_available(self, ensure_connection):
        response = self.client.get('/health/')

        ensure_connection.assert_called_once_with()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {'status': 'ok'})

    @patch(
        'example.health.connection.ensure_connection',
        side_effect=DatabaseError('database is unavailable'),
    )
    def test_health_returns_503_when_database_is_unavailable(self, ensure_connection):
        response = self.client.get('/health/')

        ensure_connection.assert_called_once_with()
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json(), {'status': 'unavailable'})
