from uuid import UUID

from django.test import RequestFactory, SimpleTestCase

from users.request_id import get_request_id


class RequestIdTests(SimpleTestCase):
    def test_incoming_request_id_is_preserved(self) -> None:
        request = RequestFactory().get(
            '/admin/',
            headers={'X-Request-Id': 'admin-request-id'},
        )

        self.assertEqual(get_request_id(request), 'admin-request-id')

    def test_direct_request_reuses_generated_request_id(self) -> None:
        request = RequestFactory().get('/admin/')

        first = get_request_id(request)
        second = get_request_id(request)

        self.assertEqual(first, second)
        self.assertEqual(UUID(first).version, 4)
