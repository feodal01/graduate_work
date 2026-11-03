"""Wire validation and an actual authenticated HTTP socket round trip."""

import asyncio
import json
from uuid import uuid4

import httpx
import pytest

from notifications.clients.auth_contacts import ActiveContact, AuthContactsClient, ContactsError


def active(uid, email="current@example.test"):
    return {
        "user_id": str(uid),
        "status": "active",
        "email": email,
        "first_name": "Current",
        "last_name": None,
        "locale": "ru",
    }


@pytest.mark.parametrize("variant", ["missing", "duplicate", "extra", "malformed", "bad_status"])
async def test_incomplete_or_malformed_response_is_integration_failure(variant):
    uid = uuid4()
    items = [active(uid)]
    if variant == "missing":
        items = []
    if variant == "duplicate":
        items *= 2
    if variant == "extra":
        items.append(active(uuid4()))
    if variant == "malformed":
        items[0].pop("email")
    if variant == "bad_status":
        items[0]["status"] = "deleted"

    async def handle(request):
        return httpx.Response(200, json={"items": items})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = AuthContactsClient("https://auth.test/contacts", "secret", 1, 2, http=http)
        with pytest.raises(ContactsError, match="invalid_response"):
            await client.fetch([uid])


@pytest.mark.parametrize(
    "status,code",
    [(401, "authorization_failure"), (403, "authorization_failure"), (500, "http_failure")],
)
async def test_error_does_not_include_response_body(status, code):
    async def handle(request):
        return httpx.Response(status, text="private@example.test")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = AuthContactsClient("https://auth.test/contacts", "secret", 1, 2, http=http)
        with pytest.raises(ContactsError) as error:
            await client.fetch([uuid4()])
        assert str(error.value) == code


async def test_actual_http_authentication_and_bounded_batches():
    uids = [uuid4() for _ in range(3)]
    requests = []

    async def serve(reader, writer):
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            assert b"x-service-key: socket-secret" in headers.lower()
            length = int(
                next(
                    line.split(b":", 1)[1]
                    for line in headers.split(b"\r\n")
                    if line.lower().startswith(b"content-length:")
                )
            )
            ids = json.loads(await reader.readexactly(length))["user_ids"]
            requests.append(ids)
            body = json.dumps({"items": [active(uid) for uid in ids]}).encode()
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n"
                + f"Content-Length: {len(body)}\r\n\r\n".encode()
                + body
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    async with server, httpx.AsyncClient() as http:
        port = server.sockets[0].getsockname()[1]
        client = AuthContactsClient(
            f"http://127.0.0.1:{port}/contacts", "socket-secret", 2, 2, http=http
        )
        results = await client.fetch(uids)
    assert [len(batch) for batch in requests] == [2, 1]
    assert set(results) == set(uids)
    assert all(isinstance(item, ActiveContact) for item in results.values())


async def test_timeout_is_safe_integration_failure():
    async def handle(request):
        raise httpx.ReadTimeout("private@example.test", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = AuthContactsClient("https://auth.test/contacts", "secret", 1, 2, http=http)
        with pytest.raises(ContactsError) as error:
            await client.fetch([uuid4()])
        assert str(error.value) == "transport_failure"


async def test_duplicate_request_ids_rejected_before_http():
    uid = uuid4()

    async def handle(request):
        raise AssertionError("invalid duplicate input must not cross HTTP")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = AuthContactsClient("https://auth.test/contacts", "secret", 1, 2, http=http)
        with pytest.raises(ContactsError, match="invalid_request"):
            await client.fetch([uid, uid])
