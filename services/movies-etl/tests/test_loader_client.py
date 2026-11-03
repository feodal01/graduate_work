import json
from datetime import datetime, timezone
from typing import Any

import pytest

import postgres_to_es.pipelines.loader.client as client_module
from postgres_to_es.pipelines.loader.client import bulk_index, ensure_index_exists
from postgres_to_es.schemas import MovieDocument


class _FakeResponse:
    def __init__(
        self, status_code: int, body: dict[str, Any] | None = None, text: str = ""
    ) -> None:
        self.status_code = status_code
        self._body = body or {}
        self.text = text or json.dumps(self._body)

    def json(self) -> dict[str, Any]:
        return self._body

    def raise_for_status(self) -> None:
        if 400 <= self.status_code:
            raise RuntimeError(f"HTTP {self.status_code}")


def _movie(movie_id: str = "film-1") -> MovieDocument:
    return MovieDocument(
        id=movie_id,
        title="Star Wars",
        film_version=1,
        index_revision=1,
        catalog_published_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        catalog_updated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


def test_ensure_index_exists_skips_creation_when_index_already_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    calls: list[str] = []

    def fake_head(url: str, timeout: int) -> _FakeResponse:
        calls.append("head")
        return _FakeResponse(200)

    def fake_put(*args: Any, **kwargs: Any) -> _FakeResponse:
        calls.append("put")
        return _FakeResponse(200)

    monkeypatch.setattr(client_module.requests, "head", fake_head)
    monkeypatch.setattr(client_module.requests, "put", fake_put)

    # Act
    ensure_index_exists("movies")

    # Assert
    assert calls == ["head"]


def test_ensure_index_exists_creates_index_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    put_calls: list[dict[str, Any]] = []
    monkeypatch.setattr(client_module.requests, "head", lambda url, timeout: _FakeResponse(404))

    def fake_put(url: str, **kwargs: Any) -> _FakeResponse:
        put_calls.append({"url": url, "json": kwargs["json"]})
        return _FakeResponse(200)

    monkeypatch.setattr(client_module.requests, "put", fake_put)

    # Act
    ensure_index_exists("movies")

    # Assert
    assert len(put_calls) == 1
    assert put_calls[0]["url"].endswith("/movies")
    assert "mappings" in put_calls[0]["json"]


def test_ensure_index_exists_treats_concurrent_creation_race_as_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: другой воркер успел создать индекс между нашими HEAD и PUT
    monkeypatch.setattr(client_module.requests, "head", lambda url, timeout: _FakeResponse(404))
    monkeypatch.setattr(
        client_module.requests,
        "put",
        lambda *a, **kw: _FakeResponse(400, text="resource_already_exists_exception"),
    )

    # Act / Assert: не должно бросить исключение
    ensure_index_exists("movies")


def test_bulk_index_is_noop_for_empty_documents(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    monkeypatch.setattr(
        client_module.requests,
        "post",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not be called")),
    )

    # Act / Assert: не бросает и не делает сетевых вызовов
    bulk_index([], "movies")


def test_bulk_index_sends_ndjson_and_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    monkeypatch.setattr(client_module.requests, "head", lambda url, timeout: _FakeResponse(200))
    captured: dict[str, Any] = {}

    def fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        captured["url"] = url
        captured["data"] = kwargs["data"]
        captured["headers"] = kwargs["headers"]
        return _FakeResponse(200, {"errors": False, "items": []})

    monkeypatch.setattr(client_module.requests, "post", fake_post)

    # Act
    bulk_index([_movie("film-1"), _movie("film-2")], "movies")

    # Assert
    assert captured["url"].endswith("/movies/_bulk")
    assert captured["headers"]["Content-Type"] == "application/x-ndjson"
    lines = captured["data"].decode("utf-8").strip().split("\n")
    assert len(lines) == 4  # 2 документа x (action + source)
    assert json.loads(lines[0]) == {
        "index": {"_id": "film-1", "version": 1, "version_type": "external"}
    }
    assert json.loads(lines[1])["title"] == "Star Wars"


def test_bulk_index_raises_when_some_documents_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: _bulk отвечает 200, но конкретный документ внутри не проиндексировался
    monkeypatch.setattr(client_module.requests, "head", lambda url, timeout: _FakeResponse(200))
    monkeypatch.setattr(
        client_module.requests,
        "post",
        lambda *a, **kw: _FakeResponse(
            200,
            {
                "errors": True,
                "items": [
                    {"index": {"_id": "film-1", "status": 201}},
                    {
                        "index": {
                            "_id": "film-2",
                            "status": 400,
                            "error": {"type": "mapper_parsing_exception", "reason": "boom"},
                        }
                    },
                ],
            },
        ),
    )

    # Act / Assert
    with pytest.raises(RuntimeError, match="film-2"):
        bulk_index([_movie("film-1"), _movie("film-2")], "movies")
