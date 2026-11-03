from collections.abc import Iterator

import httpx
import pytest
from fastapi import FastAPI
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import SpanKind

from api.request_id import require_request_id
from core.tracing import create_sampler, instrument_fastapi
from main import create_app


def _traced_application(
    sample_rate: float,
) -> tuple[FastAPI, TracerProvider, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider(sampler=create_sampler(sample_rate))
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    application = FastAPI()
    application.middleware('http')(require_request_id)
    instrument_fastapi(application, provider)

    @application.get('/probe')
    async def probe() -> dict[str, str]:
        return {'status': 'ok'}

    return application, provider, exporter


def _server_spans(exporter: InMemorySpanExporter) -> Iterator[ReadableSpan]:
    return (
        span for span in exporter.get_finished_spans() if span.kind is SpanKind.SERVER
    )


@pytest.mark.asyncio
async def test_server_span_contains_request_id() -> None:
    """Removing the request hook loses the audit correlation attribute."""
    application, provider, exporter = _traced_application(1.0)
    try:
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport,
            base_url='http://testserver',
        ) as client:
            response = await client.get(
                '/probe',
                headers={'X-Request-Id': 'trace-request-id'},
            )

        assert response.status_code == 200
        server_span = next(_server_spans(exporter))
        assert server_span.attributes is not None
        assert server_span.attributes['http.request_id'] == 'trace-request-id'
    finally:
        provider.shutdown()


@pytest.mark.parametrize(
    ('sample_rate', 'expected_span_count'),
    [(0.0, 0), (1.0, 1)],
)
@pytest.mark.asyncio
async def test_root_trace_uses_configured_sample_rate(
    sample_rate: float,
    expected_span_count: int,
) -> None:
    """Changing the root sampler changes which server spans are recorded."""
    application, provider, exporter = _traced_application(sample_rate)
    try:
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport,
            base_url='http://testserver',
        ) as client:
            response = await client.get(
                '/probe',
                headers={'X-Request-Id': 'sample-rate-request'},
            )

        assert response.status_code == 200
        assert len(tuple(_server_spans(exporter))) == expected_span_count
    finally:
        provider.shutdown()


@pytest.mark.asyncio
async def test_sampled_parent_is_recorded_when_root_rate_is_zero() -> None:
    """Replacing ParentBased sampling would drop an upstream sampled trace."""
    application, provider, exporter = _traced_application(0.0)
    trace_id = '00112233445566778899aabbccddeeff'
    try:
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport,
            base_url='http://testserver',
        ) as client:
            response = await client.get(
                '/probe',
                headers={
                    'X-Request-Id': 'parent-sampled-request',
                    'traceparent': (f'00-{trace_id}-0123456789abcdef-01'),
                },
            )

        assert response.status_code == 200
        server_span = next(_server_spans(exporter))
        assert server_span.context is not None
        assert f'{server_span.context.trace_id:032x}' == trace_id
    finally:
        provider.shutdown()


@pytest.mark.asyncio
async def test_create_app_uses_explicit_tracer_provider() -> None:
    """Dropping application instrumentation removes the Auth server span."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider(sampler=create_sampler(1.0))
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    application = create_app(
        app_lifespan=None,
        tracer_provider=provider,
    )
    try:
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport,
            base_url='http://testserver',
        ) as client:
            response = await client.get(
                '/auth/openapi.json',
                headers={'X-Request-Id': 'auth-app-request'},
            )

        assert response.status_code == 200
        server_span = next(_server_spans(exporter))
        assert server_span.attributes is not None
        assert server_span.attributes['http.request_id'] == 'auth-app-request'
    finally:
        provider.shutdown()
