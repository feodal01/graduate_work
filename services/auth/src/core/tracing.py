from typing import Any

from fastapi import FastAPI
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import (
    ParentBased,
    Sampler,
    TraceIdRatioBased,
)
from opentelemetry.trace import Span
from starlette.datastructures import Headers

from core.config import TracingSettings


def _set_request_id(span: Span | None, scope: dict[str, Any]) -> None:
    if span is None or not span.is_recording():
        return
    request_id = Headers(scope=scope).get('X-Request-Id')
    if request_id:
        span.set_attribute('http.request_id', request_id)


def create_sampler(sample_rate: float) -> Sampler:
    """Create a parent-based trace ID ratio sampler."""
    return ParentBased(TraceIdRatioBased(sample_rate))


def create_tracer_provider(config: TracingSettings) -> TracerProvider:
    """Create a provider that exports sampled spans through OTLP over HTTP."""
    provider = TracerProvider(
        resource=Resource.create({SERVICE_NAME: config.service_name}),
        sampler=create_sampler(config.sample_rate),
    )
    provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint=config.otlp_endpoint),
        )
    )
    return provider


def instrument_fastapi(
    application: FastAPI,
    provider: TracerProvider,
) -> None:
    """Instrument one FastAPI application."""
    FastAPIInstrumentor.instrument_app(
        application,
        tracer_provider=provider,
        server_request_hook=_set_request_id,
        exclude_spans=['receive', 'send'],
    )
