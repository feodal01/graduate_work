from django.http import HttpRequest, HttpResponse
from django.shortcuts import render


def rate_limited_response(
    request: HttpRequest,
    retry_after_seconds: int | None,
) -> HttpResponse:
    response = render(
        request,
        'admin/auth/rate_limited.html',
        {'retry_after': retry_after_seconds},
        status=429,
    )
    if retry_after_seconds is not None:
        response['Retry-After'] = str(retry_after_seconds)
    return response
