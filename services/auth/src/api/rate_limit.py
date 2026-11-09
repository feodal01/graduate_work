import logging

from fastapi import Request
from opentelemetry import trace

from core.config import RateLimitSettings, settings
from db.cache.errors import RateLimitStoreUnavailableError
from services.errors import RateLimitExceededError
from services.rate_limit import RateLimiter, RateLimitPolicy

logger = logging.getLogger(__name__)


def policy(name: str, config: RateLimitSettings) -> RateLimitPolicy:
    """Build one named policy from its explicit configuration fields."""
    if name == 'login':
        return RateLimitPolicy(
            name,
            config.login_rate,
            config.login_window_seconds,
            config.login_capacity,
        )
    if name == 'signup':
        return RateLimitPolicy(
            name,
            config.signup_rate,
            config.signup_window_seconds,
            config.signup_capacity,
        )
    if name == 'refresh':
        return RateLimitPolicy(
            name,
            config.refresh_rate,
            config.refresh_window_seconds,
            config.refresh_capacity,
        )
    if name == 'user':
        return RateLimitPolicy(
            name,
            config.user_rate,
            config.user_window_seconds,
            config.user_capacity,
        )
    if name == 'admin':
        return RateLimitPolicy(
            name,
            config.admin_rate,
            config.admin_window_seconds,
            config.admin_capacity,
        )
    if name == 'service':
        return RateLimitPolicy(
            name,
            config.service_rate,
            config.service_window_seconds,
            config.service_capacity,
        )
    if name == 'permission_check':
        return RateLimitPolicy(
            name,
            config.permission_check_rate,
            config.permission_check_window_seconds,
            config.permission_check_capacity,
        )
    raise ValueError(f'Unknown rate-limit policy: {name}')


async def enforce_rate_limit(
    *,
    request: Request,
    limiter: RateLimiter,
    policy: RateLimitPolicy,
    identity: str,
) -> None:
    """Apply one bucket and fail open only when Redis is unavailable."""
    if not settings.rate_limit.enabled:
        return

    request_id = request.headers.get('X-Request-Id')
    try:
        decision = await limiter.check(policy=policy, identity=identity)
    except RateLimitStoreUnavailableError:
        _set_span_attributes(
            {
                'rate_limit.policy': policy.name,
                'rate_limit.store_available': False,
            }
        )
        logger.warning(
            'Rate-limit store is unavailable; request continues '
            'request_id=%s policy=%s store_available=false',
            request_id,
            policy.name,
            extra={
                'request_id': request_id,
                'rate_limit.policy': policy.name,
                'rate_limit.store_available': False,
            },
        )
        return

    attributes: dict[str, str | bool | int] = {
        'rate_limit.policy': policy.name,
        'rate_limit.allowed': decision.allowed,
    }
    if not decision.allowed:
        attributes['rate_limit.retry_after'] = decision.retry_after_seconds
    _set_span_attributes(attributes)

    if decision.allowed:
        return

    logger.warning(
        'Auth request rejected by rate limit '
        'request_id=%s policy=%s allowed=false retry_after=%s',
        request_id,
        policy.name,
        decision.retry_after_seconds,
        extra={
            'request_id': request_id,
            'rate_limit.policy': policy.name,
            'rate_limit.allowed': False,
            'rate_limit.retry_after': decision.retry_after_seconds,
        },
    )
    raise RateLimitExceededError(decision.retry_after_seconds)


def _set_span_attributes(attributes: dict[str, str | bool | int]) -> None:
    span = trace.get_current_span()
    if not span.is_recording():
        return
    for name, value in attributes.items():
        span.set_attribute(name, value)
