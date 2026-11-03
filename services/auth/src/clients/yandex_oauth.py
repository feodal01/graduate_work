import asyncio
from urllib.parse import urlencode

import httpx
from opentelemetry import trace
from pydantic import ValidationError

from schemas.social_authentication import (
    YandexTokenResponse,
    YandexUserInfoResponse,
)
from services.errors import (
    InvalidOAuthCodeError,
    SocialProviderContractError,
    SocialProviderUnavailableError,
)
from services.types import SocialIdentity

_AUTHORIZATION_URL = 'https://oauth.yandex.ru/authorize'
_TOKEN_URL = 'https://oauth.yandex.ru/token'
_USERINFO_URL = 'https://login.yandex.ru/info'
_SCOPES = 'login:info login:email'
_LOCAL_NAME_LENGTH = 50

tracer = trace.get_tracer(__name__)


class YandexOAuthClient:
    def __init__(
        self,
        *,
        http_client: httpx.AsyncClient,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
        userinfo_max_tries: int,
        userinfo_backoff_max_seconds: float,
    ) -> None:
        self._http_client = http_client
        self._client_id = client_id
        self._client_secret = client_secret
        self._redirect_uri = redirect_uri
        self._userinfo_max_tries = userinfo_max_tries
        self._userinfo_backoff_max_seconds = userinfo_backoff_max_seconds

    def build_authorization_url(
        self,
        *,
        state: str,
        code_challenge: str,
    ) -> str:
        query = urlencode(
            {
                'response_type': 'code',
                'client_id': self._client_id,
                'redirect_uri': self._redirect_uri,
                'scope': _SCOPES,
                'state': state,
                'code_challenge': code_challenge,
                'code_challenge_method': 'S256',
            }
        )
        return f'{_AUTHORIZATION_URL}?{query}'

    async def exchange_code(
        self,
        *,
        code: str,
        code_verifier: str,
    ) -> str:
        with tracer.start_as_current_span('yandex.oauth.token') as span:
            span.set_attribute('oauth.provider', 'yandex')
            span.set_attribute('oauth.operation', 'token')
            try:
                response = await self._http_client.post(
                    _TOKEN_URL,
                    auth=httpx.BasicAuth(
                        self._client_id,
                        self._client_secret,
                    ),
                    data={
                        'grant_type': 'authorization_code',
                        'code': code,
                        'code_verifier': code_verifier,
                    },
                )
            except httpx.TransportError as exc:
                raise SocialProviderUnavailableError from exc

            if response.status_code == 400 and self._is_invalid_grant(response):
                raise InvalidOAuthCodeError
            if not response.is_success:
                raise SocialProviderUnavailableError
            try:
                payload = YandexTokenResponse.model_validate(response.json())
            except (ValidationError, ValueError) as exc:
                raise SocialProviderContractError from exc
            return payload.access_token

    async def get_identity(self, access_token: str) -> SocialIdentity:
        with tracer.start_as_current_span('yandex.oauth.userinfo') as span:
            span.set_attribute('oauth.provider', 'yandex')
            span.set_attribute('oauth.operation', 'userinfo')
            response = await self._get_userinfo(access_token)
            try:
                payload = YandexUserInfoResponse.model_validate(response.json())
            except (ValidationError, ValueError) as exc:
                raise SocialProviderContractError from exc
            if payload.client_id != self._client_id:
                raise SocialProviderContractError
            return SocialIdentity(
                provider='yandex',
                provider_user_id=payload.id,
                provider_login=payload.login,
                email=payload.default_email,
                first_name=self._truncate_name(payload.first_name),
                last_name=self._truncate_name(payload.last_name),
            )

    async def _get_userinfo(self, access_token: str) -> httpx.Response:
        for attempt in range(1, self._userinfo_max_tries + 1):
            try:
                response = await self._http_client.get(
                    _USERINFO_URL,
                    headers={'Authorization': f'OAuth {access_token}'},
                )
            except httpx.TransportError as exc:
                if attempt == self._userinfo_max_tries:
                    raise SocialProviderUnavailableError from exc
                await asyncio.sleep(self._backoff(attempt))
                continue

            if response.status_code == 429 or response.status_code >= 500:
                if attempt == self._userinfo_max_tries:
                    raise SocialProviderUnavailableError
                await asyncio.sleep(self._retry_delay(response, attempt))
                continue
            if not response.is_success:
                raise SocialProviderUnavailableError
            return response
        raise SocialProviderUnavailableError

    def _retry_delay(self, response: httpx.Response, attempt: int) -> float:
        retry_after = response.headers.get('Retry-After')
        if retry_after is not None:
            try:
                parsed = int(retry_after)
            except ValueError:
                parsed = 0
            if parsed > 0:
                return min(
                    float(parsed),
                    self._userinfo_backoff_max_seconds,
                )
        return self._backoff(attempt)

    def _backoff(self, attempt: int) -> float:
        return min(
            float(2 ** (attempt - 1)),
            self._userinfo_backoff_max_seconds,
        )

    @staticmethod
    def _is_invalid_grant(response: httpx.Response) -> bool:
        try:
            payload = response.json()
        except ValueError:
            return False
        return isinstance(payload, dict) and payload.get('error') == 'invalid_grant'

    @staticmethod
    def _truncate_name(value: str | None) -> str | None:
        if value is None:
            return None
        return value[:_LOCAL_NAME_LENGTH]
