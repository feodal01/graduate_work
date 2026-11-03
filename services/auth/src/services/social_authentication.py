import base64
import secrets
from hashlib import sha256
from typing import Protocol

from db.cache.oauth_state import OAuthStateData, OAuthStateStore
from repositories.social_accounts import SocialAccountRepository
from security.tokens import IssuedTokenPair
from services.authentication import AuthenticationService
from services.errors import (
    InvalidOAuthStateError,
    OAuthStateGenerationError,
    SocialProviderContractError,
)
from services.types import ClientInfo, SocialIdentity
from validation.timezones import validate_timezone

_STATE_CREATION_ATTEMPTS = 3


class SocialProviderClient(Protocol):
    def build_authorization_url(
        self,
        *,
        state: str,
        code_challenge: str,
    ) -> str: ...

    async def exchange_code(
        self,
        *,
        code: str,
        code_verifier: str,
    ) -> str: ...

    async def get_identity(self, access_token: str) -> SocialIdentity: ...


class SocialAuthenticationService:
    def __init__(
        self,
        *,
        provider_name: str,
        state_store: OAuthStateStore,
        provider_client: SocialProviderClient,
        accounts: SocialAccountRepository,
        authentication: AuthenticationService,
        state_ttl_seconds: int,
    ) -> None:
        self._provider_name = provider_name
        self._state_store = state_store
        self._provider_client = provider_client
        self._accounts = accounts
        self._authentication = authentication
        self._state_ttl_seconds = state_ttl_seconds

    async def start(self, *, tz: str) -> str:
        tz = validate_timezone(tz)
        for _ in range(_STATE_CREATION_ATTEMPTS):
            state = secrets.token_urlsafe(32)
            code_verifier = secrets.token_urlsafe(64)
            created = await self._state_store.save(
                provider=self._provider_name,
                state=state,
                code_verifier=code_verifier,
                tz=tz,
                ttl_seconds=self._state_ttl_seconds,
            )
            if created:
                return self._provider_client.build_authorization_url(
                    state=state,
                    code_challenge=self._code_challenge(code_verifier),
                )
        raise OAuthStateGenerationError

    async def authenticate(
        self,
        *,
        code: str,
        state: str,
        client: ClientInfo,
    ) -> IssuedTokenPair:
        oauth_state = await self._consume_state(state)
        provider_access_token = await self._provider_client.exchange_code(
            code=code,
            code_verifier=oauth_state.code_verifier,
        )
        identity = await self._provider_client.get_identity(provider_access_token)
        if identity.provider != self._provider_name:
            raise SocialProviderContractError
        user = await self._accounts.get_or_create_user(identity, tz=oauth_state.tz)
        return await self._authentication.issue_session(
            user=user,
            client=client,
        )

    async def cancel(self, *, state: str) -> None:
        await self._consume_state(state)

    async def _consume_state(self, state: str) -> OAuthStateData:
        try:
            oauth_state = await self._state_store.consume(
                provider=self._provider_name,
                state=state,
            )
        except ValueError as exc:
            raise InvalidOAuthStateError from exc
        if oauth_state is None:
            raise InvalidOAuthStateError
        return oauth_state

    @staticmethod
    def _code_challenge(code_verifier: str) -> str:
        digest = sha256(code_verifier.encode('ascii')).digest()
        return base64.urlsafe_b64encode(digest).rstrip(b'=').decode('ascii')
