from typing import Annotated

from fastapi import APIRouter, Depends, Query, status
from fastapi.responses import RedirectResponse

from dependencies import get_client_info, get_social_authentication_service
from schemas.authentication import TokenPairResponse
from services.errors import (
    InvalidOAuthCallbackError,
    InvalidOAuthStateError,
    SocialAuthenticationRejectedError,
)
from services.social_authentication import SocialAuthenticationService
from services.types import ClientInfo
from validation.timezones import Timezone

router = APIRouter(tags=['social authentication'])


@router.get(
    '/social/{provider_name}/login',
    status_code=status.HTTP_307_TEMPORARY_REDIRECT,
    response_class=RedirectResponse,
    responses={
        404: {'description': 'Social provider is not supported'},
        503: {'description': 'Social provider is not available'},
    },
)
async def start_social_login(
    tz: Annotated[Timezone, Query()],
    service: Annotated[
        SocialAuthenticationService,
        Depends(get_social_authentication_service),
    ],
) -> RedirectResponse:
    return RedirectResponse(
        url=await service.start(tz=tz),
        status_code=status.HTTP_307_TEMPORARY_REDIRECT,
    )


@router.get(
    '/social/{provider_name}/callback',
    response_model=TokenPairResponse,
    responses={
        400: {'description': 'Invalid, expired, or rejected OAuth callback'},
        404: {'description': 'Social provider is not supported'},
        502: {'description': 'Social provider returned an invalid response'},
        503: {'description': 'Social provider or state store is unavailable'},
    },
)
async def finish_social_login(
    service: Annotated[
        SocialAuthenticationService,
        Depends(get_social_authentication_service),
    ],
    client: Annotated[ClientInfo, Depends(get_client_info)],
    code: Annotated[str | None, Query()] = None,
    state: Annotated[str | None, Query()] = None,
    error: Annotated[str | None, Query()] = None,
    _error_description: Annotated[
        str | None,
        Query(alias='error_description'),
    ] = None,
) -> TokenPairResponse:
    if not state:
        raise InvalidOAuthStateError
    if bool(code) == bool(error):
        raise InvalidOAuthCallbackError
    if error:
        await service.cancel(state=state)
        raise SocialAuthenticationRejectedError
    if not code:
        raise InvalidOAuthCallbackError

    pair = await service.authenticate(code=code, state=state, client=client)
    return TokenPairResponse(
        access_token=pair.access.value,
        refresh_token=pair.refresh.value,
    )
