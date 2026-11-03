from typing import cast

import pytest
from pydantic import ValidationError

from schemas.authentication import ProfileUpdateRequest, SignupRequest


@pytest.mark.parametrize('tz', [None, '', 'Unknown/Zone'])
def test_signup_requires_valid_timezone(tz: str | None) -> None:
    """New profiles cannot be created with an unknown timezone."""
    with pytest.raises(ValidationError):
        SignupRequest.model_validate(
            {'login': 'Alice', 'password': 'correct horse', 'tz': tz}
        )


def test_signup_requires_explicit_timezone() -> None:
    """Omission must not manufacture a timezone."""
    with pytest.raises(ValidationError):
        SignupRequest.model_validate({'login': 'Alice', 'password': 'correct horse'})


def test_patch_cannot_clear_timezone() -> None:
    """Partial updates permit omission but cannot clear a known timezone."""
    assert ProfileUpdateRequest(first_name='Alice').tz is None
    with pytest.raises(ValidationError):
        ProfileUpdateRequest(tz=None)


@pytest.mark.asyncio
@pytest.mark.parametrize('tz', [None, '', 'Unknown/Zone'])
async def test_repository_rejects_timezone_before_database(tz: str | None) -> None:
    """Direct callers cannot bypass creation timezone validation."""
    from unittest.mock import AsyncMock

    from sqlalchemy.ext.asyncio import AsyncSession

    from repositories.users import SQLAlchemyUserRepository

    session = AsyncMock(spec=AsyncSession)
    with pytest.raises(ValueError):
        await SQLAlchemyUserRepository(session).create(
            login='Alice',
            password_hash=None,
            first_name=None,
            last_name=None,
            tz=cast(str, tz),
        )
    session.add.assert_not_called()
    session.flush.assert_not_awaited()
