from typing import Annotated
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import AfterValidator, Field


def validate_timezone(value: object) -> str:
    if not isinstance(value, str) or not value or len(value) > 64:
        raise ValueError('tz must be an IANA timezone')
    try:
        ZoneInfo(value)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError('tz must be an IANA timezone') from exc
    return value


Timezone = Annotated[str, Field(max_length=64), AfterValidator(validate_timezone)]
