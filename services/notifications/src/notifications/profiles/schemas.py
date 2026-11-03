"""Public preferences contain only the three user-controlled flags."""

from typing import Self

from pydantic import BaseModel, ConfigDict, StrictBool, model_validator


class Preferences(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    likes: bool
    new_films: bool
    marketing: bool


class PreferencesPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    likes: StrictBool | None = None
    new_films: StrictBool | None = None
    marketing: StrictBool | None = None

    @model_validator(mode="after")
    def nonempty_boolean_subset(self) -> Self:
        if not self.model_fields_set:
            raise ValueError("at least one preference is required")
        if any(getattr(self, name) is None for name in self.model_fields_set):
            raise ValueError("preferences must be booleans, not null")
        return self
