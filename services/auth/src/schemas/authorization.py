from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field, model_validator

from models import Role
from services.film_access import FilmAccessContext

PermissionCode = Literal[
    'roles:manage',
    'user_roles:manage',
    'films.early_access:read',
]


class RoleCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    permissions: set[PermissionCode] = Field(default_factory=set[PermissionCode])


class RoleUpdateRequest(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=100)
    permissions: set[PermissionCode] | None = None

    @model_validator(mode='after')
    def require_change(self) -> 'RoleUpdateRequest':
        if self.name is None and self.permissions is None:
            raise ValueError('name or permissions must be provided')
        return self


class RoleAssignmentRequest(BaseModel):
    role_id: UUID


class RoleResponse(BaseModel):
    id: UUID
    name: str
    permissions: list[str]
    created_at: datetime

    @classmethod
    def from_entity(cls, role: Role) -> 'RoleResponse':
        return cls(
            id=role.id,
            name=role.name,
            permissions=sorted(permission.code for permission in role.permissions),
            created_at=role.created_at,
        )


class PermissionCheckRequest(BaseModel):
    resource: Literal['film']
    action: Literal['read']
    attributes: FilmAccessContext


class PermissionCheckResponse(BaseModel):
    allowed: bool
