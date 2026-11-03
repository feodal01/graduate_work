from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    String,
    Table,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql.functions import now

from models.base import Base


def _utc_now() -> datetime:
    return datetime.now(UTC)


role_permissions = Table(
    'role_permissions',
    Base.metadata,
    Column(
        'role_id',
        Uuid,
        ForeignKey('roles.id', ondelete='CASCADE'),
        primary_key=True,
    ),
    Column(
        'permission_code',
        String(100),
        ForeignKey('permissions.code', ondelete='CASCADE'),
        primary_key=True,
    ),
)


user_roles = Table(
    'user_roles',
    Base.metadata,
    Column('user_id', Uuid, ForeignKey('users.id'), primary_key=True),
    Column(
        'role_id',
        Uuid,
        ForeignKey('roles.id', ondelete='CASCADE'),
        primary_key=True,
    ),
)


class User(Base):
    __tablename__ = 'users'
    __table_args__ = (
        CheckConstraint('user_version >= 1', name='ck_users_user_version'),
    )

    id: Mapped[UUID] = mapped_column(
        Uuid,
        primary_key=True,
        default=uuid4,
        nullable=False,
    )
    login: Mapped[str] = mapped_column(
        String(255),
        unique=True,
        nullable=False,
    )
    password_hash: Mapped[str | None] = mapped_column(
        'password',
        String(255),
        nullable=True,
    )
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    locale: Mapped[str | None] = mapped_column(String(64), nullable=True)
    tz: Mapped[str | None] = mapped_column(String(64), nullable=True)
    user_version: Mapped[int] = mapped_column(
        BigInteger, default=1, server_default='1', nullable=False
    )
    first_name: Mapped[str | None] = mapped_column(String(50), nullable=True)
    last_name: Mapped[str | None] = mapped_column(String(50), nullable=True)
    is_superuser: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        server_default='false',
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utc_now,
        server_default=now(),
        nullable=False,
    )
    roles: Mapped[list['Role']] = relationship(
        secondary=user_roles,
        back_populates='users',
        lazy='selectin',
    )
    social_accounts: Mapped[list['SocialAccount']] = relationship(
        back_populates='user',
        cascade='all, delete-orphan',
        lazy='selectin',
    )


class SocialAccount(Base):
    __tablename__ = 'social_accounts'
    __table_args__ = (
        UniqueConstraint(
            'provider',
            'provider_user_id',
            name='uq_social_accounts_provider_identity',
        ),
        UniqueConstraint(
            'user_id',
            'provider',
            name='uq_social_accounts_user_provider',
        ),
    )

    id: Mapped[UUID] = mapped_column(
        Uuid,
        primary_key=True,
        default=uuid4,
        nullable=False,
    )
    user_id: Mapped[UUID] = mapped_column(
        Uuid,
        ForeignKey('users.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    provider_user_id: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utc_now,
        server_default=now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utc_now,
        server_default=now(),
        onupdate=_utc_now,
        nullable=False,
    )
    user: Mapped[User] = relationship(back_populates='social_accounts')


class Role(Base):
    __tablename__ = 'roles'

    id: Mapped[UUID] = mapped_column(
        Uuid,
        primary_key=True,
        default=uuid4,
        nullable=False,
    )
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utc_now,
        server_default=now(),
        nullable=False,
    )
    permissions: Mapped[list['Permission']] = relationship(
        secondary=role_permissions,
        back_populates='roles',
        lazy='selectin',
    )
    users: Mapped[list[User]] = relationship(
        secondary=user_roles,
        back_populates='roles',
        lazy='selectin',
    )


class Permission(Base):
    __tablename__ = 'permissions'

    code: Mapped[str] = mapped_column(String(100), primary_key=True)
    roles: Mapped[list[Role]] = relationship(
        secondary=role_permissions,
        back_populates='permissions',
        lazy='selectin',
    )


class LoginHistory(Base):
    __tablename__ = 'login_history'

    user_id: Mapped[UUID] = mapped_column(
        Uuid,
        ForeignKey('users.id'),
        primary_key=True,
        nullable=False,
    )
    id: Mapped[UUID] = mapped_column(
        Uuid,
        primary_key=True,
        default=uuid4,
        nullable=False,
    )
    logged_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utc_now,
        server_default=now(),
        nullable=False,
    )
    ip_address: Mapped[str | None] = mapped_column(String(45), nullable=True)
    user_agent: Mapped[str | None] = mapped_column(String(512), nullable=True)
    __table_args__ = (
        Index(
            'ix_login_history_user_id_logged_at',
            'user_id',
            logged_at.desc(),
        ),
        {'postgresql_partition_by': 'HASH (user_id)'},
    )
