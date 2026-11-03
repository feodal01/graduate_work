"""Идемпотентная начальная настройка GlitchTip.

Создаёт администратора, организацию, команду и проект, чей id и ключ
совпадают с SENTRY_DSN, — сервисы начинают отправлять ошибки без ручной
настройки в UI. Повторный запуск ничего не меняет.
"""

import os
import sys
from urllib.parse import urlparse
from uuid import UUID

import django

from bootstrap_settings import BootstrapSettings

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'glitchtip.settings')
django.setup()

from allauth.account.models import EmailAddress  # noqa: E402
from django.core.management.color import no_style  # noqa: E402
from django.db import connection, transaction  # noqa: E402

from apps.organizations_ext.models import Organization  # noqa: E402
from apps.projects.models import Project, ProjectKey  # noqa: E402
from apps.teams.models import Team  # noqa: E402
from apps.users.models import User  # noqa: E402

ORG_SLUG = 'cinema'
TEAM_SLUG = 'backend'
PROJECT_SLUG = 'backend'


def ensure_admin(email: str, password: str) -> User:
    user = User.objects.filter(email=email).first()
    if user is None:
        user = User.objects.create_superuser(email=email, password=password)
        print(f'Создан администратор {email}')
    EmailAddress.objects.get_or_create(
        user=user, email=email, defaults={'primary': True, 'verified': True}
    )
    return user


def ensure_project(user: User, project_id: int, public_key: UUID) -> None:
    org, _ = Organization.objects.get_or_create(slug=ORG_SLUG, defaults={'name': ORG_SLUG})
    if not org.users.filter(pk=user.pk).exists():
        org.add_user(user)

    project = Project.objects.filter(pk=project_id).first()
    if project is None:
        project = Project.objects.create(
            id=project_id,
            slug=PROJECT_SLUG,
            name=PROJECT_SLUG,
            organization=org,
            platform='python-fastapi',
        )
        # Проект создан с явным id: сдвигаем sequence, иначе следующий
        # проект из UI получит занятый id.
        with connection.cursor() as cursor:
            for sql in connection.ops.sequence_reset_sql(no_style(), [Project]):
                cursor.execute(sql)
        # Ключ, созданный автоматически, заменяем ключом из SENTRY_DSN.
        ProjectKey.objects.filter(project=project).delete()
        print(f'Создан проект {PROJECT_SLUG} (id={project_id})')
    elif project.organization_id != org.pk:
        print(
            f'Проект id={project_id} уже принадлежит другой организации; '
            'ключ всё равно будет добавлен'
        )

    key = ProjectKey.objects.filter(public_key=public_key).first()
    if key is None:
        ProjectKey.objects.create(project=project, public_key=public_key, name='compose')
        print('Создан ключ проекта из SENTRY_DSN')
    elif key.project_id != project.pk:
        sys.exit(
            f'Ключ из SENTRY_DSN привязан к проекту id={key.project_id}, '
            f'а в DSN указан {project_id}'
        )

    team, _ = Team.objects.get_or_create(slug=TEAM_SLUG, organization=org)
    team.members.add(org.organization_users.get(user=user))
    team.projects.add(project)


def main() -> None:
    settings = BootstrapSettings()
    user = ensure_admin(settings.admin_email, settings.admin_password.get_secret_value())

    dsn = settings.dsn.get_secret_value()
    if not dsn:
        print('SENTRY_DSN пуст: проект не создаётся')
        return
    parsed = urlparse(dsn)
    try:
        public_key = UUID(parsed.username or '')
        project_id = int(parsed.path.rstrip('/').rsplit('/', 1)[-1])
    except ValueError:
        sys.exit('SENTRY_DSN должен иметь вид http://<uuid-ключ>@glitchtip:8000/<id проекта>')

    with transaction.atomic():
        ensure_project(user, project_id, public_key)
    print(f'GlitchTip готов: вход {user.email}, DSN настроен')


if __name__ == '__main__':
    main()
