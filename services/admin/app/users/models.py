from django.conf import settings
from django.db import models


class AuthIdentity(models.Model):
    """Связывает технического пользователя Django с UUID пользователя Auth."""

    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='auth_identity',
    )
    auth_user_id = models.UUIDField(unique=True, editable=False)
    login = models.CharField(max_length=255)

    def __str__(self) -> str:
        return self.login
