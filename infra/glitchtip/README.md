# GlitchTip bootstrap

`bootstrap_settings.py` читает `SENTRY_ADMIN_EMAIL`, `SENTRY_ADMIN_PASSWORD` и
`SENTRY_DSN` через Pydantic Settings. Email и пароль обязательны и непустые.
Пустой DSN сохраняет прежнее поведение: создаётся только администратор. Пароль
и DSN хранятся как `SecretStr`; DSN не выводится в журнал.

Compose использует фиксированный образ `glitchtip/glitchtip:6.2.6`. Он уже содержит
runtime-зависимости `pydantic==2.13.3` и `pydantic-settings==2.13.1`: отдельная
установка при запуске и производный образ не нужны. При смене образа наличие
зависимостей и импорт settings нужно проверить отдельно, не запуская bootstrap.

Проверка текущего образа без сети и обращения к Django/БД:

```sh
docker run --rm --network none --entrypoint python \
  -v "$PWD/infra/glitchtip:/config:ro" -w /config \
  glitchtip/glitchtip:6.2.6 \
  -m unittest discover -s tests -v
```
