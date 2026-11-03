FROM alpine:3.22

RUN apk add --no-cache logrotate

# logrotate пропускает конфиги, которые принадлежат не root или доступны на запись
# другим; при bind mount с хоста владелец не root, поэтому конфиг копируется в образ.
COPY --chmod=644 nginx.conf /etc/logrotate.d/nginx

# Проверка размера каждые 5 минут.
CMD ["sh", "-c", "while true; do logrotate -s /tmp/logrotate.status /etc/logrotate.d/nginx; sleep 300; done"]
