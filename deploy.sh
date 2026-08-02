#!/usr/bin/env bash
# Деплой alpine-rusty на сервере: подтянуть код из GitHub и перезапустить сервис.
# Запускать на сервере: bash /opt/alpine-rusty/deploy.sh
# Обновление кода = git push (локально) -> этот скрипт на сервере.
set -euo pipefail

APP_DIR="/opt/alpine-rusty"
SERVICE="alpine-rusty"
BRANCH="main"

cd "$APP_DIR"

echo "[deploy] git fetch origin..."
git fetch --prune origin

echo "[deploy] reset -> origin/$BRANCH..."
git reset --hard "origin/$BRANCH"

echo "[deploy] pip install requirements..."
./.venv/bin/pip install -q -r requirements.txt

echo "[deploy] restart $SERVICE..."
systemctl restart "$SERVICE"
sleep 3

if systemctl is-active --quiet "$SERVICE"; then
    echo "[deploy] OK: $SERVICE active"
else
    echo "[deploy] FAIL: $SERVICE не запущен"
    systemctl status "$SERVICE" --no-pager -l | tail -20
    exit 1
fi
