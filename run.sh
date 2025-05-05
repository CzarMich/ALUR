#!/bin/bash
set -e

# Base path for mounted volumes
ALUR_HOME="/opt/alur"

echo "[INFO] Ensuring directory structure at $ALUR_HOME..."

# Create folders
mkdir -p \
  "$ALUR_HOME/data" \
  "$ALUR_HOME/logs" \
  "$ALUR_HOME/conf/environment/cert" \
  "$ALUR_HOME/conf/environment/key" \
  "$ALUR_HOME/resources"

# Copy default config if missing
[ ! -f "$ALUR_HOME/conf/settings.yml" ] && cp ./application/conf/settings.yml "$ALUR_HOME/conf/settings.yml"
[ ! -f "$ALUR_HOME/conf/environment/.env" ] && cp ./application/conf/environment/.env.example "$ALUR_HOME/conf/environment/.env"

echo "[INFO] Building and starting containers..."
docker-compose up --build -d

echo "[INFO] Waiting for PostgreSQL to become available..."
until docker exec alur-db pg_isready -U aluruser > /dev/null 2>&1; do
  sleep 1
done

echo "[INFO] ALUR is now running."

echo "[INFO] Tailing logs (Ctrl+C to exit)..."
docker logs -f alur-app
