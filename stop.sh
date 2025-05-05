#!/bin/bash
set -e

echo "[INFO] Stopping Alur services..."
docker-compose down
echo "[INFO] Alur has been stopped."
