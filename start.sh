#!/bin/sh
set -e

mkdir -p /data/data
# Seed persistent volume with bundled artifacts only if files are missing.
cp -rn /app/data/* /data/data/ || true

exec uvicorn server.api:app --host 0.0.0.0 --port ${PORT:-8080}
