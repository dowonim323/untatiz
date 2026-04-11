#!/bin/bash
# Untatiz Docker Entrypoint
# 환경 설정 및 초기화 후 supervisord 실행

set -e

echo "============================================"
echo "Untatiz Docker Container Starting..."
echo "============================================"

# 디렉토리 권한 설정
echo "[Entrypoint] Setting directory permissions..."
chown -R untatiz:untatiz /app/db /app/log /app/api /app/backup /app/news 2>/dev/null || true

mkdir -p /app/log
echo "[Entrypoint] Writing container startup marker..."
date --iso-8601=seconds > /app/log/container_start_marker

# 로그 디렉토리 확인
mkdir -p /app/log
touch /app/log/scraper.log /app/log/web.log
chown -R untatiz:untatiz /app/log

# 데이터베이스 존재 확인
if [ ! -f /app/db/untatiz_db.db ]; then
    echo "[Entrypoint] Warning: Database file not found. Please mount the database volume."
fi

# 스크립트 실행 권한 설정
chmod +x /app/docker/scraper_loop.sh 2>/dev/null || true

echo "[Entrypoint] Startup setup complete."
echo "============================================"
echo "Starting supervisord..."
echo "============================================"

# 명령 실행 (기본: supervisord)
exec "$@"
