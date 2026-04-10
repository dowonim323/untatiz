#!/bin/bash
# Scraper loop wrapper - 1분마다 scraper 실행
# supervisord에서 관리하는 프로세스로 실행됨

set -e

INTERVAL=60  # 1분

echo "[Scraper Loop] Starting scraper loop with ${INTERVAL}s interval..."

while true; do
    echo "[Scraper Loop] $(date '+%Y-%m-%d %H:%M:%S') - Running scraper..."
    
    # Python 스크래퍼 실행
    python /app/untatiz.py || {
        echo "[Scraper Loop] $(date '+%Y-%m-%d %H:%M:%S') - Scraper exited with error, continuing..."
    }
    
    echo "[Scraper Loop] $(date '+%Y-%m-%d %H:%M:%S') - Sleeping for ${INTERVAL}s..."
    sleep $INTERVAL
done
